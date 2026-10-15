"""Telegram bot-account, progress, and send-recovery invariants."""

from __future__ import annotations

import asyncio
import io
import json
import os
import secrets
import socket
import subprocess
import sys
import tarfile
from datetime import timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import func, select, text

from core.config import SettlingPolicy
from core.errors import DomainError
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    DeliveryReconciliation,
    Message,
    TelegramAdmission,
    TelegramImmediateTurn,
    TelegramProviderAccount,
    TelegramResponseAttempt,
    TurnInputMessage,
)
from schemas.contracts import Channel, TurnClaim, TurnExecute
from services import managed_execution, turns
from services.common import fingerprint
from services.telegram import expected_response_attempt_id, select_immediate
from tests.conftest import ROOT
from tests.test_managed_migration import migrate
from tests.test_telegram import (
    AGENT,
    INTEGRATION,
    OWNER,
    DeterministicManagedPort,
    admit,
    inbound,
)

pytest_plugins = ("tests.test_telegram",)

OLD_0005_SHA = "13972164da3e4f5ad392c79eb60ea2b9e877bc1c"


def _identity(payload: dict[str, object], *, include_bot_id: bool = False) -> dict[str, object]:
    keys = ["owner_id", "integration_id", "agent_id", "provider_thread_id",
            "provider_event_id"]
    if include_bot_id:
        keys.append("bot_id")
    return {key: payload[key] for key in keys}


def _payload(*, bot_id: int = 999, **kwargs: Any) -> dict[str, object]:
    payload = inbound(**kwargs)
    payload["bot_id"] = bot_id
    payload["provider_event_id"] = (
        f"b{bot_id}c{payload['chat_id']}m{payload['message_id']}"
    )
    return payload


async def _run_immediate(h: dict[str, Any], count: int) -> None:
    from services.telegram_worker import TelegramWorker

    worker = TelegramWorker(h["database"], h["settings"], DeterministicManagedPort())
    for _ in range(count):
        assert await worker.run_once()


class Accepted0005TelegramRuntime:
    """Run the exact retained 0005 Telegram API against this test's database."""

    def __init__(self, h: dict[str, Any], database_url: str, tmp_path: Path,
                 network_allowlist: set[tuple[str, int]]) -> None:
        source_archive = subprocess.run(
            ["git", "archive", OLD_0005_SHA], cwd=ROOT, check=True, capture_output=True
        ).stdout
        self.source = tmp_path / "accepted-0005-telegram-source"
        self.source.mkdir()
        with tarfile.open(fileobj=io.BytesIO(source_archive)) as archive:
            archive.extractall(self.source, filter="data")
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        self.port = int(listener.getsockname()[1])
        listener.close()
        self.url = f"http://127.0.0.1:{self.port}"
        network_allowlist.add(("127.0.0.1", self.port))
        self.token = h["gateway_headers"]["X-Service-Token"]
        self.ai_token = secrets.token_urlsafe(32)
        self.worker_token = secrets.token_urlsafe(32)
        self.database_url = database_url
        self.log_path = tmp_path / "accepted-0005-telegram.log"
        self.log = None
        self.process: subprocess.Popen | None = None
        self.client = httpx.AsyncClient(timeout=10, trust_env=False)

    async def start(self) -> None:
        grant = [{
            "owner_id": OWNER,
            "integration_id": str(INTEGRATION),
            "channel": "TELEGRAM",
            "provider_namespace": "main-chota.telegram.v1",
        }]
        principals = [{
            "principal_id": "old-telegram-gateway",
            "role": "TELEGRAM",
            "token": self.token,
            "grants": grant,
        }, {
            "principal_id": "old-telegram-worker",
            "role": "TELEGRAM_PROCESSING",
            "token": self.worker_token,
            "grants": grant,
        }]
        env = {
            **os.environ,
            "DATABASE_URL": self.database_url,
            "CONVERSATION_AUTH_PRINCIPALS": json.dumps(principals),
            "TELEGRAM_CANARY_INTEGRATION_IDS": json.dumps([str(INTEGRATION)]),
            "MANAGED_AI_BINDINGS": json.dumps([{
                "owner_id": OWNER,
                "integration_id": str(INTEGRATION),
                "channel": "TELEGRAM",
                "agent_id": str(AGENT),
                "service_token": self.ai_token,
            }]),
            "MANAGED_AI_BASE_URL": "http://127.0.0.1:1",
            "PYTHONPATH": str(self.source),
            "PYTHONDONTWRITEBYTECODE": "1",
            "LOG_LEVEL": "INFO",
        }
        self.environment = env
        self.log = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
             "--port", str(self.port), "--no-access-log", "--log-level", "info"],
            cwd=self.source, env=env, stdout=self.log, stderr=self.log,
        )
        deadline = asyncio.get_running_loop().time() + 12
        last_status = "no response"
        while asyncio.get_running_loop().time() < deadline:
            if self.process.poll() is not None:
                pytest.fail(
                    "Exact 0005 Telegram API exited before readiness: "
                    f"exit={self.process.returncode}; "
                    f"{self.log_path.read_text(errors='replace')}"
                )
            try:
                ready = await self.client.get(self.url + "/health")
                last_status = f"{ready.status_code}:{ready.text}"
                if ready.status_code == 200:
                    return
            except httpx.HTTPError as exc:
                last_status = type(exc).__name__
            await asyncio.sleep(0.05)
        pytest.fail(
            f"Exact 0005 Telegram API not ready ({last_status}); "
            f"log={self.log_path.read_text(errors='replace')}"
        )

    async def post(self, path: str, body: dict[str, object]) -> httpx.Response:
        return await self.client.post(
            self.url + "/internal/v1/telegram" + path, json=body,
            headers={"X-Service-Token": self.token},
        )

    async def select_mark_then_select(self) -> tuple[UUID, UUID]:
        """Use the exact archived 0005 selector and marker path, without AI I/O."""
        script = self.source / "goal5_select_marked_turn.py"
        script.write_text("""
import asyncio
import json
from pydantic import SecretStr
from core.config import load_settings
from db.database import Database
from schemas.contracts import TurnClaim, TurnExecute
from services import managed_execution, telegram, turns

async def main():
    settings = load_settings()
    principal = next(entry.principal() for entry in settings.auth_principals
                     if entry.principal_id == 'old-telegram-worker')
    database = Database(settings.database_url.get_secret_value())
    first = await telegram.select_immediate(database, principal, settings)
    assert first is not None
    claim = await turns.claim_turn(database, principal, first, TurnClaim(
        worker_ref='old-telegram-worker', lease_seconds=30))
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(
        database, principal, first, proof, settings)
    prepared = managed_execution.prepare_snapshot(snapshot)
    await managed_execution.mark_dispatch(database, principal, first, proof, prepared)
    second = await telegram.select_immediate(database, principal, settings)
    assert second is not None and second != first
    print(json.dumps({'marked_turn_id': str(first), 'unmarked_turn_id': str(second)}))
    await database.dispose()

asyncio.run(main())
""", encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(script)], cwd=self.source, env=self.environment,
            capture_output=True, text=True, timeout=30, check=True,
        )
        selected = json.loads(result.stdout.strip().splitlines()[-1])
        return UUID(selected["marked_turn_id"]), UUID(selected["unmarked_turn_id"])

    async def close(self) -> None:
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.wait(timeout=8)
            self.process = None
        await self.client.aclose()
        if self.log is not None:
            self.log.close()

    async def __aenter__(self) -> Accepted0005TelegramRuntime:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()


async def _create_0005_turn(
    sql, admission_id: str,
    *, complete_with: str | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    async with sql.connect() as connection:
        row = (await connection.execute(text(
            "SELECT a.conversation_id::text AS conversation_id, "
            "a.canonical_message_id::text AS message_id, m.sequence, "
            "c.input_revision, c.authority_revision FROM telegram_admissions a "
            "JOIN messages m ON m.id=a.canonical_message_id "
            "JOIN conversations c ON c.id=a.conversation_id WHERE a.id=:id"
        ), {"id": admission_id})).mappings().one()
    turn_id = uuid4()
    terminal = complete_with is not None
    async with sql.begin() as connection:
        await connection.execute(text("""
            INSERT INTO agent_turns (
              id,conversation_id,selection_key,creation_fingerprint,
              input_first_sequence,input_last_sequence,context_last_sequence,
              input_revision,authority_revision,status,claim_epoch,claim_token_digest,
              worker_ref,lease_expires_at,started_at,completed_at,outcome,draft_text,
              terminal_fingerprint,managed_agent_id,managed_dispatch_epoch,
              managed_dispatch_started_at,managed_request_fingerprint
            ) VALUES (
              :id,:conversation,:selection,:creation_fingerprint,
              :sequence,:sequence,:sequence,:input_revision,:authority_revision,
              :status,1,repeat('c',64),'historical-telegram-worker',
              CASE WHEN :terminal THEN clock_timestamp()+interval '1 minute'
                   ELSE clock_timestamp()-interval '1 second' END,
              clock_timestamp()-interval '2 seconds',
              CASE WHEN :terminal THEN clock_timestamp() ELSE NULL END,
              CASE WHEN :terminal THEN 'DRAFT_READY' ELSE NULL END,
              :draft_text,
              CASE WHEN :terminal THEN repeat('d',64) ELSE NULL END,
              CASE WHEN :terminal THEN NULL ELSE CAST(:agent AS uuid) END,
              CASE WHEN :terminal THEN NULL ELSE 1 END,
              CASE WHEN :terminal THEN NULL ELSE clock_timestamp()-interval '1 second' END,
              CASE WHEN :terminal THEN NULL ELSE repeat('a',64) END
            )
        """), {
            "id": turn_id,
            "conversation": row["conversation_id"],
            "selection": uuid4(),
            "creation_fingerprint": "b" * 64,
            "sequence": row["sequence"],
            "input_revision": row["input_revision"],
            "authority_revision": row["authority_revision"],
            "status": "COMPLETED" if terminal else "CLAIMED",
            "terminal": terminal,
            "draft_text": complete_with,
            "agent": AGENT,
        })
    async with sql.begin() as connection:
        await connection.execute(text(
            "INSERT INTO telegram_immediate_turns(message_id,conversation_id,turn_id) "
            "VALUES (:message,:conversation,:turn)"
        ), {"message": row["message_id"], "conversation": row["conversation_id"],
            "turn": turn_id})
    turn = {
        "id": str(turn_id), "status": "COMPLETED" if terminal else "CLAIMED",
        "claim_epoch": 1, "managed_dispatch_epoch": None if terminal else 1,
    }
    # This seed is retained 0005-shaped fixture data. The real archived 0005
    # app creates the admissions and authorizes the provider dispatch below.
    return row, turn, {"claim_epoch": 1}


@pytest.mark.asyncio
async def test_first_managed_bot_writer_pins_account_and_same_bot_can_rotate(
    sql, telegram_harness,
) -> None:
    h = telegram_harness
    first = _payload(message_id=101, update_id=201, chat_id=30_101, bot_id=999)
    competing = _payload(message_id=102, update_id=202, chat_id=30_102, bot_id=1_000)
    responses = await asyncio.gather(*(
        h["client"].post("/internal/v1/telegram/admissions", json=payload,
                         headers=h["gateway_headers"])
        for payload in (first, competing)
    ))

    assert sorted(response.status_code for response in responses) == [200, 404]
    winner = first if responses[0].status_code == 200 else competing
    loser = competing if responses[0].status_code == 200 else first
    result = responses[0] if responses[0].status_code == 200 else responses[1]
    assert result.json()["bot_id"] == winner["bot_id"]

    same_bot = _payload(message_id=103, update_id=203, chat_id=30_103,
                        bot_id=int(winner["bot_id"]))
    rotated = await h["client"].post(
        "/internal/v1/telegram/admissions", json=same_bot, headers=h["gateway_headers"]
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["bot_id"] == winner["bot_id"]
    async with h["database"].session_factory() as session:
        account = await session.get(TelegramProviderAccount, (OWNER, INTEGRATION))
        assert account is not None and account.state == "BOUND"
        assert account.bot_id == str(winner["bot_id"])

    wrong_identity = _identity(loser, include_bot_id=True)
    wrong = await h["client"].post(
        "/internal/v1/telegram/results", json=wrong_identity,
        headers=h["gateway_headers"],
    )
    assert wrong.status_code == 404
    # A token rotation that keeps the immutable provider bot can still dispatch
    # new generic v2 work through the BOUND-account guard.
    await _run_immediate(h, 2)
    async with h["database"].session_factory() as session:
        completed = await session.scalar(select(func.count()).select_from(AgentTurn).where(
            AgentTurn.status == "COMPLETED",
        ))
        assert completed == 2


@pytest.mark.asyncio
async def test_work_requires_runtime_bot_but_old_result_and_send_requests_remain_compatible(
    sql, telegram_harness,
) -> None:
    h = telegram_harness
    admitted = await admit(h["client"], h["gateway_headers"])
    assert admitted.status_code == 200, admitted.text
    await _run_immediate(h, 1)
    body = inbound()
    identity = _identity(body)

    missing_bot = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT)},
        headers=h["gateway_headers"],
    )
    assert missing_bot.status_code == 422
    work = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 999},
        headers=h["gateway_headers"],
    )
    assert work.status_code == 200 and len(work.json()["items"]) == 1
    item = work.json()["items"][0]
    assert item["bot_id"] == 999
    assert item["work_kind"] == "SEND_CANDIDATE"
    assert item["admission_id"] == admitted.json()["admission_id"]

    progress = await h["client"].post(
        "/internal/v1/telegram/results", json=identity,
        headers=h["gateway_headers"],
    )
    assert progress.status_code == 200
    assert progress.json()["bot_id"] == 999
    assert progress.json()["admission_id"] == admitted.json()["admission_id"]
    assert progress.json()["progress_disposition"] == "WAIT"

    begin_body = {**identity, "response_attempt_id": item["response_attempt_id"]}
    begun = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=begin_body,
        headers=h["gateway_headers"],
    )
    assert begun.status_code == 200 and begun.json()["send"] is True
    assert begun.json()["bot_id"] == 999
    assert begun.json()["admission_id"] == item["admission_id"]
    assert "admission_id" not in begin_body

    complete_body = {**begin_body, "outcome": "PROVIDER_ACCEPTED",
                     "provider_message_id": "801"}
    completed = await h["client"].post(
        "/internal/v1/telegram/responses/complete", json=complete_body,
        headers=h["gateway_headers"],
    )
    assert completed.status_code == 200
    assert completed.json()["bot_id"] == 999
    assert completed.json()["admission_id"] == item["admission_id"]
    final = await h["client"].post(
        "/internal/v1/telegram/results", json=identity,
        headers=h["gateway_headers"],
    )
    assert final.status_code == 200
    assert final.json()["progress_disposition"] == "TERMINAL"
    assert final.json()["delivery_state"] == "PROVIDER_ACCEPTED"


@pytest.mark.asyncio
async def test_progress_disposition_truthfully_covers_every_delivery_state(
    sql, telegram_harness,
) -> None:
    h = telegram_harness
    admitted = await admit(h["client"], h["gateway_headers"])
    assert admitted.status_code == 200, admitted.text
    identity = _identity(inbound())
    not_started = await h["client"].post(
        "/internal/v1/telegram/results", json=identity,
        headers=h["gateway_headers"],
    )
    assert not_started.status_code == 200, not_started.text
    assert not_started.json()["delivery_state"] == "NOT_STARTED"
    assert not_started.json()["progress_disposition"] == "WAIT"
    await _run_immediate(h, 1)
    work = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 999},
        headers=h["gateway_headers"],
    )
    assert work.status_code == 200, work.text
    assert len(work.json()["items"]) == 1
    item = work.json()["items"][0]
    begun = await h["client"].post(
        "/internal/v1/telegram/responses/begin",
        json={**identity, "response_attempt_id": item["response_attempt_id"]},
        headers=h["gateway_headers"],
    )
    assert begun.status_code == 200 and begun.json()["send"] is True
    states = {
        "GENERATED": "WAIT",
        "DISPATCH_REQUESTED": "WAIT",
        "SUPERSEDED": "TERMINAL",
        "PROVIDER_ACCEPTED": "TERMINAL",
        "DELIVERED": "TERMINAL",
        "READ": "TERMINAL",
        "FAILED": "TERMINAL",
        "UNKNOWN": "TERMINAL",
        "PARTIAL": "TERMINAL",
        "INTERRUPTED": "TERMINAL",
    }
    async with h["database"].session_factory() as session:
        admission = await session.get(
            TelegramAdmission, UUID(admitted.json()["admission_id"])
        )
        assert admission is not None and admission.canonical_message_id is not None
        link = await session.scalar(select(TelegramImmediateTurn).where(
            TelegramImmediateTurn.message_id == admission.canonical_message_id,
        ))
        assert link is not None
        turn = await session.get(AgentTurn, link.turn_id)
        assert turn is not None
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.source_turn_id == turn.id,
        ))
        assert delivery is not None
        delivery_id = delivery.id

    observed: dict[str, str] = {}
    for delivery_state, disposition in states.items():
        async with h["database"].session_factory() as session, session.begin():
            delivery = await session.get(DeliveryAttempt, delivery_id)
            assert delivery is not None
            delivery.status = delivery_state
            if delivery_state == "GENERATED":
                delivery.revision = 0
                delivery.dispatch_requested_at = None
                delivery.provider_accepted_at = None
                delivery.delivered_at = None
                delivery.read_at = None
                delivery.provider_message_id = None
                delivery.error_code = None
            else:
                delivery.revision = 1
                delivery.dispatch_requested_at = delivery.created_at
                delivery.provider_accepted_at = delivery.created_at
                delivery.delivered_at = delivery.created_at
                delivery.read_at = delivery.created_at
                delivery.error_code = (
                    "UNSPECIFIED" if delivery_state in {
                        "FAILED", "UNKNOWN", "PARTIAL", "INTERRUPTED",
                    } else None
                )
        progress = await h["client"].post(
            "/internal/v1/telegram/results", json=identity,
            headers=h["gateway_headers"],
        )
        assert progress.status_code == 200, progress.text
        body = progress.json()
        assert body["delivery_state"] == delivery_state
        assert body["progress_disposition"] == disposition
        observed[delivery_state] = body["progress_disposition"]

    assert observed == states


@pytest.mark.asyncio
async def test_delivery_work_scans_131_malformed_candidates_to_send_and_recovery_siblings(
    sql, telegram_harness,
) -> None:
    h = telegram_harness
    seed = await admit(
        h["client"], h["gateway_headers"], message_id=701, update_id=801,
        chat_id=37_001,
    )
    assert seed.status_code == 200, seed.text
    seed_identity = inbound(message_id=701, update_id=801, chat_id=37_001)
    async with h["database"].session_factory() as session:
        conversation = await session.scalar(select(Conversation).where(
            Conversation.provider_thread_id == seed_identity["provider_thread_id"],
            Conversation.integration_id == INTEGRATION,
        ))
        assert conversation is not None
        now = await session.scalar(select(func.clock_timestamp()))
        assert now is not None
        for index in range(131):
            turn_id = uuid4()
            created_at = now - timedelta(days=1) + timedelta(microseconds=index)
            session.add(AgentTurn(
                id=turn_id,
                conversation_id=conversation.id,
                selection_key=uuid4(),
                creation_fingerprint=fingerprint({"malformed_candidate": str(turn_id)}),
                input_first_sequence=1,
                input_last_sequence=1,
                context_last_sequence=1,
                input_revision=1,
                authority_revision=0,
                status="COMPLETED",
                claim_epoch=1,
                claim_token_digest=sha256(str(turn_id).encode()).hexdigest(),
                worker_ref="fixture-worker",
                lease_expires_at=created_at,
                created_at=created_at,
                started_at=created_at,
                completed_at=created_at,
                outcome="DRAFT_READY",
                draft_text="malformed retained candidate",
                terminal_fingerprint=fingerprint({"terminal_turn": str(turn_id)}),
                operation_refs=[],
            ))
        await session.commit()

    send_payload = _payload(message_id=702, update_id=802, chat_id=37_002)
    recover_payload = _payload(message_id=703, update_id=803, chat_id=37_003)
    send_admission = await h["client"].post(
        "/internal/v1/telegram/admissions", json=send_payload,
        headers=h["gateway_headers"],
    )
    recover_admission = await h["client"].post(
        "/internal/v1/telegram/admissions", json=recover_payload,
        headers=h["gateway_headers"],
    )
    assert send_admission.status_code == recover_admission.status_code == 200
    await _run_immediate(h, 2)

    response_attempt_id = str(expected_response_attempt_id(
        UUID(recover_admission.json()["admission_id"])
    ))
    begun = await h["client"].post(
        "/internal/v1/telegram/responses/begin",
        json={**_identity(recover_payload, include_bot_id=True),
              "response_attempt_id": response_attempt_id},
        headers=h["gateway_headers"],
    )
    assert begun.status_code == 200 and begun.json()["send"] is True

    work = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 999},
        headers=h["gateway_headers"],
    )
    assert work.status_code == 200, work.text
    items = {item["admission_id"]: item for item in work.json()["items"]}
    assert set(items) == {
        send_admission.json()["admission_id"], recover_admission.json()["admission_id"],
    }
    assert items[send_admission.json()["admission_id"]]["work_kind"] == "SEND_CANDIDATE"
    assert items[recover_admission.json()["admission_id"]]["work_kind"] == "RECOVER_ONLY"


@pytest.mark.asyncio
async def test_authority_fence_preserves_exact_ai_and_response_recovery(
    sql, telegram_harness,
) -> None:
    h = telegram_harness
    pending_payload = _payload(message_id=301, update_id=401, chat_id=31_301)
    candidate_payload = _payload(message_id=302, update_id=402, chat_id=31_302)
    pending_admission = await h["client"].post(
        "/internal/v1/telegram/admissions", json=pending_payload,
        headers=h["gateway_headers"],
    )
    candidate_admission = await h["client"].post(
        "/internal/v1/telegram/admissions", json=candidate_payload,
        headers=h["gateway_headers"],
    )
    assert pending_admission.status_code == candidate_admission.status_code == 200
    await _run_immediate(h, 2)

    pending_identity = _identity(pending_payload, include_bot_id=True)
    attempt_id = str(expected_response_attempt_id(
        UUID(pending_admission.json()["admission_id"])
    ))
    begin_body = {**pending_identity, "response_attempt_id": attempt_id}
    begun = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=begin_body,
        headers=h["gateway_headers"],
    )
    assert begun.status_code == 200 and begun.json()["send"] is True
    assert begun.json()["work_kind"] == "SEND_CANDIDATE"

    async with h["database"].session_factory() as session, session.begin():
        account = await session.get(TelegramProviderAccount, (OWNER, INTEGRATION))
        assert account is not None
        for payload in (pending_payload, candidate_payload):
            conversation = await session.scalar(select(Conversation).where(
                Conversation.provider_thread_id == payload["provider_thread_id"],
                Conversation.integration_id == INTEGRATION,
            ))
            assert conversation is not None
            conversation.control_mode = "HUMAN"
            conversation.authority_revision += 1

    pending_progress = await h["client"].post(
        "/internal/v1/telegram/results", json=pending_identity,
        headers=h["gateway_headers"],
    )
    assert pending_progress.status_code == 200
    assert pending_progress.json()["progress_disposition"] == "WAIT"
    assert pending_progress.json()["delivery_state"] == "DISPATCH_REQUESTED"
    candidate_progress = await h["client"].post(
        "/internal/v1/telegram/results", json=_identity(candidate_payload),
        headers=h["gateway_headers"],
    )
    assert candidate_progress.status_code == 200
    assert candidate_progress.json()["progress_disposition"] == "AUTHORITY_FENCED"
    assert candidate_progress.json()["delivery_state"] == "NOT_STARTED"
    async with h["database"].session_factory() as session:
        candidate_turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.conversation_id == select(Conversation.id).where(
                Conversation.provider_thread_id == candidate_payload["provider_thread_id"],
                Conversation.integration_id == INTEGRATION,
            ).scalar_subquery(),
        ))
        assert candidate_turn is not None
        assert await session.scalar(select(func.count()).select_from(TelegramResponseAttempt)
                                    .where(TelegramResponseAttempt.conversation_id ==
                                           candidate_turn.conversation_id)) == 0
        assert await session.scalar(select(func.count()).select_from(DeliveryAttempt)
                                    .where(DeliveryAttempt.conversation_id ==
                                           candidate_turn.conversation_id)) == 0
        assert await session.scalar(select(func.count()).select_from(DeliveryReconciliation)
                                    .where(DeliveryReconciliation.conversation_id ==
                                           candidate_turn.conversation_id)) == 0

    conflicted_candidate = await h["client"].post(
        "/internal/v1/telegram/results", json=_identity(candidate_payload),
        headers=h["gateway_headers"],
    )
    assert conflicted_candidate.status_code == 200
    assert conflicted_candidate.json()["progress_disposition"] == "AUTHORITY_FENCED"

    work = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 999},
        headers=h["gateway_headers"],
    )
    assert work.status_code == 200
    assert len(work.json()["items"]) == 1
    recovery = work.json()["items"][0]
    assert recovery["admission_id"] == pending_admission.json()["admission_id"]
    assert recovery["work_kind"] == "RECOVER_ONLY"
    wrong_bot = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 1_000},
        headers=h["gateway_headers"],
    )
    assert wrong_bot.status_code == 200 and wrong_bot.json()["items"] == []

    wrong_bot_begin = await h["client"].post(
        "/internal/v1/telegram/responses/begin",
        json={**pending_identity, "bot_id": 1_000,
              "response_attempt_id": attempt_id},
        headers=h["gateway_headers"],
    )
    assert wrong_bot_begin.status_code == 404
    async with h["database"].session_factory() as session:
        attempt = await session.get(TelegramResponseAttempt, UUID(attempt_id))
        assert attempt is not None and attempt.outcome is None
    delivery_before_recovery = await h["client"].post(
        "/internal/v1/telegram/results", json=pending_identity,
        headers=h["gateway_headers"],
    )
    assert delivery_before_recovery.status_code == 200
    assert delivery_before_recovery.json()["delivery_state"] == "DISPATCH_REQUESTED"
    assert delivery_before_recovery.json()["progress_disposition"] == "WAIT"

    recovered_begin = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=begin_body,
        headers=h["gateway_headers"],
    )
    assert recovered_begin.status_code == 200
    assert recovered_begin.json()["work_kind"] == "RECOVER_ONLY"
    assert recovered_begin.json()["send"] is False
    unknown_progress = await h["client"].post(
        "/internal/v1/telegram/results", json=pending_identity,
        headers=h["gateway_headers"],
    )
    assert unknown_progress.status_code == 200
    assert unknown_progress.json()["delivery_state"] == "UNKNOWN"
    assert unknown_progress.json()["progress_disposition"] == "TERMINAL"

    # A persisted managed-dispatch marker remains resumable after both account
    # conflict and authority change; unmarked, new work stays fenced.
    async with h["database"].session_factory() as session, session.begin():
        turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.conversation_id == select(Conversation.id).where(
                Conversation.provider_thread_id == candidate_payload["provider_thread_id"],
                Conversation.integration_id == INTEGRATION,
            ).scalar_subquery(),
        ))
        assert turn is not None and turn.managed_dispatch_epoch is not None
        now = await session.scalar(select(func.clock_timestamp()))
        assert now is not None
        turn.status = "CLAIMED"
        turn.completed_at = None
        turn.outcome = None
        turn.draft_text = None
        turn.failure_code = None
        turn.terminal_fingerprint = None
        turn.operation_refs = []
        turn.lease_expires_at = now - timedelta(seconds=1)

    retained_progress = await h["client"].post(
        "/internal/v1/telegram/results", json=_identity(candidate_payload),
        headers=h["gateway_headers"],
    )
    assert retained_progress.status_code == 200
    assert retained_progress.json()["state"] == "PENDING"
    assert retained_progress.json()["progress_disposition"] == "WAIT"
    selected = await select_immediate(
        h["database"], next(
            item.principal() for item in h["settings"].auth_principals
            if item.principal_id == "telegram-worker"
        ), h["settings"],
    )
    assert selected == turn.id

    # A late, correlated provider fact can resolve UNKNOWN without authorizing
    # another provider send or mutating any unrelated admission.
    late_outcome = await h["client"].post(
        "/internal/v1/telegram/responses/complete",
        json={**begin_body, "outcome": "PROVIDER_ACCEPTED",
              "provider_message_id": "802"},
        headers=h["gateway_headers"],
    )
    assert late_outcome.status_code == 200
    assert late_outcome.json()["outcome"] == "PROVIDER_ACCEPTED"
    terminal = await h["client"].post(
        "/internal/v1/telegram/results", json=pending_identity,
        headers=h["gateway_headers"],
    )
    assert terminal.status_code == 200
    assert terminal.json()["progress_disposition"] == "TERMINAL"
    assert terminal.json()["delivery_state"] == "PROVIDER_ACCEPTED"
    after_unknown = await h["client"].post(
        "/internal/v1/telegram/responses/work",
        json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
              "agent_id": str(AGENT), "bot_id": 999},
        headers=h["gateway_headers"],
    )
    assert after_unknown.status_code == 200 and after_unknown.json()["items"] == []


@pytest.mark.asyncio
async def test_historical_0005_conflict_keeps_only_exact_response_and_marked_ai_recovery(
    telegram_harness, sql, database_url, tmp_path, network_allowlist,
) -> None:
    h = telegram_harness
    await h["database"].dispose()
    await sql.dispose()
    migrate(database_url, "downgrade", "0005_telegram_canary")
    runtime = Accepted0005TelegramRuntime(h, database_url, tmp_path, network_allowlist)
    upgraded = False
    try:
        await runtime.start()
        send_payload = _payload(message_id=501, update_id=601, chat_id=35_501)
        marked_payload = _payload(message_id=502, update_id=602, chat_id=35_502)
        conflicting_payload = _payload(
            message_id=503, update_id=603, chat_id=35_503, bot_id=1_000,
        )
        old_admissions = []
        for payload in (send_payload, marked_payload, conflicting_payload):
            admitted = await runtime.post("/admissions", payload)
            assert admitted.status_code == 200, admitted.text
            assert admitted.json()["route"] == "MANAGED"
            old_admissions.append(admitted.json())

        _, send_turn, _ = await _create_0005_turn(
            sql, old_admissions[0]["admission_id"],
            complete_with="retained pre-0006 draft",
        )
        attempt_id = str(expected_response_attempt_id(UUID(old_admissions[0]["admission_id"])))
        old_begin = await runtime.post("/responses/begin", {
            **_identity(send_payload), "response_attempt_id": attempt_id,
        })
        assert old_begin.status_code == 200, old_begin.text
        assert old_begin.json()["send"] is True and old_begin.json()["replayed"] is False

        _, marked_turn, _ = await _create_0005_turn(
            sql, old_admissions[1]["admission_id"], complete_with=None,
        )
        assert marked_turn["status"] == "CLAIMED"
        assert marked_turn["managed_dispatch_epoch"] == marked_turn["claim_epoch"]
        async with sql.connect() as connection:
            assert await connection.scalar(text(
                "SELECT count(DISTINCT bot_id) FROM telegram_admissions "
                "WHERE owner_id=:owner AND integration_id=:integration AND route='MANAGED'"
            ), {"owner": OWNER, "integration": str(INTEGRATION)}) == 2
            assert await connection.scalar(text(
                "SELECT status FROM delivery_attempts WHERE source_turn_id=:turn"
            ), {"turn": send_turn["id"]}) == "DISPATCH_REQUESTED"
            assert await connection.scalar(text(
                "SELECT count(*) FROM telegram_response_attempts WHERE id=:attempt"
            ), {"attempt": attempt_id}) == 1

        await runtime.close()
        migrate(database_url, "upgrade", "head")
        upgraded = True
        assert await h["database"].ready()

        async with h["database"].session_factory() as session:
            account = await session.get(TelegramProviderAccount, (OWNER, INTEGRATION))
            assert account is not None and account.state == "CONFLICT"
            assert account.bot_id is None

        pending_identity = _identity(send_payload)
        live_dispatch = await h["client"].post(
            "/internal/v1/telegram/results", json=pending_identity,
            headers=h["gateway_headers"],
        )
        assert live_dispatch.status_code == 200
        assert live_dispatch.json()["progress_disposition"] == "WAIT"
        assert live_dispatch.json()["delivery_state"] == "DISPATCH_REQUESTED"

        blocked_new = await h["client"].post(
            "/internal/v1/telegram/results", json=_identity(conflicting_payload),
            headers=h["gateway_headers"],
        )
        assert blocked_new.status_code == 200
        assert blocked_new.json()["progress_disposition"] == "ACCOUNT_CONFLICT"
        no_send = await h["client"].post(
            "/internal/v1/telegram/responses/work",
            json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
                  "agent_id": str(AGENT), "bot_id": 1_000},
            headers=h["gateway_headers"],
        )
        assert no_send.status_code == 200 and no_send.json()["items"] == []
        recovery_work = await h["client"].post(
            "/internal/v1/telegram/responses/work",
            json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
                  "agent_id": str(AGENT), "bot_id": 999},
            headers=h["gateway_headers"],
        )
        assert recovery_work.status_code == 200
        assert len(recovery_work.json()["items"]) == 1
        item = recovery_work.json()["items"][0]
        assert item["admission_id"] == old_admissions[0]["admission_id"]
        assert item["response_attempt_id"] == attempt_id
        assert item["work_kind"] == "RECOVER_ONLY"

        wrong_begin = await h["client"].post(
            "/internal/v1/telegram/responses/begin",
            json={**_identity(send_payload, include_bot_id=True), "bot_id": 1_000,
                  "response_attempt_id": attempt_id},
            headers=h["gateway_headers"],
        )
        assert wrong_begin.status_code == 404
        async with h["database"].session_factory() as session:
            attempt = await session.get(TelegramResponseAttempt, UUID(attempt_id))
            assert attempt is not None and attempt.outcome is None
            delivery = await session.get(DeliveryAttempt, attempt.delivery_id)
            assert delivery is not None and delivery.status == "DISPATCH_REQUESTED"

        recover_begin = await h["client"].post(
            "/internal/v1/telegram/responses/begin",
            json={**_identity(send_payload, include_bot_id=True),
                  "response_attempt_id": attempt_id},
            headers=h["gateway_headers"],
        )
        assert recover_begin.status_code == 200
        assert recover_begin.json()["work_kind"] == "RECOVER_ONLY"
        assert recover_begin.json()["send"] is False
        assert recover_begin.json()["admission_id"] == old_admissions[0]["admission_id"]

        marked_identity = _identity(marked_payload, include_bot_id=True)
        marked_progress = await h["client"].post(
            "/internal/v1/telegram/results", json=marked_identity,
            headers=h["gateway_headers"],
        )
        assert marked_progress.status_code == 200
        assert marked_progress.json()["progress_disposition"] == "WAIT"
        assert marked_progress.json()["turn_id"] == marked_turn["id"]
        selected = await select_immediate(
            h["database"], next(item.principal()
                                 for item in h["settings"].auth_principals
                                 if item.principal_id == "telegram-worker"), h["settings"],
        )
        assert selected == UUID(marked_turn["id"])
        from services.telegram_worker import TelegramWorker

        port = DeterministicManagedPort()
        worker = TelegramWorker(h["database"], h["settings"], port)
        assert await worker.run_once()
        assert port.calls == 0
        marked_terminal = await h["client"].post(
            "/internal/v1/telegram/results", json=marked_identity,
            headers=h["gateway_headers"],
        )
        assert marked_terminal.status_code == 200
        assert marked_terminal.json()["state"] == "UNKNOWN"
        assert marked_terminal.json()["progress_disposition"] == "TERMINAL"

        late_success = await h["client"].post(
            "/internal/v1/telegram/responses/complete",
            json={**_identity(send_payload, include_bot_id=True),
                  "response_attempt_id": attempt_id,
                  "outcome": "PROVIDER_ACCEPTED", "provider_message_id": "881"},
            headers=h["gateway_headers"],
        )
        assert late_success.status_code == 200
        final_progress = await h["client"].post(
            "/internal/v1/telegram/results", json=pending_identity,
            headers=h["gateway_headers"],
        )
        assert final_progress.status_code == 200
        assert final_progress.json()["progress_disposition"] == "TERMINAL"
        assert final_progress.json()["delivery_state"] == "PROVIDER_ACCEPTED"
        assert (await h["client"].post(
            "/internal/v1/telegram/responses/work",
            json={"owner_id": OWNER, "integration_id": str(INTEGRATION),
                  "agent_id": str(AGENT), "bot_id": 999},
            headers=h["gateway_headers"],
        )).json()["items"] == []
    finally:
        await runtime.close()
        if not upgraded:
            migrate(database_url, "upgrade", "head")
            await h["database"].ready()


@pytest.mark.asyncio
async def test_historical_conflict_blocks_generic_unmarked_telegram_execution(
    telegram_harness, sql, database_url, tmp_path, network_allowlist,
) -> None:
    """Old selector-created v2 work must meet the account fence at generic execute."""
    from core.config import AuthPrincipal

    h = telegram_harness
    await h["database"].dispose()
    await sql.dispose()
    migrate(database_url, "downgrade", "0005_telegram_canary")
    runtime = Accepted0005TelegramRuntime(h, database_url, tmp_path, network_allowlist)
    upgraded = False
    try:
        await runtime.start()
        first = _payload(message_id=701, update_id=801, chat_id=37_701, bot_id=999)
        second = _payload(message_id=702, update_id=802, chat_id=37_702, bot_id=1_000)
        accepted = []
        for payload in (first, second):
            response = await runtime.post("/admissions", payload)
            assert response.status_code == 200, response.text
            accepted.append(response.json())

        marked_turn_id, unmarked_turn_id = await runtime.select_mark_then_select()
        async with sql.connect() as connection:
            before_upgrade = (await connection.execute(text(
                "SELECT id::text AS id, managed_contract_version, managed_dispatch_epoch, "
                "managed_dispatch_started_at, managed_request_fingerprint "
                "FROM agent_turns WHERE id IN (:marked,:unmarked) ORDER BY id"
            ), {"marked": marked_turn_id, "unmarked": unmarked_turn_id})).mappings().all()
            assert {row["id"] for row in before_upgrade} == {
                str(marked_turn_id), str(unmarked_turn_id),
            }
            assert all(row["managed_contract_version"] == 2 for row in before_upgrade)
            assert next(row for row in before_upgrade if row["id"] == str(marked_turn_id))[
                "managed_dispatch_epoch"] == 1
            assert next(row for row in before_upgrade if row["id"] == str(unmarked_turn_id))[
                "managed_dispatch_epoch"] is None

        await runtime.close()
        migrate(database_url, "upgrade", "head")
        upgraded = True
        assert await h["database"].ready()
        async with h["database"].session_factory() as session:
            account = await session.get(TelegramProviderAccount, (OWNER, INTEGRATION))
            assert account is not None and account.state == "CONFLICT"
            marked = await session.get(AgentTurn, marked_turn_id)
            unmarked = await session.get(AgentTurn, unmarked_turn_id)
            assert marked is not None and marked.managed_dispatch_epoch == 1
            assert unmarked is not None and unmarked.managed_dispatch_epoch is None

        full_token = secrets.token_urlsafe(32)
        full = AuthPrincipal(
            principal_id="r4-generic-full",
            role="FULL",
            token=full_token,
            grants=h["settings"].auth_principals[0].grants,
        )
        h["settings"] = h["settings"].model_copy(update={
            "auth_principals": (*h["settings"].auth_principals, full),
        })
        h["app"].state.settings = h["settings"]
        port = DeterministicManagedPort()
        h["app"].state.managed_port = port
        full_headers = {"X-Service-Token": full_token}
        claimed = await h["client"].post(
            f"/internal/v1/turns/{unmarked_turn_id}/claim",
            json={"worker_ref": "generic-full-r4", "lease_seconds": 30},
            headers=full_headers,
        )
        assert claimed.status_code == 200, claimed.text
        claim = claimed.json()
        denied = await h["client"].post(
            f"/internal/v1/turns/{unmarked_turn_id}/execute",
            json={"claim_epoch": claim["turn"]["claim_epoch"],
                  "claim_token": claim["claim_token"]},
            headers=full_headers,
        )
        assert denied.status_code == 422, denied.text
        assert denied.json()["detail"]["code"] == "VALIDATION_ERROR"
        assert port.calls == 0
        async with sql.connect() as connection:
            blocked = (await connection.execute(text(
                "SELECT status,managed_dispatch_epoch,managed_dispatch_started_at "
                "FROM agent_turns WHERE id=:id"
            ), {"id": unmarked_turn_id})).mappings().one()
            assert blocked["status"] == "CLAIMED"
            assert blocked["managed_dispatch_epoch"] is None
            assert blocked["managed_dispatch_started_at"] is None

        async with sql.begin() as connection:
            await connection.execute(text(
                "UPDATE agent_turns SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE id=:id"
            ), {"id": marked_turn_id})
        from services.telegram_worker import TelegramWorker

        worker = TelegramWorker(h["database"], h["settings"], port)
        assert await worker.run_once()
        assert port.calls == 0
        async with h["database"].session_factory() as session:
            recovered = await session.get(AgentTurn, marked_turn_id)
            assert recovered is not None
            assert recovered.status == "FAILED"
            assert recovered.failure_code == "EXECUTION_UNKNOWN"
            assert recovered.managed_dispatch_epoch == 1
            assert recovered.operation_refs == []
        async with h["database"].session_factory() as session:
            marked_admission = await session.scalar(select(TelegramAdmission).join(
                TelegramImmediateTurn,
                TelegramImmediateTurn.message_id == TelegramAdmission.canonical_message_id,
            ).where(TelegramImmediateTurn.turn_id == marked_turn_id))
            assert marked_admission is not None
            marked_identity = {
                "owner_id": marked_admission.owner_id,
                "integration_id": str(marked_admission.integration_id),
                "agent_id": str(marked_admission.agent_id),
                "provider_thread_id": marked_admission.provider_thread_id,
                "provider_event_id": marked_admission.provider_event_id,
                "bot_id": int(marked_admission.bot_id),
            }
        marked_result = await h["client"].post(
            "/internal/v1/telegram/results", json=marked_identity,
            headers=h["gateway_headers"],
        )
        assert marked_result.status_code == 200
        assert marked_result.json()["turn_id"] == str(marked_turn_id)
        assert marked_result.json()["progress_disposition"] == "TERMINAL"
    finally:
        await runtime.close()
        if not upgraded:
            migrate(database_url, "upgrade", "head")
            await h["database"].ready()


@pytest.mark.asyncio
async def test_generic_telegram_execution_rechecks_complete_members_at_both_boundaries(
    sql, telegram_harness, test_database_clock,
) -> None:
    h = telegram_harness
    settings = h["settings"].model_copy(update={
        "turn_settling_policies": (SettlingPolicy(
            version="r4-member-range", channel=Channel.TELEGRAM,
            quiet_window_ms=10, max_wait_ms=100,
        ),),
    })
    h["settings"] = settings
    h["app"].state.settings = settings
    values = [
        _payload(message_id=901 + index, update_id=1_001 + index,
                 chat_id=39_901, bot_id=999)
        for index in range(3)
    ]
    admissions = []
    for value in values:
        response = await h["client"].post(
            "/internal/v1/telegram/admissions", json=value,
            headers=h["gateway_headers"],
        )
        assert response.status_code == 200, response.text
        admissions.append(response.json())

    async with sql.connect() as connection:
        deadline = await connection.scalar(text(
            "SELECT max(b.ready_at) FROM settling_bursts b "
            "JOIN telegram_admissions a ON a.conversation_id=b.conversation_id "
            "WHERE a.id IN (:first,:second,:third)"
        ), {"first": admissions[0]["admission_id"],
            "second": admissions[1]["admission_id"],
            "third": admissions[2]["admission_id"]})
    assert deadline is not None
    test_database_clock.advance_to(deadline)
    principal = next(entry.principal() for entry in h["settings"].auth_principals
                     if entry.principal_id == "telegram-worker")
    from services.settling import select_settled_turn

    turn_id = await select_settled_turn(
        h["database"], principal, settings, (Channel.TELEGRAM,)
    )
    assert turn_id is not None
    claim = await turns.claim_turn(
        h["database"], principal, turn_id,
        TurnClaim(worker_ref="r4-membership-check", lease_seconds=30),
    )
    proof = TurnExecute(
        claim_epoch=claim.turn.claim_epoch,
        claim_token=SecretStr(claim.claim_token),
    )
    turn_read = claim.turn
    conversation_id = turn_read.conversation_id
    async with h["database"].session_factory() as session:
        original_members = (await session.execute(select(
            TelegramAdmission.canonical_message_id, Message.sequence,
        ).join(Message, Message.id == TelegramAdmission.canonical_message_id)
         .where(TelegramAdmission.id.in_([
             UUID(item["admission_id"]) for item in admissions
         ])).order_by(Message.sequence))).all()
    assert len(original_members) == 3
    missing_message_id = original_members[1].canonical_message_id

    # The first-range snapshot has endpoints 1 and 3 but deliberately omits
    # canonical sequence 2; endpoint-only validation would accept this shape.
    async with h["database"].session_factory() as session, session.begin():
        missing_map = await session.get(TurnInputMessage, missing_message_id)
        missing_link = await session.get(TelegramImmediateTurn, missing_message_id)
        assert missing_map is not None and missing_link is not None
        await session.delete(missing_map)
        await session.delete(missing_link)

    with pytest.raises(DomainError) as missing_middle:
        await managed_execution.take_snapshot(
            h["database"], principal, turn_id, proof, h["settings"]
        )
    assert missing_middle.value.code.value == "VALIDATION_ERROR"
    async with h["database"].session_factory() as session:
        persisted = await session.get(AgentTurn, turn_id)
        assert persisted is not None and persisted.managed_dispatch_epoch is None

    # Restore the valid complete snapshot, then remove the middle member
    # after take_snapshot to prove mark_dispatch repeats the full membership check.
    async with h["database"].session_factory() as session, session.begin():
        session.add(TelegramImmediateTurn(
            message_id=missing_message_id, conversation_id=conversation_id, turn_id=turn_id,
        ))
        session.add(TurnInputMessage(
            message_id=missing_message_id, conversation_id=conversation_id,
            turn_id=turn_id, sequence=original_members[1].sequence, ordinal=1,
        ))
    snapshot = await managed_execution.take_snapshot(
        h["database"], principal, turn_id, proof, h["settings"]
    )
    prepared = managed_execution.prepare_snapshot(snapshot)
    async with h["database"].session_factory() as session, session.begin():
        missing_map = await session.get(TurnInputMessage, missing_message_id)
        missing_link = await session.get(TelegramImmediateTurn, missing_message_id)
        assert missing_map is not None and missing_link is not None
        await session.delete(missing_map)
        await session.delete(missing_link)
    with pytest.raises(DomainError) as changed_members:
        await managed_execution.mark_dispatch(
            h["database"], principal, turn_id, proof, prepared
        )
    assert changed_members.value.code.value == "VALIDATION_ERROR"
    async with h["database"].session_factory() as session:
        persisted = await session.get(AgentTurn, turn_id)
        assert persisted is not None and persisted.managed_dispatch_epoch is None
    async with h["database"].session_factory() as session, session.begin():
        session.add(TelegramImmediateTurn(
            message_id=missing_message_id, conversation_id=conversation_id, turn_id=turn_id,
        ))
        session.add(TurnInputMessage(
            message_id=missing_message_id, conversation_id=conversation_id, turn_id=turn_id,
            sequence=original_members[1].sequence, ordinal=1,
        ))


@pytest.mark.parametrize("bad_member", ["agent", "bot", "thread"])
async def test_generic_telegram_snapshot_rejects_wrong_member_identity(
    sql, telegram_harness, bad_member: str,
) -> None:
    h = telegram_harness
    value = _payload(message_id=1_101, update_id=1_201, chat_id=40_101, bot_id=999)
    accepted = await h["client"].post(
        "/internal/v1/telegram/admissions", json=value,
        headers=h["gateway_headers"],
    )
    assert accepted.status_code == 200, accepted.text
    principal = next(entry.principal() for entry in h["settings"].auth_principals
                     if entry.principal_id == "telegram-worker")
    turn_id = await select_immediate(h["database"], principal, h["settings"])
    assert turn_id is not None
    claim = await turns.claim_turn(
        h["database"], principal, turn_id,
        TurnClaim(worker_ref="r4-identity-check", lease_seconds=30),
    )
    proof = TurnExecute(
        claim_epoch=claim.turn.claim_epoch,
        claim_token=SecretStr(claim.claim_token),
    )
    admission_id = UUID(accepted.json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        admission = await session.get(TelegramAdmission, admission_id)
        assert admission is not None
        if bad_member == "agent":
            admission.agent_id = uuid4()
        elif bad_member == "bot":
            admission.bot_id = "1000"
            admission.provider_event_id = (
                f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
            )
        else:
            admission.provider_thread_id = "40102"
            admission.chat_id = "40102"
            admission.sender_id = "40102"
            admission.provider_event_id = (
                f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
            )

    port = DeterministicManagedPort()
    with pytest.raises(DomainError) as rejected:
        await managed_execution.execute_claimed_turn(
            h["database"], principal, turn_id, proof, h["settings"], port,
        )
    assert rejected.value.code.value == "VALIDATION_ERROR"
    assert port.calls == 0
    async with h["database"].session_factory() as session:
        persisted = await session.get(AgentTurn, turn_id)
        assert persisted is not None and persisted.managed_dispatch_epoch is None
    async with h["database"].session_factory() as session, session.begin():
        admission = await session.get(TelegramAdmission, admission_id)
        assert admission is not None
        admission.agent_id = UUID(str(value["agent_id"]))
        admission.bot_id = str(value["bot_id"])
        admission.provider_thread_id = str(value["provider_thread_id"])
        admission.chat_id = str(value["chat_id"])
        admission.sender_id = str(value["sender_id"])
        admission.provider_event_id = str(value["provider_event_id"])
