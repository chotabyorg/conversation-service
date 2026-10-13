"""Telegram canary boundary and real PostgreSQL identity/evidence tests."""

from __future__ import annotations

import secrets
from datetime import timedelta
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select, text

import services.telegram as telegram_service
from models import AgentTurn, Conversation, TelegramAdmission, TelegramImmediateTurn
from schemas.managed import ManagedTurnResult, PreparedManagedRequest
from services.common import fingerprint
from services.managed_client import ManagedAIPort
from services.telegram import expected_response_attempt_id

OWNER = "telegram-owner"
INTEGRATION = UUID("00000000-0000-4000-8000-000000000041")
AGENT = UUID("00000000-0000-4000-8000-000000000042")
NAMESPACE = "main-chota.telegram.v1"


class DeterministicManagedPort(ManagedAIPort):
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, prepared: PreparedManagedRequest, binding: Any,
                      request_id: str = "") -> ManagedTurnResult:
        self.calls += 1
        request = prepared.request
        return ManagedTurnResult(
            contract_version=request.contract_version,
            conversation_id=request.conversation_id,
            turn_id=request.turn_id,
            agent_id=request.agent_id,
            input_revision=request.input_revision,
            authority_revision=request.authority_revision,
            input_first_sequence=request.input_first_sequence,
            input_last_sequence=request.input_last_sequence,
            context_last_sequence=request.context_last_sequence,
            status="DRAFT_READY",
            draft_text="Managed answer **bold**",
            error_code=None,
            operation_refs=[],
        )

    async def evidence(self, request: Any, binding: Any) -> Any:
        raise AssertionError("successful deterministic execution should not request evidence")


@pytest_asyncio.fixture
async def telegram_harness(database_url: str):
    from core.config import Settings
    from db.database import Database
    from main import create_app

    gateway_token = secrets.token_urlsafe(32)
    worker_token = secrets.token_urlsafe(32)
    ai_token = secrets.token_urlsafe(32)
    grants = [{"owner_id": OWNER, "integration_id": str(INTEGRATION),
               "channel": "TELEGRAM", "provider_namespace": NAMESPACE}]
    settings = Settings(
        database_url=database_url,
        auth_principals=[
            {"principal_id": "telegram-gateway", "role": "TELEGRAM",
             "token": gateway_token, "grants": grants},
            {"principal_id": "telegram-worker", "role": "TELEGRAM_PROCESSING",
             "token": worker_token, "grants": grants},
        ],
        managed_ai_bindings=[{"owner_id": OWNER, "integration_id": INTEGRATION,
                              "channel": "TELEGRAM", "agent_id": AGENT,
                              "service_token": ai_token}],
        managed_ai_base_url="http://127.0.0.1:55570",
        telegram_cohort_integration_ids=(INTEGRATION,),
        telegram_worker_enabled=True,
        telegram_worker_principal_id="telegram-worker",
        _env_file=None,
    )
    database = Database(database_url)
    app = create_app(settings=settings, database=database)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://conversation.test"
        ) as client:
            yield {
                "app": app,
                "client": client,
                "database": database,
                "settings": settings,
                "gateway_headers": {"X-Service-Token": gateway_token},
                "worker_token": worker_token,
            }


def inbound(*, message_id: int = 7, text_value: str = "hello", update_id: int = 55,
            desired_route: str = "MANAGED", chat_id: int = 123) -> dict[str, object]:
    return {
        "owner_id": OWNER,
        "integration_id": str(INTEGRATION),
        "agent_id": str(AGENT),
        "provider_thread_id": str(chat_id),
        "provider_event_id": f"b999c{chat_id}m{message_id}",
        "update_id": update_id,
        "chat_id": chat_id,
        "message_id": message_id,
        "sender_id": chat_id,
        "bot_id": 999,
        "text": text_value,
        "desired_route": desired_route,
    }


async def admit(client: httpx.AsyncClient, headers: dict[str, str], **changes: Any):
    response = await client.post("/internal/v1/telegram/admissions",
                                 json=inbound(**changes), headers=headers)
    return response


async def _admit_distinct_private_chats(h: dict[str, Any], count: int, *, base: int):
    admission_ids = []
    for index in range(count):
        response = await admit(
            h["client"], h["gateway_headers"], message_id=1_000 + base + index,
            update_id=2_000 + base + index, chat_id=10_000 + base + index,
        )
        assert response.status_code == 200, response.text
        admission_ids.append(response.json()["admission_id"])

    async with h["database"].session_factory() as session:
        rows = (await session.execute(
            select(Conversation.id, TelegramAdmission.canonical_message_id)
            .join(TelegramAdmission,
                  TelegramAdmission.conversation_id == Conversation.id)
            .where(TelegramAdmission.id.in_(admission_ids))
            .order_by(Conversation.id)
        )).all()
    assert len(rows) == count
    return [(row.id, row.canonical_message_id) for row in rows]


async def _seed_active_telegram_leases(h: dict[str, Any], rows) -> None:
    async with h["database"].session_factory() as session, session.begin():
        now = await session.scalar(select(func.clock_timestamp()))
        assert now is not None
        for index, (conversation_id, message_id) in enumerate(rows):
            turn_id = uuid4()
            session.add(AgentTurn(
                id=turn_id,
                conversation_id=conversation_id,
                selection_key=message_id,
                creation_fingerprint=fingerprint({"fixture_turn": str(turn_id)}),
                input_first_sequence=1,
                input_last_sequence=1,
                context_last_sequence=1,
                input_revision=1,
                authority_revision=0,
                status="CLAIMED",
                claim_epoch=1,
                claim_token_digest=sha256(f"active-lease-{index}".encode()).hexdigest(),
                worker_ref="active-test-worker",
                lease_expires_at=now + timedelta(hours=1),
                started_at=now,
                operation_refs=[],
            ))
            session.add(TelegramImmediateTurn(
                message_id=message_id,
                conversation_id=conversation_id,
                turn_id=turn_id,
            ))


def _record_locked_candidates(monkeypatch):
    locked_ids = []
    original = telegram_service.authorized_conversation

    async def record(session, principal, conversation_id, *, lock=False):
        assert lock, "Telegram candidate must be revalidated under the Conversation lock"
        locked_ids.append(conversation_id)
        return await original(session, principal, conversation_id, lock=lock)

    monkeypatch.setattr(telegram_service, "authorized_conversation", record)
    return locked_ids


def _telegram_worker_principal(h: dict[str, Any]):
    return next(
        entry.principal() for entry in h["settings"].auth_principals
        if entry.principal_id == "telegram-worker"
    )


@pytest.mark.asyncio
async def test_candidate_keyset_scan_skips_130_active_leases_and_selects_later_work(
    telegram_harness, monkeypatch, sql
) -> None:
    h = telegram_harness
    rows = await _admit_distinct_private_chats(h, 131, base=0)
    blocked_rows, eligible_row = rows[:130], rows[130]
    await _seed_active_telegram_leases(h, blocked_rows)

    locked_ids = _record_locked_candidates(monkeypatch)
    principal = _telegram_worker_principal(h)
    selected = await telegram_service.select_immediate(
        h["database"], principal, h["settings"]
    )
    assert selected is not None

    async with h["database"].session_factory() as session:
        selected_turn = await session.scalar(
            select(AgentTurn).where(AgentTurn.id == selected)
        )
        assert selected_turn is not None
        assert selected_turn.conversation_id == eligible_row[0]
        assert selected_turn.selection_key == eligible_row[1]

        leased_before_repeat = (await session.execute(
            select(
                AgentTurn.id,
                AgentTurn.status,
                AgentTurn.claim_epoch,
                AgentTurn.claim_token_digest,
                AgentTurn.worker_ref,
                AgentTurn.lease_expires_at,
            ).where(AgentTurn.conversation_id.in_([row[0] for row in blocked_rows]))
             .order_by(AgentTurn.id)
        )).all()

    # The next poll starts at the first UUID again, skips the same active leases,
    # and returns the existing selected turn instead of creating a duplicate.
    selected_again = await telegram_service.select_immediate(
        h["database"], principal, h["settings"]
    )
    assert selected_again == selected
    expected_locked = [row[0] for row in rows]
    assert locked_ids == expected_locked + expected_locked

    async with h["database"].session_factory() as session:
        leased_after_repeat = (await session.execute(
            select(
                AgentTurn.id,
                AgentTurn.status,
                AgentTurn.claim_epoch,
                AgentTurn.claim_token_digest,
                AgentTurn.worker_ref,
                AgentTurn.lease_expires_at,
            ).where(AgentTurn.conversation_id.in_([row[0] for row in blocked_rows]))
             .order_by(AgentTurn.id)
        )).all()
        turn_count = await session.scalar(
            select(func.count()).select_from(AgentTurn)
            .where(AgentTurn.conversation_id == eligible_row[0])
        )
        mapping_count = await session.scalar(
            select(func.count()).select_from(TelegramImmediateTurn)
            .where(TelegramImmediateTurn.conversation_id == eligible_row[0])
        )

    assert leased_after_repeat == leased_before_repeat
    assert turn_count == 1
    assert mapping_count == 1


@pytest.mark.asyncio
async def test_candidate_keyset_scan_selects_eligible_row_in_mixed_first_page(
    telegram_harness, monkeypatch, sql
) -> None:
    h = telegram_harness
    rows = await _admit_distinct_private_chats(h, 101, base=10_000)
    skipped_rows, eligible_row = rows[:60], rows[60]
    await _seed_active_telegram_leases(h, skipped_rows)

    locked_ids = _record_locked_candidates(monkeypatch)
    selected = await telegram_service.select_immediate(
        h["database"], _telegram_worker_principal(h), h["settings"]
    )

    assert selected is not None
    assert locked_ids == [row[0] for row in rows[:61]]
    async with h["database"].session_factory() as session:
        selected_conversation = await session.scalar(
            select(AgentTurn.conversation_id).where(AgentTurn.id == selected)
        )
        assert selected_conversation == eligible_row[0]


@pytest.mark.asyncio
async def test_provider_identity_canonical_history_worker_turn_and_delivery(
    telegram_harness, sql
) -> None:
    h = telegram_harness
    first = await admit(h["client"], h["gateway_headers"])
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["route"] == "MANAGED" and not first_body["replayed"]

    exact = await admit(h["client"], h["gateway_headers"])
    assert exact.status_code == 200 and exact.json()["replayed"]
    assert exact.json()["admission_id"] == first_body["admission_id"]

    conflict = await admit(h["client"], h["gateway_headers"], text_value="changed")
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "EVENT_CONFLICT"

    second = await admit(h["client"], h["gateway_headers"], message_id=8,
                         update_id=56, text_value="hello")
    assert second.status_code == 200 and second.json()["route"] == "MANAGED"
    assert second.json()["admission_id"] != first_body["admission_id"]

    from services.telegram_worker import TelegramWorker

    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], h["settings"], port)
    assert await worker.run_once()
    assert await worker.run_once()
    assert not await worker.run_once()
    assert port.calls == 2

    identity = {key: inbound()[key] for key in
                ("owner_id", "integration_id", "agent_id", "provider_thread_id",
                 "provider_event_id")}
    retained = await h["client"].post("/internal/v1/telegram/results", json=identity,
                                       headers=h["gateway_headers"])
    assert retained.status_code == 200
    assert retained.json()["state"] == "DRAFT_READY"
    assert retained.json()["draft_text"] == "Managed answer **bold**"

    response_attempt_id = expected_response_attempt_id(UUID(first_body["admission_id"]))
    begin_body = {**identity, "response_attempt_id": str(response_attempt_id)}
    begun = await h["client"].post("/internal/v1/telegram/responses/begin",
                                  json=begin_body, headers=h["gateway_headers"])
    assert begun.status_code == 200 and begun.json()["send"] is True
    complete_body = {
        **begin_body,
        "outcome": "PROVIDER_ACCEPTED",
        "provider_message_id": "501",
    }
    completed = await h["client"].post("/internal/v1/telegram/responses/complete",
                                      json=complete_body, headers=h["gateway_headers"])
    assert completed.status_code == 200 and completed.json()["replayed"] is False
    completed_again = await h["client"].post("/internal/v1/telegram/responses/complete",
                                            json=complete_body,
                                            headers=h["gateway_headers"])
    assert completed_again.status_code == 200 and completed_again.json()["replayed"] is True
    replayed_begin = await h["client"].post("/internal/v1/telegram/responses/begin",
                                           json=begin_body,
                                           headers=h["gateway_headers"])
    assert replayed_begin.status_code == 200 and replayed_begin.json()["send"] is False

    unknown_identity = {**identity, "provider_event_id": "b999c123m8"}
    unknown_attempt = expected_response_attempt_id(UUID(second.json()["admission_id"]))
    unknown_begin_body = {**unknown_identity,
                          "response_attempt_id": str(unknown_attempt)}
    unknown_begin = await h["client"].post("/internal/v1/telegram/responses/begin",
                                           json=unknown_begin_body,
                                           headers=h["gateway_headers"])
    assert unknown_begin.status_code == 200 and unknown_begin.json()["send"] is True
    unknown_complete_body = {**unknown_begin_body, "outcome": "UNKNOWN",
                             "error_code": "TIMEOUT"}
    unknown_complete = await h["client"].post("/internal/v1/telegram/responses/complete",
                                              json=unknown_complete_body,
                                              headers=h["gateway_headers"])
    assert unknown_complete.status_code == 200
    assert unknown_complete.json()["outcome"] == "UNKNOWN"
    unknown_replay = await h["client"].post("/internal/v1/telegram/responses/begin",
                                            json=unknown_begin_body,
                                            headers=h["gateway_headers"])
    assert unknown_replay.status_code == 200 and unknown_replay.json()["send"] is False

    async with sql.connect() as conn:
        counts = (await conn.execute(text(
            "SELECT (SELECT count(*) FROM telegram_admissions), "
            "(SELECT count(*) FROM inbound_events WHERE channel='TELEGRAM'), "
            "(SELECT count(*) FROM messages WHERE actor='CUSTOMER'), "
            "(SELECT count(*) FROM messages WHERE actor='AGENT'), "
            "(SELECT count(*) FROM agent_turns), "
            "(SELECT count(*) FROM delivery_attempts)"
        ))).one()
        assert tuple(counts) == (2, 2, 2, 1, 2, 2)
        deliveries = (await conn.execute(text(
            "SELECT status,provider_message_id FROM delivery_attempts ORDER BY created_at"
        ))).all()
        assert deliveries[0] == ("PROVIDER_ACCEPTED", "501")
        assert deliveries[1][0] == "UNKNOWN" and deliveries[1][1] is None


@pytest.mark.asyncio
async def test_concurrent_replay_cohort_rollback_and_exact_binding(telegram_harness, sql) -> None:
    h = telegram_harness
    payload = inbound()
    replies = await __import__("asyncio").gather(*(
        h["client"].post("/internal/v1/telegram/admissions", json=payload,
                         headers=h["gateway_headers"])
        for _ in range(6)
    ))
    assert all(response.status_code == 200 for response in replies)
    assert len({response.json()["admission_id"] for response in replies}) == 1
    assert sum(not response.json()["replayed"] for response in replies) == 1

    wrong_agent = {**inbound(message_id=8),
                   "agent_id": "00000000-0000-4000-8000-000000000099"}
    rejected_agent = await h["client"].post("/internal/v1/telegram/admissions",
                                           json=wrong_agent,
                                           headers=h["gateway_headers"])
    assert rejected_agent.status_code == 404
    wrong_integration = {**inbound(message_id=9),
                         "integration_id": "00000000-0000-4000-8000-000000000099"}
    rejected_integration = await h["client"].post("/internal/v1/telegram/admissions",
                                                  json=wrong_integration,
                                                  headers=h["gateway_headers"])
    assert rejected_integration.status_code == 404

    # Rollback disables only new managed admissions. Retained route wins on replay.
    h["app"].state.settings = h["settings"].model_copy(
        update={"telegram_cohort_integration_ids": ()})
    rollback_replay = await admit(h["client"], h["gateway_headers"],
                                  desired_route="LEGACY")
    assert rollback_replay.status_code == 200
    assert rollback_replay.json()["route"] == "MANAGED"
    new_after_rollback = await admit(h["client"], h["gateway_headers"],
                                     message_id=8, update_id=56)
    assert new_after_rollback.status_code == 200
    assert new_after_rollback.json()["route"] == "LEGACY"

    async with sql.connect() as conn:
        counts = (await conn.execute(text(
            "SELECT (SELECT count(*) FROM telegram_admissions), "
            "(SELECT count(*) FROM messages WHERE actor='CUSTOMER')"
        ))).one()
        assert tuple(counts) == (2, 1)
