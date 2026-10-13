"""Telegram canary boundary and real PostgreSQL identity/evidence tests."""

from __future__ import annotations

import secrets
from typing import Any
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import text

from schemas.managed import ManagedTurnResult, PreparedManagedRequest
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
            desired_route: str = "MANAGED") -> dict[str, object]:
    return {
        "owner_id": OWNER,
        "integration_id": str(INTEGRATION),
        "agent_id": str(AGENT),
        "provider_thread_id": "123",
        "provider_event_id": f"b999c123m{message_id}",
        "update_id": update_id,
        "chat_id": 123,
        "message_id": message_id,
        "sender_id": 123,
        "bot_id": 999,
        "text": text_value,
        "desired_route": desired_route,
    }


async def admit(client: httpx.AsyncClient, headers: dict[str, str], **changes: Any):
    response = await client.post("/internal/v1/telegram/admissions",
                                 json=inbound(**changes), headers=headers)
    return response


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
