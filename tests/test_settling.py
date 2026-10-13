"""Real PostgreSQL tests for durable settling, concurrency and delivery fencing."""

from __future__ import annotations

import asyncio
import multiprocessing
import queue
import secrets
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import select, text

from core.config import Settings, SettlingPolicy
from models import AgentTurn, Conversation, DeliveryAttempt, TelegramAdmission, TurnInputMessage
from schemas.contracts import Channel
from services.settling import select_settled_turn
from services.telegram import expected_response_attempt_id
from services.telegram_worker import TelegramWorker
from tests.test_telegram import DeterministicManagedPort, inbound
from tests.test_telegram import admit as telegram_admit


@pytest_asyncio.fixture
async def telegram_harness(database_url):
    from db.database import Database
    from main import create_app

    integration = UUID("00000000-0000-4000-8000-000000000041")
    agent = UUID("00000000-0000-4000-8000-000000000042")
    owner = "telegram-owner"
    namespace = "main-chota.telegram.v1"
    gateway_token, worker_token, ai_token = [secrets.token_urlsafe(32) for _ in range(3)]
    grants = [{"owner_id": owner, "integration_id": str(integration),
               "channel": "TELEGRAM", "provider_namespace": namespace}]
    settings = Settings(
        database_url=database_url,
        auth_principals=[
            {"principal_id": "telegram-gateway", "role": "TELEGRAM",
             "token": gateway_token, "grants": grants},
            {"principal_id": "telegram-worker", "role": "TELEGRAM_PROCESSING",
             "token": worker_token, "grants": grants},
        ],
        managed_ai_bindings=[{"owner_id": owner, "integration_id": integration,
                              "channel": "TELEGRAM", "agent_id": agent,
                              "service_token": ai_token}],
        managed_ai_base_url="http://127.0.0.1:55570",
        telegram_cohort_integration_ids=(integration,),
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
            yield {"app": app, "client": client, "database": database,
                   "settings": settings,
                   "gateway_headers": {"X-Service-Token": gateway_token},
                   "worker_token": worker_token}


def _policy_settings(settings: Settings, *, quiet_ms: int, max_wait_ms: int) -> Settings:
    policy = SettlingPolicy(version="test-telegram-v1", channel=Channel.TELEGRAM,
                            quiet_window_ms=quiet_ms, max_wait_ms=max_wait_ms)
    value = settings.model_copy(update={"turn_settling_policies": (policy,)})
    return value


def _install_settings(harness, settings: Settings) -> None:
    harness["settings"] = settings
    harness["app"].state.settings = settings


async def _age_messages(sql, ages_ms: dict[int, int]) -> None:
    async with sql.begin() as connection:
        for sequence, age in ages_ms.items():
            await connection.execute(text(
                "UPDATE messages SET created_at=clock_timestamp() - "
                "(:age * interval '1 millisecond') WHERE sequence=:sequence"
            ), {"age": age, "sequence": sequence})


@pytest.mark.asyncio
async def test_quiet_reset_selects_original_messages_once_as_one_turn(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=250, max_wait_ms=1200)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    second = await telegram_admit(h["client"], h["gateway_headers"], message_id=8, update_id=56)
    assert first.status_code == second.status_code == 200
    first_id = UUID(first.json()["admission_id"])
    second_id = UUID(second.json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        first_message_id = await session.scalar(
            select(TelegramAdmission.canonical_message_id).where(TelegramAdmission.id == first_id)
        )
        second_message_id = await session.scalar(
            select(TelegramAdmission.canonical_message_id).where(TelegramAdmission.id == second_id)
        )
        conversation_id = await session.scalar(
            select(TelegramAdmission.conversation_id).where(TelegramAdmission.id == first_id)
        )
        conversation = await session.get(Conversation, conversation_id)
        assert conversation is not None
        expected_input_revision = conversation.input_revision
        expected_authority_revision = conversation.authority_revision

    replay = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert replay.status_code == 200 and replay.json()["replayed"] is True
    invalid_direct = await h["client"].post(
        f"/internal/v1/conversations/{conversation_id}/turns",
        json={"selection_key": str(uuid4()), "input_first_sequence": 1,
              "input_last_sequence": 2,
              "expected_input_revision": expected_input_revision,
              "expected_authority_revision": expected_authority_revision},
        headers=h["gateway_headers"],
    )
    assert invalid_direct.status_code == 409
    assert invalid_direct.json()["detail"]["code"] == "SETTLING_REQUIRED"

    # The second accepted Message extends quiet to 250ms from its own DB time.
    await _age_messages(sql, {1: 300, 2: 80})
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once() is False
    await asyncio.sleep(0.20)
    assert await worker.run_once() is True
    assert port.calls == 1
    request = port.prepared[0].request
    assert request.input_first_sequence == 1
    assert request.input_last_sequence == request.context_last_sequence == 2
    assert request.selected_input_message_ids == (first_message_id, second_message_id)
    assert request.input_revision == 2
    async with h["database"].session_factory() as session, session.begin():
        assignments = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
        turns = (await session.scalars(select(AgentTurn))).all()
    assert [assignment.message_id for assignment in assignments] == [
        first_message_id, second_message_id,
    ]
    assert len(turns) == 1
    assert await worker.run_once() is False


@pytest.mark.asyncio
async def test_max_wait_bounds_continuous_burst(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=500, max_wait_ms=1000)
    _install_settings(h, settings)
    admissions = []
    for message_id in range(7, 11):
        response = await telegram_admit(h["client"], h["gateway_headers"],
                                        message_id=message_id, update_id=message_id + 48)
        assert response.status_code == 200
        admissions.append(UUID(response.json()["admission_id"]))
    async with sql.begin() as connection:
        await connection.execute(text("""
            WITH base AS (SELECT clock_timestamp() AS now),
                 ages(sequence, age_ms) AS (VALUES (1,2000),(2,1600),(3,1200),(4,1000))
            UPDATE messages m SET created_at=base.now-(ages.age_ms*interval '1 millisecond')
            FROM base,ages WHERE m.sequence=ages.sequence
        """))
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once() is True
    assert len(port.prepared[0].request.selected_input_message_ids) == 3
    assert port.prepared[0].request.input_last_sequence == 3
    # The fourth Message is on/after the max-wait cutoff and stays for a later Turn.
    async with h["database"].session_factory() as session, session.begin():
        mapped = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
    assert [row.sequence for row in mapped] == [1, 2, 3]
    assert len(admissions) == 4


@pytest.mark.asyncio
async def test_settling_survives_worker_process_restart(telegram_harness, sql, database_url):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=50, max_wait_ms=200)
    _install_settings(h, settings)
    accepted = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert accepted.status_code == 200
    await _age_messages(sql, {1: 500})
    process_settings = {
        "database_url": database_url,
        "auth_principals": [{
            "principal_id": item.principal_id,
            "role": item.role,
            "token": item.token.get_secret_value(),
            "grants": [grant.model_dump(mode="json") for grant in item.grants],
        } for item in settings.auth_principals],
        "managed_ai_bindings": [{
            "owner_id": item.owner_id,
            "integration_id": str(item.integration_id),
            "channel": item.channel.value,
            "agent_id": str(item.agent_id),
            "service_token": item.service_token.get_secret_value(),
        } for item in settings.managed_ai_bindings],
        "managed_ai_base_url": settings.managed_ai_base_url,
        "telegram_cohort_integration_ids": [str(value)
                                            for value in settings.telegram_cohort_integration_ids],
        "telegram_worker_enabled": True,
        "telegram_worker_principal_id": settings.telegram_worker_principal_id,
        "turn_settling_policies": [{
            "version": item.version, "channel": item.channel.value,
            "integration_id": str(item.integration_id) if item.integration_id else None,
            "quiet_window_ms": item.quiet_window_ms, "max_wait_ms": item.max_wait_ms,
        } for item in settings.turn_settling_policies],
    }
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    process = context.Process(target=_select_in_fresh_process,
                              args=(database_url, process_settings, results))
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("fresh worker process did not finish settling selection")
    assert process.exitcode == 0
    try:
        selected_id = results.get(timeout=2)
    except queue.Empty:
        pytest.fail("fresh worker process returned no selection result")
    assert not isinstance(selected_id, dict), selected_id
    assert selected_id is not None
    async with h["database"].session_factory() as session, session.begin():
        turn = await session.get(AgentTurn, UUID(selected_id))
        count = await session.scalar(select(TurnInputMessage.message_id).where(
            TurnInputMessage.turn_id == UUID(selected_id)))
    assert turn is not None and turn.input_first_sequence == turn.input_last_sequence == 1
    assert count is not None


def _select_in_fresh_process(database_url: str, data: dict[str, Any], output) -> None:
    async def run() -> str | None:
        from db.database import Database

        settings = Settings(**data, _env_file=None)
        principal = next(item for item in settings.auth_principals
                         if item.principal_id == settings.telegram_worker_principal_id).principal()
        database = Database(database_url)
        try:
            result = await select_settled_turn(
                database, principal, settings, (Channel.TELEGRAM,)
            )
            return str(result) if result else None
        finally:
            await database.dispose()

    try:
        output.put(asyncio.run(run()))
    except BaseException as error:
        output.put({"error": repr(error)})
        raise


@pytest.mark.asyncio
async def test_two_workers_racing_select_one_turn(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    accepted = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert accepted.status_code == 200
    await _age_messages(sql, {1: 500})
    principal = next(item for item in settings.auth_principals
                     if item.principal_id == settings.telegram_worker_principal_id).principal()
    left, right = await asyncio.gather(
        select_settled_turn(h["database"], principal, settings, (Channel.TELEGRAM,)),
        select_settled_turn(h["database"], principal, settings, (Channel.TELEGRAM,)),
    )
    assert sum(value is not None for value in (left, right)) == 1
    async with h["database"].session_factory() as session, session.begin():
        assert await session.scalar(select(AgentTurn.id)) is not None


@pytest.mark.asyncio
async def test_large_unsettled_prefix_does_not_hide_later_due_conversation(
    telegram_harness, sql
):
    h = telegram_harness
    headers = h["gateway_headers"]

    async def submit(message_id: int, thread_id: str):
        value = inbound(message_id=message_id, update_id=message_id + 100)
        value["provider_thread_id"] = thread_id
        return await h["client"].post("/internal/v1/telegram/admissions", json=value,
                                     headers=headers)

    # Create a long, ineligible prefix before enabling policy for new admissions.
    for batch_start in range(0, 300, 25):
        responses = await asyncio.gather(*(
            submit(message_id=message_id, thread_id="backlog")
            for message_id in range(batch_start + 7, batch_start + 32)
        ))
        assert all(response.status_code == 200 for response in responses)
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    backlog = await submit(message_id=1000, thread_id="backlog")
    later = await submit(message_id=1001, thread_id="later")
    assert backlog.status_code == later.status_code == 200
    async with sql.begin() as connection:
        await connection.execute(text(
            "UPDATE messages SET created_at=clock_timestamp()-interval '500 milliseconds' "
            "WHERE settling_policy_version IS NOT NULL"
        ))
    principal = next(item for item in settings.auth_principals
                     if item.principal_id == settings.telegram_worker_principal_id).principal()
    selected_id = await select_settled_turn(
        h["database"], principal, settings, (Channel.TELEGRAM,)
    )
    assert selected_id is not None
    async with h["database"].session_factory() as session, session.begin():
        selected = await session.get(AgentTurn, selected_id)
        conversation = await session.get(Conversation, selected.conversation_id)
    assert selected is not None and conversation is not None
    assert conversation.provider_thread_id == "later"


@pytest.mark.asyncio
async def test_new_eligible_input_supersedes_draft_and_retains_operation_refs(
    telegram_harness, sql
):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    await _age_messages(sql, {1: 500})
    port = DeterministicManagedPort()
    port.operation_refs = [{"origin": "AI_AGENT_SERVICE", "operation_id": "op-unknown",
                            "outcome": "UNKNOWN"}]
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once()
    second = await telegram_admit(h["client"], h["gateway_headers"],
                                  message_id=8, update_id=56, text_value="new request")
    assert second.status_code == 200
    first_identity = {key: inbound()[key] for key in (
        "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id")}
    response_id = expected_response_attempt_id(UUID(first.json()["admission_id"]))
    begun = await h["client"].post("/internal/v1/telegram/responses/begin",
                                   json={**first_identity, "response_attempt_id": str(response_id)},
                                   headers=h["gateway_headers"])
    assert begun.status_code == 200 and begun.json()["send"] is False
    async with h["database"].session_factory() as session, session.begin():
        turn = await session.get(AgentTurn, port.prepared[0].request.turn_id)
        delivery = await session.scalar(select(DeliveryAttempt))
    assert turn is not None and turn.operation_refs == port.operation_refs
    assert delivery is not None and delivery.status == "SUPERSEDED"
    replay = await h["client"].post("/internal/v1/telegram/responses/begin",
                                    json={**first_identity,
                                          "response_attempt_id": str(response_id)},
                                    headers=h["gateway_headers"])
    assert replay.status_code == 200 and replay.json()["send"] is False


@pytest.mark.asyncio
async def test_inbound_and_delivery_fence_have_one_postgres_ordering(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    await _age_messages(sql, {1: 500})
    worker = TelegramWorker(h["database"], settings, DeterministicManagedPort())
    assert await worker.run_once()
    first_identity = {key: inbound()[key] for key in (
        "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id")}
    response_id = expected_response_attempt_id(UUID(first.json()["admission_id"]))
    begin_body = {**first_identity, "response_attempt_id": str(response_id)}
    begin_request = h["client"].post("/internal/v1/telegram/responses/begin", json=begin_body,
                                     headers=h["gateway_headers"])
    inbound_request = h["client"].post(
        "/internal/v1/telegram/admissions", json=inbound(message_id=8, update_id=56),
        headers=h["gateway_headers"])
    begun, accepted = await asyncio.gather(begin_request, inbound_request)
    assert begun.status_code == accepted.status_code == 200
    attempt = await _delivery_for_turn(h)
    assert attempt is not None
    status = attempt.status
    assert (begun.json()["send"] is True and status == "DISPATCH_REQUESTED") or (
        begun.json()["send"] is False and status == "SUPERSEDED"
    )


async def _delivery_for_turn(harness):
    async with harness["database"].session_factory() as session, session.begin():
        return await session.scalar(select(DeliveryAttempt))
