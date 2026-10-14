"""Real PostgreSQL tests for durable settling, concurrency and delivery fencing."""

from __future__ import annotations

import asyncio
import multiprocessing
import queue
import secrets
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select, text, update

from core.config import Settings, SettlingPolicy
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    Message,
    SettlingBurst,
    TelegramAdmission,
    TurnInputMessage,
)
from schemas.contracts import Channel
from schemas.managed import MAX_PROJECTED_TEXT_BYTES
from services.common import (
    advance_test_database_clock,
    current_test_database_clock,
    reset_test_database_clock,
    set_test_database_clock,
)
from services.settling import CANDIDATE_PAGE_SIZE, select_settled_turn
from services.telegram import expected_response_attempt_id
from services.telegram_worker import TelegramWorker
from tests.test_telegram import DeterministicManagedPort, inbound
from tests.test_telegram import admit as telegram_admit


class PausingManagedPort(DeterministicManagedPort):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, prepared, binding, request_id=""):
        self.started.set()
        await self.release.wait()
        return await super().execute(prepared, binding, request_id)


@pytest_asyncio.fixture
async def telegram_harness(database_url, test_database_clock):
    from db.database import Database
    from main import create_app

    integration = UUID("00000000-0000-4000-8000-000000000041")
    agent = UUID("00000000-0000-4000-8000-000000000042")
    owner = "telegram-owner"
    namespace = "main-chota.telegram.v1"
    gateway_token, worker_token, ai_token, full_token = [
        secrets.token_urlsafe(32) for _ in range(4)
    ]
    grants = [{"owner_id": owner, "integration_id": str(integration),
               "channel": "TELEGRAM", "provider_namespace": namespace}]
    settings = Settings(
        database_url=database_url,
        auth_principals=[
            {"principal_id": "telegram-gateway", "role": "TELEGRAM",
             "token": gateway_token, "grants": grants},
            {"principal_id": "telegram-worker", "role": "TELEGRAM_PROCESSING",
             "token": worker_token, "grants": grants},
            {"principal_id": "settling-test-full", "role": "FULL",
             "token": full_token, "grants": grants},
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
                   "test_clock": test_database_clock,
                   "gateway_headers": {"X-Service-Token": gateway_token},
                   "worker_token": worker_token, "full_token": full_token}


def _policy_settings(settings: Settings, *, quiet_ms: int, max_wait_ms: int) -> Settings:
    policy = SettlingPolicy(version="test-telegram-v1", channel=Channel.TELEGRAM,
                            quiet_window_ms=quiet_ms, max_wait_ms=max_wait_ms)
    value = settings.model_copy(update={"turn_settling_policies": (policy,)})
    return value


def _install_settings(harness, settings: Settings) -> None:
    harness["settings"] = settings
    harness["app"].state.settings = settings


async def _advance_to_due(
    sql,
    *,
    sequences: tuple[int, ...] | list[int] | None = None,
    conversation_ids: tuple[UUID, ...] | list[UUID] | None = None,
    offset_ms: int = 0,
) -> datetime:
    """Move the deterministic service clock to a durable burst deadline."""
    statement = select(func.max(SettlingBurst.ready_at)).join(
        Message, Message.settling_burst_id == SettlingBurst.id
    ).where(SettlingBurst.closed_at.is_(None))
    if sequences is not None:
        statement = statement.where(Message.sequence.in_(sequences))
    if conversation_ids is not None:
        statement = statement.where(Message.conversation_id.in_(conversation_ids))
    async with sql.connect() as connection:
        ready_at = await connection.scalar(statement)
    if not isinstance(ready_at, datetime):
        raise AssertionError("No open durable settling burst matched the test filter")
    current = current_test_database_clock()
    if current is None:
        raise AssertionError("The deterministic test database clock is not installed")
    target = max(current, ready_at + timedelta(milliseconds=offset_ms))
    advance_test_database_clock(target)
    return target


@pytest.mark.asyncio
async def test_quiet_reset_selects_original_messages_once_as_one_turn(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=1000, max_wait_ms=5000)
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
        headers={"X-Service-Token": h["full_token"]},
    )
    assert invalid_direct.status_code == 409
    assert invalid_direct.json()["detail"]["code"] == "SETTLING_REQUIRED"

    # The second accepted Message extends the durable burst's quiet deadline.
    await _advance_to_due(sql, sequences=[1, 2], offset_ms=-1)
    # A config update affects future admissions only; both accepted Messages keep
    # their original 1000ms/5000ms policy snapshot.
    changed_settings = _policy_settings(h["settings"], quiet_ms=10_000,
                                        max_wait_ms=20_000)
    _install_settings(h, changed_settings)
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], changed_settings, port)
    assert await worker.run_once() is False
    await _advance_to_due(sql, sequences=[1, 2])
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
async def test_telegram_delivery_work_coalesces_burst_and_fences_restarts(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=50, max_wait_ms=500)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    second = await telegram_admit(h["client"], h["gateway_headers"], message_id=8, update_id=56)
    assert first.status_code == second.status_code == 200
    second_admission = second.json()
    await _advance_to_due(sql, sequences=[1, 2])
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once()
    turn_id = port.prepared[0].request.turn_id

    async def work():
        binding = settings.managed_ai_bindings[0]
        response = await h["client"].post(
            "/internal/v1/telegram/responses/work",
            json={"owner_id": binding.owner_id,
                  "integration_id": str(binding.integration_id),
                  "agent_id": str(binding.agent_id), "bot_id": 999},
            headers=h["gateway_headers"],
        )
        assert response.status_code == 200, response.text
        return response.json()["items"]

    items = await work()
    assert len(items) == 1
    item = items[0]
    assert item["turn_id"] == str(turn_id)
    assert item["draft_text"] == "Managed answer **bold**"
    identity = {key: item[key] for key in (
        "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id",
    )}
    begin_body = {**identity, "response_attempt_id": item["response_attempt_id"]}
    begun = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=begin_body,
        headers=h["gateway_headers"],
    )
    assert begun.status_code == 200 and begun.json()["send"] is True

    second_identity = inbound(message_id=8, update_id=56)
    second_begin_body = {
        **{key: second_identity[key] for key in (
            "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id",
        )},
        "response_attempt_id": str(expected_response_attempt_id(
            UUID(second_admission["admission_id"])
        )),
    }
    suppressed = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=second_begin_body,
        headers=h["gateway_headers"],
    )
    assert suppressed.status_code == 404
    malicious = await h["client"].post(
        "/internal/v1/telegram/responses/complete",
        json={**second_begin_body, "outcome": "PROVIDER_ACCEPTED",
              "provider_message_id": "forged-second-send"},
        headers=h["gateway_headers"],
    )
    assert malicious.status_code == 404

    # A fresh Gateway process recovers the pending durable fence. Replaying the
    # same attempt records uncertainty and withholds another provider write.
    recovered = await h["client"].post(
        "/internal/v1/telegram/responses/begin", json=begin_body,
        headers=h["gateway_headers"],
    )
    assert recovered.status_code == 200
    assert recovered.json()["send"] is False and recovered.json()["replayed"] is True
    late = await h["client"].post(
        "/internal/v1/telegram/responses/complete",
        json={**begin_body, "outcome": "PROVIDER_ACCEPTED",
              "provider_message_id": "accepted-late"},
        headers=h["gateway_headers"],
    )
    assert late.status_code == 200
    assert await work() == []
    async with sql.connect() as connection:
        assert await connection.scalar(text(
            "SELECT count(*) FROM messages WHERE actor='AGENT'")) == 1
        assert await connection.scalar(text(
            "SELECT count(*) FROM telegram_response_attempts WHERE send_authorized")) == 1
        assert await connection.scalar(text(
            "SELECT count(*) FROM telegram_response_attempts WHERE NOT send_authorized")) == 0


@pytest.mark.asyncio
async def test_settled_inputs_outside_window_form_later_turns(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=50, max_wait_ms=2000)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    first_id = UUID(first.json()["admission_id"])
    await _advance_to_due(sql, sequences=[1])
    second = await telegram_admit(h["client"], h["gateway_headers"], message_id=8, update_id=56)
    assert second.status_code == 200
    second_id = UUID(second.json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        first_message_id = await session.scalar(select(
            TelegramAdmission.canonical_message_id).where(TelegramAdmission.id == first_id))
        second_message_id = await session.scalar(select(
            TelegramAdmission.canonical_message_id).where(TelegramAdmission.id == second_id))
    assert first_message_id is not None and second_message_id is not None
    message_rows = (first_message_id, second_message_id)
    await _advance_to_due(sql, sequences=[2])
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once()
    assert await worker.run_once()
    assert [item.request.selected_input_message_ids for item in port.prepared] == [
        (message_rows[0],), (message_rows[1],),
    ]
    assert all(item.request.input_first_sequence == item.request.input_last_sequence
               for item in port.prepared)
    async with h["database"].session_factory() as session, session.begin():
        turns = (await session.scalars(select(AgentTurn).order_by(
            AgentTurn.input_first_sequence))).all()
        assignments = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
    assert [turn.input_first_sequence for turn in turns] == [1, 2]
    assert [assignment.message_id for assignment in assignments] == list(message_rows)


@pytest.mark.asyncio
async def test_byte_bounded_burst_splits_maximal_prefix_and_later_work_progresses(
    telegram_harness, sql
):
    h = telegram_harness
    # Admissions are real HTTP transactions; allow the full 33-message setup to
    # finish inside max_wait so this test isolates byte splitting.
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=10_000)
    _install_settings(h, settings)
    large = [await telegram_admit(
        h["client"], h["gateway_headers"], message_id=100 + index,
        update_id=200 + index, text_value="x" * 16_384,
    ) for index in range(33)]
    assert all(response.status_code == 200 for response in large)
    large_admissions = [UUID(response.json()["admission_id"]) for response in large]
    async with h["database"].session_factory() as session, session.begin():
        large_ids = list((await session.scalars(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(large_admissions)
            ).order_by(TelegramAdmission.update_id))).all())
        conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(
                TelegramAdmission.id == large_admissions[0]))
    assert conversation_id is not None
    # Give the distinct conversation a later immutable deadline so the 33-item
    # burst remains the first candidate while still being settled.
    await _advance_to_due(sql, conversation_ids=[conversation_id])
    later = await telegram_admit(
        h["client"], h["gateway_headers"], message_id=500, update_id=600,
        chat_id=124, text_value="later conversation",
    )
    assert later.status_code == 200
    async with h["database"].session_factory() as session, session.begin():
        later_id = await session.scalar(select(TelegramAdmission.canonical_message_id).where(
            TelegramAdmission.id == UUID(later.json()["admission_id"])))
    assert len(large_ids) == 33 and later_id is not None
    await _advance_to_due(sql)

    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once() is True
    first = port.prepared[0].request
    # The actual ManagedTurn v2 validator accepts 32 messages at the text byte
    # cap; the 33rd canonical Message remains pending without changing identity.
    assert len(first.selected_input_message_ids) == 32
    assert first.selected_input_message_ids == tuple(large_ids[:32])
    assert await worker.run_once() is True
    second = port.prepared[1].request
    assert second.selected_input_message_ids == (
        large_ids[32],
    )
    # Recreate the worker to model restart between the byte-split Turns. The
    # later due Conversation remains visible after the oversized candidate.
    restarted = TelegramWorker(h["database"], settings, port)
    assert await restarted.run_once() is True
    third = port.prepared[2].request
    assert third.selected_input_message_ids == (later_id,)
    assert await restarted.run_once() is False
    all_selected = [message_id for prepared in port.prepared
                    for message_id in prepared.request.selected_input_message_ids]
    assert len(all_selected) == 34
    assert len(set(all_selected)) == 34
    assert len(port.prepared) == 3
    async with h["database"].session_factory() as session, session.begin():
        rows = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
    assert len(rows) == 34
    assert len({row.message_id for row in rows}) == 34


@pytest.mark.asyncio
async def test_message_count_and_byte_limits_both_bound_a_257_message_burst(
    telegram_harness, sql
):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    accepted = []
    for index in range(257):
        response = await telegram_admit(
            h["client"], h["gateway_headers"], message_id=1000 + index,
            update_id=2000 + index, text_value="small",
        )
        assert response.status_code == 200
        accepted.append(response)
    first_admission = UUID(accepted[0].json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(TelegramAdmission.id == first_admission))
    assert conversation_id is not None
    await _advance_to_due(sql, conversation_ids=[conversation_id])
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once() is True
    assert len(port.prepared[0].request.selected_input_message_ids) == 256
    assert await worker.run_once() is True
    assert len(port.prepared[1].request.selected_input_message_ids) == 1
    selected = [message_id for prepared in port.prepared
                for message_id in prepared.request.selected_input_message_ids]
    admission_ids = [UUID(response.json()["admission_id"]) for response in accepted]
    async with h["database"].session_factory() as session, session.begin():
        expected = list((await session.scalars(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(admission_ids)
            ).order_by(TelegramAdmission.update_id))).all())
    assert selected == expected
    async with h["database"].session_factory() as session, session.begin():
        rows = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
    assert len(rows) == 257 and len({row.message_id for row in rows}) == 257


@pytest.mark.asyncio
async def test_projection_prunes_old_context_but_keeps_every_new_burst_input(
    telegram_harness, sql
):
    h = telegram_harness
    # Create one old no-policy input and consume it through the immediate path.
    # It and the following SYSTEM context rows must not become new burst input.
    old = await telegram_admit(
        h["client"], h["gateway_headers"], message_id=7, update_id=55,
        text_value="o" * 16_384,
    )
    assert old.status_code == 200
    port = DeterministicManagedPort()
    immediate = TelegramWorker(h["database"], h["settings"], port)
    assert await immediate.run_once() is True

    old_admission = UUID(old.json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(TelegramAdmission.id == old_admission))
    assert conversation_id is not None
    async with sql.begin() as connection:
        for sequence in range(2, 33):
            await connection.execute(text(
                "INSERT INTO messages(id,conversation_id,sequence,actor,direction,"
                "content_type,text) VALUES(:id,:conversation,:sequence,'SYSTEM','OUTBOUND',"
                "'TEXT',:text)"), {
                    "id": uuid4(), "conversation": conversation_id, "sequence": sequence,
                    "text": "c" * 16_384,
                })
        await connection.execute(text(
            "UPDATE conversations SET last_message_sequence=32 WHERE id=:id"),
            {"id": conversation_id},
        )

    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    burst = [await telegram_admit(
        h["client"], h["gateway_headers"], message_id=100 + index,
        update_id=200 + index, text_value="n" * 8_192,
    ) for index in range(2)]
    assert all(response.status_code == 200 for response in burst)
    admission_ids = [UUID(response.json()["admission_id"]) for response in burst]
    async with h["database"].session_factory() as session, session.begin():
        input_ids = list((await session.scalars(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(admission_ids)
            ).order_by(TelegramAdmission.update_id))).all())
    await _advance_to_due(sql, sequences=[33, 34])

    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once() is True
    request = port.prepared[-1].request
    assert request.selected_input_message_ids == tuple(input_ids)
    assert request.input_first_sequence == 33 and request.input_last_sequence == 34
    assert request.messages[0].sequence > 1
    assert sum(len(message.text.encode("utf-8")) for message in request.messages) == (
        MAX_PROJECTED_TEXT_BYTES
    )
    async with h["database"].session_factory() as session, session.begin():
        owners = (await session.scalars(select(TurnInputMessage).where(
            TurnInputMessage.message_id.in_(input_ids)))).all()
    assert {owner.message_id for owner in owners} == set(input_ids)
    assert len(owners) == 2


@pytest.mark.asyncio
async def test_max_wait_bounds_continuous_burst(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=500, max_wait_ms=1000)
    _install_settings(h, settings)
    admissions = []
    clock = h["test_clock"]
    opened_at = clock.current
    for index, message_id in enumerate(range(7, 11)):
        if index:
            elapsed_ms = (400, 800, 1000)[index - 1]
            clock.advance_to(opened_at + timedelta(milliseconds=elapsed_ms))
        response = await telegram_admit(h["client"], h["gateway_headers"],
                                        message_id=message_id, update_id=message_id + 48)
        assert response.status_code == 200
        admissions.append(UUID(response.json()["admission_id"]))
    clock.advance_to(opened_at + timedelta(milliseconds=1000))
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
    logical_now = await _advance_to_due(sql, sequences=[1])
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
                              args=(database_url, process_settings, logical_now, results))
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


def _select_in_fresh_process(
    database_url: str, data: dict[str, Any], logical_now: datetime, output
) -> None:
    async def run() -> str | None:
        from db.database import Database

        settings = Settings(**data, _env_file=None)
        principal = next(item for item in settings.auth_principals
                         if item.principal_id == settings.telegram_worker_principal_id).principal()
        clock_token = set_test_database_clock(logical_now)
        database = Database(database_url)
        try:
            result = await select_settled_turn(
                database, principal, settings, (Channel.TELEGRAM,)
            )
            return str(result) if result else None
        finally:
            await database.dispose()
            reset_test_database_clock(clock_token)

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
    await _advance_to_due(sql, sequences=[1])
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
async def test_inbound_during_claimed_turn_is_retained_for_next_selection(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    assert first.json()["route"] == "MANAGED"
    await _advance_to_due(sql, sequences=[1])
    port = PausingManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    running = asyncio.create_task(worker.run_once())
    await asyncio.wait_for(port.started.wait(), timeout=5)

    second = await telegram_admit(h["client"], h["gateway_headers"],
                                  message_id=8, update_id=56, text_value="during execution")
    assert second.status_code == 200
    async with h["database"].session_factory() as session, session.begin():
        first_turn = await session.scalar(select(AgentTurn))
        assignments = (await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all()
        customer_count = await session.scalar(select(func.count()).select_from(Message).where(
            Message.actor == "CUSTOMER", Message.direction == "INBOUND"))
    assert first_turn is not None and first_turn.status == "CLAIMED"
    assert [row.sequence for row in assignments] == [1]
    assert customer_count == 2

    port.release.set()
    assert await running
    await _advance_to_due(sql, sequences=[2])
    assert await worker.run_once()
    assert [prepared.request.input_first_sequence for prepared in port.prepared] == [1, 2]
    assert [prepared.request.input_last_sequence for prepared in port.prepared] == [1, 2]


@pytest.mark.asyncio
async def test_large_unsettled_prefix_does_not_hide_later_due_conversation(
    telegram_harness, sql
):
    h = telegram_harness
    headers = h["gateway_headers"]

    async def submit(message_id: int, thread_id: str):
        value = inbound(message_id=message_id, update_id=message_id + 100,
                        chat_id=int(thread_id))
        return await h["client"].post("/internal/v1/telegram/admissions", json=value,
                                     headers=headers)

    # Create a long, ineligible prefix before enabling policy for new admissions.
    for batch_start in range(0, 300, 25):
        responses = await asyncio.gather(*(
            submit(message_id=message_id, thread_id="123")
            for message_id in range(batch_start + 7, batch_start + 32)
        ))
        assert all(response.status_code == 200 for response in responses)
    async with h["database"].session_factory() as session, session.begin():
        backlog_conversation = await session.scalar(select(Conversation).where(
            Conversation.provider_thread_id == "123"))
    assert backlog_conversation is not None and backlog_conversation.settling_revision == 300
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    backlog = await submit(message_id=1000, thread_id="123")
    later = await submit(message_id=1001, thread_id="124")
    assert backlog.status_code == later.status_code == 200
    await _advance_to_due(sql)
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
    assert conversation.provider_thread_id == "124"


@pytest.mark.asyncio
async def test_candidate_scan_uses_bounded_keyset_pages(telegram_harness, sql):
    h = telegram_harness
    old_settings = _policy_settings(h["settings"], quiet_ms=5, max_wait_ms=100)
    _install_settings(h, old_settings)
    candidate_count = CANDIDATE_PAGE_SIZE + 1
    chat_ids = [6000 + index for index in range(candidate_count)]

    for batch_start in range(0, len(chat_ids), 20):
        responses = await asyncio.gather(*(
            h["client"].post(
                "/internal/v1/telegram/admissions",
                json=inbound(message_id=10_000 + index, update_id=20_000 + index,
                             chat_id=chat_id),
                headers=h["gateway_headers"],
            )
            for index, chat_id in enumerate(chat_ids[batch_start:batch_start + 20],
                                             start=batch_start)
        ))
        assert all(response.status_code == 200 for response in responses)

    old_chat_ids = chat_ids
    due_chat_id = 7000 + candidate_count
    async with h["database"].session_factory() as session, session.begin():
        old_conversation_ids = (await session.scalars(select(Conversation.id).where(
            Conversation.provider_thread_id.in_(list(map(str, old_chat_ids)))
        ))).all()
        assert len(old_conversation_ids) == candidate_count
        await session.execute(update(Conversation).where(
            Conversation.id.in_(old_conversation_ids)
        ).values(control_mode="HUMAN"))

    # The first page contains 101 real due bursts that are skipped by the
    # locked control-mode check. The eligible conversation has a later deadline.
    await _advance_to_due(sql, conversation_ids=old_conversation_ids)
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    due = await h["client"].post(
        "/internal/v1/telegram/admissions",
        json=inbound(message_id=10_000 + candidate_count,
                     update_id=20_000 + candidate_count, chat_id=due_chat_id),
        headers=h["gateway_headers"],
    )
    assert due.status_code == 200, due.text
    async with h["database"].session_factory() as session, session.begin():
        due_conversation_id = await session.scalar(select(Conversation.id).where(
            Conversation.provider_thread_id == str(due_chat_id)
        ))
    assert due_conversation_id is not None
    await _advance_to_due(sql, conversation_ids=[due_conversation_id])

    principal = next(item for item in settings.auth_principals
                     if item.principal_id == settings.telegram_worker_principal_id).principal()
    selected_id = await select_settled_turn(
        h["database"], principal, settings, (Channel.TELEGRAM,)
    )
    assert selected_id is not None
    async with h["database"].session_factory() as session, session.begin():
        selected = await session.get(AgentTurn, selected_id)
        assert selected is not None
        conversation = await session.get(Conversation, selected.conversation_id)
    assert selected is not None and conversation is not None
    assert conversation.provider_thread_id == str(due_chat_id)


@pytest.mark.asyncio
async def test_new_eligible_input_supersedes_draft_and_retains_operation_refs(
    telegram_harness, sql
):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    await _advance_to_due(sql, sequences=[1])
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
async def test_control_change_before_dispatch_blocks_telegram_send(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    accepted = await telegram_admit(h["client"], h["gateway_headers"],
                                    message_id=7, update_id=55)
    assert accepted.status_code == 200
    admission_id = UUID(accepted.json()["admission_id"])
    async with h["database"].session_factory() as session, session.begin():
        conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(TelegramAdmission.id == admission_id))
    assert conversation_id is not None
    await _advance_to_due(sql, sequences=[1])
    worker = TelegramWorker(h["database"], settings, DeterministicManagedPort())
    assert await worker.run_once()

    changed = await h["client"].post(
        f"/internal/v1/conversations/{conversation_id}/control",
        json={"expected_authority_revision": 0, "control_mode": "HUMAN"},
        headers={"X-Service-Token": h["full_token"]},
    )
    assert changed.status_code == 200
    identity = {key: inbound()[key] for key in (
        "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id")}
    response_id = expected_response_attempt_id(admission_id)
    begin = await h["client"].post(
        "/internal/v1/telegram/responses/begin",
        json={**identity, "response_attempt_id": str(response_id)},
        headers=h["gateway_headers"],
    )
    assert begin.status_code == 409
    assert begin.json()["detail"]["code"] == "AUTHORITY_REVISION_CONFLICT"
    async with h["database"].session_factory() as session, session.begin():
        assert await session.scalar(select(DeliveryAttempt.id)) is None


@pytest.mark.asyncio
async def test_inbound_and_delivery_fence_have_one_postgres_ordering(telegram_harness, sql):
    h = telegram_harness
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=100)
    _install_settings(h, settings)
    first = await telegram_admit(h["client"], h["gateway_headers"], message_id=7, update_id=55)
    assert first.status_code == 200
    await _advance_to_due(sql, sequences=[1])
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
