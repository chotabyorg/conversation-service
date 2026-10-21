"""Native PostgreSQL barriers, exact evidence, bounded catch-up and replay precedence."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

from core.config import CatchUpBinding, Settings
from core.errors import DomainError
from models import (
    AgentTurn,
    CatchUpMember,
    CatchUpSelection,
    Conversation,
    TelegramAdmission,
)
from schemas.contracts import ControlTransition, DeliveryCreate
from schemas.effects import ExternalEffectPermitRequest
from services import authority, catch_up, delivery, effects
from services.settling import select_settled_turn
from services.telegram_worker import TelegramWorker
from tests import test_website
from tests.test_settling import (
    PausingManagedPort,
    _advance_to_due,
    _install_settings,
    _policy_settings,
)
from tests.test_settling import telegram_harness as _telegram_harness
from tests.test_telegram import DeterministicManagedPort, inbound
from tests.test_telegram import admit as telegram_admit

telegram_harness = _telegram_harness
wh = test_website.wh


def enable(h):
    settings = _policy_settings(h["settings"], quiet_ms=10, max_wait_ms=30_000)
    binding = settings.managed_ai_bindings[0]
    settings = settings.model_copy(update={"catch_up_bindings": (CatchUpBinding(
        owner_id=binding.owner_id, integration_id=binding.integration_id,
        agent_id=binding.agent_id, channel=binding.channel,
    ),)})
    _install_settings(h, settings)
    return settings


async def add(h, sql, n, *, close=True, text_value="hello"):
    accepted = await telegram_admit(h["client"], h["gateway_headers"],
        message_id=7+n, update_id=55+n, text_value=text_value)
    assert accepted.status_code == 200, accepted.text
    if close:
        await _advance_to_due(sql, sequences=[n+1])
    return accepted.json()


async def turn_and_conversation(h, turn_id):
    async with h["database"].session_factory() as session:
        turn = await session.get(AgentTurn, turn_id)
        conversation = await session.get(Conversation, turn.conversation_id)
        return turn, conversation


def principal(h):
    return next(p for p in h["settings"].auth_principals if p.role == "FULL").principal()


def permit_request(turn, conversation):
    return ExternalEffectPermitRequest(
        contract_version=1, conversation_id=conversation.id, turn_id=turn.id,
        origin_turn_id=turn.id, owner_id=conversation.owner_id,
        integration_id=conversation.integration_id, channel=conversation.channel,
        agent_id=turn.managed_agent_id, authority_revision=turn.authority_revision,
        managed_request_fingerprint=turn.managed_request_fingerprint,
        operation_namespace="main-chota.ai-business.v1", operation_id=uuid4(),
        operation_fingerprint="b"*64,
    )


async def begin(h, n=0):
    from services.telegram import expected_response_attempt_id
    value = inbound(message_id=7+n, update_id=55+n)
    async with h["database"].session_factory() as session:
        admission = await session.scalar(select(TelegramAdmission).where(
            TelegramAdmission.message_id == str(7+n)))
    identity = {key: value[key] for key in (
        "owner_id", "integration_id", "agent_id", "provider_thread_id", "provider_event_id",
    )}
    body = {**identity, "response_attempt_id": str(expected_response_attempt_id(admission.id))}
    result = await h["client"].post("/internal/v1/telegram/responses/begin",
        json=body, headers=h["gateway_headers"])
    return result, body


async def test_busy_turn_then_three_bursts_produces_one_current_turn(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    paused = PausingManagedPort()
    running = asyncio.create_task(TelegramWorker(h["database"], settings, paused).run_once())
    try:
        await asyncio.wait_for(paused.started.wait(), 8)
        for n in range(1, 4):
            await add(h, sql, n)
        competing = TelegramWorker(h["database"], settings, DeterministicManagedPort())
        assert await competing.run_once() is False
    finally:
        paused.release.set()
    assert await running
    stale, _ = await begin(h)
    assert stale.status_code == 200 and stale.json()["send"] is False
    current = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, current).run_once()
    assert current.calls == 1
    request = current.prepared[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (2, 4)
    assert len(request.selected_input_message_ids) == 3
    turn, conversation = await turn_and_conversation(h, request.turn_id)
    async with h["database"].session_factory() as session:
        assert await catch_up.validate(session, conversation, turn)
        assert await catch_up.effect_complete(session, conversation, turn)
        members = list((await session.scalars(select(CatchUpMember).where(
            CatchUpMember.turn_id == turn.id).order_by(CatchUpMember.ordinal))).all())
        assert [(m.first_sequence, m.last_sequence) for m in members] == [(2, 2), (3, 3), (4, 4)]
    sent, _ = await begin(h, 1)
    assert sent.status_code == 200 and sent.json()["send"] is True
    assert await TelegramWorker(h["database"], settings, current).run_once() is False


async def test_identity_drift_after_permit_keeps_exact_replay_but_denies_new_work(
    telegram_harness, sql,
):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    paused = PausingManagedPort()
    running = asyncio.create_task(TelegramWorker(h["database"], settings, paused).run_once())
    try:
        await asyncio.wait_for(paused.started.wait(), 8)
        async with h["database"].session_factory() as session:
            turn = await session.scalar(select(AgentTurn))
            conversation = await session.get(Conversation, turn.conversation_id)
        request = permit_request(turn, conversation)
        original = await effects.permit(h["database"], principal(h), request)
        async with sql.begin() as connection:
            await connection.execute(update(TelegramAdmission).values(update_id=9999))
        replay = await effects.permit(h["database"], principal(h), request)
        assert replay.replayed and replay.permit_id == original.permit_id
        with pytest.raises(effects.EffectServiceConflict, match="EXTERNAL_EFFECT_DENIED"):
            await effects.permit(h["database"], principal(h),
                                 request.model_copy(update={"operation_id": uuid4()}))
        async with h["database"].session_factory() as session:
            assert not await catch_up.validate(session, conversation, turn)
    finally:
        paused.release.set()
    assert await running
    begun, _ = await begin(h)
    assert begun.status_code != 200 or begun.json()["send"] is False


async def test_committed_send_recovers_original_evidence_after_identity_drift(
    telegram_harness, sql,
):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    assert await TelegramWorker(h["database"], settings, DeterministicManagedPort()).run_once()
    begun, body = await begin(h)
    assert begun.status_code == 200 and begun.json()["send"] is True
    async with sql.begin() as connection:
        await connection.execute(update(TelegramAdmission).values(update_id=9999))
    binding = settings.managed_ai_bindings[0]
    work = await h["client"].post("/internal/v1/telegram/responses/work", json={
        "owner_id": binding.owner_id, "integration_id": str(binding.integration_id),
        "agent_id": str(binding.agent_id), "bot_id": 999,
    }, headers=h["gateway_headers"])
    assert work.status_code == 200, work.text
    assert len(work.json()["items"]) == 1
    assert work.json()["items"][0]["work_kind"] == "RECOVER_ONLY"
    assert work.json()["items"][0]["response_attempt_id"] == body["response_attempt_id"]
    recovered, _ = await begin(h)
    assert recovered.status_code == 200
    assert recovered.json()["send"] is False
    assert recovered.json()["work_kind"] == "RECOVER_ONLY"
    completed = await h["client"].post("/internal/v1/telegram/responses/complete",
        json={**body, "outcome": "PROVIDER_ACCEPTED", "provider_message_id": "original-write"},
        headers=h["gateway_headers"])
    assert completed.status_code == 200, completed.text
    replay, _ = await begin(h)
    assert replay.status_code == 200 and replay.json()["send"] is False


@pytest.mark.parametrize("mutation", [
    "UPDATE catch_up_selections SET latest_customer_sequence=99",
    "DELETE FROM catch_up_members",
    "UPDATE turn_input_messages SET ordinal=9",
    "DELETE FROM turn_input_messages",
    "UPDATE agent_turns SET catch_up_version=0",
])
async def test_sealed_selection_is_immutable(telegram_harness, sql, mutation):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    await add(h, sql, 1)
    port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, port).run_once()
    with pytest.raises(IntegrityError):
        async with sql.begin() as connection:
            await connection.execute(text(mutation))
    turn, conversation = await turn_and_conversation(h, port.prepared[0].request.turn_id)
    async with h["database"].session_factory() as session:
        assert await catch_up.validate(session, conversation, turn)


async def test_generic_full_delivery_cannot_bypass_channel_fence(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, port).run_once()
    turn, conversation = await turn_and_conversation(h, port.prepared[0].request.turn_id)
    with pytest.raises(DomainError) as raised:
        await delivery.create_delivery(h["database"], principal(h), conversation.id,
            DeliveryCreate(delivery_key=uuid4(), source_turn_id=turn.id,
                           expected_authority_revision=conversation.authority_revision))
    assert raised.value.code.value == "INVALID_DELIVERY_TRANSITION"


async def test_not_due_boundary_is_not_waited_out_or_absorbed(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    await add(h, sql, 1, close=False)
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once()
    request = port.prepared[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (1, 1)
    turn, conversation = await turn_and_conversation(h, request.turn_id)
    async with h["database"].session_factory() as session:
        assert not await catch_up.communication_current(session, conversation, turn)
        assert not await catch_up.effect_complete(session, conversation, turn)
    assert await worker.run_once() is False
    await _advance_to_due(sql, sequences=[2])
    assert await worker.run_once()
    latest, _ = await begin(h, 1)
    assert latest.status_code == 200 and latest.json()["send"] is True


def test_catch_up_cohort_requires_managed_settling_text_binding(telegram_harness):
    assert Settings.model_fields["catch_up_bindings"].default == ()
    values = telegram_harness["settings"].model_dump()
    values["catch_up_bindings"] = [dict(owner_id="x", integration_id=uuid4(),
                                       agent_id=uuid4(), channel="TELEGRAM")]
    with pytest.raises(ValueError, match="invalid catch-up configuration"):
        Settings(**values, _env_file=None)
    with pytest.raises(ValueError):
        CatchUpBinding(owner_id="x", integration_id=uuid4(), agent_id=uuid4(), channel="VOICE")


@pytest.mark.parametrize("bounded", ["count", "bytes"])
async def test_bounded_prefix_never_gains_effect_eligibility_after_residual_is_consumed(
    telegram_harness, sql, bounded,
):
    h = telegram_harness
    settings = enable(h)
    count, body = (258, "small") if bounded == "count" else (68, "x"*8000)
    # First burst is complete; splitting a later member must also establish the floor.
    await add(h, sql, 0)
    for n in range(1, count):
        await add(h, sql, n, close=False, text_value=body)
    await _advance_to_due(sql, sequences=[count])
    port = DeterministicManagedPort()
    worker = TelegramWorker(h["database"], settings, port)
    assert await worker.run_once()
    first, conversation = await turn_and_conversation(h, port.prepared[0].request.turn_id)
    assert first.input_first_sequence == 1 and first.input_last_sequence < count
    async with h["database"].session_factory() as session:
        assert not await catch_up.effect_complete(session, conversation, first)
        members = list((await session.scalars(select(CatchUpMember).where(
            CatchUpMember.turn_id == first.id).order_by(CatchUpMember.ordinal))).all())
        assert len(members) == 2 and members[1].last_sequence < count
        assert await effects._split_floor(session, conversation.id, 0, count) == count
    stale, _ = await begin(h)
    assert stale.status_code == 200 and stale.json()["send"] is False
    assert await worker.run_once()
    last, conversation = await turn_and_conversation(h, port.prepared[-1].request.turn_id)
    assert last.input_last_sequence == count
    async with h["database"].session_factory() as session:
        assert await catch_up.communication_current(session, conversation, last)
        assert not await catch_up.effect_complete(session, conversation, first)
        assert not await catch_up.effect_complete(session, conversation, last)
        assert await effects._split_floor(session, conversation.id, 0, count) == count
    latest, _ = await begin(h, first.input_last_sequence)
    assert latest.status_code == 200 and latest.json()["send"] is True
    assert await worker.run_once() is False


@pytest.mark.parametrize("drift", ["none", "new_input", "authority", "proof", "fence"])
async def test_website_coalesces_bursts_and_writes_once(wh, sql, test_database_clock, drift):
    from core.config import SettlingPolicy
    from services.website_worker import WebsiteWorker

    binding = wh.settings.managed_ai_bindings[0]
    settings = wh.settings.model_copy(update={
        "turn_settling_policies": (SettlingPolicy(version="catch-up-website-v1",
            channel="WEBSITE_WIDGET", quiet_window_ms=10, max_wait_ms=30_000),),
        "catch_up_bindings": (CatchUpBinding(owner_id=binding.owner_id,
            integration_id=binding.integration_id, agent_id=binding.agent_id,
            channel=binding.channel),),
    })
    wh.client._transport.app.state.settings = settings
    session_id = str(uuid4())
    first = None
    for n in range(3):
        value = test_website.payload(wh, session_id=session_id)
        await test_website.admit(wh, value)
        first = value if first is None else first
        await _advance_to_due(sql, sequences=[n+1])
    worker = WebsiteWorker(wh.database, settings, wh.ai)
    assert await worker.run_once()
    assert len(wh.ai.calls) == 1
    request = wh.ai.calls[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (1, 3)
    body = {**test_website.identity(first), "response_attempt_id":
            await test_website.v2_response_attempt_id(wh, first)}
    begun = await wh.call("POST", "/website/responses/begin", body)
    assert begun.status_code == 200 and begun.json()["send"] is True
    completed = await wh.call("POST", "/website/responses/complete",
                              dict(body, outcome="WRITE_COMPLETED"))
    assert completed.status_code == 200, completed.text
    replay = await wh.call("POST", "/website/responses/begin", body)
    assert replay.status_code == 200 and replay.json()["send"] is False
    assert replay.json()["action"] == "REPLAY_COMPLETED"
    assert await worker.run_once() is False
    if drift == "new_input":
        await test_website.admit(wh, test_website.payload(wh, session_id=session_id))
    elif drift == "authority":
        async with sql.begin() as conn:
            await conn.execute(text("UPDATE conversations SET control_mode='HUMAN', "
                                    "authority_revision=authority_revision+1"))
    elif drift == "proof":
        async with sql.begin() as conn:
            await conn.execute(text("UPDATE website_admissions SET fingerprint=:fingerprint "
                                    "WHERE message_id=:message_id"),
                               {"fingerprint": "d" * 64, "message_id": first["message_id"]})
    elif drift == "fence":
        async with sql.begin() as conn:
            await conn.execute(text("UPDATE delivery_attempts SET fenced_settling_revision=NULL"))
    after_drift = await wh.call("POST", "/website/responses/begin", body)
    assert after_drift.status_code == 200 and after_drift.json()["send"] is False
    if drift in {"none", "new_input"}:
        assert after_drift.json()["action"] == "REPLAY_COMPLETED"
        assert after_drift.json()["replay_source_attempt_id"] == body["response_attempt_id"]
        assert after_drift.json()["draft_text"] == begun.json()["draft_text"]
    else:
        assert after_drift.json()["action"] == "DENY"
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM delivery_attempts")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM website_response_attempts")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM messages WHERE actor='AGENT'")) == 1
    async with wh.database.session_factory() as session:
        header = await session.get(CatchUpSelection, request.turn_id)
        assert header.member_count == 3


async def test_return_requires_fresh_input_and_separates_original_authority_burst(
    telegram_harness, sql,
):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0, close=False)
    async with h["database"].session_factory() as session:
        conversation = await session.scalar(select(Conversation))
    await authority.transition_control(h["database"], principal(h), conversation.id,
        ControlTransition(control_mode="HUMAN", expected_authority_revision=0))
    await add(h, sql, 1, close=False)
    await authority.transition_control(h["database"], principal(h), conversation.id,
        ControlTransition(control_mode="AGENT", expected_authority_revision=1))
    worker = TelegramWorker(h["database"], settings, DeterministicManagedPort())
    assert await worker.run_once() is False
    await add(h, sql, 2)
    assert await worker.run_once()
    request = worker.port.prepared[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (3, 3)
    assert request.authority_revision == 2
    turn, conversation = await turn_and_conversation(h, request.turn_id)
    async with h["database"].session_factory() as session:
        assert await catch_up.effect_complete(session, conversation, turn)
        header = await session.get(CatchUpSelection, turn.id)
        assert header.authority_input_floor_sequence == 2


async def test_two_workers_cannot_select_or_dispatch_the_same_catch_up(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    for n in range(3):
        await add(h, sql, n)
    port = PausingManagedPort()
    first = asyncio.create_task(TelegramWorker(h["database"], settings, port).run_once())
    try:
        await asyncio.wait_for(port.started.wait(), 8)
        assert await TelegramWorker(h["database"], settings, port).run_once() is False
    finally:
        port.release.set()
    assert await first and port.calls == 1
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM catch_up_selections")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM turn_input_messages")) == 3


async def test_malformed_oldest_candidate_does_not_block_another_conversation(
    telegram_harness, sql,
):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    async with sql.begin() as connection:
        await connection.execute(update(TelegramAdmission).values(agent_id=uuid4()))
    other = await telegram_admit(h["client"], h["gateway_headers"],
                                 message_id=8, update_id=56, chat_id=456)
    assert other.status_code == 200
    await _advance_to_due(sql)
    port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, port).run_once()
    assert port.calls == 1
    _, conversation = await turn_and_conversation(h, port.prepared[0].request.turn_id)
    assert conversation.provider_thread_id == "456"


async def test_catch_up_evidence_blocks_downgrade_without_deleting_history(
    telegram_harness, sql, database_url,
):
    from tests.test_managed_migration import migrate

    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, port).run_once()
    await h["database"].dispose()
    rejected = migrate(database_url, "downgrade", "0010_voice_caller_number", expected=1)
    assert "Retained catch-up evidence prevents downgrade." in rejected.stderr
    assert await h["database"].ready()
    turn, conversation = await turn_and_conversation(h, port.prepared[0].request.turn_id)
    async with h["database"].session_factory() as session:
        assert await catch_up.validate(session, conversation, turn)


async def test_catch_up_marker_cannot_commit_without_its_selection(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, port).run_once()
    async with sql.connect() as connection:
        columns = list((await connection.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name='agent_turns' ORDER BY ordinal_position"))).scalars())
    expressions = [":new_id" if name == "id" else ":new_key" if name == "selection_key"
                   else name for name in columns]
    with pytest.raises(IntegrityError, match="Incomplete catch-up selection"):
        async with sql.begin() as connection:
            await connection.execute(text(
                "INSERT INTO agent_turns ("+",".join(columns)+") SELECT "
                +",".join(expressions)+" FROM agent_turns WHERE id=:source"),
                {"new_id": uuid4(), "new_key": uuid4(), "source": port.prepared[0].request.turn_id})


async def test_fresh_dispatch_revalidates_frozen_identity_after_restart(telegram_harness, sql):
    h = telegram_harness
    settings = enable(h)
    await add(h, sql, 0)
    turn_id = await select_settled_turn(h["database"], principal(h), settings)
    assert turn_id is not None
    async with sql.begin() as connection:
        await connection.execute(update(TelegramAdmission).values(update_id=9999))
    port = DeterministicManagedPort()
    # The restarted worker finds the retained pending Turn, but cannot dispatch it.
    assert await TelegramWorker(h["database"], settings, port).run_once()
    assert port.calls == 0
    turn, _ = await turn_and_conversation(h, turn_id)
    assert turn.status == "FAILED" and turn.managed_dispatch_epoch is None
