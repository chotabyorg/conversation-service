"""Real PostgreSQL legacy text fences using the common grant rows."""
import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text

from core.config import SettlingPolicy
from core.errors import DomainError
from models import Conversation, InboxGrantScope
from schemas.contracts import Channel, ControlTransition, DeliveryCreate, ErrorCode, TurnCreate
from services import authority, delivery, inbox_lifecycle, turns, website_session
from services.settling import select_settled_turn
from services.website_worker import select_immediate
from tests.conftest import error, observed_waiters
from tests.test_website import admit, identity, payload, v2_response_attempt_id
from tests.test_website import wh as website_fixture

wh = website_fixture


async def enrolled(wh, *, retained="generation"):
    async with wh.database.session_factory() as session, session.begin():
        row = await inbox_lifecycle._lock_scope(session, dict(
            owner_id=wh.grants["a"]["owner_id"],
            integration_id=UUID(wh.grants["a"]["integration_id"]),
            channel="WEBSITE_WIDGET", agent_id=UUID(wh.agent_id)))
        row.active = False
        if retained == "generation":
            row.grant_generation = 2
        elif retained == "cp_floor":
            row.cp_binding_high_water = 8
        elif retained == "ai_floor":
            row.agent_lifecycle_high_water = 12


async def root(wh):
    async with wh.database.session_factory() as session:
        return await session.scalar(select(Conversation))


async def test_unenrolled_legacy_website_does_not_create_common_lifecycle_rows(wh, sql):
    await admit(wh, payload(wh))
    assert await wh.worker.run_once()
    conversation = await root(wh)
    turn_id = wh.ai.calls[0].request.turn_id
    await delivery.create_delivery(wh.database, wh.principal, conversation.id,
        DeliveryCreate(delivery_key=uuid4(), source_turn_id=turn_id,
                       expected_authority_revision=conversation.authority_revision),
        settings=wh.settings)
    await authority.transition_control(wh.database, wh.principal, conversation.id,
        ControlTransition(control_mode="HUMAN", expected_authority_revision=0), wh.settings)
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM inbox_grant_scopes")) == 0


@pytest.mark.parametrize("retained", ["generation", "cp_floor", "ai_floor"])
async def test_website_new_admission_fenced_but_original_decision_replays(wh, retained):
    value = payload(wh)
    first = await admit(wh, value)
    await enrolled(wh, retained=retained)
    assert await admit(wh, dict(value, desired_route="LEGACY")) == dict(first, replayed=True)
    for route in ("LEGACY", "MANAGED"):
        response = await wh.call("POST", "/website/admissions",
            dict(value, message_id=str(uuid4()), desired_route=route))
        error(response, "HANDOFF_CONFLICT", 409)


@pytest.mark.parametrize("retained", ["generation", "cp_floor", "ai_floor"])
async def test_generic_authority_and_selection_cannot_bypass_website_scope(wh, retained):
    await admit(wh, payload(wh))
    conversation = await root(wh)
    request = TurnCreate(selection_key=uuid4(), input_first_sequence=1, input_last_sequence=1,
        expected_input_revision=1, expected_authority_revision=0)
    original = await turns.create_turn(wh.database, wh.principal, conversation.id, request,
                                      settings=wh.settings)
    await enrolled(wh, retained=retained)
    replay = await turns.create_turn(wh.database, wh.principal, conversation.id, request,
                                    settings=wh.settings)
    assert replay.replayed and replay.turn.id == original.turn.id
    for operation in (
        authority.transition_control(wh.database, wh.principal, conversation.id,
            ControlTransition(control_mode="HUMAN", expected_authority_revision=0), wh.settings),
        turns.create_turn(wh.database, wh.principal, conversation.id,
            request.model_copy(update={"selection_key": uuid4()}), settings=wh.settings),
    ):
        with pytest.raises(DomainError) as captured:
            await operation
        assert captured.value.code == ErrorCode.HANDOFF_CONFLICT
    assert (await root(wh)).control_mode == "AGENT"


async def test_generic_delivery_replay_survives_but_new_creation_is_fenced(wh):
    value = payload(wh)
    await admit(wh, value)
    assert await wh.worker.run_once()
    second = dict(value, message_id=str(uuid4()))
    await admit(wh, second)
    assert await wh.worker.run_once()
    first_turn, second_turn = [prepared.request.turn_id for prepared in wh.ai.calls]
    conversation = await root(wh)
    request = DeliveryCreate(delivery_key=uuid4(), source_turn_id=first_turn,
                             expected_authority_revision=0)
    original = await delivery.create_delivery(wh.database, wh.principal, conversation.id,
                                              request, settings=wh.settings)
    await enrolled(wh)
    replay = await delivery.create_delivery(wh.database, wh.principal, conversation.id,
                                            request, settings=wh.settings)
    assert replay.replayed and replay.attempt.id == original.attempt.id
    with pytest.raises(DomainError) as captured:
        await delivery.create_delivery(wh.database, wh.principal, conversation.id,
            request.model_copy(update={"delivery_key": uuid4(), "source_turn_id": second_turn}),
            settings=wh.settings)
    assert captured.value.code == ErrorCode.HANDOFF_CONFLICT


async def test_fresh_legacy_website_begin_cannot_send_after_enrollment(wh):
    value = payload(wh)
    await admit(wh, value)
    assert await wh.worker.run_once()
    await enrolled(wh)
    response = await wh.call("POST", "/website/responses/begin", dict(identity(value),
        response_attempt_id=await v2_response_attempt_id(wh, value)))
    assert response.status_code == 200 and not response.json()["send"]
    assert response.json()["action"] == "DENY"


async def test_original_website_completion_remains_available_after_fence(wh):
    value = payload(wh)
    await admit(wh, value)
    assert await wh.worker.run_once()
    request = dict(identity(value), response_attempt_id=await v2_response_attempt_id(wh, value))
    assert (await wh.call("POST", "/website/responses/begin", request)).json()["send"]
    await enrolled(wh)
    repeated = await wh.call("POST", "/website/responses/begin", request)
    assert repeated.status_code == 200 and not repeated.json()["send"]
    completed = await wh.call("POST", "/website/responses/complete",
                             dict(request, outcome="WRITE_COMPLETED"))
    assert completed.status_code == 200
    assert completed.json()["outcome"] == "WRITE_COMPLETED"


@pytest.mark.parametrize("kind", ["admission", "credential"])
async def test_common_revoke_fences_website_agent_root_without_handoff_case(wh, kind):
    if kind == "admission":
        await admit(wh, payload(wh))
    else:
        binding = wh.settings.website_bindings[0].model_copy(update={"capability_generation": 7})
        settings = wh.settings.model_copy(update={"website_session_capabilities_enabled": True,
                                                 "website_bindings": (binding,)})
        wh.client._transport.app.state.settings = settings
        created = await wh.call("POST", "/website/sessions", dict(owner_id=binding.owner_id,
            integration_id=str(binding.integration_id), agent_id=wh.agent_id,
            binding_generation=7))
        assert created.status_code == 201
    original = await root(wh)
    await enrolled(wh)
    async with wh.database.session_factory() as session, session.begin():
        scope = await session.scalar(select(InboxGrantScope).with_for_update())
        other = Conversation(id=uuid4(), owner_id=scope.owner_id,
            integration_id=uuid4(), channel="WEBSITE_WIDGET", provider_thread_id=str(uuid4()))
        session.add(other)
        await session.flush()
        await inbox_lifecycle._fence_grant_outbound(session, scope,
                                                   await website_session.db_clock(session))
        await session.refresh(other)
        assert other.authority_revision == 0
    async with wh.database.session_factory() as session:
        current = await session.get(Conversation, original.id)
        assert current.authority_revision == original.authority_revision + 1
        assert current.control_mode == "AGENT"
        assert current.handoff_resume_floor_sequence == current.last_message_sequence


@pytest.mark.parametrize("route", ["LEGACY", "MANAGED"])
async def test_uuid_only_admission_cannot_attach_to_credentialed_root(wh, route, sql):
    binding = wh.settings.website_bindings[0].model_copy(update={"capability_generation": 7})
    settings = wh.settings.model_copy(update={"website_session_capabilities_enabled": True,
                                             "website_bindings": (binding,)})
    wh.client._transport.app.state.settings = settings
    created = await wh.call("POST", "/website/sessions", dict(owner_id=binding.owner_id,
        integration_id=str(binding.integration_id), agent_id=wh.agent_id, binding_generation=7))
    assert created.status_code == 201
    response = await wh.call("POST", "/website/admissions", payload(wh,
        session_id=created.json()["session_id"], desired_route=route))
    error(response, "NOT_FOUND", 404)
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM conversations")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM website_admissions")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM messages")) == 0


async def test_website_admission_waiter_observes_scope_enrollment(wh, sql):
    await enrolled(wh, retained="empty")
    value = payload(wh)
    await admit(wh, value)
    async with wh.database.session_factory() as blocker, blocker.begin():
        row = await blocker.scalar(select(InboxGrantScope).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(wh.call("POST", "/website/admissions",
                                             dict(value, message_id=str(uuid4()))))
        try:
            await observed_waiters(sql, pid, 1, "website_legacy_admission_after_enrollment")
            row.grant_generation = 1
        finally:
            await blocker.commit()
    error(await pending, "HANDOFF_CONFLICT", 409)
    assert (await root(wh)).input_revision == 1


async def test_stale_settings_post_root_check_observes_new_website_scope(wh, sql):
    await admit(wh, payload(wh))
    conversation = await root(wh)
    async with sql.begin() as conn:
        await conn.execute(text("DELETE FROM inbox_grant_scopes"))
    stale = wh.settings.model_copy(update={"managed_ai_bindings": (), "website_bindings": ()})
    async with wh.database.session_factory() as blocker, blocker.begin():
        await blocker.scalar(select(Conversation).where(
            Conversation.id == conversation.id).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(authority.transition_control(wh.database, wh.principal,
            conversation.id, ControlTransition(control_mode="HUMAN",
                                               expected_authority_revision=0), stale))
        try:
            await observed_waiters(sql, pid, 1, "website_stale_settings_new_scope_before_root")
            row = await inbox_lifecycle._lock_scope(blocker, dict(owner_id=conversation.owner_id,
                integration_id=conversation.integration_id, channel="WEBSITE_WIDGET",
                agent_id=UUID(wh.agent_id)))
            row.grant_generation = 1
        finally:
            await blocker.commit()
    with pytest.raises(DomainError) as captured:
        await pending
    assert captured.value.code == ErrorCode.HANDOFF_CONFLICT
    assert (await root(wh)).control_mode == "AGENT"


async def test_immediate_website_coordinator_cannot_select_after_enrollment(wh, sql):
    await admit(wh, payload(wh))
    await enrolled(wh)
    assert await select_immediate(wh.database, wh.principal, wh.settings) is None
    assert not await wh.worker.run_once()
    assert wh.ai.calls == []
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM agent_turns")) == 0


async def test_settling_website_coordinator_cannot_select_after_enrollment(
    wh, sql, test_database_clock,
):
    policy = SettlingPolicy(version="website-inbox-fence", channel=Channel.WEBSITE_WIDGET,
                           quiet_window_ms=250, max_wait_ms=1200)
    settings = wh.settings.model_copy(update={"turn_settling_policies": (policy,)})
    wh.client._transport.app.state.settings = settings
    await admit(wh, payload(wh))
    test_database_clock.advance_to(test_database_clock.current + timedelta(seconds=2))
    await enrolled(wh)
    assert await select_settled_turn(wh.database, wh.principal, settings,
                                     (Channel.WEBSITE_WIDGET,)) is None
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM agent_turns")) == 0
