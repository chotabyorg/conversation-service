"""Credential input uses the common selector, restart recovery and writer barrier."""
import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from core.config import CatchUpBinding
from core.errors import DomainError
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    HandoffCase,
    InboxGrantScope,
    TurnInputMessage,
    WebsiteSessionCapability,
)
from schemas.contracts import Channel, ErrorCode, TurnClaim, TurnExecute
from services import turns
from services.authority import advance_authority
from services.common import db_clock
from services.managed_execution_v3 import execute_claimed_turn_v3
from services.settling import select_settled_turn
from services.website_worker import WebsiteWorker, select_immediate
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_session import sh as session_fixture
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import append, common_website, due, select_turn

sh, suppliers, wh = session_fixture, supplier_fixture, common_website


async def settled(wh, settings=None):
    return await select_settled_turn(wh.database, wh.principal, settings or wh.settings,
                                      (Channel.WEBSITE_WIDGET,))


async def test_website_selector_shares_scope_with_credential_ingress(wh):
    await append(wh)
    await due(wh)
    async with wh.database.session_factory() as session, session.begin():
        assert await session.scalar(select(InboxGrantScope).with_for_update(read=True))
        # This real second transaction must finish while the admission-style
        # shared scope lock remains held. Lifecycle writers still need FOR UPDATE.
        selected = await asyncio.wait_for(settled(wh), timeout=3)
        assert isinstance(selected, UUID)


async def proof(wh, turn_id):
    claim = await turns.claim_turn(wh.database, wh.principal, turn_id,
                                  TurnClaim(worker_ref="website-restart", lease_seconds=5))
    return TurnExecute(claim_epoch=claim.turn.claim_epoch, claim_token=claim.claim_token)


async def revoke(wh, kind):
    async with wh.database.session_factory() as session, session.begin():
        scope = await session.scalar(select(InboxGrantScope).with_for_update())
        root = await session.scalar(select(Conversation).with_for_update())
        cap = await session.get(WebsiteSessionCapability, root.id)
        if kind == "grant":
            scope.active = False
        elif kind == "cp_floor":
            scope.active = False
            scope.cp_binding_high_water = scope.cp_binding_generation
        elif kind == "ai_floor":
            scope.active = False
            scope.agent_lifecycle_high_water = scope.agent_lifecycle_generation
        elif kind == "epoch":
            scope.grant_generation += 1
        elif kind == "credential":
            cap.revoked_at = await db_clock(session)
        elif kind == "expiry":
            cap.expires_at = await db_clock(session) + timedelta(milliseconds=1)
            wh.clock.advance_to(cap.expires_at)
        else:
            raise AssertionError("Unknown fixture fence")


async def live_writer(wh, turn_id):
    async with wh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        attempt = DeliveryAttempt(id=uuid4(), conversation_id=root.id, delivery_key=uuid4(),
            source_turn_id=turn_id, creation_fingerprint="1" * 64,
            authority_revision=root.authority_revision, writer_state="LIVE",
            writer_supplier_instance_id=uuid4(), writer_process_incarnation_id=uuid4(),
            writer_started_at=await db_clock(session))
        session.add(attempt)
        return attempt.id


@pytest.mark.parametrize("mode", ["DRAFT", "HANDOFF"])
async def test_real_credential_input_runs_common_website_worker(wh, suppliers, mode):
    await append(wh)
    await due(wh)
    suppliers.calls.clear()
    port = WebsiteV3Port(mode)
    assert await WebsiteWorker(wh.database, wh.settings, port).run_once()
    assert len(port.requests) == 1 and suppliers.calls == []
    async with wh.database.session_factory() as session:
        turn = await session.scalar(select(AgentTurn))
        assert turn.managed_contract_version == 3 and turn.status == "COMPLETED"
        assert await session.scalar(select(func.count()).select_from(TurnInputMessage)) == 1
        cases = list(await session.scalars(select(HandoffCase)))
        assert len(cases) == (1 if mode == "HANDOFF" else 0)
        if cases:
            assert cases[0].channel == "WEBSITE_WIDGET"
            assert (await session.scalar(select(Conversation))).control_mode == "HUMAN"


@pytest.mark.parametrize("kind", ["grant", "cp_floor", "ai_floor", "epoch",
                                  "credential", "expiry"])
async def test_common_selection_rechecks_all_retained_website_fences(wh, kind):
    await append(wh)
    await due(wh)
    await revoke(wh, kind)
    assert await settled(wh) is None
    assert await select_immediate(wh.database, wh.principal, wh.settings) is None


@pytest.mark.parametrize("kind", ["cp_floor", "ai_floor", "epoch", "credential", "expiry"])
async def test_fence_after_selection_blocks_first_dispatch(wh, kind):
    turn_id = await select_turn(wh)
    claim = await proof(wh, turn_id)
    await revoke(wh, kind)
    port = WebsiteV3Port("DRAFT")
    with pytest.raises(DomainError) as captured:
        await execute_claimed_turn_v3(wh.database, wh.principal, turn_id, claim,
                                      wh.settings, port)
    assert captured.value.code == ErrorCode.GRANT_CONFLICT
    assert port.requests == []


async def test_pending_v3_survives_restart_but_not_credential_revocation(wh):
    turn_id = await select_turn(wh)
    assert await select_immediate(wh.database, wh.principal, wh.settings) == turn_id
    await revoke(wh, "credential")
    assert await select_immediate(wh.database, wh.principal, wh.settings) is None


async def test_original_marked_turn_recovers_after_revoke_human_and_config_removal(wh):
    turn_id = await select_turn(wh)
    port = WebsiteV3Port("UNSEALED")
    assert await WebsiteWorker(wh.database, wh.settings, port).run_once()
    await revoke(wh, "grant")
    async with wh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        advance_authority(root)
        root.control_mode = "HUMAN"
        turn = await session.get(AgentTurn, turn_id)
        wh.clock.advance_to(turn.lease_expires_at + timedelta(seconds=1))
    removed = wh.settings.model_copy(update={"managed_ai_bindings": (), "website_bindings": ()})
    assert await select_immediate(wh.database, wh.principal, removed) == turn_id
    port.mode = "HANDOFF"
    assert await WebsiteWorker(wh.database, wh.settings, port).run_once()
    assert len(port.requests) == 2 and port.requests[0].body == port.requests[1].body
    async with wh.database.session_factory() as session:
        assert (await session.get(AgentTurn, turn_id)).status == "COMPLETED"
        assert await session.scalar(select(func.count()).select_from(HandoffCase)) == 0


async def test_live_writer_blocks_new_dispatch_until_positive_end(wh):
    turn_id = await select_turn(wh)
    attempt_id = await live_writer(wh, turn_id)
    assert await select_immediate(wh.database, wh.principal, wh.settings) is None
    claim = await proof(wh, turn_id)
    port = WebsiteV3Port("DRAFT")
    with pytest.raises(DomainError) as captured:
        await execute_claimed_turn_v3(wh.database, wh.principal, turn_id, claim,
                                      wh.settings, port)
    assert captured.value.code == ErrorCode.GRANT_CONFLICT and port.requests == []
    # Fixture represents an already verified positive writer end, not a timeout.
    async with wh.database.session_factory() as session, session.begin():
        attempt = await session.get(DeliveryAttempt, attempt_id)
        attempt.writer_state, attempt.writer_ended_at = "ENDED", await db_clock(session)
    # The existing claim still owns its live lease after the writer has ended.
    assert await select_immediate(wh.database, wh.principal, wh.settings) is None
    result = await execute_claimed_turn_v3(wh.database, wh.principal, turn_id, claim,
                                          wh.settings, port)
    assert result.turn.status == "COMPLETED" and len(port.requests) == 1


async def test_website_uses_one_bounded_catch_up_across_due_bursts(wh):
    binding = wh.settings.managed_ai_bindings[0]
    settings = wh.settings.model_copy(update={"catch_up_bindings": (CatchUpBinding(
        owner_id=binding.owner_id, integration_id=binding.integration_id,
        agent_id=binding.agent_id, channel=binding.channel),)})
    wh.settings = settings
    wh.client._transport.app.state.settings = settings
    for index in range(3):
        await append(wh, f"input {index}")
        await due(wh)
    port = WebsiteV3Port("DRAFT")
    assert await WebsiteWorker(wh.database, settings, port).run_once()
    assert len(port.requests) == 1
    request = port.requests[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (1, 3)
    async with wh.database.session_factory() as session:
        turn = await session.scalar(select(AgentTurn))
        assert turn.managed_contract_version == 3 and turn.catch_up_version == 1


async def test_human_burst_is_skipped_and_only_post_return_input_is_selected(wh):
    async with wh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        advance_authority(root)
        root.control_mode = "HUMAN"
    await append(wh)
    await due(wh)
    assert await settled(wh) is None
    async with wh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        advance_authority(root)
        root.control_mode = "AGENT"
        root.handoff_resume_floor_sequence = root.last_message_sequence
    await append(wh)
    await due(wh)
    turn_id = await settled(wh)
    async with wh.database.session_factory() as session:
        turn = await session.get(AgentTurn, turn_id)
        assert (turn.input_first_sequence, turn.input_last_sequence) == (2, 2)
