"""Real PG common v3 execution from a seeded private Website enrollment."""
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from core.errors import DomainError
from models import AgentTurn, Conversation, HandoffCase, InboxGrantScope, Message
from schemas.contracts import Channel, ErrorCode, TurnClaim, TurnExecute
from schemas.managed_v3 import (
    ControlEvidenceResult,
    HandoffControlRequest,
    ManagedTurnV3Request,
    ManagedTurnV3Result,
)
from services import inbox_lifecycle, turns
from services.common import db_clock
from services.managed_client import handoff_request_id, prepare_v3_request
from services.managed_execution_v3 import execute_claimed_turn_v3
from services.settling import _create_turn
from tests.test_handoff import SealedUnknownPort
from tests.test_website import admit, payload
from tests.test_website import wh as website_fixture

wh = website_fixture


class WebsiteV3Port(SealedUnknownPort):
    def __init__(self, mode):
        super().__init__()
        self.mode, self.requests, self.evidence = mode, [], []

    async def execute_v3(self, prepared, binding, request_id=""):
        assert prepared.request.channel == binding.channel.value == "WEBSITE_WIDGET"
        self.requests.append(prepared)
        if self.mode in {"UNKNOWN", "UNSEALED"}:
            return await super().execute_v3(prepared, binding, request_id)
        control = (HandoffControlRequest(type="HUMAN_HANDOFF",
            request_id=handoff_request_id(prepared.request), reason="CUSTOMER_REQUEST", note=None)
            if self.mode == "HANDOFF" else None)
        return ManagedTurnV3Result(**self._correlation(prepared),
            result_kind="PLATFORM_CONTROL_REQUEST" if control else "EXECUTION_RESULT",
            processing_outcome="NO_RESPONSE" if control else "DRAFT_READY",
            draft_text=None if control else "common Website reply", error_code=None,
            operation_refs=[], control_request=control)

    async def control_evidence_v3(self, request, binding):
        assert request.channel == binding.channel.value == "WEBSITE_WIDGET"
        self.evidence.append(request)
        if self.mode != "UNSEALED":
            return await super().control_evidence_v3(request, binding)
        fields = request.model_dump(exclude={"owner_id", "integration_id", "channel"})
        return ControlEvidenceResult(**fields, evidence_kind="UNSEALED", execution_state=None,
            processing_outcome=None, error_code=None, control_request=None, operation_refs=[])


async def selected(wh):
    await admit(wh, payload(wh))
    async with wh.database.session_factory() as session, session.begin():
        binding = wh.settings.managed_ai_bindings[0]
        scope = await inbox_lifecycle._lock_scope(session, dict(owner_id=binding.owner_id,
            integration_id=binding.integration_id, channel=binding.channel.value,
            agent_id=UUID(wh.agent_id)))
        scope.active, scope.grant_generation = True, 1
        scope.cp_binding_generation = scope.agent_lifecycle_generation = 1
        scope.account_id = scope.agent_owner_id = uuid4()
        root = await session.scalar(select(Conversation).with_for_update())
        message = await session.scalar(select(Message))
        turn_id = await _create_turn(session, root, [message], wh.settings.managed_ai_bindings[0],
            Channel.WEBSITE_WIDGET, await db_clock(session), handoff_enrolled=True)
    claim = await turns.claim_turn(wh.database, wh.principal, turn_id,
                                  TurnClaim(worker_ref="website-v3-test", lease_seconds=30))
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch, claim_token=claim.claim_token)
    return turn_id, proof


async def execute(wh, turn_id, proof, port):
    return await execute_claimed_turn_v3(wh.database, wh.principal, turn_id, proof,
                                        wh.settings, port)


@pytest.mark.parametrize("mode", ["DRAFT", "HANDOFF", "UNKNOWN"])
async def test_website_uses_common_v3_result_case_and_unknown_ledger(wh, mode):
    turn_id, proof = await selected(wh)
    port = WebsiteV3Port(mode)
    result = await execute(wh, turn_id, proof, port)
    assert result.turn.status == ("FAILED" if mode == "UNKNOWN" else "COMPLETED")
    if mode == "UNKNOWN":
        assert result.turn.failure_code == "EXECUTION_UNKNOWN"
        assert [item.operation_id for item in result.turn.operation_refs] == ["tool-run-unknown"]
        assert len(port.requests) == 2 and len(port.evidence) == 1
    before = len(port.requests)
    replay = await execute(wh, turn_id, proof, port)
    assert replay.replayed and len(port.requests) == before
    async with wh.database.session_factory() as session:
        cases = list(await session.scalars(select(HandoffCase)))
        root = await session.scalar(select(Conversation))
        if mode == "HANDOFF":
            assert len(cases) == 1 and root.control_mode == "HUMAN"
            assert cases[0].channel == "WEBSITE_WIDGET" and cases[0].source_turn_id == turn_id
        else:
            assert cases == [] and root.control_mode == "AGENT"
    request = port.requests[0].request
    telegram = prepare_v3_request(request.model_copy(update={"channel": "TELEGRAM"}))
    assert telegram.request_fingerprint != port.requests[0].request_fingerprint
    assert telegram.original_dispatch_id != port.requests[0].original_dispatch_id


async def test_original_website_snapshot_recovers_after_revoke_without_new_authority(wh):
    turn_id, proof = await selected(wh)
    port = WebsiteV3Port("UNSEALED")
    pending = await execute(wh, turn_id, proof, port)
    assert pending.turn.status == "CLAIMED"
    async with wh.database.session_factory() as session, session.begin():
        scope = await session.scalar(select(InboxGrantScope).with_for_update())
        scope.active, scope.cp_binding_high_water = False, 1
    port.mode = "HANDOFF"
    result = await execute(wh, turn_id, proof, port)
    assert result.turn.status == "COMPLETED" and result.turn.outcome == "NO_RESPONSE"
    assert len(port.requests) == 2 and port.requests[0].body == port.requests[1].body
    assert port.evidence[0].channel == "WEBSITE_WIDGET"
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(HandoffCase)) == 0
        assert (await session.scalar(select(Conversation))).control_mode == "AGENT"


@pytest.mark.parametrize("field", ["channel", "owner_id", "integration_id"])
async def test_recomputed_snapshot_cannot_move_original_website_scope(wh, field):
    turn_id, proof = await selected(wh)
    port = WebsiteV3Port("UNSEALED")
    await execute(wh, turn_id, proof, port)
    async with wh.database.session_factory() as session, session.begin():
        turn = await session.get(AgentTurn, turn_id)
        value = dict(turn.managed_v3_snapshot)
        value[field] = {"channel": "TELEGRAM", "owner_id": "different-owner",
                        "integration_id": str(uuid4())}[field]
        forged = prepare_v3_request(ManagedTurnV3Request.model_validate(value))
        turn.managed_v3_snapshot = forged.request.model_dump(mode="json")
        turn.managed_v3_request_fingerprint = forged.request_fingerprint
        turn.managed_v3_dispatch_id = forged.original_dispatch_id
        turn.managed_request_fingerprint = forged.sidecar_fingerprint
    with pytest.raises(DomainError) as caught:
        await execute(wh, turn_id, proof, port)
    assert caught.value.code == ErrorCode.VALIDATION_ERROR
    assert len(port.requests) == 1
