"""Real PG common v3 execution from a seeded private Website enrollment."""
import pytest
from sqlalchemy import func, select

from core.errors import DomainError
from models import AgentTurn, Conversation, HandoffCase, InboxGrantScope
from schemas.contracts import ErrorCode, TurnClaim, TurnExecute
from schemas.managed_v3 import (
    ControlEvidenceResult,
    HandoffControlRequest,
    ManagedTurnV3Request,
    ManagedTurnV3Result,
)
from services import turns
from services.managed_client import handoff_request_id, prepare_v3_request
from services.managed_execution_v3 import execute_claimed_turn_v3
from tests.test_handoff import SealedUnknownPort
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_session import sh as session_fixture
from tests.website_common_fixture import common_website, select_turn

sh, suppliers, wh = session_fixture, supplier_fixture, common_website


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
    turn_id = await select_turn(wh)
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
        scope.active, scope.cp_binding_high_water = False, 7
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
                        "integration_id": "8f6ff6f1-2c84-40e1-a28d-049b3b99cafe"}[field]
        forged = prepare_v3_request(ManagedTurnV3Request.model_validate(value))
        turn.managed_v3_snapshot = forged.request.model_dump(mode="json")
        turn.managed_v3_request_fingerprint = forged.request_fingerprint
        turn.managed_v3_dispatch_id = forged.original_dispatch_id
        turn.managed_request_fingerprint = forged.sidecar_fingerprint
    with pytest.raises(DomainError) as caught:
        await execute(wh, turn_id, proof, port)
    assert caught.value.code == ErrorCode.VALIDATION_ERROR
    assert len(port.requests) == 1
