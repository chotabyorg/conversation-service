"""Read-only opaque effect recovery. Missing references never establish no effect."""
from uuid import UUID

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import AgentTurn, Conversation
from schemas.contracts import ErrorCode, OperationReference, Principal, TurnExecute
from schemas.website import ManagedEvidenceRequest, ManagedEvidenceResult
from services import turns
from services.common import db_clock, locked_turn
from services.managed_client import ManagedAIPort


def binding_for(conversation: Conversation, turn: AgentTurn,
                settings: Settings) -> ManagedBinding | None:
    if conversation.channel != "WEBSITE_WIDGET" or turn.managed_agent_id is None:
        return None
    if not any(b.owner_id == conversation.owner_id
               and b.integration_id == conversation.integration_id
               and b.agent_id == turn.managed_agent_id for b in settings.website_bindings):
        return None
    return next((b for b in settings.managed_ai_bindings
                 if b.scope() == (conversation.owner_id, conversation.integration_id,
                                  conversation.channel) and b.agent_id == turn.managed_agent_id),
                None)


async def read_refs(port: ManagedAIPort, binding: ManagedBinding,
                    request: ManagedEvidenceRequest) -> list[OperationReference]:
    try:
        evidence_method = getattr(port, "evidence", None)
        if evidence_method is None:
            return []
        result = ManagedEvidenceResult.model_validate(
            (await evidence_method(request, binding)).model_dump(mode="json"))
        if (result.turn_id != request.turn_id or result.conversation_id != request.conversation_id
                or result.agent_id != request.agent_id):
            return []
        from schemas.contracts import canonical_operation_refs
        return canonical_operation_refs(result.operation_refs)
    except Exception:
        return []


async def recovery_refs(database: Database, principal: Principal, turn_id: UUID,
                        proof: TurnExecute, settings: Settings,
                        port: ManagedAIPort) -> list[OperationReference]:
    async def snapshot() -> tuple[ManagedBinding, ManagedEvidenceRequest] | None:
        async with database.session_factory() as session, session.begin():
            conversation, turn = await locked_turn(session, principal, turn_id)
            turns.winning_claim(turn, proof.claim_epoch, proof.claim_token.get_secret_value())
            clock = await db_clock(session)
            if (turn.status != "CLAIMED" or turn.lease_expires_at is None
                    or clock >= turn.lease_expires_at):
                raise DomainError(ErrorCode.STALE_CLAIM)
            binding = binding_for(conversation, turn, settings)
            if binding is None:
                return None
            return binding, ManagedEvidenceRequest(
                owner_id=conversation.owner_id, integration_id=conversation.integration_id,
                agent_id=binding.agent_id, conversation_id=conversation.id, turn_id=turn.id)
    captured = await snapshot()
    if captured is None:
        return []
    binding, request = captured
    refs = await read_refs(port, binding, request)
    verified = await snapshot()
    if verified is None or verified[1] != request:
        raise DomainError(ErrorCode.STALE_CLAIM)
    return refs


async def operational_evidence(database: Database, principal: Principal, turn_id: UUID,
                               settings: Settings, port: ManagedAIPort) -> ManagedEvidenceResult:
    if principal.role not in {"FULL", "PROCESSING"}:
        raise DomainError(ErrorCode.NOT_FOUND)
    async with database.session_factory() as session, session.begin():
        conversation, turn = await locked_turn(session, principal, turn_id)
        if not any(g.owner_id == conversation.owner_id
                   and g.integration_id == conversation.integration_id
                   and g.channel.value == "WEBSITE_WIDGET"
                   and g.provider_namespace == "main-chota.website-widget.v1"
                   for g in principal.grants):
            raise DomainError(ErrorCode.NOT_FOUND)
        binding = binding_for(conversation, turn, settings)
        if binding is None or turn.status not in {"COMPLETED", "FAILED"}:
            raise DomainError(ErrorCode.NOT_FOUND)
        request = ManagedEvidenceRequest(owner_id=conversation.owner_id,
                                         integration_id=conversation.integration_id,
                                         agent_id=binding.agent_id,
                                         conversation_id=conversation.id, turn_id=turn.id)
    refs = await read_refs(port, binding, request)
    # Immutable terminal data is untouched. Only the external read result is returned.
    return ManagedEvidenceResult(conversation_id=request.conversation_id, turn_id=turn_id,
                                 agent_id=request.agent_id, operation_refs=refs)
