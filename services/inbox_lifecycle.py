"""Exact CP/AI proofs and durable owner Inbox grant lifecycle transitions."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, TypeVar, cast
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, ValidationError
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    HandoffCase,
    InboxGrantLifecycleReceipt,
    InboxGrantScope,
    TelegramAdmission,
    TelegramResponseAttempt,
    WebsiteAdmission,
    WebsiteResponseAttempt,
    WebsiteSessionCapability,
)
from schemas.contracts import Channel, ErrorCode
from schemas.handoff import (
    AiAgentProof,
    AiAgentProofRequest,
    AiGrantRevoke,
    AiGrantRevoked,
    CpBindingProof,
    CpBindingProofRequest,
    CpGrantRevoke,
    CpGrantRevoked,
    InboxGrantProvision,
    InboxGrantProvisioned,
)
from services.authority import advance_authority
from services.common import db_clock, fingerprint

ModelT = TypeVar("ModelT", bound=BaseModel)
MAX_PROOF_BYTES = 16_384


def _scope(request: BaseModel) -> dict[str, object]:
    value = request.model_dump(mode="python")
    return {
        "owner_id": value["owner_id"],
        "integration_id": value["integration_id"],
        "channel": value["channel"],
        "agent_id": value["agent_id"],
    }


def _wire_fingerprint(request: BaseModel) -> str:
    return fingerprint(request.model_dump(mode="json"))


def _cohort(settings: Settings, integration_id: UUID, channel: str) -> None:
    ids = (settings.website_handoff_cohort_integration_ids if channel == "WEBSITE_WIDGET"
           else settings.handoff_cohort_integration_ids if channel == "TELEGRAM" else ())
    if integration_id not in ids:
        raise DomainError(ErrorCode.NOT_FOUND)


def _website_provision_policy(settings: Settings, request: InboxGrantProvision) -> None:
    if request.channel != "WEBSITE_WIDGET":
        return
    binding = next((b for b in settings.website_bindings
                    if b.owner_id == request.owner_id and b.integration_id == request.integration_id
                    and b.agent_id == request.agent_id
                    and b.capability_generation == request.expected_binding_generation), None)
    managed = any(b.owner_id == request.owner_id and b.integration_id == request.integration_id
                  and b.agent_id == request.agent_id and b.channel == Channel.WEBSITE_WIDGET
                  for b in settings.managed_ai_bindings)
    if (binding is None or not managed or not settings.website_session_capabilities_enabled
            or not settings.website_worker_enabled
            or settings.settling_policy_for(
                Channel.WEBSITE_WIDGET, request.integration_id) is None):
        raise DomainError(ErrorCode.GRANT_CONFLICT)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


async def _post_proof(
    url: str,
    path: str,
    headers: dict[str, str],
    request: BaseModel,
    response_model: type[ModelT],
) -> ModelT:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(connect=2, read=3, write=2, pool=2),
            trust_env=False,
            follow_redirects=False,
        ) as client:
            async with client.stream(
                "POST",
                url.rstrip("/") + path,
                headers={
                    **headers,
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json=request.model_dump(mode="json"),
            ) as response:
                if response.status_code == 404:
                    raise DomainError(ErrorCode.NOT_FOUND)
                if response.status_code == 409:
                    raise DomainError(ErrorCode.GRANT_CONFLICT)
                if response.status_code != 200:
                    raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                types = response.headers.get_list("content-type")
                encodings = response.headers.get_list("content-encoding")
                if (
                    len(types) != 1
                    or types[0].split(";", 1)[0].strip().lower() != "application/json"
                    or any(value.lower().strip() != "identity" for value in encodings)
                ):
                    raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                raw = bytearray()
                async for chunk in response.aiter_raw():
                    if len(raw) + len(chunk) > MAX_PROOF_BYTES:
                        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                    raw.extend(chunk)
        json.loads(bytes(raw), object_pairs_hook=_unique_object)
        return response_model.model_validate_json(bytes(raw))
    except DomainError:
        raise
    except (httpx.HTTPError, ValueError, ValidationError, TypeError):
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE) from None


async def _current_proofs(
    settings: Settings,
    request: InboxGrantProvision,
) -> tuple[CpBindingProof, AiAgentProof]:
    if (
        settings.cp_handoff_base_url is None
        or settings.ai_handoff_base_url is None
        or settings.inbox_lifecycle_token is None
        or settings.handoff_ai_lifecycle_token is None
    ):
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
    cp_request = CpBindingProofRequest(
        contract_version=1,
        command_id=request.command_id,
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        channel=request.channel,
        agent_id=request.agent_id,
        expected_binding_generation=request.expected_binding_generation,
    )
    ai_request = AiAgentProofRequest(
        contract_version=1,
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        channel=request.channel,
        agent_id=request.agent_id,
    )
    cp = await _post_proof(
        settings.cp_handoff_base_url,
        "/internal/v1/handoff/integration-proof",
        {"X-Inbox-Lifecycle-Token": settings.inbox_lifecycle_token.get_secret_value()},
        cp_request,
        CpBindingProof,
    )
    ai = await _post_proof(
        settings.ai_handoff_base_url,
        "/internal/v1/handoff/agent-proof",
        {"X-Service-Token": settings.handoff_ai_lifecycle_token.get_secret_value()},
        ai_request,
        AiAgentProof,
    )
    if (
        cp.owner_id != request.owner_id
        or cp.integration_id != request.integration_id
        or cp.channel != request.channel
        or cp.agent_id != request.agent_id
        or cp.binding_generation != request.expected_binding_generation
        or ai.owner_id != request.owner_id
        or ai.integration_id != request.integration_id
        or ai.channel != request.channel
        or ai.agent_id != request.agent_id
        or ai.agent_owner_id != cp.account_id
    ):
        raise DomainError(ErrorCode.GRANT_CONFLICT)
    return cp, ai


async def _lock_scope(session: AsyncSession, values: dict[str, object]) -> InboxGrantScope:
    await session.execute(
        insert(InboxGrantScope)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=["owner_id", "integration_id", "channel", "agent_id"]
        )
    )
    scope = cast(InboxGrantScope | None, await session.scalar(
        select(InboxGrantScope)
        .where(*[getattr(InboxGrantScope, key) == value for key, value in values.items()])
        .with_for_update()
        .execution_options(populate_existing=True)
    ))
    if scope is None:
        raise RuntimeError("Inbox grant scope unavailable")
    return scope


async def _prior_receipt(
    session: AsyncSession, values: dict[str, object], command_id: UUID
) -> InboxGrantLifecycleReceipt | None:
    return cast(InboxGrantLifecycleReceipt | None, await session.scalar(
        select(InboxGrantLifecycleReceipt).where(
            InboxGrantLifecycleReceipt.owner_id == values["owner_id"],
            InboxGrantLifecycleReceipt.integration_id == values["integration_id"],
            InboxGrantLifecycleReceipt.channel == values["channel"],
            InboxGrantLifecycleReceipt.agent_id == values["agent_id"],
            InboxGrantLifecycleReceipt.command_id == command_id,
        )
    ))


def _replay(
    receipt: InboxGrantLifecycleReceipt,
    digest: str,
    kind: str,
    model: type[ModelT],
) -> ModelT:
    if receipt.request_fingerprint != digest or receipt.kind != kind:
        raise DomainError(ErrorCode.COMMAND_CONFLICT)
    payload = dict(receipt.acknowledgement)
    payload["replayed"] = True
    return model.model_validate(payload)


async def _preflight(
    database: Database,
    request: InboxGrantProvision | CpGrantRevoke | AiGrantRevoke,
    kind: str,
    model: type[ModelT],
) -> ModelT | None:
    values = _scope(request)
    digest = _wire_fingerprint(request)
    async with database.session_factory() as session:
        prior = await _prior_receipt(session, values, request.command_id)
        if prior is not None:
            return _replay(prior, digest, kind, model)
    return None


async def _record_lifecycle_receipt(
    session: AsyncSession,
    values: dict[str, object],
    command_id: UUID,
    kind: str,
    digest: str,
    acknowledgement: BaseModel,
) -> None:
    session.add(
        InboxGrantLifecycleReceipt(
            id=uuid4(),
            **values,
            command_id=command_id,
            kind=kind,
            request_fingerprint=digest,
            acknowledgement=acknowledgement.model_dump(mode="json"),
        )
    )


async def _has_unjoined_v1_writer(
    session: AsyncSession, request: InboxGrantProvision
) -> bool:
    if request.channel == "WEBSITE_WIDGET":
        # UNKNOWN writes record finished_at too. That timestamp is not positive
        # task-end evidence and cannot authorize a cutover into common authority.
        writer = await session.scalar(select(WebsiteResponseAttempt.id)
            .join(WebsiteAdmission, WebsiteAdmission.id == WebsiteResponseAttempt.admission_id)
            .where(WebsiteAdmission.owner_id == request.owner_id,
                   WebsiteAdmission.integration_id == request.integration_id,
                   WebsiteAdmission.channel == "WEBSITE_WIDGET",
                   WebsiteAdmission.agent_id == request.agent_id,
                   WebsiteResponseAttempt.send_authorized.is_(True),
                   or_(WebsiteResponseAttempt.outcome.is_(None),
                       WebsiteResponseAttempt.outcome == "UNKNOWN"))
            .limit(1))
        return writer is not None
    writer = await session.scalar(
        select(TelegramResponseAttempt.id)
        .join(TelegramAdmission, TelegramAdmission.id == TelegramResponseAttempt.admission_id)
        .where(
            TelegramAdmission.owner_id == request.owner_id,
            TelegramAdmission.integration_id == request.integration_id,
            TelegramAdmission.channel == "TELEGRAM",
            TelegramAdmission.agent_id == request.agent_id,
            TelegramResponseAttempt.send_authorized.is_(True),
            TelegramResponseAttempt.finished_at.is_(None),
        )
        .limit(1)
    )
    return writer is not None


async def _has_active_agent_turn(
    session: AsyncSession, request: InboxGrantProvision
) -> bool:
    """Require old coordinators to quiesce before making the grant active.

    Enrollment and Telegram selectors share scope -> ordered Conversation ->
    AgentTurn locking. A pending or expired legacy selection therefore either
    wins first and blocks provision, or observes the active grant and cannot
    be created as a legacy turn.
    """
    roots = list(
        (
            await session.scalars(
                select(Conversation.id)
                .where(
                    Conversation.owner_id == request.owner_id,
                    Conversation.integration_id == request.integration_id,
                    Conversation.channel == request.channel,
                )
                .order_by(Conversation.id)
                .with_for_update()
            )
        ).all()
    )
    if not roots:
        return False
    active = await session.scalar(
        select(AgentTurn.id)
        .where(
            AgentTurn.conversation_id.in_(roots),
            AgentTurn.status.in_(("PENDING", "CLAIMED")),
            AgentTurn.retired_at.is_(None),
        )
        .order_by(AgentTurn.conversation_id, AgentTurn.id)
        .limit(1)
        .with_for_update()
    )
    return active is not None


async def _has_legacy_human_root_without_case(
    session: AsyncSession, request: InboxGrantProvision
) -> bool:
    """Reject initial cutover of roots already in HUMAN without a Handoff case.

    Such a root has no owner Inbox path back to AGENT. A current OPEN/CLAIMED
    case is the durable evidence that an earlier Handoff owns the human state
    and may be re-enrolled after revocation.
    """
    current_case = (
        select(HandoffCase.id)
        .where(
            HandoffCase.conversation_id == Conversation.id,
            HandoffCase.owner_id == request.owner_id,
            HandoffCase.integration_id == request.integration_id,
            HandoffCase.channel == request.channel,
            HandoffCase.agent_id == request.agent_id,
            HandoffCase.status.in_(("OPEN", "CLAIMED")),
            HandoffCase.authority_revision == Conversation.authority_revision,
        )
        .exists()
    )
    stranded = await session.scalar(
        select(Conversation.id)
        .where(
            Conversation.owner_id == request.owner_id,
            Conversation.integration_id == request.integration_id,
            Conversation.channel == request.channel,
            Conversation.control_mode != "AGENT",
            or_(Conversation.control_mode != "HUMAN", ~current_case),
        )
        .order_by(Conversation.id)
        .limit(1)
    )
    return stranded is not None


async def provision_grant(
    database: Database,
    settings: Settings,
    request: InboxGrantProvision,
) -> InboxGrantProvisioned:
    _cohort(settings, request.integration_id, request.channel)
    kind = "PROVISION"
    digest = _wire_fingerprint(request)
    replay = await _preflight(database, request, kind, InboxGrantProvisioned)
    if replay is not None:
        return replay
    _website_provision_policy(settings, request)
    cp, ai = await _current_proofs(settings, request)
    values = _scope(request)
    async with database.session_factory() as session, session.begin():
        scope = await _lock_scope(session, values)
        prior = await _prior_receipt(session, values, request.command_id)
        if prior is not None:
            result = _replay(prior, digest, kind, InboxGrantProvisioned)
        else:
            if (
                scope.cp_binding_high_water is not None
                and cp.binding_generation <= scope.cp_binding_high_water
            ) or (
                scope.agent_lifecycle_high_water is not None
                and ai.agent_lifecycle_generation <= scope.agent_lifecycle_high_water
            ):
                raise DomainError(ErrorCode.GRANT_CONFLICT)
            if await _has_active_agent_turn(session, request):
                raise DomainError(ErrorCode.ACTIVE_TURN_EXISTS)
            if await _has_legacy_human_root_without_case(session, request):
                raise DomainError(ErrorCode.HANDOFF_CONFLICT)
            if await _has_unjoined_v1_writer(session, request):
                raise DomainError(ErrorCode.WEBSITE_WRITER_LIVE
                                  if request.channel == "WEBSITE_WIDGET"
                                  else ErrorCode.TELEGRAM_WRITER_LIVE)
            if scope.active and (
                scope.account_id != cp.account_id
                or scope.agent_owner_id != ai.agent_owner_id
                or scope.cp_binding_generation != cp.binding_generation
                or scope.agent_lifecycle_generation != ai.agent_lifecycle_generation
            ):
                raise DomainError(ErrorCode.GRANT_CONFLICT)
            clock = await db_clock(session)
            if not scope.active:
                scope.grant_generation += 1
            scope.account_id = cp.account_id
            scope.agent_owner_id = ai.agent_owner_id
            scope.cp_binding_generation = cp.binding_generation
            scope.agent_lifecycle_generation = ai.agent_lifecycle_generation
            scope.active = True
            scope.updated_at = clock
            ack = InboxGrantProvisioned(
                contract_version=1,
                command_id=request.command_id,
                account_id=cp.account_id,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                binding_generation=cp.binding_generation,
                agent_owner_id=ai.agent_owner_id,
                agent_lifecycle_generation=ai.agent_lifecycle_generation,
                grant_generation=max(scope.grant_generation, 1),
                active=True,
                request_fingerprint=digest,
                replayed=False,
            )
            if scope.grant_generation < 1:
                scope.grant_generation = 1
            session.add(scope)
            await _record_lifecycle_receipt(session, values, request.command_id, kind, digest, ack)
            result = ack
    return result


def _revoke_kind(request: CpGrantRevoke | AiGrantRevoke) -> str:
    return (
        "REVOKE_CONTROL_PLANE"
        if request.lifecycle_source == "CONTROL_PLANE_BINDING"
        else "REVOKE_AI_AGENT"
    )


async def _fence_grant_outbound(
    session: AsyncSession, scope: InboxGrantScope, clock: datetime
) -> None:
    # Include every historical case when collecting roots. A RETURNED case may
    # still own an unacquired HUMAN intent, which must be fenced by revocation.
    roots = set(
        (
            await session.scalars(
                select(HandoffCase.conversation_id).where(
                    HandoffCase.owner_id == scope.owner_id,
                    HandoffCase.integration_id == scope.integration_id,
                    HandoffCase.channel == scope.channel,
                    HandoffCase.agent_id == scope.agent_id,
                )
            )
        ).all()
    )
    admission_model = WebsiteAdmission if scope.channel == "WEBSITE_WIDGET" else TelegramAdmission
    admissions = (
        await session.scalars(
            select(admission_model.conversation_id).where(
                admission_model.owner_id == scope.owner_id,
                admission_model.integration_id == scope.integration_id,
                admission_model.channel == scope.channel,
                admission_model.agent_id == scope.agent_id,
                admission_model.conversation_id.is_not(None),
            )
        )
    ).all()
    roots.update(root for root in admissions if root is not None)
    if scope.channel == "WEBSITE_WIDGET":
        roots.update(await session.scalars(select(WebsiteSessionCapability.conversation_id).where(
            WebsiteSessionCapability.owner_id == scope.owner_id,
            WebsiteSessionCapability.integration_id == scope.integration_id,
            WebsiteSessionCapability.channel == scope.channel,
            WebsiteSessionCapability.agent_id == scope.agent_id,
        )))
    if not roots:
        return
    conversations = (
        await session.scalars(
            select(Conversation)
            .where(Conversation.id.in_(roots))
            .order_by(Conversation.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    case_rows = (
        await session.scalars(
            select(HandoffCase)
            .where(
                HandoffCase.conversation_id.in_([row.id for row in conversations]),
                HandoffCase.owner_id == scope.owner_id,
                HandoffCase.integration_id == scope.integration_id,
                HandoffCase.channel == scope.channel,
                HandoffCase.agent_id == scope.agent_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    conversation_by_id = {row.id: row for row in conversations}
    for conversation in conversations:
        # This is the Goal5 authority transition: existing permits remain
        # immutable, while every new eligibility/AGENT send sees a new epoch.
        advance_authority(conversation)
        conversation.handoff_resume_floor_sequence = conversation.last_message_sequence
        if conversation.control_mode == "HUMAN_PENDING":
            conversation.control_mode = "HUMAN"
        conversation.updated_at = clock
    for case in case_rows:
        case_conversation = conversation_by_id.get(case.conversation_id)
        if case_conversation is None:
            continue
        case.authority_revision = case_conversation.authority_revision
        if case.status == "CLAIMED":
            case.status = "OPEN"
            case.assigned_account_id = None
            case.assignment_revision += 1
        case.case_revision += 1
        case.updated_at = clock

    root_ids = [row.id for row in conversations]
    turns = (
        await session.scalars(
            select(AgentTurn)
            .where(
                AgentTurn.conversation_id.in_(root_ids),
                AgentTurn.status.in_(("PENDING", "CLAIMED")),
                AgentTurn.retired_at.is_(None),
            )
            .order_by(AgentTurn.conversation_id, AgentTurn.id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    for turn in turns:
        turn.retired_at = clock

    case_ids = [case.id for case in case_rows]
    intent_sources = [DeliveryAttempt.source_turn_id.is_not(None)]
    if case_ids:
        intent_sources.append(DeliveryAttempt.case_id.in_(case_ids))
    intents = (
        await session.scalars(
            select(DeliveryAttempt)
            .where(
                DeliveryAttempt.conversation_id.in_(root_ids),
                or_(*intent_sources),
                DeliveryAttempt.writer_state == "NONE",
                DeliveryAttempt.status == "GENERATED",
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).all()
    for intent in intents:
        intent.status = "SUPERSEDED"
        intent.revision += 1
        intent.dispatch_requested_at = clock
        intent.updated_at = clock


async def revoke_grant(
    database: Database,
    settings: Settings,
    request: CpGrantRevoke | AiGrantRevoke,
) -> CpGrantRevoked | AiGrantRevoked:
    if (request.channel == "WEBSITE_WIDGET"
            and request.integration_id not in settings.website_handoff_cohort_integration_ids):
        # A config rollback cannot strand an existing Website lifecycle epoch.
        # The authenticated supplier may still revoke only its exact retained scope.
        async with database.session_factory() as session:
            retained = await session.scalar(select(InboxGrantScope.owner_id).where(
                *[getattr(InboxGrantScope, key) == value for key, value in _scope(request).items()]
            ))
        if retained is None:
            raise DomainError(ErrorCode.NOT_FOUND)
    else:
        _cohort(settings, request.integration_id, request.channel)
    kind = _revoke_kind(request)
    digest = _wire_fingerprint(request)
    ack_model: type[CpGrantRevoked] | type[AiGrantRevoked]
    ack_model = CpGrantRevoked if isinstance(request, CpGrantRevoke) else AiGrantRevoked
    replay = await _preflight(database, request, kind, ack_model)
    if replay is not None:
        return replay
    values = _scope(request)
    async with database.session_factory() as session, session.begin():
        scope = await _lock_scope(session, values)
        prior = await _prior_receipt(session, values, request.command_id)
        if prior is not None:
            result = _replay(prior, digest, kind, ack_model)
        else:
            already_fenced = False
            if isinstance(request, CpGrantRevoke):
                source = request.source_binding_generation
                target = request.target_binding_generation
                high_water = scope.cp_binding_high_water
                already_fenced = (
                    not scope.active and high_water is not None and high_water >= target
                )
                if not already_fenced and (
                    target <= (high_water if high_water is not None else -1)
                    or (
                        scope.cp_binding_generation is not None
                        and scope.cp_binding_generation > source
                    )
                ):
                    raise DomainError(ErrorCode.GRANT_CONFLICT)
                if not already_fenced:
                    scope.cp_binding_high_water = target
                source_binding_generation = source
                target_binding_generation = target
            else:
                source = request.source_agent_lifecycle_generation
                target = request.target_agent_lifecycle_generation
                high_water = scope.agent_lifecycle_high_water
                if (
                    target <= (high_water if high_water is not None else -1)
                    or (
                        scope.agent_lifecycle_generation is not None
                        and scope.agent_lifecycle_generation > source
                    )
                    or (
                        scope.agent_owner_id is not None
                        and scope.agent_owner_id != request.agent_owner_id
                    )
                ):
                    raise DomainError(ErrorCode.GRANT_CONFLICT)
                scope.agent_lifecycle_high_water = target
                agent_owner_id = request.agent_owner_id
                source_agent_lifecycle_generation = source
                target_agent_lifecycle_generation = target
            if not already_fenced:
                clock = await db_clock(session)
                scope.active = False
                # The source high-water transition is itself an effective revoke even
                # when no Conversation or active grant existed yet.
                scope.grant_generation += 1
                scope.updated_at = clock
                await _fence_grant_outbound(session, scope, clock)
            if isinstance(request, CpGrantRevoke):
                ack: CpGrantRevoked | AiGrantRevoked = CpGrantRevoked(
                    contract_version=1,
                    command_id=request.command_id,
                    owner_id=request.owner_id,
                    integration_id=request.integration_id,
                    channel=request.channel,
                    agent_id=request.agent_id,
                    grant_generation=scope.grant_generation,
                    active=False,
                    request_fingerprint=digest,
                    replayed=False,
                    lifecycle_source="CONTROL_PLANE_BINDING",
                    source_binding_generation=source_binding_generation,
                    target_binding_generation=target_binding_generation,
                    binding_generation_fence=scope.cp_binding_high_water,
                )
            else:
                ack = AiGrantRevoked(
                    contract_version=1,
                    command_id=request.command_id,
                    owner_id=request.owner_id,
                    integration_id=request.integration_id,
                    channel=request.channel,
                    agent_id=request.agent_id,
                    grant_generation=scope.grant_generation,
                    active=False,
                    request_fingerprint=digest,
                    replayed=False,
                    lifecycle_source="AI_AGENT",
                    agent_owner_id=agent_owner_id,
                    source_agent_lifecycle_generation=source_agent_lifecycle_generation,
                    target_agent_lifecycle_generation=target_agent_lifecycle_generation,
                )
            await _record_lifecycle_receipt(session, values, request.command_id, kind, digest, ack)
            result = ack
    return result
