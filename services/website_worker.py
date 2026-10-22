"""Immediate FIFO coordinator. PostgreSQL is the scheduling authority."""
from __future__ import annotations

import asyncio
import time
from uuid import UUID, uuid4

from pydantic import SecretStr
from sqlalchemy import and_, exists, false, or_, select

from core.config import Settings
from core.errors import DomainError
from core.logging import logger
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    Message,
    TurnInputMessage,
    WebsiteAdmission,
    WebsiteImmediateTurn,
)
from schemas.contracts import (
    Channel,
    ErrorCode,
    Principal,
    TurnClaim,
    TurnExecute,
    TurnFail,
    TurnFailureCode,
    TurnRenew,
)
from services import managed_execution, turns
from services.common import (
    authorized_conversation,
    db_clock,
    fingerprint,
    handoff_was_ever_enrolled,
    message_is_consumed,
    scope_filter,
)
from services.history_projection import freeze_website_projection
from services.managed_client import ManagedAIPort
from services.settling import select_settled_turn


async def select_immediate(database: Database, principal: Principal,
                           settings: Settings) -> UUID | None:
    configured = tuple(settings.website_bindings)
    configured_scope = or_(*(and_(Conversation.owner_id == item.owner_id,
                                  Conversation.integration_id == item.integration_id)
                             for item in configured)) if configured else None
    pending_inbound = exists(select(Message.id).where(
        Message.conversation_id == Conversation.id,
        Message.actor == "CUSTOMER", Message.direction == "INBOUND",
        ~message_is_consumed(),
    ))
    marked_recovery = exists(select(AgentTurn.id).join(
        WebsiteImmediateTurn, WebsiteImmediateTurn.turn_id == AgentTurn.id,
    ).where(AgentTurn.conversation_id == Conversation.id,
            AgentTurn.status.in_(("PENDING", "CLAIMED")),
            AgentTurn.managed_dispatch_epoch.is_not(None)))
    active_website_turn = exists(select(AgentTurn.id).join(
        WebsiteImmediateTurn, WebsiteImmediateTurn.turn_id == AgentTurn.id,
    ).where(AgentTurn.conversation_id == Conversation.id,
            AgentTurn.status.in_(("PENDING", "CLAIMED"))))
    candidate_query = select(Conversation.id).where(
        Conversation.channel == Channel.WEBSITE_WIDGET.value,
        scope_filter(principal),
        or_(marked_recovery,
            and_(configured_scope if configured_scope is not None else false(),
                 pending_inbound),
            and_(active_website_turn,
                 configured_scope if configured_scope is not None else false(),
                 Conversation.control_mode == "AGENT")),
    )
    last_candidate_id: UUID | None = None
    while True:
        async with database.session_factory() as session, session.begin():
            candidate_page = candidate_query
            if last_candidate_id is not None:
                candidate_page = candidate_page.where(Conversation.id > last_candidate_id)
            ids = (await session.scalars(
                candidate_page.order_by(Conversation.id).limit(100)
            )).all()
        if not ids:
            return None

        # The UUID cursor is only a scan position within this poll. Every
        # candidate has its own transaction; a typed malformed candidate is
        # rolled back before the keyset scan advances.
        for conversation_id in ids:
            try:
                async with database.session_factory() as session, session.begin():
                    conversation = await authorized_conversation(
                        session, principal, conversation_id, lock=True
                    )
                    handoff_enrolled = await handoff_was_ever_enrolled(session, conversation)
                    active = await session.scalar(select(AgentTurn).where(
                        AgentTurn.conversation_id == conversation_id,
                        AgentTurn.status.in_(("PENDING", "CLAIMED")),
                    ).with_for_update().execution_options(populate_existing=True))
                    clock = await db_clock(session)
                    if active is not None:
                        is_website = await session.scalar(
                            select(WebsiteImmediateTurn.turn_id).where(
                                WebsiteImmediateTurn.turn_id == active.id,
                                WebsiteImmediateTurn.conversation_id == conversation_id,
                            )
                        )
                        if is_website is None:
                            continue
                        if active.managed_dispatch_epoch is None:
                            if handoff_enrolled or conversation.control_mode != "AGENT":
                                continue
                            admission = await session.scalar(
                                select(WebsiteAdmission).join(
                                    WebsiteImmediateTurn,
                                    WebsiteImmediateTurn.message_id
                                    == WebsiteAdmission.canonical_message_id,
                                ).where(
                                    WebsiteImmediateTurn.turn_id == active.id,
                                    WebsiteImmediateTurn.conversation_id == conversation_id,
                                    WebsiteAdmission.conversation_id == conversation_id,
                                    WebsiteAdmission.route == "MANAGED",
                                )
                            )
                            binding = next((item for item in configured
                                            if item.owner_id == conversation.owner_id
                                            and item.integration_id == conversation.integration_id
                                            and admission is not None
                                            and item.agent_id == admission.agent_id), None)
                            if (admission is None
                                    or admission.owner_id != conversation.owner_id
                                    or admission.integration_id != conversation.integration_id
                                    or binding is None
                                    or (active.managed_contract_version == 2
                                        and active.managed_projection_agent_id
                                        != admission.agent_id)):
                                continue
                        if (active.status == "CLAIMED" and active.lease_expires_at is not None
                                and active.lease_expires_at > clock):
                            continue
                        return active.id
                    if handoff_enrolled or conversation.control_mode != "AGENT":
                        continue
                    if (configured_scope is None or not any(
                        item.owner_id == conversation.owner_id
                        and item.integration_id == conversation.integration_id
                        for item in configured
                    )):
                        continue
                    message = await session.scalar(select(Message).where(
                        Message.conversation_id == conversation_id,
                        Message.actor == "CUSTOMER", Message.direction == "INBOUND",
                        ~message_is_consumed(),
                    ).order_by(Message.sequence).limit(1))
                    if message is None or message.settling_burst_id is not None:
                        continue
                    admission = await session.scalar(select(WebsiteAdmission).where(
                        WebsiteAdmission.canonical_message_id == message.id,
                        WebsiteAdmission.conversation_id == conversation_id,
                    ))
                    binding = next((item for item in configured
                                    if item.owner_id == conversation.owner_id
                                    and item.integration_id == conversation.integration_id
                                    and admission is not None
                                    and item.agent_id == admission.agent_id), None)
                    if (admission is None or admission.route != "MANAGED"
                            or admission.owner_id != conversation.owner_id
                            or admission.integration_id != conversation.integration_id
                            or binding is None):
                        # The earliest pending input is a barrier; never bypass it.
                        continue
                    try:
                        session_id_matches = (
                            admission.session_id == UUID(conversation.provider_thread_id)
                        )
                    except ValueError:
                        session_id_matches = False
                    if not session_id_matches:
                        raise DomainError(ErrorCode.VALIDATION_ERROR)
                    fields = {"contract_version": 1, "conversation_id": str(conversation_id),
                              "selection_key": str(message.id),
                              "input_first_sequence": message.sequence,
                              "input_last_sequence": message.sequence,
                              "context_last_sequence": message.sequence,
                              "expected_input_revision": conversation.input_revision,
                              "expected_authority_revision": conversation.authority_revision}
                    turn = AgentTurn(
                        id=uuid4(), conversation_id=conversation_id, selection_key=message.id,
                        creation_fingerprint=fingerprint(fields),
                        input_first_sequence=message.sequence,
                        input_last_sequence=message.sequence,
                        context_last_sequence=message.sequence,
                        input_revision=conversation.input_revision,
                        authority_revision=conversation.authority_revision,
                        settling_revision=conversation.settling_revision,
                        settling_burst_id=None,
                        status="PENDING", claim_epoch=0,
                        operation_refs=[], created_at=clock,
                    )
                    await freeze_website_projection(session, conversation, turn, binding.agent_id)
                    session.add(turn)
                    await session.flush()
                    session.add(WebsiteImmediateTurn(message_id=message.id,
                                                     conversation_id=conversation_id,
                                                     turn_id=turn.id))
                    session.add(TurnInputMessage(message_id=message.id,
                                                 conversation_id=conversation_id,
                                                 turn_id=turn.id,
                                                 sequence=message.sequence, ordinal=0))
                    await session.flush()
                    return turn.id
            except DomainError as exc:
                if exc.code == ErrorCode.VALIDATION_ERROR:
                    continue
                raise

        last_candidate_id = ids[-1]


class WebsiteWorker:
    def __init__(self, database: Database, settings: Settings, port: ManagedAIPort) -> None:
        self.database, self.settings, self.port = database, settings, port
        self.last_poll = 0.0
        self.running = False
        self.stop_requested = asyncio.Event()
        entry = next((p for p in settings.auth_principals
                      if p.principal_id == settings.website_worker_principal_id), None)
        self.principal = entry.principal() if entry is not None else None

    def request_stop(self) -> None:
        """Close local admission while preserving the current owned execution."""
        self.stop_requested.set()

    async def wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(self.stop_requested.wait(),
                                   timeout=self.settings.website_worker_poll_seconds)
        except TimeoutError:
            pass

    async def run_once(self) -> bool:
        if (self.stop_requested.is_set() or not self.settings.website_worker_enabled
                or self.principal is None):
            self.last_poll = time.monotonic()
            return False
        turn_id = await select_settled_turn(
            self.database, self.principal, self.settings, (Channel.WEBSITE_WIDGET,)
        )
        if turn_id is None:
            turn_id = await select_immediate(self.database, self.principal, self.settings)
        self.last_poll = time.monotonic()
        if turn_id is None or self.stop_requested.is_set():
            return False
        # A claim already started before stop remains owned. Complete it with the
        # existing lease and execution path; never abandon a newly committed claim.
        try:
            claim = await turns.claim_turn(self.database, self.principal, turn_id, TurnClaim(
                worker_ref=self.principal.principal_id,
                lease_seconds=self.settings.website_worker_lease_seconds))
        except DomainError as exc:
            if exc.code in {ErrorCode.LEASE_HELD, ErrorCode.TURN_TERMINAL}:
                return False
            raise
        proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                            claim_token=SecretStr(claim.claim_token))
        execution = asyncio.create_task(self.execute(turn_id, proof), name="website-turn-execution")
        renewal = asyncio.create_task(self.renew(turn_id, proof, execution),
                                      name="website-turn-lease")
        try:
            await execution
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        return True

    async def execute(self, turn_id: UUID, proof: TurnExecute) -> None:
        assert self.principal is not None
        try:
            await managed_execution.execute_claimed_turn(self.database, self.principal, turn_id,
                                                         proof, self.settings, self.port)
        except DomainError as exc:
            if exc.code in {ErrorCode.SERVICE_UNAVAILABLE, ErrorCode.VALIDATION_ERROR}:
                # A projection/binding failure before dispatch is known; preserve same Turn.
                from services.common import locked_turn
                async with self.database.session_factory() as session, session.begin():
                    _, turn = await locked_turn(session, self.principal, turn_id)
                    marked = turn.managed_dispatch_epoch is not None
                if not marked:
                    await turns.finish_turn(self.database, self.principal, turn_id, TurnFail(
                        claim_epoch=proof.claim_epoch, claim_token=proof.claim_token,
                        failure_code=TurnFailureCode.INVALID_RESULT, operation_refs=[]))
                    return
            raise

    async def renew(self, turn_id: UUID, proof: TurnExecute,
                    execution: asyncio.Task[None]) -> None:
        assert self.principal is not None
        while not execution.done():
            await asyncio.sleep(self.settings.website_worker_lease_seconds / 3)
            if execution.done():
                return
            try:
                await turns.renew_turn(self.database, self.principal, turn_id, TurnRenew(
                    claim_epoch=proof.claim_epoch, claim_token=proof.claim_token,
                    lease_seconds=self.settings.website_worker_lease_seconds))
                self.last_poll = time.monotonic()
            except DomainError as exc:
                if exc.code == ErrorCode.TURN_TERMINAL:
                    return
                execution.cancel()
                return
            except Exception:
                execution.cancel()
                return

    async def run(self) -> None:
        self.running = True
        try:
            while not self.stop_requested.is_set():
                try:
                    worked = await self.run_once()
                except Exception:
                    logger.warning("", extra={"stage": "turn", "code": "SERVICE_UNAVAILABLE"})
                    worked = False
                if not worked:
                    await self.wait_for_poll()
        finally:
            self.running = False

    async def ready(self) -> bool:
        return (not self.stop_requested.is_set()
                and self.settings.website_worker_enabled and self.principal is not None
                and self.running and time.monotonic() - self.last_poll <
                max(15, self.settings.website_worker_lease_seconds)
                and await self.database.ready() and not self.stop_requested.is_set())
