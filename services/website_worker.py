"""Immediate FIFO coordinator. PostgreSQL is the scheduling authority."""
from __future__ import annotations

import asyncio
import time
from uuid import UUID, uuid4

from pydantic import SecretStr
from sqlalchemy import and_, exists, or_, select

from core.config import Settings
from core.errors import DomainError
from core.logging import logger
from db.database import Database
from models import AgentTurn, Conversation, Message, WebsiteAdmission, WebsiteImmediateTurn
from schemas.contracts import (
    ErrorCode,
    Principal,
    TurnClaim,
    TurnExecute,
    TurnFail,
    TurnFailureCode,
    TurnRenew,
)
from services import managed_execution, turns
from services.common import authorized_conversation, db_clock, fingerprint, scope_filter
from services.managed_client import ManagedAIPort


async def select_immediate(database: Database, principal: Principal,
                           settings: Settings) -> UUID | None:
    scopes = or_(*(and_(WebsiteAdmission.owner_id == b.owner_id,
                       WebsiteAdmission.integration_id == b.integration_id,
                       WebsiteAdmission.agent_id == b.agent_id)
                   for b in settings.website_bindings))
    async with database.session_factory() as session, session.begin():
        ids = (await session.scalars(select(Conversation.id).join(
            WebsiteAdmission, WebsiteAdmission.conversation_id == Conversation.id,
        ).where(WebsiteAdmission.route == "MANAGED", scopes, scope_filter(principal),
                or_(~exists(select(WebsiteImmediateTurn.message_id).where(
                    WebsiteImmediateTurn.message_id == WebsiteAdmission.canonical_message_id)),
                    exists(select(AgentTurn.id).where(
                        AgentTurn.conversation_id == Conversation.id,
                        AgentTurn.status.in_(("PENDING", "CLAIMED"))))))
            .distinct().order_by(Conversation.id).limit(100))).all()
        for conversation_id in ids:
            conversation = await authorized_conversation(session, principal, conversation_id,
                                                          lock=True)
            clock = await db_clock(session)
            if conversation.control_mode != "AGENT":
                continue
            active = await session.scalar(select(AgentTurn).where(
                AgentTurn.conversation_id == conversation_id,
                AgentTurn.status.in_(("PENDING", "CLAIMED"))))
            if active is not None:
                selected = await session.scalar(select(WebsiteImmediateTurn).join(
                    WebsiteAdmission,
                    WebsiteAdmission.canonical_message_id == WebsiteImmediateTurn.message_id,
                ).where(WebsiteImmediateTurn.turn_id == active.id, scopes))
                if selected is None:
                    continue
                if (active.status == "CLAIMED" and active.lease_expires_at is not None
                        and active.lease_expires_at > clock):
                    continue
                return active.id
            message = await session.scalar(select(Message).join(
                WebsiteAdmission, WebsiteAdmission.canonical_message_id == Message.id,
            ).where(Message.conversation_id == conversation_id,
                    Message.actor == "CUSTOMER", Message.direction == "INBOUND",
                    scopes, ~exists(select(WebsiteImmediateTurn.message_id).where(
                        WebsiteImmediateTurn.message_id == Message.id)))
                .order_by(Message.sequence).limit(1))
            if message is None:
                continue
            fields = {"contract_version": 1, "conversation_id": str(conversation_id),
                      "selection_key": str(message.id), "input_first_sequence": message.sequence,
                      "input_last_sequence": message.sequence,
                      "context_last_sequence": message.sequence,
                      "expected_input_revision": conversation.input_revision,
                      "expected_authority_revision": conversation.authority_revision}
            turn = AgentTurn(
                id=uuid4(), conversation_id=conversation_id, selection_key=message.id,
                creation_fingerprint=fingerprint(fields), input_first_sequence=message.sequence,
                input_last_sequence=message.sequence, context_last_sequence=message.sequence,
                input_revision=conversation.input_revision,
                authority_revision=conversation.authority_revision, status="PENDING", claim_epoch=0,
                operation_refs=[], created_at=clock,
            )
            session.add(turn)
            await session.flush()
            session.add(WebsiteImmediateTurn(message_id=message.id,
                                             conversation_id=conversation_id, turn_id=turn.id))
            await session.flush()
            return turn.id
    return None


class WebsiteWorker:
    def __init__(self, database: Database, settings: Settings, port: ManagedAIPort) -> None:
        self.database, self.settings, self.port = database, settings, port
        self.last_poll = 0.0
        self.running = False
        entry = next((p for p in settings.auth_principals
                      if p.principal_id == settings.website_worker_principal_id), None)
        self.principal = entry.principal() if entry is not None else None

    async def run_once(self) -> bool:
        if not self.settings.website_worker_enabled or self.principal is None:
            self.last_poll = time.monotonic()
            return False
        turn_id = await select_immediate(self.database, self.principal, self.settings)
        self.last_poll = time.monotonic()
        if turn_id is None:
            return False
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
            while True:
                try:
                    worked = await self.run_once()
                except Exception:
                    logger.warning("", extra={"stage": "turn", "code": "SERVICE_UNAVAILABLE"})
                    worked = False
                if not worked:
                    await asyncio.sleep(self.settings.website_worker_poll_seconds)
        finally:
            self.running = False

    async def ready(self) -> bool:
        return (self.settings.website_worker_enabled and self.principal is not None
                and self.running and time.monotonic() - self.last_poll <
                max(15, self.settings.website_worker_lease_seconds)
                and await self.database.ready())
