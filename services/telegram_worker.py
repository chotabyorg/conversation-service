"""Narrow immediate Telegram coordinator over Conversation-owned AgentTurns."""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

from pydantic import SecretStr

from core.config import Settings
from core.errors import DomainError
from core.logging import logger
from db.database import Database
from schemas.contracts import (
    Channel,
    ErrorCode,
    TurnClaim,
    TurnExecute,
    TurnFail,
    TurnFailureCode,
    TurnRenew,
)
from services import managed_execution, telegram, turns
from services.managed_client import ManagedAIPort
from services.settling import select_settled_turn


class TelegramWorker:
    def __init__(self, database: Database, settings: Settings, port: ManagedAIPort) -> None:
        self.database, self.settings, self.port = database, settings, port
        self.last_poll = 0.0
        self.running = False
        self.stop_requested = asyncio.Event()
        entry = next((p for p in settings.auth_principals
                      if p.principal_id == settings.telegram_worker_principal_id), None)
        self.principal = entry.principal() if entry is not None else None

    def request_stop(self) -> None:
        self.stop_requested.set()

    async def wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(self.stop_requested.wait(),
                                   timeout=self.settings.telegram_worker_poll_seconds)
        except TimeoutError:
            pass

    async def run_once(self) -> bool:
        if (self.stop_requested.is_set() or not self.settings.telegram_worker_enabled
                or self.principal is None):
            self.last_poll = time.monotonic()
            return False
        turn_id = await select_settled_turn(
            self.database, self.principal, self.settings, (Channel.TELEGRAM,)
        )
        if turn_id is None:
            turn_id = await telegram.select_immediate(
                self.database, self.principal, self.settings
            )
        self.last_poll = time.monotonic()
        if turn_id is None or self.stop_requested.is_set():
            return False
        try:
            claim = await turns.claim_turn(self.database, self.principal, turn_id, TurnClaim(
                worker_ref=self.principal.principal_id,
                lease_seconds=self.settings.telegram_worker_lease_seconds,
            ))
        except DomainError as exc:
            if exc.code in {ErrorCode.LEASE_HELD, ErrorCode.TURN_TERMINAL}:
                return False
            raise
        proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                            claim_token=SecretStr(claim.claim_token))
        execution = asyncio.create_task(self.execute(turn_id, proof),
                                        name="telegram-turn-execution")
        renewal = asyncio.create_task(self.renew(turn_id, proof, execution),
                                      name="telegram-turn-lease")
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
            await managed_execution.execute_claimed_turn(
                self.database, self.principal, turn_id, proof, self.settings, self.port)
        except DomainError as exc:
            if exc.code in {ErrorCode.SERVICE_UNAVAILABLE, ErrorCode.VALIDATION_ERROR}:
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
            await asyncio.sleep(self.settings.telegram_worker_lease_seconds / 3)
            if execution.done():
                return
            try:
                await turns.renew_turn(self.database, self.principal, turn_id, TurnRenew(
                    claim_epoch=proof.claim_epoch, claim_token=proof.claim_token,
                    lease_seconds=self.settings.telegram_worker_lease_seconds))
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
                and self.settings.telegram_worker_enabled and self.principal is not None
                and self.running and time.monotonic() - self.last_poll <
                max(15, self.settings.telegram_worker_lease_seconds)
                and await self.database.ready() and not self.stop_requested.is_set())
