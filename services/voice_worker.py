"""Narrow immediate Voice coordinator over Conversation-owned AgentTurns."""

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
    ErrorCode,
    TurnClaim,
    TurnExecute,
    TurnFail,
    TurnFailureCode,
    TurnRenew,
)
from services import managed_execution, turns, voice
from services.managed_client import ManagedAIPort


class VoiceWorker:
    def __init__(self, database: Database, settings: Settings, port: ManagedAIPort) -> None:
        self.database, self.settings, self.port = database, settings, port
        self.last_poll = 0.0
        self.running = False
        self.stop_requested = asyncio.Event()
        entry = next((p for p in settings.auth_principals
                      if p.principal_id == settings.voice_worker_principal_id), None)
        self.principal = entry.principal() if entry is not None else None

    def request_stop(self) -> None:
        self.stop_requested.set()

    async def wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(self.stop_requested.wait(),
                                   timeout=self.settings.voice_worker_poll_seconds)
        except TimeoutError:
            pass

    async def _claim(self) -> tuple[UUID, TurnExecute] | None:
        """Select and claim serially; PostgreSQL remains the fencing authority."""
        if (self.stop_requested.is_set() or not self.settings.voice_worker_enabled
                or self.principal is None):
            self.last_poll = time.monotonic()
            return None
        await voice.expire_calls(self.database, self.principal)
        turn_id = await voice.select_turn(self.database, self.principal, self.settings)
        self.last_poll = time.monotonic()
        if turn_id is None or self.stop_requested.is_set():
            return None
        try:
            claim = await turns.claim_turn(self.database, self.principal, turn_id, TurnClaim(
                worker_ref=self.principal.principal_id,
                lease_seconds=self.settings.voice_worker_lease_seconds,
            ))
        except DomainError as exc:
            if exc.code in {ErrorCode.LEASE_HELD, ErrorCode.TURN_TERMINAL}:
                return None
            raise
        proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                            claim_token=SecretStr(claim.claim_token))
        return turn_id, proof

    async def run_once(self) -> bool:
        """Retain the one-turn entry point used by controlled callers/tests."""
        claimed = await self._claim()
        if claimed is None:
            return False
        await self._run_owned(*claimed)
        return True

    async def _run_owned(self, turn_id: UUID, proof: TurnExecute) -> None:
        """Own execution and renewal until completion or bounded cancellation."""
        execution = asyncio.create_task(self.execute(turn_id, proof),
                                        name="voice-turn-execution")
        renewal = asyncio.create_task(self.renew(turn_id, proof, execution),
                                      name="voice-turn-lease")
        try:
            await execution
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)

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
            await asyncio.sleep(self.settings.voice_worker_lease_seconds / 3)
            if execution.done():
                return
            try:
                await turns.renew_turn(self.database, self.principal, turn_id, TurnRenew(
                    claim_epoch=proof.claim_epoch, claim_token=proof.claim_token,
                    lease_seconds=self.settings.voice_worker_lease_seconds))
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
        """Bound independent executions while retaining serialized admission.

        Stop closes admission first, then drains every acquired claim with its
        renewal. The existing lifecycle owner bounds the total drain deadline.
        No semaphore task is created for work that has not been claimed.
        """
        self.running = True
        active: set[asyncio.Task[None]] = set()
        stopped = asyncio.create_task(self.stop_requested.wait(), name="voice-worker-stop")

        def reap(completed: set[asyncio.Task[None]]) -> None:
            for task in completed:
                active.discard(task)
                try:
                    task.result()
                except (Exception, asyncio.CancelledError):
                    logger.warning("", extra={"stage": "turn", "code": "SERVICE_UNAVAILABLE"})

        try:
            while not self.stop_requested.is_set():
                reap({task for task in active if task.done()})
                if len(active) < self.settings.voice_worker_concurrency:
                    try:
                        claimed = await self._claim()
                    except Exception:
                        logger.warning("", extra={"stage": "turn", "code": "SERVICE_UNAVAILABLE"})
                        claimed = None
                    if claimed is not None:
                        # A claim already committing when stop was requested is
                        # owned work, even though admission is now closed.
                        active.add(asyncio.create_task(
                            self._run_owned(*claimed), name="voice-owned-turn"))
                        continue
                await asyncio.wait(active | {stopped},
                                   timeout=self.settings.voice_worker_poll_seconds,
                                   return_when=asyncio.FIRST_COMPLETED)
            if active:
                await asyncio.gather(*active, return_exceptions=True)
                reap(set(active))
        finally:
            stopped.cancel()
            for task in active:
                task.cancel()
            await asyncio.gather(stopped, *active, return_exceptions=True)
            self.running = False

    async def ready(self) -> bool:
        return (not self.stop_requested.is_set()
                and self.settings.voice_worker_enabled and self.principal is not None
                and self.running and time.monotonic() - self.last_poll <
                max(15, self.settings.voice_worker_lease_seconds)
                and await self.database.ready() and not self.stop_requested.is_set())
