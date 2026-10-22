"""Best-effort Voice call-start hint; independent of turns and text scheduling."""
from __future__ import annotations

import asyncio
from typing import Protocol
from uuid import UUID

import httpx

from core.config import ManagedBinding, Settings
from db.database import Database
from schemas.contracts import Principal
from schemas.voice import CallStart
from services import voice
from services.common import db_clock
from services.managed_http_owner import NativeHTTPResourceOwner


class WarmupPort(Protocol):
    async def warm(self, call: CallStart, binding: ManagedBinding) -> None: ...


class WarmupClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def warm(self, call: CallStart, binding: ManagedBinding) -> None:
        resources = NativeHTTPResourceOwner()
        owner = asyncio.create_task(self._owned(call, binding, resources),
                                    name="voice-warmup-http-owner")
        cancellation = None
        while True:
            try:
                await asyncio.shield(owner)
                break
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
                resources.cancel_operation()
                if owner.done():
                    await asyncio.gather(owner, return_exceptions=True)
                    raise cancellation
            except Exception:
                if cancellation is not None:
                    raise cancellation
                raise
        if cancellation is not None:
            raise cancellation

    async def _owned(self, call: CallStart, binding: ManagedBinding,
                     resources: NativeHTTPResourceOwner) -> None:
        if self.settings.managed_ai_base_url is None:
            return
        transport = httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        resources.own_pool(transport._pool)
        try:
            with resources.operation(3.0):
                async with httpx.AsyncClient(transport=transport, trust_env=False,
                                             follow_redirects=False, timeout=3.0) as client:
                    async with client.stream(
                        "POST", self.settings.managed_ai_base_url + "/internal/v1/voice/warmup",
                        json={"contract_version": 1, "owner_id": binding.owner_id,
                              "integration_id": str(binding.integration_id), "channel": "VOICE",
                              "agent_id": str(binding.agent_id), "call_id": str(call.call_id)},
                        headers={"X-Service-Token": binding.service_token.get_secret_value()},
                    ) as response:
                        # Cache hint only: no result is delivered, persisted or executed.
                        size = 0
                        async for chunk in response.aiter_raw():
                            size += len(chunk)
                            if size > 4096:
                                break
        finally:
            await resources.aclose()


class VoiceWarmups:
    def __init__(self, database: Database, settings: Settings, port: WarmupPort) -> None:
        self.database, self.settings, self.port = database, settings, port
        self.tasks: dict[UUID, asyncio.Task[None]] = {}
        self.closed = False

    def start(self, call: CallStart, principal: Principal) -> None:
        if (self.closed or not self.settings.voice_prompt_warmup_enabled
                or call.integration_id not in self.settings.voice_cohort_integration_ids
                or call.call_id in self.tasks or len(self.tasks) >= 4):
            return
        task = asyncio.create_task(self._run(call, principal), name="voice-call-warmup")
        self.tasks[call.call_id] = task
        task.add_done_callback(lambda done: self.tasks.pop(call.call_id, None))

    async def _run(self, call: CallStart, principal: Principal) -> None:
        try:
            # Recheck after scheduling. Hold no call/database lock across provider I/O.
            async with self.database.session_factory() as session, session.begin():
                conversation, stored = await voice.locked_call(session, principal, call)
                voice.live(self.settings, conversation, stored, await db_clock(session))
                if conversation.control_mode != "AGENT" or stored.last_sequence:
                    return
                binding = next(b for b in self.settings.managed_ai_bindings
                    if b.owner_id == conversation.owner_id
                    and b.integration_id == conversation.integration_id
                    and b.channel == "VOICE" and b.agent_id == stored.agent_id)
            await self.port.warm(call, binding)
        except Exception:
            # A cache miss, unavailable AI or bad configuration cannot break a call.
            pass

    def stop(self, call_id: UUID) -> None:
        task = self.tasks.get(call_id)
        if task is not None and not task.done() and not task.cancelling():
            task.cancel()

    async def close(self) -> None:
        self.closed = True
        tasks = list(self.tasks.values())
        for call_id in list(self.tasks):
            self.stop(call_id)
        await asyncio.gather(*tasks, return_exceptions=True)
