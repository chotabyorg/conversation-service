"""Voice warmup cannot delay admission, create messages, or survive its call."""
# ruff: noqa: F811
import asyncio

import httpx
from sqlalchemy import func, select

from models import AgentTurn, Message
from services.voice_warmup import WarmupClient
from tests.test_voice import AGENT, INTEGRATION, OWNER, post, start, vh  # noqa: F401


class BlockedWarmup:
    def __init__(self):
        self.entered = asyncio.Event()
        self.closed = asyncio.Event()
        self.calls = 0

    async def warm(self, call, binding):
        self.calls += 1
        self.entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.closed.set()


def enable(h):
    controller = h["app"].state.voice_warmups
    controller.settings = h["settings"].model_copy(update={"voice_prompt_warmup_enabled": True})
    controller.port = BlockedWarmup()
    return controller, controller.port


async def test_off_by_default(vh):
    await start(vh)
    assert vh["app"].state.voice_warmups.tasks == {}


async def test_nonblocking_once_and_no_canonical_input(vh):
    controller, port = enable(vh)
    await asyncio.wait_for(start(vh), 1)
    await asyncio.wait_for(port.entered.wait(), 1)
    assert (await start(vh))["replayed"]
    assert port.calls == 1
    async with vh["database"].session_factory() as db:
        assert await db.scalar(select(func.count()).select_from(Message)) == 0
        assert await db.scalar(select(func.count()).select_from(AgentTurn)) == 0
    result = await post(vh, "utterances", sequence=1, text="Real customer input")
    assert result.status_code == 200
    await asyncio.wait_for(port.closed.wait(), 1)
    await asyncio.sleep(0)
    assert not controller.tasks
    assert (await start(vh))["replayed"]
    assert port.calls == 1


async def test_end_cancels_warmup(vh):
    controller, port = enable(vh)
    await start(vh)
    await asyncio.wait_for(port.entered.wait(), 1)
    assert (await post(vh, "calls/end", reason="DISCONNECTED")).status_code == 200
    await asyncio.wait_for(port.closed.wait(), 1)


async def test_shutdown_joins_inflight(vh):
    controller, port = enable(vh)
    await start(vh)
    await asyncio.wait_for(port.entered.wait(), 1)
    await asyncio.wait_for(controller.close(), 1)
    assert port.closed.is_set() and not controller.tasks


async def test_failed_warmup_does_not_break_voice(vh):
    controller, _ = enable(vh)
    class Failed:
        async def warm(self, call, binding):
            raise RuntimeError("private-provider-detail")
    controller.port = Failed()
    await start(vh)
    await asyncio.gather(*controller.tasks.values())
    assert (await post(vh, "utterances", sequence=1, text="hello")).status_code == 200
    assert await vh["worker"].run_once()


async def test_denied_start_never_warms(vh):
    controller, port = enable(vh)
    response = await post(vh, "calls/start", owner_id="other-owner",
                          integration_id=str(INTEGRATION), agent_id=str(AGENT))
    assert response.status_code == 404 and port.calls == 0 and not controller.tasks


async def test_http_warmup_uses_only_trusted_binding_and_no_retries(vh, monkeypatch):
    from schemas.voice import CallStart
    calls = []
    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"untrusted":"ignored"}'
    def handle(request):
        calls.append(request)
        return httpx.Response(503, stream=Body())
    from services.managed_http_owner import NativeHTTPResourceOwner
    monkeypatch.setattr(NativeHTTPResourceOwner, "own_pool", lambda *args: None)
    # MockTransport has no native pool; supply the same acquisition surface.
    transport = httpx.MockTransport(handle)
    transport._pool = None
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", lambda **kwargs: transport)
    client = WarmupClient(vh["settings"])
    await client.warm(CallStart(**vh["identity"], owner_id=OWNER,
        integration_id=INTEGRATION, agent_id=AGENT), vh["settings"].managed_ai_bindings[0])
    import json
    assert len(calls) == 1
    assert calls[0].url.path == "/internal/v1/voice/warmup"
    body = json.loads(calls[0].content)
    assert body["channel"] == "VOICE" and body["agent_id"] == str(AGENT)
    assert "media_session_id" not in body and "messages" not in body
