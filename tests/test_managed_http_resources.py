"""Real protected execute route, native partial TLS, physical release and no resend."""

import asyncio
import os
import weakref
from contextlib import asynccontextmanager

import pytest
from httpcore._backends.anyio import AnyIOBackend

from services.managed_http_owner import NativeHTTPResourceOwner
from tests.test_managed_client import install_client
from tests.test_managed_execution import execute
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture


@asynccontextmanager
async def stalled_tls(network_allowlist):
    peers, tasks = [], []
    accepted = asyncio.Event()

    async def serve(reader, writer):
        eof = asyncio.Event()
        peers.append(eof)
        tasks.append(asyncio.current_task())
        try:
            hello = await reader.read(4096)
            assert hello and hello[0] == 22
            accepted.set()
            while await reader.read(4096):
                pass
            eof.set()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    network_allowlist.add(("127.0.0.1", port))
    try:
        yield f"https://127.0.0.1:{port}", accepted, peers, tasks
    finally:
        server.close()
        await server.wait_closed()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        network_allowlist.discard(("127.0.0.1", port))


@pytest.mark.parametrize("mode", ["total_deadline", "connect_deadline", "caller_cancel"])
async def test_native_partial_tls_release_precedes_result_or_original_cancel_and_no_resend(
    h, managed, network_allowlist, monkeypatch, mode
):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    streams, close_tasks = [], []
    original_connect = AnyIOBackend.connect_tcp
    original_close = NativeHTTPResourceOwner.aclose
    closed = asyncio.Event()

    async def observe(backend, *args, **kwargs):
        stream = await original_connect(backend, *args, **kwargs)
        streams.append((weakref.ref(stream), stream.get_extra_info("socket").fileno()))
        return stream

    async def owned_close(owner):
        close_tasks.append(asyncio.current_task())
        await original_close(owner)
        closed.set()
        # Make the retained join observable while a repeated caller cancel lands.
        if mode == "caller_cancel":
            await asyncio.sleep(.05)

    monkeypatch.setattr(AnyIOBackend, "connect_tcp", observe)
    monkeypatch.setattr(NativeHTTPResourceOwner, "aclose", owned_close)
    async with stalled_tls(network_allowlist) as (origin, accepted, peers, tasks):
        settings = settings.model_copy(update={
            "managed_ai_connect_seconds": .05 if mode == "connect_deadline" else 5,
        })
        install_client(h, settings, origin, .1 if mode == "total_deadline" else 5)
        before = set(asyncio.all_tasks())
        request = asyncio.create_task(execute(h, turn, claim))
        if mode == "caller_cancel":
            await asyncio.wait_for(accepted.wait(), 1)
            request.cancel("original TLS cancellation")
            await asyncio.wait_for(closed.wait(), 1)
            request.cancel("repeated TLS cancellation")
            with pytest.raises(asyncio.CancelledError) as cancelled:
                await request
            assert cancelled.value.args == ("original TLS cancellation",)
        else:
            response = await request
            assert response.status_code == 200
            assert response.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
            assert response.json()["turn"]["draft_text"] is None
        assert accepted.is_set() and len(peers) == len(streams) == 1
        await asyncio.wait_for(peers[0].wait(), .5)
        await asyncio.gather(*tasks)
        for _, fd in streams:
            with pytest.raises(OSError):
                os.fstat(fd)
        assert len(close_tasks) == 1 and close_tasks[0].get_name() == "managed-ai-http-owner"
        assert h.database.engine.pool.checkedout() == 0
        assert not (set(asyncio.all_tasks()) - before - {asyncio.current_task()})
        repeat = await execute(h, turn, claim)
        assert repeat.status_code == 200 and repeat.json()["replayed"]
        assert repeat.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
        assert len(peers) == 1
