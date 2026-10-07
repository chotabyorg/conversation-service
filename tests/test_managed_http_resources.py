"""Real protected execute route, native partial TLS, physical release and no resend."""

import asyncio
import json
import os
import socket
import weakref
from contextlib import asynccontextmanager

import pytest
from httpcore._backends.anyio import AnyIOBackend
from sqlalchemy import text

from services.managed_http_owner import NativeHTTPResourceOwner, _OwnedStream
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


@asynccontextmanager
async def unread_http_peer(network_allowlist):
    """A native peer reads headers, then deliberately stops consuming the body."""
    peers, tasks, calls = [], [], []
    accepted = asyncio.Event()
    drain = asyncio.Event()

    async def serve(reader, writer):
        tasks.append(asyncio.current_task())
        disconnected = asyncio.Event()
        peers.append(disconnected)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            calls.append(headers)
            writer.transport.pause_reading()
            accepted.set()
            await drain.wait()
            writer.transport.resume_reading()
            try:
                while await reader.read(65536):
                    pass
            except ConnectionError:
                # An abort may produce reset rather than orderly EOF on some OSes.
                pass
            disconnected.set()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    listener = await asyncio.start_server(serve, "127.0.0.1", 0, limit=4096)
    listener.sockets[0].setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    port = listener.sockets[0].getsockname()[1]
    network_allowlist.add(("127.0.0.1", port))
    try:
        yield f"http://127.0.0.1:{port}", accepted, drain, peers, tasks, calls
    finally:
        listener.close()
        await listener.wait_closed()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        network_allowlist.discard(("127.0.0.1", port))


@pytest.mark.parametrize("phase", ["operation", "final"])
@pytest.mark.parametrize("deadline", ["total", "write"])
@pytest.mark.parametrize("cancel", ["none", "first", "repeated"])
async def test_buffered_native_close_releases_before_result_or_original_cancel(
    h, managed, sql, network_allowlist, monkeypatch, phase, deadline, cancel
):
    await buffered_close_case(
        h, managed, sql, network_allowlist, monkeypatch, phase, deadline, cancel
    )


@pytest.mark.parametrize("cancel", ["first", "repeated"])
async def test_buffered_active_operation_cancellation_releases_before_original_cancel(
    h, managed, sql, network_allowlist, monkeypatch, cancel
):
    await buffered_close_case(
        h, managed, sql, network_allowlist, monkeypatch, "operation", "caller", cancel
    )


@pytest.mark.parametrize("deadline", ["total", "write"])
async def test_already_closing_buffered_native_transport_is_physically_released(
    h, managed, sql, network_allowlist, monkeypatch, deadline
):
    await buffered_close_case(
        h, managed, sql, network_allowlist, monkeypatch, "interrupted", deadline, "none"
    )


async def buffered_close_case(
    h, managed, sql, network_allowlist, monkeypatch, phase, deadline, cancel
):
    import anyio

    settings, _ = managed
    # Each inbound route body remains <=65536B. Canonical text is valid and the
    # escaped 16-message projection approaches, but never exceeds, the 1MiB cap.
    for _ in range(16):
        payload = h.event(text="\x01" * 10000)
        assert len(json.dumps(payload, separators=(",", ":")).encode()) < 65536
        response = await h.call("POST", "/inbound-events", payload)
        assert response.status_code == 201
    accepted_event = response.json()
    turn, _ = await h.select(accepted_event, input_first_sequence=16)
    claim = await h.claim(turn, lease_seconds=300)
    originals = NativeHTTPResourceOwner.retain, NativeHTTPResourceOwner.aclose, _OwnedStream.aclose
    observations, streams, owner_tasks = [], [], []
    caller = None
    final = False
    faulted = False
    cancelled = False

    def retain(owner, stream):
        while hasattr(stream, "transport_stream"):
            stream = stream.transport_stream
        if not streams:
            transport = stream._transport
            native_socket = transport.get_extra_info("socket")
            native_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
            streams.append((weakref.ref(stream), transport, native_socket.fileno()))
            original = stream.aclose

            async def close():
                nonlocal cancelled
                buffered = transport.get_write_buffer_size()
                observations.append((final, buffered, transport.is_closing()))
                owner_tasks.append(asyncio.current_task())
                if buffered and cancel != "none" and not cancelled and deadline != "caller":
                    cancelled = True
                    caller.cancel("original buffered cancellation")
                    if cancel == "repeated":
                        asyncio.get_running_loop().call_soon(
                            caller.cancel, "repeated buffered cancellation"
                        )
                if phase == "interrupted" and not final:
                    # A close's own deadline can interrupt the REAL native
                    # close->checkpoint->abort; no pre-native await is inserted.
                    with anyio.CancelScope(deadline=anyio.current_time()):
                        await original()
                else:
                    await original()

            stream.aclose = close
        originals[0](owner, stream)

    async def final_close(owner):
        nonlocal final
        final = True
        await originals[1](owner)

    async def operation_close(stream):
        nonlocal faulted
        if phase == "final" and not faulted:
            # Native protocol close fails before graceful socket close. The
            # retained final native close is the first to touch buffered bytes.
            faulted = True
            raise RuntimeError("synthetic operation close failure")
        await originals[2](stream)

    monkeypatch.setattr(NativeHTTPResourceOwner, "retain", retain)
    monkeypatch.setattr(NativeHTTPResourceOwner, "aclose", final_close)
    monkeypatch.setattr(_OwnedStream, "aclose", operation_close)
    async with unread_http_peer(network_allowlist) as peer:
        origin, entered, drain, peers, tasks, calls = peer
        settings = settings.model_copy(update={
            "managed_ai_write_seconds": .1 if deadline == "write" else 5,
        })
        install_client(h, settings, origin, .1 if deadline == "total" else 5)
        before = set(asyncio.all_tasks())
        caller = asyncio.create_task(execute(h, turn, claim))
        try:
            if deadline == "caller":
                await asyncio.wait_for(entered.wait(), 1)
                caller.cancel("original buffered cancellation")
                cancelled = True
                if cancel == "repeated":
                    asyncio.get_running_loop().call_soon(
                        caller.cancel, "repeated buffered cancellation"
                    )
            if cancel == "none":
                result = await asyncio.wait_for(caller, 2)
                assert result.status_code == 200
                assert result.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
            else:
                with pytest.raises(asyncio.CancelledError) as caught:
                    await asyncio.wait_for(caller, 2)
                assert caught.value.args == ("original buffered cancellation",)
            assert len(streams) == len(peers) == len(calls) == 1
            assert calls[0].startswith(b"POST /internal/v1/managed-turns/execute ")
            length = next(line for line in calls[0].split(b"\r\n")
                          if line.lower().startswith(b"content-length:"))
            assert 950000 < int(length.split(b":", 1)[1]) <= 1048576
            assert any(buffered > 0 for _, buffered, _ in observations)
            if phase == "interrupted":
                assert any(is_final and buffered > 0 and closing
                           for is_final, buffered, closing in observations)
            if phase == "final":
                assert faulted and observations[0][0]
            for _, transport, fd in streams:
                with pytest.raises(OSError):
                    os.fstat(fd)
                assert transport.get_write_buffer_size() == 0
            assert owner_tasks and len(set(owner_tasks)) == 1
            assert owner_tasks[0].get_name() == "managed-ai-http-owner"
            assert owner_tasks[0].done()
            assert h.database.engine.pool.checkedout() == 0
            # Only after FD/buffer/owner/checkout evidence, resume our peer to
            # observe EOF/reset. Fixture cleanup has not touched either socket.
            drain.set()
            await asyncio.wait_for(peers[0].wait(), 1)
            await asyncio.gather(*tasks)
            assert not (set(asyncio.all_tasks()) - before - {asyncio.current_task()})
            saved = (await h.call("GET", f"/turns/{turn['id']}")).json()
            assert saved["id"] == turn["id"] and saved["failure_code"] == "EXECUTION_UNKNOWN"
            assert saved["draft_text"] is None and saved["operation_refs"] == []
            assert saved["managed_dispatch_epoch"] == claim["turn"]["claim_epoch"]
            replay = await execute(h, turn, claim)
            assert replay.status_code == 200 and replay.json()["replayed"]
            assert replay.json()["turn"] == saved and len(calls) == 1
            async with sql.connect() as connection:
                assert await connection.scalar(text("SELECT count(*) FROM messages")) == 16
                assert await connection.scalar(text("SELECT count(*) FROM delivery_attempts")) == 0
                assert await connection.scalar(
                    text("SELECT count(*) FROM delivery_reconciliations")
                ) == 0
        finally:
            # Release ONLY observed fixture resources, after all release claims.
            for _, transport, _ in streams:
                transport.abort()
            if not caller.done():
                caller.cancel()
                await asyncio.gather(caller, return_exceptions=True)
