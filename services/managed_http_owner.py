"""Native HTTP/1 acquisition ownership for the narrow managed AI port.

Inspected ABI: HTTPX0.28.1/httpcore1.0.9 with AnyIO4.15.1 on asyncio.
Retain stream objects before TLS and protocol setup, never bare FD numbers.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import Awaitable, Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import version
from typing import Any

import anyio
from anyio._backends._asyncio import SocketStream
from httpcore._backends.anyio import AnyIOBackend, AnyIOStream
from httpcore._backends.auto import AutoBackend
from httpcore._backends.base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream
from httpcore._synchronization import current_async_library


@dataclass(frozen=True)
class _NativeSocket:
    close: Callable[[], Awaitable[None]]
    transport: asyncio.Transport
    socket: Any


class NativeHTTPResourceOwner:
    def __init__(self) -> None:
        self.sockets: dict[int, _NativeSocket] = {}
        self.active_scope: anyio.CancelScope | None = None
        self.cancel_requested = False
        self.failed = False

    def cancel_operation(self) -> None:
        self.cancel_requested = True
        if self.active_scope is not None:
            self.active_scope.cancel()

    @contextmanager
    def operation(self, seconds: float) -> Iterator[anyio.CancelScope]:
        with anyio.CancelScope(deadline=anyio.current_time() + seconds) as scope:
            self.active_scope = scope
            if self.cancel_requested:
                scope.cancel()
            try:
                yield scope
            finally:
                self.active_scope = None

    def own_pool(self, pool: Any) -> None:
        if (
            current_async_library() != "asyncio"
            or any(version(package) != expected for package, expected in {
                "httpx": "0.28.1", "httpcore": "1.0.9", "anyio": "4.15.1",
            }.items())
            or pool._http2
            or type(pool._network_backend) not in {AutoBackend, AnyIOBackend}
        ):
            raise RuntimeError("Unsupported managed HTTP backend")
        pool._network_backend = _OwnedBackend(pool._network_backend, self)

    def retain(self, stream: Any) -> None:
        while hasattr(stream, "transport_stream"):
            stream = stream.transport_stream
        if not isinstance(stream, SocketStream) or not isinstance(
            stream._transport, asyncio.Transport
        ):
            raise RuntimeError("Unsupported managed HTTP socket")
        self.sockets.setdefault(id(stream), _NativeSocket(
            stream.aclose, stream._transport, stream._transport.get_extra_info("socket")
        ))

    async def aclose(self) -> None:
        # Runs in the retained HTTP task, after operation timeout scopes exit.
        # Both first and repeated caller cancellation only signal the operation;
        # they never cancel this retained task. No operation scope remains here.
        with anyio.CancelScope(shield=True):
            for resource in reversed(list(self.sockets.values())):
                try:
                    await resource.close()
                except (Exception, asyncio.CancelledError):
                    self.failed = True
                finally:
                    # Native aclose skips abort on an already-closing transport.
                    # Finish physical release through the captured ACTUAL owner,
                    # even after a native close's own error/deadline. Never close
                    # a bare descriptor, which could have been reused.
                    resource.transport.abort()
                    while resource.socket.fileno() != -1:
                        await anyio.sleep(0)
        if self.failed:
            raise RuntimeError("Managed HTTP cleanup failed")


class _OwnedStream(AsyncNetworkStream):
    def __init__(self, stream: AsyncNetworkStream, owner: NativeHTTPResourceOwner) -> None:
        if not isinstance(stream, AnyIOStream):
            raise RuntimeError("Unsupported managed HTTP stream")
        self.native, self.owner = stream, owner
        owner.retain(stream._stream)
        self.native_close = stream.aclose
        # Native start_tls's own error close must have the same protection as
        # HTTPcore protocol close, including before a TLS wrapper is returned.
        stream.aclose = self.aclose  # type: ignore[method-assign]

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return await self.native.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self.native.write(buffer, timeout)

    async def aclose(self) -> None:
        failed = False
        with anyio.CancelScope(shield=True, deadline=anyio.current_time() + 2) as scope:
            try:
                await self.native_close()
            except (Exception, asyncio.CancelledError):
                failed = True
        if failed or scope.cancel_called:
            self.owner.failed = True
            raise RuntimeError("Managed HTTP cleanup failed") from None

    async def start_tls(
        self, ssl_context: ssl.SSLContext, server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        stream = await self.native.start_tls(ssl_context, server_hostname, timeout)
        return _OwnedStream(stream, self.owner)

    def get_extra_info(self, info: str) -> Any:
        return self.native.get_extra_info(info)


class _OwnedBackend(AsyncNetworkBackend):
    def __init__(self, native: AsyncNetworkBackend, owner: NativeHTTPResourceOwner) -> None:
        self.native, self.owner = native, owner

    async def connect_tcp(
        self, host: str, port: int, timeout: float | None = None,
        local_address: str | None = None, socket_options: Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        stream = await self.native.connect_tcp(host, port, timeout, local_address, socket_options)
        return _OwnedStream(stream, self.owner)

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None,
        socket_options: Iterable[SOCKET_OPTION] | None = None,
    ) -> AsyncNetworkStream:
        stream = await self.native.connect_unix_socket(path, timeout, socket_options)
        return _OwnedStream(stream, self.owner)

    async def sleep(self, seconds: float) -> None:
        await self.native.sleep(seconds)
