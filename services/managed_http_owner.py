"""Native HTTP/1 acquisition ownership for the narrow managed AI port.

Inspected ABI: HTTPX0.28.1/httpcore1.0.9 with AnyIO4.15.1 on asyncio.
Retain stream objects before TLS and protocol setup, never bare FD numbers.
"""

from __future__ import annotations

import ssl
from collections.abc import Iterable
from importlib.metadata import version
from typing import Any

import anyio
from httpcore._backends.anyio import AnyIOBackend, AnyIOStream
from httpcore._backends.auto import AutoBackend
from httpcore._backends.base import SOCKET_OPTION, AsyncNetworkBackend, AsyncNetworkStream
from httpcore._synchronization import current_async_library


class NativeHTTPResourceOwner:
    def __init__(self) -> None:
        self.sockets: dict[int, Any] = {}

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
        self.sockets.setdefault(id(stream), stream.aclose)

    async def aclose(self) -> None:
        # Runs in the retained HTTP task, after operation timeout scopes exit.
        # Caller cancellation joins this task; repeated cancellation cannot
        # interrupt its cleanup. Final native close has no universal OS bound.
        with anyio.CancelScope(shield=True):
            for close in reversed(list(self.sockets.values())):
                await close()


class _OwnedStream(AsyncNetworkStream):
    def __init__(self, stream: AsyncNetworkStream, owner: NativeHTTPResourceOwner) -> None:
        if not isinstance(stream, AnyIOStream):
            raise RuntimeError("Unsupported managed HTTP stream")
        self.native, self.owner = stream, owner
        owner.retain(stream._stream)

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return await self.native.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self.native.write(buffer, timeout)

    async def aclose(self) -> None:
        await self.native.aclose()

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
