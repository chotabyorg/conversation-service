"""Fresh-environment entry points for the Goal4B process acceptance stack."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys


def install_loopback_guard() -> None:
    allowed = {int(port) for port in json.loads(os.environ["GOAL4B_ALLOWLIST_PORTS"])}
    report = os.environ["GOAL4B_GUARD_REPORT"]
    denials = 0

    def persist(probe_blocked: bool) -> None:
        with open(report, "w", encoding="utf-8") as stream:
            json.dump({"probe_blocked": probe_blocked, "denials": denials}, stream)

    def check(sock: socket.socket, address: object) -> None:
        nonlocal denials
        if sock.family not in {socket.AF_INET, socket.AF_INET6}:
            return
        if not isinstance(address, tuple) or len(address) < 2:
            denials += 1
            persist(False)
            raise PermissionError("Goal4B process network guard denied destination")
        host, port = address[:2]
        if host != "127.0.0.1" or int(port) not in allowed:
            denials += 1
            persist(False)
            raise PermissionError("Goal4B process network guard denied destination")

    original_connect = socket.socket.connect
    original_connect_ex = socket.socket.connect_ex

    def guarded_connect(sock: socket.socket, address: object) -> object:
        check(sock, address)
        return original_connect(sock, address)

    def guarded_connect_ex(sock: socket.socket, address: object) -> object:
        check(sock, address)
        return original_connect_ex(sock, address)

    socket.socket.connect = guarded_connect  # type: ignore[method-assign]
    socket.socket.connect_ex = guarded_connect_ex  # type: ignore[method-assign]
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        try:
            probe.connect(("203.0.113.17", 9))
        except PermissionError:
            persist(True)
        else:
            persist(False)
            raise RuntimeError("Goal4B network guard probe unexpectedly connected")
    finally:
        probe.close()


def run_conversation() -> None:
    import uvicorn

    from main import create_app

    uvicorn.run(create_app(), host="127.0.0.1",
                port=int(os.environ["GOAL4B_SERVICE_PORT"]),
                access_log=False, log_config=None)


def run_worker() -> None:
    import uvicorn

    from core.config import load_settings
    from telegram_worker import create_worker_app

    settings = load_settings()
    uvicorn.run(create_worker_app(settings), host="127.0.0.1",
                port=int(os.environ["GOAL4B_SERVICE_PORT"]),
                access_log=False, log_config=None)


def run_telegram_gateway() -> None:
    import services.bot_manager as bot_manager
    import services.health_server as health_server
    from aiogram import Bot
    from aiogram.client.session.aiohttp import AiohttpSession
    from aiogram.client.telegram import TelegramAPIServer

    api = TelegramAPIServer.from_base(os.environ["GOAL4B_TELEGRAM_API_BASE"])

    class LocalAiohttpSession(AiohttpSession):
        async def make_request(self, bot, method, timeout=None):
            if method.__api_method__ == "sendChatAction":
                timeout = 0.2
            return await super().make_request(bot, method, timeout)

    def local_bot(*, token: str, default: object) -> Bot:
        return Bot(token=token, session=LocalAiohttpSession(api=api), default=default)

    bot_manager.Bot = local_bot  # type: ignore[assignment]
    original_health = health_server.start_health_server

    async def start_local_health(host: str = "0.0.0.0", port: int = 8080):
        return await original_health(host="127.0.0.1", port=port)

    health_server.start_health_server = start_local_health

    from main import main

    asyncio.run(main())


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("expected process kind")
    install_loopback_guard()
    kind = sys.argv[1]
    if kind == "conversation":
        run_conversation()
    elif kind == "worker":
        run_worker()
    elif kind == "telegram":
        run_telegram_gateway()
    else:
        raise SystemExit("unknown process kind")


if __name__ == "__main__":
    main()
