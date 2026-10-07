"""Real Conversation app with an exact test-only runtime network guard."""

import json
import os
import socket
from pathlib import Path


def install_guard(allowlist):
    endpoints = {(host, int(port)) for host, port in allowlist}
    if not endpoints or any(host != "127.0.0.1" for host, _ in endpoints):
        raise RuntimeError("Invalid Goal2 runtime network allowlist")
    original = socket.socket.connect
    original_ex = socket.socket.connect_ex
    resolve = socket.getaddrinfo

    def check(address):
        if (address[0], int(address[1])) not in endpoints:
            raise RuntimeError("Goal2 runtime network destination forbidden")

    def connect(sock, address):
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            check(address)
        elif sock.family == socket.AF_UNIX and address:
            raise RuntimeError("Goal2 named Unix socket forbidden")
        return original(sock, address)

    def connect_ex(sock, address):
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            check(address)
        return original_ex(sock, address)

    def getaddrinfo(host, port, *args, **kwargs):
        check((host, port))
        return resolve(host, port, *args, **kwargs)

    socket.socket.connect = connect
    socket.socket.connect_ex = connect_ex
    socket.getaddrinfo = getaddrinfo


def run():
    config = json.loads(Path(os.environ["GOAL2_BOOTSTRAP_CONFIG"]).read_text())
    candidate = Path.cwd().resolve()
    if (candidate / ".env").exists():
        raise RuntimeError("Goal2 refuses candidate .env configuration")
    install_guard(config["allowlist"])
    denials = 0
    for method in ["connect", "connect_ex"]:
        with socket.socket() as probe:
            try:
                getattr(probe, method)(("203.0.113.1", 443))
            except RuntimeError:
                denials += 1
            else:
                raise RuntimeError("Goal2 runtime guard self-check failed")
    (Path(config["state_dir"]) / "cs-guard.json").write_text(json.dumps({"denials": denials}))
    import uvicorn

    import main
    from core.config import Settings
    from main import create_app

    if Path(main.__file__).resolve().parent != candidate:
        raise RuntimeError("Goal2 imported wrong Conversation candidate")
    listener = socket.fromfd(config["fd"], socket.AF_INET, socket.SOCK_STREAM)
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(Settings(_env_file=None)),
            host="127.0.0.1",
            port=config["port"],
            access_log=False,
            log_level="warning",
            timeout_graceful_shutdown=3,
        )
    )
    server.run(sockets=[listener])


if __name__ == "__main__":
    run()
