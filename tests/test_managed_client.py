import asyncio
import json
from contextlib import asynccontextmanager

import pytest

from core.config import Settings
from services.managed_client import MANAGED_PATH, PREEXECUTION_ERRORS, ManagedAIClient
from tests.test_managed_execution import execute
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture


@asynccontextmanager
async def fake_http(network_allowlist, mode, status=200, payload=None):
    tasks, calls = set(), []
    received = asyncio.Event()

    async def connection(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            raw = await reader.readuntil(b"\r\n\r\n")
            lines = raw.decode("ascii").split("\r\n")
            headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
            body = await reader.readexactly(int(headers["Content-Length"]))
            calls.append((lines[0], headers, json.loads(body)))
            received.set()
            if mode == "timeout":
                await asyncio.Event().wait()
            content = payload(calls[-1][2]) if callable(payload) else payload
            encoded = content if isinstance(content, bytes) else json.dumps(content).encode()
            media = "text/plain" if mode == "media" else "application/json"
            extra = "Content-Encoding: gzip\r\n" if mode == "compressed" else ""
            if mode == "redirect":
                extra += "Location: http://127.0.0.1:1/forbidden\r\n"
            if mode == "oversized":
                writer.write(
                    (
                        f"HTTP/1.1 {status} OK\r\nContent-Type: {media}\r\n"
                        "Transfer-Encoding: chunked\r\n\r\n"
                    ).encode()
                )
                for _ in range(5):
                    chunk = b"x" * 65536
                    writer.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                writer.write(b"0\r\n\r\n")
            else:
                length = len(encoded) + (10 if mode == "truncated" else 0)
                writer.write(
                    (
                        f"HTTP/1.1 {status} OK\r\nContent-Type: {media}\r\n{extra}"
                        f"Content-Length: {length}\r\nConnection: close\r\n\r\n"
                    ).encode()
                )
                writer.write(encoded)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            tasks.discard(task)

    server = await asyncio.start_server(connection, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    endpoint = ("127.0.0.1", port)
    network_allowlist.add(endpoint)
    try:
        yield f"http://127.0.0.1:{port}", calls, received
    finally:
        server.close()
        await server.wait_closed()
        pending = list(tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        network_allowlist.discard(endpoint)


def install_client(h, settings, origin, total=2.0):
    settings = Settings(
        **(
            settings.model_dump()
            | {
                "managed_ai_base_url": origin,
                "managed_ai_total_seconds": total,
                "managed_ai_read_seconds": 1.0,
            }
        ),
        _env_file=None,
    )
    h.client._transport.app.state.settings = settings
    h.client._transport.app.state.managed_port = ManagedAIClient(settings)


def success(request):
    from schemas.managed import ECHO_FIELDS

    return dict(
        {name: request[name] for name in ECHO_FIELDS},
        status="DRAFT_READY",
        draft_text="HTTP draft",
        error_code=None,
        operation_refs=[],
    )


@pytest.mark.parametrize(
    "mode",
    [
        "timeout",
        "malformed",
        "oversized",
        "truncated",
        "media",
        "compressed",
        "redirect",
        "server_error",
        "wrong_echo",
        "invalid_shape",
        "duplicate_key",
        "proxy_rejection",
    ],
)
async def test_real_http_untrusted_outcomes_become_unknown_and_never_retry(
    h, managed, network_allowlist, mode, caplog
):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)

    def payload(request):
        value = success(request)
        if mode == "wrong_echo":
            value["input_revision"] += 1
        elif mode == "invalid_shape":
            value["draft_text"] = None
        elif mode == "malformed":
            return b'{"raw":"synthetic-response-secret"'
        elif mode == "duplicate_key":
            return b'{"status":"NO_RESPONSE","status":"DRAFT_READY"}'
        elif mode == "proxy_rejection":
            return {"detail": {"code": "AGENT_NOT_FOUND", "message": "proxy secret"}}
        return value

    status = (
        302
        if mode == "redirect"
        else 503
        if mode == "server_error"
        else (404 if mode == "proxy_rejection" else 200)
    )
    async with fake_http(network_allowlist, mode, status, payload) as (origin, calls, _):
        install_client(h, settings, origin, 0.1 if mode == "timeout" else 2.0)
        result = await execute(h, turn, claim)
        assert result.status_code == 200
        assert result.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
        assert result.json()["turn"]["draft_text"] is None
        repeat = await execute(h, turn, claim)
        assert repeat.json()["replayed"] and len(calls) == 1
        assert calls[0][0] == f"POST {MANAGED_PATH} HTTP/1.1"
        assert (
            calls[0][1]["X-Service-Token"]
            == settings.managed_ai_bindings[0].service_token.get_secret_value()
        )
    assert "synthetic-response-secret" not in caplog.text
    assert "proxy secret" not in caplog.text


@pytest.mark.parametrize("status", list(PREEXECUTION_ERRORS))
async def test_only_exact_known_preexecution_envelope_is_known_failure(
    h, managed, network_allowlist, status
):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    code, message, failure = PREEXECUTION_ERRORS[status]
    async with fake_http(
        network_allowlist, "known", status, {"detail": {"code": code, "message": message}}
    ) as (origin, calls, _):
        install_client(h, settings, origin)
        result = await execute(h, turn, claim)
        assert result.status_code == 200 and result.json()["turn"]["failure_code"] == failure.value
        assert (await execute(h, turn, claim)).json()["replayed"] and len(calls) == 1


async def test_real_http_success_correlated_body_and_sanitized_request_id(
    h, managed, network_allowlist
):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    async with fake_http(network_allowlist, "success", 200, success) as (origin, calls, _):
        install_client(h, settings, origin)
        response = await h.client.post(
            f"/internal/v1/turns/{turn['id']}/execute",
            json=h.proof(claim),
            headers=h.headers() | {"X-Request-ID": "safe-request:1"},
        )
        assert response.status_code == 200 and response.json()["turn"]["draft_text"] == "HTTP draft"
        assert calls[0][1]["X-Request-ID"] == "safe-request:1"
        assert calls[0][2]["turn_id"] == turn["id"]
        assert calls[0][2]["selected_input_message_ids"] == [calls[0][2]["messages"][0]["id"]]
