"""Goal4B end-to-end acceptance across Telegram, Conversation, worker and PostgreSQL.

Run explicitly with the command in README.md. Ordinary unit-test discovery does
not collect this file because it requires separate disposable processes.
"""

from __future__ import annotations

import asyncio
import json
import os
import secrets
import socket
import subprocess
from collections import defaultdict, deque
from dataclasses import dataclass, field
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from time import time
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

CS_ROOT = Path(__file__).resolve().parents[2]
ROOT = Path(__file__).resolve().parent
BOOTSTRAP = ROOT / "runtime_bootstrap.py"
TG_ROOT = Path(os.environ.get(
    "GOAL4B_TELEGRAM_REPO", str(CS_ROOT.parent / "telegram-gateway-service")
)).resolve()
TG_PYTHON = Path(os.environ.get("GOAL4B_TELEGRAM_PYTHON", str(TG_ROOT / ".venv/bin/python")))
CS_PYTHON = CS_ROOT / ".venv/bin/python"
EVIDENCE: list[dict[str, object]] = []


@pytest.fixture(scope="session")
def telegram_runtime() -> tuple[Path, Path]:
    if (not TG_ROOT.is_absolute() or not (TG_ROOT / "main.py").is_file()
            or not (TG_ROOT / "bot/handlers.py").is_file() or not TG_PYTHON.is_file()
            or not CS_PYTHON.is_file()):
        pytest.fail("Explicit Telegram candidate, pinned runtimes and clean process env required")
    return TG_ROOT, TG_PYTHON


@pytest.fixture
def process_ports() -> dict[str, int]:
    names = ("fake", "conversation", "worker", "gateway")
    listeners: list[socket.socket] = []
    ports: dict[str, int] = {}
    try:
        for name in names:
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
            ports[name] = int(listener.getsockname()[1])
    finally:
        for listener in listeners:
            listener.close()
    return ports


@pytest.fixture
def network_allowlist(network_allowlist, process_ports):
    network_allowlist.update(("127.0.0.1", port) for port in process_ports.values())
    return network_allowlist


class FakePeer:
    """Small HTTP/1.1 fake for Control Plane, Managed AI, legacy AI and Telegram."""

    def __init__(self, integration_rows: list[dict[str, object]], cp_token: str,
                 managed_token: str, legacy_token: str) -> None:
        self.integration_rows = integration_rows
        self.cp_token = cp_token
        self.managed_token = managed_token
        self.legacy_token = legacy_token
        self.server: asyncio.AbstractServer | None = None
        self.base_url = ""
        self.updates: dict[str, deque[dict[str, object]]] = defaultdict(deque)
        self.forced_updates: dict[str, deque[dict[str, object]]] = defaultdict(deque)
        self.managed_calls: list[dict[str, object]] = []
        self.legacy_calls: list[dict[str, object]] = []
        self.provider_calls: list[dict[str, object]] = []
        self.accepted_messages: list[dict[str, object]] = []
        self.get_updates_calls: dict[str, int] = defaultdict(int)
        self.send_modes: deque[str] = deque()
        self.managed_block = False
        self.managed_release = asyncio.Event()
        self.managed_release.set()
        self.managed_entered = asyncio.Event()
        self.send_entered = asyncio.Event()
        self.send_release = asyncio.Event()
        self.send_release.set()
        self.provider_message_id = 71000
        self.request_errors: list[str] = []

    async def start(self, port: int) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        self.base_url = f"http://127.0.0.1:{port}"

    async def close(self) -> None:
        self.managed_release.set()
        self.send_release.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _reply(self, writer: asyncio.StreamWriter, value: object,
                     status: int = 200) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        reason = "OK" if status == 200 else "Unauthorized"
        writer.write((f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
                      f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
                     + body)
        await writer.drain()

    async def _read_payload(self, reader: asyncio.StreamReader,
                            headers: dict[str, str]) -> dict[str, object]:
        if "chunked" in headers.get("transfer-encoding", "").lower():
            chunks = bytearray()
            while True:
                size_line = await reader.readline()
                size = int(size_line.split(b";", 1)[0].strip(), 16)
                if size == 0:
                    while await reader.readline() not in {b"\r\n", b"\n", b""}:
                        pass
                    break
                chunks.extend(await reader.readexactly(size))
                await reader.readexactly(2)
            raw = bytes(chunks)
        else:
            content_length = int(headers.get("content-length", "0"))
            raw = await reader.readexactly(content_length) if content_length else b"{}"
        content_type = headers.get("content-type", "application/json")
        if "multipart/form-data" in content_type:
            message = BytesParser(policy=default).parsebytes(
                f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode()
                + raw
            )
            payload: dict[str, object] = {}
            for part in message.iter_parts():
                options = part.get_params(header="content-disposition", unquote=True) or []
                field = next((value for name, value in options if name == "name"), None)
                if field is not None:
                    value = part.get_payload(decode=True) or b""
                    payload[str(field)] = value.decode("utf-8")
            return payload
        if "application/x-www-form-urlencoded" in content_type:
            return {key: values[0] for key, values in parse_qs(raw.decode()).items()}
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
            request_line = head.split(b"\r\n", 1)[0].decode("ascii")
            method, target, _ = request_line.split(" ", 2)
            headers: dict[str, str] = {}
            for line in head.split(b"\r\n")[1:]:
                if b":" in line:
                    key, value = line.split(b":", 1)
                    headers[key.decode("ascii").lower()] = value.decode("latin1").strip()
            payload = await self._read_payload(reader, headers)
            path = urlsplit(target).path

            if path == "/v1/internal/integrations":
                if headers.get("x-internal-token") != self.cp_token:
                    await self._reply(writer, {"detail": "unauthorized"}, 401)
                else:
                    await self._reply(writer, self.integration_rows)
                return
            if path == "/internal/v1/managed-turns/execute":
                if headers.get("x-service-token") != self.managed_token:
                    await self._reply(
                        writer,
                        {"detail": {"code": "UNAUTHENTICATED",
                                    "message": "Managed authentication required."}},
                        401,
                    )
                    return
                self.managed_calls.append(payload)
                self.managed_entered.set()
                if self.managed_block:
                    await self.managed_release.wait()
                    self.managed_block = False
                result = {key: payload[key] for key in (
                    "contract_version", "conversation_id", "turn_id", "agent_id",
                    "input_revision", "authority_revision", "input_first_sequence",
                    "input_last_sequence", "context_last_sequence",
                )}
                result.update(status="DRAFT_READY", draft_text="Hello **managed** `Telegram`",
                              error_code=None, operation_refs=[])
                await self._reply(writer, result)
                return
            if path == "/v1/chat/completions":
                if headers.get("x-service-token") != self.legacy_token:
                    await self._reply(writer, {"detail": "unauthorized"}, 401)
                    return
                self.legacy_calls.append(payload)
                await self._reply(writer, {"session_id": "fixture-session",
                                           "answer": "Hello from legacy"})
                return
            if path.startswith("/bot"):
                token, method_name = path[4:].split("/", 1)
                self.provider_calls.append({"method": method_name, "token": token,
                                            "payload": payload})
                if method_name == "getMe":
                    bot_id = int(token.split(":", 1)[0])
                    await self._reply(writer, {"ok": True, "result": {
                        "id": bot_id, "is_bot": True, "first_name": "Fixture Bot",
                        "username": f"fixture_{bot_id}_bot"}})
                    return
                if method_name == "getUpdates":
                    offset = int(payload.get("offset", 0))
                    queue = self.updates[token]
                    while queue and int(queue[0]["update_id"]) < offset:
                        queue.popleft()
                    forced = self.forced_updates[token]
                    updates = [forced.popleft()] if forced else list(queue)[:100]
                    self.get_updates_calls[token] += 1
                    if not updates:
                        await asyncio.sleep(0.05)
                    await self._reply(writer, {"ok": True, "result": updates})
                    return
                if method_name == "sendChatAction":
                    await self._reply(writer, {"ok": True, "result": True})
                    return
                if method_name == "sendMessage":
                    mode = self.send_modes.popleft() if self.send_modes else "success"
                    chat_id = int(payload["chat_id"])
                    record = {"token": token, "chat_id": chat_id,
                              "text": payload.get("text"),
                              "parse_mode": payload.get("parse_mode"), "mode": mode}
                    if mode == "reject":
                        record["accepted"] = False
                        self.provider_calls[-1]["send_record"] = record
                        await self._reply(writer, {"ok": False, "error_code": 400,
                                                   "description": "fixture rejection"})
                        return
                    self.provider_message_id += 1
                    record["accepted"] = True
                    record["provider_message_id"] = self.provider_message_id
                    self.accepted_messages.append(record)
                    self.provider_calls[-1]["send_record"] = record
                    if mode == "accept_drop":
                        writer.close()
                        await writer.wait_closed()
                        return
                    if mode == "hold_after_accept":
                        self.send_entered.set()
                        await self.send_release.wait()
                        if writer.is_closing():
                            return
                    chat_type = "private" if chat_id > 0 else "group"
                    await self._reply(writer, {"ok": True, "result": {
                        "message_id": self.provider_message_id, "date": int(time()),
                        "chat": {"id": chat_id, "type": chat_type,
                                 "first_name": "Fixture User" if chat_type == "private" else None,
                                 "title": "Fixture Group" if chat_type == "group" else None},
                        "from": {"id": int(token.split(":", 1)[0]), "is_bot": True,
                                 "first_name": "Fixture Bot"},
                        "text": payload.get("text", "")}})
                    return
            self.request_errors.append(f"unhandled fixture path {method} {path}")
            await self._reply(writer, {"detail": "not found"}, 404)
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
            pass
        except Exception as exc:  # Fixture failures become visible to the parent test.
            method_hint = path.rsplit("/", 1)[-1] if "path" in locals() else "request"
            self.request_errors.append(f"{method_hint}:{type(exc).__name__}")
            if not writer.is_closing():
                try:
                    await self._reply(writer, {"detail": "fixture error"}, 500)
                except (ConnectionError, BrokenPipeError):
                    pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    def enqueue(self, token: str, update: dict[str, object], *, force: bool = False,
                duplicate: bool = False) -> None:
        if force:
            self.forced_updates[token].append(update)
            return
        self.updates[token].append(update)
        if duplicate:
            self.updates[token].append(dict(update))

    async def wait_for(self, predicate, label: str, timeout: float = 12.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.025)
        pytest.fail(f"Goal4B acceptance timed out waiting for {label}; "
                    f"fixture errors={self.request_errors!r}")


@dataclass
class TelegramHarness:
    root: Path
    database_url: str = field(repr=False)
    ports: dict[str, int]
    ids: dict[str, str]
    tokens: dict[str, str] = field(repr=False)
    peer: FakePeer = field(repr=False)
    client: httpx.AsyncClient = field(repr=False)
    processes: dict[str, subprocess.Popen] = field(default_factory=dict, repr=False)
    log_handles: list[object] = field(default_factory=list, repr=False)
    cohort_enabled: bool = True
    db: asyncpg.Pool | None = field(default=None, repr=False)

    def url(self, name: str) -> str:
        port = self.peer_port if name == "fake" else self.ports[name]
        return f"http://127.0.0.1:{port}"

    @property
    def peer_port(self) -> int:
        return int(urlsplit(self.peer.base_url).port or 0)

    def env(self, name: str) -> dict[str, str]:
        allowed_ports = [self.peer_port, *self.ports.values(),
                         int(make_url(self.database_url).port or 5432)]
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(CS_ROOT if name != "gateway" else TG_ROOT),
            "PYTHONUNBUFFERED": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "GOAL4B_SERVICE_PORT": str(self.ports[name]),
            "GOAL4B_ALLOWLIST_PORTS": json.dumps(allowed_ports),
            "GOAL4B_GUARD_REPORT": str(self.root / f"{name}-guard.json"),
        }
        grants = [
            {"owner_id": self.ids["owner"], "integration_id": self.ids["canary"],
             "channel": "TELEGRAM", "provider_namespace": "main-chota.telegram.v1"},
            {"owner_id": self.ids["owner"], "integration_id": self.ids["legacy"],
             "channel": "TELEGRAM", "provider_namespace": "main-chota.telegram.v1"},
        ]
        bindings = [
            {"owner_id": self.ids["owner"], "integration_id": self.ids["canary"],
             "channel": "TELEGRAM", "agent_id": self.ids["canary_agent"],
             "service_token": self.tokens["managed"]},
            {"owner_id": self.ids["owner"], "integration_id": self.ids["legacy"],
             "channel": "TELEGRAM", "agent_id": self.ids["legacy_agent"],
             "service_token": self.tokens["managed"]},
        ]
        if name in {"conversation", "worker"}:
            principals = [
                {"principal_id": "goal4b-telegram", "role": "TELEGRAM",
                 "token": self.tokens["conversation_gateway"], "grants": grants},
                {"principal_id": "goal4b-worker", "role": "TELEGRAM_PROCESSING",
                 "token": self.tokens["worker"], "grants": grants},
            ]
            env.update({
                "DATABASE_URL": self.database_url,
                "CONVERSATION_AUTH_PRINCIPALS": json.dumps(principals),
                "MANAGED_AI_BINDINGS": json.dumps(bindings),
                "MANAGED_AI_BASE_URL": self.peer.base_url,
                "TELEGRAM_CANARY_INTEGRATION_IDS": json.dumps(
                    [self.ids["canary"]] if self.cohort_enabled else []),
                "TELEGRAM_WORKER_ENABLED": "true",
                "TELEGRAM_WORKER_PRINCIPAL_ID": "goal4b-worker",
                "TELEGRAM_WORKER_LEASE_SECONDS": "5",
                "TELEGRAM_WORKER_POLL_SECONDS": "0.1",
                "TELEGRAM_WORKER_HEALTH_PORT": str(self.ports["worker"]),
                "LOG_LEVEL": "INFO",
            })
        elif name == "gateway":
            telegram_bindings = [
                {"owner_id": self.ids["owner"], "integration_id": self.ids["canary"],
                 "agent_id": self.ids["canary_agent"]},
                {"owner_id": self.ids["owner"], "integration_id": self.ids["legacy"],
                 "agent_id": self.ids["legacy_agent"]},
            ]
            env.update({
                "AI_AGENT_SERVICE_URL": self.peer.base_url,
                "CONTROL_PLANE_URL": self.peer.base_url,
                "INTERNAL_API_TOKEN": self.tokens["control_plane"],
                "CHAT_S2S_TOKEN": self.tokens["legacy"],
                "CONVERSATION_SERVICE_URL": self.url("conversation"),
                "CONVERSATION_SERVICE_TOKEN": self.tokens["conversation_gateway"],
                "TELEGRAM_MANAGED_BINDINGS": json.dumps(telegram_bindings),
                "TELEGRAM_CANARY_INTEGRATION_IDS": json.dumps(
                    [self.ids["canary"]] if self.cohort_enabled else []),
                "INTEGRATIONS_POLL_INTERVAL": "1",
                "HEALTH_PORT": str(self.ports["gateway"]),
                "LOG_LEVEL": "WARNING",
                "GOAL4B_TELEGRAM_API_BASE": self.peer.base_url,
            })
        return env

    def start(self, name: str) -> None:
        assert name not in self.processes
        python = TG_PYTHON if name == "gateway" else CS_PYTHON
        env = self.env(name)
        log = (self.root / f"{name}.log").open("ab")
        self.log_handles.append(log)
        process_kind = "telegram" if name == "gateway" else name
        self.processes[name] = subprocess.Popen(
            [str(python), str(BOOTSTRAP), process_kind], cwd=self.root, env=env,
            stdout=log, stderr=log,
        )

    def kill(self, name: str) -> None:
        process = self.processes.pop(name, None)
        if process is not None:
            process.kill()
            process.wait(timeout=6)

    async def wait_ready(self, name: str) -> None:
        path = "/ready" if name == "worker" else "/health"
        deadline = asyncio.get_running_loop().time() + 15
        last_response = "no response"
        while asyncio.get_running_loop().time() < deadline:
            process = self.processes[name]
            if process.poll() is not None:
                pytest.fail(f"{name} process exited; inspect its private fixture log")
            try:
                response = await self.client.get(self.url(name) + path)
                last_response = f"{response.status_code}:{response.text}"
                if response.status_code == 200:
                    guard = self.root / f"{name}-guard.json"
                    if guard.exists():
                        assert json.loads(guard.read_text())["probe_blocked"] is True
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        pytest.fail(f"{name} process did not become ready: {last_response}")

    async def restart(self, name: str, *, cohort_enabled: bool | None = None) -> None:
        self.kill(name)
        await self.client.aclose()
        self.client = httpx.AsyncClient(timeout=20, trust_env=False)
        await asyncio.sleep(0.1)
        if cohort_enabled is not None:
            self.cohort_enabled = cohort_enabled
        self.start(name)
        await self.wait_ready(name)

    async def rows(self, query: str, *args: object) -> list[asyncpg.Record]:
        assert self.db is not None
        async with self.db.acquire() as connection:
            return await connection.fetch(query, *args)

    async def state(self, event_id: str) -> asyncpg.Record:
        values = await self.rows(
            "SELECT a.route,a.id AS admission_id,a.canonical_message_id,a.conversation_id,"
            "i.turn_id,t.status,t.failure_code,t.draft_text,d.status AS delivery_status,"
            "d.provider_message_id FROM telegram_admissions a "
            "LEFT JOIN telegram_immediate_turns i ON i.message_id=a.canonical_message_id "
            "LEFT JOIN agent_turns t ON t.id=i.turn_id "
            "LEFT JOIN delivery_attempts d ON d.source_turn_id=t.id "
            "WHERE a.provider_event_id=$1", event_id)
        assert len(values) == 1
        return values[0]

    def update(self, *, integration: str = "canary", message_id: int,
               update_id: int, text_value: str, chat_id: int = 404040,
               chat_type: str = "private", anonymous: bool = False) -> dict[str, object]:
        token = {"canary": "900001:fixture-token", "legacy": "900002:fixture-token"}[integration]
        message: dict[str, object] = {
            "message_id": message_id, "date": int(time()),
            "chat": {"id": chat_id if chat_type == "private" else -chat_id,
                     "type": chat_type,
                     "first_name": "Fixture User" if chat_type == "private" else None,
                     "title": "Fixture Group" if chat_type != "private" else None},
            "text": text_value,
        }
        if not anonymous:
            message["from"] = {"id": chat_id, "is_bot": False, "first_name": "Fixture User"}
        return {"update_id": update_id, "message": message, "_token": token}

    def queue(self, update: dict[str, object], *, force: bool = False,
              duplicate: bool = False) -> str:
        token = str(update.pop("_token"))
        self.peer.enqueue(token, update, force=force, duplicate=duplicate)
        return token

    async def close(self) -> None:
        for name in list(self.processes):
            self.kill(name)
        for handle in self.log_handles:
            handle.close()
        self.peer.send_release.set()
        self.peer.managed_release.set()
        if self.db is not None:
            await self.db.close()
        await self.peer.close()
        await self.client.aclose()


@pytest_asyncio.fixture
async def th(sql, database_url, telegram_runtime, process_ports, tmp_path):
    del telegram_runtime  # Validated candidate/runtime paths are used by the harness.
    async with sql.begin() as connection:
        await connection.execute(__import__("sqlalchemy").text(
            "TRUNCATE telegram_response_attempts, telegram_immediate_turns, "
            "telegram_admissions, delivery_reconciliations, delivery_attempts, "
            "agent_turns, messages, inbound_events, conversations CASCADE"
        ))
    ids = {name: str(uuid4()) for name in (
        "owner", "canary", "legacy", "canary_agent", "legacy_agent")}
    tokens = {name: secrets.token_urlsafe(36) for name in (
        "control_plane", "managed", "legacy", "conversation_gateway", "worker")}
    rows = [
        {"integration_id": ids["canary"], "agent_id": ids["canary_agent"],
         "owner_id": ids["owner"], "credentials": {"BOT_TOKEN": "900001:fixture-token"}},
        {"integration_id": ids["legacy"], "agent_id": ids["legacy_agent"],
         "owner_id": ids["owner"], "credentials": {"BOT_TOKEN": "900002:fixture-token"}},
        {"integration_id": str(uuid4()), "agent_id": str(uuid4()),
         "owner_id": ids["owner"], "credentials": {"BOT_TOKEN": "not-a-valid-token"}},
    ]
    peer = FakePeer(rows, tokens["control_plane"], tokens["managed"], tokens["legacy"])
    await peer.start(process_ports["fake"])
    client = httpx.AsyncClient(timeout=20, trust_env=False)
    dburl = make_url(database_url)
    pgurl = dburl.set(drivername="postgresql").render_as_string(hide_password=False)
    db = await asyncpg.create_pool(pgurl, min_size=1, max_size=4)
    harness = TelegramHarness(tmp_path, database_url, process_ports, ids, tokens,
                              peer, client, db=db)
    try:
        harness.start("conversation")
        await harness.wait_ready("conversation")
        harness.start("worker")
        await harness.wait_ready("worker")
        harness.start("gateway")
        await harness.wait_ready("gateway")
        for token in ("900001:fixture-token", "900002:fixture-token"):
            await peer.wait_for(lambda t=token: peer.get_updates_calls[t] > 0,
                                f"Telegram long polling for {token.split(':')[0]}")
        yield harness
    finally:
        await harness.close()


def event_key(update: dict[str, object]) -> str:
    message = update["message"]
    assert isinstance(message, dict)
    token = str(update.get("_token", "900001:fixture-token"))
    bot_id = token.split(":", 1)[0]
    return f"b{bot_id}c{message['chat']['id']}m{message['message_id']}"


async def wait_for_state(h: TelegramHarness, event_id: str,
                         predicate, label: str, timeout: float = 15) -> asyncpg.Record:
    deadline = asyncio.get_running_loop().time() + timeout
    last_state: asyncpg.Record | None = None
    while asyncio.get_running_loop().time() < deadline:
        values = await h.rows(
            "SELECT a.route,a.id AS admission_id,a.canonical_message_id,a.conversation_id,"
            "i.turn_id,t.status,t.failure_code,t.draft_text,d.status AS delivery_status,"
            "d.provider_message_id FROM telegram_admissions a "
            "LEFT JOIN telegram_immediate_turns i ON i.message_id=a.canonical_message_id "
            "LEFT JOIN agent_turns t ON t.id=i.turn_id "
            "LEFT JOIN delivery_attempts d ON d.source_turn_id=t.id "
            "WHERE a.provider_event_id=$1", event_id)
        if len(values) == 1 and predicate(values[0]):
            return values[0]
        if values:
            last_state = values[0]
        await asyncio.sleep(0.05)
    queued = {key.split(":", 1)[0]: len(values)
              for key, values in h.peer.updates.items()}
    final_state = dict(last_state) if last_state else None
    pytest.fail(
        f"Goal4B acceptance timed out waiting for {label}; state={final_state}; "
        f"managed_calls={len(h.peer.managed_calls)}; legacy_calls={len(h.peer.legacy_calls)}; "
        f"accepted_sends={len(h.peer.accepted_messages)}; "
        f"polls={dict(h.peer.get_updates_calls)}; "
        f"queued={queued}; "
        f"fixture_errors={h.peer.request_errors!r}"
    )


async def wait_count(h: TelegramHarness, getter, count: int, label: str) -> None:
    await h.peer.wait_for(lambda: len(getter()) >= count, label)


async def assert_guard(h: TelegramHarness, name: str) -> None:
    value = json.loads((h.root / f"{name}-guard.json").read_text())
    assert value == {"probe_blocked": True, "denials": 1}


@pytest.mark.asyncio
async def test_goal4b_required_local_process_matrix(th, sql) -> None:
    h = th
    canary_token = "900001:fixture-token"
    legacy_token = "900002:fixture-token"

    # 1: Full managed path through the real polling handler, API, worker, AI HTTP
    # peer, Telegram Bot API peer and PostgreSQL. Also proves formatter/evidence,
    # no Business operation, and no Redis dependency (16-18).
    managed = h.update(message_id=1, update_id=100, text_value="same words")
    managed_key = event_key(managed)
    h.queue(managed)
    state = await wait_for_state(h, managed_key,
        lambda row: row["status"] == "COMPLETED" and row["delivery_status"] == "PROVIDER_ACCEPTED",
        "managed turn and provider acceptance")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == 1, "managed send")
    sent = h.peer.accepted_messages[0]
    assert sent["parse_mode"] == "HTML"
    assert sent["text"] == "Hello <b>managed</b> <code>Telegram</code>"
    assert sent["provider_message_id"] == int(state["provider_message_id"])
    assert len(h.peer.managed_calls) == 1 and h.peer.legacy_calls == []
    assert h.peer.managed_calls[0]["contract_version"] == 2
    assert state["canonical_message_id"] is not None and state["turn_id"] is not None
    counts = await h.rows(
        "SELECT (SELECT count(*) FROM telegram_admissions WHERE route='MANAGED') AS admissions,"
        "(SELECT count(*) FROM inbound_events WHERE channel='TELEGRAM') AS events,"
        "(SELECT count(*) FROM messages WHERE actor='CUSTOMER') AS customers,"
        "(SELECT count(*) FROM messages WHERE actor='AGENT') AS agents,"
        "(SELECT count(*) FROM agent_turns) AS turns,"
        "(SELECT count(*) FROM delivery_attempts) AS deliveries,"
        "to_regclass('business_operations') AS business_operations"
    )
    assert tuple(counts[0]) == (1, 1, 1, 1, 1, 1, None)
    EVIDENCE.append({"scenario": 1, "managed_process_path": "PASS", "contract_version": 2,
                     "provider_delivery": state["delivery_status"], "message_id":
                     state["provider_message_id"], "business_operations": "none"})

    # 3/4: exact provider replay retains the same canonical message/Turn and
    # sends nothing twice; changed immutable payload with the same provider key
    # fails closed and never falls back to legacy.
    original_message_id = state["canonical_message_id"]
    original_turn_id = state["turn_id"]
    h.queue(h.update(message_id=1, update_id=100, text_value="same words"), force=True)
    await asyncio.sleep(0.5)
    replay = await h.state(managed_key)
    assert replay["canonical_message_id"] == original_message_id
    assert replay["turn_id"] == original_turn_id
    assert len(h.peer.managed_calls) == 1 and len(h.peer.accepted_messages) == 1
    EVIDENCE.append({"scenario": 3, "same_provider_replay": "PASS",
                     "managed_calls": 1, "telegram_sends": 1})
    before_calls = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    h.queue(h.update(message_id=1, update_id=100, text_value="changed immutable text"), force=True)
    await asyncio.sleep(0.4)
    assert len(h.peer.managed_calls) == before_calls
    assert len(h.peer.accepted_messages) == before_sends and h.peer.legacy_calls == []
    EVIDENCE.append({"scenario": 4, "conflicting_provider_replay": "PASS",
                     "managed_or_legacy_fallback": False})

    # 5 and 19: same content on a new provider Message is a new turn; two
    # concurrent duplicate admissions over real HTTP/PG converge on one identity.
    second = h.update(message_id=2, update_id=101, text_value="same words")
    second_key = event_key(second)
    h.queue(second, duplicate=True)
    second_state = await wait_for_state(h, second_key,
        lambda row: row["status"] == "COMPLETED" and row["delivery_status"] == "PROVIDER_ACCEPTED",
        "second distinct provider Message")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == 2, "second managed send")
    assert second_state["canonical_message_id"] != original_message_id
    assert second_state["turn_id"] != original_turn_id
    assert len(h.peer.managed_calls) == 2
    EVIDENCE.append({"scenario": 5, "same_text_new_provider_message": "PASS",
                     "new_turn": str(second_state["turn_id"])})

    concurrent = h.update(message_id=30, update_id=130, text_value="parallel replay")
    concurrent_key = event_key(concurrent)
    message = concurrent["message"]
    assert isinstance(message, dict)
    identity = {
        "owner_id": h.ids["owner"], "integration_id": h.ids["canary"],
        "agent_id": h.ids["canary_agent"], "provider_thread_id": str(message["chat"]["id"]),
        "provider_event_id": concurrent_key, "update_id": concurrent["update_id"],
        "chat_id": message["chat"]["id"], "message_id": message["message_id"],
        "sender_id": message["from"]["id"], "bot_id": 900001,
        "text": message["text"], "desired_route": "MANAGED",
    }
    headers = {"X-Service-Token": h.tokens["conversation_gateway"]}
    admission_replies = await asyncio.gather(*(
        h.client.post(h.url("conversation") + "/internal/v1/telegram/admissions",
                      json=identity, headers=headers)
        for _ in range(8)
    ))
    assert all(response.status_code == 200 for response in admission_replies)
    assert len({response.json()["admission_id"] for response in admission_replies}) == 1
    assert sum(not response.json()["replayed"] for response in admission_replies) == 1
    concurrent_state = await wait_for_state(h, concurrent_key,
        lambda row: row["status"] == "COMPLETED", "concurrent admission worker turn")
    assert concurrent_state["canonical_message_id"] is not None
    assert (await h.rows("SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
                         concurrent_key))[0][0] == 1
    EVIDENCE.append({"scenario": 19, "concurrent_real_http_admission_replays": 8,
                     "durable_admissions": 1, "duplicate_send_permission": False})

    # 2/15: a non-cohort integration stays on the existing CoreAPI path. An
    # invalid bot in the same Control Plane response does not stop either bot.
    legacy = h.update(integration="legacy", message_id=1, update_id=200,
                      text_value="legacy only", chat_id=404041)
    legacy_key = event_key(legacy)
    h.queue(legacy)
    await h.peer.wait_for(lambda: len(h.peer.legacy_calls) == 1, "legacy CoreAPI request")
    await h.peer.wait_for(lambda: any(
        call.get("method") == "sendMessage" and call.get("token") == legacy_token
        for call in h.peer.provider_calls), "legacy Telegram send")
    legacy_state = await h.state(legacy_key)
    assert legacy_state["route"] == "LEGACY" and legacy_state["canonical_message_id"] is None
    assert h.peer.legacy_calls[0]["channel"] == "telegram"
    # The third execution came from the direct concurrent admission case above.
    assert len(h.peer.managed_calls) == 3
    EVIDENCE.append({"scenario": 2, "non_cohort": "LEGACY", "core_api_calls": 1,
                     "bad_bot_isolation": "PASS"})
    EVIDENCE.append({"scenario": 15,
                     "one_invalid_integration_did_not_stop_canary_and_legacy": "PASS"})

    # 6: Kill Gateway after durable admission and while AI is executing. Telegram
    # has not acknowledged its update, so the restarted poller reuses that event.
    h.peer.managed_block = True
    h.peer.managed_release.clear()
    h.peer.managed_entered.clear()
    restart_event = h.update(message_id=3, update_id=102, text_value="gateway restart")
    restart_key = event_key(restart_event)
    h.queue(restart_event)
    await asyncio.wait_for(h.peer.managed_entered.wait(), 12)
    durable = await wait_for_state(h, restart_key,
        lambda row: row["canonical_message_id"] is not None and row["turn_id"] is not None,
        "durable ingress before Gateway restart")
    assert durable["status"] in {"CLAIMED", "COMPLETED"}
    h.kill("gateway")
    h.peer.managed_release.set()
    await h.restart("gateway", cohort_enabled=True)
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == 3,
                          "managed response after Gateway restart")
    restarted_state = await wait_for_state(h, restart_key,
        lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "retained result after Gateway restart")
    assert restarted_state["canonical_message_id"] == durable["canonical_message_id"]
    assert restarted_state["turn_id"] == durable["turn_id"]
    EVIDENCE.append({"scenario": 6, "gateway_restart_after_admission": "PASS",
                     "same_message_and_turn": True})

    # 7: Restart Conversation after a durable completed Turn, then replay the
    # exact provider update through the real Gateway against the restarted API.
    managed_calls_before_api_restart = len(h.peer.managed_calls)
    sends_before_api_restart = len(h.peer.accepted_messages)
    h.kill("conversation")
    await h.restart("conversation")
    actions_before_api_replay = sum(
        call.get("method") == "sendChatAction" and call.get("token") == canary_token
        for call in h.peer.provider_calls
    )
    h.queue(h.update(message_id=1, update_id=100, text_value="same words"), force=True)
    await h.peer.wait_for(lambda: sum(
        call.get("method") == "sendChatAction" and call.get("token") == canary_token
        for call in h.peer.provider_calls) > actions_before_api_replay,
        "retained provider replay after Conversation restart")
    await asyncio.sleep(0.5)
    api_restarted = await h.state(managed_key)
    assert api_restarted["delivery_status"] == "PROVIDER_ACCEPTED"
    assert len(h.peer.managed_calls) == managed_calls_before_api_restart
    assert len(h.peer.accepted_messages) == sends_before_api_restart
    EVIDENCE.append({"scenario": 7, "conversation_api_restart": "PASS",
                     "retained_message": str(api_restarted["canonical_message_id"])})

    # 8: Stop the independent worker after durable ingress, then restart it.
    # The pending provider event remains unacknowledged until the retained Turn
    # completes and it is sent exactly once.
    h.kill("worker")
    await asyncio.sleep(0.15)
    worker_event = h.update(message_id=12, update_id=111,
                            text_value="worker restart after ingress")
    worker_key = event_key(worker_event)
    sends_before_worker_restart = len(h.peer.accepted_messages)
    h.queue(worker_event)
    waiting_admission = await wait_for_state(h, worker_key,
        lambda row: row["canonical_message_id"] is not None and row["turn_id"] is None,
        "durable admission while worker is stopped")
    assert waiting_admission["route"] == "MANAGED"
    await h.restart("worker")
    worker_state = await wait_for_state(h, worker_key,
        lambda row: row["status"] == "COMPLETED" and row["delivery_status"] == "PROVIDER_ACCEPTED",
        "worker restart processing accepted ingress")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == sends_before_worker_restart + 1,
                          "worker restart Telegram reply")
    assert worker_state["canonical_message_id"] == waiting_admission["canonical_message_id"]
    EVIDENCE.append({"scenario": 8, "worker_restart_after_durable_ingress": "PASS",
                     "same_message": True})

    # 11: Provider rejection is a known FAILED fact, with no canonical AGENT
    # message and no false success. Re-enable exact cohort for new admissions.
    await h.restart("gateway", cohort_enabled=True)
    h.peer.send_modes.append("reject")
    rejected = h.update(message_id=5, update_id=104, text_value="provider rejects")
    rejected_key = event_key(rejected)
    h.queue(rejected)
    rejected_state = await wait_for_state(h, rejected_key,
        lambda row: row["delivery_status"] == "FAILED", "known Telegram send failure")
    assert rejected_state["status"] == "COMPLETED"
    assert rejected_state["provider_message_id"] is None
    assert (await h.rows(
        "SELECT count(*) FROM messages WHERE source_agent_turn_id=$1",
        rejected_state["turn_id"]))[0][0] == 0
    EVIDENCE.append({"scenario": 11, "known_provider_rejection": "FAILED",
                     "false_success": False})

    # 9/12: provider accepts a message but drops the response; the retained
    # dispatch is UNKNOWN and an exact duplicate is never resent.
    h.peer.send_modes.append("accept_drop")
    unknown = h.update(message_id=6, update_id=105, text_value="ambiguous provider outcome")
    unknown_key = event_key(unknown)
    h.queue(unknown)
    unknown_state = await wait_for_state(h, unknown_key,
        lambda row: row["delivery_status"] == "UNKNOWN", "ambiguous Telegram write")
    accepted_before = len(h.peer.accepted_messages)
    ai_before = len(h.peer.managed_calls)
    h.queue(h.update(message_id=6, update_id=105,
                     text_value="ambiguous provider outcome"), force=True)
    await asyncio.sleep(0.5)
    assert len(h.peer.accepted_messages) == accepted_before
    assert len(h.peer.managed_calls) == ai_before
    assert unknown_state["provider_message_id"] is None
    EVIDENCE.append({"scenario": 12, "lost_send_response": "UNKNOWN",
                     "resend_after_replay": False})
    EVIDENCE.append({"scenario": 9, "restart_boundary_after_retained_draft":
                     "covered by Goal4B durable begin-response before Telegram write"})

    # 13: Rollback only affects new admissions. An already accepted managed
    # update resumes its sticky route and cannot fall back; subsequent input is
    # admitted LEGACY. Hold the provider response after recording acceptance to
    # kill Gateway in the uncertain write window.
    h.peer.send_modes.append("hold_after_accept")
    h.peer.send_entered.clear()
    sticky = h.update(message_id=7, update_id=106, text_value="sticky managed")
    sticky_key = event_key(sticky)
    h.queue(sticky)
    await asyncio.wait_for(h.peer.send_entered.wait(), 12)
    sticky_before = await h.state(sticky_key)
    assert sticky_before["route"] == "MANAGED"
    h.kill("gateway")
    h.peer.send_release.set()
    await h.restart("gateway", cohort_enabled=False)
    updates_before_restart = h.peer.get_updates_calls[canary_token]
    # BotManager already polls the canary. Observe a fresh request from the new process.
    await h.peer.wait_for(lambda: h.peer.get_updates_calls[canary_token] > updates_before_restart,
                          "canary polling after rollback")
    await asyncio.sleep(0.5)
    sticky_after = await h.state(sticky_key)
    assert sticky_after["route"] == "MANAGED"
    assert len(h.peer.legacy_calls) == 1
    assert sum(item.get("chat_id") == 404040 and item.get("mode") == "hold_after_accept"
               for item in h.peer.accepted_messages) == 1
    rolled_back = h.update(message_id=8, update_id=107, text_value="new message after rollback")
    rolled_back_key = event_key(rolled_back)
    h.queue(rolled_back)
    await h.peer.wait_for(lambda: len(h.peer.legacy_calls) == 2,
                          "new legacy message after rollback")
    assert (await h.state(rolled_back_key))["route"] == "LEGACY"
    EVIDENCE.append({"scenario": 13, "sticky_managed_and_new_legacy_rollback": "PASS"})
    EVIDENCE.append({"scenario": 9,
                     "gateway_restart_after_draft_before_delivery_completion": "PASS"})

    # 14: Wrong expected agent fails closed at Conversation before any canonical
    # ingress; correct integrations remain healthy. This uses the actual network API.
    wrong = dict(identity)
    wrong["provider_event_id"] = "b900001c404040m901"
    wrong["provider_thread_id"] = "404040"
    wrong["message_id"] = 901
    wrong["update_id"] = 901
    wrong["text"] = "wrong agent binding"
    wrong["agent_id"] = str(uuid4())
    response = await h.client.post(h.url("conversation") + "/internal/v1/telegram/admissions",
                                   json=wrong, headers=headers)
    assert response.status_code == 404
    assert (await h.rows("SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
                         wrong["provider_event_id"]))[0][0] == 0
    wrong_integration = dict(identity)
    wrong_integration["integration_id"] = str(uuid4())
    wrong_integration["provider_event_id"] = "b900001c404040m902"
    wrong_integration["message_id"] = 902
    wrong_integration["update_id"] = 902
    response = await h.client.post(h.url("conversation") + "/internal/v1/telegram/admissions",
                                   json=wrong_integration, headers=headers)
    assert response.status_code == 404
    assert (await h.rows("SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
                         wrong_integration["provider_event_id"]))[0][0] == 0
    EVIDENCE.append({"scenario": 14, "wrong_agent_and_integration_binding": "FAIL_CLOSED"})

    # Unsupported chat shape retains legacy behavior; non-text receives the
    # established unsupported-message response; anonymous channel posts are ignored.
    h.peer.send_release.set()
    group = h.update(message_id=9, update_id=108, text_value="group legacy",
                     chat_type="group", chat_id=404042)
    group_key = event_key(group)  # This key is not submitted to Conversation.
    legacy_before = len(h.peer.legacy_calls)
    h.queue(group)
    await h.peer.wait_for(lambda: len(h.peer.legacy_calls) == legacy_before + 1,
                          "group message legacy path")
    assert (await h.rows("SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
                         group_key))[0][0] == 0
    non_text = h.update(message_id=10, update_id=109, text_value="", chat_id=404043)
    token = str(non_text.pop("_token"))
    non_text["message"].pop("text")
    non_text["message"]["photo"] = [{"file_id": "synthetic-photo", "file_unique_id": "photo-1",
                                      "width": 1, "height": 1}]
    h.peer.enqueue(token, non_text)
    await h.peer.wait_for(
        lambda: any(item.get("text") and "только текстовые" in str(item.get("text"))
                    for item in h.peer.accepted_messages),
        "non-text compatibility reply",
    )
    anonymous = h.update(message_id=11, update_id=110, text_value="anonymous",
                         chat_type="channel", chat_id=404044, anonymous=True)
    anonymous_token = str(anonymous.pop("_token"))
    before_sends = len(h.peer.accepted_messages)
    h.peer.enqueue(anonymous_token, anonymous)
    await asyncio.sleep(0.4)
    assert len(h.peer.accepted_messages) == before_sends
    EVIDENCE.append({"scenario": "extra", "group_nontext_and_anonymous_compatibility": "PASS"})
    EVIDENCE.append({"scenario": 16, "managed_html_formatter_parity": "PASS"})
    EVIDENCE.append({"scenario": 17, "no_redis_or_legacy_session_on_managed_path": "PASS"})
    EVIDENCE.append({"scenario": 18, "business_operations": "none"})

    # 10/20/21: Full service and Website regressions are proven by this process
    # stack plus the repository's existing v1/v2 suite. No Redis process exists.
    await assert_guard(h, "conversation")
    await assert_guard(h, "worker")
    await assert_guard(h, "gateway")
    assert not h.peer.request_errors
    EVIDENCE.append({"scenario": 10, "provider_success_evidence": "PROVIDER_ACCEPTED + message id"})
    EVIDENCE.append({"scenario": 20, "website_regression":
                     "run existing Website process acceptance separately"})
    EVIDENCE.append({"scenario": 21, "conversation_v1_v2_regression":
                     "run complete Conversation suite separately"})
    evidence_path = os.environ.get("GOAL4B_EVIDENCE_PATH")
    if evidence_path:
        target = Path(evidence_path)
        if not target.is_absolute() or target.parent != Path("/private/tmp"):
            raise RuntimeError("Evidence path must be inside /private/tmp")
        target.write_text(json.dumps(EVIDENCE, indent=2, sort_keys=True, default=str))
        target.chmod(0o600)


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    if EVIDENCE:
        terminalreporter.write_line("Goal4B Telegram local process matrix (synthetic IDs only):")
        for row in EVIDENCE:
            terminalreporter.write_line(json.dumps(row, sort_keys=True, default=str))
        path = os.environ.get("GOAL4B_EVIDENCE_PATH")
        if path:
            target = Path(path)
            if not target.is_absolute() or target.parent != Path("/private/tmp"):
                raise RuntimeError("Evidence path must be inside /private/tmp")
            target.write_text(json.dumps(EVIDENCE, indent=2, sort_keys=True, default=str))
            target.chmod(0o600)
