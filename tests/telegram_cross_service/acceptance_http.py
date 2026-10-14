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
from datetime import timedelta
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
ACCEPTED_TG_ROOT = Path(os.environ.get(
    "GOAL4B_TELEGRAM_ACCEPTED_REPO",
    "/tmp/goal5-r2-telegram-review/accepted-base-4ee10d9",
)).resolve()
ACCEPTED_TG_SHA = "4ee10d9271d1b72427f44aa86c6a151c5250678d"
CS_PYTHON = Path(os.environ.get(
    "GOAL5_CONVERSATION_PYTHON", str(CS_ROOT / ".venv/bin/python")
))
EVIDENCE: list[dict[str, object]] = []


@pytest.fixture(scope="session")
def telegram_runtime() -> tuple[Path, Path]:
    if (not TG_ROOT.is_absolute() or not (TG_ROOT / "main.py").is_file()
            or not (TG_ROOT / "bot/handlers.py").is_file() or not TG_PYTHON.is_file()
            or not CS_PYTHON.is_file()):
        pytest.fail("Explicit Telegram candidate, pinned runtimes and clean process env required")
    if (not (ACCEPTED_TG_ROOT / "main.py").is_file()
            or not (ACCEPTED_TG_ROOT / "bot/handlers.py").is_file()):
        pytest.fail("Exact accepted Telegram source archive is unavailable")
    accepted_sha = subprocess.run(
        ["git", "-C", str(ACCEPTED_TG_ROOT), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if accepted_sha != ACCEPTED_TG_SHA:
        pytest.fail("Accepted Telegram oracle source does not match its pinned commit")
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
        self.managed_call_times: list[float] = []
        self.legacy_calls: list[dict[str, object]] = []
        self.provider_calls: list[dict[str, object]] = []
        self.accepted_messages: list[dict[str, object]] = []
        self.get_updates_calls: dict[str, int] = defaultdict(int)
        self.get_updates_offsets: dict[str, list[int | None]] = defaultdict(list)
        self.acknowledged_update_ids: dict[str, list[int]] = defaultdict(list)
        self._ack_events: dict[tuple[str, int], asyncio.Event] = {}
        self._forced_inflight: dict[str, int] = {}
        self._forced_next_poll: dict[tuple[str, int], asyncio.Event] = {}
        self.send_modes: deque[str] = deque()
        self.chat_action_modes: dict[str, str] = {}
        self.action_entered: dict[str, asyncio.Event] = {}
        self.action_release: dict[str, asyncio.Event] = {}
        self.current_updates: dict[str, dict[str, object]] = {}
        self.db: asyncpg.Pool | None = None
        self.raw_provider_response = "fixture-raw-telegram-response-secret"
        self.managed_block = False
        self.managed_release = asyncio.Event()
        self.managed_release.set()
        self.managed_entered = asyncio.Event()
        self.send_entered = asyncio.Event()
        self.send_release = asyncio.Event()
        self.send_release.set()
        self.conversation_base_url = ""
        self.admission_pre_modes: dict[str, deque[str]] = defaultdict(deque)
        self.admission_after_modes: dict[str, deque[str]] = defaultdict(deque)
        self.admission_attempts: dict[str, int] = defaultdict(int)
        self.admission_responses: dict[str, list[dict[str, object]]] = defaultdict(list)
        self.delivery_api_calls: list[dict[str, object]] = []
        self.admission_mode_entered: dict[tuple[str, str], asyncio.Event] = {}
        self.admission_mode_release: dict[tuple[str, str], asyncio.Event] = {}
        self.provider_message_id = 71000
        self.request_errors: list[str] = []
        self.transition_generation = 0
        self.active_transitions: set[str] = set()
        self.transition_history: list[dict[str, object]] = []
        self.expected_transition_errors: list[dict[str, object]] = []
        self.unexpected_error_diagnostics: list[dict[str, object]] = []

    def begin_transition(self, service: str) -> None:
        self.transition_generation += 1
        self.active_transitions.add(service)
        self.transition_history.append({
            "generation": self.transition_generation,
            "service": service,
            "phase": "start",
        })

    def end_transition(self, service: str) -> None:
        self.transition_generation += 1
        self.active_transitions.discard(service)
        self.transition_history.append({
            "generation": self.transition_generation,
            "service": service,
            "phase": "ready",
        })

    def transitions_since(self, generation: int) -> list[str]:
        return sorted({
            str(item["service"]) for item in self.transition_history
            if int(item["generation"]) > generation
        })

    async def start(self, port: int) -> None:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", port)
        self.base_url = f"http://127.0.0.1:{port}"

    async def close(self) -> None:
        self.managed_release.set()
        self.send_release.set()
        for event in self.action_release.values():
            event.set()
        for event in self.admission_mode_release.values():
            event.set()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _reply(self, writer: asyncio.StreamWriter, value: object,
                     status: int = 200) -> None:
        body = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        reason = {200: "OK", 401: "Unauthorized", 403: "Forbidden",
                  500: "Internal Server Error"}.get(
            status, "Test response")
        writer.write((f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
                      f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
                     + body)
        await writer.drain()

    async def _redirect(self, writer: asyncio.StreamWriter, path: str) -> None:
        body = b"fixture redirect body"
        writer.write((f"HTTP/1.1 302 Found\r\nLocation: {self.base_url}{path}\r\n"
                      f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
                     + body)
        await writer.drain()

    async def _reply_raw(self, writer: asyncio.StreamWriter, body: bytes,
                         status: int) -> None:
        reason = {200: "OK", 201: "Created", 401: "Unauthorized", 403: "Forbidden",
                  404: "Not Found", 422: "Unprocessable Entity",
                  500: "Internal Server Error"}.get(status, "Test response")
        writer.write((f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
                      f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
                     + body)
        await writer.drain()

    async def _proxy_conversation(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
        method: str, path: str, headers: dict[str, str], payload: dict[str, object],
    ) -> None:
        event_id = str(payload.get("provider_event_id", ""))
        is_admission = path.endswith("/admissions")
        if is_admission:
            self.admission_attempts[event_id] += 1
        pre_modes = self.admission_pre_modes[event_id] if is_admission else deque()
        pre_mode = pre_modes.popleft() if pre_modes else ""
        if pre_mode in {"unauthorized", "forbidden", "server_500", "malformed",
                        "reset", "timeout"}:
            if pre_mode == "unauthorized":
                self.admission_responses[event_id].append(
                    {"status_code": 401, "replayed": None, "mode": pre_mode})
                await self._reply(writer, {"detail": "fixture rejected before commit"}, 401)
            elif pre_mode == "forbidden":
                self.admission_responses[event_id].append(
                    {"status_code": 403, "replayed": None, "mode": pre_mode})
                await self._reply(writer, {"detail": "fixture forbidden before commit"}, 403)
            elif pre_mode == "server_500":
                self.admission_responses[event_id].append(
                    {"status_code": 500, "replayed": None, "mode": pre_mode})
                await self._reply(writer, {"detail": "fixture unavailable before commit"}, 500)
            elif pre_mode == "malformed":
                self.admission_responses[event_id].append(
                    {"status_code": 200, "replayed": None, "mode": pre_mode})
                await self._reply(writer, {"route": "MANAGED"})
            elif pre_mode == "timeout":
                self.admission_responses[event_id].append(
                    {"status_code": None, "replayed": None, "mode": pre_mode})
                # The Conversation client has a ten-second request timeout.
                # Keep this socket pending long enough for that timeout and
                # the gateway's bounded retry policy to take effect.
                await asyncio.sleep(10.25)
                writer.close()
                await writer.wait_closed()
            else:
                self.admission_responses[event_id].append(
                    {"status_code": None, "replayed": None, "mode": pre_mode})
                writer.close()
                await writer.wait_closed()
            return
        if pre_mode == "hold":
            key = (event_id, "pre")
            self.admission_mode_entered.setdefault(key, asyncio.Event()).set()
            await self.admission_mode_release.setdefault(key, asyncio.Event()).wait()

        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            response = await client.request(
                method, self.conversation_base_url + path, json=payload,
                headers={"X-Service-Token": headers.get("x-service-token", "")},
            )
        if path.endswith("/responses/work") or path.endswith("/responses/begin") \
                or path.endswith("/responses/complete"):
            try:
                response_payload = response.json()
            except ValueError:
                response_payload = {}
            if not isinstance(response_payload, dict):
                response_payload = {}
            items = response_payload.get("items")
            self.delivery_api_calls.append({
                "path": path,
                "status_code": response.status_code,
                "request_bot_id": payload.get("bot_id"),
                "request_event_id": payload.get("provider_event_id"),
                "request_attempt_id": payload.get("response_attempt_id"),
                "request_outcome": payload.get("outcome"),
                "request_provider_message_id": payload.get("provider_message_id"),
                "work_kinds": [item.get("work_kind") for item in items
                               if isinstance(item, dict)] if isinstance(items, list) else [],
                "begin_send": response_payload.get("send"),
                "begin_work_kind": response_payload.get("work_kind"),
                "response_outcome": response_payload.get("outcome"),
                "response_attempt_id": response_payload.get("response_attempt_id"),
                "response_admission_id": response_payload.get("admission_id"),
                "response_bot_id": response_payload.get("bot_id"),
                "response_provider_message_id": response_payload.get("provider_message_id"),
                "detail_code": (response_payload.get("detail", {}).get("code")
                                if isinstance(response_payload.get("detail"), dict) else None),
            })
        if is_admission:
            try:
                response_payload = response.json()
            except ValueError:
                response_payload = {}
            self.admission_responses[event_id].append({
                "status_code": response.status_code,
                "replayed": (response_payload.get("replayed")
                             if isinstance(response_payload, dict) else None),
            })
        after_modes = self.admission_after_modes[event_id] if is_admission else deque()
        after_mode = after_modes.popleft() if after_modes else ""
        if after_mode in {"hold", "drop"}:
            key = (event_id, "post")
            self.admission_mode_entered.setdefault(key, asyncio.Event()).set()
            if after_mode == "hold":
                await self.admission_mode_release.setdefault(key, asyncio.Event()).wait()
            else:
                writer.close()
                await writer.wait_closed()
                return
        await self._reply_raw(writer, response.content, response.status_code)

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
        transition_generation_at_start = self.transition_generation
        active_transitions_at_start = set(self.active_transitions)
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

            if path.startswith("/internal/v1/telegram/"):
                await self._proxy_conversation(
                    reader, writer, method, path, headers, payload,
                )
                return

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
                self.managed_call_times.append(asyncio.get_running_loop().time())
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
                    previous_forced = self._forced_inflight.pop(token, None)
                    if previous_forced is not None:
                        next_poll = self._forced_next_poll.get((token, previous_forced))
                        if next_poll is not None:
                            next_poll.set()
                    raw_offset = payload.get("offset")
                    offset = int(raw_offset) if raw_offset is not None else None
                    self.get_updates_offsets[token].append(offset)
                    queue = self.updates[token]
                    while (offset is not None and queue
                           and int(queue[0]["update_id"]) < offset):
                        removed = queue.popleft()
                        removed_id = int(removed["update_id"])
                        self.acknowledged_update_ids[token].append(removed_id)
                        ack_event = self._ack_events.get((token, removed_id))
                        if ack_event is not None:
                            ack_event.set()
                    forced = self.forced_updates[token]
                    if forced:
                        forced_update = forced.popleft()
                        self._forced_inflight[token] = int(forced_update["update_id"])
                        updates = [forced_update]
                    else:
                        updates = list(queue)[:100]
                    self.get_updates_calls[token] += 1
                    if updates:
                        self.current_updates[token] = updates[0]
                    if not updates:
                        await asyncio.sleep(0.05)
                    await self._reply(writer, {"ok": True, "result": updates})
                    return
                if method_name == "sendChatAction":
                    update = self.current_updates.get(token)
                    event_id = None
                    if update is not None and isinstance(update.get("message"), dict):
                        current_message = update["message"]
                        current_chat = current_message.get("chat")
                        if isinstance(current_chat, dict):
                            event_id = (f"b{token.split(':', 1)[0]}c{current_chat['id']}"
                                        f"m{current_message['message_id']}")
                    if event_id in self.action_entered:
                        self.action_entered[event_id].set()
                        await self.action_release[event_id].wait()
                    mode = self.chat_action_modes.get(event_id or "", "success")
                    if mode == "server_500":
                        await self._reply(writer, {
                            "ok": False,
                            "error_code": 500,
                            "description": self.raw_provider_response,
                        }, status=500)
                        return
                    if mode == "network":
                        writer.close()
                        await writer.wait_closed()
                        return
                    if mode == "timeout":
                        await asyncio.sleep(0.8)
                        if not writer.is_closing():
                            await self._reply(writer, {"ok": True, "result": True})
                        return
                    if mode == "redirect_loop":
                        await self._redirect(writer, path)
                        return
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
            changed_services = sorted(
                set(self.transitions_since(transition_generation_at_start))
                | active_transitions_at_start | self.active_transitions
            )
            expected_proxy_failure = (
                bool(changed_services)
                and method_hint in {"work", "results"}
                and isinstance(exc, (httpx.ConnectError, httpx.RemoteProtocolError))
                and "conversation" in changed_services
            )
            diagnostic = {
                "path": method_hint,
                "error_class": type(exc).__name__,
                "transition_services": changed_services,
                "transition_generation_at_start": transition_generation_at_start,
                "transition_generation_at_failure": self.transition_generation,
            }
            if expected_proxy_failure:
                self.expected_transition_errors.append(diagnostic)
            else:
                self.request_errors.append(f"{method_hint}:{type(exc).__name__}")
                self.unexpected_error_diagnostics.append(diagnostic)
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

    def after_forced_update(self, token: str, update_id: int) -> asyncio.Event:
        return self._forced_next_poll.setdefault((token, update_id), asyncio.Event())

    def acknowledged(self, token: str, update_id: int) -> asyncio.Event:
        return self._ack_events.setdefault((token, update_id), asyncio.Event())

    def hold_typing(self, event_id: str) -> asyncio.Event:
        entered = self.action_entered.setdefault(event_id, asyncio.Event())
        self.action_release.setdefault(event_id, asyncio.Event()).clear()
        return entered

    def release_typing(self, event_id: str) -> None:
        self.action_release.setdefault(event_id, asyncio.Event()).set()

    def hold_admission_request(self, event_id: str) -> asyncio.Event:
        key = (event_id, "pre")
        entered = self.admission_mode_entered.setdefault(key, asyncio.Event())
        self.admission_mode_release.setdefault(key, asyncio.Event()).clear()
        self.admission_pre_modes[event_id].append("hold")
        return entered

    def release_admission_request(self, event_id: str) -> None:
        self.admission_mode_release.setdefault((event_id, "pre"), asyncio.Event()).set()

    def hold_admission_response(self, event_id: str) -> asyncio.Event:
        key = (event_id, "post")
        entered = self.admission_mode_entered.setdefault(key, asyncio.Event())
        self.admission_mode_release.setdefault(key, asyncio.Event()).clear()
        self.admission_after_modes[event_id].append("hold")
        return entered

    def release_admission_response(self, event_id: str) -> None:
        self.admission_mode_release.setdefault((event_id, "post"), asyncio.Event()).set()

    def offset_advanced(self, token: str, update_id: int) -> bool:
        return any(
            call.get("method") == "getUpdates" and call.get("token") == token
            and int(call.get("payload", {}).get("offset", 0)) > update_id
            for call in self.provider_calls
        )

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
    settling_enabled: bool = False
    settling_quiet_window_ms: int = 1000
    settling_max_wait_ms: int = 1200
    db: asyncpg.Pool | None = field(default=None, repr=False)
    gateway_source_root: Path = field(default=TG_ROOT, repr=False)

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
            "PYTHONPATH": str(CS_ROOT if name != "gateway" else self.gateway_source_root),
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
            if self.settling_enabled:
                env["TURN_SETTLING_POLICIES"] = json.dumps([{
                    "version": "telegram-process-v1",
                    "channel": "TELEGRAM",
                    "integration_id": self.ids["canary"],
                    "quiet_window_ms": self.settling_quiet_window_ms,
                    "max_wait_ms": self.settling_max_wait_ms,
                }])
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
                "CONVERSATION_SERVICE_URL": self.peer.base_url,
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

    def start(self, name: str, *, gateway_source_root: Path | None = None) -> None:
        assert name not in self.processes
        if name == "gateway" and gateway_source_root is not None:
            self.gateway_source_root = gateway_source_root.resolve()
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
            if name in {"conversation", "gateway"}:
                self.peer.begin_transition(name)
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
                    if name in {"conversation", "gateway"}:
                        self.peer.end_transition(name)
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        pytest.fail(f"{name} process did not become ready: {last_response}")

    async def restart(
        self, name: str, *, cohort_enabled: bool | None = None,
        gateway_source_root: Path | None = None,
    ) -> None:
        self.kill(name)
        await self.client.aclose()
        self.client = httpx.AsyncClient(timeout=20, trust_env=False)
        await asyncio.sleep(0.1)
        if cohort_enabled is not None:
            self.cohort_enabled = cohort_enabled
        self.start(name, gateway_source_root=gateway_source_root)
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
    peer.conversation_base_url = f"http://127.0.0.1:{process_ports['conversation']}"
    client = httpx.AsyncClient(timeout=20, trust_env=False)
    dburl = make_url(database_url)
    pgurl = dburl.set(drivername="postgresql").render_as_string(hide_password=False)
    db = await asyncpg.create_pool(pgurl, min_size=1, max_size=4)
    peer.db = db
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
    process_states = {
        name: process.poll() for name, process in h.processes.items()
    }
    process_logs = {name: str(h.root / f"{name}.log") for name in h.processes}
    provider_trace = [{
        "method": call.get("method"),
        "chat_id": (call.get("send_record") or {}).get("chat_id"),
        "accepted": (call.get("send_record") or {}).get("accepted"),
        "provider_message_id": (call.get("send_record") or {}).get(
            "provider_message_id"),
    } for call in h.peer.provider_calls[-20:]]
    provider_frontiers = {
        token.split(":", 1)[0]: {
            "offsets": h.peer.get_updates_offsets[token][-12:],
            "acknowledged_update_ids": h.peer.acknowledged_update_ids[token][-20:],
            "queued_update_ids": [int(update["update_id"])
                                  for update in h.peer.updates[token]],
            "forced_update_ids": [int(update["update_id"])
                                  for update in h.peer.forced_updates[token]],
            "current_update_id": (
                int(h.peer.current_updates[token]["update_id"])
                if token in h.peer.current_updates else None
            ),
        }
        for token in ("900001:fixture-token", "900002:fixture-token")
    }
    pytest.fail(
        f"Goal4B acceptance timed out waiting for {label}; state={final_state}; "
        f"managed_calls={len(h.peer.managed_calls)}; legacy_calls={len(h.peer.legacy_calls)}; "
        f"accepted_sends={len(h.peer.accepted_messages)}; "
        f"polls={dict(h.peer.get_updates_calls)}; "
        f"process_exit_codes={process_states}; "
        f"provider_trace={provider_trace}; "
        f"provider_frontiers={provider_frontiers}; "
        f"delivery_api_calls={h.peer.delivery_api_calls[-20:]}; "
        f"queued={queued}; "
        f"fixture_errors={h.peer.request_errors!r}; "
        f"process_logs={process_logs}"
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
    assert not any(call.get("method") == "sendChatAction"
                   and call.get("token") == canary_token for call in h.peer.provider_calls)
    EVIDENCE.append({"scenario": 1, "managed_process_path": "PASS", "contract_version": 2,
                     "provider_delivery": state["delivery_status"], "message_id":
                     state["provider_message_id"], "business_operations": "none"})

    # 3/4: a duplicate update below the acknowledged provider frontier is
    # ignored, while Conversation's exact replay and immutable-payload conflict
    # remain independently checked against the retained event identity.
    original_message_id = state["canonical_message_id"]
    original_turn_id = state["turn_id"]
    replay_attempts = h.peer.admission_attempts[managed_key]
    duplicate_poll = h.peer.after_forced_update(canary_token, 100)
    h.queue(h.update(message_id=1, update_id=100, text_value="same words"), force=True)
    await asyncio.wait_for(duplicate_poll.wait(), 12)
    assert h.peer.admission_attempts[managed_key] == replay_attempts
    replay = await h.state(managed_key)
    assert replay["canonical_message_id"] == original_message_id
    assert replay["turn_id"] == original_turn_id
    assert len(h.peer.managed_calls) == 1 and len(h.peer.accepted_messages) == 1

    managed_message = managed["message"]
    assert isinstance(managed_message, dict)
    managed_chat = managed_message["chat"]
    managed_sender = managed_message["from"]
    assert isinstance(managed_chat, dict) and isinstance(managed_sender, dict)
    admission_request = {
        "owner_id": h.ids["owner"], "integration_id": h.ids["canary"],
        "agent_id": h.ids["canary_agent"],
        "provider_thread_id": str(managed_chat["id"]),
        "provider_event_id": managed_key, "update_id": managed["update_id"],
        "chat_id": managed_chat["id"], "message_id": managed_message["message_id"],
        "sender_id": managed_sender["id"], "bot_id": 900001,
        "text": managed_message["text"], "desired_route": "MANAGED",
    }
    api_headers = {"X-Service-Token": h.tokens["conversation_gateway"]}
    admission_url = h.url("conversation") + "/internal/v1/telegram/admissions"
    exact_replay = await h.client.post(
        admission_url, json=admission_request, headers=api_headers,
    )
    assert exact_replay.status_code == 200 and exact_replay.json()["replayed"] is True
    assert exact_replay.json()["admission_id"] == str(state["admission_id"])
    EVIDENCE.append({"scenario": 3, "provider_frontier_duplicate_skipped": True,
                     "exact_http_replay": "PASS", "managed_calls": 1,
                     "telegram_sends": 1})

    before_calls = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    conflict = await h.client.post(
        admission_url, json={**admission_request, "text": "changed immutable text"},
        headers=api_headers,
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["code"] == "EVENT_CONFLICT"
    assert len(h.peer.managed_calls) == before_calls
    assert len(h.peer.accepted_messages) == before_sends and h.peer.legacy_calls == []
    EVIDENCE.append({"scenario": 4, "conflicting_http_replay": "PASS",
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
    accepted_before_gateway_restart = len(h.peer.accepted_messages)
    h.queue(restart_event)
    await asyncio.wait_for(h.peer.managed_entered.wait(), 12)
    durable = await wait_for_state(h, restart_key,
        lambda row: row["canonical_message_id"] is not None and row["turn_id"] is not None,
        "durable ingress before Gateway restart")
    assert durable["status"] in {"CLAIMED", "COMPLETED"}
    h.kill("gateway")
    h.peer.managed_release.set()
    await h.restart("gateway", cohort_enabled=True)
    restarted_state = await wait_for_state(h, restart_key,
        lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "retained result after Gateway restart")
    assert len(h.peer.accepted_messages) > accepted_before_gateway_restart
    assert any(item.get("provider_message_id") == int(restarted_state["provider_message_id"])
               for item in h.peer.accepted_messages)
    assert restarted_state["canonical_message_id"] == durable["canonical_message_id"]
    assert restarted_state["turn_id"] == durable["turn_id"]
    EVIDENCE.append({"scenario": 6, "gateway_restart_after_admission": "PASS",
                     "same_message_and_turn": True})

    # 7: Restart Conversation after a durable completed Turn, then replay the
    # exact admission body over HTTP against the restarted API.
    managed_calls_before_api_restart = len(h.peer.managed_calls)
    sends_before_api_restart = len(h.peer.accepted_messages)
    h.kill("conversation")
    await h.restart("conversation")
    api_replay = await h.client.post(
        admission_url, json=admission_request, headers=api_headers,
    )
    assert api_replay.status_code == 200 and api_replay.json()["replayed"] is True
    assert api_replay.json()["admission_id"] == str(state["admission_id"])
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
    rejected = h.update(message_id=5, update_id=112, text_value="provider rejects")
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
    unknown = h.update(message_id=6, update_id=113, text_value="ambiguous provider outcome")
    unknown_key = event_key(unknown)
    h.queue(unknown)
    unknown_state = await wait_for_state(h, unknown_key,
        lambda row: row["delivery_status"] == "UNKNOWN", "ambiguous Telegram write")
    accepted_before = len(h.peer.accepted_messages)
    ai_before = len(h.peer.managed_calls)
    duplicate_poll = h.peer.after_forced_update(canary_token, 113)
    h.queue(h.update(message_id=6, update_id=113,
                     text_value="ambiguous provider outcome"), force=True)
    await asyncio.wait_for(duplicate_poll.wait(), 12)
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
    sticky = h.update(message_id=7, update_id=114, text_value="sticky managed")
    sticky_key = event_key(sticky)
    h.queue(sticky)
    await asyncio.wait_for(h.peer.send_entered.wait(), 12)
    sticky_before = await h.state(sticky_key)
    assert sticky_before["route"] == "MANAGED"
    h.kill("gateway")
    h.peer.send_release.set()
    await h.restart("gateway", cohort_enabled=False)
    await asyncio.wait_for(h.peer.acknowledged(canary_token, 114).wait(), 12)
    sticky_after = await h.state(sticky_key)
    assert sticky_after["route"] == "MANAGED"
    assert len(h.peer.legacy_calls) == 1
    assert sum(item.get("chat_id") == 404040 and item.get("mode") == "hold_after_accept"
               for item in h.peer.accepted_messages) == 1
    rolled_back = h.update(message_id=8, update_id=115, text_value="new message after rollback")
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
    group = h.update(message_id=9, update_id=116, text_value="group legacy",
                     chat_type="group", chat_id=404042)
    group_key = event_key(group)  # This key is not submitted to Conversation.
    legacy_before = len(h.peer.legacy_calls)
    h.queue(group)
    await h.peer.wait_for(lambda: len(h.peer.legacy_calls) == legacy_before + 1,
                          "group message legacy path")
    assert (await h.rows("SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
                         group_key))[0][0] == 0
    non_text = h.update(message_id=10, update_id=117, text_value="", chat_id=404043)
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
    anonymous = h.update(message_id=11, update_id=118, text_value="anonymous",
                         chat_type="channel", chat_id=404044, anonymous=True)
    anonymous_token = str(anonymous.pop("_token"))
    before_sends = len(h.peer.accepted_messages)
    h.peer.enqueue(anonymous_token, anonymous)
    await asyncio.wait_for(h.peer.acknowledged(anonymous_token, 118).wait(), 12)
    assert len(h.peer.accepted_messages) == before_sends
    EVIDENCE.append({"scenario": "extra", "group_nontext_and_anonymous_compatibility": "PASS"})
    EVIDENCE.append({"scenario": 16, "managed_html_formatter_parity": "PASS"})
    EVIDENCE.append({"scenario": 17, "no_redis_or_legacy_session_on_managed_path": "PASS"})
    EVIDENCE.append({"scenario": 18, "business_operations": "none"})

    # Inspect every process log sink after realistic aiohttp/aiogram failures.
    all_logs = "\n".join(path.read_text(errors="replace")
                         for path in h.root.glob("*.log"))
    for secret in (*h.tokens.values(), canary_token, legacy_token,
                   h.peer.raw_provider_response):
        assert secret not in all_logs
    assert f"/bot{canary_token}/" not in all_logs
    assert f"/bot{legacy_token}/" not in all_logs
    EVIDENCE.append({"scenario": "secret_redaction", "log_sinks": "all process logs",
                     "provider_url_token_raw_response_and_configured_secrets": "absent"})

    # 10/20/21: Full service and Website regressions are proven by this process
    # stack plus the repository's existing v1/v2 suite. No Redis process exists.
    await assert_guard(h, "conversation")
    await assert_guard(h, "worker")
    await assert_guard(h, "gateway")
    assert not h.peer.request_errors, {
        "request_errors": h.peer.request_errors,
        "diagnostics": h.peer.unexpected_error_diagnostics,
        "transition_history": h.peer.transition_history,
    }
    assert all(
        item["path"] in {"work", "results"}
        and item["error_class"] in {"ConnectError", "RemoteProtocolError"}
        and "conversation" in item["transition_services"]
        for item in h.peer.expected_transition_errors
    )
    EVIDENCE.append({
        "scenario": "planned conversation restart transport evidence",
        "expected_failures": h.peer.expected_transition_errors,
        "unexpected_failures": h.peer.request_errors,
    })
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


@pytest.mark.asyncio
async def test_goal5_single_immediate_process_delivery_smoke(th) -> None:
    h = th
    token = "900001:fixture-token"
    update = h.update(message_id=505, update_id=505,
                      text_value="single immediate smoke", chat_id=406505)
    key = event_key(update)
    h.queue(update)
    await wait_for_state(
        h, key,
        lambda row: row["status"] == "COMPLETED"
        and row["delivery_status"] == "PROVIDER_ACCEPTED",
        "single immediate update provider acceptance",
    )
    await h.peer.wait_for(
        lambda: h.peer.acknowledged_update_ids[token] == [505],
        "single provider update acknowledged after terminal delivery",
    )


@pytest.mark.asyncio
async def test_goal5_same_bot_token_rotation_preserves_pinned_account(th) -> None:
    h = th
    original = "900001:fixture-token"
    rotated = "900001:rotated-fixture-token"
    h.peer.integration_rows[0]["credentials"]["BOT_TOKEN"] = rotated
    await h.peer.wait_for(
        lambda: h.peer.get_updates_calls[rotated] > 0,
        "same provider bot ID re-registered with its rotated token",
    )
    update = h.update(message_id=506, update_id=506,
                      text_value="same bot after token rotation", chat_id=406506)
    update["_token"] = rotated
    key = event_key(update)
    h.queue(update)
    await wait_for_state(
        h, key,
        lambda row: row["status"] == "COMPLETED"
        and row["delivery_status"] == "PROVIDER_ACCEPTED",
        "same-bot rotated credential terminal delivery",
    )
    await h.peer.wait_for(
        lambda: h.peer.acknowledged_update_ids[rotated] == [506],
        "rotated-token poll safely acknowledges provider event",
    )
    account = await h.rows(
        "SELECT state,bot_id FROM telegram_provider_accounts "
        "WHERE owner_id=$1 AND integration_id=$2",
        h.ids["owner"], h.ids["canary"],
    )
    assert len(account) == 1 and account[0]["state"] == "BOUND"
    assert account[0]["bot_id"] == "900001"
    assert h.peer.get_updates_calls[original] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_index,failure_mode", [
    (0, "unauthorized"), (1, "forbidden"), (2, "server_500"),
    (0, "malformed"), (1, "reset"), (2, "timeout"),
])
async def test_goal5_rejected_admission_holds_the_provider_prefix(
    th, failure_index, failure_mode,
) -> None:
    h = th
    token = "900001:fixture-token"
    update_ids = [510, 540, 900]
    updates = [h.update(
        message_id=510 + index, update_id=update_ids[index],
        text_value=f"safe prefix failure {index}", chat_id=406510 + index,
    ) for index in range(3)]
    keys = [event_key(update) for update in updates]
    failed_key = keys[failure_index]

    # Commit the prefix before injecting the failure. This makes N/N+1/N+2
    # independent of AI and delivery latency while retaining a real provider
    # getUpdates batch for the failed event and its suffix.
    for update, key in zip(updates[:failure_index], keys[:failure_index]):
        acknowledged_before = len(h.peer.acknowledged_update_ids[token])
        h.queue(update)
        await wait_for_state(
            h, key,
            lambda row: row["status"] == "COMPLETED"
            and row["delivery_status"] == "PROVIDER_ACCEPTED",
            "prior provider prefix event reaches terminal delivery",
        )
        await h.peer.wait_for(
            lambda: len(h.peer.acknowledged_update_ids[token]) == acknowledged_before + 1,
            "prior provider prefix is acknowledged",
        )
    h.peer.admission_pre_modes[failed_key].append(failure_mode)
    retry_entered = h.peer.hold_admission_request(failed_key)
    for update in updates[failure_index:]:
        h.queue(update)

    # The first request is a real aiogram update against a fake Telegram queue;
    # the proxy rejects its Conversation admission and holds the retry request.
    # While held, no provider offset may confirm the failed update or its suffix.
    await asyncio.wait_for(retry_entered.wait(), timeout=12)
    assert h.peer.admission_attempts[failed_key] >= 2
    assert h.peer.acknowledged_update_ids[token] == update_ids[:failure_index]
    assert not any(offset is not None and offset > update_ids[failure_index]
                   for offset in h.peer.get_updates_offsets[token])
    for index, key in enumerate(keys):
        count = (await h.rows(
            "SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1", key
        ))[0][0]
        assert count == (1 if index < failure_index else 0)

    h.peer.release_admission_request(failed_key)
    for update, key in zip(updates, keys):
        await wait_for_state(
            h, key,
            lambda row: row["status"] == "COMPLETED"
            and row["delivery_status"] == "PROVIDER_ACCEPTED",
            "all provider events after the admission retry",
        )
    await h.peer.wait_for(
        lambda: h.peer.acknowledged_update_ids[token] == update_ids,
        "fake Telegram deleting exactly the fully committed prefix",
    )
    assert h.peer.admission_responses[failed_key][0]["status_code"] == {
        "unauthorized": 401, "forbidden": 403, "server_500": 500,
        "malformed": 200, "reset": None, "timeout": None,
    }[failure_mode]
    assert h.peer.admission_responses[failed_key][-1]["replayed"] is False
    EVIDENCE.append({
        "scenario": "safe provider offset", "failure_index": failure_index,
        "updates": update_ids, "rejected_admission_holds_suffix": True,
        "provider_acknowledged_only_after_recovery": True,
    })


@pytest.mark.asyncio
async def test_goal5_commit_response_loss_restarts_from_absent_offset(th) -> None:
    h = th
    token = "900001:fixture-token"
    h.peer.managed_block = True
    h.peer.managed_release.clear()
    h.peer.managed_entered.clear()
    first = h.update(message_id=570, update_id=570,
                     text_value="commit before admission response loss", chat_id=406570)
    first_key = event_key(first)
    response_held = h.peer.hold_admission_response(first_key)
    h.queue(first)
    await asyncio.wait_for(response_held.wait(), timeout=12)
    committed = await wait_for_state(
        h, first_key, lambda row: row["canonical_message_id"] is not None,
        "database commit before dropping the admission HTTP response",
    )
    await asyncio.wait_for(h.peer.managed_entered.wait(), timeout=12)
    second = h.update(message_id=571, update_id=580,
                      text_value="next retained provider event", chat_id=406571)
    third = h.update(message_id=572, update_id=920,
                     text_value="gapped retained provider event", chat_id=406572)
    h.queue(second)
    h.queue(third)
    assert not h.peer.offset_advanced(token, 570)
    assert h.peer.admission_responses[first_key] == [
        {"status_code": 200, "replayed": False},
    ]

    # Killing Gateway after PostgreSQL commit but before the HTTP body reaches it
    # makes the outcome unknown to the caller. Telegram still retains all three
    # updates, so restart must begin with offset omitted and replay the first.
    h.kill("gateway")
    h.peer.release_admission_response(first_key)
    await h.restart("gateway")
    await h.peer.wait_for(
        lambda: any(row.get("replayed") is True
                    for row in h.peer.admission_responses[first_key]),
        "same provider event replayed after response-loss restart",
    )
    assert committed["canonical_message_id"] is not None
    assert any(offset is None for offset in h.peer.get_updates_offsets[token])
    h.peer.managed_release.set()
    for update in (first, second, third):
        await wait_for_state(
            h, event_key(update),
            lambda row: row["status"] == "COMPLETED"
            and row["delivery_status"] == "PROVIDER_ACCEPTED",
            "provider events recovered after committed receipt response loss",
        )
    await h.peer.wait_for(
        lambda: h.peer.acknowledged_update_ids[token] == [570, 580, 920],
        "provider offset confirming each recovered event in order",
    )
    final = await h.state(first_key)
    assert final["admission_id"] == committed["admission_id"]
    assert len(h.peer.managed_calls) == 3
    assert len(h.peer.accepted_messages) == 3
    EVIDENCE.append({
        "scenario": "unknown admission response", "commit_survived_gateway_restart": True,
        "replay_same_admission": True, "restart_offset": None,
        "provider_gaps_preserved": [570, 580, 920],
    })


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


@pytest.mark.asyncio
async def test_goal5_real_polling_bursts_max_wait_and_restart_recovery(th, sql) -> None:
    h = th
    h.settling_enabled = True
    h.settling_quiet_window_ms = 2500
    h.settling_max_wait_ms = 3000
    # The test has no retained traffic yet, so restart the actual services with
    # the exact binding policy enabled and preserve the accepted Goal4B heads.
    for name in ("conversation", "worker", "gateway"):
        await h.restart(name)

    token = "900001:fixture-token"
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    burst = [h.update(message_id=201 + index, update_id=401 + index,
                      text_value=f"two message burst {index}", chat_id=406001)
             for index in range(2)]
    burst_keys = [event_key(update) for update in burst]
    typing_blocked = h.peer.hold_typing(burst_keys[0])
    for update in burst:
        h.queue(update)
    burst_states = [await wait_for_state(
        h, key, lambda row: row["canonical_message_id"] is not None,
        "both provider burst updates durably admitted",
    ) for key in burst_keys]
    await h.peer.wait_for(lambda: len(h.peer.managed_calls) == before_ai + 1,
                          "one Managed AI execution for two admitted updates")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == before_sends + 1,
                          "one Telegram response for two admitted updates")
    assert burst_states[0]["turn_id"] == burst_states[1]["turn_id"]
    selected_ids = [str(row["canonical_message_id"]) for row in await h.rows(
        "SELECT canonical_message_id FROM telegram_admissions "
        "WHERE provider_event_id = ANY($1::text[]) ORDER BY message_id", burst_keys)]
    request_ids = h.peer.managed_calls[-1]["selected_input_message_ids"]
    assert request_ids == selected_ids and len(set(request_ids)) == 2
    assert len(h.peer.managed_calls) == before_ai + 1
    assert len(h.peer.accepted_messages) == before_sends + 1
    assert not typing_blocked.is_set(), "managed ingress must not wait on Telegram typing"
    h.peer.release_typing(burst_keys[0])

    # A three-message burst is also represented by original canonical IDs in
    # one frozen ManagedTurn request and results in one provider write.
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    three = [h.update(message_id=211 + index, update_id=411 + index,
                       text_value=f"three message burst {index}", chat_id=406002)
             for index in range(3)]
    three_keys = [event_key(update) for update in three]
    for update in three:
        h.queue(update)
    three_states = [await wait_for_state(
        h, key, lambda row: row["canonical_message_id"] is not None,
        "three provider burst updates durably admitted",
    ) for key in three_keys]
    await h.peer.wait_for(lambda: len(h.peer.managed_calls) == before_ai + 1,
                          "one Managed AI execution for three admitted updates")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == before_sends + 1,
                          "one Telegram response for three admitted updates")
    assert len({state["turn_id"] for state in three_states}) == 1
    three_ids = [str(row["canonical_message_id"]) for row in await h.rows(
        "SELECT canonical_message_id FROM telegram_admissions "
        "WHERE provider_event_id = ANY($1::text[]) ORDER BY message_id", three_keys)]
    assert h.peer.managed_calls[-1]["selected_input_message_ids"] == three_ids
    assert len(h.peer.managed_calls) == before_ai + 1
    assert len(h.peer.accepted_messages) == before_sends + 1

    # Distinct messages beyond the quiet window stay on separate Turns/replies.
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    outside_one = h.update(message_id=221, update_id=421,
                           text_value="outside window one", chat_id=406003)
    outside_one_key = event_key(outside_one)
    h.queue(outside_one)
    await wait_for_state(h, outside_one_key,
        lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "first outside-window Telegram response")
    outside_two = h.update(message_id=222, update_id=422,
                           text_value="outside window two", chat_id=406003)
    outside_two_key = event_key(outside_two)
    h.queue(outside_two)
    await wait_for_state(h, outside_two_key,
        lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "second outside-window Telegram response")
    outside_states = [await h.state(outside_one_key), await h.state(outside_two_key)]
    assert outside_states[0]["turn_id"] != outside_states[1]["turn_id"]
    assert len(h.peer.managed_calls) == before_ai + 2
    assert len(h.peer.accepted_messages) == before_sends + 2

    # A continuous burst resets quiet time but cannot exceed its stored max wait.
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    before_call_times = len(h.peer.managed_call_times)
    continuous = [h.update(message_id=231 + index, update_id=431 + index,
                           text_value=f"continuous burst {index}", chat_id=406004)
                  for index in range(3)]
    continuous_keys = [event_key(update) for update in continuous]
    async def enqueue_and_confirm(update: dict[str, object], key: str) -> None:
        h.queue(update)
        await wait_for_state(
            h, key, lambda row: row["canonical_message_id"] is not None,
            "continuous burst provider event admission",
        )
        await h.peer.wait_for(
            lambda: h.peer.offset_advanced(token, int(update["update_id"])),
            "continuous burst event durable acknowledgment",
        )

    # These gaps deliberately keep each real DB arrival inside the prior quiet
    # window. The persisted input timestamps below verify that precondition.
    await enqueue_and_confirm(continuous[0], continuous_keys[0])
    await asyncio.sleep(0.7)
    await enqueue_and_confirm(continuous[1], continuous_keys[1])
    await asyncio.sleep(0.7)
    await enqueue_and_confirm(continuous[2], continuous_keys[2])
    continuous_states = [await wait_for_state(
        h, key, lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "continuous burst response bounded by max wait",
    ) for key in continuous_keys]
    await h.peer.wait_for(lambda: len(h.peer.managed_calls) == before_ai + 1,
                          "one Managed AI execution for continuous burst")
    assert len({state["turn_id"] for state in continuous_states}) == 1
    timed_turn = await h.rows(
        "SELECT t.created_at, t.settling_burst_id, b.opened_at,b.max_cutoff_at,"
        "b.last_input_at,b.ready_at,b.closed_at,b.quiet_window_ms,b.max_wait_ms "
        "FROM agent_turns t JOIN settling_bursts b ON b.id=t.settling_burst_id "
        "JOIN telegram_immediate_turns i ON i.turn_id=t.id "
        "WHERE t.id=$1 GROUP BY t.id,b.id",
        continuous_states[0]["turn_id"],
    )
    assert len(timed_turn) == 1
    burst = timed_turn[0]
    input_times = await h.rows(
        "SELECT m.id::text AS message_id,m.created_at,m.settling_burst_id FROM messages m "
        "JOIN telegram_admissions a ON a.canonical_message_id=m.id "
        "WHERE a.provider_event_id=ANY($1::text[]) ORDER BY m.sequence",
        continuous_keys,
    )
    assert len(input_times) == 3
    assert {row["settling_burst_id"] for row in input_times} == {
        burst["settling_burst_id"]
    }
    assert all(
        later["created_at"] - earlier["created_at"] < timedelta(
            milliseconds=burst["quiet_window_ms"]
        )
        for earlier, later in zip(input_times, input_times[1:])
    )
    assert burst["max_wait_ms"] == 3000 and burst["quiet_window_ms"] == 2500
    assert burst["max_cutoff_at"] == burst["opened_at"] + timedelta(
        milliseconds=burst["max_wait_ms"]
    )
    assert burst["last_input_at"] + timedelta(
        milliseconds=burst["quiet_window_ms"]
    ) > burst["max_cutoff_at"]
    assert burst["ready_at"] == burst["closed_at"] == burst["max_cutoff_at"]
    assert burst["created_at"] >= burst["ready_at"]
    assert len(h.peer.managed_call_times) == before_call_times + 1
    assert h.peer.managed_calls[-1]["selected_input_message_ids"] == [
        row["message_id"] for row in input_times
    ]
    assert len(h.peer.accepted_messages) == before_sends + 1

    # Once Conversation has durably admitted an update, Gateway polling may
    # acknowledge it before Managed AI finishes. Restart recovers delivery from
    # Conversation's retained work; no in-memory Gateway task owns the draft.
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    h.peer.managed_block = True
    h.peer.managed_release.clear()
    h.peer.managed_entered.clear()
    recovery = h.update(message_id=241, update_id=441,
                        text_value="restart after durable admission", chat_id=406005)
    recovery_key = event_key(recovery)
    h.queue(recovery)
    admitted = await wait_for_state(h, recovery_key,
        lambda row: row["canonical_message_id"] is not None,
        "durable ingress before the Gateway restart")
    await h.peer.wait_for(lambda: h.peer.offset_advanced(token, 441),
                          "provider offset after durable Conversation admission")
    await asyncio.wait_for(h.peer.managed_entered.wait(), timeout=12)
    h.kill("gateway")
    h.peer.managed_release.set()
    await h.restart("gateway")
    recovered = await wait_for_state(h, recovery_key,
        lambda row: row["delivery_status"] == "PROVIDER_ACCEPTED",
        "Gateway restart recovers retained response")
    await h.peer.wait_for(lambda: len(h.peer.accepted_messages) == before_sends + 1,
                          "one response after Gateway restart")
    assert recovered["canonical_message_id"] == admitted["canonical_message_id"]
    assert len(h.peer.managed_calls) == before_ai + 1

    # A provider accepted the write but the response was held while Gateway
    # stopped. Recovery marks the retained intent UNKNOWN and does not resend.
    before_ai = len(h.peer.managed_calls)
    before_sends = len(h.peer.accepted_messages)
    h.peer.send_modes.append("hold_after_accept")
    h.peer.send_entered.clear()
    h.peer.send_release.clear()
    uncertain = h.update(message_id=242, update_id=442,
                         text_value="restart after provider acceptance", chat_id=406006)
    uncertain_key = event_key(uncertain)
    h.queue(uncertain)
    await asyncio.wait_for(h.peer.send_entered.wait(), timeout=15)
    h.kill("gateway")
    h.peer.send_release.set()
    await h.restart("gateway")
    uncertain_state = await wait_for_state(h, uncertain_key,
        lambda row: row["delivery_status"] == "UNKNOWN",
        "UNKNOWN after provider accepted but response was lost")
    await asyncio.wait_for(h.peer.acknowledged(token, 442).wait(), 12)
    assert uncertain_state["canonical_message_id"] is not None
    assert len(h.peer.accepted_messages) == before_sends + 1
    assert len(h.peer.managed_calls) == before_ai + 1
    EVIDENCE.append({
        "scenario": "Goal5 real polling", "two_message_burst": "one Turn, one AI call, one send",
        "three_message_burst": "one Turn, one AI call, one send",
        "outside_quiet_window": "two Turns and two sends",
        "continuous_burst": "max_wait bounded before quiet timeout",
        "durable_admission_restart": "offset advanced; response recovered",
        "provider_acceptance_restart": "UNKNOWN; no blind resend",
    })


@pytest.mark.asyncio
async def test_no_policy_history_matches_exact_accepted_gateway_source(th) -> None:
    h = th

    async def run_source(source_root: Path, *, first_update_id: int,
                         chat_id: int) -> dict[str, object]:
        token = "900001:fixture-token"
        h.peer.managed_block = True
        h.peer.managed_release.clear()
        h.peer.managed_entered.clear()
        before_ai = len(h.peer.managed_calls)
        before_sends = len(h.peer.accepted_messages)
        before_ack = len(h.peer.acknowledged_update_ids[token])
        first = h.update(
            message_id=first_update_id, update_id=first_update_id,
            text_value="no-policy first input", chat_id=chat_id,
        )
        second = h.update(
            message_id=first_update_id + 1, update_id=first_update_id + 1,
            text_value="no-policy second input", chat_id=chat_id,
        )
        first_key, second_key = event_key(first), event_key(second)
        h.queue(first)
        await asyncio.wait_for(h.peer.managed_entered.wait(), timeout=15)
        first_waiting = await wait_for_state(
            h, first_key,
            lambda row: row["canonical_message_id"] is not None and row["turn_id"] is not None,
            "first no-policy admission enters held Managed AI",
        )
        assert first_waiting["status"] in {"CLAIMED", "COMPLETED"}
        h.queue(second)

        # The first Managed request is blocked behind an event barrier. The
        # second provider update is buffered in Fake Telegram and cannot cross
        # the held first admission/turn frontier.
        second_count = await h.rows(
            "SELECT count(*) FROM telegram_admissions WHERE provider_event_id=$1",
            second_key,
        )
        assert second_count[0][0] == 0
        assert len(h.peer.managed_calls) == before_ai + 1
        assert len(h.peer.accepted_messages) == before_sends
        assert h.peer.acknowledged_update_ids[token][before_ack:] == []

        h.peer.managed_release.set()
        for update, key in ((first, first_key), (second, second_key)):
            await wait_for_state(
                h, key,
                lambda row: row["status"] == "COMPLETED"
                and row["delivery_status"] == "PROVIDER_ACCEPTED",
                "no-policy provider event reaches accepted terminal delivery",
            )
        await h.peer.wait_for(
            lambda: h.peer.acknowledged_update_ids[token][before_ack:]
            == [first_update_id, first_update_id + 1],
            "both committed provider events are acknowledged in order",
        )
        await h.peer.wait_for(
            lambda: len(h.peer.accepted_messages) == before_sends + 2,
            "two no-policy Telegram replies complete",
        )
        assert len(h.peer.managed_calls) == before_ai + 2

        states = [await h.state(key) for key in (first_key, second_key)]
        assert states[0]["conversation_id"] == states[1]["conversation_id"]
        history_rows = await h.rows(
            "SELECT sequence,actor,direction,content_type,text FROM messages "
            "WHERE conversation_id=$1 ORDER BY sequence", states[0]["conversation_id"],
        )
        call_rows = h.peer.managed_calls[before_ai:]

        def projected_request(call: dict[str, object]) -> dict[str, object]:
            messages = call["messages"]
            assert isinstance(messages, list)
            normalized = [
                {"sequence": message["sequence"], "actor": message["actor"],
                 "direction": message["direction"], "content_type": message["content_type"],
                 "text": message["text"]}
                for message in messages
            ]
            sequence_by_id = {message["id"]: message["sequence"] for message in messages}
            selected = call["selected_input_message_ids"]
            assert isinstance(selected, list)
            return {
                "input_revision": call["input_revision"],
                "authority_revision": call["authority_revision"],
                "input_first_sequence": call["input_first_sequence"],
                "input_last_sequence": call["input_last_sequence"],
                "context_last_sequence": call["context_last_sequence"],
                "messages": normalized,
                "selected_input_sequences": [sequence_by_id[item] for item in selected],
            }

        provider_outputs = [
            item["text"] for item in h.peer.accepted_messages[before_sends:]
        ]
        projection = [projected_request(row) for row in call_rows]
        normalized_history = [
            {"sequence": row["sequence"], "actor": row["actor"],
             "direction": row["direction"], "content_type": row["content_type"],
             "text": row["text"]}
            for row in history_rows
        ]
        return {
            "history": normalized_history,
            "ai_projection": projection,
            "ai_calls": len(call_rows),
            "provider_outputs": provider_outputs,
            "provider_sends": len(provider_outputs),
            "delivery_facts": [
                {"status": row["status"], "delivery_status": row["delivery_status"]}
                for row in states
            ],
        }

    await h.restart("gateway", gateway_source_root=ACCEPTED_TG_ROOT)
    accepted = await run_source(
        ACCEPTED_TG_ROOT, first_update_id=710, chat_id=406710,
    )
    await h.restart("gateway", gateway_source_root=TG_ROOT)
    current = await run_source(
        TG_ROOT, first_update_id=720, chat_id=406720,
    )
    assert accepted == current
    assert [row["actor"] for row in accepted["history"]] == [
        "CUSTOMER", "AGENT", "CUSTOMER", "AGENT",
    ]
    assert [row["input_revision"] for row in accepted["ai_projection"]] == [1, 2]
    assert accepted["ai_calls"] == accepted["provider_sends"] == 2
    assert all(row["status"] == "COMPLETED"
               and row["delivery_status"] == "PROVIDER_ACCEPTED"
               for row in accepted["delivery_facts"])
    EVIDENCE.append({
        "scenario": "no-policy accepted-source differential",
        "accepted_sha": ACCEPTED_TG_SHA,
        "matches": ["ordered customer/AGENT history", "AI projection and input revision",
                    "AI/provider call counts", "provider output order", "delivery facts"],
        "first_held_before_second_admitted": True,
    })
