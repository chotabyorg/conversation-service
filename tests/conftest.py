"""Real PostgreSQL fixtures. No runtime URL fallback and no skipped DB tests."""

import asyncio
import json
import os
import secrets
import socket
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from services.common import (
    advance_test_database_clock,
    reset_test_database_clock,
    set_test_database_clock,
)

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "/internal/v1"
LOCK_EVIDENCE: list[dict[str, Any]] = []


@dataclass
class TestDatabaseClock:
    current: datetime

    def advance_to(self, value: datetime) -> None:
        if value < self.current:
            raise ValueError("Test database clock cannot move backwards")
        self.current = value
        advance_test_database_clock(value)


@pytest.fixture
def test_database_clock() -> TestDatabaseClock:
    clock = TestDatabaseClock(datetime.now(UTC))
    token = set_test_database_clock(clock.current)
    try:
        yield clock
    finally:
        reset_test_database_clock(token)


@pytest.fixture(scope="session")
def database_url() -> str:
    value = os.environ.get("TEST_DATABASE_URL")
    if not value:
        pytest.fail("TEST_DATABASE_URL is required: provide a disposable PostgreSQL16 DB.")
    url = make_url(value)
    if os.environ.get("TEST_DATABASE_DISPOSABLE") != "yes":
        pytest.fail("TEST_DATABASE_DISPOSABLE=yes is required before destructive test cleanup.")
    if url.drivername != "postgresql+asyncpg" or url.host not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("Tests require a loopback PostgreSQL+asyncpg disposable URL.")
    if not url.database or "test" not in url.database.lower():
        pytest.fail("Disposable database name must contain 'test'.")
    if os.environ.get("PYTEST_XDIST_WORKER"):
        pytest.fail("This DB suite must run without xdist or a shared cleanup database.")
    return value


@pytest.fixture(scope="session", autouse=True)
def migrated(database_url: str) -> None:
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    env["CONVERSATION_AUTH_PRINCIPALS"] = json.dumps(
        [
            {
                "principal_id": "migration-test",
                "token": secrets.token_urlsafe(32),
                "grants": [
                    {
                        "owner_id": "test-owner",
                        "integration_id": str(uuid4()),
                        "channel": "TELEGRAM",
                        "provider_namespace": "test",
                    }
                ],
            }
        ]
    )
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode:
        pytest.fail(
            "Disposable PostgreSQL migration failed. "
            + result.stderr.replace(database_url, "[REDACTED_DATABASE_URL]")
        )


@pytest.fixture
def network_allowlist(database_url: str) -> set[tuple[str, int]]:
    """Only explicit fresh test endpoints may extend the designated DB sockets."""
    port = make_url(database_url).port or 5432
    return {("127.0.0.1", port), ("::1", port)}


@pytest.fixture(autouse=True)
def no_external_network(
    monkeypatch: pytest.MonkeyPatch, network_allowlist: set[tuple[str, int]]
) -> None:
    """Ordinary tests remain PG-only; harness fixtures add exact loopback ports."""
    original = socket.socket.connect
    if sys.platform == "win32":
        # Windows implements asyncio's internal socketpair through a temporary
        # loopback listener. The socketpair API accepts no remote endpoint.
        original_pair = socket.socketpair

        def local_socketpair(*args: Any, **kwargs: Any) -> Any:
            with monkeypatch.context() as local:
                local.setattr(socket.socket, "connect", original)
                return original_pair(*args, **kwargs)

        monkeypatch.setattr(socket, "socketpair", local_socketpair)

    def guarded(sock: socket.socket, address: Any) -> Any:
        if sock.family in {socket.AF_INET, socket.AF_INET6}:
            host, target_port = address[:2]
            assert host in {"127.0.0.1", "::1"} and (host, target_port) in network_allowlist, (
                "Unexpected non-PostgreSQL endpoint outside the explicit test allowlist"
            )
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)


@pytest_asyncio.fixture
async def sql(database_url: str):
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as connection:
        version = await connection.scalar(text("SHOW server_version_num"))
        assert 160000 <= int(version) < 170000, "Required real PostgreSQL16"
        await connection.execute(
            text(
                "TRUNCATE website_creation_buckets, inbox_command_receipts, "
                "inbox_grant_lifecycle_receipts, "
                "handoff_cases, inbox_grant_scopes, external_effect_permits, "
                "telegram_response_attempts, "
                "telegram_immediate_turns, "
                "telegram_admissions, telegram_provider_accounts, settling_bursts, "
                "website_response_attempts, website_immediate_turns, website_admissions, "
                "delivery_reconciliations, delivery_attempts, agent_turns, messages, "
                "inbound_events, conversations CASCADE"
            )
        )
    try:
        yield engine
    finally:
        await engine.dispose()


@dataclass
class Harness:
    client: httpx.AsyncClient
    database: Any
    settings: Any
    tokens: dict[str, str] = field(repr=False)
    grants: dict[str, dict[str, Any]]

    def headers(self, principal: str = "a") -> dict[str, str]:
        return {"X-Service-Token": self.tokens[principal]}

    def event(self, principal: str = "a", **changes: Any) -> dict[str, Any]:
        payload = dict(
            self.grants[principal],
            provider_thread_id="thread-123",
            provider_event_id=str(uuid4()),
            text="hello",
        )
        payload.update(changes)
        return payload

    async def call(
        self, method: str, path: str, payload: Any = None, principal: str = "a"
    ) -> httpx.Response:
        return await self.client.request(
            method, PREFIX + path, json=payload, headers=self.headers(principal)
        )

    async def ingest(self, principal: str = "a", **changes: Any) -> dict[str, Any]:
        response = await self.call(
            "POST", "/inbound-events", self.event(principal, **changes), principal
        )
        assert response.status_code == 201, response.text
        return response.json()

    async def select(
        self, accepted: dict[str, Any], **changes: Any
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        conversation = accepted["conversation"]
        payload = dict(
            selection_key=str(uuid4()),
            input_first_sequence=1,
            input_last_sequence=conversation["last_message_sequence"],
            expected_input_revision=conversation["input_revision"],
            expected_authority_revision=conversation["authority_revision"],
        )
        payload.update(changes)
        response = await self.call("POST", f"/conversations/{conversation['id']}/turns", payload)
        assert response.status_code == 201, response.text
        return response.json()["turn"], payload

    async def claim(self, turn: dict[str, Any], **changes: Any) -> dict[str, Any]:
        payload = dict(worker_ref="worker-a", lease_seconds=30)
        payload.update(changes)
        response = await self.call("POST", f"/turns/{turn['id']}/claim", payload)
        assert response.status_code == 200, response.text
        return response.json()

    @staticmethod
    def proof(claim: dict[str, Any]) -> dict[str, Any]:
        return dict(claim_epoch=claim["turn"]["claim_epoch"], claim_token=claim["claim_token"])

    async def draft(
        self, accepted: dict[str, Any] | None = None
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        accepted = accepted or await self.ingest()
        turn, _ = await self.select(accepted)
        claim = await self.claim(turn)
        response = await self.call(
            "POST",
            f"/turns/{turn['id']}/complete",
            dict(self.proof(claim), outcome="DRAFT_READY", draft_text="candidate answer"),
        )
        assert response.status_code == 200, response.text
        return accepted, response.json()["turn"]

    async def delivery(self) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        accepted, turn = await self.draft()
        payload = dict(
            delivery_key=str(uuid4()), source_turn_id=turn["id"], expected_authority_revision=0
        )
        response = await self.call(
            "POST", f"/conversations/{accepted['conversation']['id']}/delivery-attempts", payload
        )
        assert response.status_code == 201, response.text
        return accepted, response.json()["attempt"], payload

    async def reconcile(self, attempt: dict[str, Any], status: str, **changes: Any):
        payload = dict(
            reconciliation_key=str(uuid4()), expected_revision=attempt["revision"], status=status
        )
        if status == "DISPATCH_REQUESTED":
            payload["expected_authority_revision"] = attempt["authority_revision"]
        payload.update(changes)
        response = await self.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", payload)
        assert response.status_code == 200, response.text
        return response.json()["attempt"], payload


@asynccontextmanager
async def app_client(settings: Any, database_url: str):
    from db.database import Database
    from main import create_app

    database = Database(database_url)
    app = create_app(settings=settings, database=database)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, database


@pytest_asyncio.fixture
async def h(sql, database_url: str):
    from core.config import Settings

    integration_a, integration_b = str(uuid4()), str(uuid4())
    grants = {
        "a": dict(
            owner_id="owner-a",
            integration_id=integration_a,
            channel="TELEGRAM",
            provider_namespace="account-a",
        ),
        "b": dict(
            owner_id="owner-b",
            integration_id=integration_b,
            channel="INSTAGRAM",
            provider_namespace="account-b",
        ),
        "other_integration": dict(
            owner_id="owner-a",
            integration_id=integration_b,
            channel="TELEGRAM",
            provider_namespace="account-a",
        ),
        "other_owner": dict(
            owner_id="owner-b",
            integration_id=integration_a,
            channel="TELEGRAM",
            provider_namespace="account-a",
        ),
    }
    tokens = {name: secrets.token_urlsafe(32) for name in [*grants, "multi", "namespace"]}
    registry = [
        dict(principal_id=name, token=tokens[name], grants=[grant])
        for name, grant in grants.items()
    ]
    registry.append(
        dict(principal_id="multi", token=tokens["multi"], grants=[grants["a"], grants["b"]])
    )
    registry.append(
        dict(
            principal_id="namespace",
            token=tokens["namespace"],
            grants=[grants["a"], dict(grants["a"], provider_namespace="account-extra")],
        )
    )
    settings = Settings(database_url=database_url, auth_principals=registry, _env_file=None)
    async with app_client(settings, database_url) as (client, database):
        yield Harness(client, database, settings, tokens, grants)


def error(response: httpx.Response, code: str, status: int = 409) -> None:
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"] == code, response.text
    assert response.headers["X-Request-ID"] == response.json()["detail"]["request_id"]


async def observed_waiters(sql, blocker: int, count: int, scenario: str) -> list[dict[str, Any]]:
    deadline = asyncio.get_running_loop().time() + 8
    while asyncio.get_running_loop().time() < deadline:
        # New connection and transaction every poll prevents stale statistics snapshots.
        async with sql.connect() as observer:
            observer_pid = await observer.scalar(text("SELECT pg_backend_pid()"))
            rows = (
                (
                    await observer.execute(
                        text(
                            "SELECT pid, wait_event_type, wait_event, xact_start, "
                            "pg_blocking_pids(pid) AS blockers FROM pg_stat_activity "
                            "WHERE datname=current_database() AND wait_event_type='Lock'"
                        )
                    )
                )
                .mappings()
                .all()
            )
        live = [dict(row) for row in rows if row["pid"] not in {observer_pid, blocker}]
        if len(live) >= count and any(blocker in row["blockers"] for row in live):
            assert len({row["pid"] for row in live}) >= count
            assert all(row["xact_start"] is not None and row["blockers"] for row in live)
            LOCK_EVIDENCE.append(
                dict(
                    scenario=scenario,
                    blocker_pid=blocker,
                    observer_pid=observer_pid,
                    workers=[
                        dict(pid=row["pid"], blockers=row["blockers"], wait=row["wait_event"])
                        for row in live
                    ],
                )
            )
            return live
        await asyncio.sleep(0.01)
    pytest.fail(f"No actual independent PostgreSQL transaction overlap observed: {scenario}")


async def locked_race(sql, conversation_id: str, calls: list[Any], scenario: str):
    tasks: list[asyncio.Task[Any]] = []
    try:
        async with sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                {"id": conversation_id},
            )
            tasks = [asyncio.create_task(call) for call in calls]
            await observed_waiters(sql, pid, len(tasks), scenario)
            assert not any(task.done() for task in tasks)
        return await asyncio.wait_for(asyncio.gather(*tasks), timeout=8)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    if LOCK_EVIDENCE:
        terminalreporter.write_line("Observed PostgreSQL transaction overlap (safe PID evidence):")
        for record in LOCK_EVIDENCE:
            terminalreporter.write_line(json.dumps(record, sort_keys=True))
