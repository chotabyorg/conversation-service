"""Disposable, numeric-loopback real process fixture for Website Goal4A."""

import asyncio
import json
import os
import secrets
import socket
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url

CS_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = Path(__file__).with_name("runtime_bootstrap.py")


def private_json(path, value):
    path.write_text(json.dumps(value))
    path.chmod(0o600)


@pytest.fixture(scope="session")
def website_inputs(database_url):
    repo = Path(os.environ.get("GOAL4A_WEBSITE_REPO", "/missing"))
    python = Path(os.environ.get("GOAL4A_WEBSITE_PYTHON", "/missing"))
    url = make_url(database_url)
    if (not repo.is_absolute() or not python.is_absolute() or not python.is_file()
            or not (repo / "services/conversation_client.py").is_file()
            or (repo / ".env").exists() or (CS_ROOT / ".env").exists()
            or url.host != "127.0.0.1"):
        pytest.fail("Explicit Website candidate/runtime and loopback disposable DB required")
    return repo.resolve(), python


@pytest.fixture
def process_ports():
    listeners = []
    ports = {}
    try:
        for name in ("fake", "cs", "worker", "worker2", "website", "website2"):
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
            ports[name] = listener.getsockname()[1]
    finally:
        for listener in listeners:
            listener.close()
    return ports


@pytest.fixture
def network_allowlist(network_allowlist, process_ports):
    network_allowlist.update(("127.0.0.1", port) for port in process_ports.values())
    return network_allowlist


@dataclass
class WebsiteHarness:
    root: Path
    website_root: Path
    website_python: Path
    database_url: str = field(repr=False)
    sql: object = field(repr=False)
    ports: dict
    client: httpx.AsyncClient = field(repr=False)
    ids: dict
    tokens: dict = field(repr=False)
    processes: dict = field(default_factory=dict, repr=False)
    handles: list = field(default_factory=list, repr=False)

    def url(self, name):
        return f"http://127.0.0.1:{self.ports[name]}"

    def read(self, filename):
        return json.loads((self.root / filename).read_text())

    def observations(self):
        return self.read("observations.json")

    def body(self, **changes):
        return {"session_id": self.ids["session"], "message_id": str(uuid4()),
                "message": "synthetic communication test", **changes}

    def start(self, name, fault=None, cohort=True):
        assert name not in self.processes
        repo = self.website_root if name.startswith("website") else CS_ROOT
        python = self.website_python if name.startswith("website") else Path(sys.executable)
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONPATH": str(repo),
               "PYTHONUNBUFFERED": "1", "PYTHONDONTWRITEBYTECODE": "1"}
        grant = {"owner_id": self.ids["owner"], "integration_id": self.ids["integration"],
                 "channel": "WEBSITE_WIDGET", "provider_namespace":
                 "main-chota.website-widget.v1"}
        binding = {key: self.ids[key] for key in ("owner", "integration", "agent")}
        binding = {key + "_id": value for key, value in binding.items()}
        if name.startswith("website"):
            sessions = [self.ids["session"], self.ids["session2"]]
            policy = {self.ids["integration"]: {"mode": "SESSION_ALLOWLIST",
                      "expected_agent_id": self.ids["agent"], "session_ids": sessions}}
            env.update({"CONTROL_PLANE_URL": self.url("fake"),
                        "INTERNAL_API_TOKEN": self.tokens["cp"],
                        "AI_AGENT_SERVICE_URL": self.url("fake"),
                        "CHAT_S2S_TOKEN": self.tokens["legacy"],
                        "CONVERSATION_SERVICE_URL": self.url("cs"),
                        "CONVERSATION_S2S_TOKEN": self.tokens["website"],
                        "CONVERSATION_RESULT_WAIT_SECONDS": "4",
                        "CONVERSATION_RESULT_POLL_SECONDS": "0.05",
                        "CONVERSATION_REQUEST_TIMEOUT_SECONDS": "1",
                        "MANAGED_WEBSITE_GUARDED_INTEGRATIONS": json.dumps({
                            self.ids["integration"]: self.ids["agent"]}),
                        "MANAGED_WEBSITE_COHORTS": json.dumps(policy if cohort else {}),
                        "HOST": "127.0.0.1", "PORT": str(self.ports[name]),
                        "ALLOWED_ORIGINS": "https://fixture.example", "LOG_LEVEL": "INFO"})
        elif name != "fake":
            env.update({"DATABASE_URL": self.database_url,
                        "CONVERSATION_AUTH_PRINCIPALS": json.dumps([
                            {"principal_id": "fixture-website", "role": "WEBSITE",
                             "token": self.tokens["website"], "grants": [grant]},
                            {"principal_id": "fixture-processing", "role": "PROCESSING",
                             "token": self.tokens["processing"], "grants": [grant]}]),
                        "WEBSITE_BINDINGS": json.dumps([binding]),
                        "MANAGED_AI_BASE_URL": self.url("fake"),
                        "MANAGED_AI_BINDINGS": json.dumps([dict(binding,
                            channel="WEBSITE_WIDGET", service_token=self.tokens["managed"])]),
                        "WEBSITE_WORKER_ENABLED": "true",
                        "WEBSITE_WORKER_PRINCIPAL_ID": "fixture-processing",
                        "WEBSITE_WORKER_LEASE_SECONDS": "5",
                        "WEBSITE_WORKER_POLL_SECONDS": "0.1",
                        "WEBSITE_WORKER_HEALTH_PORT": str(self.ports[name]), "LOG_LEVEL": "INFO"})
        widgets = {self.tokens["widget"]: binding, self.tokens["other_widget"]: {
            "owner_id": self.ids["owner"], "integration_id": self.ids["other_integration"],
            "agent_id": self.ids["agent"]}}
        config_path = self.root / (name + "-bootstrap.json")
        config = {"name": name, "state_dir": str(self.root), "port": self.ports[name],
                  "allowlist": [["127.0.0.1", self.ports[key]] for key in self.ports] +
                  [["127.0.0.1", make_url(self.database_url).port or 5432]],
                  "widgets": widgets, "fault": fault}
        private_json(config_path, config)
        env["GOAL4A_BOOTSTRAP_CONFIG"] = str(config_path)
        log = (self.root / (name + ".log")).open("ab")
        self.handles.append(log)
        self.processes[name] = subprocess.Popen([str(python), str(BOOTSTRAP)], cwd=repo,
                                                env=env, stdout=log, stderr=log)

    async def wait_ready(self, name):
        path = "/health" if name.startswith("website") or name == "fake" else "/ready"
        deadline = asyncio.get_running_loop().time() + 12
        while asyncio.get_running_loop().time() < deadline:
            process = self.processes[name]
            assert process.poll() is None, f"{name} exited; inspect private fixture log"
            try:
                response = await self.client.get(self.url(name) + path)
                if response.status_code == 200:
                    assert self.read(name + "-guard.json")["denials"] == 2
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        pytest.fail(f"{name} not ready; inspect private fixture log")

    def kill(self, name):
        process = self.processes.pop(name, None)
        if process is not None:
            process.kill()
            process.wait(timeout=5)

    async def restart(self, name, **options):
        self.kill(name)
        self.start(name, **options)
        await self.wait_ready(name)

    async def chat(self, body, website="website", other=False):
        return await self.client.post(self.url(website) + "/v1/chat", json=body, headers={
            "X-Widget-Key": self.tokens["other_widget" if other else "widget"],
            "Origin": "https://fixture.example"})

    async def eventual_chat(self, body, status=200, website="website"):
        deadline = asyncio.get_running_loop().time() + 18
        while asyncio.get_running_loop().time() < deadline:
            response = await self.chat(body, website)
            if response.status_code == status:
                return response
            assert response.status_code == 503, response.text
            await asyncio.sleep(0.1)
        pytest.fail("Retained Website result did not reach expected state")

    async def barrier(self, point):
        deadline = asyncio.get_running_loop().time() + 12
        while asyncio.get_running_loop().time() < deadline:
            if (self.root / "barrier.json").exists():
                value = self.read("barrier.json")
                assert value["point"] == point
                return value
            await asyncio.sleep(0.02)
        pytest.fail("Required process crash barrier not reached")

    async def rows(self, statement, **params):
        async with self.sql.connect() as connection:
            return [dict(row) for row in (await connection.execute(
                text(statement), params)).mappings()]

    async def state(self, message_id):
        values = await self.rows("SELECT a.id AS admission_id,a.route,a.canonical_message_id,"
            "a.conversation_id,t.id AS turn_id,t.status,t.failure_code,t.claim_epoch,"
            "t.managed_dispatch_epoch,t.draft_text,t.operation_refs,"
            "d.id AS delivery_id,d.status AS delivery_status FROM website_admissions a "
            "LEFT JOIN website_immediate_turns i ON i.message_id=a.canonical_message_id "
            "LEFT JOIN agent_turns t ON t.id=i.turn_id "
            "LEFT JOIN delivery_attempts d ON d.source_turn_id=t.id WHERE a.message_id=:id",
            id=message_id)
        assert len(values) == 1
        return values[0]

    async def close(self):
        for name in list(self.processes):
            self.kill(name)
        for log in self.handles:
            log.close()
        await self.client.aclose()


@pytest_asyncio.fixture
async def wh(sql, database_url, website_inputs, process_ports, tmp_path):
    # Parent suite's reset cascades managed journals; LEGACY reservations have no
    # Message FK, so explicitly clean them in this isolated disposable DB as well.
    async with sql.begin() as connection:
        await connection.execute(text("TRUNCATE website_admissions CASCADE"))
    ids = {name: str(uuid4()) for name in (
        "owner", "integration", "other_integration", "agent", "session", "session2")}
    tokens = {name: secrets.token_urlsafe(32) for name in (
        "widget", "other_widget", "cp", "legacy", "managed", "website", "processing")}
    private_json(tmp_path / "observations.json", {
        "managed": [], "effects": [], "legacy": [], "evidence": []})
    client = httpx.AsyncClient(timeout=20, trust_env=False)
    h = WebsiteHarness(tmp_path, *website_inputs, database_url, sql, process_ports,
                       client, ids, tokens)
    try:
        h.start("fake")
        await h.wait_ready("fake")
        yield h
    finally:
        await h.close()


async def start_chain(h, fault=None):
    h.start("cs", fault=fault if fault in ("A", "G") else None)
    await h.wait_ready("cs")
    h.start("worker", fault=fault if fault in ("B", "C", "D", "E") else None)
    await h.wait_ready("worker")
    h.start("website", fault=fault if fault in ("F", "H") else None)
    await h.wait_ready("website")


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    from tests.website_cross_service.acceptance_http import EVIDENCE
    if EVIDENCE:
        terminalreporter.write_line("Goal4A real process crash evidence (synthetic fixture IDs):")
        for row in EVIDENCE:
            terminalreporter.write_line(json.dumps(row, default=str, sort_keys=True))
        path = os.environ.get("GOAL4A_EVIDENCE_PATH")
        if path:
            target = Path(path)
            if not target.is_absolute() or target.parent != Path("/private/tmp"):
                raise RuntimeError("Explicit private temporary evidence path required")
            target.write_text(json.dumps(EVIDENCE, default=str, indent=2))
            target.chmod(0o600)
