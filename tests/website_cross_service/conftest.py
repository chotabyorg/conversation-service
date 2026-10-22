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
    path.write_text(json.dumps(value, default=str))
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
    replies: dict = field(default_factory=dict)
    settling_enabled: bool = False
    settling_quiet_window_ms: int = 300
    settling_max_wait_ms: int = 1200
    multiple_scopes: bool = False

    def widget_scopes(self):
        scopes = [
            ("widget", self.ids["owner"], self.ids["integration"]),
            ("other_widget", self.ids["owner"], self.ids["other_integration"]),
        ]
        if self.multiple_scopes:
            scopes.append(("third_widget", self.ids["owner2"], self.ids["integration2"]))
        return [dict(widget_key=self.tokens[token], owner_id=owner,
                     integration_id=integration, agent_id=self.ids["agent"])
                for token, owner, integration in scopes]

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
        if os.name == "nt":
            # Winsock needs SystemRoot even in an otherwise isolated process.
            env["SystemRoot"] = os.environ["SystemRoot"]
        scopes = self.widget_scopes()
        grants = [{"owner_id": item["owner_id"], "integration_id": item["integration_id"],
                   "channel": "WEBSITE_WIDGET", "provider_namespace":
                   "main-chota.website-widget.v1"} for item in scopes]
        bindings = [{"owner_id": item["owner_id"], "integration_id": item["integration_id"],
                     "agent_id": item["agent_id"]} for item in scopes]
        if name.startswith("website"):
            sessions = [self.ids["session"], self.ids["session2"]]
            scope_integrations = ([item["integration_id"] for item in scopes]
                                  if self.multiple_scopes else [self.ids["integration"]])
            guard_values = {integration: self.ids["agent"]
                            for integration in scope_integrations}
            policy = {integration: {"mode": "SESSION_ALLOWLIST",
                      "expected_agent_id": self.ids["agent"], "session_ids": sessions}
                      for integration in scope_integrations}
            env.update({"CONTROL_PLANE_URL": self.url("fake"),
                        "INTERNAL_API_TOKEN": self.tokens["cp"],
                        "AI_AGENT_SERVICE_URL": self.url("fake"),
                        "CHAT_S2S_TOKEN": self.tokens["legacy"],
                        "CONVERSATION_SERVICE_URL": self.url("cs"),
                        "CONVERSATION_S2S_TOKEN": self.tokens["website"],
                        "CONVERSATION_RESULT_WAIT_SECONDS": "4",
                        "CONVERSATION_RESULT_POLL_SECONDS": "0.05",
                        "CONVERSATION_RESULT_POLL_INITIAL_SECONDS": "0.05",
                        "CONVERSATION_REQUEST_TIMEOUT_SECONDS":
                            "10" if self.settling_enabled else "1",
                        "MANAGED_WEBSITE_GUARDED_INTEGRATIONS": json.dumps(guard_values),
                        "MANAGED_WEBSITE_COHORTS": json.dumps(policy if cohort else {}),
                        "HOST": "127.0.0.1", "PORT": str(self.ports[name]),
                        "ALLOWED_ORIGINS": "https://fixture.example", "LOG_LEVEL": "INFO"})
        elif name != "fake":
            env.update({"DATABASE_URL": self.database_url,
                        "CONVERSATION_AUTH_PRINCIPALS": json.dumps([
                            {"principal_id": "fixture-website", "role": "WEBSITE",
                             "token": self.tokens["website"], "grants": grants},
                            {"principal_id": "fixture-processing", "role": "PROCESSING",
                             "token": self.tokens["processing"], "grants": grants}]),
                        "WEBSITE_BINDINGS": json.dumps(bindings),
                        "MANAGED_AI_BASE_URL": self.url("fake"),
                        "MANAGED_AI_BINDINGS": json.dumps([
                            dict(binding, channel="WEBSITE_WIDGET",
                                 service_token=self.tokens["managed"])
                            for binding in bindings]),
                        "WEBSITE_WORKER_ENABLED": "true",
                        "WEBSITE_WORKER_PRINCIPAL_ID": "fixture-processing",
                        "WEBSITE_WORKER_LEASE_SECONDS": "5",
                        "WEBSITE_WORKER_POLL_SECONDS": "0.1",
                        "WEBSITE_WORKER_HEALTH_PORT": str(self.ports[name]), "LOG_LEVEL": "INFO"})
            if self.settling_enabled:
                env["TURN_SETTLING_POLICIES"] = json.dumps([{
                    "version": "website-process-v1",
                    "channel": "WEBSITE_WIDGET",
                    "integration_id": self.ids["integration"],
                    "quiet_window_ms": self.settling_quiet_window_ms,
                    "max_wait_ms": self.settling_max_wait_ms,
                }])
        widgets = {item["widget_key"]: {key: item[key] for key in
                   ("owner_id", "integration_id", "agent_id")} for item in scopes}
        config_path = self.root / (name + "-bootstrap.json")
        config = {"name": name, "state_dir": str(self.root), "port": self.ports[name],
                  "allowlist": [["127.0.0.1", self.ports[key]] for key in self.ports] +
                  [["127.0.0.1", make_url(self.database_url).port or 5432]],
                  "widgets": widgets, "fault": fault, "replies": self.replies}
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

    async def chat(self, body, website="website", other=False, widget=None):
        widget_key = widget or ("other_widget" if other else "widget")
        return await self.client.post(self.url(website) + "/v1/chat", json=body, headers={
            "X-Widget-Key": self.tokens[widget_key],
            "Origin": "https://fixture.example"})

    async def eventual_chat(self, body, status=200, website="website", widget=None):
        deadline = asyncio.get_running_loop().time() + 18
        while asyncio.get_running_loop().time() < deadline:
            response = await self.chat(body, website, widget=widget)
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

    async def diagnostic_snapshot(self, label, message_id, *, extra=None):
        """Persist safe process/DB state before fixture teardown destroys the DB."""
        try:
            pg_activity = await self.rows(
                "SELECT pid,application_name,state,wait_event_type,wait_event,"
                "pg_blocking_pids(pid) AS blocking_pids,"
                "clock_timestamp()-query_start AS query_age,left(query,200) AS query "
                "FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid"
            )
        except Exception as exc:  # Diagnostics must not change test behavior.
            pg_activity = [{"error_type": type(exc).__name__}]
        processes = {}
        for name, process in self.processes.items():
            sample = {"pid": process.pid, "alive": process.poll() is None,
                      "returncode": process.returncode}
            if process.poll() is None:
                try:
                    sample["cpu_percent"] = subprocess.run(
                        ["ps", "-o", "%cpu=", "-p", str(process.pid)],
                        capture_output=True, text=True, timeout=1, check=False,
                    ).stdout.strip()
                except (OSError, subprocess.TimeoutExpired):
                    sample["cpu_percent"] = None
            processes[name] = sample
        admissions = await self.rows(
            "SELECT id,owner_id,integration_id,agent_id,session_id,message_id,route,"
            "conversation_id,canonical_message_id FROM website_admissions "
            "WHERE message_id=:id ORDER BY created_at,id", id=message_id,
        )
        conversations = sorted({str(row["conversation_id"]) for row in admissions
                                if row["conversation_id"] is not None})
        snapshot = {"label": label, "message_id": message_id, "admissions": admissions,
                    "managed_observations": self.observations(), "pg_activity": pg_activity,
                    "processes": processes, "extra": extra or {}}
        if conversations:
            conversation_id = conversations[0]
            snapshot["conversation_id"] = conversation_id
            snapshot["turns"] = await self.rows(
                "SELECT id,status,failure_code,claim_epoch,managed_dispatch_epoch,draft_text,"
                "operation_refs,settling_revision,created_at FROM agent_turns "
                "WHERE conversation_id=:id ORDER BY created_at,id", id=conversation_id,
            )
            snapshot["deliveries"] = await self.rows(
                "SELECT id,source_turn_id,status,revision,error_code,"
                "expected_settling_revision,fenced_settling_revision,created_at,updated_at "
                "FROM delivery_attempts WHERE conversation_id=:id ORDER BY created_at,id",
                id=conversation_id,
            )
            snapshot["response_attempts"] = await self.rows(
                "SELECT id,admission_id,delivery_id,send_authorized,outcome,created_at,finished_at "
                "FROM website_response_attempts WHERE conversation_id=:id "
                "ORDER BY created_at,id", id=conversation_id,
            )
            snapshot["reconciliations"] = await self.rows(
                "SELECT id,delivery_attempt_id,reconciliation_key,"
                "expected_revision,result_revision,status,"
                "expected_settling_revision,result_settling_revision,accepted_at "
                "FROM delivery_reconciliations WHERE conversation_id=:id "
                "ORDER BY accepted_at,id", id=conversation_id,
            )
            snapshot["messages"] = await self.rows(
                "SELECT id,sequence,actor,direction,content_type,source_agent_turn_id,text "
                "FROM messages WHERE conversation_id=:id ORDER BY sequence,id",
                id=conversation_id,
            )
        target = self.root / "diagnostic-snapshots.json"
        values = json.loads(target.read_text()) if target.exists() else []
        values.append(snapshot)
        private_json(target, values)
        return snapshot

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
        "owner", "owner2", "integration", "other_integration", "integration2",
        "agent", "session", "session2")}
    tokens = {name: secrets.token_urlsafe(32) for name in (
        "widget", "other_widget", "third_widget", "cp", "legacy", "managed",
        "website", "processing")}
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
