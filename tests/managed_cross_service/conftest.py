"""Explicit disposable PG16 and actual independent service-process fixtures."""

import asyncio
import base64
import json
import os
import re
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
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.managed_cross_service.fake_http import ScriptedServer

CS_ROOT = Path(__file__).resolve().parents[2]
PREFIX = "/internal/v1"


@pytest.fixture(scope="session")
def cross_inputs(database_url):
    ai_url = os.environ.get("GOAL2_AI_TEST_DATABASE_URL", "")
    repo = os.environ.get("GOAL2_AI_REPO", "")
    python = os.environ.get("GOAL2_AI_PYTHON", "")
    if (
        not ai_url
        or not repo
        or not python
        or os.environ.get("GOAL2_AI_TEST_DATABASE_DISPOSABLE") != "yes"
    ):
        pytest.fail("Explicit AI candidate/runtime/disposable database inputs are required")
    try:
        url = make_url(ai_url)
    except Exception:
        pytest.fail("Invalid explicit AI disposable database configuration")
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host != "127.0.0.1"
        or not url.database
        or "test" not in url.database.lower()
        or not url.username
        or not url.password
    ):
        pytest.fail(
            "AI database must be an explicit numeric-loopback disposable PostgreSQL test DB"
        )
    cs_url = make_url(database_url)
    if cs_url.host != "127.0.0.1" or (url.host, url.port, url.database) == (
        cs_url.host,
        cs_url.port,
        cs_url.database,
    ):
        pytest.fail("Two distinct numeric-loopback disposable databases are required")
    ai_root, ai_python = Path(repo), Path(python)
    if (
        not ai_root.is_absolute()
        or not ai_python.is_absolute()
        or not ai_python.is_file()
        or not (ai_root / "tests/goal2_cross_service_bootstrap.py").is_file()
        or not (ai_root / "api/internal/managed_turns.py").is_file()
        or (ai_root / ".env").exists()
        or (CS_ROOT / ".env").exists()
    ):
        pytest.fail("Explicit checked candidate/runtime/bootstrap required; .env is forbidden")
    return ai_url, ai_root.resolve(), ai_python


@pytest.fixture
def listeners():
    sockets = {}
    try:
        for name in ["cs", "ai", "model", "kb"]:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(64)
            sockets[name] = listener
        yield sockets
    finally:
        for listener in sockets.values():
            listener.close()


@pytest.fixture
def network_allowlist(network_allowlist, listeners, cross_inputs):
    ai_url = make_url(cross_inputs[0])
    network_allowlist.add(("127.0.0.1", ai_url.port or 5432))
    network_allowlist.update(listener.getsockname() for listener in listeners.values())
    return network_allowlist


def private_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False))
    path.chmod(0o600)


def clean_env():
    # No inherited secrets, proxies or deployment/runtime config.
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
    }


@pytest.fixture(scope="session")
def ai_migrated(cross_inputs, tmp_path_factory):
    ai_url, ai_root, ai_python = cross_inputs
    url = make_url(ai_url)
    env = clean_env()
    env.update(
        POSTGRES_HOST="127.0.0.1",
        POSTGRES_PORT=str(url.port),
        POSTGRES_DB=url.database,
        POSTGRES_USER=url.username,
        POSTGRES_PASSWORD=url.password,
        POSTGRES_ECHO="false",
    )
    log_path = tmp_path_factory.mktemp("goal2-ai-migration") / "migration.log"
    with log_path.open("wb") as log:
        result = subprocess.run(
            [str(ai_python), "-m", "alembic", "upgrade", "head"],
            cwd=ai_root,
            env=env,
            stdout=log,
            stderr=log,
            timeout=30,
        )
    assert result.returncode == 0, "Real AI migration failed; inspect private fixture log"
    return ai_url


@pytest.fixture
def cs_deadline():
    return 8.0


@dataclass
class CrossHarness:
    root: Path
    ai_root: Path
    ai_python: Path
    ai_url: str = field(repr=False)
    cs_url: str = field(repr=False)
    sql: object = field(repr=False)
    ai_sql: object = field(repr=False)
    client: httpx.AsyncClient = field(repr=False)
    listeners: dict = field(repr=False)
    model: ScriptedServer = field(repr=False)
    kb: ScriptedServer = field(repr=False)
    tokens: dict = field(repr=False)
    ids: dict
    grants: dict
    processes: dict = field(default_factory=dict, repr=False)
    handles: list = field(default_factory=list, repr=False)
    cs_env: dict = field(default_factory=dict, repr=False)
    ai_env: dict = field(default_factory=dict, repr=False)

    def read(self, name):
        return json.loads((self.root / name).read_text())

    def scenario(self, **value):
        private_json(self.root / "scenario.json", value)

    def observations(self):
        return self.read("observations.json")

    def url(self, service):
        return f"http://127.0.0.1:{self.listeners[service].getsockname()[1]}"

    async def ready(self, service):
        deadline = asyncio.get_running_loop().time() + 20
        while asyncio.get_running_loop().time() < deadline:
            assert self.processes[service].poll() is None, "Owned service process exited"
            try:
                response = await self.client.get(self.url(service) + "/health", timeout=0.3)
                if response.status_code == 200:
                    if service == "cs":
                        response = await self.client.get(self.url(service) + "/ready", timeout=0.3)
                        assert response.status_code == 200, "Migrated Conversation not ready"
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.03)
        pytest.fail("Owned service readiness deadline exceeded")

    async def start(self, service):
        env = self.cs_env if service == "cs" else self.ai_env
        python = sys.executable if service == "cs" else str(self.ai_python)
        module = (
            "tests.managed_cross_service.runtime_bootstrap"
            if service == "cs"
            else "tests.goal2_cross_service_bootstrap"
        )
        log = (self.root / f"{service}-{len(self.handles)}.log").open("wb")
        self.handles.append(log)
        self.processes[service] = subprocess.Popen(
            [python, "-m", module],
            cwd=CS_ROOT if service == "cs" else self.ai_root,
            env=env,
            stdout=log,
            stderr=log,
            pass_fds=(self.listeners[service].fileno(),),
            start_new_session=True,
        )
        await self.ready(service)

    async def stop(self, service, abrupt=False):
        process = self.processes.get(service)
        if process is None or process.poll() is not None:
            return
        process.kill() if abrupt else process.terminate()
        try:
            await asyncio.to_thread(process.wait, 6)
        except subprocess.TimeoutExpired:
            process.kill()
            await asyncio.to_thread(process.wait, 3)

    async def restart_cs(self, abrupt=False, binding_agent=None):
        await self.stop("cs", abrupt)
        if binding_agent:
            bindings = json.loads(self.cs_env["MANAGED_AI_BINDINGS"])
            bindings[0]["agent_id"] = binding_agent
            self.cs_env["MANAGED_AI_BINDINGS"] = json.dumps(bindings)
        await self.start("cs")

    async def request(self, method, path, value=None, principal="a"):
        return await self.client.request(
            method,
            self.url("cs") + PREFIX + path,
            json=value,
            headers={"X-Service-Token": self.tokens[principal]},
        )

    async def ingest(self, text_value="Привет", thread=None, principal="a"):
        value = dict(
            self.grants[principal],
            provider_thread_id=thread or str(uuid4()),
            provider_event_id=str(uuid4()),
            text=text_value,
        )
        response = await self.request("POST", "/inbound-events", value, principal)
        assert response.status_code == 201
        return response.json()

    async def turn(self, accepted, first=None, last=None, principal="a"):
        conversation = (
            await self.request(
                "GET", "/conversations/" + accepted["conversation"]["id"], principal=principal
            )
        ).json()
        value = dict(
            selection_key=str(uuid4()),
            input_first_sequence=first or 1,
            input_last_sequence=last or conversation["last_message_sequence"],
            expected_input_revision=conversation["input_revision"],
            expected_authority_revision=conversation["authority_revision"],
        )
        response = await self.request(
            "POST", "/conversations/" + conversation["id"] + "/turns", value, principal
        )
        assert response.status_code == 201
        return response.json()["turn"]

    async def claim(self, turn, principal="a"):
        response = await self.request(
            "POST",
            f"/turns/{turn['id']}/claim",
            {"worker_ref": "cross-service", "lease_seconds": 300},
            principal,
        )
        assert response.status_code == 200
        return response.json()

    @staticmethod
    def proof(claim):
        return {"claim_epoch": claim["turn"]["claim_epoch"], "claim_token": claim["claim_token"]}

    async def execute(self, claim, principal="a"):
        return await self.request(
            "POST", f"/turns/{claim['turn']['id']}/execute", self.proof(claim), principal
        )

    async def selected(self, text_value="Привет", first=None, last=None):
        accepted = await self.ingest(text_value)
        turn = await self.turn(accepted, first, last)
        return accepted, await self.claim(turn)

    async def historical(self, conversation_id, actor, content):
        async with self.sql.begin() as connection:
            sequence = await connection.scalar(
                text(
                    "UPDATE conversations SET last_message_sequence=last_message_sequence+1 "
                    "WHERE id=:id RETURNING last_message_sequence"
                ),
                {"id": conversation_id},
            )
            message_id = str(uuid4())
            await connection.execute(
                text(
                    "INSERT INTO messages(id,conversation_id,sequence,actor,direction,"
                    "content_type,text) "
                    "VALUES(:id,:conversation,:sequence,:actor,'OUTBOUND','TEXT',:content)"
                ),
                {
                    "id": message_id,
                    "conversation": conversation_id,
                    "sequence": sequence,
                    "actor": actor,
                    "content": content,
                },
            )
        return message_id

    async def snapshot(self, conversation_id):
        async with self.sql.connect() as connection:
            messages = (
                (
                    await connection.execute(
                        text("SELECT * FROM messages WHERE conversation_id=:id ORDER BY sequence"),
                        {"id": conversation_id},
                    )
                )
                .mappings()
                .all()
            )
            counters = (
                await connection.execute(
                    text(
                        "SELECT last_message_sequence,input_revision,authority_revision "
                        "FROM conversations "
                        "WHERE id=:id"
                    ),
                    {"id": conversation_id},
                )
            ).one()
            deliveries = await connection.scalar(text("SELECT count(*) FROM delivery_attempts"))
            reconciliations = await connection.scalar(
                text("SELECT count(*) FROM delivery_reconciliations")
            )
        async with self.ai_sql.connect() as connection:
            sessions = await connection.scalar(text("SELECT count(*) FROM conversation_sessions"))
        return (
            [dict(row) for row in messages],
            tuple(counters),
            deliveries,
            reconciliations,
            sessions,
        )

    async def expire(self, turn_id):
        async with self.sql.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE agent_turns SET lease_expires_at="
                    "clock_timestamp()+interval '150 milliseconds' "
                    "WHERE id=:id"
                ),
                {"id": turn_id},
            )
        deadline = asyncio.get_running_loop().time() + 4
        while asyncio.get_running_loop().time() < deadline:
            async with self.sql.connect() as connection:
                expired = await connection.scalar(
                    text(
                        "SELECT clock_timestamp()>=lease_expires_at FROM agent_turns WHERE id=:id"
                    ),
                    {"id": turn_id},
                )
            if expired:
                return
            await asyncio.sleep(0.02)
        pytest.fail("Real PostgreSQL lease did not expire")

    async def latch(self):
        assert await asyncio.to_thread(self.model.entered.wait, 5), (
            "Real model HTTP was not in flight"
        )

    def safe_logs(self, sentinel):
        for handle in self.handles:
            handle.flush()
        logs = "\n".join(path.read_text(errors="replace") for path in self.root.glob("*.log"))
        assert sentinel not in logs, (
            "Synthetic secret escaped into application/dependency/stderr logs"
        )
        assert "Goal2 runtime network destination forbidden" not in logs
        for token in self.tokens.values():
            assert token not in logs, "Fixture token escaped into runtime logs"
        for secret in [
            make_url(self.ai_url).password,
            make_url(self.cs_url).password,
            self.ai_env["ENCRYPTION_KEY"],
            "synthetic-model-key",
        ]:
            assert secret not in logs, "Fixture database/key secret escaped into runtime logs"


@pytest_asyncio.fixture
async def cross(
    sql,
    database_url,
    ai_migrated,
    cross_inputs,
    listeners,
    network_allowlist,
    tmp_path,
    cs_deadline,
):
    ai_url, ai_root, ai_python = cross_inputs
    ai_sql = create_async_engine(ai_url, poolclass=NullPool)
    async with ai_sql.begin() as connection:
        version = int(await connection.scalar(text("SHOW server_version_num")))
        assert 160000 <= version < 170000, "Required real AI PostgreSQL16"
        await connection.execute(
            text("TRUNCATE conversation_sessions,agent_credentials,agent_templates CASCADE")
        )
    ids = {
        key: str(uuid4())
        for key in ["agent_a", "agent_b", "owner_a", "owner_b", "integration_a", "integration_b"]
    }
    tokens = {name: secrets.token_urlsafe(32) for name in ["a", "b", "managed", "chat", "internal"]}
    grants = {
        name: {
            "owner_id": f"opaque-owner-{name}",
            "integration_id": ids[f"integration_{name}"],
            "channel": "TELEGRAM",
            "provider_namespace": "fixture.gateway",
        }
        for name in ["a", "b"]
    }
    private_json(tmp_path / "observations.json", {})
    private_json(tmp_path / "scenario.json", {})
    private_json(
        tmp_path / "redis.json",
        {
            "chat_history:poison": {
                "value": '[{"role":"user","content":"POISON_NOT_CANONICAL"}]',
                "ttl": 999,
            }
        },
    )
    model = ScriptedServer(listeners["model"], "model")
    kb = ScriptedServer(listeners["kb"], "kb")
    model.start()
    kb.start()
    async with httpx.AsyncClient(timeout=15, trust_env=False) as client:
        harness = CrossHarness(
            tmp_path,
            ai_root,
            ai_python,
            ai_url,
            database_url,
            sql,
            ai_sql,
            client,
            listeners,
            model,
            kb,
            tokens,
            ids,
            grants,
        )
        cs_port, ai_port = (listeners[key].getsockname()[1] for key in ["cs", "ai"])
        cs_config = {
            "fd": listeners["cs"].fileno(),
            "port": cs_port,
            "state_dir": str(tmp_path),
            "allowlist": [["127.0.0.1", make_url(database_url).port], ["127.0.0.1", ai_port]],
        }
        agents = [
            {
                "id": ids[f"agent_{name}"],
                "owner": ids[f"owner_{name}"],
                "prompt_config": {
                    "company": {"name": "Fixture company"},
                    "personality": {"use_emoji": False},
                },
            }
            for name in ["a", "b"]
        ]
        ai_config = {
            "fd": listeners["ai"].fileno(),
            "port": ai_port,
            "state_dir": str(tmp_path),
            "agents": agents,
            "allowlist": [
                ["127.0.0.1", make_url(ai_url).port],
                ["127.0.0.1", listeners["model"].getsockname()[1]],
                ["127.0.0.1", listeners["kb"].getsockname()[1]],
            ],
        }
        private_json(tmp_path / "cs-config.json", cs_config)
        private_json(tmp_path / "ai-config.json", ai_config)
        principals = [
            {
                "principal_id": "fixture-conversation",
                "token": tokens["managed"],
                "caller": "conversation-service",
                "execution_namespace": "main-chota.conversation.v1",
                "grants": [
                    {
                        **{
                            key: value
                            for key, value in grants[name].items()
                            if key != "provider_namespace"
                        },
                        "agent_id": ids[f"agent_{name}"],
                        "agent_owner_id": ids[f"owner_{name}"],
                    }
                    for name in ["a", "b"]
                ] + [{
                    "owner_id": grants["a"]["owner_id"],
                    "integration_id": ids["integration_a"],
                    "channel": "WEBSITE_WIDGET",
                    "agent_id": ids["agent_a"],
                    "agent_owner_id": ids["owner_a"],
                }],
            }
        ]
        harness.cs_env = clean_env()
        harness.cs_env.update(
            DATABASE_URL=database_url,
            LOG_LEVEL="WARNING",
            GOAL2_BOOTSTRAP_CONFIG=str(tmp_path / "cs-config.json"),
            WEBSITE_BINDINGS=json.dumps([{
                "owner_id": grants["a"]["owner_id"],
                "integration_id": ids["integration_a"],
                "agent_id": ids["agent_a"],
            }]),
            CONVERSATION_AUTH_PRINCIPALS=json.dumps(
                [
                    {
                        "principal_id": name,
                        "token": tokens[name],
                        "grants": [grants[name]] + ([{
                            "owner_id": grants[name]["owner_id"],
                            "integration_id": ids["integration_a"],
                            "channel": "WEBSITE_WIDGET",
                            "provider_namespace": "main-chota.website-widget.v1",
                        }] if name == "a" else []),
                    }
                    for name in ["a", "b"]
                ]
            ),
            MANAGED_AI_BASE_URL=harness.url("ai"),
            MANAGED_AI_TOTAL_SECONDS=str(cs_deadline),
            MANAGED_AI_READ_SECONDS=str(cs_deadline),
            MANAGED_AI_BINDINGS=json.dumps(
                [
                    {
                        **{
                            key: value
                            for key, value in grants[name].items()
                            if key != "provider_namespace"
                        },
                        "agent_id": ids[f"agent_{name}"],
                        "service_token": tokens["managed"],
                    }
                    for name in ["a", "b"]
                ] + [{
                    "owner_id": grants["a"]["owner_id"],
                    "integration_id": ids["integration_a"],
                    "channel": "WEBSITE_WIDGET",
                    "agent_id": ids["agent_a"],
                    "service_token": tokens["managed"],
                }]
            ),
        )
        url = make_url(ai_url)
        harness.ai_env = clean_env()
        harness.ai_env.update(
            POSTGRES_HOST="127.0.0.1",
            POSTGRES_PORT=str(url.port),
            POSTGRES_DB=url.database,
            POSTGRES_USER=url.username,
            POSTGRES_PASSWORD=url.password,
            POSTGRES_ECHO="false",
            ENCRYPTION_KEY=base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
            GOAL2_BOOTSTRAP_CONFIG=str(tmp_path / "ai-config.json"),
            MANAGED_AI_AUTH_PRINCIPALS=json.dumps(principals),
            MANAGED_AI_EXECUTION_SECONDS="10",
            CHAT_S2S_TOKEN=tokens["chat"],
            CHAT_S2S_AUTH_REQUIRED="true",
            INTERNAL_API_TOKEN=tokens["internal"],
            REDIS_HOST="",
            DEBUG="false",
            LLM_BASE_URL=harness.url("model") + "/v1",
            KNOWLEDGE_BASE_ENABLED="true",
            KNOWLEDGE_BASE_SERVICE_URL=harness.url("kb"),
            KNOWLEDGE_BASE_TIMEOUT_SECONDS="0.2",
        )
        # Default KB is empty unless a test deliberately scripts a context/error.
        kb.queue(*[{"contexts": [], "total": 0} for _ in range(20)])
        try:
            await harness.start("ai")
            await harness.start("cs")
            yield harness
        finally:
            model.release.set()
            kb.release.set()
            await harness.stop("cs")
            await harness.stop("ai")
            model.close()
            kb.close()
            for handle in harness.handles:
                handle.close()
            await ai_sql.dispose()


def unknown(response):
    assert response.status_code == 200
    turn = response.json()["turn"]
    assert turn["status"] == "FAILED" and turn["failure_code"] == "EXECUTION_UNKNOWN"
    assert turn["draft_text"] is None and turn["outcome"] is None and turn["operation_refs"] == []
    assert turn["managed_dispatch_epoch"] is not None
    assert re.fullmatch("[0-9a-f]{64}", turn["managed_request_fingerprint"])
    return turn
