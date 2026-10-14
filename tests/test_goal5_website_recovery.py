"""Real 0005 Website writes remain authoritative after the 0006 upgrade."""

import asyncio
import io
import json
import os
import socket
import subprocess
import sys
import tarfile
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from models import AgentTurn, WebsiteAdmission, WebsiteImmediateTurn
from tests.conftest import ROOT
from tests.test_managed_migration import migrate
from tests.test_website import identity, payload
from tests.test_website import wh as website_fixture

wh = website_fixture
OLD_0005_SHA = "13972164da3e4f5ad392c79eb60ea2b9e877bc1c"
BOOTSTRAP = ROOT / "tests/website_cross_service/runtime_bootstrap.py"
V2_NAMESPACE = "main-chota.website-widget-response.v2:"


def private_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value))
    path.chmod(0o600)


def archive_0005(tmp_path: Path) -> Path:
    archived = subprocess.run(
        ["git", "archive", OLD_0005_SHA], cwd=ROOT, check=True, capture_output=True
    ).stdout
    source = tmp_path / "accepted-0005-source"
    source.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archived)) as archive:
        archive.extractall(source)
    return source


def allocate_ports(names: tuple[str, ...]) -> dict[str, int]:
    listeners = []
    ports = {}
    try:
        for name in names:
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
            ports[name] = listener.getsockname()[1]
    finally:
        for listener in listeners:
            listener.close()
    return ports


class Accepted0005Runtime:
    """Run the archived API and worker with only explicitly allowed loopback peers."""

    def __init__(self, wh, database_url: str, tmp_path: Path, network_allowlist,
                 scopes: list[dict]):
        self.wh = wh
        self.database_url = make_url(database_url).set(
            host="127.0.0.1").render_as_string(hide_password=False)
        self.tmp_path = tmp_path
        self.network_allowlist = network_allowlist
        # Existing Website harness grants intentionally omit agent_id because
        # they are authorization grants. The 0005 service process separately
        # requires its configured binding's agent_id.
        self.scopes = [{"agent_id": wh.agent_id, **scope} for scope in scopes]
        self.source = archive_0005(tmp_path)
        self.ports = allocate_ports(("fake", "cs", "worker"))
        self.processes = {}
        self.handles = []
        self.client = httpx.AsyncClient(timeout=10, trust_env=False)
        network_allowlist.update(("127.0.0.1", port) for port in self.ports.values())
        private_json(tmp_path / "observations.json", {
            "managed": [], "effects": [], "legacy": [], "evidence": [],
        })

    def url(self, name: str) -> str:
        return f"http://127.0.0.1:{self.ports[name]}"

    def start(self, name: str) -> None:
        assert name not in self.processes
        grants = [{
            "owner_id": scope["owner_id"],
            "integration_id": scope["integration_id"],
            "channel": "WEBSITE_WIDGET",
            "provider_namespace": "main-chota.website-widget.v1",
        } for scope in self.scopes]
        bindings = [{key: scope[key] for key in ("owner_id", "integration_id", "agent_id")}
                    for scope in self.scopes]
        widgets = {
            scope["widget_key"]: {key: scope[key] for key in (
                "owner_id", "integration_id", "agent_id")}
            for scope in self.scopes if "widget_key" in scope
        }
        db_port = make_url(self.database_url).port or 5432
        config_path = self.tmp_path / f"{name}-bootstrap.json"
        private_json(config_path, {
            "name": name,
            "state_dir": str(self.tmp_path),
            "port": self.ports[name],
            "allowlist": [["127.0.0.1", port] for port in self.ports.values()] +
                         [["127.0.0.1", db_port]],
            "widgets": widgets,
            "fault": None,
            "replies": {},
        })
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "PYTHONPATH": str(self.source),
            "PYTHONUNBUFFERED": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "GOAL4A_BOOTSTRAP_CONFIG": str(config_path),
        }
        if name != "fake":
            env.update({
                "DATABASE_URL": self.database_url,
                "CONVERSATION_AUTH_PRINCIPALS": json.dumps([
                    {"principal_id": "old-website", "role": "WEBSITE",
                     "token": self.wh.tokens["a"], "grants": grants},
                    {"principal_id": "old-processing", "role": "PROCESSING",
                     "token": self.wh.tokens["worker"], "grants": grants},
                ]),
                "WEBSITE_BINDINGS": json.dumps(bindings),
                "MANAGED_AI_BASE_URL": self.url("fake"),
                "MANAGED_AI_BINDINGS": json.dumps([
                    {**binding, "channel": "WEBSITE_WIDGET",
                     "service_token": self.wh.tokens["ai"]}
                    for binding in bindings
                ]),
                "WEBSITE_WORKER_ENABLED": "true",
                "WEBSITE_WORKER_PRINCIPAL_ID": "old-processing",
                "WEBSITE_WORKER_LEASE_SECONDS": "5",
                "WEBSITE_WORKER_POLL_SECONDS": "0.1",
                "WEBSITE_WORKER_HEALTH_PORT": str(self.ports["worker"]),
                "LOG_LEVEL": "INFO",
            })
        log = (self.tmp_path / f"{name}.log").open("ab")
        self.handles.append(log)
        self.processes[name] = subprocess.Popen(
            [sys.executable, str(BOOTSTRAP)], cwd=self.source, env=env,
            stdout=log, stderr=log,
        )

    async def wait_ready(self, name: str) -> None:
        path = "/health" if name in {"fake", "cs"} else "/ready"
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            process = self.processes[name]
            assert process.poll() is None, f"Archived 0005 {name} process exited"
            try:
                response = await self.client.get(self.url(name) + path)
                if response.status_code == 200:
                    guard = json.loads((self.tmp_path / f"{name}-guard.json").read_text())
                    assert guard["denials"] == 2
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.05)
        pytest.fail(f"Archived 0005 {name} process did not become ready")

    async def start_api(self) -> None:
        if "fake" not in self.processes:
            self.start("fake")
            await self.wait_ready("fake")
        self.start("cs")
        await self.wait_ready("cs")

    async def start_worker(self) -> None:
        self.start("worker")
        await self.wait_ready("worker")

    async def website(self, method: str, path: str, body: dict) -> httpx.Response:
        return await self.client.request(
            method, self.url("cs") + "/internal/v1/website" + path, json=body,
            headers={"X-Service-Token": self.wh.tokens["a"]},
        )

    async def close(self) -> None:
        for name in list(self.processes):
            process = self.processes.pop(name)
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        await self.client.aclose()
        for handle in self.handles:
            handle.close()


async def rows(sql, statement: str, **params):
    async with sql.connect() as connection:
        return [dict(row) for row in (await connection.execute(
            text(statement), params)).mappings()]


async def wait_for_turn(sql, admission_id, timeout: float = 15):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        found = await rows(sql,
            "SELECT t.id,t.status,t.input_revision,a.canonical_message_id,a.conversation_id "
            "FROM website_admissions a JOIN website_immediate_turns i "
            "ON i.message_id=a.canonical_message_id JOIN agent_turns t ON t.id=i.turn_id "
            "WHERE a.id=:id", id=admission_id)
        if found and found[0]["status"] in {"COMPLETED", "FAILED"}:
            assert found[0]["status"] == "COMPLETED"
            return found[0]
        await asyncio.sleep(0.05)
    pytest.fail("Archived 0005 Website worker did not complete the accepted admission")


async def admit_old(runtime: Accepted0005Runtime, value: dict) -> dict:
    response = await runtime.website("POST", "/admissions", value)
    assert response.status_code == 200, response.text
    return response.json()


async def begin_old(runtime: Accepted0005Runtime, value: dict, attempt_id: str):
    body = {**identity(value), "response_attempt_id": attempt_id}
    response = await runtime.website("POST", "/responses/begin", body)
    assert response.status_code == 200, response.text
    # The exact 0005 DTO returned no `send` bit; a non-replayed result and the
    # retained attempt row prove it traversed that historical begin path.
    assert response.json()["replayed"] is False
    return body, response.json()


async def complete_old(runtime: Accepted0005Runtime, body: dict, outcome: str):
    response = await runtime.website("POST", "/responses/complete",
                                     {**body, "outcome": outcome})
    assert response.status_code == 200, response.text
    return response.json()


async def move_to_0005(wh, sql, database_url: str) -> None:
    await wh.database.dispose()
    await sql.dispose()
    migrate(database_url, "downgrade", "0005_telegram_canary")


async def move_to_head(wh, sql, database_url: str) -> None:
    await wh.database.dispose()
    await sql.dispose()
    migrate(database_url, "upgrade", "head")


async def upgrade_after_0005(wh, sql, database_url: str) -> None:
    """Always leave the shared disposable DB on the current runtime schema."""
    await move_to_head(wh, sql, database_url)


async def assert_completed_old_write_replays_after_upgrade(
    wh, sql, database_url, network_allowlist, tmp_path
):
    value = payload(wh, text="accepted by the exact old Website writer")
    grant = wh.grants["a"]
    runtime = Accepted0005Runtime(wh, database_url, tmp_path, network_allowlist, [grant])
    try:
        await runtime.start_api()
        accepted = await admit_old(runtime, value)
        assert accepted["route"] == "MANAGED" and accepted["replayed"] is False
        await runtime.start_worker()
        turn = await wait_for_turn(sql, accepted["admission_id"])
        first_body, _ = await begin_old(runtime, value, str(uuid4()))
        second_body, second_begun = await begin_old(runtime, value, str(uuid4()))
        assert second_begun["replayed"] is False
        await complete_old(runtime, first_body, "WRITE_COMPLETED")
        old_sources = await rows(sql,
            "SELECT id::text AS id FROM website_response_attempts WHERE admission_id=:id "
            "AND outcome='WRITE_COMPLETED' ORDER BY created_at,id",
            id=accepted["admission_id"])
        assert old_sources == [{"id": first_body["response_attempt_id"]}]
        assert turn["conversation_id"] is not None
        await runtime.close()
        await move_to_head(wh, sql, database_url)

        v2_id = str(uuid5(NAMESPACE_URL, V2_NAMESPACE + str(accepted["admission_id"])))
        begin_body = {**identity(value), "response_attempt_id": v2_id}
        attempts_before = await rows(sql,
            "SELECT id,admission_id,delivery_id,conversation_id,send_authorized,outcome,"
            "created_at,finished_at FROM website_response_attempts ORDER BY created_at,id")
        reconciliations_before = await rows(sql,
            "SELECT id,delivery_attempt_id,reconciliation_key,status,error_code,result_revision,"
            "accepted_at FROM delivery_reconciliations ORDER BY accepted_at,id")
        deliveries_before = await rows(sql,
            "SELECT id,status,revision,error_code,dispatch_requested_at,updated_at "
            "FROM delivery_attempts ORDER BY created_at,id")
        history_before = await rows(sql,
            "SELECT id,conversation_id,sequence,actor,direction,text,source_agent_turn_id "
            "FROM messages ORDER BY conversation_id,sequence")
        observations_before = json.loads((tmp_path / "observations.json").read_text())
        response = await wh.call("POST", "/website/responses/begin", begin_body)
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["action"] == "REPLAY_COMPLETED"
        assert result["response_attempt_id"] == v2_id
        assert result["replay_source_attempt_id"] == first_body["response_attempt_id"]
        assert result["send"] is False and result["replayed"] is True
        assert result["denial_reason"] is None
        assert await rows(sql,
            "SELECT id,admission_id,delivery_id,conversation_id,send_authorized,outcome,"
            "created_at,finished_at FROM website_response_attempts ORDER BY created_at,id"
        ) == attempts_before
        assert await rows(sql,
            "SELECT id,delivery_attempt_id,reconciliation_key,status,error_code,result_revision,"
            "accepted_at FROM delivery_reconciliations ORDER BY accepted_at,id"
        ) == reconciliations_before
        assert await rows(sql,
            "SELECT id,status,revision,error_code,dispatch_requested_at,updated_at "
            "FROM delivery_attempts ORDER BY created_at,id"
        ) == deliveries_before
        assert await rows(sql,
            "SELECT id,conversation_id,sequence,actor,direction,text,source_agent_turn_id "
            "FROM messages ORDER BY conversation_id,sequence"
        ) == history_before
        assert json.loads((tmp_path / "observations.json").read_text()) == observations_before

        # A second old UUID4 was authorized by the 0005 begin path. Its late
        # successful socket result remains valid after the 0006 migration.
        late = await wh.call("POST", "/website/responses/complete",
                             {**second_body, "outcome": "WRITE_COMPLETED"})
        assert late.status_code == 200, late.text
        assert late.json()["outcome"] == "WRITE_COMPLETED"
        replay = await wh.call("POST", "/website/responses/begin", begin_body)
        assert replay.status_code == 200 and replay.json() == result
        repeated = await wh.call("POST", "/website/responses/complete",
                                 {**second_body, "outcome": "WRITE_COMPLETED"})
        assert repeated.status_code == 200 and repeated.json()["replayed"] is True
        agent_messages = await rows(sql,
            "SELECT id,source_agent_turn_id,text FROM messages WHERE actor='AGENT'")
        assert len(agent_messages) == 1
        assert agent_messages[0]["source_agent_turn_id"] == turn["id"]
    finally:
        await runtime.close()
        await upgrade_after_0005(wh, sql, database_url)


@pytest.mark.asyncio
async def test_actual_0005_authorized_writes_replay_without_gateway_mutation(
    wh, sql, database_url, network_allowlist, tmp_path
):
    await move_to_0005(wh, sql, database_url)
    await assert_completed_old_write_replays_after_upgrade(
        wh, sql, database_url, network_allowlist, tmp_path)


@pytest.mark.asyncio
async def test_denied_v2_attempt_replays_late_old_success_without_rewriting_denial(
    wh, sql, database_url, network_allowlist, tmp_path
):
    await move_to_0005(wh, sql, database_url)
    value = payload(wh, text="late old socket success")
    runtime = Accepted0005Runtime(wh, database_url, tmp_path, network_allowlist,
                                  [wh.grants["a"]])
    try:
        await runtime.start_api()
        accepted = await admit_old(runtime, value)
        await runtime.start_worker()
        turn = await wait_for_turn(sql, accepted["admission_id"])
        unknown_body, _ = await begin_old(runtime, value, str(uuid4()))
        late_body, late_begun = await begin_old(runtime, value, str(uuid4()))
        assert late_begun["replayed"] is False
        await complete_old(runtime, unknown_body, "UNKNOWN")
        await runtime.close()
        await move_to_head(wh, sql, database_url)

        v2_id = str(uuid5(NAMESPACE_URL, V2_NAMESPACE + str(accepted["admission_id"])))
        v2_body = {**identity(value), "response_attempt_id": v2_id}
        denied = await wh.call("POST", "/website/responses/begin", v2_body)
        assert denied.status_code == 200, denied.text
        denied_result = denied.json()
        assert denied_result["action"] == "DENY"
        assert denied_result["send"] is False and denied_result["replayed"] is False
        assert denied_result["replay_source_attempt_id"] is None
        assert denied_result["denial_reason"] in {
            "SUPERSEDED", "DELIVERY_NOT_RETRYABLE",
        }
        denied_row = await rows(sql,
            "SELECT id::text AS id,send_authorized,outcome,finished_at "
            "FROM website_response_attempts "
            "WHERE id=:id", id=v2_id)
        assert denied_row == [{"id": v2_id, "send_authorized": False,
                               "outcome": None, "finished_at": None}]

        late = await wh.call("POST", "/website/responses/complete",
                             {**late_body, "outcome": "WRITE_COMPLETED"})
        assert late.status_code == 200, late.text
        replay = await wh.call("POST", "/website/responses/begin", v2_body)
        assert replay.status_code == 200, replay.text
        assert replay.json()["action"] == "REPLAY_COMPLETED"
        assert replay.json()["response_attempt_id"] == v2_id
        assert replay.json()["replay_source_attempt_id"] == late_body["response_attempt_id"]
        assert replay.json()["send"] is False and replay.json()["replayed"] is True
        assert await rows(sql,
            "SELECT id::text AS id,send_authorized,outcome,finished_at "
            "FROM website_response_attempts "
            "WHERE id=:id", id=v2_id) == denied_row
        agent_messages = await rows(sql,
            "SELECT id,source_agent_turn_id,text FROM messages WHERE actor='AGENT'")
        assert len(agent_messages) == 1
        assert agent_messages[0]["source_agent_turn_id"] == turn["id"]
        repeated = await wh.call("POST", "/website/responses/complete",
                                 {**late_body, "outcome": "WRITE_COMPLETED"})
        assert repeated.status_code == 200 and repeated.json()["replayed"] is True
    finally:
        await runtime.close()
        await upgrade_after_0005(wh, sql, database_url)


@pytest.mark.asyncio
async def test_same_public_id_old_and_v2_attempts_remain_scope_isolated(
    wh, sql, database_url, network_allowlist, tmp_path
):
    await move_to_0005(wh, sql, database_url)
    first, second = wh.grants["a"], wh.grants["other"]
    first = {**first, "agent_id": wh.agent_id}
    second = {**second, "agent_id": wh.agent_id}
    runtime = Accepted0005Runtime(wh, database_url, tmp_path, network_allowlist,
                                  [first, second])
    try:
        await runtime.start_api()
        shared_id = str(uuid4())
        left = payload(wh, message_id=shared_id, text="shared public identity")
        right = {**left, "integration_id": second["integration_id"]}
        admission_a = await admit_old(runtime, left)
        admission_b = await admit_old(runtime, right)
        assert admission_a["admission_id"] != admission_b["admission_id"]
        await runtime.start_worker()
        await wait_for_turn(sql, admission_a["admission_id"])
        await wait_for_turn(sql, admission_b["admission_id"])
        source_body, _ = await begin_old(runtime, left, str(uuid4()))
        await complete_old(runtime, source_body, "WRITE_COMPLETED")
        await runtime.close()
        await move_to_head(wh, sql, database_url)

        result_rows = await rows(sql,
            "SELECT id,owner_id,integration_id FROM website_admissions "
            "WHERE message_id=:id ORDER BY integration_id", id=shared_id)
        assert len(result_rows) == 2
        from core.config import WebsiteBinding
        second_binding = WebsiteBinding(owner_id=second["owner_id"],
                                        integration_id=second["integration_id"],
                                        agent_id=second["agent_id"])
        settings = wh.settings.model_copy(update={
            "website_bindings": (*wh.settings.website_bindings, second_binding),
        })
        wh.settings = settings
        wh.client._transport.app.state.settings = settings
        v2_a = str(uuid5(NAMESPACE_URL, V2_NAMESPACE + str(admission_a["admission_id"])))
        v2_b = str(uuid5(NAMESPACE_URL, V2_NAMESPACE + str(admission_b["admission_id"])))
        assert v2_a != v2_b
        replay = await wh.call("POST", "/website/responses/begin",
                               {**identity(left), "response_attempt_id": v2_a})
        assert replay.status_code == 200 and replay.json()["action"] == "REPLAY_COMPLETED"
        assert replay.json()["replay_source_attempt_id"] == source_body["response_attempt_id"]
        isolated = await wh.call("POST", "/website/responses/begin",
                                 {**identity(right), "response_attempt_id": v2_b},
                                 principal="other")
        assert isolated.status_code == 200, isolated.text
        assert isolated.json()["action"] == "SEND"
        assert isolated.json()["response_attempt_id"] == v2_b
        assert isolated.json()["replay_source_attempt_id"] is None
        assert isolated.json()["send"] is True and isolated.json()["replayed"] is False
    finally:
        await runtime.close()
        await upgrade_after_0005(wh, sql, database_url)


@pytest.mark.asyncio
async def test_no_policy_input_revision_matches_across_0005_and_0006(
    wh, sql, database_url, network_allowlist, tmp_path
):
    await move_to_0005(wh, sql, database_url)
    runtime = Accepted0005Runtime(wh, database_url, tmp_path, network_allowlist,
                                  [wh.grants["a"]])
    old_revisions = None
    try:
        await runtime.start_api()
        session_id = str(uuid4())
        first = payload(wh, session_id=session_id, text="no policy first")
        second = payload(wh, session_id=session_id, text="no policy second")
        admission_a = await admit_old(runtime, first)
        admission_b = await admit_old(runtime, second)
        await runtime.start_worker()
        first_turn = await wait_for_turn(sql, admission_a["admission_id"])
        await wait_for_turn(sql, admission_b["admission_id"])
        old_revisions = await rows(sql,
            "SELECT c.input_revision AS conversation_revision,t.input_revision "
            "FROM website_admissions a JOIN conversations c ON c.id=a.conversation_id "
            "JOIN website_immediate_turns i ON i.message_id=a.canonical_message_id "
            "JOIN agent_turns t ON t.id=i.turn_id WHERE a.id=:id",
            id=admission_a["admission_id"])
        assert first_turn["input_revision"] == 2
        assert old_revisions == [{"conversation_revision": 2, "input_revision": 2}]
        await runtime.close()
        await move_to_head(wh, sql, database_url)

        current_session = str(uuid4())
        current_first = payload(wh, session_id=current_session, text="no policy first")
        current_second = payload(wh, session_id=current_session, text="no policy second")
        for value in (current_first, current_second):
            response = await wh.call("POST", "/website/admissions", value)
            assert response.status_code == 200, response.text
        assert await wh.worker.run_once()
        async with wh.database.session_factory() as session, session.begin():
            admission = await session.scalar(select(WebsiteAdmission).where(
                WebsiteAdmission.message_id == current_first["message_id"]))
            turn_id = await session.scalar(select(WebsiteImmediateTurn.turn_id).where(
                WebsiteImmediateTurn.message_id == admission.canonical_message_id))
            turn = await session.get(AgentTurn, turn_id)
            assert turn.input_revision == old_revisions[0]["input_revision"]
        new_revisions = await rows(sql,
            "SELECT c.input_revision AS conversation_revision,t.input_revision "
            "FROM website_admissions a JOIN conversations c ON c.id=a.conversation_id "
            "JOIN website_immediate_turns i ON i.message_id=a.canonical_message_id "
            "JOIN agent_turns t ON t.id=i.turn_id WHERE a.message_id=:id",
            id=current_first["message_id"])
        assert new_revisions == old_revisions
    finally:
        await runtime.close()
        await upgrade_after_0005(wh, sql, database_url)
