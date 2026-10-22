"""Opt-in composed HTTP: real Gateway/TLS/Conversation/PG, synthetic AI/User/CP."""

import asyncio
import json
import os
import socket
import ssl
import subprocess
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
import uvicorn
from sqlalchemy import select

from core.config import Settings
from models import DeliveryAttempt
from schemas.handoff import HumanPrincipal
from services.human_auth import INBOX_ORIGIN
from services.website_worker import WebsiteWorker
from tests.test_website_inbox_provision import sh as session_fixture
from tests.test_website_inbox_provision import suppliers as supplier_fixture
from tests.test_website_inbox_provision import wh as provision_fixture
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import due

sh, suppliers, wh = session_fixture, supplier_fixture, provision_fixture


def revision(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


@pytest_asyncio.fixture
async def composed(wh, suppliers, monkeypatch, tmp_path, network_allowlist):
    fixture_file = os.environ.get("TRACK_WEBSITE_FIXTURE_INPUT")
    if not fixture_file:
        pytest.fail("Explicit real Gateway source, runtime and ephemeral TLS fixture required")
    fixture = json.loads(Path(fixture_file).read_text(encoding="utf-8"))
    gateway = Path(fixture["gateway_source"])
    assert revision(gateway, "rev-parse", "HEAD") == fixture["gateway_revision"]
    assert not revision(gateway, "status", "--porcelain") and not (gateway / ".env").exists()
    from api import middleware

    async def owner(*_):
        return HumanPrincipal(
            account_id=suppliers.account, email_verified=True, consent_required=False
        )

    monkeypatch.setattr(middleware, "authenticate_browser", owner)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    conversation_port = listener.getsockname()[1]
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    gateway_port = probe.getsockname()[1]
    probe.close()
    network_allowlist.update(("127.0.0.1", port) for port in (conversation_port, gateway_port))
    config = dict(
        fixture,
        conversation_port=conversation_port,
        gateway_port=gateway_port,
        conversation_url=f"https://127.0.0.1:{conversation_port}",
        widget_key="wk_composed_fixture",
        owner_id=wh.settings.website_bindings[0].owner_id,
        integration_id=str(wh.settings.website_bindings[0].integration_id),
        agent_id=str(wh.settings.website_bindings[0].agent_id),
        binding_generation=7,
        service_token=wh.tokens["a"],
    )
    input_file = tmp_path / "private-gateway.json"
    input_file.write_text(json.dumps(config), encoding="utf-8")
    app = wh.client._transport.app
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=conversation_port,
            ssl_certfile=fixture["ca_file"],
            ssl_keyfile=fixture["key_file"],
            lifespan="off",
            access_log=False,
            log_level="critical",
            timeout_graceful_shutdown=2,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper() in {"PATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "COMSPEC"}
    }
    selected = json.dumps({config["integration_id"]: config["agent_id"]})
    env.update(
        PYTHONUTF8="1",
        PYTHONDONTWRITEBYTECODE="1",
        COMMON_WEBSITE_INTEGRATIONS=selected,
        MANAGED_WEBSITE_GUARDED_INTEGRATIONS=selected,
        COMMON_WEBSITE_TRUSTED_PROXY_CIDRS="127.0.0.1/32",
        COMMON_WEBSITE_POLL_ENABLED="true",
        COMMON_WEBSITE_POLL_WAIT_SECONDS="0.5",
        COMMON_WEBSITE_CONVERSATION_URL=config["conversation_url"],
        COMMON_WEBSITE_CONVERSATION_PEER="127.0.0.1",
        COMMON_WEBSITE_S2S_TOKEN=config["service_token"],
        CONVERSATION_SERVICE_URL=config["conversation_url"],
        CONVERSATION_S2S_TOKEN=config["service_token"],
    )
    process = None
    headers = {
        "X-Widget-Key": config["widget_key"],
        "X-Forwarded-Proto": "https",
        "X-Real-IP": "192.0.2.9",
    }
    tls = ssl.create_default_context(cafile=fixture["ca_file"])
    try:
        with (tmp_path / "private-gateway.log").open("xb") as log:
            process = subprocess.Popen(
                [
                    fixture["gateway_python"],
                    str(Path(__file__).with_name("gateway_peer.py")),
                    str(input_file),
                ],
                cwd=gateway,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            async with (
                httpx.AsyncClient(
                    base_url=f"http://127.0.0.1:{gateway_port}", trust_env=False, timeout=5
                ) as public,
                httpx.AsyncClient(
                    base_url=config["conversation_url"], verify=tls, trust_env=False, timeout=5,
                    headers={"Origin": INBOX_ORIGIN},
                ) as owner_api,
            ):
                for _ in range(100):
                    assert process.poll() is None, (
                        "Gateway fixture exited; inspect private peer log"
                    )
                    try:
                        if server.started and (await public.get("/health")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("Composed peers did not start")
                yield SimpleNamespace(public=public, owner=owner_api, headers=headers, wh=wh)
    finally:
        if process is not None:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, timeout=5)
        server.should_exit = True
        await asyncio.wait_for(server_task, 5)
        assert revision(gateway, "rev-parse", "HEAD") == fixture["gateway_revision"]
        assert not revision(gateway, "status", "--porcelain")


async def create_session(peer):
    response = await peer.public.post("/v1/sessions", json={}, headers=peer.headers)
    assert response.status_code == 201
    peer.wh.created = response.json()
    peer.headers["X-Widget-Session-Token"] = peer.wh.created["session_token"]
    return peer.wh.created["session_id"]


async def message(peer, session, text, message_id=None):
    envelope = dict(session_id=session, message_id=str(message_id or uuid4()), message=text)
    response = await peer.public.post("/v1/sessions/messages", json=envelope, headers=peer.headers)
    assert response.status_code == 202
    return envelope


@asynccontextmanager
async def accepted_poll(peer, session):
    response = await peer.public.post(
        "/v1/sessions/poll", json=dict(session_id=session), headers=peer.headers
    )
    assert response.status_code == 200
    result = response.json()
    assert result["state"] == "DELIVERY"
    # Receiving the frame is not end proof: wait for the original Gateway
    # completion/cleanup request and inspect the Conversation-owned ledger.
    for _ in range(100):
        async with peer.wh.database.session_factory() as db:
            attempt = await db.scalar(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.provider_message_id == result["delivery"]["delivery_id"]
                )
            )
            if attempt is not None and attempt.writer_state == "ENDED":
                break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("Original Gateway writer did not commit joined end evidence")
    yield result["delivery"]


async def test_composed_handoff_owner_reply_return_and_fresh_ai_input(composed):
    peer = composed
    session = await create_session(peer)
    first = await message(peer, session, "Нужен оператор")
    await due(peer.wh)
    assert await WebsiteWorker(
        peer.wh.database, peer.wh.settings, WebsiteV3Port("HANDOFF")
    ).run_once()
    page = await peer.owner.get("/public/v1/inbox/cases")
    assert page.status_code == 200 and len(page.json()["items"]) == 1
    case = page.json()["items"][0]
    base = "/public/v1/inbox/cases/" + case["case_id"]

    def command(value, **extra):
        return dict(
            command_id=str(uuid4()),
            expected_authority_revision=value["authority_revision"],
            expected_case_revision=value["case_revision"],
            **extra,
        )

    claimed = await peer.owner.post(base + "/claim", json=command(case))
    assert claimed.status_code == 200
    case = claimed.json()["case"]
    send = command(
        case, expected_assignment_revision=case["assignment_revision"], text="Ответ владельца"
    )
    sent = await peer.owner.post(base + "/messages", json=send)
    replay = await peer.owner.post(base + "/messages", json=send)
    assert sent.status_code == replay.status_code == 200 and replay.json()["receipt"]["replayed"]
    assert not await WebsiteWorker(
        peer.wh.database, peer.wh.settings, WebsiteV3Port("DRAFT")
    ).run_once()
    async with accepted_poll(peer, session) as human:
        assert human["actor"] == "HUMAN" and human["text"] == send["text"]
    history = await peer.public.post(
        "/v1/sessions/history", json=dict(session_id=session), headers=peer.headers
    )
    items = history.json()["items"]
    assert [item["client_message_id"] for item in items if item["actor"] == "CUSTOMER"] == [
        first["message_id"]
    ]
    assert [item["id"] for item in items if item["actor"] == "HUMAN"] == [human["delivery_id"]]
    returned = await peer.owner.post(base + "/return-to-agent", json=command(sent.json()["case"]))
    assert returned.status_code == 200 and returned.json()["case"]["status"] == "RETURNED"
    draft = WebsiteV3Port("DRAFT")
    worker = WebsiteWorker(peer.wh.database, peer.wh.settings, draft)
    assert not await worker.run_once()
    await message(peer, session, "Новый запрос")
    await due(peer.wh)
    assert await worker.run_once() and len(draft.requests) == 1
    async with accepted_poll(peer, session) as agent:
        assert agent["actor"] == "AGENT" and agent["text"] == "common Website reply"
    async with peer.wh.database.session_factory() as db:
        assert len(list(await db.scalars(select(DeliveryAttempt)))) == 2


async def test_composed_creation_quota_and_visitor_isolation(composed):
    peer = composed
    first = await create_session(peer)
    envelope = await message(peer, first, "Первый посетитель")
    await message(peer, first, envelope["message"], envelope["message_id"])
    second = await create_session(peer)
    assert second != first
    wrong = await peer.public.post(
        "/v1/sessions/history", json=dict(session_id=first), headers=peer.headers
    )
    assert wrong.status_code == 404
    own = await peer.public.post(
        "/v1/sessions/history", json=dict(session_id=second), headers=peer.headers
    )
    assert own.status_code == 200 and own.json()["items"] == []
    denied = await peer.public.post("/v1/sessions", json={}, headers=peer.headers)
    assert denied.status_code == 429 and 1 <= int(denied.headers["Retry-After"]) <= 60
    assert denied.headers["Access-Control-Expose-Headers"] == "Retry-After"
    headers = {key: value for key, value in peer.headers.items() if key != "X-Forwarded-Proto"}
    insecure = await peer.public.post("/v1/sessions", json={}, headers=headers)
    assert insecure.status_code == 400


async def test_composed_busy_agent_has_one_bounded_catch_up_reply(composed):
    peer = composed
    values = peer.wh.settings.model_dump()
    binding = peer.wh.settings.website_bindings[0]
    values["catch_up_bindings"] = [dict(owner_id=binding.owner_id,
        integration_id=binding.integration_id, agent_id=binding.agent_id, channel="WEBSITE_WIDGET")]
    settings = Settings(**values, _env_file=None)
    peer.wh.settings = peer.wh.client._transport.app.state.settings = settings

    class PausedPort(WebsiteV3Port):
        def __init__(self):
            super().__init__("DRAFT")
            self.started, self.release = asyncio.Event(), asyncio.Event()

        async def execute_v3(self, prepared, binding, request_id=""):
            self.started.set()
            await self.release.wait()
            return await super().execute_v3(prepared, binding, request_id)

    session = await create_session(peer)
    await message(peer, session, "Первый вопрос")
    await due(peer.wh)
    paused = PausedPort()
    running = asyncio.create_task(WebsiteWorker(peer.wh.database, settings, paused).run_once())
    try:
        await asyncio.wait_for(paused.started.wait(), 5)
        for text in ("Уточнение один", "Уточнение два", "Последнее уточнение"):
            await message(peer, session, text)
            await due(peer.wh)
    finally:
        paused.release.set()
        assert await asyncio.wait_for(running, 5)
    current = WebsiteV3Port("DRAFT")
    worker = WebsiteWorker(peer.wh.database, settings, current)
    assert await worker.run_once()
    assert not await worker.run_once() and len(current.requests) == 1
    request = current.requests[0].request
    assert (request.input_first_sequence, request.input_last_sequence) == (2, 4)
    selected = [item.text for item in request.messages
                if item.id in request.selected_input_message_ids]
    assert selected == ["Уточнение один", "Уточнение два", "Последнее уточнение"]
    async with accepted_poll(peer, session) as delivered:
        assert delivered["actor"] == "AGENT"
    history = await peer.public.post("/v1/sessions/history", json=dict(session_id=session),
                                      headers=peer.headers)
    assert len([item for item in history.json()["items"] if item["actor"] == "AGENT"]) == 1
