"""Positive writer termination, original-attempt recovery and authority fencing."""
from __future__ import annotations

import json
import secrets
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import func, select

import services.inbox_lifecycle as lifecycle
from core.config import Settings
from models import Conversation, DeliveryAttempt, ExternalEffectPermit, Message
from services.common import fingerprint
from tests.test_handoff import (
    add_fresh_agent_turn,
    begin_request,
    browser_headers,
    claim_and_send_human,
    telegram_work,
)
from tests.test_handoff import inbox_harness as inbox_fixture
from tests.test_handoff import user_me_transport as user_me_fixture
from tests.test_managed_migration import migrate

RETIRE = "/internal/v1/outbound-writers/retire"
inbox_harness = inbox_fixture
user_me_transport = user_me_fixture


@pytest.fixture
def writer_harness(inbox_harness):
    token = secrets.token_urlsafe(32)
    values = inbox_harness.settings.model_dump()
    values.update(writer_retirement_token=SecretStr(token),
        writer_supervisor_token=SecretStr(secrets.token_urlsafe(32)),
        writer_supervisor_base_url="http://writer-supervisor.test")
    settings = Settings(**values, _env_file=None)
    inbox_harness.settings = inbox_harness.app.state.settings = settings
    inbox_harness.retirement_headers = {"X-Writer-Retirement-Token": token}
    return inbox_harness


@pytest.fixture
def proof_transport(monkeypatch, writer_harness):
    real = httpx
    control = SimpleNamespace(calls=0, status=200, changes={}, raw=None, hook=None)

    async def handler(request):
        # The remote read must not retain any Conversation locks/checkout.
        assert writer_harness.database.engine.pool.checkedout() == 0
        assert request.method == "POST"
        assert str(request.url) == (
            "http://writer-supervisor.test/internal/v1/writer-termination/proof"
        )
        assert request.headers["X-Writer-Supervisor-Token"] == (
            writer_harness.settings.writer_supervisor_token.get_secret_value()
        )
        assert "X-Writer-Retirement-Token" not in request.headers
        control.calls += 1
        command = json.loads(request.content)
        if control.hook is not None:
            await control.hook()
        value = dict(command, request_fingerprint=fingerprint(command),
            proof_kind="PROCESS_TERMINATED", fenced=True, tasks_ended=True,
            observed_at=datetime.now(UTC).isoformat())
        value.update(control.changes)
        raw = json.dumps(value).encode() if control.raw is None else control.raw
        return real.Response(control.status, headers={"Content-Type": "application/json"},
            stream=real.ByteStream(raw), request=request)

    class Client:
        def __init__(self, **kwargs):
            assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
            self.client = real.AsyncClient(transport=real.MockTransport(handler), **kwargs)

        async def __aenter__(self):
            return await self.client.__aenter__()

        async def __aexit__(self, *args):
            return await self.client.__aexit__(*args)

    monkeypatch.setattr(lifecycle, "httpx", SimpleNamespace(
        AsyncClient=Client, Timeout=real.Timeout, HTTPError=real.HTTPError))
    return control


async def acquired(h):
    ids, sent = await claim_and_send_human(h)
    items = await telegram_work(h)
    assert len(items) == 1
    begin = begin_request(items[0])
    response = await h.client.post("/internal/v2/telegram/delivery/begin",
        json=begin, headers=h.service_headers)
    assert response.status_code == 200 and response.json()["send"] is True
    command = dict(begin, work_kind="RECOVER_ONLY", command_id=str(uuid4()),
        expected_delivery_revision=1, proof_ref=str(uuid4()))
    return ids, sent, begin, command


async def intent(h, command):
    async with h.database.session_factory() as session:
        value = await session.get(DeliveryAttempt, UUID(command["outbound_intent_id"]))
        assert value is not None
        return value


def return_command(sent):
    return dict(command_id=str(uuid4()),
        expected_authority_revision=sent["case"]["authority_revision"],
        expected_case_revision=sent["case"]["case_revision"])


async def return_to_agent(h, ids, command):
    return await h.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/return-to-agent",
        json=command, headers=browser_headers("session-a", write=True))


async def test_positive_proof_unblocks_return_without_resend_and_keeps_late_receipt(
    writer_harness, proof_transport, user_me_transport,
):
    h = writer_harness
    ids, sent, begin, command = await acquired(h)
    returned = return_command(sent)
    assert (await return_to_agent(h, ids, returned)).status_code == 409
    result = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert result.status_code == 200, result.text
    assert result.json()["writer_state"] == "RETIRED"
    assert result.json()["replayed"] is False
    first = await intent(h, command)
    assert first.status == "UNKNOWN" and first.error_code == "TRANSPORT_ERROR"
    assert first.writer_state == "RETIRED" and first.revision == 2
    assert first.writer_retirement_receipt["proof"]["fenced"] is True
    receipt = first.writer_retirement_receipt
    async with h.database.session_factory() as session:
        root = await session.get(Conversation, ids["conversation_id"])
        assert root.control_mode == "HUMAN"
        assert root.authority_revision == sent["case"]["authority_revision"]
        assert await session.scalar(select(func.count()).select_from(ExternalEffectPermit)) == 0

    # Pool/process restart and unavailable proof source cannot defeat exact replay.
    await h.database.dispose()
    proof_transport.status = 503
    replay = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert replay.status_code == 200
    assert replay.json() == dict(result.json(), replayed=True)
    assert proof_transport.calls == 1
    forbidden_retry = await h.client.post("/internal/v2/telegram/delivery/begin",
        json=begin, headers=h.service_headers)
    assert forbidden_retry.status_code == 200 and forbidden_retry.json()["send"] is False
    assert await telegram_work(h) == []

    # A recovered receipt refines the original attempt; it creates no new send.
    late = dict(begin, work_kind="RECOVER_ONLY", outcome="PROVIDER_ACCEPTED",
        provider_message_id="late-original-receipt", writer_end={"kind": "TASK_JOINED",
            "supplier_instance_id": begin["supplier_instance_id"],
            "process_incarnation_id": begin["process_incarnation_id"]})
    for _ in range(2):
        response = await h.client.post("/internal/v2/telegram/delivery/complete",
            json=late, headers=h.service_headers)
        assert response.status_code == 200, response.text
        assert response.json()["writer_state"] == "RETIRED"
    final = await intent(h, command)
    assert final.status == "PROVIDER_ACCEPTED" and final.revision == 3
    assert final.writer_retirement_receipt == receipt
    assert (await h.client.post(RETIRE, json=command,
        headers=h.retirement_headers)).json() == dict(result.json(), replayed=True)
    assert (await return_to_agent(h, ids, returned)).status_code == 200
    await add_fresh_agent_turn(h, ids["conversation_id"])
    next_items = await telegram_work(h)
    assert len(next_items) == 1 and next_items[0]["source_kind"] == "AGENT_TURN"
    assert next_items[0]["outbound_intent_id"] != command["outbound_intent_id"]
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message).where(
            Message.conversation_id == ids["conversation_id"], Message.actor == "HUMAN")) == 1


@pytest.mark.parametrize("outcome,error,provider", [
    ("UNKNOWN", "TIMEOUT", None),
    ("FAILED", "PROVIDER_REJECTED", None),
    ("PROVIDER_ACCEPTED", None, "known-receipt"),
])
async def test_retirement_preserves_already_observed_outcome(
    writer_harness, proof_transport, user_me_transport, outcome, error, provider,
):
    h = writer_harness
    _, _, begin, command = await acquired(h)
    completed = await h.client.post("/internal/v2/telegram/delivery/complete",
        json=dict(begin, outcome=outcome, error_code=error, provider_message_id=provider),
        headers=h.service_headers)
    assert completed.status_code == 200
    before = await intent(h, command)
    command["expected_delivery_revision"] = before.revision
    response = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert response.status_code == 200, response.text
    after = await intent(h, command)
    assert (after.status, after.error_code, after.provider_message_id, after.revision) == (
        before.status, before.error_code, before.provider_message_id, before.revision)
    assert after.writer_state == "RETIRED"


@pytest.mark.parametrize("changes", [
    {"fenced": False}, {"tasks_ended": False}, {"fenced": 1}, {"tasks_ended": "true"},
    {"proof_kind": "LEASE_EXPIRED"}, {"proof_ref": str(uuid4())},
    {"process_incarnation_id": str(uuid4())}, {"supplier_instance_id": str(uuid4())},
    {"request_fingerprint": "0" * 64}, {"conversation_id": str(uuid4())},
])
async def test_nonpositive_or_mismatched_proof_keeps_writer_live(
    writer_harness, proof_transport, user_me_transport, changes,
):
    h = writer_harness
    ids, sent, _, command = await acquired(h)
    proof_transport.changes = changes
    response = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert response.status_code in {409, 503}, response.text
    retained = await intent(h, command)
    assert retained.writer_state == "LIVE" and retained.status == "DISPATCH_REQUESTED"
    assert retained.writer_retirement_receipt is None and retained.revision == 1
    assert (await return_to_agent(h, ids, return_command(sent))).status_code == 409


@pytest.mark.parametrize("raw", [b'{"fenced":true,"fenced":false}', b"not-json",
                                     b"x" * 16_385])
async def test_invalid_or_oversized_proof_cannot_release_writer(
    writer_harness, proof_transport, user_me_transport, raw,
):
    h = writer_harness
    _, _, _, command = await acquired(h)
    proof_transport.raw = raw
    response = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert response.status_code == 503
    assert (await intent(h, command)).writer_state == "LIVE"


@pytest.mark.parametrize("field,value,status", [
    ("owner_id", "another-owner", 404), ("integration_id", str(uuid4()), 404),
    ("agent_id", str(uuid4()), 404), ("conversation_id", str(uuid4()), 404),
    ("source_id", str(uuid4()), 409), ("supplier_instance_id", str(uuid4()), 409),
    ("process_incarnation_id", str(uuid4()), 409), ("expected_delivery_revision", 0, 409),
])
async def test_wrong_scope_writer_or_revision_is_denied_before_proof(
    writer_harness, proof_transport, user_me_transport, field, value, status,
):
    h = writer_harness
    _, _, _, command = await acquired(h)
    response = await h.client.post(RETIRE, json=dict(command, **{field: value}),
        headers=h.retirement_headers)
    assert response.status_code == status, response.text
    assert proof_transport.calls == 0
    assert (await intent(h, command)).writer_state == "LIVE"


@pytest.mark.parametrize("join", [False, True])
async def test_completion_during_proof_read_wins_without_lock_or_stale_retirement(
    writer_harness, proof_transport, user_me_transport, join,
):
    h = writer_harness
    _, _, begin, command = await acquired(h)

    async def complete_original():
        payload = dict(begin, outcome="PROVIDER_ACCEPTED", provider_message_id="race-receipt")
        if join:
            payload["writer_end"] = dict(kind="TASK_JOINED",
                supplier_instance_id=begin["supplier_instance_id"],
                process_incarnation_id=begin["process_incarnation_id"])
        result = await h.client.post("/internal/v2/telegram/delivery/complete",
            json=payload, headers=h.service_headers)
        assert result.status_code == 200, result.text

    proof_transport.hook = complete_original
    result = await h.client.post(RETIRE, json=command, headers=h.retirement_headers)
    assert result.status_code == 409, result.text
    retained = await intent(h, command)
    assert retained.status == "PROVIDER_ACCEPTED"
    assert retained.writer_state == ("ENDED" if join else "LIVE")
    assert retained.writer_retirement_receipt is None


async def test_retained_command_rejects_payload_changes_without_proof(
    writer_harness, proof_transport, user_me_transport,
):
    h = writer_harness
    _, _, _, command = await acquired(h)
    assert (await h.client.post(RETIRE, json=command,
        headers=h.retirement_headers)).status_code == 200
    for changes in ({"proof_ref": str(uuid4())}, {"command_id": str(uuid4())},
                    {"expected_delivery_revision": 2}):
        result = await h.client.post(RETIRE, json=dict(command, **changes),
            headers=h.retirement_headers)
        assert result.status_code == 409
    assert proof_transport.calls == 1


async def test_supervisor_credential_is_exclusive_and_has_no_other_write_authority(
    writer_harness, proof_transport, user_me_transport,
):
    h = writer_harness
    ids, _, _, command = await acquired(h)
    for headers in (h.service_headers, h.full_headers,
                    h.cp_lifecycle_headers, h.ai_lifecycle_headers):
        result = await h.client.post(RETIRE, json=command, headers=headers)
        assert result.status_code == 404
    for headers in (list(h.retirement_headers.items()) * 2,
                    list(h.retirement_headers.items()) + list(h.service_headers.items())):
        assert (await h.client.post(RETIRE, json=command, headers=headers)).status_code == 401
    for path in ("/internal/v1/inbox-grants/provision", "/internal/v1/inbox-grants/revoke",
                 "/internal/v1/external-effect-permits", "/internal/v1/inbound-events",
                 "/internal/v2/telegram/delivery/begin"):
        assert (await h.client.post(path, json={}, headers=h.retirement_headers)).status_code == 404
    for suffix in ("claim", "messages", "return-to-agent"):
        response = await h.client.post(f"/public/v1/inbox/cases/{ids['case_id']}/{suffix}",
            json={}, headers=dict(h.retirement_headers, Origin="https://agents.chota.by"))
        assert response.status_code == 401
    assert proof_transport.calls == 0
    assert (await intent(h, command)).writer_state == "LIVE"


async def test_retirement_migration_cannot_discard_retained_evidence(
    writer_harness, proof_transport, user_me_transport, database_url,
):
    h = writer_harness
    _, _, _, command = await acquired(h)
    assert (await h.client.post(RETIRE, json=command,
        headers=h.retirement_headers)).status_code == 200
    before = (await intent(h, command)).writer_retirement_receipt
    await h.database.dispose()
    rejected = migrate(database_url, "downgrade", "0012_shared_handoff", expected=1)
    assert "Retained writer retirement evidence prevents downgrade" in rejected.stderr
    assert await h.database.ready()
    assert (await intent(h, command)).writer_retirement_receipt == before


def test_writer_supervisor_configuration_is_complete_and_separate(writer_harness):
    h = writer_harness
    values = h.settings.model_dump()
    assert Settings(**values, _env_file=None).writer_supervisor_base_url is not None
    for name in ("writer_supervisor_base_url", "writer_supervisor_token",
                 "writer_retirement_token"):
        with pytest.raises(ValidationError):
            Settings(**dict(values, **{name: None}), _env_file=None)
    for token in (h.settings.inbox_lifecycle_token, h.settings.auth_principals[0].token,
                  h.settings.managed_ai_bindings[0].service_token,
                  h.settings.writer_retirement_token):
        with pytest.raises(ValidationError):
            Settings(**dict(values, writer_supervisor_token=token), _env_file=None)
