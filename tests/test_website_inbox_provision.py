"""Real common provisioning and authority; suppliers and socket evidence are fakes."""
import secrets
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import ValidationError
from sqlalchemy import func, select

from core.config import Settings
from core.errors import DomainError
from models import HandoffCase, InboxGrantScope, WebsiteSessionCapability
from schemas.contracts import ErrorCode
from schemas.handoff import (
    AiAgentProof,
    ClaimCommand,
    CpBindingProof,
    CpGrantRevoke,
    HumanPrincipal,
    InboxGrantProvision,
    ReturnToAgentCommand,
    SendHumanCommand,
)
from services import inbox, inbox_lifecycle
from services.website_worker import WebsiteWorker
from tests.test_website import admit, payload, v2_response_attempt_id
from tests.test_website import identity as legacy_identity
from tests.test_website import wh as legacy_fixture
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_idle_delivery import call, completion, identity, work
from tests.test_website_session import issued, scope
from tests.test_website_session import sh as session_fixture
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import append, due

sh, suppliers, legacy = session_fixture, supplier_fixture, legacy_fixture


def configured(sh):
    binding = sh.settings.website_bindings[0]
    values = sh.settings.model_dump()
    values.update(
        website_handoff_cohort_integration_ids=(binding.integration_id,),
        website_session_capabilities_enabled=True,
        website_bindings=[b.model_copy(update={"capability_generation": 7})
                          for b in sh.settings.website_bindings],
        managed_ai_base_url="http://127.0.0.1:1", website_worker_enabled=True,
        website_worker_principal_id="worker", user_service_base_url="https://user.invalid",
        cp_handoff_base_url="https://cp.invalid", ai_handoff_base_url="https://ai.invalid",
        inbox_lifecycle_token=secrets.token_urlsafe(32),
        handoff_ai_lifecycle_token=secrets.token_urlsafe(32),
        managed_ai_bindings=[dict(owner_id=binding.owner_id, integration_id=binding.integration_id,
            channel="WEBSITE_WIDGET", agent_id=binding.agent_id,
            service_token=secrets.token_urlsafe(32))],
        turn_settling_policies=[dict(integration_id=binding.integration_id,
            channel="WEBSITE_WIDGET",
            version="website-inbox-v1", quiet_window_ms=100, max_wait_ms=1000)],
    )
    return Settings(**values, _env_file=None)


def provision_command(sh):
    values = scope(sh)
    del values["binding_generation"]
    return InboxGrantProvision(**values, contract_version=1, command_id=uuid4(),
                               channel="WEBSITE_WIDGET", expected_binding_generation=7)


@pytest_asyncio.fixture
async def wh(sh, suppliers, test_database_clock):
    sh.settings = configured(sh)
    sh.client._transport.app.state.settings = sh.settings
    sh.principal = sh.settings.auth_principals[1].principal()
    sh.agent_id = str(sh.settings.website_bindings[0].agent_id)
    sh.clock = test_database_clock
    sh.provision_command = provision_command(sh)
    sh.provisioned = await inbox_lifecycle.provision_grant(
        sh.database, sh.settings, sh.provision_command)
    sh.created = await issued(sh)
    return sh


@pytest.mark.parametrize("missing", ["policy", "capability", "worker", "user", "proofs", "scope"])
async def test_website_cohort_requires_complete_current_configuration(sh, suppliers, missing):
    values = configured(sh).model_dump()
    values.update({
        "policy": {"turn_settling_policies": []},
        "capability": {"website_session_capabilities_enabled": False},
        "worker": {"website_worker_enabled": False},
        "user": {"user_service_base_url": None},
        "proofs": {"inbox_lifecycle_token": None},
        "scope": {"website_handoff_cohort_integration_ids": [uuid4()]},
    }[missing])
    with pytest.raises(ValidationError):
        Settings(**values, _env_file=None)


async def test_real_provision_receipt_issues_credential_and_replays(wh, suppliers):
    assert wh.provisioned.active and wh.provisioned.grant_generation == 1
    before = len(suppliers.calls)
    replay = await inbox_lifecycle.provision_grant(wh.database, wh.settings, wh.provision_command)
    assert replay.replayed and replay.command_id == wh.provisioned.command_id
    assert len(suppliers.calls) == before
    async with wh.database.session_factory() as session:
        capability = await session.scalar(select(WebsiteSessionCapability).where(
            WebsiteSessionCapability.provider_thread_id == wh.created["session_id"]))
        assert capability is not None
        assert capability.provision_command_id == wh.provision_command.command_id
        assert await session.scalar(select(func.count()).select_from(InboxGrantScope)) == 1


async def test_wrong_current_binding_cannot_provision_or_fetch_proofs(sh, suppliers):
    settings = configured(sh)
    command = provision_command(sh).model_copy(update={"agent_id": uuid4()})
    with pytest.raises(DomainError) as caught:
        await inbox_lifecycle.provision_grant(sh.database, settings, command)
    assert caught.value.code == ErrorCode.GRANT_CONFLICT
    assert suppliers.calls == []


async def test_legacy_unknown_finished_timestamp_blocks_provision_until_original_evidence(
    legacy, monkeypatch,
):
    value = payload(legacy)
    await admit(legacy, value)
    assert await legacy.worker.run_once()
    attempt = dict(legacy_identity(value),
                   response_attempt_id=await v2_response_attempt_id(legacy, value))
    begun = await legacy.call("POST", "/website/responses/begin", attempt)
    assert begun.status_code == 200 and begun.json()["send"]
    unknown = await legacy.call("POST", "/website/responses/complete",
                                dict(attempt, outcome="UNKNOWN"))
    assert unknown.status_code == 200
    settings = configured(legacy)
    command = provision_command(legacy)
    account = uuid4()

    async def current(_settings, request):
        fields = request.model_dump(exclude={"command_id", "expected_binding_generation"})
        return (CpBindingProof(**fields, account_id=account, binding_generation=7),
                AiAgentProof(**fields, agent_owner_id=account, agent_lifecycle_generation=11))

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current)
    with pytest.raises(DomainError) as caught:
        await inbox_lifecycle.provision_grant(legacy.database, settings, command)
    assert caught.value.code == ErrorCode.WEBSITE_WRITER_LIVE
    async with legacy.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(InboxGrantScope)) == 0
    ended = await legacy.call("POST", "/website/responses/complete",
                              dict(attempt, outcome="WRITE_FAILED"))
    assert ended.status_code == 200
    assert (await inbox_lifecycle.provision_grant(legacy.database, settings, command)).active


async def test_retained_website_scope_can_be_revoked_after_cohort_config_rollback(wh, suppliers):
    settings = wh.settings.model_copy(update={"website_handoff_cohort_integration_ids": ()})
    values = wh.provision_command.model_dump(exclude={"expected_binding_generation"})
    values.update(command_id=uuid4(), lifecycle_source="CONTROL_PLANE_BINDING",
                  source_binding_generation=7, target_binding_generation=8)
    request = CpGrantRevoke(**values)
    ack = await inbox_lifecycle.revoke_grant(wh.database, settings, request)
    assert not ack.active
    replay = await inbox_lifecycle.revoke_grant(wh.database, settings, request)
    assert replay.replayed
    foreign = request.model_copy(update={"integration_id": uuid4(), "command_id": uuid4()})
    with pytest.raises(DomainError) as caught:
        await inbox_lifecycle.revoke_grant(wh.database, settings, foreign)
    assert caught.value.code == ErrorCode.NOT_FOUND


async def test_provisioned_visitor_handoff_claim_reply_join_return_and_new_ai_input(
    wh, suppliers, monkeypatch,
):
    await append(wh, "Нужен оператор")
    await due(wh)
    worker = WebsiteWorker(wh.database, wh.settings, WebsiteV3Port("HANDOFF"))
    assert await worker.run_once()
    human = HumanPrincipal(account_id=suppliers.account, email_verified=True,
                           consent_required=False)
    from api import middleware

    async def authenticated_owner(*_):
        return human

    monkeypatch.setattr(middleware, "authenticate_browser", authenticated_owner)
    assert wh.settings.handoff_cohort_integration_ids == ()
    public = await wh.client.get("/public/v1/inbox/cases")
    assert public.status_code == 200 and len(public.json()["items"]) == 1
    page = await inbox.list_cases(wh.database, human, 20, None)
    assert len(page.items) == 1
    foreign = human.model_copy(update={"account_id": uuid4()})
    assert (await inbox.list_cases(wh.database, foreign, 20, None)).items == []
    case = page.items[0]
    claimed = await inbox.claim_case(wh.database, human, case.case_id, ClaimCommand(
        command_id=uuid4(), expected_authority_revision=case.authority_revision,
        expected_case_revision=case.case_revision))
    case = claimed.case
    send = SendHumanCommand(command_id=uuid4(), text="Ответ владельца", 
        expected_authority_revision=case.authority_revision,
        expected_case_revision=case.case_revision,
        expected_assignment_revision=case.assignment_revision)
    sent = await inbox.send_human(wh.database, human, case.case_id, send)
    replay = await inbox.send_human(wh.database, human, case.case_id, send)
    assert replay.receipt.replayed and replay.receipt.message_id == sent.receipt.message_id
    value = await work(wh)
    assert value["frame"]["actor"] == "HUMAN" and value["frame"]["text"] == send.text
    request = identity(wh, value)
    begun = await call(wh, "begin", request)
    assert begun.status_code == 200 and begun.json()["send"]
    unknown = await call(wh, "complete", completion(request, "UNKNOWN", joined=False))
    assert unknown.status_code == 200 and unknown.json()["recovery"]["writer_state"] == "LIVE"
    case = sent.case
    back = ReturnToAgentCommand(command_id=uuid4(),
        expected_authority_revision=case.authority_revision,
        expected_case_revision=case.case_revision)
    with pytest.raises(DomainError) as caught:
        await inbox.return_to_agent(wh.database, human, case.case_id, back)
    assert caught.value.code == ErrorCode.WEBSITE_WRITER_LIVE
    ended = await call(wh, "complete", completion(request, "UNKNOWN", joined=True))
    assert ended.status_code == 200 and ended.json()["recovery"]["writer_state"] == "ENDED"
    returned = await inbox.return_to_agent(wh.database, human, case.case_id, back)
    assert returned.case.status == "RETURNED"
    assert not await worker.run_once()
    await append(wh, "Новый запрос ассистенту")
    await due(wh)
    draft = WebsiteV3Port("DRAFT")
    assert await WebsiteWorker(wh.database, wh.settings, draft).run_once()
    assert len(draft.requests) == 1
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(HandoffCase)) == 1
