"""Website recovery uses the common writer journal; termination supplier is fake."""
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from core.config import Settings
from core.errors import DomainError
from models import Conversation, InboxGrantScope, Message
from schemas.handoff import ReturnToAgentCommand
from schemas.writer_retirement import WebsiteWriterRetireCommand
from services import inbox
from tests.test_common_text_delivery import human_intents
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_idle_delivery import call, completion, drafted, identity, work
from tests.test_website_inbox_provision import wh as website_fixture
from tests.test_website_session import scope
from tests.test_website_session import sh as session_fixture
from tests.test_writer_retirement import RETIRE, intent
from tests.test_writer_retirement import proof_transport as proof_fixture

sh, wh, suppliers = session_fixture, website_fixture, supplier_fixture
proof_transport = proof_fixture


@pytest.fixture
def writer_harness(wh):
    import secrets

    values = wh.settings.model_dump()
    values.update(writer_retirement_token=SecretStr(secrets.token_urlsafe(32)),
        writer_supervisor_token=SecretStr(secrets.token_urlsafe(32)),
        writer_supervisor_base_url="http://writer-supervisor.test")
    wh.settings = Settings(**values, _env_file=None)
    wh.client._transport.app.state.settings = wh.settings
    wh.retirement_headers = {"X-Writer-Retirement-Token":
                            wh.settings.writer_retirement_token.get_secret_value()}
    return wh


async def acquired(h, suppliers):
    principal, case, ids = await human_intents(h, suppliers, texts=("first reply", "next reply"))
    value = await work(h)
    request = identity(h, value)
    begun = await call(h, "begin", request)
    assert begun.status_code == 200 and begun.json()["send"]
    command = WebsiteWriterRetireCommand(**scope(h), **value["item"], contract_version=1,
        channel="WEBSITE_WIDGET", conversation_id=case.conversation_id,
        session_id=h.created["session_id"], supplier_instance_id=request["supplier_instance_id"],
        process_incarnation_id=request["process_incarnation_id"], command_id=uuid4(),
        expected_delivery_revision=1, proof_ref=uuid4()).model_dump(mode="json")
    assert "session_token" not in command and "poll_id" not in command
    return principal, case, ids, request, command


async def retire(h, command):
    return await h.client.post(RETIRE, json=command, headers=h.retirement_headers)


async def test_website_retirement_releases_ordered_reply_without_retry_and_retains_late_evidence(
    writer_harness, suppliers, proof_transport,
):
    h = writer_harness
    principal, case, ids, request, command = await acquired(h, suppliers)
    assert (await work(h))["state"] == "RECOVER_ONLY"
    retired = await retire(h, command)
    assert retired.status_code == 200 and retired.json()["writer_state"] == "RETIRED"
    before = await intent(h, command)
    assert before.status == "UNKNOWN" and before.writer_state == "RETIRED"
    receipt = before.writer_retirement_receipt
    async with h.database.session_factory() as session:
        root = await session.get(Conversation, UUID(command["conversation_id"]))
        assert root.control_mode == "HUMAN" and root.authority_revision == case.authority_revision
        assert await session.scalar(select(func.count()).select_from(InboxGrantScope)) == 1
    retry = await call(h, "begin", request)
    assert retry.status_code == 200 and not retry.json()["send"]
    assert retry.json()["recovery"]["status"] == "UNKNOWN"
    next_value = await work(h)
    assert next_value["state"] == "SEND_CANDIDATE"
    assert next_value["item"]["outbound_intent_id"] == str(ids[1])
    # Only original positive outcome refines this attempt, even after retirement.
    late = await call(h, "complete", completion(request))
    assert late.status_code == 200 and late.json()["recovery"]["writer_state"] == "RETIRED"
    final = await intent(h, command)
    assert final.status == "PROVIDER_ACCEPTED" and final.writer_retirement_receipt == receipt
    await h.database.dispose()
    proof_transport.status = 503
    replay = await retire(h, command)
    assert replay.status_code == 200 and replay.json() == dict(retired.json(), replayed=True)
    assert proof_transport.calls == 1
    returned = await inbox.return_to_agent(h.database, principal, case.case_id,
        ReturnToAgentCommand(command_id=uuid4(), expected_case_revision=case.case_revision,
                             expected_authority_revision=case.authority_revision))
    assert returned.case.status == "RETURNED" and (await work(h))["state"] == "EMPTY"
    await drafted(h)
    assert (await work(h))["frame"]["actor"] == "AGENT"
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message).where(
            Message.actor == "HUMAN")) == 2


@pytest.mark.parametrize("change", [
    {"tasks_ended": False}, {"fenced": False}, {"channel": "TELEGRAM"},
    {"supplier_instance_id": str(uuid4())}, {"session_id": str(uuid4())},
    {"request_fingerprint": "0" * 64},
], ids=["tasks-live", "unfenced", "wrong-channel", "wrong-supplier", "wrong-session", "wrong-hash"])
async def test_website_wrong_proof_keeps_writer_live(
    writer_harness, suppliers, proof_transport, change,
):
    h = writer_harness
    principal, case, _, _, command = await acquired(h, suppliers)
    proof_transport.changes = change
    result = await retire(h, command)
    assert result.status_code in {409, 503}
    assert (await intent(h, command)).writer_state == "LIVE"
    with pytest.raises(DomainError):
        await inbox.return_to_agent(h.database, principal, case.case_id,
            ReturnToAgentCommand(command_id=uuid4(), expected_case_revision=case.case_revision,
                                 expected_authority_revision=case.authority_revision))


@pytest.mark.parametrize("field", ["owner_id", "integration_id", "agent_id", "conversation_id",
                                  "session_id", "binding_generation", "delivery_id", "source_id",
                                  "response_attempt_id", "outbound_ordinal"])
async def test_website_wrong_identity_does_not_contact_supplier_or_create_scope(
    writer_harness, suppliers, proof_transport, field,
):
    h = writer_harness
    _, _, _, _, command = await acquired(h, suppliers)
    changed = dict(command)
    changed[field] = ("foreign-owner" if field == "owner_id" else
                      changed[field] + 1 if field in {"binding_generation", "outbound_ordinal"}
                      else str(uuid4()))
    response = await retire(h, changed)
    assert response.status_code in {404, 409}
    assert proof_transport.calls == 0 and (await intent(h, command)).writer_state == "LIVE"
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(InboxGrantScope)) == 1


@pytest.mark.parametrize("joined", [False, True])
async def test_original_completion_wins_during_website_proof_read(
    writer_harness, suppliers, proof_transport, joined,
):
    h = writer_harness
    _, _, _, request, command = await acquired(h, suppliers)

    async def original():
        assert (await call(h, "complete", completion(request, joined=joined))).status_code == 200

    proof_transport.hook = original
    assert (await retire(h, command)).status_code == 409
    value = await intent(h, command)
    assert value.status == "PROVIDER_ACCEPTED"
    assert value.writer_state == ("ENDED" if joined else "LIVE")
    assert value.writer_retirement_receipt is None


async def test_retained_website_writer_can_end_after_cohort_rollback_without_visitor_secret(
    writer_harness, suppliers, proof_transport,
):
    h = writer_harness
    _, _, _, _, command = await acquired(h, suppliers)
    h.settings = h.settings.model_copy(update={"website_handoff_cohort_integration_ids": ()})
    h.client._transport.app.state.settings = h.settings
    result = await retire(h, command)
    assert result.status_code == 200 and (await intent(h, command)).writer_state == "RETIRED"
    for changes in ({"proof_ref": str(uuid4())}, {"expected_delivery_revision": 2}):
        assert (await retire(h, dict(command, **changes))).status_code == 409
    assert proof_transport.calls == 1
