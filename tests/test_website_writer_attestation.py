"""Website proof is recoverable without visitor credentials or binding configuration."""

import secrets
from uuid import uuid4

from core.config import Settings
from tests.test_common_text_delivery import human_intents
from tests.test_website_idle_delivery import call, identity, work
from tests.test_website_session import scope
from tests.test_website_writer_retirement import sh as session_fixture
from tests.test_website_writer_retirement import suppliers as supplier_fixture
from tests.test_website_writer_retirement import wh as website_fixture
from tests.test_website_writer_retirement import writer_harness as writer_fixture
from tests.test_writer_attestation import PROOF, REGISTER, WITNESS, closure, post, recover
from tests.test_writer_retirement import intent

writer_harness = writer_fixture
sh, wh, suppliers = session_fixture, website_fixture, supplier_fixture


async def test_website_registration_resolves_retained_root_and_recovers_after_rollback(
    writer_harness,
    suppliers,
    monkeypatch,
):
    h = writer_harness
    _, case, _ = await human_intents(h, suppliers)
    value = await work(h)
    begin = identity(h, value)
    descriptor = dict(
        **scope(h),
        **value["item"],
        contract_version=1,
        channel="WEBSITE_WIDGET",
        session_id=h.created["session_id"],
        supplier_instance_id=begin["supplier_instance_id"],
        process_incarnation_id=begin["process_incarnation_id"],
    )
    assert "conversation_id" not in descriptor and "session_token" not in descriptor
    token = secrets.token_urlsafe(32)
    values = h.settings.model_dump()
    values.update(
        writer_attestation_required=True,
        writer_producers=[
            dict(
                principal_id="website-parent",
                token=token,
                grants=[
                    dict(
                        owner_id=descriptor["owner_id"],
                        integration_id=descriptor["integration_id"],
                        agent_id=descriptor["agent_id"],
                        channel="WEBSITE_WIDGET",
                    )
                ],
            )
        ],
    )
    h.settings = Settings(**values, _env_file=None)
    h.client._transport.app.state.settings = h.settings
    h.producer_headers = {"X-Writer-Producer-Token": token}
    h.proof_headers = {
        "X-Writer-Supervisor-Token": h.settings.writer_supervisor_token.get_secret_value()
    }
    registration = dict(contract_version=1, registration_id=str(uuid4()), descriptor=descriptor)
    assert not (await call(h, "begin", begin)).json()["send"]
    invalid = dict(registration, descriptor=dict(descriptor, binding_generation=999))
    assert (await post(h, REGISTER, invalid)).status_code == 404
    response = await post(h, REGISTER, registration)
    assert response.status_code == 200, response.text
    assert (await call(h, "begin", begin)).json()["send"]
    assert (await post(h, WITNESS, closure(registration))).status_code == 200
    command = (await recover(h, registration))["command"]
    assert command["conversation_id"] == str(case.conversation_id)
    assert "session_token" not in command
    h.settings = h.settings.model_copy(
        update={"website_handoff_cohort_integration_ids": (), "website_bindings": ()}
    )
    h.client._transport.app.state.settings = h.settings
    assert (await post(h, PROOF, command, h.proof_headers)).status_code == 200
    from services import writer_attestation

    async def local_proof(request, digest, settings):
        return await writer_attestation.proof(h.database, request, settings)

    monkeypatch.setattr("services.writer_retirement._termination_proof", local_proof)
    from retirement_worker import sweep

    assert await sweep(h.database, h.settings) == 1
    assert (await intent(h, command)).status == "UNKNOWN"
