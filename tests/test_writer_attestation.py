"""Real PostgreSQL lifecycle closure races and exact recovery identity."""

import asyncio
import secrets
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from core.config import Settings
from models import WriterRecoveryCommand, WriterRegistration, WriterWitness
from services import writer_attestation
from tests.conftest import observed_waiters
from tests.test_handoff import begin_request, claim_and_send_human, telegram_work
from tests.test_handoff import inbox_harness as inbox_fixture
from tests.test_writer_retirement import intent
from tests.test_writer_retirement import user_me_transport as user_me_fixture
from tests.test_writer_retirement import writer_harness as writer_fixture

inbox_harness = inbox_fixture
writer_harness = writer_fixture
user_me_transport = user_me_fixture
REGISTER = "/internal/v1/writer-attestations/register"
WITNESS = "/internal/v1/writer-attestations/witness"
PROOF = "/internal/v1/writer-termination/proof"
RECOVERY = "/internal/v1/outbound-writers/recovery"
BEGIN = "/internal/v2/telegram/delivery/begin"


async def prepared(h):
    await claim_and_send_human(h)
    descriptor = begin_request((await telegram_work(h))[0])
    token = secrets.token_urlsafe(32)
    values = h.settings.model_dump()
    values.update(
        writer_attestation_required=True,
        writer_producers=[
            dict(
                principal_id="gateway-parent",
                token=token,
                grants=[
                    dict(
                        owner_id=descriptor["owner_id"],
                        integration_id=descriptor["integration_id"],
                        agent_id=descriptor["agent_id"],
                        channel="TELEGRAM",
                    )
                ],
            )
        ],
    )
    h.settings = h.app.state.settings = Settings(**values, _env_file=None)
    h.producer_headers = {"X-Writer-Producer-Token": token}
    h.proof_headers = {
        "X-Writer-Supervisor-Token": h.settings.writer_supervisor_token.get_secret_value()
    }
    return dict(contract_version=1, registration_id=str(uuid4()), descriptor=descriptor)


def closure(registration):
    return dict(
        contract_version=1,
        registration_id=registration["registration_id"],
        proof_ref=str(uuid4()),
        proof_kind="TASKS_FENCED_AND_JOINED",
        fenced=True,
        tasks_ended=True,
        observed_at=datetime.now(UTC).isoformat(),
    )


async def post(h, path, payload, headers=None):
    return await h.client.post(path, json=payload, headers=headers or h.producer_headers)


async def recover(h, registration):
    result = await post(
        h,
        RECOVERY,
        dict(contract_version=1, registration_id=registration["registration_id"]),
        h.retirement_headers,
    )
    assert result.status_code == 200, result.text
    return result.json()


async def test_registration_has_no_authority_and_closed_fence_survives_rollback(
    writer_harness,
    user_me_transport,
):
    h = writer_harness
    registration = await prepared(h)
    before = await intent(h, registration["descriptor"])
    assert not (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    first = await post(h, REGISTER, registration)
    assert first.status_code == 200 and first.json()["state"] == "OPEN"
    assert (await post(h, REGISTER, registration)).json()["replayed"]
    after = await intent(h, registration["descriptor"])
    assert (after.writer_state, after.status, after.revision) == (
        before.writer_state,
        before.status,
        before.revision,
    )
    witness = closure(registration)
    assert (await post(h, WITNESS, witness)).json()["state"] == "CLOSED"
    assert (await post(h, WITNESS, witness)).json()["replayed"]
    values = h.settings.model_dump()
    values["writer_attestation_required"] = False
    h.settings = h.app.state.settings = Settings(**values, _env_file=None)
    assert not (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    assert (await recover(h, registration))["state"] == "DONE"
    assert (await intent(h, registration["descriptor"])).writer_state == "NONE"


async def test_only_winner_proof_and_exact_recovery_after_restart_and_lost_ack(
    writer_harness,
    user_me_transport,
    monkeypatch,
):
    h = writer_harness
    registration = await prepared(h)
    loser = dict(
        registration,
        registration_id=str(uuid4()),
        descriptor=dict(
            registration["descriptor"],
            supplier_instance_id=str(uuid4()),
            process_incarnation_id=str(uuid4()),
        ),
    )
    results = await asyncio.gather(post(h, REGISTER, registration), post(h, REGISTER, loser))
    assert [r.status_code for r in results] == [200, 200]
    begin = await post(h, BEGIN, registration["descriptor"], h.service_headers)
    assert begin.status_code == 200 and begin.json()["send"]
    assert not (await post(h, BEGIN, loser["descriptor"], h.service_headers)).json()["send"]
    lost = closure(loser)
    assert (await post(h, WITNESS, lost)).status_code == 200
    assert (await recover(h, loser))["state"] == "DONE"
    proof_command = dict(
        loser["descriptor"],
        work_kind="RECOVER_ONLY",
        command_id=str(uuid4()),
        expected_delivery_revision=1,
        proof_ref=lost["proof_ref"],
    )
    assert (await post(h, PROOF, proof_command, h.proof_headers)).status_code == 404
    closed = closure(registration)
    assert (await post(h, WITNESS, closed)).status_code == 200
    command = (await recover(h, registration))["command"]
    assert command["expected_delivery_revision"] == 1
    await h.database.dispose()
    assert (await recover(h, registration))["command"] == command

    async def local_proof(request, digest, settings):
        assert h.database.engine.pool.checkedout() == 0
        return await writer_attestation.proof(h.database, request, settings)

    monkeypatch.setattr("services.writer_retirement._termination_proof", local_proof)
    response = await post(h, "/internal/v1/outbound-writers/retire", command, h.retirement_headers)
    assert response.status_code == 200, response.text
    # Lost response/restart recovers the exact persisted command before current revision checks.
    await h.database.dispose()
    assert (await recover(h, registration))["command"] == command
    replay = await post(h, "/internal/v1/outbound-writers/retire", command, h.retirement_headers)
    assert replay.status_code == 200 and replay.json()["replayed"]
    assert (await intent(h, command)).status == "UNKNOWN"
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WriterRecoveryCommand)) == 1


@pytest.mark.parametrize("closure_first", [True, False])
async def test_begin_and_closure_serialize_on_actual_scope_lock(
    writer_harness,
    user_me_transport,
    sql,
    closure_first,
):
    h = writer_harness
    registration = await prepared(h)
    assert (await post(h, REGISTER, registration)).status_code == 200
    closed = closure(registration)
    first_path, first_payload = (
        (WITNESS, closed) if closure_first else (BEGIN, registration["descriptor"])
    )
    second_path, second_payload = (
        (BEGIN, registration["descriptor"]) if closure_first else (WITNESS, closed)
    )
    async with sql.begin() as blocker:
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(text("SELECT owner_id FROM inbox_grant_scopes FOR UPDATE"))
        first = asyncio.create_task(
            post(
                h,
                first_path,
                first_payload,
                h.producer_headers if closure_first else h.service_headers,
            )
        )
        await observed_waiters(sql, pid, 1, "attestor first waiter")
        second = asyncio.create_task(
            post(
                h,
                second_path,
                second_payload,
                h.service_headers if closure_first else h.producer_headers,
            )
        )
        await observed_waiters(sql, pid, 2, "attestor ordered begin/closure")
    responses = await asyncio.gather(first, second)
    assert all(r.status_code == 200 for r in responses)
    result = responses[1 if closure_first else 0].json()
    assert result["send"] is (not closure_first)
    retained = await intent(h, registration["descriptor"])
    assert retained.writer_state == ("NONE" if closure_first else "LIVE")


async def test_producer_auth_strict_payload_and_immutable_evidence(
    writer_harness,
    user_me_transport,
    sql,
):
    h = writer_harness
    registration = await prepared(h)
    for headers in (h.service_headers, h.retirement_headers, h.proof_headers):
        assert (await post(h, REGISTER, registration, headers)).status_code == 404
    assert (await post(h, REGISTER, dict(registration, text="forbidden"))).status_code == 422
    assert (await post(h, REGISTER, registration)).status_code == 200
    for field in ("fenced", "tasks_ended"):
        assert (
            await post(h, WITNESS, dict(closure(registration), **{field: 1}))
        ).status_code == 422
    closed = closure(registration)
    assert (await post(h, WITNESS, closed)).status_code == 200
    assert (await post(h, WITNESS, dict(closed, proof_ref=str(uuid4())))).status_code == 409
    from sqlalchemy.exc import DBAPIError

    for table in ("writer_registrations", "writer_witnesses"):
        with pytest.raises(DBAPIError):
            async with sql.begin() as connection:
                await connection.execute(text(f"DELETE FROM {table}"))
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WriterRegistration)) == 1
        assert await session.scalar(select(func.count()).select_from(WriterWitness)) == 1


async def test_retained_telegram_recovery_survives_binding_and_cohort_removal(
    writer_harness,
    user_me_transport,
    monkeypatch,
):
    h = writer_harness
    registration = await prepared(h)
    assert (await post(h, REGISTER, registration)).status_code == 200
    assert (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    assert (await post(h, WITNESS, closure(registration))).status_code == 200
    command = (await recover(h, registration))["command"]
    values = h.settings.model_dump()
    values.update(
        handoff_cohort_integration_ids=(),
        telegram_cohort_integration_ids=(),
        managed_ai_bindings=(),
        telegram_worker_enabled=False,
    )
    h.settings = h.app.state.settings = Settings(**values, _env_file=None)
    proof = await post(h, PROOF, command, h.proof_headers)
    assert proof.status_code == 200, proof.text

    async def local_proof(request, digest, settings):
        return await writer_attestation.proof(h.database, request, settings)

    monkeypatch.setattr("services.writer_retirement._termination_proof", local_proof)
    from retirement_worker import sweep

    assert await sweep(h.database, h.settings) == 1
    assert (await intent(h, command)).writer_state == "RETIRED"
    assert (await post(h, BEGIN, registration["descriptor"], h.service_headers)).status_code == 404


async def test_no_witness_stays_live_and_stale_command_is_retained_before_new_attempt(
    writer_harness,
    user_me_transport,
):
    h = writer_harness
    registration = await prepared(h)
    assert (await post(h, REGISTER, registration)).status_code == 200
    assert (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    # Parent death / no positively retained observation cannot retire anything.
    from retirement_worker import sweep

    assert (await recover(h, registration))["state"] == "PENDING"
    assert await sweep(h.database, h.settings) == 0
    assert (await intent(h, registration["descriptor"])).writer_state == "LIVE"
    # Newly attempting retrospective provenance for an acquired writer is forbidden.
    late = dict(
        registration,
        registration_id=str(uuid4()),
        descriptor=dict(registration["descriptor"], supplier_instance_id=str(uuid4())),
    )
    assert (await post(h, REGISTER, late)).status_code == 409
    assert (await post(h, WITNESS, closure(registration))).status_code == 200
    original = (await recover(h, registration))["command"]
    # Original provider evidence may win while recovery is preparing/submitting.
    complete = dict(
        registration["descriptor"],
        outcome="PROVIDER_ACCEPTED",
        provider_message_id="retained-provider-receipt",
    )
    response = await post(h, "/internal/v2/telegram/delivery/complete", complete, h.service_headers)
    assert response.status_code == 200, response.text
    rejected = await post(h, "/internal/v1/outbound-writers/retire", original, h.retirement_headers)
    assert rejected.status_code == 409
    replacement = (await recover(h, registration))["command"]
    assert replacement["command_id"] != original["command_id"]
    assert replacement["expected_delivery_revision"] == 2
    assert replacement["proof_ref"] == original["proof_ref"]
    async with h.database.session_factory() as session:
        retained = (
            await session.scalars(
                select(WriterRecoveryCommand).order_by(WriterRecoveryCommand.expected_revision)
            )
        ).all()
        assert [r.command for r in retained] == [original, replacement]


async def test_migration_preserves_evidence_and_refuses_unsafe_downgrade(
    writer_harness,
    user_me_transport,
    database_url,
):
    h = writer_harness
    registration = await prepared(h)
    assert (await post(h, REGISTER, registration)).status_code == 200
    from tests.test_managed_migration import migrate

    rejected = migrate(database_url, "downgrade", "0018_website_poll_admission", expected=1)
    assert "Retained writer evidence requires a compatible reader" in rejected.stderr
    async with h.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WriterRegistration)) == 1
    assert await h.database.ready()


@pytest.mark.parametrize(
    "reserved_id,path",
    [
        ("writer-producer:gateway-parent", REGISTER),
        ("writer-proof-reader", PROOF),
        ("delivery-writer-supervisor", RECOVERY),
    ],
)
async def test_full_service_principal_name_cannot_impersonate_dedicated_credentials(
    writer_harness,
    user_me_transport,
    reserved_id,
    path,
):
    h = writer_harness
    registration = await prepared(h)
    token = secrets.token_urlsafe(32)
    values = h.settings.model_dump()
    entries = [entry.model_dump() for entry in h.settings.auth_principals]
    entries.append(
        dict(
            principal_id=reserved_id,
            token=token,
            role="FULL",
            grants=[h.settings.auth_principals[0].grants[0].model_dump()],
        )
    )
    values["auth_principals"] = entries
    h.settings = h.app.state.settings = Settings(**values, _env_file=None)
    result = await post(h, path, registration, {"X-Service-Token": token})
    assert result.status_code == 404


async def test_begin_must_match_the_registered_descriptor(writer_harness, user_me_transport):
    h = writer_harness
    registration = await prepared(h)
    wrong = dict(
        registration,
        descriptor=dict(
            registration["descriptor"], bot_id=registration["descriptor"]["bot_id"] + 1
        ),
    )
    assert (await post(h, REGISTER, wrong)).status_code == 200
    assert not (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    assert (await intent(h, registration["descriptor"])).writer_state == "NONE"
