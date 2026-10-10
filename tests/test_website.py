"""Real PostgreSQL Website arbitration, FIFO recovery and HTTP evidence behavior."""
import asyncio
import io
import logging
import secrets
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import select, text, update

from core.config import Settings
from core.logging import configure_logging
from models import AgentTurn, Conversation, WebsiteAdmission
from schemas.contracts import OperationReference, TurnClaim, TurnExecute
from schemas.managed import ManagedTurnResult
from schemas.website import ManagedEvidenceResult
from services import managed_execution, turns
from services.website_worker import WebsiteWorker, select_immediate
from tests.conftest import Harness, app_client

BASE = "/website"


class FakeAI:
    def __init__(self):
        self.calls = []
        self.refs = []
        self.pause = 0
        self.evidence_calls = 0
        self.uncertain = False

    async def execute(self, prepared, binding, request_id=""):
        self.calls.append(prepared)
        if self.pause:
            await asyncio.sleep(self.pause)
        if self.uncertain:
            raise RuntimeError("discarded fake transport error")
        value = prepared.request.model_dump(mode="json")
        return ManagedTurnResult.model_validate({
            **{key: value[key] for key in (
                "contract_version", "conversation_id", "turn_id", "agent_id", "input_revision",
                "authority_revision", "input_first_sequence", "input_last_sequence",
                "context_last_sequence")},
            "status": "DRAFT_READY", "draft_text": "retained reply", "error_code": None,
            "operation_refs": self.refs,
        })

    async def evidence(self, request, binding):
        self.evidence_calls += 1
        return ManagedEvidenceResult(conversation_id=request.conversation_id,
                                     turn_id=request.turn_id, agent_id=request.agent_id,
                                     operation_refs=[OperationReference.model_validate(r)
                                                     for r in self.refs])


@pytest_asyncio.fixture
async def wh(sql, database_url):
    integration, agent = str(uuid4()), str(uuid4())
    grant = dict(owner_id="website-owner", integration_id=integration, channel="WEBSITE_WIDGET",
                 provider_namespace="main-chota.website-widget.v1")
    tokens = {name: secrets.token_urlsafe(32) for name in ("a", "worker", "other", "ai")}
    other = dict(grant, integration_id=str(uuid4()))
    settings = Settings(database_url=database_url, auth_principals=[
        dict(principal_id="a", token=tokens["a"], role="WEBSITE", grants=[grant]),
        dict(principal_id="worker", token=tokens["worker"], role="PROCESSING", grants=[grant]),
        dict(principal_id="other", token=tokens["other"], role="WEBSITE", grants=[other]),
    ], website_bindings=[dict(owner_id=grant["owner_id"], integration_id=integration,
                             agent_id=agent)],
        website_worker_enabled=True, website_worker_principal_id="worker",
        website_worker_lease_seconds=5,
        managed_ai_base_url="http://127.0.0.1:1",
        managed_ai_bindings=[dict(owner_id=grant["owner_id"], integration_id=integration,
                                 channel="WEBSITE_WIDGET", agent_id=agent,
                                 service_token=tokens["ai"])], _env_file=None)
    async with app_client(settings, database_url) as (client, database):
        h = Harness(client, database, settings, tokens, {"a": grant, "other": other})
        h.agent_id = agent
        h.ai = FakeAI()
        client._transport.app.state.managed_port = h.ai
        h.worker = WebsiteWorker(database, settings, h.ai)
        h.principal = settings.auth_principals[1].principal()
        yield h


def payload(wh, **changes):
    value = dict(owner_id=wh.grants["a"]["owner_id"],
                 integration_id=wh.grants["a"]["integration_id"], agent_id=wh.agent_id,
                 session_id=str(uuid4()), message_id=str(uuid4()), text="hello",
                 desired_route="MANAGED")
    value.update(changes)
    return value


def identity(value):
    return {k: v for k, v in value.items() if k not in {"text", "desired_route"}}


async def admit(wh, value):
    response = await wh.call("POST", BASE + "/admissions", value)
    assert response.status_code == 200, response.text
    return response.json()


async def query(wh, value):
    response = await wh.call("POST", BASE + "/results", identity(value))
    assert response.status_code == 200, response.text
    return response.json()


async def test_arbitration_sticky_both_policy_directions_and_no_legacy_message(wh, sql):
    value = payload(wh)
    first = await admit(wh, value)
    assert first["route"] == "MANAGED" and not first["replayed"]
    replay = await admit(wh, dict(value, desired_route="LEGACY"))
    assert replay == dict(first, replayed=True)
    legacy = payload(wh, desired_route="LEGACY")
    accepted = await admit(wh, legacy)
    assert accepted["route"] == "LEGACY"
    assert await admit(wh, dict(legacy, desired_route="MANAGED")) == dict(accepted, replayed=True)
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM messages")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM website_admissions")) == 2
    assert (await wh.call("POST", BASE + "/results", identity(legacy))).status_code == 404


@pytest.mark.parametrize("change", ["text", "session_id", "agent_id"])
async def test_changed_envelope_cannot_replay(wh, change):
    value = payload(wh)
    await admit(wh, value)
    changed = dict(value, **{change: "different" if change == "text" else str(uuid4())})
    response = await wh.call("POST", BASE + "/admissions", changed)
    assert response.status_code == (404 if change == "agent_id" else 409)


async def test_external_db_overlap_old_and_new_policy_one_frozen_winner(wh, sql):
    value = payload(wh)
    # Separate transactions wait on the exact same admission serialization lock.
    from services.common import fingerprint
    digest = fingerprint({"owner": value["owner_id"], "integration": value["integration_id"],
                          "namespace": "main-chota.website-widget.v1",
                          "message": value["message_id"]})
    async with sql.begin() as blocker:
        await blocker.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                              {"key": int(digest[:16], 16) - (1 << 63)})
        calls = [asyncio.create_task(admit(wh, dict(value, desired_route=route)))
                 for route in ("MANAGED", "LEGACY")]
        from tests.conftest import observed_waiters
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await observed_waiters(sql, pid, 2, "Website managed/rollback atomic arbitration")
    results = await asyncio.gather(*calls)
    assert results[0]["route"] == results[1]["route"]
    assert results[0]["admission_id"] == results[1]["admission_id"]
    assert sorted(r["replayed"] for r in results) == [False, True]


async def test_distinct_ids_identical_text_fifo_frozen_prefix_restart(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    second = dict(value, message_id=str(uuid4()))
    await admit(wh, second)
    assert (await query(wh, value))["state"] == "PENDING"
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    assert selected == await select_immediate(wh.database, wh.principal, wh.settings)
    # Independent new worker instance discovers same pending durable Turn.
    worker = WebsiteWorker(wh.database, wh.settings, wh.ai)
    assert await worker.run_once()
    assert (await query(wh, value))["state"] == "DRAFT_READY"
    assert (await query(wh, second))["state"] == "PENDING"
    assert await worker.run_once()
    assert len(wh.ai.calls) == 2
    assert wh.ai.calls[0].request.context_last_sequence == 1
    assert wh.ai.calls[1].request.context_last_sequence == 2
    assert wh.ai.calls[0].request.turn_id == selected
    assert wh.ai.calls[0].request.turn_id != wh.ai.calls[1].request.turn_id
    assert wh.ai.calls[1].request.input_first_sequence == 2
    assert not await worker.run_once()
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM website_immediate_turns")) == 2


async def test_ineligible_first_candidate_page_does_not_starve_eligible_work(wh, sql):
    admitted = {}
    for _ in range(101):
        value = payload(wh)
        await admit(wh, value)
        admitted[value["session_id"]] = value

    async with sql.begin() as conn:
        conversation_ids = (await conn.scalars(
            select(Conversation.id).order_by(Conversation.id)
        )).all()
        assert len(conversation_ids) == 101
        await conn.execute(update(Conversation)
                           .where(Conversation.id.in_(conversation_ids[:100]))
                           .values(control_mode="HUMAN"))
        eligible_id = conversation_ids[100]
        eligible_session = await conn.scalar(
            select(WebsiteAdmission.session_id)
            .where(WebsiteAdmission.conversation_id == eligible_id)
        )

    assert await wh.worker.run_once()
    assert len(wh.ai.calls) == 1
    assert wh.ai.calls[0].request.conversation_id == eligible_id
    assert (await query(wh, admitted[str(eligible_session)]))["state"] == "DRAFT_READY"


async def test_namespace_scope_and_narrow_role_deny_foundation_and_openapi(wh):
    value = payload(wh)
    accepted = await admit(wh, value)
    for path in ("/inbound-events", "/turns/" + str(uuid4()) + "/claim",
                 "/conversations/" + str(uuid4()) + "/control"):
        assert (await wh.call("POST", path, {})).status_code == 404
    assert (await wh.client.get("/internal/openapi.json", headers=wh.headers())).status_code == 404
    assert (await wh.call("POST", BASE + "/admissions", value, "other")).status_code == 404
    assert (await wh.call("POST", BASE + "/admissions", value, "worker")).status_code == 404
    assert accepted["route"] == "MANAGED"


async def test_claim_before_marker_reclaims_same_turn_and_dispatches_once(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    old = await turns.claim_turn(wh.database, wh.principal, selected,
                                TurnClaim(worker_ref="crashed", lease_seconds=5))
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE agent_turns SET lease_expires_at=clock_timestamp() "
                                "- interval '1 second' WHERE id=:id"), {"id": selected})
    assert await wh.worker.run_once()
    assert len(wh.ai.calls) == 1 and wh.ai.calls[0].request.turn_id == selected
    fresh = await turns.get_turn(wh.database, wh.principal, selected)
    assert fresh.claim_epoch == old.turn.claim_epoch + 1


async def test_marked_recovery_unknown_reads_refs_without_execute_and_late_query(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    claim = await turns.claim_turn(wh.database, wh.principal, selected,
                                  TurnClaim(worker_ref="crashed", lease_seconds=5))
    from pydantic import SecretStr
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(wh.database, wh.principal, selected, proof,
                                                    wh.settings)
    prepared = managed_execution.prepare_snapshot(snapshot)
    await managed_execution.mark_dispatch(wh.database, wh.principal, selected, proof, prepared)
    wh.ai.refs = [dict(origin="AI_AGENT_SERVICE", operation_id="opaque:effect", outcome="UNKNOWN")]
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE agent_turns SET lease_expires_at=clock_timestamp() "
                                "- interval '1 second' WHERE id=:id"), {"id": selected})
    assert await wh.worker.run_once()
    assert not wh.ai.calls and wh.ai.evidence_calls == 1
    assert (await query(wh, value))["state"] == "UNKNOWN"
    terminal = await turns.get_turn(wh.database, wh.principal, selected)
    assert terminal.operation_refs[0].operation_id == "opaque:effect"
    wh.ai.refs.append(dict(origin="AI_AGENT_SERVICE", operation_id="late:effect",
                           outcome="SUCCEEDED"))
    response = await wh.call("GET", f"/processing/turns/{selected}/evidence", principal="worker")
    assert response.status_code == 200 and len(response.json()["operation_refs"]) == 2
    assert await turns.get_turn(wh.database, wh.principal, selected) == terminal
    assert (await wh.call("GET", f"/processing/turns/{selected}/evidence")).status_code == 404


async def test_normal_uncertainty_retains_refs_but_never_reexecutes(wh):
    value = payload(wh)
    await admit(wh, value)
    wh.ai.uncertain = True
    wh.ai.refs = [dict(origin="AI_AGENT_SERVICE", operation_id="lost:effect", outcome="SUCCEEDED")]
    assert await wh.worker.run_once()
    assert (await query(wh, value))["state"] == "UNKNOWN"
    assert not await wh.worker.run_once()
    assert len(wh.ai.calls) == 1 and wh.ai.evidence_calls == 1


async def test_lease_renews_during_long_execution(wh):
    value = payload(wh)
    await admit(wh, value)
    wh.ai.pause = 6
    assert await wh.worker.run_once()
    assert (await query(wh, value))["state"] == "DRAFT_READY"
    assert len(wh.ai.calls) == 1


async def test_draft_replay_response_attempts_and_socket_fact_never_delivered(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    assert await wh.worker.run_once()
    first = dict(identity(value), response_attempt_id=str(uuid4()))
    began = await wh.call("POST", BASE + "/responses/begin", first)
    assert began.status_code == 200 and began.json()["draft_text"] == "retained reply"
    assert (await wh.call("POST", BASE + "/responses/begin", first)).json()["replayed"]
    for outcome in ("WRITE_COMPLETED", "WRITE_FAILED", "UNKNOWN"):
        attempt = dict(identity(value), response_attempt_id=str(uuid4()))
        assert (await wh.call("POST", BASE + "/responses/begin", attempt)).status_code == 200
        finished = dict(attempt, outcome=outcome)
        response = await wh.call("POST", BASE + "/responses/complete", finished)
        assert response.status_code == 200 and response.json()["outcome"] == outcome
        assert (await wh.call("POST", BASE + "/responses/complete", finished)).json()["replayed"]
        changed = "UNKNOWN" if outcome != "UNKNOWN" else "WRITE_COMPLETED"
        assert (await wh.call("POST", BASE + "/responses/complete",
                              dict(finished, outcome=changed))).status_code == 409
    assert not await wh.worker.run_once() and len(wh.ai.calls) == 1
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM delivery_attempts")) == 1
        row = await conn.execute(text(
            "SELECT status,delivered_at,read_at FROM delivery_attempts"))
        assert tuple(row.first()) == ("UNKNOWN", None, None)
        assert await conn.scalar(text("SELECT count(*) FROM website_response_attempts")) == 4


async def test_response_replay_binding_scope_and_agent_reassignment_denied(wh):
    value = payload(wh)
    await admit(wh, value)
    await wh.worker.run_once()
    attempt = dict(identity(value), response_attempt_id=str(uuid4()))
    assert (await wh.call("POST", BASE + "/responses/begin", attempt)).status_code == 200
    assert (await wh.call("POST", BASE + "/responses/begin", attempt, "other")).status_code == 404
    altered = dict(attempt, session_id=str(uuid4()))
    assert (await wh.call("POST", BASE + "/responses/begin", altered)).status_code == 404


@pytest.mark.parametrize("missing", ["message_id", "session_id", "agent_id", "owner_id"])
async def test_required_stable_trusted_identity_fields(wh, missing):
    value = payload(wh)
    del value[missing]
    assert (await wh.call("POST", BASE + "/admissions", value)).status_code == 422


async def test_atomic_failure_rolls_back_admission_and_all_inbound_parents(wh, sql):
    value = payload(wh)
    async with sql.begin() as conn:
        await conn.execute(text("ALTER TABLE website_admissions ADD CONSTRAINT test_fail "
                                "CHECK(route <> 'MANAGED')"))
    try:
        assert (await wh.call("POST", BASE + "/admissions", value)).status_code == 500
        async with sql.connect() as conn:
            for table in ("website_admissions", "inbound_events", "messages", "conversations"):
                assert await conn.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        async with sql.begin() as conn:
            await conn.execute(text("ALTER TABLE website_admissions DROP CONSTRAINT test_fail"))


def test_logging_all_sinks_descendants_exceptions_and_reconfiguration_are_safe():
    sentinel = "fake-widget-secret-query-header-proof"
    stream = io.StringIO()
    configure_logging("DEBUG")
    independent = logging.StreamHandler(stream)
    independent.setFormatter(logging.Formatter("%(message)s %(exc_text)s"))
    child = logging.getLogger("httpx.untrusted.child")
    child.setLevel(logging.DEBUG)
    child.addHandler(independent)
    child.propagate = False
    for level in (logging.DEBUG, logging.INFO, logging.ERROR):
        child.log(level, sentinel)
    app = logging.getLogger("test-independent-app")
    app.addHandler(independent)
    app.propagate = False
    try:
        raise ValueError(sentinel)
    except ValueError:
        app.exception(sentinel, extra={"code": "SERVICE_UNAVAILABLE", "stage": "request"},
                      stack_info=True)
    logging.basicConfig(force=True, level=logging.DEBUG, handlers=[independent])
    logging.getLogger("httpcore.foo.bar").critical(sentinel)
    app.error(ValueError(sentinel), extra={"code": "SERVICE_UNAVAILABLE", "stage": "request"})
    output = stream.getvalue()
    assert sentinel not in output and "ValueError" not in output
    assert "SERVICE_UNAVAILABLE" in output
    child.removeHandler(independent)
    app.removeHandler(independent)
    configure_logging("INFO")


async def test_new_website_window_continues_beyond_v1_prefix_limit(wh, sql):
    # New Website work freezes a bounded absolute v2 window beyond sequence256.
    value = payload(wh)
    for index in range(257):
        await admit(wh, dict(value, message_id=str(uuid4()), text=str(index)))
    for _ in range(257):
        assert await wh.worker.run_once()
    async with wh.database.session_factory() as session:
        final = await session.scalar(select(AgentTurn).order_by(
            AgentTurn.input_last_sequence.desc()).limit(1))
        assert final.input_last_sequence == 257
        assert final.status == "COMPLETED" and final.failure_code is None
        assert final.managed_contract_version == 2
        assert final.managed_context_first_sequence == 2
    assert len(wh.ai.calls) == 257
    assert [m.sequence for m in wh.ai.calls[-1].request.messages] == list(range(2, 258))


async def test_recovery_no_evidence_stays_unknown_and_does_not_repeat(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    claim = await turns.claim_turn(wh.database, wh.principal, selected,
                                  TurnClaim(worker_ref="crashed", lease_seconds=5))
    from pydantic import SecretStr
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(wh.database, wh.principal, selected, proof,
                                                    wh.settings)
    await managed_execution.mark_dispatch(wh.database, wh.principal, selected, proof,
                                         managed_execution.prepare_snapshot(snapshot))

    async def no_evidence(request, binding):
        raise RuntimeError("safe discarded credential exception")
    wh.ai.evidence = no_evidence
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE agent_turns SET lease_expires_at=clock_timestamp() "
                                "- interval '1 second' WHERE id=:id"), {"id": selected})
    assert await wh.worker.run_once()
    assert (await query(wh, value))["state"] == "UNKNOWN"
    terminal = await turns.get_turn(wh.database, wh.principal, selected)
    assert terminal.operation_refs == []
    assert not await wh.worker.run_once() and not wh.ai.calls


async def test_evidence_fencing_rechecks_live_winning_proof_after_network(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    claim = await turns.claim_turn(wh.database, wh.principal, selected,
                                  TurnClaim(worker_ref="claimed", lease_seconds=5))
    from pydantic import SecretStr
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(wh.database, wh.principal, selected, proof,
                                                    wh.settings)
    await managed_execution.mark_dispatch(wh.database, wh.principal, selected, proof,
                                         managed_execution.prepare_snapshot(snapshot))
    original = wh.ai.evidence

    async def lose_lease(request, binding):
        async with sql.begin() as conn:
            await conn.execute(text("UPDATE agent_turns SET lease_expires_at=clock_timestamp() "
                                    "- interval '1 second' WHERE id=:id"), {"id": selected})
        return await original(request, binding)
    wh.ai.evidence = lose_lease
    from core.errors import DomainError
    from services.website_evidence import recovery_refs
    with pytest.raises(DomainError):
        await recovery_refs(wh.database, wh.principal, selected, proof, wh.settings, wh.ai)
    terminal = await turns.get_turn(wh.database, wh.principal, selected)
    assert terminal.status == "CLAIMED" and terminal.operation_refs == []


async def test_changed_authority_after_draft_cannot_begin_response(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    await wh.worker.run_once()
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE conversations SET authority_revision=authority_revision+1"))
    attempt = dict(identity(value), response_attempt_id=str(uuid4()))
    response = await wh.call("POST", BASE + "/responses/begin", attempt)
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "AUTHORITY_REVISION_CONFLICT"
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM website_response_attempts")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM delivery_attempts")) == 0


@pytest.mark.parametrize(
    "invalid", ["principal", "role", "grant_namespace", "ai_agent", "duplicate"])
async def test_worker_startup_requires_exact_complete_processing_configuration(wh, invalid):
    config = wh.settings.model_dump(mode="python")
    if invalid == "principal":
        config["website_worker_principal_id"] = "not-configured"
    elif invalid == "role":
        config["auth_principals"][1]["role"] = "WEBSITE"
    elif invalid == "grant_namespace":
        config["auth_principals"][1]["grants"][0]["provider_namespace"] = "different.namespace"
    elif invalid == "ai_agent":
        config["managed_ai_bindings"][0]["agent_id"] = uuid4()
    elif invalid == "duplicate":
        config["website_bindings"] = [*config["website_bindings"], config["website_bindings"][0]]
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Settings.model_validate(config)
