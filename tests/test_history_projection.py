"""Real-PG frozen windows, physical evidence, provenance and concurrency."""
import hashlib
import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy import select, text

from core.errors import DomainError
from models import (
    AgentTurn,
    Conversation,
    Message,
    TurnInputMessage,
    WebsiteAdmission,
    WebsiteImmediateTurn,
)
from schemas.contracts import ErrorCode, TurnClaim, TurnComplete, TurnCreate, TurnExecute
from schemas.managed import MAX_REQUEST_BYTES, ManagedTurnRequest, prepare_request
from services import managed_execution, turns
from services.common import fingerprint
from services.history_projection import (
    canonical_window,
    freeze_website_projection,
    projected_request,
)
from services.website_worker import WebsiteWorker, select_immediate
from tests.conftest import locked_race
from tests.test_database import violation
from tests.test_managed_contracts import wire_request
from tests.test_managed_migration import migrate
from tests.test_website import BASE, admit, identity, payload, query, v2_response_attempt_id
from tests.test_website import wh as website_fixture

wh = website_fixture
RESPONSE_ATTEMPT_IDS: dict[str, str] = {}


def window(first=245, bound=500, selected_first=500, selected_last=500):
    value = wire_request(bound - first + 1, 1, 1)
    value.update(contract_version=2, context_last_sequence=bound,
                 input_first_sequence=selected_first, input_last_sequence=selected_last)
    for sequence, message in enumerate(value["messages"], first):
        message["sequence"] = sequence
    value["selected_input_message_ids"] = [m["id"] for m in value["messages"]
        if selected_first <= m["sequence"] <= selected_last]
    return value


@pytest.mark.parametrize("selected_first", [497, 500])
def test_absolute_window_has_exact_selection_and_v1_default_stays_prefix(selected_first):
    request = ManagedTurnRequest.model_validate(window(selected_first=selected_first))
    assert [m.sequence for m in request.messages] == list(range(245, 501))
    assert request.selected_input_message_ids == tuple(m.id for m in request.messages[
        selected_first - 245:])
    assert prepare_request(request).request == request
    assert ManagedTurnRequest.model_validate(wire_request()).contract_version == 1
    old = window()
    del old["contract_version"]
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(old)


@pytest.mark.parametrize("mutation", ["empty", "gap", "duplicate", "reorder", "bound",
    "foreign_id", "selected_gap", "outside", "count", "text", "extra_start"])
def test_v2_rejects_invalid_windows(mutation):
    value = window(selected_first=497)
    if mutation == "empty":
        value["messages"] = []
    elif mutation == "gap":
        value["messages"][2]["sequence"] += 1
    elif mutation == "duplicate":
        value["messages"][2]["id"] = value["messages"][1]["id"]
    elif mutation == "reorder":
        value["messages"].reverse()
    elif mutation == "bound":
        value["context_last_sequence"] += 1
    elif mutation == "foreign_id":
        value["selected_input_message_ids"][0] = str(uuid4())
    elif mutation == "selected_gap":
        value["selected_input_message_ids"].pop(1)
    elif mutation == "outside":
        value["input_first_sequence"] = 244
    elif mutation == "count":
        value = window(first=244)
    elif mutation == "text":
        for m in value["messages"]:
            m["text"] = "é" * 16384
    else:
        value["context_first_sequence"] = 245
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(value)


def test_exact_text_and_serialized_utf8_caps_include_control_escaping():
    at = window(first=1, bound=16, selected_first=16, selected_last=16)
    for m in at["messages"]:
        m["text"] = "é" * 16384
    assert sum(len(m.text.encode()) for m in ManagedTurnRequest(**at).messages) == 524288
    at["messages"][0]["text"] = "é" * 16383
    assert prepare_request(ManagedTurnRequest(**at))
    over = window(first=1, bound=17, selected_first=17, selected_last=17)
    for m in over["messages"][:16]:
        m["text"] = "é" * 16384
    over["messages"][-1]["text"] = "x"
    with pytest.raises(ValidationError):
        ManagedTurnRequest(**over)
    escaped = window(first=1, bound=11, selected_first=11, selected_last=11)
    for m in escaped["messages"][:10]:
        m["text"] = "\x01" * 16384
    escaped["messages"][-1]["text"] = "x"
    delta = MAX_REQUEST_BYTES - len(prepare_request(ManagedTurnRequest(**escaped)).body)
    controls, singles = divmod(delta, 6)
    escaped["messages"][-1]["text"] = "x" + "\x01" * controls + "x" * singles
    assert len(prepare_request(ManagedTurnRequest(**escaped)).body) == MAX_REQUEST_BYTES
    exact = escaped["messages"][-1]["text"]
    escaped["messages"][-1]["text"] = exact[:-1] + ("xxxxx" if exact[-1] == "\x01" else "")
    assert len(prepare_request(ManagedTurnRequest(**escaped)).body) == MAX_REQUEST_BYTES - 1
    escaped["messages"][-1]["text"] = exact + "x"
    with pytest.raises(ValidationError):
        ManagedTurnRequest(**escaped)


async def complete(wh, value, outcome="WRITE_COMPLETED", *, expected_status=200):
    # A transport retry/reconciliation reuses the one retained response attempt.
    # A fresh attempt id must not borrow the earlier attempt's send permission.
    message_id = str(value["message_id"])
    response_attempt_id = RESPONSE_ATTEMPT_IDS.get(message_id)
    if response_attempt_id is None:
        response_attempt_id = await v2_response_attempt_id(wh, value)
        RESPONSE_ATTEMPT_IDS[message_id] = response_attempt_id
    attempt = dict(identity(value), response_attempt_id=response_attempt_id)
    begun = await wh.call("POST", BASE + "/responses/begin", attempt)
    assert begun.status_code == 200, begun.text
    done = dict(attempt, outcome=outcome)
    response = await wh.call("POST", BASE + "/responses/complete", done)
    assert response.status_code == expected_status, response.text
    return done if expected_status == 200 else response


async def history(sql):
    async with sql.connect() as conn:
        return (await conn.execute(text("SELECT id,conversation_id,sequence,actor,direction,text,"
            "content_type,"
            "source_agent_turn_id FROM messages ORDER BY sequence"))).mappings().all()


async def test_completed_promotes_retained_draft_once_replay_and_restart(wh, sql):
    value = payload(wh, text="Привет")
    await admit(wh, value)
    assert await wh.worker.run_once()
    assert len(await history(sql)) == 1
    done = await complete(wh, value)
    before = await history(sql)
    assert [m["actor"] for m in before] == ["CUSTOMER", "AGENT"]
    assert before[-1]["text"] == "retained reply"
    assert before[-1]["source_agent_turn_id"] == wh.ai.calls[0].request.turn_id
    assert (await wh.call("POST", BASE + "/responses/complete", done)).json()["replayed"]
    await wh.database.dispose()
    assert await query(wh, value) == dict(state="DRAFT_READY", draft_text="retained reply")
    await complete(wh, value)
    assert await history(sql) == before
    second = dict(value, message_id=str(uuid4()), text="Какие услуги")
    await admit(wh, second)
    worker = WebsiteWorker(wh.database, wh.settings, wh.ai)
    assert await worker.run_once()
    request = wh.ai.calls[-1].request
    assert request.contract_version == 2
    assert [m.actor for m in request.messages] == ["CUSTOMER", "AGENT", "CUSTOMER"]
    assert [m.id for m in request.messages[:2]] == [m["id"] for m in before]
    async with sql.connect() as conn:
        assert tuple((await conn.execute(text("SELECT last_message_sequence,input_revision "
            "FROM conversations"))).one()) == (3, 2)
        assert await conn.scalar(text("SELECT count(*) FROM website_admissions")) == 2
        assert await conn.scalar(text("SELECT count(*) FROM agent_turns")) == 2
        assert tuple((await conn.execute(text("SELECT status,delivered_at,read_at "
            "FROM delivery_attempts"))).one()) == ("UNKNOWN", None, None)
    assert len(wh.ai.calls) == 2


@pytest.mark.parametrize("outcome", ["WRITE_FAILED", "UNKNOWN"])
async def test_negative_fact_not_promoted_late_success_does_not_retrofit_frozen_turn(
    wh, sql, outcome
):
    first = payload(wh)
    await admit(wh, first)
    await wh.worker.run_once()
    await complete(wh, first, outcome)
    assert [m["actor"] for m in await history(sql)] == ["CUSTOMER"]
    second = dict(first, message_id=str(uuid4()), text="next")
    await admit(wh, second)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    async with wh.database.session_factory() as session:
        frozen = await session.get(AgentTurn, selected)
        digest = frozen.managed_projection_fingerprint
    if outcome == "WRITE_FAILED":
        await complete(wh, first, expected_status=409)
        assert [m["actor"] for m in await history(sql)] == ["CUSTOMER", "CUSTOMER"]
    else:
        # An authorized uncertain write may later be reconciled from UNKNOWN.
        await complete(wh, first)
    await wh.worker.run_once()
    assert wh.ai.calls[-1].fingerprint == digest
    assert [m.actor for m in wh.ai.calls[-1].request.messages] == ["CUSTOMER", "CUSTOMER"]
    third = dict(first, message_id=str(uuid4()), text="later")
    await admit(wh, third)
    await wh.worker.run_once()
    expected = ["CUSTOMER", "CUSTOMER", "AGENT", "CUSTOMER"]
    if outcome == "WRITE_FAILED":
        expected = ["CUSTOMER", "CUSTOMER", "CUSTOMER"]
    assert [m.actor for m in wh.ai.calls[-1].request.messages] == expected


@pytest.mark.parametrize("iteration", range(5))
async def test_concurrent_same_v2_completion_and_admission_serialize_actual_pg(
    wh, sql, iteration
):
    value = payload(wh)
    await admit(wh, value)
    await wh.worker.run_once()
    attempt = dict(identity(value),
                   response_attempt_id=await v2_response_attempt_id(wh, value))
    first = await wh.call("POST", BASE + "/responses/begin", attempt)
    assert first.status_code == 200 and first.json()["send"] is True
    # A canonical v2 admission owns one stable transport id. A repeated begin
    # may mark its unresolved write UNKNOWN but cannot mint another permission.
    retry = await wh.call("POST", BASE + "/responses/begin", attempt)
    assert retry.status_code == 200 and retry.json()["action"] == "DENY"
    assert retry.json()["send"] is False and retry.json()["replayed"] is True
    async with sql.connect() as conn:
        conversation_id = str(await conn.scalar(select(Conversation.id)))
    responses = await locked_race(sql, conversation_id, [
        wh.call("POST", BASE + "/responses/complete",
                dict(attempt, outcome="WRITE_COMPLETED")),
        wh.call("POST", BASE + "/responses/complete",
                dict(attempt, outcome="WRITE_COMPLETED")),
        wh.call("POST", BASE + "/admissions", dict(value, message_id=str(uuid4()))),
    ],
        f"Goal4A.1 same v2 transport retries and customer admission, iteration={iteration}")
    assert all(r.status_code == 200 for r in responses)
    replayed = await wh.call("POST", BASE + "/responses/complete",
                             dict(attempt, outcome="WRITE_COMPLETED"))
    assert replayed.status_code == 200 and replayed.json()["replayed"] is True
    rows = await history(sql)
    assert [m["sequence"] for m in rows] == [1, 2, 3]
    assert len([m for m in rows if m["actor"] == "AGENT"]) == 1
    async with sql.connect() as conn:
        assert await conn.scalar(select(Conversation.input_revision)) == 2


async def test_assistant_append_failure_rolls_back_physical_outcome_and_counters(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    await wh.worker.run_once()
    attempt = dict(identity(value),
                   response_attempt_id=await v2_response_attempt_id(wh, value))
    assert (await wh.call("POST", BASE + "/responses/begin", attempt)).status_code == 200
    async with sql.begin() as conn:
        await conn.execute(text("ALTER TABLE messages ADD CONSTRAINT history_test_failure "
                                "CHECK(actor<>'AGENT')"))
    done = dict(attempt, outcome="WRITE_COMPLETED")
    try:
        assert (await wh.call("POST", BASE + "/responses/complete", done)).status_code == 500
        async with sql.connect() as conn:
            assert await conn.scalar(text("SELECT outcome FROM website_response_attempts")) is None
            assert await conn.scalar(select(Conversation.last_message_sequence)) == 1
            assert await conn.scalar(select(Conversation.input_revision)) == 1
            assert await conn.scalar(text(
                "SELECT status FROM delivery_attempts")) == "DISPATCH_REQUESTED"
        assert len(await history(sql)) == 1
    finally:
        async with sql.begin() as conn:
            await conn.execute(text("ALTER TABLE messages DROP CONSTRAINT history_test_failure"))
    assert (await wh.call("POST", BASE + "/responses/complete", done)).status_code == 200
    assert len(await history(sql)) == 2


async def test_history_isolated_by_canonical_session_and_retained_scope(wh, sql):
    first = payload(wh, text="visitor-one")
    second = payload(wh, text="visitor-two")
    for value in (first, second):
        await admit(wh, value)
        await wh.worker.run_once()
        await complete(wh, value)
    next_second = dict(second, message_id=str(uuid4()), text="next-second")
    await admit(wh, next_second)
    await wh.worker.run_once()
    request = wh.ai.calls[-1].request
    assert [m.text for m in request.messages] == ["visitor-two", "retained reply", "next-second"]
    first_request = wh.ai.calls[0].request
    assert request.conversation_id != first_request.conversation_id
    assert not {m.id for m in request.messages} & {m.id for m in first_request.messages}
    for value in (dict(identity(first), session_id=second["session_id"]),
                  dict(identity(first), integration_id=wh.grants["other"]["integration_id"]),
                  dict(identity(first), owner_id="another-owner"),
                  dict(identity(first), agent_id=str(uuid4()))):
        assert (await wh.call("POST", BASE + "/results", value)).status_code == 404
    assert (await wh.call("POST", BASE + "/results", identity(first), "other")).status_code == 404


async def claimed_snapshot(wh, turn_id):
    claim = await turns.claim_turn(wh.database, wh.principal, turn_id,
                                  TurnClaim(worker_ref="proof", lease_seconds=30))
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(
        wh.database, wh.principal, turn_id, proof, wh.settings)
    return proof, snapshot, managed_execution.prepare_snapshot(snapshot)


@pytest.mark.parametrize("column,value", [
    ("managed_contract_version", "1"), ("managed_context_first_sequence", "2"),
    ("managed_projection_fingerprint", "'" + "a" * 64 + "'"),
    ("managed_projection_agent_id", "'" + str(uuid4()) + "'"),
    ("input_revision", "2"), ("input_first_sequence", "2"),
    ("context_last_sequence", "2"), ("conversation_id", "'" + str(uuid4()) + "'"),
])
async def test_v2_freeze_db_guard_rejects_altered_retained_identity(wh, sql, column, value):
    await admit(wh, payload(wh))
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    await violation(sql, f"UPDATE agent_turns SET {column}={value} WHERE id=:id",
                    dict(id=selected), "23514")


async def test_predispatch_snapshot_and_final_fence_detect_changed_canonical_rows(wh, sql):
    await admit(wh, payload(wh))
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    proof, snapshot, prepared = await claimed_snapshot(wh, selected)
    with pytest.raises(DomainError):
        managed_execution.prepare_snapshot(replace(snapshot, contract_version=1))
    # Alteration between snapshot and marker must fail at the second locked read.
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE messages SET text='changed'"))
    with pytest.raises(DomainError) as failure:
        await managed_execution.mark_dispatch(wh.database, wh.principal, selected, proof, prepared)
    assert failure.value.code == ErrorCode.VALIDATION_ERROR
    with pytest.raises(DomainError):
        await managed_execution.take_snapshot(
            wh.database, wh.principal, selected, proof, wh.settings)
    assert not wh.ai.calls
    async with sql.connect() as conn:
        assert await conn.scalar(select(AgentTurn.managed_dispatch_epoch)) is None


async def test_historical_website_projection_replays_exact_pre_stream_body_and_marks_same_hash(
    wh, sql, monkeypatch
):
    """A retained pre-stream v2 digest selects its old body through both fences."""
    import services.website_worker as website_worker_module

    original_freeze = website_worker_module.freeze_website_projection
    historical_hashes = []

    async def freeze_pre_stream(session, conversation, turn, agent_id):
        await original_freeze(session, conversation, turn, agent_id)
        messages = await canonical_window(
            session, conversation.id, turn.managed_context_first_sequence,
            turn.context_last_sequence,
        )
        current = projected_request(turn, conversation, agent_id, messages)
        fields = current.request.model_dump(mode="json")
        fields.pop("stream_partials")
        fields.pop("max_partial_chars")
        identity = json.dumps(
            {"execution_namespace": "main-chota.conversation.v1", "request": fields},
            sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
        turn.managed_projection_fingerprint = hashlib.sha256(identity).hexdigest()
        historical_hashes.append(turn.managed_projection_fingerprint)

    monkeypatch.setattr(website_worker_module, "freeze_website_projection", freeze_pre_stream)
    await admit(wh, payload(wh))
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    assert selected is not None and len(historical_hashes) == 1

    claim = await turns.claim_turn(
        wh.database, wh.principal, selected,
        TurnClaim(worker_ref="history-replay", lease_seconds=30),
    )
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    snapshot = await managed_execution.take_snapshot(
        wh.database, wh.principal, selected, proof, wh.settings
    )
    prepared = managed_execution.prepare_snapshot(snapshot)
    assert prepared.fingerprint == historical_hashes[0]
    assert "stream_partials" not in json.loads(prepared.body)
    assert "max_partial_chars" not in json.loads(prepared.body)
    assert prepared.request.stream_partials is False

    # execute_claimed_turn performs the second locked projection fence before
    # sending the request to the fake local AI port.
    await managed_execution.execute_claimed_turn(
        wh.database, wh.principal, selected, proof, wh.settings, wh.ai
    )
    assert len(wh.ai.calls) == 1
    sent = wh.ai.calls[0]
    assert sent.fingerprint == historical_hashes[0]
    assert sent.body == prepared.body
    async with sql.connect() as conn:
        projection, dispatch = (await conn.execute(text(
            "SELECT managed_projection_fingerprint,managed_request_fingerprint "
            "FROM agent_turns WHERE id=:id"), {"id": selected})).one()
    assert projection == dispatch == sent.fingerprint


async def test_binding_agent_cannot_override_immutable_website_admission(wh):
    await admit(wh, payload(wh))
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    claim = await turns.claim_turn(wh.database, wh.principal, selected,
                                  TurnClaim(worker_ref="proof", lease_seconds=30))
    proof = TurnExecute(claim_epoch=claim.turn.claim_epoch,
                        claim_token=SecretStr(claim.claim_token))
    binding = wh.settings.managed_ai_bindings[0].model_copy(update={"agent_id": uuid4()})
    changed = wh.settings.model_copy(update={"managed_ai_bindings": [binding]})
    with pytest.raises(DomainError):
        await managed_execution.take_snapshot(wh.database, wh.principal, selected, proof, changed)
    assert not wh.ai.calls


async def test_raw_source_turn_unique_same_conversation_and_actor_constraints(wh, sql):
    value = payload(wh)
    await admit(wh, value)
    await wh.worker.run_once()
    await complete(wh, value)
    source = wh.ai.calls[0].request.turn_id
    conversation = wh.ai.calls[0].request.conversation_id
    insertion = "INSERT INTO messages(id,conversation_id,sequence,actor,direction,text," \
                "source_agent_turn_id) VALUES(:id,:conversation,:sequence,:actor,'OUTBOUND'," \
                "'test',:source)"
    await violation(sql, insertion, dict(id=uuid4(), conversation=conversation, sequence=3,
                    actor="AGENT", source=source), "23505", "uq_message_source_agent_turn")
    other = payload(wh)
    await admit(wh, other)
    await wh.worker.run_once()
    other_source = wh.ai.calls[-1].request.turn_id
    await violation(sql, insertion, dict(id=uuid4(), conversation=conversation, sequence=3,
                    actor="AGENT", source=other_source), "23503")
    await violation(sql, insertion, dict(id=uuid4(), conversation=conversation, sequence=3,
                    actor="CUSTOMER", source=other_source), "23514",
                    "ck_message_agent_source_actor")


async def retained_v1_turn(wh, value):
    await admit(wh, value)
    async with wh.database.session_factory() as session, session.begin():
        admission = await session.scalar(select(WebsiteAdmission).where(
            WebsiteAdmission.message_id == UUID(value["message_id"])))
        conversation = await session.get(Conversation, admission.conversation_id)
        message = await session.get(Message, admission.canonical_message_id)
        turn = AgentTurn(id=uuid4(), conversation_id=conversation.id, selection_key=message.id,
            creation_fingerprint=fingerprint({"retained-v1-fixture": str(message.id)}),
            input_first_sequence=message.sequence, input_last_sequence=message.sequence,
            context_last_sequence=message.sequence, input_revision=conversation.input_revision,
            authority_revision=conversation.authority_revision,
            settling_revision=conversation.settling_revision, operation_refs=[])
        session.add(turn)
        await session.flush()
        session.add(WebsiteImmediateTurn(message_id=message.id,
            conversation_id=conversation.id, turn_id=turn.id))
        session.add(TurnInputMessage(message_id=message.id,
            conversation_id=conversation.id, turn_id=turn.id,
            sequence=message.sequence, ordinal=0))
        await session.flush()
        return turn.id


async def test_retained_v1_cannot_be_converted_to_v2_even_with_coherent_freeze_fields(wh, sql):
    turn_id = await retained_v1_turn(wh, payload(wh))
    await violation(sql, "UPDATE agent_turns SET managed_contract_version=2,"
        "managed_projection_agent_id=:agent,managed_projection_fingerprint=:digest WHERE id=:id",
        dict(id=turn_id, agent=uuid4(), digest="a" * 64), "23514")


@pytest.mark.parametrize("prior", [None, "WRITE_FAILED", "UNKNOWN"])
async def test_retained_v1_new_success_can_promote_without_rewriting_projection(wh, sql, prior):
    value = payload(wh)
    turn_id = await retained_v1_turn(wh, value)
    # Recreate the exact 0005 representation before running the historical
    # downgrade/upgrade compatibility check; these test rows were just created
    # by the Goal5 API and therefore carry fresh settling revisions.
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE conversations SET settling_revision=0"))
        await conn.execute(text("UPDATE messages SET settling_revision=0"))
        await conn.execute(text("UPDATE agent_turns SET settling_revision=0"))
    await wh.database.dispose()
    migrate(wh.settings.database_url.get_secret_value(), "downgrade", "0003_website_canary")
    migrate(wh.settings.database_url.get_secret_value(), "upgrade", "head")
    async with wh.database.session_factory() as session:
        old = await session.get(AgentTurn, turn_id)
        assert old.managed_contract_version == old.managed_context_first_sequence == 1
        assert old.managed_projection_agent_id is old.managed_projection_fingerprint is None
    proof, snapshot, prepared = await claimed_snapshot(wh, turn_id)
    assert prepared.request.contract_version == 1
    await managed_execution.execute_claimed_turn(wh.database, wh.principal, turn_id, proof,
                                                wh.settings, wh.ai)
    retained = await turns.get_turn(wh.database, wh.principal, turn_id)
    if prior:
        await complete(wh, value, prior)
    assert len(await history(sql)) == 1
    if prior == "WRITE_FAILED":
        await complete(wh, value, expected_status=409)
        assert len(await history(sql)) == 1
        assert await turns.get_turn(wh.database, wh.principal, turn_id) == retained
        assert retained.managed_request_fingerprint == prepared.fingerprint
        assert managed_execution.prepare_snapshot(snapshot).body == prepared.body
        assert len(wh.ai.calls) == 1
        return
    done = await complete(wh, value)
    assert len(await history(sql)) == 2
    assert await turns.get_turn(wh.database, wh.principal, turn_id) == retained
    assert retained.managed_request_fingerprint == prepared.fingerprint
    assert managed_execution.prepare_snapshot(snapshot).body == prepared.body
    assert (await wh.call("POST", BASE + "/responses/complete", done)).json()["replayed"]
    assert len(wh.ai.calls) == 1


async def test_retained_old_success_replay_does_not_backfill_history(wh, sql):
    value = payload(wh)
    turn_id = await retained_v1_turn(wh, value)
    proof, _, _ = await claimed_snapshot(wh, turn_id)
    await managed_execution.execute_claimed_turn(wh.database, wh.principal, turn_id, proof,
                                                wh.settings, wh.ai)
    attempt = dict(identity(value),
                   response_attempt_id=await v2_response_attempt_id(wh, value))
    RESPONSE_ATTEMPT_IDS[str(value["message_id"])] = attempt["response_attempt_id"]
    assert (await wh.call("POST", BASE + "/responses/begin", attempt)).status_code == 200
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE website_response_attempts SET outcome='WRITE_COMPLETED',"
            "finished_at=clock_timestamp() WHERE id=:id"), dict(id=attempt["response_attempt_id"]))
    response = await wh.call("POST", BASE + "/responses/complete",
                             dict(attempt, outcome="WRITE_COMPLETED"))
    assert response.json()["replayed"] and len(await history(sql)) == 1
    await complete(wh, value)
    assert len(await history(sql)) == 1


async def seed_window(wh, sql, count, content="hello", paired=True):
    value = payload(wh)
    await admit(wh, value)
    async with sql.begin() as conn:
        conversation = await conn.scalar(select(Conversation.id))
        await conn.execute(text("UPDATE messages SET text=:text"), dict(text=content))
        for seq in range(2, count + 1):
            actor = "AGENT" if paired and seq % 2 == 0 else "CUSTOMER"
            await conn.execute(text("INSERT INTO messages(id,conversation_id,sequence,actor,"
                "direction,text) VALUES(:id,:conversation,:sequence,:actor,:direction,:text)"),
                dict(id=uuid4(), conversation=conversation, sequence=seq, actor=actor,
                     direction="OUTBOUND" if actor == "AGENT" else "INBOUND", text=content))
        await conn.execute(text("UPDATE conversations SET last_message_sequence=:count,"
            "input_revision=:revision WHERE id=:id"),
            dict(count=count, revision=(count + 1) // 2 if paired else count, id=conversation))
    return conversation


async def consume_seeded_inputs(wh, conversation_id, last_sequence):
    async with wh.database.session_factory() as session:
        conversation = await session.get(Conversation, conversation_id)
        assert conversation is not None
        input_revision = conversation.input_revision
        authority_revision = conversation.authority_revision
    created = await turns.create_turn(
        wh.database, wh.principal, conversation_id,
        TurnCreate(
            selection_key=uuid4(), input_first_sequence=1,
            input_last_sequence=last_sequence,
            expected_input_revision=input_revision,
            expected_authority_revision=authority_revision,
        ),
    )
    claim = await turns.claim_turn(
        wh.database, wh.principal, created.turn.id,
        TurnClaim(worker_ref="seed-history", lease_seconds=30),
    )
    await turns.complete_turn(
        wh.database, wh.principal, created.turn.id,
        TurnComplete(
            claim_epoch=claim.turn.claim_epoch,
            claim_token=SecretStr(claim.claim_token), outcome="NO_RESPONSE",
        ),
    )


@pytest.mark.parametrize("count,content,paired,expected_first", [
    (255, "hello", True, 1), (256, "hello", True, 1), (257, "hello", True, 3),
    (500, "hello", False, 245), (501, "hello", True, 247),
    (16, "é" * 16384, True, 1), (17, "é" * 16384, True, 3),
    (12, "\x01" * 16384, True, 3),
], ids=["below-count", "at-count", "over-count", "long-singletons", "long-pairs",
        "at-multibyte-text", "over-multibyte-text", "over-escaped-json"])
async def test_projection_caps_drop_whole_oldest_units_keep_order_ids_and_selected(
    wh, sql, count, content, paired, expected_first
):
    conversation_id = await seed_window(wh, sql, count, content, paired)
    async with wh.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, conversation_id, with_for_update=True)
        turn = AgentTurn(id=uuid4(), conversation_id=conversation.id, selection_key=uuid4(),
            input_first_sequence=count, input_last_sequence=count, context_last_sequence=count,
            input_revision=conversation.input_revision, authority_revision=0)
        await freeze_website_projection(session, conversation, turn, UUID(wh.agent_id))
        assert turn.managed_context_first_sequence == expected_first
        messages = tuple(m for m in await history(sql) if m["sequence"] >= expected_first)
        from schemas.managed import ManagedMessage
        projected = tuple(ManagedMessage.model_validate({k: m[k] for k in (
            "id", "sequence", "actor", "direction", "content_type", "text")},
            from_attributes=True)
            for m in messages)
        prepared = projected_request(turn, conversation, UUID(wh.agent_id), projected)
        assert [m.sequence for m in prepared.request.messages] == list(
            range(expected_first, count + 1))
        assert [m.id for m in prepared.request.messages] == [m["id"] for m in messages]
        assert prepared.request.selected_input_message_ids == (projected[-1].id,)
        assert prepared.fingerprint == turn.managed_projection_fingerprint
        assert len(prepared.body) <= MAX_REQUEST_BYTES


@pytest.mark.parametrize("count,content", [(257, "hello"), (17, "é" * 16384),
                                           (11, "\x01" * 16384)],
                         ids=["selected-count", "selected-multibyte", "selected-json"])
async def test_selected_alone_overflow_fails_before_any_dispatch(wh, sql, count, content):
    conversation_id = await seed_window(wh, sql, count, content, paired=False)
    async with wh.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, conversation_id, with_for_update=True)
        turn = AgentTurn(id=uuid4(), conversation_id=conversation.id, selection_key=uuid4(),
            input_first_sequence=1, input_last_sequence=count, context_last_sequence=count,
            input_revision=count, authority_revision=0)
        with pytest.raises(DomainError):
            await freeze_website_projection(session, conversation, turn, UUID(wh.agent_id))
    assert not wh.ai.calls


@pytest.mark.parametrize("retained_version", [1, 2])
async def test_downgrade_refuses_retained_v2_or_history(wh, sql, database_url, retained_version):
    value = payload(wh)
    if retained_version == 1:
        turn_id = await retained_v1_turn(wh, value)
        proof, _, _ = await claimed_snapshot(wh, turn_id)
        await managed_execution.execute_claimed_turn(wh.database, wh.principal, turn_id, proof,
                                                    wh.settings, wh.ai)
        await complete(wh, value)
    else:
        await admit(wh, value)
        await wh.worker.run_once()
    await wh.database.dispose()
    result = migrate(database_url, "downgrade", "0003_website_canary", expected=1)
    assert "Conversation settling revisions prevent downgrade." in result.stderr
    assert await wh.database.ready()


async def test_long_window_survives_unmarked_claim_recovery_without_budget_reselection(wh, sql,
                                                                                     monkeypatch):
    conversation_id = await seed_window(wh, sql, 499)
    # Mark the synthetic seed prefix as completed input ownership. The next
    # admission then exercises projection over old conversation history only.
    await consume_seeded_inputs(wh, conversation_id, 499)
    async with sql.connect() as conn:
        session_id = await conn.scalar(select(WebsiteAdmission.session_id))
    value = payload(wh, session_id=str(session_id), text="selected500")
    await admit(wh, value)
    selected = await select_immediate(wh.database, wh.principal, wh.settings)
    _, snapshot, prepared = await claimed_snapshot(wh, selected)
    assert snapshot.turn.conversation_id == conversation_id
    assert snapshot.messages[0].sequence == 245
    assert [m.sequence for m in prepared.request.messages] == list(range(245, 501))

    async def forbidden_reselection(*args, **kwargs):
        raise AssertionError("Retained projection budget selection repeated")
    monkeypatch.setattr("services.website_worker.freeze_website_projection", forbidden_reselection)
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE agent_turns SET lease_expires_at=clock_timestamp() "
            "- interval '1 second' WHERE id=:id"), dict(id=selected))
    await wh.database.dispose()
    assert await WebsiteWorker(wh.database, wh.settings, wh.ai).run_once()
    assert wh.ai.calls[-1].request.turn_id == selected
    assert wh.ai.calls[-1].body == prepared.body
    assert wh.ai.calls[-1].fingerprint == prepared.fingerprint
