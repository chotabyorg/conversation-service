"""Explicit process/PG crash, FIFO, sticky routing and HTTP replay acceptance.

Run directly; ordinary pytest collection intentionally excludes this filename.
The fake effect counter records every AI execution rather than deduplicating it.
"""

import asyncio
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest
from sqlalchemy import text

from services.common import fingerprint
from tests.conftest import observed_waiters
from tests.website_cross_service.conftest import start_chain

EVIDENCE = []


async def settle_request(task):
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def wait_terminal(h, message_id):
    deadline = asyncio.get_running_loop().time() + 14
    last_state = None
    while asyncio.get_running_loop().time() < deadline:
        state = await h.state(message_id)
        last_state = state
        if state["status"] in ("COMPLETED", "FAILED"):
            return state
        await asyncio.sleep(0.05)
    pytest.fail(
        "Stable Turn did not terminalize after process recovery; "
        f"last persisted state={last_state!r}"
    )


async def wait_response_outcome(h, message_id, outcome):
    deadline = asyncio.get_running_loop().time() + 8
    attempts = []
    while asyncio.get_running_loop().time() < deadline:
        attempts = await h.rows(
            "SELECT r.id,r.send_authorized,r.outcome FROM website_response_attempts r "
            "JOIN website_admissions a ON a.id=r.admission_id "
            "WHERE a.message_id=:id ORDER BY r.created_at,r.id",
            id=message_id,
        )
        if any(row["send_authorized"] and row["outcome"] == outcome
               for row in attempts):
            return attempts
        await asyncio.sleep(0.02)
    pytest.fail(f"Authorized Website response did not persist {outcome}; "
                f"attempts={attempts!r}")


async def response_attempt_state(h, message_id):
    return await h.rows(
        "SELECT r.id,r.send_authorized,r.outcome FROM website_response_attempts r "
        "JOIN website_admissions a ON a.id=r.admission_id "
        "WHERE a.message_id=:id ORDER BY r.created_at,r.id", id=message_id,
    )


async def recover_public_result(h, body, *, initial=None, label, website="website"):
    """Retry transient Conversation timeouts only when the DB proves no send is active."""
    deadline = asyncio.get_running_loop().time() + 8
    response = initial
    while asyncio.get_running_loop().time() < deadline:
        if response is None:
            response = await h.chat(body, website=website)
        attempts = await response_attempt_state(h, body["message_id"])
        await h.diagnostic_snapshot(label, body["message_id"], extra={
            "public_status": response.status_code,
            "public_json": response.json(),
            "response_attempts": attempts,
        })
        if response.status_code == 200:
            await wait_response_outcome(h, body["message_id"], "WRITE_COMPLETED")
            return response
        error = response.json().get("error")
        if response.status_code != 503 or error not in {
                "Managed service unavailable; retry the same message_id",
                "Managed result pending; retry the same message_id",
        }:
            return response

        # The same deterministic v2 ID is safe to retry: begin() sees an
        # existing authorized row, fences an unresolved delivery as UNKNOWN,
        # and denies another write. Never change IDs to obtain another send.
        # Repeating this exact public identity cannot create a second physical
        # write. The v2 begin contract authorizes its one attempt once, replays
        # WRITE_COMPLETED, or returns the retained denial.
        await asyncio.sleep(0.05)
        response = None
    await h.diagnostic_snapshot(label + "-retry-window-expired", body["message_id"],
                                extra={"last_status": response.status_code if response else None})
    pytest.fail("Same-identity Website retry did not reach a durable public outcome")


async def wait_history(h, conversation_id, expected_agents):
    deadline = asyncio.get_running_loop().time() + 8
    last_rows = []
    while asyncio.get_running_loop().time() < deadline:
        rows = await h.rows("SELECT id,sequence,actor,direction,content_type,text,"
            "source_agent_turn_id "
            "FROM messages WHERE conversation_id=:id ORDER BY sequence", id=conversation_id)
        last_rows = rows
        if sum(row["actor"] == "AGENT" for row in rows) == expected_agents:
            return rows
        await asyncio.sleep(0.02)
    turns = await h.rows(
        "SELECT id,status,failure_code,draft_text,settling_revision "
        "FROM agent_turns WHERE conversation_id=:id ORDER BY created_at,id",
        id=conversation_id,
    )
    deliveries = await h.rows(
        "SELECT id,status,revision,error_code FROM delivery_attempts "
        "WHERE conversation_id=:id ORDER BY created_at,id",
        id=conversation_id,
    )
    attempts = await h.rows(
        "SELECT id,send_authorized,outcome FROM website_response_attempts "
        "WHERE conversation_id=:id ORDER BY created_at,id",
        id=conversation_id,
    )
    pytest.fail("Required durable assistant history was not committed; "
                f"messages={last_rows!r}, turns={turns!r}, "
                f"deliveries={deliveries!r}, response_attempts={attempts!r}")


def record(point, before, after, observations, attempts):
    safe = {"scenario": point, "admission_id": after["admission_id"],
            "conversation_id": after["conversation_id"],
            "message_id": after["canonical_message_id"], "turn_id": after["turn_id"],
            "before_status": before["status"], "after_status": after["status"],
            "failure_code": after["failure_code"], "claim_epoch": after["claim_epoch"],
            "ai_http_calls": len(observations["managed"]),
            "fake_effects": len(observations["effects"]),
            "legacy_calls": len(observations["legacy"]),
            "evidence_reads": len(observations["evidence"]),
            "delivery_status": after["delivery_status"], "transport_attempts": attempts}
    EVIDENCE.append(safe)


@pytest.mark.parametrize("point", list("ABCDEFGH"))
async def test_crash_commit_boundaries_keep_same_turn_and_no_repeat_effect(wh, point):
    h = wh
    if point == "A":
        # No coordinator exists until after inbound positive COMMIT + API kill.
        h.start("cs", fault="A")
        await h.wait_ready("cs")
        h.start("website")
        await h.wait_ready("website")
    else:
        await start_chain(h, fault=point)
    body = h.body()
    request = asyncio.create_task(h.chat(body))
    barrier = await h.barrier(point)
    await h.diagnostic_snapshot(f"crash-{point}-at-barrier", body["message_id"],
                                extra={"barrier": barrier})
    before = await h.state(body["message_id"])
    assert before["route"] == "MANAGED"
    assert before["canonical_message_id"] is not None
    if point == "A":
        assert before["turn_id"] is None
    elif point == "B":
        assert before["status"] == "PENDING" and before["claim_epoch"] == 0
    elif point == "C":
        assert before["status"] == "CLAIMED" and before["managed_dispatch_epoch"] is None
    elif point in ("D", "E"):
        assert before["status"] == "CLAIMED" and before["managed_dispatch_epoch"] is not None
    else:
        assert before["status"] == "COMPLETED" and before["draft_text"]
    if point == "F":
        assert before["delivery_id"] is None
    elif point in ("G", "H"):
        assert before["delivery_status"] == "DISPATCH_REQUESTED"
        intent = await h.rows("SELECT outcome FROM website_response_attempts")
        assert intent == [{"outcome": None}]
    if point == "H":
        response = await asyncio.wait_for(request, 2)
        assert response.status_code == 200  # Local HTTP bytes observed before reconcile.
    h.kill(barrier["process"])
    await settle_request(request)
    h.start(barrier["process"])
    await h.wait_ready(barrier["process"])
    if point == "A":
        h.start("worker")
        await h.wait_ready("worker")
    after = await wait_terminal(h, body["message_id"])
    await h.diagnostic_snapshot(f"crash-{point}-after-recovery", body["message_id"],
                                extra={"before": before, "after": after})
    if before["turn_id"] is not None:
        assert after["turn_id"] == before["turn_id"]
    assert after["canonical_message_id"] == before["canonical_message_id"]
    assert after["admission_id"] == before["admission_id"]
    uncertain_execution = point in ("D", "E")
    response = (await h.chat(body) if uncertain_execution else
                await recover_public_result(h, body, label=f"crash-{point}-public-retry"))
    if uncertain_execution:
        assert response.status_code == 503
    else:
        assert response.status_code in {200, 503}
    retry_status = response.status_code
    if uncertain_execution:
        assert after["failure_code"] == "EXECUTION_UNKNOWN" and after["draft_text"] is None
        assert after["delivery_id"] is None
        if point == "E":
            operation = h.observations()["effects"][0]["operation_id"]
            assert after["operation_refs"] == [{"origin": "AI_AGENT_SERVICE",
                                                "operation_id": operation,
                                                "outcome": "SUCCEEDED"}]
        else:
            assert after["operation_refs"] == []
    elif retry_status == 200:
        assert response.json()["reply"] == "Managed fixture reply"
        assert response.headers["Access-Control-Allow-Origin"] == "https://fixture.example"
        assert response.headers["Cache-Control"] == "no-store"
    else:
        assert response.json() == {
            "error": "response_unavailable", "session_id": body["session_id"]
        }
        assert "reply" not in response.json()
    if retry_status == 200:
        await wait_response_outcome(h, body["message_id"], "WRITE_COMPLETED")
    replay = await h.chat(body)
    assert replay.status_code == retry_status
    if retry_status != 200:
        assert "reply" not in replay.json()
    final = await h.state(body["message_id"])
    assert final["turn_id"] == after["turn_id"]
    assert final["canonical_message_id"] == after["canonical_message_id"]
    observations = h.observations()
    expected_calls = 0 if point == "D" else 1
    assert len(observations["managed"]) == expected_calls
    assert len(observations["effects"]) == expected_calls
    assert observations["legacy"] == []
    sends_authorized = retry_status == 200
    history = await wait_history(h, final["conversation_id"], 1 if sends_authorized else 0)
    if sends_authorized:
        assert history[-1]["source_agent_turn_id"] == final["turn_id"]
        assert history[-1]["text"] == final["draft_text"]
    counts = await h.rows("SELECT (SELECT count(*) FROM messages) AS messages,"
                         "(SELECT count(*) FROM inbound_events) AS events,"
                         "(SELECT count(*) FROM agent_turns) AS turns")
    assert counts == [{"messages": 2 if sends_authorized else 1,
                       "events": 1, "turns": 1}]
    attempts = await h.rows("SELECT id,outcome FROM website_response_attempts ORDER BY created_at")
    if sends_authorized:
        assert final["delivery_status"] == "UNKNOWN"
        assert any(row["outcome"] == "WRITE_COMPLETED" for row in attempts)
    else:
        assert not any(row["outcome"] == "WRITE_COMPLETED" for row in attempts)
        assert not await h.rows(
            "SELECT id FROM messages WHERE actor='AGENT' AND source_agent_turn_id=:id",
            id=final["turn_id"],
        )
    if point in ("G", "H") and attempts:
        assert attempts[0]["outcome"] in {None, "UNKNOWN"}
    record(point, before, final, observations, attempts)


@pytest.mark.parametrize("customer,replies", [
    (["Привет", "Какие услуги", "А маникюр"],
     ["Здравствуйте", "Маникюр и педикюр", "Есть маникюр"]),
    (["Хочу записаться", "Маникюр", "Завтра"],
     ["На какую услугу?", "На какой день?", "Есть 18:00, записать?"]),
    (["Хочу завтра в 18", "+"], ["Записать?", "Технический ответ без Business операции"]),
])
async def test_deterministic_dialogs_actual_processes_use_prior_canonical_agents(
    wh, customer, replies
):
    h = wh
    h.replies = dict(zip(customer, replies, strict=True))
    await h.restart("fake")
    await start_chain(h)
    previous = []
    bodies = []
    for index, message in enumerate(customer):
        body = h.body(message=message)
        bodies.append(body)
        response = await h.eventual_chat(body)
        assert response.json()["reply"] == replies[index]
        state = await h.state(body["message_id"])
        history = await wait_history(h, state["conversation_id"], index + 1)
        request = h.observations()["managed"][-1]
        assert request["contract_version"] == 2
        assert request["input_revision"] == index + 1
        assert request["context_last_sequence"] == index * 2 + 1
        assert request["selected_input_message_ids"] == [str(state["canonical_message_id"])]
        assert [m["actor"] for m in request["messages"]] == ["CUSTOMER", "AGENT"] * index + [
            "CUSTOMER"]
        assert [m["id"] for m in request["messages"][:-1]] == [str(m["id"]) for m in previous]
        assert [m["text"] for m in request["messages"]] == [
            item for pair in zip(customer[:index], replies[:index], strict=True) for item in pair
        ] + [message]
        previous = history
    await h.restart("cs")
    await h.restart("worker")
    await h.restart("website")
    before_attempts = await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    )
    before_reconciliations = await h.rows(
        "SELECT id,delivery_attempt_id,reconciliation_key,status,result_revision "
        "FROM delivery_reconciliations ORDER BY accepted_at,id"
    )
    before_history = await h.rows(
        "SELECT id,sequence,actor,source_agent_turn_id,text FROM messages ORDER BY sequence"
    )
    before_observations = h.observations()
    for index, (body, reply) in enumerate(zip(bodies, replies, strict=True)):
        response = await h.chat(body)
        assert response.status_code == 200
        assert response.json() == {"reply": reply, "session_id": body["session_id"]}
        assert response.headers["Access-Control-Allow-Origin"] == "https://fixture.example"
        assert response.headers["Cache-Control"] == "no-store"
    history = await wait_history(h, state["conversation_id"], len(customer))
    assert len(history) == 2 * len(customer)
    assert len({m["source_agent_turn_id"] for m in history
                if m["actor"] == "AGENT"}) == len(customer)
    assert await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    ) == before_attempts
    assert await h.rows(
        "SELECT id,delivery_attempt_id,reconciliation_key,status,result_revision "
        "FROM delivery_reconciliations ORDER BY accepted_at,id"
    ) == before_reconciliations
    assert await h.rows(
        "SELECT id,sequence,actor,source_agent_turn_id,text FROM messages ORDER BY sequence"
    ) == before_history
    assert h.observations() == before_observations
    assert len(before_observations["managed"]) == len(customer)
    assert before_observations["legacy"] == []
    # Every external endpoint is denied by all process guards; this fake has no
    # Business/provider mutation route. Its effects are execution-count sentinels.
    EVIDENCE.append({"scenario": "Goal4A.1 deterministic dialog", "customer_turns": len(customer),
                     "canonical_messages": len(history), "prior_agent_rows_in_final_request":
                     len(customer) - 1, "ai_http_calls": len(h.observations()["managed"]),
                     "business_provider_calls": 0})


@pytest.mark.parametrize("outcome", ["WRITE_FAILED", "UNKNOWN"])
async def test_failed_or_unknown_old_draft_is_not_retried_after_newer_input(wh, outcome):
    h = wh
    h.settling_enabled = True
    await start_chain(h, fault="F")
    first = h.body(message="first-draft")
    pending = asyncio.create_task(h.chat(first))
    await h.barrier("F")  # Actual draft retained, before Website physical write.
    state = await h.state(first["message_id"])
    h.kill("website")
    await settle_request(pending)
    assert len(await wait_history(h, state["conversation_id"], 0)) == 1
    response_attempt_id = str(uuid5(
        NAMESPACE_URL,
        "main-chota.website-widget-response.v2:" + str(state["admission_id"]),
    ))
    attempt = {"owner_id": h.ids["owner"], "integration_id": h.ids["integration"],
               "agent_id": h.ids["agent"], "session_id": first["session_id"],
               "message_id": first["message_id"],
               "response_attempt_id": response_attempt_id}
    # The canonical admission-derived v2 identity is authorized once. Record a
    # failed or uncertain socket fact, then prove a later turn cannot resend it.
    headers = {"X-Service-Token": h.tokens["website"]}
    begun = await h.client.post(
        h.url("cs") + "/internal/v1/website/responses/begin",
        headers=headers, json=attempt,
    )
    assert begun.status_code == 200
    assert begun.json()["action"] == "SEND"
    assert begun.json()["send"] is True and begun.json()["replayed"] is False
    assert begun.json()["response_attempt_id"] == response_attempt_id
    completed = await h.client.post(
        h.url("cs") + "/internal/v1/website/responses/complete",
        headers=headers, json=dict(attempt, outcome=outcome),
    )
    assert completed.status_code == 200
    assert completed.json()["outcome"] == outcome
    assert len(await wait_history(h, state["conversation_id"], 0)) == 1

    # Wait on the persisted PostgreSQL burst deadline before admitting the next
    # customer Message. This exercises a genuinely newer canonical Turn.
    burst = await h.rows(
        "SELECT b.ready_at FROM settling_bursts b JOIN messages m "
        "ON m.settling_burst_id=b.id WHERE m.id=:id",
        id=state["canonical_message_id"],
    )
    assert len(burst) == 1
    deadline = burst[0]["ready_at"]
    db_deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < db_deadline:
        current = await h.rows("SELECT clock_timestamp() AS now")
        if current[0]["now"] > deadline:
            break
        await asyncio.sleep(0.025)
    else:
        pytest.fail("Stored Website burst deadline did not pass in PostgreSQL")

    h.start("website")
    await h.wait_ready("website")
    second = h.body(message="second-draft")
    assert (await h.eventual_chat(second)).status_code == 200
    second_state = await h.state(second["message_id"])
    before_retry = await wait_history(h, state["conversation_id"], 1)
    second_request = h.observations()["managed"][-1]
    assert [m["actor"] for m in second_request["messages"]] == ["CUSTOMER", "CUSTOMER"]
    before_stale_retry = await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    )
    before_stale_observations = h.observations()
    stale_retry = await h.chat(first)
    assert stale_retry.status_code in {409, 503}
    if stale_retry.status_code == 409:
        assert stale_retry.json() == {
            "error": "response_superseded", "session_id": first["session_id"]
        }
    else:
        assert stale_retry.json() == {
            "error": "response_unavailable", "session_id": first["session_id"]
        }
    assert "reply" not in stale_retry.json()
    assert await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    ) == before_stale_retry
    assert h.observations() == before_stale_observations
    after_retry = await wait_history(h, state["conversation_id"], 1)
    assert after_retry[-1]["source_agent_turn_id"] == second_state["turn_id"]
    assert after_retry[:3] == before_retry
    third = h.body(message="third-draft")
    assert (await h.eventual_chat(third)).status_code == 200
    await wait_history(h, state["conversation_id"], 2)
    request = h.observations()["managed"][-1]
    assert [m["actor"] for m in request["messages"]] == [
        "CUSTOMER", "CUSTOMER", "AGENT", "CUSTOMER"]
    assert [m["id"] for m in request["messages"][:-1]] == [str(m["id"]) for m in after_retry]
    assert len(h.observations()["managed"]) == 3
    assert h.observations()["legacy"] == []
    EVIDENCE.append({"scenario": "Goal5 superseded transport retry",
                     "first_outcome": outcome,
                     "agent_commit_order": "second Turn, third Turn",
                     "stale_first_draft_retried": False,
                     "ai_http_calls": 3, "duplicate_assistant_rows": 0})


async def test_real_http_identity_replay_conflict_same_text_new_id_and_isolation(wh):
    h = wh
    await start_chain(h)
    body = h.body()
    first = await h.eventual_chat(body)
    first_state = await h.state(body["message_id"])
    replay = await h.chat(body)
    assert replay.status_code == 200 and replay.json() == first.json()
    assert await h.state(body["message_id"]) == first_state
    assert (await h.chat(dict(body, message="changed"))).status_code == 409
    assert (await h.chat(dict(body, session_id=h.ids["session2"]))).status_code == 409
    missing_id = {"session_id": h.ids["session"], "message": "missing ID"}
    assert (await h.chat(missing_id)).status_code == 400
    assert (await h.chat(dict(body, message_id="invalid"))).status_code == 400
    second = dict(body, message_id=str(uuid4()))
    assert (await h.eventual_chat(second)).status_code == 200
    state = await h.state(second["message_id"])
    assert state["canonical_message_id"] != first_state["canonical_message_id"]
    assert state["turn_id"] != first_state["turn_id"]
    assert state["conversation_id"] == first_state["conversation_id"]
    ordinary = h.body(session_id=str(uuid4()))
    assert (await h.chat(ordinary)).json()["reply"] == "Legacy fixture reply"
    assert (await h.state(ordinary["message_id"]))["route"] == "LEGACY"
    other = h.body()
    assert (await h.chat(other, other=True)).json()["reply"] == "Legacy fixture reply"
    assert await h.rows("SELECT id FROM website_admissions WHERE message_id=:id",
                        id=other["message_id"]) == []
    calls = h.observations()
    assert len(calls["managed"]) == len(calls["effects"]) == 2
    assert len(calls["legacy"]) == 2


async def test_public_message_id_is_scoped_to_each_managed_admission(wh):
    h = wh
    h.multiple_scopes = True
    # The fixture starts `fake` before this test enables the third trusted
    # scope, so reload its bootstrap before exercising that widget key.
    await h.restart("fake")
    await start_chain(h)
    body = h.body(message="same public identity across trusted scopes")
    responses = [
        await h.eventual_chat(body),
        await h.eventual_chat(body, widget="other_widget"),
        await h.eventual_chat(body, widget="third_widget"),
    ]
    assert all(response.status_code == 200 for response in responses)
    assert all(response.json() == {"reply": "Managed fixture reply",
                                   "session_id": body["session_id"]}
               for response in responses)
    rows = await h.rows(
        "SELECT id AS admission_id,owner_id,integration_id,conversation_id,"
        "canonical_message_id FROM website_admissions WHERE message_id=:id "
        "ORDER BY owner_id,integration_id",
        id=body["message_id"],
    )
    assert len(rows) == 3
    assert len({row["admission_id"] for row in rows}) == 3
    assert len({row["conversation_id"] for row in rows}) == 3
    assert len({row["canonical_message_id"] for row in rows}) == 3
    assert {(row["owner_id"], str(row["integration_id"])) for row in rows} == {
        (h.ids["owner"], h.ids["integration"]),
        (h.ids["owner"], h.ids["other_integration"]),
        (h.ids["owner2"], h.ids["integration2"]),
    }
    attempts = await h.rows(
        "SELECT id,admission_id,send_authorized,outcome FROM website_response_attempts "
        "ORDER BY created_at"
    )
    assert len(attempts) == 3
    expected_ids = {
        str(uuid5(NAMESPACE_URL,
                  "main-chota.website-widget-response.v2:" + str(row["admission_id"])))
        for row in rows
    }
    assert {str(row["id"]) for row in attempts} == expected_ids
    assert all(row["send_authorized"] and row["outcome"] == "WRITE_COMPLETED"
               for row in attempts)
    calls = h.observations()
    assert len(calls["managed"]) == len(calls["effects"]) == 3
    assert calls["legacy"] == []


async def test_completed_response_replay_after_gateway_restart_is_read_only(wh):
    h = wh
    await start_chain(h)
    body = h.body(message="restart cache replay")
    first = await h.eventual_chat(body)
    assert first.status_code == 200
    assert first.headers["Cache-Control"] == "no-store"
    before = await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    )
    before_reconciliations = await h.rows(
        "SELECT id,delivery_attempt_id,reconciliation_key,status,result_revision "
        "FROM delivery_reconciliations ORDER BY accepted_at,id"
    )
    before_history = await h.rows(
        "SELECT id,sequence,actor,source_agent_turn_id,text FROM messages "
        "ORDER BY sequence"
    )
    assert len(before) == 1 and before[0]["outcome"] == "WRITE_COMPLETED"
    await h.restart("website")
    replay = await h.chat(body)
    assert replay.status_code == 200 and replay.json() == first.json()
    assert replay.headers["Access-Control-Allow-Origin"] == "https://fixture.example"
    assert replay.headers["Cache-Control"] == "no-store"
    assert await h.rows(
        "SELECT id,admission_id,delivery_id,send_authorized,outcome "
        "FROM website_response_attempts ORDER BY created_at,id"
    ) == before
    assert await h.rows(
        "SELECT id,delivery_attempt_id,reconciliation_key,status,result_revision "
        "FROM delivery_reconciliations ORDER BY accepted_at,id"
    ) == before_reconciliations
    assert await h.rows(
        "SELECT id,sequence,actor,source_agent_turn_id,text FROM messages "
        "ORDER BY sequence"
    ) == before_history
    calls = h.observations()
    assert len(calls["managed"]) == len(calls["effects"]) == 1
    assert calls["legacy"] == []


@pytest.mark.asyncio
async def test_goal5_website_process_burst_supersede_and_restart(wh):
    h = wh
    h.settling_enabled = True
    # Keep both concurrent admissions within one frozen burst regardless of
    # host load. A DB ready_at barrier below releases that burst deterministically.
    h.settling_quiet_window_ms = 5_000
    h.settling_max_wait_ms = 15_000
    h.replies.update({
        "old user request": "STALE DRAFT MUST NOT REACH THE CLIENT",
        "new user request": "Fresh response",
    })
    configured = await h.client.post(h.url("fake") + "/test/replies", json=h.replies)
    assert configured.status_code == 200
    await start_chain(h)
    # Hold the real worker before either message is admitted, then use durable
    # admission rows as the barrier before allowing a settling selection.
    h.kill("worker")

    burst_one = h.body(session_id=h.ids["session"], message="burst one")
    burst_two = h.body(session_id=h.ids["session"], message="burst two")
    requests = [asyncio.create_task(h.chat(burst_one))]
    admission_deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < admission_deadline:
        admitted = await h.rows(
            "SELECT message_id FROM website_admissions WHERE message_id=:one",
            one=burst_one["message_id"],
        )
        if len(admitted) == 1:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("First burst message was not durably admitted before selection")
    # The durable first-row barrier keeps the actual Gateway request path while
    # making the quiet-window grouping independent of simultaneous socket load.
    requests.append(asyncio.create_task(h.chat(burst_two)))
    admission_deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < admission_deadline:
        admitted = await h.rows(
            "SELECT message_id FROM website_admissions WHERE message_id IN (:one,:two)",
            one=burst_one["message_id"], two=burst_two["message_id"],
        )
        if len(admitted) == 2:
            break
        await asyncio.sleep(0.02)
    else:
        task_results = []
        for task in requests:
            if task.done() and not task.cancelled():
                try:
                    response = task.result()
                    task_results.append({"status": response.status_code,
                                        "body": response.text[:160]})
                except Exception as exc:
                    task_results.append({"error": type(exc).__name__})
            else:
                task_results.append({"pending": True})
        rows = await h.rows(
            "SELECT message_id,route FROM website_admissions "
            "WHERE message_id IN (:one,:two)",
            one=burst_one["message_id"], two=burst_two["message_id"],
        )
        fake_counts = {key: len(value) for key, value in h.observations().items()}
        for task in requests:
            await settle_request(task)
        pytest.fail(f"Both burst messages were not durably admitted before selection; "
                    f"matching_rows={len(rows)}, http={task_results}, "
                    f"fake_counts={fake_counts}")
    burst_membership = await h.rows(
        "SELECT m.id,m.settling_burst_id,b.ready_at,b.max_cutoff_at "
        "FROM website_admissions a JOIN messages m ON m.id=a.canonical_message_id "
        "LEFT JOIN settling_bursts b ON b.id=m.settling_burst_id "
        "WHERE a.message_id IN (:one,:two) ORDER BY m.sequence",
        one=burst_one["message_id"], two=burst_two["message_id"],
    )
    assert len(burst_membership) == 2
    assert burst_membership[0]["settling_burst_id"] is not None
    assert burst_membership[0]["settling_burst_id"] == burst_membership[1]["settling_burst_id"]
    # Release on the persisted DB deadline. This is an explicit state barrier;
    # the production immutability trigger keeps the original cutoff intact.
    ready_at_deadline = asyncio.get_running_loop().time() + 12
    while asyncio.get_running_loop().time() < ready_at_deadline:
        readiness = await h.rows(
            "SELECT ready_at <= clock_timestamp() AS ready "
            "FROM settling_bursts WHERE id=:id",
            id=burst_membership[0]["settling_burst_id"],
        )
        assert len(readiness) == 1
        if readiness[0]["ready"]:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("Stored settling burst deadline did not become ready")
    h.start("worker")
    await h.wait_ready("worker")
    initial_responses = await asyncio.gather(*requests)
    assert all(response.status_code in {200, 503} for response in initial_responses)
    for response in initial_responses:
        if response.status_code == 503:
            assert "reply" not in response.json()
    # The first HTTP wait may end while the result is still PENDING. Wait on the
    # durable Turn state, then retry both exact public identities for their
    # committed response decisions.
    pending_state = await wait_terminal(h, burst_one["message_id"])
    assert pending_state["status"] == "COMPLETED"
    responses = await asyncio.gather(h.chat(burst_one), h.chat(burst_two))
    assert sorted(response.status_code for response in responses) == [200, 503]
    assert sum(response.status_code == 200 for response in responses) == 1
    accepted = next(response for response in responses if response.status_code == 200)
    assert accepted.json() == {
        "reply": "Managed fixture reply", "session_id": burst_one["session_id"]
    }
    assert accepted.headers["Cache-Control"] == "no-store"
    denied = next(response for response in responses if response.status_code == 503)
    assert denied.json()["error"] == "response_unavailable"
    assert "reply" not in denied.json()
    attempt_deadline = asyncio.get_running_loop().time() + 8
    burst_attempts = []
    while asyncio.get_running_loop().time() < attempt_deadline:
        burst_attempts = await h.rows(
            "SELECT a.message_id,a.id AS admission_id,r.id AS response_attempt_id,"
            "r.send_authorized,r.outcome "
        "FROM website_response_attempts r JOIN website_admissions a "
        "ON a.id=r.admission_id WHERE a.message_id IN (:one,:two) "
        "ORDER BY a.message_id",
        one=burst_one["message_id"], two=burst_two["message_id"],
        )
        if (len(burst_attempts) == 2 and any(
                row["send_authorized"] and row["outcome"] == "WRITE_COMPLETED"
                for row in burst_attempts)):
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("Public 200 response did not reach retained WRITE_COMPLETED; "
                    f"attempts={burst_attempts!r}")
    assert len(burst_attempts) == 2
    assert sum(row["send_authorized"] for row in burst_attempts) == 1
    assert all(row["outcome"] is None for row in burst_attempts
               if not row["send_authorized"])
    assert {str(row["response_attempt_id"]) for row in burst_attempts} == {
        str(uuid5(NAMESPACE_URL, "main-chota.website-widget-response.v2:"
                  + str(row["admission_id"])))
        for row in burst_attempts
    }
    assert len(h.observations()["managed"]) == 1
    burst_states = [await h.state(value["message_id"]) for value in (burst_one, burst_two)]
    assert burst_states[0]["turn_id"] == burst_states[1]["turn_id"]
    assert len(await wait_history(h, burst_states[0]["conversation_id"], 1)) == 3

    # Return to the short ordinary policy for the following live supersede
    # scenario; the completed burst retained its original 5-second snapshot.
    h.settling_quiet_window_ms = 300
    h.settling_max_wait_ms = 1200
    await h.restart("cs")
    await h.restart("worker")

    # Hold the first AI response after the Turn is claimed. A newer canonical
    # inbound Message commits first, so the older draft must receive send=false.
    assert (await h.client.post(h.url("fake") + "/test/managed/block")).status_code == 200
    old = h.body(session_id=h.ids["session2"], message="old user request")
    old_request = asyncio.create_task(h.chat(old))
    deadline = asyncio.get_running_loop().time() + 8
    while asyncio.get_running_loop().time() < deadline:
        entered = await h.client.get(h.url("fake") + "/test/managed/entered")
        if entered.json()["entered"]:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("Website process did not reach the held Managed AI execution")
    new = h.body(session_id=h.ids["session2"], message="new user request")
    new_request = asyncio.create_task(h.chat(new))
    admission_deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < admission_deadline:
        count = await h.rows(
            "SELECT id FROM website_admissions WHERE message_id IN (:old,:new)",
            old=old["message_id"], new=new["message_id"],
        )
        if len(count) == 2:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("New Website Message was not durably admitted during the first Turn")
    assert (await h.client.post(h.url("fake") + "/test/managed/release")).status_code == 200
    old_response, new_response = await asyncio.gather(old_request, new_request)
    assert old_response.status_code == 409
    assert old_response.json() == {
        "error": "response_superseded", "session_id": h.ids["session2"]
    }
    assert "STALE DRAFT MUST NOT REACH THE CLIENT" not in old_response.text
    assert new_response.status_code == 200
    assert new_response.json()["reply"] == "Fresh response"
    old_state = await h.state(old["message_id"])
    new_state = await h.state(new["message_id"])
    assert old_state["turn_id"] != new_state["turn_id"]
    history = await wait_history(h, old_state["conversation_id"], 1)
    assert [row["source_agent_turn_id"] for row in history if row["actor"] == "AGENT"] == [
        new_state["turn_id"],
    ]
    assert old_state["delivery_status"] == "SUPERSEDED"
    assert len(h.observations()["managed"]) == 3

    # A Gateway process restart after admission cannot orphan retained work;
    # replaying the exact public identity retrieves the same durable Turn.
    restart = h.body(session_id=h.ids["session"], message="restart retained result")
    await h.client.post(h.url("fake") + "/test/managed/block")
    pending = asyncio.create_task(h.chat(restart))
    execute_deadline = asyncio.get_running_loop().time() + 8
    while asyncio.get_running_loop().time() < execute_deadline:
        if len(h.observations()["managed"]) >= 4:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail("Goal5 Website retained execution did not reach the fake AI")
    restart_state = await h.state(restart["message_id"])
    assert restart_state["canonical_message_id"] is not None
    h.kill("website")
    await settle_request(pending)
    await h.client.post(h.url("fake") + "/test/managed/release")
    h.start("website")
    await h.wait_ready("website")
    replay = await h.eventual_chat(restart)
    assert replay.status_code == 200
    replay_state = await h.state(restart["message_id"])
    assert replay_state["canonical_message_id"] == restart_state["canonical_message_id"]
    assert replay_state["turn_id"] == restart_state["turn_id"]
    assert len(h.observations()["managed"]) == 4


async def test_overlapping_identical_public_requests_observe_pg_identity_lock(wh):
    h = wh
    await start_chain(h)
    body = h.body()
    digest = fingerprint({"owner": h.ids["owner"], "integration": h.ids["integration"],
                          "namespace": "main-chota.website-widget.v1",
                          "message": body["message_id"]})
    tasks = []
    try:
        async with h.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                                  {"key": int(digest[:16], 16) - (1 << 63)})
            tasks = [asyncio.create_task(h.chat(body)) for _ in range(2)]
            await observed_waiters(h.sql, pid, 2, "Goal4A real HTTP same ID admission")
        responses = await asyncio.gather(*tasks)
        assert all(response.status_code in {200, 503} for response in responses)
        successful = [response for response in responses if response.status_code == 200]
        assert successful and all(response.json() == successful[0].json()
                                  for response in successful)
        assert all("reply" not in response.json() for response in responses
                   if response.status_code == 503)
        calls = h.observations()
        assert len(calls["managed"]) == len(calls["effects"]) == 1
        assert calls["legacy"] == []
        assert len(await h.rows("SELECT id FROM website_response_attempts")) == 1
    finally:
        for task in tasks:
            await settle_request(task)


async def test_overlapping_two_messages_fifo_and_two_workers_use_stable_selection(wh):
    h = wh
    h.start("cs")
    await h.wait_ready("cs")
    h.start("website")
    await h.wait_ready("website")
    first = h.body(message="first")
    assert (await h.chat(first)).status_code == 503  # Durable ingress; no coordinator yet.
    initial = await h.state(first["message_id"])
    bodies = [h.body(message="second"), h.body(message="third")]
    tasks = []
    try:
        async with h.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                                  {"id": initial["conversation_id"]})
            tasks = [asyncio.create_task(h.chat(body)) for body in bodies]
            await observed_waiters(h.sql, pid, 2, "Goal4A real HTTP two Message row lock")
        await asyncio.gather(*tasks)
        async with h.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                                  {"id": initial["conversation_id"]})
            h.start("worker")
            h.start("worker2")
            await observed_waiters(h.sql, pid, 2, "Goal4A two real workers selecting FIFO")
        await h.wait_ready("worker")
        await h.wait_ready("worker2")
        for body in [first, *bodies]:
            assert (await h.eventual_chat(body)).status_code == 200
        calls = h.observations()
        assert len(calls["managed"]) == len(calls["effects"]) == 3
        assert [row["input_first_sequence"] for row in calls["managed"]] == [1, 2, 3]
        assert [row["context_last_sequence"] for row in calls["managed"]] == [1, 2, 3]
        assert all(row["input_first_sequence"] == row["input_last_sequence"]
                   for row in calls["managed"])
        assert len({row["turn_id"] for row in calls["managed"]}) == 3
        assert calls["legacy"] == []
    finally:
        for task in tasks:
            await settle_request(task)


async def test_second_session_progresses_while_first_session_pg_row_is_locked(wh):
    h = wh
    await start_chain(h)
    first = h.body()
    await h.eventual_chat(first)
    initial = await h.state(first["message_id"])
    waiting = h.body()
    second = h.body(session_id=h.ids["session2"])
    request = None
    try:
        async with h.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                                  {"id": initial["conversation_id"]})
            request = asyncio.create_task(h.chat(waiting))
            await observed_waiters(h.sql, pid, 1, "Goal4A one session held, another independent")
            # A queued first session does not hold a global lock or block ingress
            # for another session. Its Turn will run after the held lock releases.
            response = await h.chat(second)
            assert response.status_code in (200, 503)
            if response.status_code == 503:
                assert "reply" not in response.json()
            state = await h.state(second["message_id"])
            assert state["canonical_message_id"] is not None
            assert state["conversation_id"] != initial["conversation_id"]
        first_while_locked = await asyncio.wait_for(request, timeout=15)
        assert first_while_locked.status_code in (200, 503)
        if first_while_locked.status_code == 503:
            assert "reply" not in first_while_locked.json()
        assert (await h.eventual_chat(waiting)).status_code == 200
        assert (await h.eventual_chat(second)).status_code == 200
        calls = h.observations()
        assert len(calls["managed"]) == len(calls["effects"]) == 3
    finally:
        if request is not None:
            await settle_request(request)


async def test_rollback_retains_old_managed_and_new_legacy_reservations(wh):
    h = wh
    await start_chain(h)
    admitted = h.body()
    before_reply = await h.eventual_chat(admitted)
    before = await h.state(admitted["message_id"])
    await h.restart("website", cohort=False)
    replay = await h.chat(admitted)
    assert replay.status_code == 200 and replay.json() == before_reply.json()
    assert (await h.state(admitted["message_id"]))["turn_id"] == before["turn_id"]
    new = h.body()
    assert (await h.chat(new)).json()["reply"] == "Legacy fixture reply"
    assert (await h.state(new["message_id"]))["route"] == "LEGACY"
    await h.restart("website", cohort=True)
    assert (await h.chat(new)).json()["reply"] == "Legacy fixture reply"
    assert (await h.state(new["message_id"]))["route"] == "LEGACY"
    calls = h.observations()
    assert len(calls["managed"]) == len(calls["effects"]) == 1
    assert len(calls["legacy"]) == 2  # Legacy behavior has no AI idempotency promise.


async def test_old_and_new_gateway_policy_race_observes_one_sticky_disposition(wh):
    h = wh
    await start_chain(h)
    h.start("website2", cohort=False)
    await h.wait_ready("website2")
    body = h.body()
    digest = fingerprint({"owner": h.ids["owner"], "integration": h.ids["integration"],
                          "namespace": "main-chota.website-widget.v1",
                          "message": body["message_id"]})
    tasks = []
    try:
        async with h.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                                  {"key": int(digest[:16], 16) - (1 << 63)})
            tasks = [asyncio.create_task(h.chat(body, service))
                     for service in ("website", "website2")]
            await observed_waiters(h.sql, pid, 2, "Goal4A old/new real gateway routing race")
        responses = await asyncio.gather(*tasks)
        await h.diagnostic_snapshot("old-new-policy-after-first-responses", body["message_id"],
                                    extra={"responses": [
                                        {"status": response.status_code,
                                         "json": response.json()}
                                        for response in responses]})
        for index, response in enumerate(responses):
            if response.status_code == 503:
                responses[index] = await recover_public_result(
                    h, body, initial=response, label=f"old-new-policy-retry-{index}",
                    website="website" if index == 0 else "website2")
        state = await h.state(body["message_id"])
        if state["route"] == "MANAGED":
            assert all(response.status_code in {200, 503} for response in responses)
            assert all("reply" not in response.json() for response in responses
                       if response.status_code == 503)
            calls = h.observations()
            assert all(response.status_code == 200 for response in responses)
            assert responses[0].json() == responses[1].json()
            assert len(calls["managed"]) == len(calls["effects"]) == 1
            assert calls["legacy"] == []
        else:
            assert all(response.status_code in {200, 503} for response in responses)
            assert all("reply" not in response.json() for response in responses
                       if response.status_code == 503)
            calls = h.observations()
            assert all(response.status_code == 200 for response in responses)
            assert responses[0].json() == responses[1].json()
            assert state["canonical_message_id"] is None and state["turn_id"] is None
            assert calls["managed"] == calls["effects"] == []
            assert len(calls["legacy"]) == 2
    finally:
        for task in tasks:
            await settle_request(task)


async def test_cs_outage_before_or_after_admission_never_calls_legacy(wh):
    h = wh
    await start_chain(h)
    completed = h.body()
    initial = await h.eventual_chat(completed)
    before = await h.state(completed["message_id"])
    await wait_response_outcome(h, completed["message_id"], "WRITE_COMPLETED")
    await h.diagnostic_snapshot("outage-before-kill", completed["message_id"],
                                extra={"initial_status": initial.status_code})
    h.kill("cs")
    assert (await h.chat(completed)).status_code == 503
    fresh = h.body()
    assert (await h.chat(fresh)).status_code == 503
    assert await h.rows("SELECT id FROM website_admissions WHERE message_id=:id",
                        id=fresh["message_id"]) == []
    assert h.observations()["legacy"] == []
    h.start("cs")
    await h.wait_ready("cs")
    response = await recover_public_result(
        h, completed, label="outage-after-restart-replay")
    assert response.status_code == 200
    assert (await h.state(completed["message_id"]))["turn_id"] == before["turn_id"]
    assert len(h.observations()["managed"]) == 1
