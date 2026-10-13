"""Explicit process/PG crash, FIFO, sticky routing and HTTP replay acceptance.

Run directly; ordinary pytest collection intentionally excludes this filename.
The fake effect counter records every AI execution rather than deduplicating it.
"""

import asyncio
from uuid import uuid4

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
    while asyncio.get_running_loop().time() < deadline:
        state = await h.state(message_id)
        if state["status"] in ("COMPLETED", "FAILED"):
            return state
        await asyncio.sleep(0.05)
    pytest.fail("Stable Turn did not terminalize after process recovery")


async def wait_history(h, conversation_id, expected_agents):
    deadline = asyncio.get_running_loop().time() + 8
    while asyncio.get_running_loop().time() < deadline:
        rows = await h.rows("SELECT id,sequence,actor,direction,content_type,text,"
            "source_agent_turn_id "
            "FROM messages WHERE conversation_id=:id ORDER BY sequence", id=conversation_id)
        if sum(row["actor"] == "AGENT" for row in rows) == expected_agents:
            return rows
        await asyncio.sleep(0.02)
    pytest.fail("Required durable assistant history was not committed")


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
    if before["turn_id"] is not None:
        assert after["turn_id"] == before["turn_id"]
    assert after["canonical_message_id"] == before["canonical_message_id"]
    assert after["admission_id"] == before["admission_id"]
    uncertain_execution = point in ("D", "E")
    response = await h.chat(body)
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
    for index, (body, reply) in enumerate(zip(bodies, replies, strict=True)):
        response = await h.chat(body)
        if index < len(bodies) - 1:
            assert response.status_code == 409
            assert response.json() == {
                "error": "response_superseded", "session_id": body["session_id"]
            }
            assert "reply" not in response.json()
        else:
            assert response.status_code == 200 and response.json()["reply"] == reply
    history = await wait_history(h, state["conversation_id"], len(customer))
    assert len(history) == 2 * len(customer)
    assert len({m["source_agent_turn_id"] for m in history
                if m["actor"] == "AGENT"}) == len(customer)
    assert len(h.observations()["managed"]) == len(customer)
    assert h.observations()["legacy"] == []
    # Every external endpoint is denied by all process guards; this fake has no
    # Business/provider mutation route. Its effects are execution-count sentinels.
    EVIDENCE.append({"scenario": "Goal4A.1 deterministic dialog", "customer_turns": len(customer),
                     "canonical_messages": len(history), "prior_agent_rows_in_final_request":
                     len(customer) - 1, "ai_http_calls": len(h.observations()["managed"]),
                     "business_provider_calls": 0})


@pytest.mark.parametrize("outcome", ["WRITE_FAILED", "UNKNOWN"])
async def test_failed_or_unknown_old_draft_is_not_retried_after_newer_input(wh, outcome):
    h = wh
    await start_chain(h, fault="F")
    first = h.body(message="first-draft")
    pending = asyncio.create_task(h.chat(first))
    await h.barrier("F")  # Actual draft retained, before Website physical write.
    state = await h.state(first["message_id"])
    h.kill("website")
    await settle_request(pending)
    assert len(await wait_history(h, state["conversation_id"], 0)) == 1
    attempt = {"owner_id": h.ids["owner"], "integration_id": h.ids["integration"],
               "agent_id": h.ids["agent"], "session_id": first["session_id"],
               "message_id": first["message_id"], "response_attempt_id": str(uuid4())}
    # Explicit fixture transport fact exercises the real protected HTTP API.
    # The later message fences the old draft; a different attempt cannot retry it.
    for path, body in (("begin", attempt), ("complete", dict(attempt, outcome=outcome))):
        response = await h.client.post(h.url("cs") + "/internal/v1/website/responses/" + path,
            headers={"X-Service-Token": h.tokens["website"]}, json=body)
        assert response.status_code == 200
    assert len(await wait_history(h, state["conversation_id"], 0)) == 1
    h.start("website")
    await h.wait_ready("website")
    second = h.body(message="second-draft")
    assert (await h.eventual_chat(second)).status_code == 200
    second_state = await h.state(second["message_id"])
    before_retry = await wait_history(h, state["conversation_id"], 1)
    second_request = h.observations()["managed"][-1]
    assert [m["actor"] for m in second_request["messages"]] == ["CUSTOMER", "CUSTOMER"]
    stale_retry = await h.chat(first)
    assert stale_retry.status_code == 409
    assert stale_retry.json() == {
        "error": "response_superseded", "session_id": first["session_id"]
    }
    assert "reply" not in stale_retry.json()
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


@pytest.mark.asyncio
async def test_goal5_website_process_burst_supersede_and_restart(wh):
    h = wh
    h.settling_enabled = True
    h.replies.update({
        "old user request": "STALE DRAFT MUST NOT REACH THE CLIENT",
        "new user request": "Fresh response",
    })
    configured = await h.client.post(h.url("fake") + "/test/replies", json=h.replies)
    assert configured.status_code == 200
    await start_chain(h)

    burst_one = h.body(session_id=h.ids["session"], message="burst one")
    burst_two = h.body(session_id=h.ids["session"], message="burst two")
    responses = await asyncio.gather(h.chat(burst_one), h.chat(burst_two))
    assert sorted(response.status_code for response in responses) == [200, 503]
    assert sum(response.status_code == 200 for response in responses) == 1
    denied = next(response for response in responses if response.status_code == 503)
    assert denied.json()["error"] == "response_unavailable"
    assert "reply" not in denied.json()
    assert len(h.observations()["managed"]) == 1
    burst_states = [await h.state(value["message_id"]) for value in (burst_one, burst_two)]
    assert burst_states[0]["turn_id"] == burst_states[1]["turn_id"]
    assert len(await wait_history(h, burst_states[0]["conversation_id"], 1)) == 3

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
            state = await h.state(second["message_id"])
            assert state["canonical_message_id"] is not None
            assert state["conversation_id"] != initial["conversation_id"]
            assert not request.done()
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
        state = await h.state(body["message_id"])
        calls = h.observations()
        if state["route"] == "MANAGED":
            assert all(response.status_code in {200, 503} for response in responses)
            successful = [response for response in responses if response.status_code == 200]
            assert successful and all(response.json() == successful[0].json()
                                      for response in successful)
            assert all("reply" not in response.json() for response in responses
                       if response.status_code == 503)
            assert len(calls["managed"]) == len(calls["effects"]) == 1
            assert calls["legacy"] == []
        else:
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
    await h.eventual_chat(completed)
    before = await h.state(completed["message_id"])
    h.kill("cs")
    assert (await h.chat(completed)).status_code == 503
    fresh = h.body()
    assert (await h.chat(fresh)).status_code == 503
    assert await h.rows("SELECT id FROM website_admissions WHERE message_id=:id",
                        id=fresh["message_id"]) == []
    assert h.observations()["legacy"] == []
    h.start("cs")
    await h.wait_ready("cs")
    assert (await h.chat(completed)).status_code == 200
    assert (await h.state(completed["message_id"]))["turn_id"] == before["turn_id"]
    assert len(h.observations()["managed"]) == 1
