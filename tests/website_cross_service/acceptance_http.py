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
    uncertain = point in ("D", "E")
    response = await h.eventual_chat(body, status=503 if uncertain else 200)
    if uncertain:
        assert after["failure_code"] == "EXECUTION_UNKNOWN" and after["draft_text"] is None
        assert after["delivery_id"] is None
        if point == "E":
            operation = h.observations()["effects"][0]["operation_id"]
            assert after["operation_refs"] == [{"origin": "AI_AGENT_SERVICE",
                                                "operation_id": operation,
                                                "outcome": "SUCCEEDED"}]
        else:
            assert after["operation_refs"] == []
    else:
        assert response.json()["reply"] == "Managed fixture reply"
        assert response.headers["Access-Control-Allow-Origin"] == "https://fixture.example"
        assert response.headers["Cache-Control"] == "no-store"
    replay = await h.chat(body)
    assert replay.status_code == (503 if uncertain else 200)
    final = await h.state(body["message_id"])
    assert final["turn_id"] == after["turn_id"]
    assert final["canonical_message_id"] == after["canonical_message_id"]
    observations = h.observations()
    expected_calls = 0 if point == "D" else 1
    assert len(observations["managed"]) == expected_calls
    assert len(observations["effects"]) == expected_calls
    assert observations["legacy"] == []
    counts = await h.rows("SELECT (SELECT count(*) FROM messages) AS messages,"
                         "(SELECT count(*) FROM inbound_events) AS events,"
                         "(SELECT count(*) FROM agent_turns) AS turns")
    assert counts == [{"messages": 1, "events": 1, "turns": 1}]
    attempts = await h.rows("SELECT id,outcome FROM website_response_attempts ORDER BY created_at")
    if not uncertain:
        assert final["delivery_status"] == "UNKNOWN"
        assert any(row["outcome"] == "WRITE_COMPLETED" for row in attempts)
        if point in ("G", "H"):
            assert attempts[0]["outcome"] is None  # Crash intent never becomes browser ACK.
    record(point, before, final, observations, attempts)


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
        assert all(response.status_code == 200 for response in responses)
        assert responses[0].json() == responses[1].json()
        calls = h.observations()
        assert len(calls["managed"]) == len(calls["effects"]) == 1
        assert calls["legacy"] == []
        assert len(await h.rows("SELECT id FROM website_response_attempts")) == 2
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
        assert all(response.status_code == 200 for response in responses)
        assert responses[0].json() == responses[1].json()
        state = await h.state(body["message_id"])
        calls = h.observations()
        if state["route"] == "MANAGED":
            assert len(calls["managed"]) == len(calls["effects"]) == 1
            assert calls["legacy"] == []
        else:
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
