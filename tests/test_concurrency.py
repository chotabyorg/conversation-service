import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import error, locked_race, observed_waiters
from tests.test_turns import expire


@pytest.mark.parametrize("changed_thread", [False, True])
async def test_two_public_first_ingestions_overlap_real_unique_key_transaction(
    h, sql, monkeypatch, changed_thread
):
    from sqlalchemy.ext.asyncio import AsyncSession

    first_flushed = asyncio.Event()
    release = asyncio.Event()
    original_flush = AsyncSession.flush
    holder_pid = None
    intercepted = False

    async def pause_after_real_flush(session, *args, **kwargs):
        nonlocal intercepted, holder_pid
        await original_flush(session, *args, **kwargs)
        if not intercepted:
            intercepted = True
            holder_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            first_flushed.set()
            await asyncio.wait_for(release.wait(), 8)

    # Test scheduling only: database locks/constraints remain the safety mechanism.
    # At this seam first worker has truly inserted Conversation, Event, Message.
    monkeypatch.setattr(AsyncSession, "flush", pause_after_real_flush)
    payload = h.event()
    tasks = []
    try:
        tasks.append(asyncio.create_task(h.call("POST", "/inbound-events", payload)))
        await asyncio.wait_for(first_flushed.wait(), 5)
        other = (
            dict(payload, provider_thread_id="other-uncommitted-thread")
            if changed_thread
            else payload
        )
        tasks.append(asyncio.create_task(h.call("POST", "/inbound-events", other)))
        await observed_waiters(
            sql, holder_pid, 1, f"public first ingestion changed_thread={changed_thread}"
        )
        assert not any(task.done() for task in tasks)
        release.set()
        first, second = await asyncio.wait_for(asyncio.gather(*tasks), 8)
        assert first.status_code == 201
        if changed_thread:
            error(second, "EVENT_CONFLICT")
        else:
            assert second.status_code == 200 and second.json()["replayed"]
            assert second.json()["message"]["id"] == first.json()["message"]["id"]
        async with sql.connect() as connection:
            assert await connection.scalar(text("SELECT count(*) FROM conversations")) == 1
            assert await connection.scalar(text("SELECT count(*) FROM inbound_events")) == 1
            assert await connection.scalar(text("SELECT count(*) FROM messages")) == 1
            assert (
                await connection.scalar(text("SELECT last_message_sequence FROM conversations"))
                == 1
            )
            assert await connection.scalar(text("SELECT input_revision FROM conversations")) == 1
    finally:
        release.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_concurrent_first_creation_waits_for_uncommitted_unique_identity(h, sql):
    payloads = [h.event(), h.event()]
    tasks = []
    identity_id = str(uuid4())
    try:
        async with sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text(
                    "INSERT INTO conversations (id,owner_id,integration_id,channel,"
                    "provider_thread_id,last_message_sequence,input_revision,authority_revision,"
                    "control_mode,created_at,updated_at) VALUES (:id,:owner_id,:integration_id,"
                    ":channel,:provider_thread_id,0,0,0,'AGENT',clock_timestamp(),clock_timestamp())"
                ),
                dict(h.grants["a"], id=identity_id, provider_thread_id="thread-123"),
            )
            tasks = [
                asyncio.create_task(h.call("POST", "/inbound-events", payload))
                for payload in payloads
            ]
            await observed_waiters(sql, pid, 2, "first Conversation identity insertion")
        results = await asyncio.wait_for(asyncio.gather(*tasks), 8)
        assert [result.status_code for result in results] == [201, 201]
        assert {result.json()["conversation"]["id"] for result in results} == {identity_id}
        assert {result.json()["message"]["sequence"] for result in results} == {1, 2}
        async with sql.connect() as connection:
            assert await connection.scalar(text("SELECT count(*) FROM conversations")) == 1
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def test_concurrent_duplicate_event_one_journal_item(h, sql):
    accepted = await h.ingest()
    payload = h.event()
    results = await locked_race(
        sql,
        accepted["conversation"]["id"],
        [h.call("POST", "/inbound-events", payload) for _ in range(3)],
        "duplicate inbound event",
    )
    assert sorted(result.status_code for result in results) == [200, 200, 201]
    assert len({result.json()["message"]["id"] for result in results}) == 1
    assert len({result.json()["event"]["id"] for result in results}) == 1
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 2
        assert await connection.scalar(text("SELECT last_message_sequence FROM conversations")) == 2


async def test_concurrent_changed_payload_one_winner_one_conflict(h, sql):
    accepted = await h.ingest()
    payload = h.event()
    results = await locked_race(
        sql,
        accepted["conversation"]["id"],
        [
            h.call("POST", "/inbound-events", payload),
            h.call("POST", "/inbound-events", dict(payload, text="different")),
        ],
        "conflicting inbound payload",
    )
    assert sorted(result.status_code for result in results) == [201, 409]
    loser = next(result for result in results if result.status_code == 409)
    error(loser, "EVENT_CONFLICT")
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 2


async def test_concurrent_append_contiguous_unique_sequence_no_lost_messages(h, sql):
    accepted = await h.ingest()
    payloads = [h.event(text=f"message-{i}") for i in range(4)]
    results = await locked_race(
        sql,
        accepted["conversation"]["id"],
        [h.call("POST", "/inbound-events", payload) for payload in payloads],
        "Conversation sequence allocation",
    )
    assert all(result.status_code == 201 for result in results)
    assert {result.json()["message"]["sequence"] for result in results} == {2, 3, 4, 5}
    page = (await h.call("GET", f"/conversations/{accepted['conversation']['id']}/messages")).json()
    assert [item["sequence"] for item in page["items"]] == [1, 2, 3, 4, 5]
    assert {item["text"] for item in page["items"][1:]} == {payload["text"] for payload in payloads}
    conversation = (await h.call("GET", f"/conversations/{accepted['conversation']['id']}")).json()
    assert conversation["input_revision"] == conversation["last_message_sequence"] == 5


@pytest.mark.parametrize("same_key", [False, True])
async def test_overlapping_turn_create_active_slot(h, sql, same_key):
    accepted = await h.ingest()
    conversation_id = accepted["conversation"]["id"]
    first = dict(
        selection_key=str(uuid4()),
        input_first_sequence=1,
        input_last_sequence=1,
        expected_input_revision=1,
        expected_authority_revision=0,
    )
    second = dict(first, selection_key=first["selection_key"] if same_key else str(uuid4()))
    results = await locked_race(
        sql,
        conversation_id,
        [
            h.call("POST", f"/conversations/{conversation_id}/turns", payload)
            for payload in [first, second]
        ],
        f"Turn creation same_key={same_key}",
    )
    assert sorted(result.status_code for result in results) == (
        [200, 201] if same_key else [201, 409]
    )
    if same_key:
        assert len({result.json()["turn"]["id"] for result in results}) == 1
    else:
        error(next(result for result in results if result.status_code == 409), "ACTIVE_TURN_EXISTS")
    async with sql.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM agent_turns WHERE status IN ('PENDING','CLAIMED')")
            )
            == 1
        )


@pytest.mark.parametrize("recovery", [False, True])
async def test_overlapping_claims_one_owner_on_same_turn(h, sql, recovery):
    turn, _ = await h.select(await h.ingest())
    initial = None
    if recovery:
        initial = await h.claim(turn)
        await expire(sql, turn["id"])
    results = await locked_race(
        sql,
        turn["conversation_id"],
        [
            h.call("POST", f"/turns/{turn['id']}/claim", dict(worker_ref=worker, lease_seconds=30))
            for worker in ["worker-a", "worker-b"]
        ],
        f"Turn lease claim recovery={recovery}",
    )
    assert sorted(result.status_code for result in results) == [200, 409]
    error(next(result for result in results if result.status_code == 409), "LEASE_HELD")
    winner = next(result for result in results if result.status_code == 200).json()
    assert winner["turn"]["id"] == turn["id"]
    assert winner["turn"]["claim_epoch"] == (2 if recovery else 1)
    if initial:
        assert winner["claim_token"] != initial["claim_token"]


async def test_overlapping_control_cas_one_revision_winner(h, sql):
    accepted = await h.ingest()
    conversation_id = accepted["conversation"]["id"]
    results = await locked_race(
        sql,
        conversation_id,
        [
            h.call(
                "POST",
                f"/conversations/{conversation_id}/control",
                dict(expected_authority_revision=0, control_mode=mode),
            )
            for mode in ["HUMAN", "HUMAN_PENDING"]
        ],
        "authority compare-and-set",
    )
    assert sorted(result.status_code for result in results) == [200, 409]
    error(
        next(result for result in results if result.status_code == 409),
        "AUTHORITY_REVISION_CONFLICT",
    )
    assert (
        next(result for result in results if result.status_code == 200).json()["authority_revision"]
        == 1
    )


@pytest.mark.parametrize("same_key", [False, True])
async def test_overlapping_delivery_reconciliation_cas_and_replay(h, sql, same_key):
    accepted, attempt, _ = await h.delivery()
    first = dict(
        reconciliation_key=str(uuid4()),
        expected_revision=0,
        status="DISPATCH_REQUESTED",
        expected_authority_revision=0,
    )
    second = dict(
        first, reconciliation_key=first["reconciliation_key"] if same_key else str(uuid4())
    )
    results = await locked_race(
        sql,
        accepted["conversation"]["id"],
        [
            h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", payload)
            for payload in [first, second]
        ],
        f"delivery reconciliation same_key={same_key}",
    )
    assert sorted(result.status_code for result in results) == (
        [200, 200] if same_key else [200, 409]
    )
    if same_key:
        assert sorted(result.json()["replayed"] for result in results) == [False, True]
    else:
        error(
            next(result for result in results if result.status_code == 409),
            "DELIVERY_REVISION_CONFLICT",
        )
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM delivery_reconciliations")) == 1


@pytest.mark.parametrize("suffix", ["complete", "renew", "fail", "claim"])
async def test_lease_database_clock_after_observed_lock_wait(h, sql, suffix):
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    async with sql.begin() as connection:
        expiry = await connection.scalar(
            text(
                "UPDATE agent_turns SET lease_expires_at=clock_timestamp()+interval '1.5 seconds' "
                "WHERE id=:id RETURNING lease_expires_at"
            ),
            {"id": turn["id"]},
        )
    payload = dict(h.proof(claim))
    if suffix == "complete":
        payload["outcome"] = "NO_RESPONSE"
    elif suffix == "fail":
        payload["failure_code"] = "TIMEOUT"
    elif suffix == "renew":
        payload["lease_seconds"] = 30
    else:
        payload = dict(worker_ref="recovery", lease_seconds=30)
    task = None
    try:
        async with sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                {"id": turn["conversation_id"]},
            )
            task = asyncio.create_task(h.call("POST", f"/turns/{turn['id']}/{suffix}", payload))
            waiters = await observed_waiters(sql, pid, 1, f"lease deadline while blocked: {suffix}")
            assert all(waiter["xact_start"] < expiry for waiter in waiters)
            deadline = asyncio.get_running_loop().time() + 5
            while True:
                async with sql.connect() as observer:
                    now = await observer.scalar(text("SELECT clock_timestamp()"))
                if now > expiry:
                    break
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.02)
            assert not task.done()
        result = await asyncio.wait_for(task, 8)
        if suffix == "claim":
            assert result.status_code == 200 and result.json()["turn"]["id"] == turn["id"]
            assert result.json()["turn"]["claim_epoch"] == 2
        else:
            error(result, "LEASE_EXPIRED")
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unrelated_conversation_commits_while_a_is_row_blocked(h, sql):
    a = await h.ingest(provider_thread_id="A")
    b = await h.ingest(provider_thread_id="B")
    task = None
    try:
        async with sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                {"id": a["conversation"]["id"]},
            )
            task = asyncio.create_task(
                h.call("POST", "/inbound-events", h.event(provider_thread_id="A"))
            )
            await observed_waiters(sql, pid, 1, "A blocked while unrelated B commits")
            progressed = await asyncio.wait_for(
                h.call("POST", "/inbound-events", h.event(provider_thread_id="B")), 3
            )
            assert progressed.status_code == 201 and progressed.json()["message"]["sequence"] == 2
            async with sql.connect() as observer:
                assert (
                    await observer.scalar(
                        text("SELECT last_message_sequence FROM conversations WHERE id=:id"),
                        {"id": b["conversation"]["id"]},
                    )
                    == 2
                )
            assert not task.done()
        assert (await asyncio.wait_for(task, 8)).status_code == 201
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
