import asyncio
import secrets
from uuid import uuid4

import pytest
from sqlalchemy import text

from core.config import Settings
from schemas.managed import ECHO_FIELDS, ManagedTurnResult
from services import managed_execution
from services.managed_client import ManagedPortFailure
from tests.conftest import app_client, error, observed_waiters
from tests.test_turns import expire


def response_for(prepared, status="DRAFT_READY", error_code=None):
    return ManagedTurnResult(
        **prepared.request.model_dump(include=set(ECHO_FIELDS)),
        status=status,
        draft_text="managed candidate" if status == "DRAFT_READY" else None,
        error_code=error_code,
        operation_refs=[],
    )


class CapturePort:
    def __init__(self, database):
        self.database = database
        self.calls = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.failure = None
        self.status = "DRAFT_READY"
        self.error_code = None

    async def execute(self, prepared, binding, request_id=""):
        assert self.database.engine.pool.checkedout() == 0
        self.calls.append((prepared, binding))
        self.entered.set()
        await self.release.wait()
        if self.failure:
            raise self.failure
        return response_for(prepared, self.status, self.error_code)


@pytest.fixture
def managed(h):
    binding = dict(
        owner_id=h.grants["a"]["owner_id"],
        integration_id=h.grants["a"]["integration_id"],
        channel="TELEGRAM",
        agent_id=uuid4(),
        service_token=secrets.token_urlsafe(32),
    )
    settings = Settings(
        database_url=h.settings.database_url,
        auth_principals=h.settings.auth_principals,
        managed_ai_bindings=[binding],
        managed_ai_base_url="http://127.0.0.1:1",
        _env_file=None,
    )
    port = CapturePort(h.database)
    h.client._transport.app.state.settings = settings
    h.client._transport.app.state.managed_port = port
    return settings, port


async def execute(h, turn, claim):
    return await h.call("POST", f"/turns/{turn['id']}/execute", h.proof(claim))


async def wait_expiry(sql, turn_id):
    deadline = asyncio.get_running_loop().time() + 4
    while True:
        async with sql.connect() as connection:
            expired = await connection.scalar(
                text("SELECT clock_timestamp() >= lease_expires_at FROM agent_turns WHERE id=:id"),
                {"id": turn_id},
            )
        if expired:
            return
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.02)


async def test_execute_stores_same_turn_draft_only_and_winning_replay(h, managed, sql):
    settings, port = managed
    accepted = await h.ingest()
    turn, _ = await h.select(accepted)
    claim = await h.claim(turn)
    done = await execute(h, turn, claim)
    assert done.status_code == 200 and not done.json()["replayed"]
    saved = done.json()["turn"]
    assert saved["id"] == turn["id"] and saved["outcome"] == "DRAFT_READY"
    assert saved["draft_text"] == "managed candidate"
    assert saved["managed_agent_id"] == str(settings.managed_ai_bindings[0].agent_id)
    assert saved["managed_dispatch_epoch"] == 1 and len(saved["managed_request_fingerprint"]) == 64
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM delivery_attempts")) == 0
        assert await connection.scalar(text("SELECT count(*) FROM delivery_reconciliations")) == 0
        assert await connection.scalar(text("SELECT last_message_sequence FROM conversations")) == 1
    await expire(sql, turn["id"])
    saved = (await h.call("GET", f"/turns/{turn['id']}")).json()
    replay = await execute(h, turn, claim)
    assert replay.status_code == 200 and replay.json() == dict(turn=saved, replayed=True)
    assert len(port.calls) == 1
    error(
        await h.call(
            "POST", f"/turns/{turn['id']}/execute", dict(h.proof(claim), claim_token="z" * 43)
        ),
        "STALE_CLAIM",
    )


async def test_non_suffix_selection_and_later_inbound_frozen_outside_context(h, managed):
    _, port = managed
    first = await h.ingest(text="first")
    await h.ingest(text="second")
    last = await h.ingest(text="third")
    turn, _ = await h.select(last, input_first_sequence=1, input_last_sequence=2)
    claim = await h.claim(turn)
    await h.ingest(text="new after freeze")
    assert (
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/control",
            dict(expected_authority_revision=0, control_mode="HUMAN"),
        )
    ).status_code == 200
    assert (await execute(h, turn, claim)).status_code == 200
    request = port.calls[0][0].request
    assert [m.text for m in request.messages] == ["first", "second", "third"]
    assert request.selected_input_message_ids == tuple(m.id for m in request.messages[:2])
    assert str(request.messages[0].id) == first["message"]["id"]
    assert request.input_revision == 3 and request.context_last_sequence == 3


async def test_same_epoch_duplicate_does_not_terminalize_live_execution(h, managed):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    port.release.clear()
    pending = asyncio.create_task(execute(h, turn, claim))
    try:
        await asyncio.wait_for(port.entered.wait(), 3)
        error(await execute(h, turn, claim), "MANAGED_EXECUTION_STARTED")
        assert (await h.call("GET", f"/turns/{turn['id']}")).json()["status"] == "CLAIMED"
        assert len(port.calls) == 1
    finally:
        port.release.set()
    assert (await pending).status_code == 200


async def test_inflight_stale_worker_cannot_overwrite_recovered_unknown(h, managed, sql):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    old = await h.claim(turn, lease_seconds=1)
    port.release.clear()
    pending = asyncio.create_task(execute(h, turn, old))
    try:
        await asyncio.wait_for(port.entered.wait(), 3)
        await wait_expiry(sql, turn["id"])
        new = await h.claim(turn, worker_ref="winner", lease_seconds=30)
        winner = await execute(h, turn, new)
        assert winner.status_code == 200
        saved = winner.json()["turn"]
        assert saved["claim_epoch"] == 2 and saved["failure_code"] == "EXECUTION_UNKNOWN"
        assert saved["draft_text"] is None and saved["managed_dispatch_epoch"] == 1
    finally:
        port.release.set()
    error(await pending, "STALE_CLAIM")
    assert (await h.call("GET", f"/turns/{turn['id']}")).json() == saved
    assert len(port.calls) == 1


@pytest.mark.parametrize("phase", ["snapshot", "marker", "result"])
async def test_execute_samples_database_clock_after_lock_wait(h, managed, sql, monkeypatch, phase):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn, lease_seconds=1)
    ready, proceed = asyncio.Event(), asyncio.Event()
    original = managed_execution.take_snapshot

    async def snapshot(*args, **kwargs):
        result = await original(*args, **kwargs)
        if phase == "marker":
            ready.set()
            await proceed.wait()
        return result

    monkeypatch.setattr(managed_execution, "take_snapshot", snapshot)
    if phase == "result":
        port.release.clear()
    task = None
    try:
        if phase != "snapshot":
            task = asyncio.create_task(execute(h, turn, claim))
            await asyncio.wait_for(ready.wait() if phase == "marker" else port.entered.wait(), 3)
        async with sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                {"id": turn["conversation_id"]},
            )
            if phase == "snapshot":
                task = asyncio.create_task(execute(h, turn, claim))
            elif phase == "marker":
                proceed.set()
            else:
                port.release.set()
            await observed_waiters(sql, pid, 1, f"managed {phase} post-lock clock")
            await wait_expiry(sql, turn["id"])
        error(await asyncio.wait_for(task, 3), "LEASE_EXPIRED")
        assert len(port.calls) == (1 if phase == "result" else 0)
    finally:
        proceed.set()
        port.release.set()
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_missing_binding_and_untrusted_execute_payload_cannot_dispatch(h, managed):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    error(
        await h.call(
            "POST", f"/turns/{turn['id']}/execute", dict(h.proof(claim), agent_id=str(uuid4()))
        ),
        "VALIDATION_ERROR",
        422,
    )
    error(
        await h.call("POST", f"/turns/{turn['id']}/execute", h.proof(claim), "b"), "NOT_FOUND", 404
    )
    h.client._transport.app.state.settings = h.settings
    error(await execute(h, turn, claim), "SERVICE_UNAVAILABLE", 503)
    assert not port.calls
    assert (await h.call("GET", f"/turns/{turn['id']}")).json()["managed_agent_id"] is None


async def test_ambiguous_transport_terminal_and_retained_engine_restart_never_retry(
    h, managed, database_url
):
    settings, port = managed
    port.failure = ManagedPortFailure()
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    failed = await execute(h, turn, claim)
    assert failed.status_code == 200
    assert failed.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
    async with app_client(settings, database_url) as (client, _):
        repeat = await client.post(
            f"/internal/v1/turns/{turn['id']}/execute", json=h.proof(claim), headers=h.headers()
        )
        assert repeat.status_code == 200 and repeat.json()["replayed"]
    assert len(port.calls) == 1


async def test_marker_commit_ack_loss_prevents_send_and_recovery(h, managed, sql, monkeypatch):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    original = managed_execution.mark_dispatch

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("synthetic commit uncertainty")

    monkeypatch.setattr(managed_execution, "mark_dispatch", lost_ack)
    error(await execute(h, turn, claim), "INTERNAL_ERROR", 500)
    error(await execute(h, turn, claim), "MANAGED_EXECUTION_STARTED")
    assert not port.calls
    await expire(sql, turn["id"])
    recovered = await h.claim(turn)
    done = await execute(h, turn, recovered)
    assert done.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
    assert not port.calls


async def test_result_commit_ack_loss_replay_preserves_committed_draft(h, managed, monkeypatch):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    original = managed_execution.turns.finish_turn

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("synthetic terminal ack loss")

    monkeypatch.setattr(managed_execution.turns, "finish_turn", lost_ack)
    error(await execute(h, turn, claim), "INTERNAL_ERROR", 500)
    replay = await execute(h, turn, claim)
    assert replay.status_code == 200 and replay.json()["replayed"]
    assert replay.json()["turn"]["draft_text"] == "managed candidate"
    assert len(port.calls) == 1


async def test_cancellation_after_marker_propagates_and_does_not_repeat(h, managed):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    port.release.clear()
    task = asyncio.create_task(execute(h, turn, claim))
    await asyncio.wait_for(port.entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 6)
    saved = (await h.call("GET", f"/turns/{turn['id']}")).json()
    assert saved["failure_code"] == "EXECUTION_UNKNOWN"
    assert (await execute(h, turn, claim)).json()["replayed"]
    assert len(port.calls) == 1


async def test_repeated_cancellation_joins_the_same_db_finalization(h, managed, sql):
    _, port = managed
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    port.release.clear()
    task = asyncio.create_task(execute(h, turn, claim))
    await asyncio.wait_for(port.entered.wait(), 3)
    async with sql.begin() as blocker:
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
            {"id": turn["conversation_id"]},
        )
        task.cancel()
        await observed_waiters(sql, pid, 1, "joined managed cancellation finalization")
        task.cancel()
        task.cancel()
        assert not task.done()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 6)
    assert (await h.call("GET", f"/turns/{turn['id']}")).json()[
        "failure_code"
    ] == "EXECUTION_UNKNOWN"
    assert not [
        t for t in asyncio.all_tasks() if t.get_name() == "managed-turn-cancel-finalization"
    ]
    assert len(port.calls) == 1


@pytest.mark.parametrize("count,text_value", [(257, "small"), (17, "é" * 16384)],
                         ids=["message-count", "utf8-byte-limit"])
async def test_over_limit_frozen_projection_never_sets_marker(h, managed, sql, count, text_value):
    _, port = managed
    accepted = await h.ingest(text=text_value)
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO messages(id,conversation_id,sequence,actor,direction,"
                "content_type,text) "
                "SELECT gen_random_uuid(),:conversation,series,'HUMAN','OUTBOUND','TEXT',:text "
                "FROM generate_series(2,:count) series"
            ),
            dict(conversation=accepted["conversation"]["id"], count=count, text=text_value),
        )
        await connection.execute(
            text("UPDATE conversations SET last_message_sequence=:count WHERE id=:id"),
            dict(count=count, id=accepted["conversation"]["id"]),
        )
    current = (await h.call("GET", f"/conversations/{accepted['conversation']['id']}")).json()
    turn, _ = await h.select(dict(accepted, conversation=current), input_last_sequence=1)
    claim = await h.claim(turn)
    error(await execute(h, turn, claim), "VALIDATION_ERROR", 422)
    saved = (await h.call("GET", f"/turns/{turn['id']}")).json()
    assert saved["managed_agent_id"] is None and saved["status"] == "CLAIMED"
    assert not port.calls


async def test_any_actor_selected_range_retains_canonical_ids_order(h, managed, sql):
    _, port = managed
    accepted = await h.ingest()
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO messages(id,conversation_id,sequence,actor,direction,"
                "content_type,text) "
                "VALUES (:human,:conversation,2,'HUMAN','OUTBOUND','TEXT','human'),"
                "(:system,:conversation,3,'SYSTEM','INBOUND','TEXT','system')"
            ),
            dict(
                human=str(uuid4()), system=str(uuid4()), conversation=accepted["conversation"]["id"]
            ),
        )
        await connection.execute(
            text("UPDATE conversations SET last_message_sequence=3 WHERE id=:id"),
            dict(id=accepted["conversation"]["id"]),
        )
    current = (await h.call("GET", f"/conversations/{accepted['conversation']['id']}")).json()
    turn, _ = await h.select(
        dict(accepted, conversation=current), input_first_sequence=2, input_last_sequence=3
    )
    claim = await h.claim(turn)
    assert (await execute(h, turn, claim)).status_code == 200
    request = port.calls[0][0].request
    assert [m.actor.value for m in request.messages] == ["CUSTOMER", "HUMAN", "SYSTEM"]
    assert request.selected_input_message_ids == tuple(m.id for m in request.messages[1:])


@pytest.mark.parametrize(
    "status,code,failure",
    [
        ("NO_RESPONSE", None, None),
        ("FAILED", "MODEL_TIMEOUT", "TIMEOUT"),
        ("FAILED", "CONTEXT_LIMIT", "INVALID_RESULT"),
        ("FAILED", "AGENT_INACTIVE", "PROCESSING_ERROR"),
        ("UNKNOWN", "TOOL_EXECUTION_UNKNOWN", "EXECUTION_UNKNOWN"),
    ],
)
async def test_known_runtime_result_mapping_and_terminal_replay(h, managed, status, code, failure):
    _, port = managed
    port.status, port.error_code = status, code
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    result = await execute(h, turn, claim)
    assert result.status_code == 200
    saved = result.json()["turn"]
    assert saved["failure_code"] == failure and saved["draft_text"] is None
    assert saved["status"] == ("COMPLETED" if failure is None else "FAILED")
    assert (await execute(h, turn, claim)).json()["replayed"] and len(port.calls) == 1
