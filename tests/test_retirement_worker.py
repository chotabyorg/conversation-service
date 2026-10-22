"""Native cursor fairness, lock timeout, retry and cancellation regressions."""

import asyncio
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, null, select, text
from sqlalchemy.exc import OperationalError

import retirement_worker as worker
from core.errors import DomainError
from models import (
    Conversation,
    DeliveryAttempt,
    Message,
    WriterRecoveryCommand,
    WriterRecoveryScan,
    WriterRegistration,
    WriterWitness,
)
from schemas.contracts import ErrorCode
from services import writer_attestation
from services.common import fingerprint
from tests.conftest import observed_waiters
from tests.test_writer_attestation import (
    BEGIN,
    REGISTER,
    WITNESS,
    closure,
    post,
    prepared,
)
from tests.test_writer_attestation import inbox_harness as inbox_fixture
from tests.test_writer_attestation import user_me_transport as user_fixture
from tests.test_writer_attestation import writer_harness as writer_fixture
from tests.test_writer_retirement import intent

inbox_harness, writer_harness, user_me_transport = inbox_fixture, writer_fixture, user_fixture


async def positive(h):
    registration = await prepared(h)
    registration["registration_id"] = str(UUID(int=1000))
    assert (await post(h, REGISTER, registration)).status_code == 200
    assert (await post(h, BEGIN, registration["descriptor"], h.service_headers)).json()["send"]
    assert (await post(h, WITNESS, closure(registration))).status_code == 200
    return registration


def local_proof(h, monkeypatch):
    async def read(request, digest, settings):
        assert h.database.engine.pool.checkedout() == 0
        return await writer_attestation.proof(h.database, request, settings)

    monkeypatch.setattr("services.writer_retirement._termination_proof", read)


async def seed_page(h, registration, numbers):
    """Real constrained PG candidates; injected failures never retire these fixture sources."""

    def copy(row, **changes):
        return dict({c.name: getattr(row, c.name) for c in row.__table__.columns}, **changes)

    async with h.database.session_factory() as session, session.begin():
        original = await session.get(
            DeliveryAttempt, UUID(registration["descriptor"]["outbound_intent_id"])
        )
        root = await session.get(Conversation, original.conversation_id)
        message = await session.get(Message, original.source_message_id)
        for number in numbers:
            root_id, message_id, intent_id, proof_ref = (uuid4() for _ in range(4))
            session.add(Conversation(**copy(root, id=root_id, provider_thread_id=str(root_id))))
            await session.flush()
            session.add(Message(**copy(message, id=message_id, conversation_id=root_id)))
            await session.flush()
            session.add(
                DeliveryAttempt(
                    **copy(
                        original,
                        id=intent_id,
                        conversation_id=root_id,
                        source_message_id=message_id,
                        delivery_key=uuid4(),
                        case_id=None,
                        case_revision=None,
                        assignment_revision=None,
                        grant_generation=None,
                        writer_state="LIVE",
                        writer_ended_at=None,
                        writer_retirement_command_id=None,
                        writer_retirement_fingerprint=None,
                        writer_retirement_receipt=null(),
                    )
                )
            )
            await session.flush()
            descriptor = dict(
                registration["descriptor"],
                conversation_id=str(root_id),
                outbound_intent_id=str(intent_id),
                source_id=str(message_id),
            )
            session.add(
                WriterRegistration(
                    id=UUID(int=number),
                    intent_id=intent_id,
                    producer_id="fixture-parent",
                    supplier_instance_id=original.writer_supplier_instance_id,
                    process_incarnation_id=original.writer_process_incarnation_id,
                    descriptor=descriptor,
                    fingerprint=fingerprint(descriptor),
                    created_at=datetime.now(UTC),
                )
            )
            await session.flush()
            witness = closure(dict(registration_id=str(UUID(int=number))))
            session.add(
                WriterWitness(
                    registration_id=UUID(int=number),
                    proof_ref=proof_ref,
                    witness=witness,
                    fingerprint=fingerprint(witness),
                    created_at=datetime.now(UTC),
                )
            )


async def test_101_candidates_progress_past_failures_across_restart_and_wrap(
    writer_harness,
    user_me_transport,
    monkeypatch,
):
    h = writer_harness
    registration = await positive(h)
    await seed_page(h, registration, range(1, 101))
    local_proof(h, monkeypatch)
    actual = worker.recovery
    seen = []

    async def unavailable(database, registration_id, settings):
        seen.append(registration_id)
        if registration_id.int < 1000:
            raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
        return await actual(database, registration_id, settings)

    monkeypatch.setattr(worker, "recovery", unavailable)
    assert await worker.sweep(h.database, h.settings) == 0
    assert seen == [UUID(int=i) for i in range(1, 101)]
    # Pool/process restart loses every in-memory position; durable scan still advances.
    await h.database.dispose()
    assert await worker.sweep(h.database, h.settings) == 1
    assert seen[-1] == UUID(int=1000)
    assert (await intent(h, registration["descriptor"])).writer_state == "RETIRED"
    await seed_page(h, registration, [0])
    await h.database.dispose()
    assert await worker.sweep(h.database, h.settings) == 0
    assert UUID(int=0) in seen  # insertion below the old cursor appears on wrap
    assert await worker.sweep(h.database, h.settings) == 0
    assert seen[-1] == UUID(int=100)
    async with h.database.session_factory() as session:
        original = await session.scalar(select(WriterRecoveryCommand))
        assert original is not None
        assert await session.get(WriterRecoveryScan, "text-v1") is not None


async def test_concurrent_scanners_serialize_disjoint_pages_then_wrap(
    writer_harness,
    user_me_transport,
):
    h = writer_harness
    registration = await positive(h)
    await seed_page(h, registration, range(1, 101))
    first, second = await asyncio.gather(worker.page(h.database), worker.page(h.database))
    assert sorted([len(first), len(second)]) == [1, 100]
    assert not set(first).intersection(second)
    assert len(set(first + second)) == 101
    await h.database.dispose()
    assert await worker.page(h.database) == [UUID(int=i) for i in range(1, 101)]


async def test_real_scope_lock_timeout_is_contained_and_same_command_retries(
    writer_harness,
    user_me_transport,
    sql,
    monkeypatch,
    caplog,
):
    h = writer_harness
    registration = await positive(h)
    retained = await writer_attestation.recovery(h.database, UUID(int=1000), h.settings)
    local_proof(h, monkeypatch)
    await h.database.dispose()

    def limit_lock(dbapi, record):
        cursor = dbapi.cursor()
        cursor.execute("SET lock_timeout='750ms'")
        cursor.close()

    event.listen(h.database.engine.sync_engine, "connect", limit_lock)
    async with sql.begin() as blocker:
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(text("SELECT owner_id FROM inbox_grant_scopes FOR UPDATE"))
        task = asyncio.create_task(worker.sweep(h.database, h.settings))
        await observed_waiters(sql, pid, 1, "recovery driver native lock timeout")
        assert await asyncio.wait_for(task, 3) == 0
    assert (await intent(h, registration["descriptor"])).writer_state == "LIVE"
    assert (
        await writer_attestation.recovery(h.database, UUID(int=1000), h.settings)
    ).command == retained.command
    assert await worker.sweep(h.database, h.settings) == 1
    assert (await intent(h, registration["descriptor"])).writer_state == "RETIRED"
    assert h.database.engine.pool.checkedout() == 0
    logs = caplog.text
    assert '"code":"WRITER_RECOVERY_RETRY"' in logs
    assert '"stage":"writer_recovery"' in logs
    assert "SELECT inbox_grant_scopes" not in logs and "Traceback" not in logs


async def test_snapshot_outage_rolls_back_cursor_and_continuous_loop_recovers(
    writer_harness,
    user_me_transport,
    monkeypatch,
):
    h = writer_harness
    registration = await positive(h)
    local_proof(h, monkeypatch)
    stopped = asyncio.Event()
    failed, recovered = asyncio.Event(), asyncio.Event()
    actual_page = worker.page
    attempts = 0

    async def fail_once(database):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            # Connection failure in the actual snapshot execute; the transaction closes.
            def broken(conn, cursor, statement, parameters, context, many):
                if "writer_recovery_scan" in statement:
                    raise OperationalError(
                        "private SQL sentinel", {}, OSError("private DSN sentinel")
                    )

            event.listen(database.engine.sync_engine, "before_cursor_execute", broken)
            try:
                return await actual_page(database)
            finally:
                event.remove(database.engine.sync_engine, "before_cursor_execute", broken)
                failed.set()
        value = await actual_page(database)
        recovered.set()
        return value

    monkeypatch.setattr(worker, "page", fail_once)
    monkeypatch.setattr(worker, "RETRY_SECONDS", 0.1)
    task = asyncio.create_task(worker.drive(h.database, h.settings, stopped))
    await asyncio.wait_for(failed.wait(), 2)
    async with h.database.session_factory() as session:
        assert await session.get(WriterRecoveryScan, "text-v1") is None
    assert attempts == 1 and not task.done()  # outage does not hot-loop or terminate
    await asyncio.wait_for(recovered.wait(), 2)
    stopped.set()
    await asyncio.wait_for(task, 2)
    # Stop may prevent the recovered page's row from starting; retained evidence survives.
    stopped.clear()
    assert await worker.sweep(h.database, h.settings) in {0, 1}
    assert (await intent(h, registration["descriptor"])).writer_state == "RETIRED"


async def test_cancellation_of_real_lock_wait_releases_session_without_retirement(
    writer_harness,
    user_me_transport,
    sql,
):
    h = writer_harness
    registration = await positive(h)
    async with sql.begin() as blocker:
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(text("SELECT owner_id FROM inbox_grant_scopes FOR UPDATE"))
        task = asyncio.create_task(worker.sweep(h.database, h.settings))
        await observed_waiters(sql, pid, 1, "recovery driver cancellation")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert h.database.engine.pool.checkedout() == 0
    assert (await intent(h, registration["descriptor"])).writer_state == "LIVE"


async def test_total_outage_backoff_is_cancellable(monkeypatch):
    ready = asyncio.Event()
    calls = 0

    async def unavailable():
        nonlocal calls
        calls += 1
        ready.set()
        raise OSError("private connection string")

    stopped = asyncio.Event()
    task = asyncio.create_task(worker.drive(SimpleNamespace(ready=unavailable), None, stopped))
    await ready.wait()
    await asyncio.sleep(0)
    assert calls == 1 and not task.done()
    stopped.set()
    await asyncio.wait_for(task, 1)
    assert calls == 1


def test_unexpected_top_level_failure_has_finite_diagnostic_without_traceback(monkeypatch, capsys):
    async def broken():
        raise RuntimeError("SELECT secret_column FROM private_table; private credentials")

    monkeypatch.setattr(worker, "run", broken)
    assert worker.main() == 1
    lines = capsys.readouterr().err.splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["stage"] == "writer_recovery"
    assert payload["code"] == "WRITER_RECOVERY_STOPPED"
    assert "private" not in lines[0] and "Traceback" not in lines[0]


async def test_shutdown_starts_no_more_rows_and_unprocessed_page_remains_eligible(
    writer_harness,
    user_me_transport,
    monkeypatch,
):
    h = writer_harness
    registration = await positive(h)
    await seed_page(h, registration, [1, 2])
    stopped = asyncio.Event()
    seen = []

    async def stop_on_first(database, registration_id, settings):
        seen.append(registration_id)
        stopped.set()
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)

    monkeypatch.setattr(worker, "recovery", stop_on_first)
    assert await worker.sweep(h.database, h.settings, stopped) == 0
    assert seen == [UUID(int=1)]
    assert (await intent(h, registration["descriptor"])).writer_state == "LIVE"
    # No reservation/outcome was written by the skipped scan. A new driver wraps.
    from db.database import Database

    restarted = Database(h.settings.database_url.get_secret_value())
    try:
        assert await worker.page(restarted) == [UUID(int=i) for i in [1, 2, 1000]]
    finally:
        await restarted.dispose()
