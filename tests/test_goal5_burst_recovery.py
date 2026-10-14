"""Goal5 burst recovery checks with deterministic PostgreSQL time and restarts."""

from __future__ import annotations

import multiprocessing
import queue
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

import services.settling as settling_service
from core.config import Settings, SettlingPolicy
from models import (
    AgentTurn,
    Conversation,
    Message,
    SettlingBurst,
    TelegramAdmission,
    TurnInputMessage,
)
from schemas.contracts import Channel
from services.settling import select_settled_turn
from services.telegram_worker import TelegramWorker
from tests.test_database import violation
from tests.test_settling import telegram_harness as telegram_harness_fixture
from tests.test_telegram import DeterministicManagedPort
from tests.test_telegram import admit as telegram_admit

telegram_harness = telegram_harness_fixture


def _settings(harness, *, version: str, quiet_ms: int, max_wait_ms: int,
              enabled: bool = True) -> Settings:
    policy = SettlingPolicy(
        version=version,
        channel=Channel.TELEGRAM,
        quiet_window_ms=quiet_ms,
        max_wait_ms=max_wait_ms,
    )
    return harness["settings"].model_copy(update={
        "turn_settling_policies": (policy,) if enabled else (),
    })


def _install(harness, settings: Settings) -> None:
    harness["settings"] = settings
    harness["app"].state.settings = settings


def _fixed_database_clock(monkeypatch, initial: datetime):
    value = [initial]

    async def fixed(_session):
        return value[0]

    monkeypatch.setattr(settling_service, "db_clock", fixed)
    return value


def _worker_settings(settings: Settings, database_url: str) -> dict[str, Any]:
    return {
        "database_url": database_url,
        "auth_principals": [{
            "principal_id": item.principal_id,
            "role": item.role,
            "token": item.token.get_secret_value(),
            "grants": [grant.model_dump(mode="json") for grant in item.grants],
        } for item in settings.auth_principals],
        "managed_ai_bindings": [{
            "owner_id": item.owner_id,
            "integration_id": str(item.integration_id),
            "channel": item.channel.value,
            "agent_id": str(item.agent_id),
            "service_token": item.service_token.get_secret_value(),
        } for item in settings.managed_ai_bindings],
        "managed_ai_base_url": settings.managed_ai_base_url,
        "telegram_cohort_integration_ids": [
            str(value) for value in settings.telegram_cohort_integration_ids
        ],
        "telegram_worker_enabled": True,
        "telegram_worker_principal_id": settings.telegram_worker_principal_id,
        "telegram_worker_lease_seconds": settings.telegram_worker_lease_seconds,
        "telegram_worker_poll_seconds": settings.telegram_worker_poll_seconds,
        "turn_settling_policies": [{
            "version": item.version,
            "channel": item.channel.value,
            "integration_id": str(item.integration_id) if item.integration_id else None,
            "quiet_window_ms": item.quiet_window_ms,
            "max_wait_ms": item.max_wait_ms,
        } for item in settings.turn_settling_policies],
    }


def _run_one_worker_in_fresh_process(database_url: str, data: dict[str, Any], output) -> None:
    async def run() -> tuple[bool, list[list[str]]]:
        from db.database import Database

        settings = Settings(**data, _env_file=None)
        database = Database(database_url)
        port = DeterministicManagedPort()
        try:
            worked = await TelegramWorker(database, settings, port).run_once()
            selected = [
                [str(message_id) for message_id in item.request.selected_input_message_ids]
                for item in port.prepared
            ]
            return worked, selected
        finally:
            await database.dispose()

    try:
        import asyncio

        output.put(asyncio.run(run()))
    except BaseException as error:
        output.put({"error": repr(error)})
        raise


def _fresh_worker_once(database_url: str, settings: Settings) -> list[str]:
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    process = context.Process(
        target=_run_one_worker_in_fresh_process,
        args=(database_url, _worker_settings(settings, database_url), output),
    )
    process.start()
    process.join(timeout=30)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("fresh Telegram worker process did not finish burst recovery")
    assert process.exitcode == 0
    try:
        result = output.get(timeout=2)
    except queue.Empty:
        pytest.fail("fresh Telegram worker process returned no result")
    assert not isinstance(result, dict), result
    worked, selected = result
    assert worked is True and len(selected) == 1
    return selected[0]


async def _admit(h, *, message_id: int, update_id: int, chat_id: int = 123,
                 text_value: str = "hello"):
    response = await telegram_admit(
        h["client"], h["gateway_headers"],
        message_id=message_id, update_id=update_id, chat_id=chat_id,
        text_value=text_value,
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["replace", "remove"])
async def test_open_burst_freezes_membership_and_original_policy(
    change, telegram_harness, sql, monkeypatch
):
    h = telegram_harness
    t0 = datetime(2030, 1, 1, tzinfo=UTC)
    clock = _fixed_database_clock(monkeypatch, t0)
    initial = _settings(h, version="policy-original", quiet_ms=1_000, max_wait_ms=5_000)
    _install(h, initial)
    first = await _admit(h, message_id=7, update_id=55)

    if change == "replace":
        changed = _settings(h, version="policy-replacement", quiet_ms=4_000,
                            max_wait_ms=12_000)
    else:
        changed = _settings(h, version="policy-removed", quiet_ms=4_000,
                            max_wait_ms=12_000, enabled=False)
    _install(h, changed)
    clock[0] = t0 + timedelta(milliseconds=500)
    second = await _admit(h, message_id=8, update_id=56)

    assert first["coordination_mode"] == second["coordination_mode"] == "SETTLING"
    async with h["database"].session_factory() as session, session.begin():
        admissions = [UUID(first["admission_id"]), UUID(second["admission_id"])]
        messages = list((await session.scalars(select(Message).where(
            Message.id.in_(select(TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(admissions)))
        ).order_by(Message.sequence))).all())
        bursts = list((await session.scalars(select(SettlingBurst).order_by(
            SettlingBurst.opened_at))).all())

    assert len(bursts) == 1
    burst = bursts[0]
    assert messages[0].settling_burst_id == messages[1].settling_burst_id == burst.id
    assert [(m.settling_policy_version, m.settling_quiet_window_ms,
             m.settling_max_wait_ms) for m in messages] == [
        ("policy-original", 1_000, 5_000), ("policy-original", 1_000, 5_000),
    ]
    assert burst.policy_version == "policy-original"
    assert burst.quiet_window_ms == 1_000 and burst.max_wait_ms == 5_000
    assert burst.opened_at == t0
    assert burst.max_cutoff_at == t0 + timedelta(milliseconds=5_000)
    assert burst.last_input_at == t0 + timedelta(milliseconds=500)
    assert burst.ready_at == t0 + timedelta(milliseconds=1_500)
    assert burst.closed_at is None


@pytest.mark.asyncio
async def test_exact_ready_boundary_starts_new_policy_or_immediate_input(
    telegram_harness, sql, monkeypatch
):
    h = telegram_harness
    t0 = datetime(2030, 1, 1, tzinfo=UTC)
    clock = _fixed_database_clock(monkeypatch, t0)
    _install(h, _settings(h, version="policy-v1", quiet_ms=1_000, max_wait_ms=5_000))
    await _admit(h, message_id=7, update_id=55)
    ready_at = t0 + timedelta(milliseconds=1_000)

    _install(h, _settings(h, version="policy-v2", quiet_ms=500, max_wait_ms=2_000))
    clock[0] = ready_at
    second = await _admit(h, message_id=8, update_id=56)
    assert second["coordination_mode"] == "SETTLING"

    # Equality is a boundary: it closes the old burst at its stored deadline.
    # The following input at equality to v2's deadline has no current policy,
    # so it receives the immediate mode bit while the old burst remains closed.
    second_ready_at = ready_at + timedelta(milliseconds=500)
    _install(h, _settings(h, version="policy-disabled", quiet_ms=500,
                          max_wait_ms=2_000, enabled=False))
    clock[0] = second_ready_at
    third = await _admit(h, message_id=9, update_id=57)
    assert third["coordination_mode"] == "IMMEDIATE"

    async with h["database"].session_factory() as session, session.begin():
        messages = list((await session.scalars(select(Message).order_by(
            Message.sequence))).all())
        bursts = list((await session.scalars(select(SettlingBurst).order_by(
            SettlingBurst.opened_at))).all())
    assert len(bursts) == 2
    first_burst, second_burst = bursts
    assert first_burst.id == messages[0].settling_burst_id
    assert first_burst.closed_at == first_burst.ready_at == ready_at
    assert first_burst.policy_version == "policy-v1"
    assert second_burst.id == messages[1].settling_burst_id
    assert second_burst.id != first_burst.id
    assert second_burst.opened_at == ready_at
    assert second_burst.policy_version == "policy-v2"
    assert second_burst.closed_at == second_burst.ready_at == second_ready_at
    assert messages[2].settling_burst_id is None
    assert messages[2].settling_policy_version is None


@pytest.mark.asyncio
async def test_600_member_burst_keeps_cutoff_and_residual_across_fresh_worker(
    telegram_harness, sql, database_url, monkeypatch
):
    h = telegram_harness
    async with sql.connect() as connection:
        db_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(db_now, datetime)
    t0 = db_now - timedelta(seconds=10)
    clock = _fixed_database_clock(monkeypatch, t0)
    original_settings = _settings(
        h, version="original-600", quiet_ms=5_000, max_wait_ms=20_000)
    _install(h, original_settings)

    receipts = []
    for index in range(600):
        receipts.append(await _admit(
            h, message_id=10_000 + index, update_id=20_000 + index))
    async with h["database"].session_factory() as session, session.begin():
        admission_ids = [UUID(row["admission_id"]) for row in receipts]
        message_ids = list((await session.scalars(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(admission_ids)
            ).order_by(TelegramAdmission.update_id))).all())
        conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(
                TelegramAdmission.id == admission_ids[0]))
        original_burst = await session.scalar(select(SettlingBurst).where(
            SettlingBurst.conversation_id == conversation_id,
            SettlingBurst.closed_at.is_(None),
        ))
    assert conversation_id is not None and original_burst is not None
    assert len(message_ids) == 600
    cutoff = original_burst.max_cutoff_at
    first_ready = original_burst.ready_at
    assert original_burst.tail_sequence == 600
    assert original_burst.policy_version == "original-600"
    assert cutoff == t0 + timedelta(milliseconds=20_000)

    # Admit the next member at exact equality. It must create another burst,
    # while the original 600 residual members retain their original identity.
    boundary_settings = _settings(
        h, version="next-burst", quiet_ms=86_400_000, max_wait_ms=86_400_000)
    _install(h, boundary_settings)
    clock[0] = first_ready
    next_receipt = await _admit(h, message_id=99_999, update_id=199_999)
    assert next_receipt["coordination_mode"] == "SETTLING"
    async with h["database"].session_factory() as session, session.begin():
        bursts = list((await session.scalars(select(SettlingBurst).where(
            SettlingBurst.conversation_id == conversation_id
        ).order_by(SettlingBurst.opened_at))).all())
        next_message_id = await session.scalar(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id == UUID(next_receipt["admission_id"])))
    assert len(bursts) == 2
    original_burst, next_burst = bursts
    assert original_burst.closed_at == first_ready
    assert original_burst.max_cutoff_at == cutoff
    assert next_burst.id != original_burst.id
    assert next_burst.opened_at == first_ready
    assert next_burst.policy_version == "next-burst"
    assert next_message_id is not None

    clock[0] = first_ready
    port = DeterministicManagedPort()
    first_worker = TelegramWorker(h["database"], boundary_settings, port)
    assert await first_worker.run_once() is True
    first_chunk = list(port.prepared[0].request.selected_input_message_ids)
    assert first_chunk == message_ids[:256]
    second_chunk = _fresh_worker_once(database_url, boundary_settings)
    assert second_chunk == [str(message_id) for message_id in message_ids[256:512]]
    third_worker = TelegramWorker(h["database"], boundary_settings, DeterministicManagedPort())
    assert await third_worker.run_once() is True
    async with h["database"].session_factory() as session, session.begin():
        turns = list((await session.scalars(select(AgentTurn).where(
            AgentTurn.conversation_id == conversation_id
        ).order_by(AgentTurn.input_first_sequence))).all())
        assignments = list((await session.scalars(select(Message.id).join(
            # Exact explicit ownership is what makes all residuals recoverable.
            TurnInputMessage,
            TurnInputMessage.message_id == Message.id,
        ).where(Message.conversation_id == conversation_id).order_by(
            Message.sequence))).all())
        current_bursts = list((await session.scalars(select(SettlingBurst).where(
            SettlingBurst.conversation_id == conversation_id
        ).order_by(SettlingBurst.opened_at))).all())
    assert len(turns) == 3
    assert [(turn.input_first_sequence, turn.input_last_sequence,
             turn.settling_burst_id, turn.input_revision) for turn in turns] == [
        (1, 256, original_burst.id, 601),
        (257, 512, original_burst.id, 601),
        (513, 600, original_burst.id, 601),
    ]
    assert turns[2].input_first_sequence == 513
    assert assignments == message_ids
    assert current_bursts[0].max_cutoff_at == cutoff
    assert current_bursts[0].closed_at == current_bursts[0].ready_at == first_ready
    assert current_bursts[1].id == next_burst.id and current_bursts[1].closed_at is None
    # Burst sequence is conversation-global; the first message in this new
    # burst follows the 600-message original burst.
    assert current_bursts[1].tail_sequence == 601
    # A later due-looking worker poll cannot pull the next burst into a residual.
    assert await TelegramWorker(
        h["database"], boundary_settings, DeterministicManagedPort()
    ).run_once() is False
    assert next_message_id not in assignments


@pytest.mark.asyncio
async def test_byte_limit_prefix_resumes_from_retained_burst_after_process_restart(
    telegram_harness, sql, database_url, monkeypatch
):
    h = telegram_harness
    async with sql.connect() as connection:
        db_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(db_now, datetime)
    t0 = db_now - timedelta(seconds=30)
    clock = _fixed_database_clock(monkeypatch, t0)
    settings = _settings(h, version="byte-residual", quiet_ms=10_000,
                         max_wait_ms=60_000)
    _install(h, settings)
    receipts = []
    for index in range(33):
        receipts.append(await _admit(
            h, message_id=30_000 + index, update_id=40_000 + index,
            text_value="x" * 16_384))
    async with h["database"].session_factory() as session, session.begin():
        admission_ids = [UUID(row["admission_id"]) for row in receipts]
        message_ids = list((await session.scalars(select(
            TelegramAdmission.canonical_message_id).where(
                TelegramAdmission.id.in_(admission_ids)
            ).order_by(TelegramAdmission.update_id))).all())
        burst = await session.scalar(select(SettlingBurst))
    assert burst is not None and len(message_ids) == 33
    cutoff = burst.max_cutoff_at
    clock[0] = burst.ready_at

    first_port = DeterministicManagedPort()
    assert await TelegramWorker(h["database"], settings, first_port).run_once() is True
    first = first_port.prepared[0].request
    assert first.selected_input_message_ids == tuple(message_ids[:32])
    assert sum(len(item.text.encode("utf-8")) for item in first.messages) == 524_288
    residual = _fresh_worker_once(database_url, settings)
    assert residual == [str(message_ids[32])]

    async with h["database"].session_factory() as session, session.begin():
        turns = list((await session.scalars(select(AgentTurn).order_by(
            AgentTurn.input_first_sequence))).all())
        owners = list((await session.scalars(select(TurnInputMessage).order_by(
            TurnInputMessage.sequence))).all())
        retained_burst = await session.get(SettlingBurst, burst.id)
    assert [(turn.input_first_sequence, turn.input_last_sequence,
             turn.settling_burst_id) for turn in turns] == [
        (1, 32, burst.id), (33, 33, burst.id),
    ]
    assert [owner.message_id for owner in owners] == message_ids
    assert retained_burst is not None
    assert retained_burst.max_cutoff_at == cutoff
    assert retained_burst.closed_at == retained_burst.ready_at == burst.ready_at


@pytest.mark.asyncio
async def test_invalid_candidate_prefix_of_131_isolated_rows_advances_to_valid_tail(
    telegram_harness, sql, monkeypatch
):
    h = telegram_harness
    async with sql.connect() as connection:
        db_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(db_now, datetime)
    base = db_now - timedelta(seconds=60)
    clock = _fixed_database_clock(monkeypatch, base)
    settings = _settings(h, version="candidate-isolation", quiet_ms=1_000,
                         max_wait_ms=60_000)
    _install(h, settings)

    invalid_candidates = []
    for index in range(131):
        clock[0] = base + timedelta(microseconds=index)
        receipt = await _admit(
            h, message_id=50_000 + index, update_id=60_000 + index,
            chat_id=70_000 + index)
        invalid_candidates.append(receipt)
    clock[0] = base + timedelta(seconds=2)
    valid = await _admit(h, message_id=51_000, update_id=61_000, chat_id=80_000)

    async with h["database"].session_factory() as session, session.begin():
        invalid_ids = [UUID(row["admission_id"]) for row in invalid_candidates]
        invalid_rows = list((await session.execute(select(
            TelegramAdmission.id,
            TelegramAdmission.conversation_id,
            TelegramAdmission.canonical_message_id,
            TelegramAdmission.agent_id,
            Conversation.input_revision,
            Conversation.settling_revision,
        ).join(Conversation, Conversation.id == TelegramAdmission.conversation_id).where(
            TelegramAdmission.id.in_(invalid_ids)
        ).order_by(TelegramAdmission.update_id))).all())
        valid_id = UUID(valid["admission_id"])
        valid_conversation_id = await session.scalar(select(
            TelegramAdmission.conversation_id).where(TelegramAdmission.id == valid_id))
        valid_message_id = await session.scalar(select(
            TelegramAdmission.canonical_message_id).where(TelegramAdmission.id == valid_id))
    assert len(invalid_rows) == 131
    assert valid_conversation_id is not None and valid_message_id is not None

    orphan_time = base + timedelta(milliseconds=500)
    async with sql.begin() as connection:
        for row in invalid_rows:
            burst_id = await connection.scalar(text(
                "SELECT settling_burst_id FROM messages WHERE id=:message_id"),
                {"message_id": row.canonical_message_id},
            )
            assert burst_id is not None
            await connection.execute(text(
                "UPDATE conversations SET last_message_sequence=2,input_revision=2,"
                "settling_revision=2 WHERE id=:id"), {"id": row.conversation_id})
            await connection.execute(text(
                "UPDATE settling_bursts SET last_input_at=:last_input,tail_sequence=2,"
                "ready_at=:ready WHERE id=:id"), {
                    "id": burst_id,
                    "last_input": orphan_time,
                    "ready": orphan_time + timedelta(milliseconds=1_000),
                })
            await connection.execute(text(
                "INSERT INTO messages(id,conversation_id,sequence,input_revision,"
                "settling_revision,settling_policy_version,settling_quiet_window_ms,"
                "settling_max_wait_ms,settling_burst_id,actor,direction,content_type,text) "
                "VALUES(:id,:conversation,2,2,2,'candidate-isolation',1000,60000,:burst,"
                "'CUSTOMER','INBOUND','TEXT','orphan malformed member')"), {
                    "id": uuid4(), "conversation": row.conversation_id, "burst": burst_id,
                })

    async with sql.connect() as connection:
        actual_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(actual_now, datetime)
    clock[0] = actual_now
    principal = next(item for item in settings.auth_principals
                     if item.principal_id == settings.telegram_worker_principal_id).principal()
    selected_id = await select_settled_turn(
        h["database"], principal, settings, (Channel.TELEGRAM,)
    )
    assert selected_id is not None
    async with h["database"].session_factory() as session, session.begin():
        selected = await session.get(AgentTurn, selected_id)
        owners = list((await session.scalars(select(TurnInputMessage))).all())
    assert selected is not None and selected.conversation_id == valid_conversation_id
    assert [owner.message_id for owner in owners] == [valid_message_id]


@pytest.mark.asyncio
async def test_schema_rejects_invalid_membership_and_frozen_burst_rewrites(
    telegram_harness, sql, monkeypatch
):
    h = telegram_harness
    async with sql.connect() as connection:
        db_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(db_now, datetime)
    t0 = db_now - timedelta(seconds=10)
    clock = _fixed_database_clock(monkeypatch, t0)
    settings = _settings(h, version="immutable-membership", quiet_ms=1_000,
                         max_wait_ms=5_000)
    _install(h, settings)
    await _admit(h, message_id=7, update_id=55)
    await _admit(h, message_id=8, update_id=56)
    async with h["database"].session_factory() as session, session.begin():
        conversation = await session.scalar(select(Conversation))
        messages = list((await session.scalars(select(Message).order_by(
            Message.sequence))).all())
        burst = await session.scalar(select(SettlingBurst))
    assert conversation is not None and burst is not None and len(messages) == 2

    await violation(
        sql,
        "UPDATE messages SET settling_policy_version='rewritten' WHERE id=:id",
        {"id": messages[0].id}, "23514",
    )
    await violation(
        sql,
        "UPDATE settling_bursts SET max_cutoff_at=max_cutoff_at + interval '1 second' "
        "WHERE id=:id",
        {"id": burst.id}, "23514",
    )
    await violation(
        sql,
        "INSERT INTO messages(id,conversation_id,sequence,settling_burst_id,"
        "settling_policy_version,settling_quiet_window_ms,settling_max_wait_ms,actor,"
        "direction,content_type,text) VALUES(:id,:conversation,3,:burst,:policy,1000,5000,"
        "'SYSTEM','OUTBOUND','TEXT','invalid mode')",
        {"id": uuid4(), "conversation": conversation.id, "burst": burst.id,
         "policy": burst.policy_version}, "23514", "ck_message_settling_burst_shape",
    )

    clock[0] = burst.ready_at
    worker = TelegramWorker(h["database"], settings, DeterministicManagedPort())
    assert await worker.run_once() is True
    async with h["database"].session_factory() as session, session.begin():
        turn = await session.scalar(select(AgentTurn))
    assert turn is not None and turn.settling_burst_id == burst.id
    await violation(
        sql,
        "UPDATE agent_turns SET settling_burst_id=NULL WHERE id=:id",
        {"id": turn.id}, "23514",
    )


@pytest.mark.asyncio
async def test_second_turn_input_insert_failure_rolls_back_entire_prefix(
    telegram_harness, sql, monkeypatch
):
    h = telegram_harness
    async with sql.connect() as connection:
        db_now = await connection.scalar(text("SELECT clock_timestamp()"))
    assert isinstance(db_now, datetime)
    t0 = db_now - timedelta(seconds=10)
    clock = _fixed_database_clock(monkeypatch, t0)
    settings = _settings(h, version="atomic-prefix", quiet_ms=1_000, max_wait_ms=5_000)
    _install(h, settings)
    await _admit(h, message_id=7, update_id=55)
    await _admit(h, message_id=8, update_id=56)
    async with h["database"].session_factory() as session, session.begin():
        conversation_id = await session.scalar(select(Conversation.id))
    assert conversation_id is not None
    trigger_name = "goal5_fail_second_burst_owner"
    function_name = "goal5_fail_second_burst_owner_fn"
    async with sql.begin() as connection:
        await connection.execute(text(f"""
            CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.conversation_id = '{conversation_id}'::uuid AND NEW.ordinal = 1 THEN
                RAISE EXCEPTION 'injected second burst ownership insert failure'
                  USING ERRCODE='23514';
              END IF;
              RETURN NEW;
            END $$
        """))
        await connection.execute(text(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON turn_input_messages "
            f"FOR EACH ROW EXECUTE FUNCTION {function_name}()"))
    try:
        clock[0] = t0 + timedelta(milliseconds=1_000)
        with pytest.raises(IntegrityError):
            await select_settled_turn(
                h["database"],
                next(item for item in settings.auth_principals
                     if item.principal_id == settings.telegram_worker_principal_id).principal(),
                settings,
                (Channel.TELEGRAM,),
            )
    finally:
        async with sql.begin() as connection:
            await connection.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name} "
                                           "ON turn_input_messages"))
            await connection.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))

    async with h["database"].session_factory() as session, session.begin():
        assert await session.scalar(select(AgentTurn.id)) is None
        assert await session.scalar(text("SELECT count(*) FROM turn_input_messages")) == 0
