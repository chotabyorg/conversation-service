import os
import subprocess
import sys
from uuid import uuid4

import pytest
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from tests.conftest import ROOT
from tests.test_database import violation
from tests.test_managed_execution import execute
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture
TABLES = (
    "conversations",
    "inbound_events",
    "messages",
    "agent_turns",
    "delivery_attempts",
    "delivery_reconciliations",
)
MARKERS = {
    "managed_agent_id",
    "managed_dispatch_epoch",
    "managed_dispatch_started_at",
    "managed_request_fingerprint",
}
HISTORY_COLUMNS = {
    "source_agent_turn_id", "managed_contract_version", "managed_context_first_sequence",
    "managed_projection_agent_id", "managed_projection_fingerprint",
}
SETTLING_COLUMNS = {
    "settling_revision", "settling_policy_version",
    "settling_quiet_window_ms", "settling_max_wait_ms",
    "expected_settling_revision", "fenced_settling_revision", "result_settling_revision",
}
PRE_GOAL5_COLUMNS = {
    table: HISTORY_COLUMNS | (SETTLING_COLUMNS if table in {
        "conversations", "messages", "agent_turns", "delivery_attempts",
        "delivery_reconciliations",
    } else set())
    for table in TABLES
}
PRE_GOAL5_COLUMNS["messages"] = PRE_GOAL5_COLUMNS["messages"] | {"input_revision"}


def migrate(database_url, command, revision, expected=0):
    env = os.environ.copy()
    env["DATABASE_URL"] = database_url
    result = subprocess.run(
        [sys.executable, "-m", "alembic", command, revision],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (result.returncode == 0) == (expected == 0), result.stderr.replace(
        database_url, "[REDACTED_DATABASE_URL]"
    )
    return result


async def rows(sql, exclude=()):
    result = {}
    async with sql.connect() as connection:
        for table in TABLES:
            excluded = exclude.get(table, set()) if isinstance(exclude, dict) else exclude
            result[table] = [
                {key: value for key, value in row.items() if key not in excluded}
                for row in (
                    await connection.execute(text(f"SELECT * FROM {table} ORDER BY id"))
                ).mappings()
            ]
    return result


async def test_populated_goal1_upgrade_preserves_every_journal_claim_fingerprint_and_replay(
    h, sql, database_url
):
    pending, _ = await h.select(await h.ingest(provider_thread_id="pending"))
    claimed, _ = await h.select(await h.ingest(provider_thread_id="claimed"))
    held = await h.claim(claimed)
    completed, _ = await h.select(await h.ingest(provider_thread_id="completed"))
    proof = await h.claim(completed)
    terminal = dict(
        h.proof(proof),
        outcome="DRAFT_READY",
        draft_text="retained Goal1 draft",
        operation_refs=[
            dict(origin="AI_AGENT_SERVICE", operation_id="old:ref", outcome="SUCCEEDED")
        ],
    )
    assert (await h.call("POST", f"/turns/{completed['id']}/complete", terminal)).status_code == 200
    failed, _ = await h.select(await h.ingest(provider_thread_id="failed"))
    failed_claim = await h.claim(failed)
    assert (
        await h.call(
            "POST",
            f"/turns/{failed['id']}/fail",
            dict(h.proof(failed_claim), failure_code="TIMEOUT"),
        )
    ).status_code == 200
    delivery = await h.call(
        "POST",
        f"/conversations/{completed['conversation_id']}/delivery-attempts",
        dict(
            delivery_key=str(uuid4()), source_turn_id=completed["id"], expected_authority_revision=0
        ),
    )
    attempt = delivery.json()["attempt"]
    attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    _, unknown = await h.reconcile(attempt, "UNKNOWN", error_code="TIMEOUT")
    original = await rows(sql, exclude=PRE_GOAL5_COLUMNS)
    # Capture real complete Goal1 values, then downgrade only the emptied designated DB.
    old_rows = {
        table: [{k: v for k, v in row.items()
                 if k not in PRE_GOAL5_COLUMNS[table]
                 and not (table == "agent_turns" and k in MARKERS)}
                for row in values]
        for table, values in original.items()
    }
    await h.database.dispose()
    async with sql.begin() as connection:
        await connection.execute(text("TRUNCATE " + ",".join(reversed(TABLES)) + " CASCADE"))
    migrate(database_url, "downgrade", "0001_foundation")
    # Recreate the captured populated0001 journal without using current ORM columns.
    async with sql.begin() as connection:
        for table, values in old_rows.items():
            for row in values:
                columns = list(row)
                statement = text(
                    f"INSERT INTO {table} ({','.join(columns)}) VALUES "
                    f"({','.join(':' + name for name in columns)})"
                )
                if "operation_refs" in row:
                    statement = statement.bindparams(bindparam("operation_refs", type_=JSONB))
                await connection.execute(statement, row)
    migrate(database_url, "upgrade", "head")
    assert await rows(sql, exclude=PRE_GOAL5_COLUMNS) == original
    assert (await h.call("GET", f"/turns/{pending['id']}")).json()["status"] == "PENDING"
    assert (await h.call("GET", f"/turns/{claimed['id']}")).json() == held["turn"]
    replay = await h.call("POST", f"/turns/{completed['id']}/complete", terminal)
    assert replay.status_code == 200 and replay.json()["replayed"]
    reconciliation = await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", unknown)
    assert reconciliation.status_code == 200 and reconciliation.json()["replayed"]
    assert await h.database.ready()
    assert not await h.database.ready("0001_foundation")


async def test_empty_disposable_downgrade_and_upgrade_have_no_drift(h, sql, database_url):
    assert not any((await rows(sql)).values())
    await h.database.dispose()
    migrate(database_url, "downgrade", "0001_foundation")
    async with sql.connect() as connection:
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0001_foundation"
        )
    migrate(database_url, "upgrade", "head")
    assert await h.database.ready()


@pytest.mark.parametrize("state", ["marker", "unknown_without_marker"])
async def test_downgrade_refuses_managed_evidence_without_erasing_it(
    h, managed, sql, database_url, state
):
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    if state == "marker":
        assert (await execute(h, turn, claim)).status_code == 200
    else:
        assert (
            await h.call(
                "POST",
                f"/turns/{turn['id']}/fail",
                dict(h.proof(claim), failure_code="EXECUTION_UNKNOWN"),
            )
        ).status_code == 200
    original = await rows(sql)
    result = migrate(database_url, "downgrade", "0001_foundation", expected=1)
    assert "Managed execution evidence prevents downgrade." in result.stderr
    assert await rows(sql) == original
    assert await h.database.ready()


@pytest.mark.parametrize(
    "override",
    [
        "managed_agent_id=NULL",
        "managed_dispatch_epoch=NULL",
        "managed_dispatch_started_at=NULL",
        "managed_request_fingerprint=NULL",
        "managed_dispatch_epoch=0",
        "managed_dispatch_epoch=claim_epoch+1",
        "managed_request_fingerprint='BAD'",
        "managed_dispatch_started_at=started_at-interval '1 microsecond'",
    ],
)
async def test_marker_null_shape_epoch_time_digest_checks_defend_raw_writers(
    h, managed, sql, override
):
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    assert (await execute(h, turn, claim)).status_code == 200
    await violation(
        sql,
        f"UPDATE agent_turns SET {override} WHERE id=:id",
        {"id": turn["id"]},
        "23514",
        "ck_turn_managed_marker",
    )


async def test_marker_cannot_exist_on_pending_and_exact_catalog_columns(h, sql):
    turn, _ = await h.select(await h.ingest())
    await violation(
        sql,
        "UPDATE agent_turns SET managed_agent_id=:agent,managed_dispatch_epoch=1,"
        "managed_dispatch_started_at=clock_timestamp(),managed_request_fingerprint=:digest "
        "WHERE id=:id",
        dict(id=turn["id"], agent=str(uuid4()), digest="a" * 64),
        "23514",
        "ck_turn_managed_marker",
    )
    async with sql.connect() as connection:
        columns = {
            row["column_name"]: (row["data_type"], row["is_nullable"], row["column_default"])
            for row in (
                await connection.execute(
                    text(
                        "SELECT column_name,data_type,is_nullable,column_default "
                        "FROM information_schema.columns WHERE table_name='agent_turns'"
                    )
                )
            ).mappings()
            if row["column_name"] in MARKERS
        }
        assert columns == {
            "managed_agent_id": ("uuid", "YES", None),
            "managed_dispatch_epoch": ("bigint", "YES", None),
            "managed_dispatch_started_at": ("timestamp with time zone", "YES", None),
            "managed_request_fingerprint": ("character", "YES", None),
        }
        constraint = await connection.scalar(
            text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conname='ck_turn_failure'"
            )
        )
        assert "EXECUTION_UNKNOWN" in constraint
