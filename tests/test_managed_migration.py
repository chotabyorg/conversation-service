import asyncio
import io
import json
import os
import socket
import subprocess
import sys
import tarfile
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB

from tests.conftest import ROOT
from tests.test_database import violation
from tests.test_managed_execution import execute
from tests.test_managed_execution import managed as managed_fixture
from tests.test_telegram import (
    DeterministicManagedPort,
)
from tests.test_telegram import (
    inbound as telegram_inbound,
)
from tests.test_telegram import (
    telegram_harness as telegram_fixture,
)
from tests.test_website import (
    BASE as WEBSITE_BASE,
)
from tests.test_website import (
    admit as website_admit,
)
from tests.test_website import (
    identity as website_identity,
)
from tests.test_website import (
    payload as website_payload,
)
from tests.test_website import (
    v2_response_attempt_id as website_v2_response_attempt_id,
)
from tests.test_website import (
    wh as website_fixture,
)

managed = managed_fixture
telegram_harness = telegram_fixture
wh = website_fixture
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
    "source_agent_turn_id",
    "managed_contract_version",
    "managed_context_first_sequence",
    "managed_projection_agent_id",
    "managed_projection_fingerprint",
}
SETTLING_COLUMNS = {
    "settling_revision",
    "settling_burst_id",
    "settling_policy_version",
    "settling_quiet_window_ms",
    "settling_max_wait_ms",
    "expected_settling_revision",
    "fenced_settling_revision",
    "result_settling_revision",
}
PRE_GOAL5_COLUMNS = {
    table: HISTORY_COLUMNS
    | (
        SETTLING_COLUMNS
        if table
        in {
            "conversations",
            "messages",
            "agent_turns",
            "delivery_attempts",
            "delivery_reconciliations",
        }
        else set()
    )
    for table in TABLES
}
PRE_GOAL5_COLUMNS["messages"] = PRE_GOAL5_COLUMNS["messages"] | {"input_revision"}
# Authority floors were introduced in 0008 and must be omitted both from the
# populated-0001 fixture projection and from cross-revision equality checks.
PRE_GOAL5_COLUMNS["conversations"] = PRE_GOAL5_COLUMNS["conversations"] | {
    "authority_input_floor_sequence",
    "handoff_resume_floor_sequence",
}
# Partial draft text was introduced in 0009 and must be omitted both from the
# populated-0001 fixture projection and from cross-revision equality checks.
PRE_GOAL5_COLUMNS["agent_turns"] = PRE_GOAL5_COLUMNS["agent_turns"] | {
    "partial_draft_text",
    "catch_up_version",
}
# The per-lease delivery ordinal arrived in 0009 and is likewise pre-Goal5.
PRE_GOAL5_COLUMNS["delivery_attempts"] = PRE_GOAL5_COLUMNS["delivery_attempts"] | {"ordinal"}
PRE_GOAL5_COLUMNS["messages"] |= {
    "source_agent_turn_id",
}
PRE_GOAL5_COLUMNS["agent_turns"] |= {
    "settling_revision",
    "settling_burst_id",
    "retired_at",
    "managed_v3_snapshot",
    "managed_v3_request_fingerprint",
    "managed_v3_dispatch_id",
}
PRE_GOAL5_COLUMNS["delivery_attempts"] |= {
    "source_message_id",
    "outbound_ordinal",
    "case_id",
    "case_revision",
    "assignment_revision",
    "grant_generation",
    "writer_state",
    "writer_supplier_instance_id",
    "writer_process_incarnation_id",
    "writer_started_at",
    "writer_ended_at",
}
PRE_GOAL5_COLUMNS["delivery_reconciliations"] |= {
    "expected_settling_revision",
    "result_settling_revision",
}
GOAL5_TABLES = (
    *TABLES,
    "turn_input_messages",
    "website_response_attempts",
    "telegram_response_attempts",
    "website_immediate_turns",
    "telegram_immediate_turns",
)
GOAL4B_CONVERSATION_SHA = "7cafa8ff6c64047c6d5f8e00ddf6a5f451c42142"


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


async def goal5_rows(sql):
    result = {}
    async with sql.connect() as connection:
        for table in GOAL5_TABLES:
            primary_key = (
                "message_id"
                if table
                in {
                    "turn_input_messages",
                    "website_immediate_turns",
                    "telegram_immediate_turns",
                }
                else "id"
            )
            result[table] = [
                dict(row)
                for row in (
                    await connection.execute(text(f"SELECT * FROM {table} ORDER BY {primary_key}"))
                ).mappings()
            ]
    return result


GOAL4B_TABLES = (
    *TABLES,
    "website_admissions",
    "telegram_admissions",
    "website_response_attempts",
    "telegram_response_attempts",
    "website_immediate_turns",
    "telegram_immediate_turns",
)


async def goal4b_rows(sql):
    """Project retained facts onto the exact 0005 schema before comparing an upgrade."""
    result = {}
    omitted = {table: set() for table in GOAL4B_TABLES}
    omitted["conversations"].update(
        {
            "settling_revision",
            "authority_input_floor_sequence",
            "handoff_resume_floor_sequence",
        }
    )
    omitted["messages"].update(
        {
            "input_revision",
            "settling_revision",
            "settling_policy_version",
            "settling_quiet_window_ms",
            "settling_max_wait_ms",
            "settling_burst_id",
        }
    )
    omitted["agent_turns"].update({"partial_draft_text", "catch_up_version"})
    omitted["delivery_attempts"].add("ordinal")
    omitted["agent_turns"].update(
        {
            "settling_revision",
            "settling_burst_id",
            "retired_at",
            "managed_v3_snapshot",
            "managed_v3_request_fingerprint",
            "managed_v3_dispatch_id",
        }
    )
    omitted["delivery_attempts"].update(
        {
            "expected_settling_revision",
            "fenced_settling_revision",
            "source_message_id",
            "outbound_ordinal",
            "case_id",
            "case_revision",
            "assignment_revision",
            "grant_generation",
            "writer_state",
            "writer_supplier_instance_id",
            "writer_process_incarnation_id",
            "writer_started_at",
            "writer_ended_at",
        }
    )
    omitted["delivery_reconciliations"].update(
        {
            "expected_settling_revision",
            "result_settling_revision",
        }
    )
    omitted["website_response_attempts"].add("send_authorized")
    omitted["telegram_response_attempts"].add("send_authorized")
    primary_keys = {
        "website_immediate_turns": "message_id",
        "telegram_immediate_turns": "message_id",
    }
    async with sql.connect() as connection:
        for table in GOAL4B_TABLES:
            primary_key = primary_keys.get(table, "id")
            result[table] = [
                {key: value for key, value in row.items() if key not in omitted[table]}
                for row in (
                    await connection.execute(text(f"SELECT * FROM {table} ORDER BY {primary_key}"))
                ).mappings()
            ]
    return result


async def assert_downgrade_refused(h, sql, database_url, expected_reason):
    original = await goal5_rows(sql)
    result = migrate(database_url, "downgrade", "0005_telegram_canary", expected=1)
    assert expected_reason in result.stderr
    assert await goal5_rows(sql) == original
    assert await h.database.ready()


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
        table: [
            {
                k: v
                for k, v in row.items()
                if k not in PRE_GOAL5_COLUMNS[table]
                and not (table == "agent_turns" and k in MARKERS)
            }
            for row in values
        ]
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


async def test_goal5_policy_snapshot_blocks_downgrade_without_losing_schema_or_data(
    h, sql, database_url
):
    accepted = await h.ingest()
    burst_id = uuid4()
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO settling_bursts "
                "(id,conversation_id,policy_version,quiet_window_ms,max_wait_ms,opened_at,"
                "max_cutoff_at,last_input_at,tail_sequence,ready_at) "
                "SELECT :id,:conversation,'migration-policy',250,1500,clock.opened,"
                "clock.opened+interval '1500 milliseconds',clock.opened,2,"
                "clock.opened+interval '250 milliseconds' FROM "
                "(SELECT clock_timestamp() AS opened) AS clock"
            ),
            {"id": str(burst_id), "conversation": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text(
                "INSERT INTO messages "
                "(id,conversation_id,sequence,input_revision,settling_revision,"
                "settling_policy_version,settling_quiet_window_ms,settling_max_wait_ms,"
                "settling_burst_id,actor,direction,content_type,text,created_at) "
                "VALUES (:id,:conversation,2,2,0,'migration-policy',250,1500,:burst_id,"
                "'CUSTOMER','INBOUND','TEXT','retained settling snapshot',clock_timestamp())"
            ),
            {
                "id": str(uuid4()),
                "conversation": accepted["conversation"]["id"],
                "burst_id": str(burst_id),
            },
        )
        await connection.execute(
            text(
                "UPDATE conversations SET last_message_sequence=2,input_revision=2,"
                "settling_revision=0 WHERE id=:id"
            ),
            {"id": accepted["conversation"]["id"]},
        )
    await assert_downgrade_refused(
        h, sql, database_url, "Retained Message settling snapshots prevent downgrade."
    )


async def test_goal5_superseded_delivery_blocks_downgrade_without_losing_evidence(
    h, sql, database_url
):
    accepted, attempt, _ = await h.delivery()
    async with sql.begin() as connection:
        await connection.execute(
            text("UPDATE conversations SET settling_revision=0 WHERE id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text("UPDATE messages SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text("UPDATE agent_turns SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text(
                "UPDATE delivery_attempts SET status='SUPERSEDED',revision=1,"
                "dispatch_requested_at=clock_timestamp() WHERE id=:id"
            ),
            {"id": attempt["id"]},
        )
    await assert_downgrade_refused(
        h, sql, database_url, "Retained supersede evidence prevents downgrade."
    )


async def test_goal5_no_policy_delivery_fence_blocks_downgrade(h, sql, database_url):
    accepted, attempt, _ = await h.delivery()
    _, dispatch = await h.reconcile(
        attempt,
        "DISPATCH_REQUESTED",
        expected_settling_revision=1,
        result_settling_revision=1,
    )
    assert dispatch["expected_settling_revision"] == 1
    async with sql.begin() as connection:
        await connection.execute(
            text("UPDATE conversations SET settling_revision=0 WHERE id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text("UPDATE messages SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
        await connection.execute(
            text("UPDATE agent_turns SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
    await assert_downgrade_refused(
        h, sql, database_url, "Retained delivery settling fences prevent downgrade."
    )


async def test_no_policy_inbound_settling_revision_blocks_downgrade(h, sql, database_url):
    accepted = await h.ingest()
    async with sql.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT settling_policy_version FROM messages WHERE id=:id"),
                {"id": accepted["message"]["id"]},
            )
            is None
        )
        assert (
            await connection.scalar(
                text("SELECT settling_revision FROM conversations WHERE id=:id"),
                {"id": accepted["conversation"]["id"]},
            )
            > 0
        )
    await assert_downgrade_refused(
        h, sql, database_url, "Conversation settling revisions prevent downgrade."
    )


async def test_denied_response_send_permission_blocks_downgrade(h, sql, database_url):
    accepted, attempt, _ = await h.delivery()
    admission_id = uuid4()
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO telegram_admissions "
                "(id,owner_id,integration_id,agent_id,channel,provider_namespace,"
                "provider_thread_id,provider_event_id,update_id,chat_id,message_id,sender_id,bot_id,"
                "fingerprint,route,conversation_id,canonical_message_id) "
                "VALUES (:id,'owner-a',:integration,:agent,'TELEGRAM','main-chota.telegram.v1',"
                "'thread','event',1,'1','1','1','1',repeat('a',64),'MANAGED',:conversation,:message)"
            ),
            {
                "id": admission_id,
                "integration": h.grants["a"]["integration_id"],
                "agent": uuid4(),
                "conversation": accepted["conversation"]["id"],
                "message": accepted["message"]["id"],
            },
        )
        await connection.execute(
            text(
                "INSERT INTO telegram_response_attempts "
                "(id,admission_id,delivery_id,conversation_id,send_authorized) "
                "VALUES (:id,:admission,:delivery,:conversation,false)"
            ),
            {
                "id": uuid4(),
                "admission": admission_id,
                "delivery": attempt["id"],
                "conversation": attempt["conversation_id"],
            },
        )
        await connection.execute(
            text("UPDATE conversations SET settling_revision=0 WHERE id=:id"),
            {"id": attempt["conversation_id"]},
        )
        await connection.execute(
            text("UPDATE messages SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": attempt["conversation_id"]},
        )
        await connection.execute(
            text("UPDATE agent_turns SET settling_revision=0 WHERE conversation_id=:id"),
            {"id": attempt["conversation_id"]},
        )
    await assert_downgrade_refused(
        h, sql, database_url, "Retained response send-permission decisions prevent downgrade."
    )


@pytest.mark.asyncio
async def test_actual_goal4b_reconciliation_replays_after_0005_to_0006(
    h, sql, database_url, network_allowlist, tmp_path
):
    accepted, attempt, _ = await h.delivery()
    # Remove reconstructible Goal5 freshness values so the retained row represents
    # a real 0005 baseline before the old service performs its reconciliation.
    async with sql.begin() as connection:
        await connection.execute(text("UPDATE conversations SET settling_revision=0"))
        await connection.execute(text("UPDATE messages SET settling_revision=0"))
        await connection.execute(text("UPDATE agent_turns SET settling_revision=0"))
    await h.database.dispose()

    # CI checks out only this repository. Fetch full history in the Quality
    # workflow so the accepted Goal4B source remains available by exact SHA.
    archive = subprocess.run(
        ["git", "archive", GOAL4B_CONVERSATION_SHA],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout
    old_source = tmp_path / "conversation-goal4b"
    old_source.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as source_archive:
        source_archive.extractall(old_source, filter="data")

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = int(listener.getsockname()[1])
    listener.close()
    network_allowlist.add(("127.0.0.1", port))
    principals = [
        {
            "principal_id": "goal4b-legacy-replay",
            "token": h.tokens["a"],
            "grants": [h.grants["a"]],
        }
    ]
    env = {
        **os.environ,
        "DATABASE_URL": database_url,
        "CONVERSATION_AUTH_PRINCIPALS": json.dumps(principals),
        "PYTHONPATH": str(old_source),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    # Reuse the test interpreter and dependencies; the archived source is still
    # the application under test through cwd and PYTHONPATH.
    old_python = sys.executable
    process = None
    downgraded = False
    old_body = {
        "reconciliation_key": str(uuid4()),
        "expected_revision": attempt["revision"],
        "status": "DISPATCH_REQUESTED",
        "expected_authority_revision": attempt["authority_revision"],
    }
    try:
        migrate(database_url, "downgrade", "0005_telegram_canary")
        downgraded = True
        process = subprocess.Popen(
            [
                old_python,
                "-m",
                "uvicorn",
                "main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
                "--no-access-log",
                "--log-level",
                "critical",
            ],
            cwd=old_source,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        async with httpx.AsyncClient(timeout=5, trust_env=False) as old_client:
            deadline = asyncio.get_running_loop().time() + 8
            while asyncio.get_running_loop().time() < deadline:
                if process.poll() is not None:
                    pytest.fail("Goal4B source process exited before reconciliation replay")
                try:
                    ready = await old_client.get(f"http://127.0.0.1:{port}/health")
                    if ready.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05)
            else:
                pytest.fail("Goal4B source process did not become ready")

            response = await old_client.post(
                f"http://127.0.0.1:{port}/internal/v1/delivery-attempts/{attempt['id']}/reconcile",
                json=old_body,
                headers={"X-Service-Token": h.tokens["a"]},
            )
            assert response.status_code == 200, response.text
            assert response.json()["replayed"] is False
        process.terminate()
        process.wait(timeout=8)
        process = None

        async with sql.connect() as connection:
            old_reconciliation = (
                await connection.execute(
                    text(
                        "SELECT id,fingerprint,result_revision,status "
                        "FROM delivery_reconciliations "
                        "WHERE delivery_attempt_id=:id"
                    ),
                    {"id": attempt["id"]},
                )
            ).one()
        migrate(database_url, "upgrade", "head")
        downgraded = False
        replay = await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", old_body)
        assert replay.status_code == 200, replay.text
        assert replay.json()["replayed"] is True
        async with sql.connect() as connection:
            retained = (
                await connection.execute(
                    text(
                        "SELECT id,fingerprint,result_revision,status "
                        "FROM delivery_reconciliations "
                        "WHERE delivery_attempt_id=:id"
                    ),
                    {"id": attempt["id"]},
                )
            ).all()
        assert retained == [old_reconciliation]
        assert await h.database.ready()
    finally:
        if process is not None:
            process.terminate()
            process.wait(timeout=8)
        if downgraded:
            migrate(database_url, "upgrade", "head")


@pytest.mark.asyncio
async def test_goal4b_retained_state_matrix_upgrades_without_rewriting_history(
    h, wh, telegram_harness, sql, database_url
):
    # Website managed draft with a provider-confirmed write and canonical AGENT row.
    website_value = website_payload(wh, session_id=str(uuid4()), message_id=str(uuid4()))
    assert (await website_admit(wh, website_value))["route"] == "MANAGED"
    assert await wh.worker.run_once()
    website_begin = dict(
        website_identity(website_value),
        response_attempt_id=await website_v2_response_attempt_id(wh, website_value),
    )
    begun = await wh.call("POST", WEBSITE_BASE + "/responses/begin", website_begin)
    assert begun.status_code == 200 and begun.json()["send"] is True
    written = await wh.call(
        "POST", WEBSITE_BASE + "/responses/complete", dict(website_begin, outcome="WRITE_COMPLETED")
    )
    assert written.status_code == 200

    # Goal4B Telegram managed admission and provider-accepted response.
    tg = telegram_harness
    tg_value = telegram_inbound(message_id=701, update_id=801, chat_id=901)
    tg_admission = await tg["client"].post(
        "/internal/v1/telegram/admissions", json=tg_value, headers=tg["gateway_headers"]
    )
    assert tg_admission.status_code == 200
    from services.telegram_worker import TelegramWorker

    tg_worker = TelegramWorker(tg["database"], tg["settings"], DeterministicManagedPort())
    assert await tg_worker.run_once()
    from services.telegram import expected_response_attempt_id

    tg_identity = {
        key: tg_value[key]
        for key in (
            "owner_id",
            "integration_id",
            "agent_id",
            "provider_thread_id",
            "provider_event_id",
        )
    }
    tg_begin = dict(
        tg_identity,
        response_attempt_id=str(
            expected_response_attempt_id(UUID(tg_admission.json()["admission_id"]))
        ),
    )
    tg_begun = await tg["client"].post(
        "/internal/v1/telegram/responses/begin",
        json=tg_begin,
        headers=tg["gateway_headers"],
    )
    assert tg_begun.status_code == 200 and tg_begun.json()["send"] is True
    tg_written = await tg["client"].post(
        "/internal/v1/telegram/responses/complete",
        json=dict(tg_begin, outcome="PROVIDER_ACCEPTED", provider_message_id="901"),
        headers=tg["gateway_headers"],
    )
    assert tg_written.status_code == 200

    # Goal4B-era generic rows cover all retained turn lifecycle states.
    pending, _ = await h.select(await h.ingest(provider_thread_id="matrix-pending"))
    assert pending["status"] == "PENDING"
    claimed, _ = await h.select(await h.ingest(provider_thread_id="matrix-claimed"))
    assert await h.claim(claimed)
    await h.draft(await h.ingest(provider_thread_id="matrix-completed"))
    failed, _ = await h.select(await h.ingest(provider_thread_id="matrix-failed"))
    failed_claim = await h.claim(failed)
    failure = await h.call(
        "POST", f"/turns/{failed['id']}/fail", dict(h.proof(failed_claim), failure_code="TIMEOUT")
    )
    assert failure.status_code == 200

    # Keep one real old-shape reconciliation for replay after the schema upgrade.
    async def create_delivery(thread: str):
        accepted = await h.ingest(provider_thread_id=thread)
        _, turn = await h.draft(accepted)
        response = await h.call(
            "POST",
            f"/conversations/{accepted['conversation']['id']}/delivery-attempts",
            dict(
                delivery_key=str(uuid4()), source_turn_id=turn["id"], expected_authority_revision=0
            ),
        )
        assert response.status_code == 201, response.text
        return response.json()["attempt"]

    unknown_attempt = await create_delivery("matrix-delivery-unknown")
    unknown_attempt, _ = await h.reconcile(unknown_attempt, "DISPATCH_REQUESTED")
    unknown_attempt, legacy_replay = await h.reconcile(
        unknown_attempt, "UNKNOWN", error_code="TIMEOUT"
    )
    accepted_attempt = await create_delivery("matrix-delivery-accepted")
    accepted_attempt, _ = await h.reconcile(accepted_attempt, "DISPATCH_REQUESTED")
    await h.reconcile(accepted_attempt, "PROVIDER_ACCEPTED", provider_message_id="902")

    # Add a historical overlapping range that a 0005 database could contain.
    overlap_accepted, overlap_turn = await h.draft(
        await h.ingest(provider_thread_id="matrix-overlap")
    )
    overlap_message_id = UUID(overlap_accepted["message"]["id"])
    duplicate_turn_id = uuid4()
    async with sql.begin() as connection:
        await connection.execute(
            text("""
            INSERT INTO agent_turns (
              id,conversation_id,selection_key,creation_fingerprint,
              input_first_sequence,input_last_sequence,context_last_sequence,
              input_revision,settling_revision,authority_revision,status,claim_epoch,
              claim_token_digest,worker_ref,lease_expires_at,created_at,started_at,
              completed_at,outcome,draft_text,failure_code,operation_refs,terminal_fingerprint,
              managed_contract_version,managed_context_first_sequence
            )
            SELECT :id,conversation_id,:selection,:fingerprint,
                   input_first_sequence,input_last_sequence,context_last_sequence,
                   input_revision,0,authority_revision,status,claim_epoch,
                   claim_token_digest,worker_ref,lease_expires_at,created_at,started_at,
                   completed_at,outcome,draft_text,failure_code,operation_refs,terminal_fingerprint,
                   managed_contract_version,managed_context_first_sequence
            FROM agent_turns WHERE id=:source
        """),
            {
                "id": duplicate_turn_id,
                "selection": uuid4(),
                "fingerprint": "a" * 64,
                "source": overlap_turn["id"],
            },
        )
        await connection.execute(
            text("DELETE FROM turn_input_messages WHERE message_id=:id"),
            {"id": overlap_message_id},
        )

    # Model the fields that did not exist in 0005, keeping the prior input order.
    async with sql.begin() as connection:
        await connection.execute(text("UPDATE conversations SET settling_revision=0"))
        await connection.execute(
            text("""
            WITH ranked AS (
              SELECT id,row_number() OVER (PARTITION BY conversation_id ORDER BY sequence) AS rev
              FROM messages WHERE actor='CUSTOMER' AND direction='INBOUND'
            )
            UPDATE messages m SET input_revision=r.rev,settling_revision=0,
              settling_policy_version=NULL,settling_quiet_window_ms=NULL,
              settling_max_wait_ms=NULL FROM ranked r WHERE r.id=m.id
        """)
        )
        await connection.execute(
            text("""
            UPDATE messages SET input_revision=NULL,settling_revision=NULL,
              settling_policy_version=NULL,settling_quiet_window_ms=NULL,
              settling_max_wait_ms=NULL
            WHERE actor<>'CUSTOMER' OR direction<>'INBOUND'
        """)
        )
        # A Goal4B fixture has no settling revision; temporarily remove the
        # Goal5 projection trigger while restoring that 0005-only default.
        await connection.execute(text("DROP TRIGGER preserve_website_projection ON agent_turns"))
        await connection.execute(text("UPDATE agent_turns SET settling_revision=0"))
        await connection.execute(
            text(
                "CREATE TRIGGER preserve_website_projection BEFORE UPDATE ON agent_turns "
                "FOR EACH ROW EXECUTE FUNCTION preserve_website_projection()"
            )
        )
        await connection.execute(
            text(
                "UPDATE delivery_attempts SET expected_settling_revision=NULL,"
                "fenced_settling_revision=NULL"
            )
        )
        await connection.execute(
            text(
                "UPDATE delivery_reconciliations SET expected_settling_revision=NULL,"
                "result_settling_revision=NULL"
            )
        )

    retained_0005 = await goal4b_rows(sql)
    await h.database.dispose()
    await wh.database.dispose()
    await tg["database"].dispose()
    migrate(database_url, "downgrade", "0005_telegram_canary")
    assert await goal4b_rows(sql) == retained_0005
    migrate(database_url, "upgrade", "head")
    assert await goal4b_rows(sql) == retained_0005

    async with sql.connect() as connection:
        statuses = set((await connection.scalars(text("SELECT status FROM agent_turns"))).all())
        assert {"PENDING", "CLAIMED", "COMPLETED", "FAILED"} <= statuses
        # Goal4B Website keeps transport status UNKNOWN while retaining a
        # separate WRITE_COMPLETED response fact; the generic row is also UNKNOWN.
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM delivery_attempts WHERE status='UNKNOWN'")
            )
            == 2
        )
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM delivery_attempts WHERE status='PROVIDER_ACCEPTED'")
            )
            >= 2
        )
        assert (
            await connection.scalar(
                text(
                    "SELECT count(*) FROM messages WHERE actor='AGENT' "
                    "AND source_agent_turn_id IS NOT NULL"
                )
            )
            >= 2
        )
        assert (
            await connection.scalar(
                text("SELECT count(*) FROM turn_input_messages WHERE message_id=:id"),
                {"id": overlap_message_id},
            )
            == 0
        )
        assert (
            await connection.scalar(text("SELECT count(*) FROM turn_input_messages"))
            == await connection.scalar(
                text("SELECT count(*) FROM messages WHERE actor='CUSTOMER' AND direction='INBOUND'")
            )
            - 1
        )

    replay = await h.call(
        "POST", f"/delivery-attempts/{unknown_attempt['id']}/reconcile", legacy_replay
    )
    assert replay.status_code == 200 and replay.json()["replayed"] is True
    assert await h.database.ready()


@pytest.mark.asyncio
async def test_goal5_reconciliation_fingerprint_replay_is_stable(h, sql):
    accepted, attempt, _ = await h.delivery()
    async with sql.connect() as connection:
        settling_revision = await connection.scalar(
            text("SELECT settling_revision FROM conversations WHERE id=:id"),
            {"id": accepted["conversation"]["id"]},
        )
    body = {
        "reconciliation_key": str(uuid4()),
        "expected_revision": attempt["revision"],
        "status": "DISPATCH_REQUESTED",
        "expected_authority_revision": attempt["authority_revision"],
        "expected_settling_revision": settling_revision,
        "result_settling_revision": settling_revision,
    }
    first = await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", body)
    assert first.status_code == 200 and first.json()["replayed"] is False
    replay = await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", body)
    assert replay.status_code == 200 and replay.json()["replayed"] is True
    async with sql.connect() as connection:
        retained = (
            await connection.execute(
                text(
                    "SELECT fingerprint,expected_settling_revision,result_settling_revision "
                    "FROM delivery_reconciliations WHERE delivery_attempt_id=:id"
                ),
                {"id": attempt["id"]},
            )
        ).all()
    assert len(retained) == 1
    assert retained[0][1:] == (settling_revision, settling_revision)


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
    async with sql.begin() as connection:
        await connection.execute(text("UPDATE conversations SET settling_revision=0"))
        await connection.execute(text("UPDATE messages SET settling_revision=0"))
        await connection.execute(text("UPDATE agent_turns SET settling_revision=0"))
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


async def test_0008_backfills_and_retains_nonzero_authority_input_floor(h, sql, database_url):
    accepted = await h.ingest(provider_thread_id="authority-floor-backfill")
    await h.ingest(provider_thread_id="authority-floor-backfill", text="later input")
    conversation_id = accepted["conversation"]["id"]
    await h.database.dispose()
    migrate(database_url, "downgrade", "0007_voice_bridge")
    async with sql.begin() as connection:
        await connection.execute(
            text("UPDATE conversations SET authority_revision=2 WHERE id=:id"),
            {"id": conversation_id},
        )
    migrate(database_url, "upgrade", "head")
    assert await h.database.ready()
    async with sql.connect() as connection:
        floor, tail = (
            await connection.execute(
                text(
                    "SELECT authority_input_floor_sequence,last_message_sequence "
                    "FROM conversations WHERE id=:id"
                ),
                {"id": conversation_id},
            )
        ).one()
    assert floor == tail == 2

    rejected = migrate(database_url, "downgrade", "0007_voice_bridge", expected=1)
    assert "Business effect authority evidence is retained" in rejected.stderr
    assert await h.database.ready()
    async with sql.connect() as connection:
        retained = await connection.scalar(
            text("SELECT authority_input_floor_sequence FROM conversations WHERE id=:id"),
            {"id": conversation_id},
        )
    assert retained == 2


async def test_0009_keeps_historical_delivery_outside_telegram_fifo(h, sql, database_url):
    accepted, turn = await h.draft()
    await h.database.dispose()
    migrate(database_url, "downgrade", "0008_business_effect_permits")
    attempt_id, delivery_key = uuid4(), uuid4()
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "INSERT INTO delivery_attempts "
                "(id,conversation_id,delivery_key,source_turn_id,creation_fingerprint,"
                "authority_revision,status,revision,created_at,updated_at) "
                "VALUES (:id,:conversation,:key,:turn,:fingerprint,0,'GENERATED',0,"
                "clock_timestamp(),clock_timestamp())"
            ),
            {
                "id": attempt_id,
                "conversation": accepted["conversation"]["id"],
                "key": delivery_key,
                "turn": turn["id"],
                "fingerprint": "e" * 64,
            },
        )
    migrate(database_url, "upgrade", "head")
    assert await h.database.ready()
    async with sql.connect() as connection:
        ordinal = await connection.scalar(
            text("SELECT outbound_ordinal FROM delivery_attempts WHERE id=:id"),
            {"id": attempt_id},
        )
    assert ordinal is None


async def test_0008_downgrade_refuses_retained_immutable_permit(h, sql, database_url):
    from tests.test_external_effects import claim_and_stamp, context_for, permit_request

    accepted = await h.ingest(provider_thread_id="permit-downgrade-fence")
    turn, _ = await h.select(accepted, input_first_sequence=1, input_last_sequence=1)
    await claim_and_stamp(h, sql, turn)
    context = context_for(accepted["conversation"], turn, turn)
    issued = await h.call("POST", "/external-effect-permits", permit_request(context))
    assert issued.status_code == 200, issued.text
    await h.database.dispose()

    rejected = migrate(database_url, "downgrade", "0007_voice_bridge", expected=1)
    assert "Business effect authority evidence is retained" in rejected.stderr
    assert await h.database.ready()
    async with sql.connect() as connection:
        count = await connection.scalar(text("SELECT count(*) FROM external_effect_permits"))
    assert count == 1
