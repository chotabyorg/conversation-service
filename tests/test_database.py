from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


async def violation(sql, statement, parameters, state, constraint=None):
    with pytest.raises(IntegrityError) as captured:
        async with sql.begin() as connection:
            await connection.execute(text(statement), parameters)
    original = captured.value.orig
    assert getattr(original, "sqlstate", None) == state
    if constraint:
        assert getattr(original.__cause__, "constraint_name", None) == constraint
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT 1")) == 1


async def clone_turn(sql, turn_id, overrides, expected_constraint):
    # Read actual migrated columns, then attempt an independent raw writer.
    async with sql.connect() as connection:
        columns = list(
            (
                await connection.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema='public' "
                        "AND table_name='agent_turns' ORDER BY ordinal_position"
                    )
                )
            ).scalars()
        )
    expressions = [overrides.get(column, column) for column in columns]
    sql_statement = (
        "INSERT INTO agent_turns ("
        + ",".join(columns)
        + ") SELECT "
        + ",".join(expressions)
        + " FROM agent_turns WHERE id=:source"
    )
    await violation(
        sql,
        sql_statement,
        dict(source=turn_id, new_id=str(uuid4()), new_key=str(uuid4())),
        "23514" if expected_constraint.startswith("ck_") else "23505",
        expected_constraint,
    )


async def test_migrated_catalog_constraints_indexes_utc_and_exact_collation(h, sql):
    names = {
        "uq_conversation_identity",
        "uq_conversation_id_scope",
        "ck_conversation_counters",
        "uq_inbound_event_scope",
        "uq_inbound_event_id_conversation",
        "fk_inbound_event_conversation_scope",
        "uq_message_sequence",
        "uq_message_source_event",
        "fk_message_source_event_conversation",
        "ck_message_source_actor",
        "uq_turn_selection",
        "uq_turn_id_conversation",
        "ck_turn_boundaries",
        "ck_turn_lifecycle",
        "uq_delivery_key",
        "uq_delivery_source_turn",
        "uq_delivery_id_conversation",
        "fk_delivery_source_turn_conversation",
        "uq_delivery_reconciliation_key",
        "uq_delivery_reconciliation_revision",
        "fk_reconciliation_delivery_conversation",
        "ck_delivery_evidence",
    }
    async with sql.connect() as connection:
        constraints = set(
            (
                await connection.execute(
                    text(
                        "SELECT conname FROM pg_constraint "
                        "WHERE connamespace='public'::regnamespace"
                    )
                )
            ).scalars()
        )
        assert names <= constraints, names - constraints
        definitions = dict(
            (
                await connection.execute(
                    text("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='public'")
                )
            ).all()
        )
        active = definitions["uq_turn_one_active"]
        assert (
            "UNIQUE" in active
            and "conversation_id" in active
            and "PENDING" in active
            and "CLAIMED" in active
        )
        assert "WHERE" in active and "COMPLETED" not in active and "FAILED" not in active
        tables = set(
            (
                await connection.execute(
                    text("SELECT tablename FROM pg_tables WHERE schemaname='public'")
                )
            ).scalars()
        )
        assert tables == {
            "conversations",
            "inbound_events",
            "messages",
            "agent_turns",
            "delivery_attempts",
            "delivery_reconciliations",
            "alembic_version",
        }
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0002_managed_execution"
        )
        timestamps = (
            (
                await connection.execute(
                    text(
                        "SELECT data_type FROM information_schema.columns "
                        "WHERE table_schema='public' AND "
                        "(column_name LIKE '%_at' OR column_name='lease_expires_at')"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert timestamps and set(timestamps) == {"timestamp with time zone"}
        collations = (
            (
                await connection.execute(
                    text(
                        "SELECT collation_name FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name='conversations' "
                        "AND column_name IN ('owner_id','provider_thread_id')"
                    )
                )
            )
            .scalars()
            .all()
        )
        assert collations == ["C", "C"]
    assert (await h.client.get("/ready")).status_code == 200


@pytest.mark.parametrize("claim_first", [False, True])
async def test_single_active_turn_database_constraint_independent_raw_writer(h, sql, claim_first):
    turn, _ = await h.select(await h.ingest())
    if claim_first:
        await h.claim(turn)
    await clone_turn(
        sql, turn["id"], dict(id=":new_id", selection_key=":new_key"), "uq_turn_one_active"
    )


@pytest.mark.parametrize(
    "phase,column,value",
    [
        ("PENDING", "claim_epoch", "NULL"),
        ("PENDING", "started_at", "clock_timestamp()"),
        ("CLAIMED", "claim_token_digest", "NULL"),
        ("CLAIMED", "lease_expires_at", "NULL"),
        ("CLAIMED", "worker_ref", "NULL"),
        ("CLAIMED", "started_at", "NULL"),
        ("COMPLETED", "outcome", "NULL"),
        ("COMPLETED", "draft_text", "NULL"),
        ("COMPLETED", "completed_at", "NULL"),
        ("COMPLETED", "terminal_fingerprint", "NULL"),
        ("FAILED", "failure_code", "NULL"),
        ("FAILED", "completed_at", "NULL"),
    ],
)
async def test_lifecycle_null_safe_check_rejects_invalid_raw_rows(h, sql, phase, column, value):
    turn, _ = await h.select(await h.ingest())
    if phase != "PENDING":
        claim = await h.claim(turn)
        if phase in {"COMPLETED", "FAILED"}:
            suffix = "complete" if phase == "COMPLETED" else "fail"
            result = (
                dict(outcome="DRAFT_READY", draft_text="draft")
                if phase == "COMPLETED"
                else dict(failure_code="TIMEOUT")
            )
            assert (
                await h.call(
                    "POST", f"/turns/{turn['id']}/{suffix}", dict(h.proof(claim), **result)
                )
            ).status_code == 200
    # UPDATE isolates the shape check from active-slot uniqueness.
    expected = "23502" if column == "claim_epoch" else "23514"
    await violation(
        sql,
        f"UPDATE agent_turns SET {column}={value} WHERE id=:id",
        {"id": turn["id"]},
        expected,
        None if expected == "23502" else "ck_turn_lifecycle",
    )


async def test_named_unique_and_composite_foreign_key_negative_writes(h, sql):
    a = await h.ingest()
    b = await h.ingest("b")
    await violation(
        sql,
        "UPDATE messages SET conversation_id=:other,sequence=99 WHERE id=:id",
        dict(other=b["conversation"]["id"], id=a["message"]["id"]),
        "23503",
        "fk_message_source_event_conversation",
    )
    await violation(
        sql,
        "UPDATE inbound_events SET owner_id='unrelated-owner' WHERE id=:id",
        dict(id=a["event"]["id"]),
        "23503",
        "fk_inbound_event_conversation_scope",
    )
    c = await h.ingest()
    await violation(
        sql,
        "UPDATE messages SET sequence=1 WHERE id=:id",
        dict(id=c["message"]["id"]),
        "23505",
        "uq_message_sequence",
    )
    await violation(
        sql,
        "UPDATE messages SET source_inbound_event_id=:source WHERE id=:id",
        dict(source=a["event"]["id"], id=c["message"]["id"]),
        "23505",
        "uq_message_source_event",
    )
    turn, _ = await h.select(c)
    await violation(
        sql,
        "UPDATE agent_turns SET context_last_sequence=99 WHERE id=:id",
        dict(id=turn["id"]),
        "23503",
    )
    await violation(
        sql,
        "UPDATE agent_turns SET input_first_sequence=3 WHERE id=:id",
        dict(id=turn["id"]),
        "23514",
        "ck_turn_boundaries",
    )


async def test_missing_or_wrong_revision_and_missing_table_are_not_ready(h, sql):
    async with sql.begin() as connection:
        await connection.execute(
            text("UPDATE alembic_version SET version_num='wrong-test-revision'")
        )
    try:
        response = await h.client.get("/ready")
        assert response.status_code == 503 and response.json() == dict(
            status="not_ready", service="conversation-service"
        )
    finally:
        async with sql.begin() as connection:
            await connection.execute(
                text("UPDATE alembic_version SET version_num='0002_managed_execution'")
            )
    async with sql.begin() as connection:
        await connection.execute(
            text("ALTER TABLE conversations RENAME TO test_hidden_conversations")
        )
    try:
        assert (await h.client.get("/ready")).status_code == 503
        assert (await h.client.get("/health")).status_code == 200
    finally:
        async with sql.begin() as connection:
            await connection.execute(
                text("ALTER TABLE test_hidden_conversations RENAME TO conversations")
            )
    assert (await h.client.get("/ready")).status_code == 200
