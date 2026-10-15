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
        "uq_message_identity_sequence",
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
            "website_admissions",
            "website_immediate_turns",
            "website_response_attempts",
            "telegram_admissions",
            "telegram_immediate_turns",
            "telegram_response_attempts",
            "telegram_provider_accounts",
            "turn_input_messages",
            "settling_bursts",
        }
        assert (
            await connection.scalar(text("SELECT version_num FROM alembic_version"))
            == "0006_turn_settling"
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


async def test_turn_input_message_foreign_key_binds_exact_id_and_sequence(h, sql):
    first = await h.ingest()
    await h.ingest()
    third = await h.ingest()
    first_turn, _ = await h.select(third, input_first_sequence=1, input_last_sequence=1)
    claim = await h.claim(first_turn)
    assert (await h.call(
        "POST", f"/turns/{first_turn['id']}/complete",
        dict(h.proof(claim), outcome="NO_RESPONSE"),
    )).status_code == 200
    second_turn, _ = await h.select(third, input_first_sequence=2, input_last_sequence=2)
    claim = await h.claim(second_turn)
    assert (await h.call(
        "POST", f"/turns/{second_turn['id']}/complete",
        dict(h.proof(claim), outcome="NO_RESPONSE"),
    )).status_code == 200
    await violation(
        sql,
        "INSERT INTO turn_input_messages(message_id,conversation_id,turn_id,sequence,ordinal) "
        "VALUES(:message_id,:conversation_id,:turn_id,1,1)",
        dict(message_id=third["message"]["id"],
             conversation_id=first["conversation"]["id"], turn_id=second_turn["id"]),
        "23503", "fk_turn_input_exact_message",
    )


async def test_goal5_settling_checks_reject_malformed_raw_sql(h, sql):
    accepted = await h.ingest()
    # Membership is intentionally immutable on UPDATE, so clone the canonical
    # inbound row and make malformed snapshots on INSERT to exercise the
    # named CHECKs themselves rather than the immutability trigger.
    async with sql.connect() as connection:
        columns = list((await connection.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name='messages' "
            "ORDER BY ordinal_position"
        ))).scalars())
    burst_id = uuid4()
    async with sql.begin() as connection:
        await connection.execute(text(
            "INSERT INTO settling_bursts "
            "(id,conversation_id,policy_version,quiet_window_ms,max_wait_ms,"
            "opened_at,max_cutoff_at,last_input_at,tail_sequence,ready_at) "
            "VALUES (:id,:conversation,'telegram-v1',100,1000,clock_timestamp(),"
            "clock_timestamp()+interval '1 second',clock_timestamp(),1,"
            "clock_timestamp()+interval '100 milliseconds')"
        ), {"id": str(burst_id), "conversation": accepted["conversation"]["id"]})
    overrides = {
        "id": ":new_id",
        "sequence": "sequence + 1000000",
        "settling_policy_version": "'telegram-v1'",
        "settling_quiet_window_ms": "0",
        "settling_max_wait_ms": "1000",
        "settling_burst_id": ":burst_id",
        "source_inbound_event_id": "NULL",
    }
    expressions = [overrides.get(column, column) for column in columns]
    await violation(
        sql,
        "INSERT INTO messages (" + ",".join(columns) + ") SELECT "
        + ",".join(expressions) + " FROM messages WHERE id=:id",
        {"id": accepted["message"]["id"], "new_id": str(uuid4()),
         "burst_id": str(burst_id)},
        "23514", "ck_message_settling_snapshot",
    )
    null_expressions = [
        "NULL" if column == "settling_quiet_window_ms" else expression
        for column, expression in zip(columns, expressions, strict=True)
    ]
    await violation(
        sql,
        "INSERT INTO messages (" + ",".join(columns) + ") SELECT "
        + ",".join(null_expressions) + " FROM messages WHERE id=:id",
        {"id": accepted["message"]["id"], "new_id": str(uuid4()),
         "burst_id": str(burst_id)},
        "23514", "ck_message_settling_burst_shape",
    )
    _, attempt, _ = await h.delivery()
    await violation(
        sql,
        "UPDATE delivery_attempts SET expected_settling_revision=NULL, "
        "fenced_settling_revision=0 WHERE id=:id",
        {"id": attempt["id"]}, "23514", "ck_delivery_settling_fence",
    )
    await h.reconcile(attempt, "DISPATCH_REQUESTED")
    await violation(
        sql,
        "UPDATE delivery_reconciliations SET expected_settling_revision=0, "
        "result_settling_revision=NULL WHERE delivery_attempt_id=:id",
        {"id": attempt["id"]}, "23514", "ck_reconciliation_settling_fence",
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
                text("UPDATE alembic_version SET version_num='0006_turn_settling'")
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
