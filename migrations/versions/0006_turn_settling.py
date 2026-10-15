"""Persist settling policy snapshots and selected inbound ownership."""

import sqlalchemy as sa
from alembic import op

revision = "0006_turn_settling"
down_revision = "0005_telegram_canary"
branch_labels = None
depends_on = None


DELIVERY_STATES = (
    "'GENERATED','DISPATCH_REQUESTED','SUPERSEDED','PROVIDER_ACCEPTED','DELIVERED',"
    "'READ','FAILED','UNKNOWN','PARTIAL','INTERRUPTED'"
)


def upgrade() -> None:
    op.add_column("conversations", sa.Column(
        "settling_revision", sa.BigInteger(), nullable=False, server_default="0"))
    op.drop_constraint("ck_conversation_counters", "conversations", type_="check")
    op.create_check_constraint("ck_conversation_counters", "conversations",
        "last_message_sequence >= 0 AND input_revision >= 0 AND settling_revision >= 0 "
        "AND input_revision <= last_message_sequence AND authority_revision >= 0")

    op.add_column("messages", sa.Column("input_revision", sa.BigInteger(), nullable=True))
    op.add_column("messages", sa.Column("settling_revision", sa.BigInteger(), nullable=True))
    op.add_column("messages", sa.Column("settling_policy_version", sa.String(64, collation="C")))
    op.add_column("messages", sa.Column("settling_quiet_window_ms", sa.BigInteger()))
    op.add_column("messages", sa.Column("settling_max_wait_ms", sa.BigInteger()))
    op.execute("""
        WITH ranked AS (
          SELECT id, row_number() OVER (PARTITION BY conversation_id ORDER BY sequence) AS rev
          FROM messages WHERE actor='CUSTOMER' AND direction='INBOUND'
        )
        UPDATE messages m SET input_revision=ranked.rev, settling_revision=0
        FROM ranked WHERE ranked.id=m.id
    """)
    op.create_check_constraint("ck_message_settling_snapshot", "messages",
        "(settling_policy_version IS NULL AND settling_quiet_window_ms IS NULL "
        "AND settling_max_wait_ms IS NULL) OR (settling_policy_version IS NOT NULL "
        "AND settling_quiet_window_ms IS NOT NULL AND settling_max_wait_ms IS NOT NULL "
        "AND settling_policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$' "
        "AND settling_quiet_window_ms >= 1 AND settling_max_wait_ms >= "
        "settling_quiet_window_ms)")
    op.create_index("ix_messages_settling_pending", "messages",
                    ["settling_policy_version", "created_at", "conversation_id"],
                    postgresql_where=sa.text("settling_policy_version IS NOT NULL"))

    op.add_column("agent_turns", sa.Column(
        "settling_revision", sa.BigInteger(), nullable=False, server_default="0"))
    op.create_check_constraint("ck_turn_settling_revision", "agent_turns",
                               "settling_revision >= 0")
    op.execute("""CREATE OR REPLACE FUNCTION preserve_website_projection() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.managed_contract_version IS DISTINCT FROM OLD.managed_contract_version
      OR (OLD.managed_contract_version=2 AND ROW(
        NEW.id,NEW.conversation_id,NEW.selection_key,NEW.creation_fingerprint,
        NEW.input_first_sequence,NEW.input_last_sequence,NEW.context_last_sequence,
        NEW.input_revision,NEW.settling_revision,NEW.settling_burst_id,NEW.authority_revision,
        NEW.managed_contract_version,NEW.managed_context_first_sequence,
        NEW.managed_projection_agent_id,NEW.managed_projection_fingerprint) IS DISTINCT FROM ROW(
        OLD.id,OLD.conversation_id,OLD.selection_key,OLD.creation_fingerprint,
        OLD.input_first_sequence,OLD.input_last_sequence,OLD.context_last_sequence,
        OLD.input_revision,OLD.settling_revision,OLD.settling_burst_id,OLD.authority_revision,
        OLD.managed_contract_version,OLD.managed_context_first_sequence,
        OLD.managed_projection_agent_id,OLD.managed_projection_fingerprint)) THEN
        RAISE EXCEPTION 'Frozen Website projection cannot change.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")

    op.add_column("delivery_attempts", sa.Column("expected_settling_revision", sa.BigInteger()))
    op.add_column("delivery_attempts", sa.Column("fenced_settling_revision", sa.BigInteger()))
    op.create_check_constraint("ck_delivery_settling_fence", "delivery_attempts",
        "(expected_settling_revision IS NULL AND fenced_settling_revision IS NULL) OR "
        "(expected_settling_revision IS NOT NULL AND expected_settling_revision >= 0 AND "
        "(fenced_settling_revision IS NULL OR (fenced_settling_revision IS NOT NULL AND "
        "fenced_settling_revision >= expected_settling_revision)))")
    op.drop_constraint("ck_delivery_status", "delivery_attempts", type_="check")
    op.create_check_constraint("ck_delivery_status", "delivery_attempts",
                               f"status IN ({DELIVERY_STATES})")

    op.add_column("delivery_reconciliations", sa.Column(
        "expected_settling_revision", sa.BigInteger()))
    op.add_column("delivery_reconciliations", sa.Column(
        "result_settling_revision", sa.BigInteger()))
    op.create_check_constraint("ck_reconciliation_settling_fence", "delivery_reconciliations",
        "(expected_settling_revision IS NULL AND result_settling_revision IS NULL) OR "
        "(expected_settling_revision IS NOT NULL AND result_settling_revision IS NOT NULL AND "
        "expected_settling_revision >= 0 AND result_settling_revision >= "
        "expected_settling_revision)")
    op.drop_constraint("ck_reconciliation_status", "delivery_reconciliations", type_="check")
    op.create_check_constraint("ck_reconciliation_status", "delivery_reconciliations",
        f"status IN ({DELIVERY_STATES}) AND status <> 'GENERATED'")

    op.drop_constraint("website_immediate_turns_turn_id_key", "website_immediate_turns",
                       type_="unique")
    op.drop_constraint("telegram_immediate_turns_turn_id_key", "telegram_immediate_turns",
                       type_="unique")
    op.create_index("ix_website_immediate_turn_id", "website_immediate_turns", ["turn_id"])
    op.create_index("ix_telegram_immediate_turn_id", "telegram_immediate_turns", ["turn_id"])
    op.create_unique_constraint("uq_message_identity_sequence", "messages",
                                ["id", "conversation_id", "sequence"])

    op.create_table(
        "turn_input_messages",
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("ordinal", sa.BigInteger(), nullable=False),
        sa.UniqueConstraint("turn_id", "sequence", name="uq_turn_input_sequence"),
        sa.ForeignKeyConstraint(["message_id", "conversation_id", "sequence"],
                                ["messages.id", "messages.conversation_id", "messages.sequence"],
                                name="fk_turn_input_exact_message", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["turn_id", "conversation_id"],
                                ["agent_turns.id", "agent_turns.conversation_id"],
                                name="fk_turn_input_turn_conversation", ondelete="RESTRICT"),
        sa.CheckConstraint("sequence >= 1 AND ordinal >= 0", name="ck_turn_input_position"),
    )
    op.create_index("ix_turn_input_conversation_sequence", "turn_input_messages",
                    ["conversation_id", "sequence"])
    # Only unambiguous legacy ranges receive an owner. Ambiguous overlapping
    # historical rows stay unassigned; runtime selectors fence all frozen ranges.
    op.execute("""
        WITH candidates AS (
          SELECT m.id AS message_id, m.conversation_id, t.id AS turn_id, m.sequence,
                 count(t.id) OVER (PARTITION BY m.id) AS owner_count
          FROM messages m JOIN agent_turns t ON t.conversation_id=m.conversation_id
           AND m.sequence BETWEEN t.input_first_sequence AND t.input_last_sequence
          WHERE m.actor='CUSTOMER' AND m.direction='INBOUND'
        ), chosen AS (
          SELECT *, row_number() OVER (PARTITION BY turn_id ORDER BY sequence)-1 AS ordinal
          FROM candidates WHERE owner_count=1
        )
        INSERT INTO turn_input_messages(message_id,conversation_id,turn_id,sequence,ordinal)
        SELECT message_id,conversation_id,turn_id,sequence,ordinal FROM chosen
    """)

    op.create_table(
        "settling_bursts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("policy_version", sa.String(64, collation="C"), nullable=False),
        sa.Column("quiet_window_ms", sa.BigInteger(), nullable=False),
        sa.Column("max_wait_ms", sa.BigInteger(), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("max_cutoff_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_input_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tail_sequence", sa.BigInteger(), nullable=False),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("id", "conversation_id",
                            name="uq_settling_burst_id_conversation"),
        sa.ForeignKeyConstraint(["conversation_id"], ["conversations.id"],
                                name="fk_settling_burst_conversation", ondelete="RESTRICT"),
        sa.CheckConstraint(
            "policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$' "
            "AND quiet_window_ms >= 1 AND max_wait_ms >= quiet_window_ms "
            "AND tail_sequence >= 1 AND max_cutoff_at > opened_at "
            "AND last_input_at >= opened_at AND ready_at >= last_input_at "
            "AND ready_at <= max_cutoff_at "
            "AND (closed_at IS NULL OR closed_at = ready_at)",
            name="ck_settling_burst_shape",
        ),
    )
    op.create_index("uq_settling_burst_open", "settling_bursts", ["conversation_id"],
                    unique=True, postgresql_where=sa.text("closed_at IS NULL"))
    op.create_index("ix_settling_burst_due", "settling_bursts", ["ready_at", "conversation_id"],
                    )
    op.add_column("messages", sa.Column("settling_burst_id", sa.Uuid(), nullable=True))
    op.add_column("agent_turns", sa.Column("settling_burst_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_message_settling_burst_conversation", "messages", "settling_bursts",
        ["settling_burst_id", "conversation_id"], ["id", "conversation_id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "fk_turn_settling_burst_conversation", "agent_turns", "settling_bursts",
        ["settling_burst_id", "conversation_id"], ["id", "conversation_id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_message_settling_burst_shape", "messages",
        "(settling_burst_id IS NULL AND settling_policy_version IS NULL "
        "AND settling_quiet_window_ms IS NULL AND settling_max_wait_ms IS NULL) OR "
        "(settling_burst_id IS NOT NULL AND actor='CUSTOMER' AND direction='INBOUND' "
        "AND settling_policy_version IS NOT NULL AND settling_quiet_window_ms IS NOT NULL "
        "AND settling_max_wait_ms IS NOT NULL AND input_revision IS NOT NULL "
        "AND input_revision >= 1 AND settling_revision IS NOT NULL "
        "AND settling_revision >= 0)",
    )
    op.create_check_constraint(
        "ck_turn_settling_burst_shape", "agent_turns",
        "settling_burst_id IS NULL OR settling_revision >= 0",
    )
    op.execute("""CREATE FUNCTION preserve_settling_membership() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF ROW(NEW.settling_burst_id,NEW.settling_policy_version,
             NEW.settling_quiet_window_ms,NEW.settling_max_wait_ms) IS DISTINCT FROM
         ROW(OLD.settling_burst_id,OLD.settling_policy_version,
             OLD.settling_quiet_window_ms,OLD.settling_max_wait_ms)
      OR (OLD.settling_burst_id IS NOT NULL AND ROW(
             NEW.input_revision,NEW.settling_revision) IS DISTINCT FROM ROW(
             OLD.input_revision,OLD.settling_revision)) THEN
        RAISE EXCEPTION 'Message settling membership is immutable.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.execute("CREATE TRIGGER preserve_message_settling_membership BEFORE UPDATE ON messages "
               "FOR EACH ROW EXECUTE FUNCTION preserve_settling_membership()")
    op.execute("""CREATE FUNCTION preserve_burst_definition() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF ROW(NEW.id,NEW.conversation_id,NEW.policy_version,NEW.quiet_window_ms,
             NEW.max_wait_ms,NEW.opened_at,NEW.max_cutoff_at) IS DISTINCT FROM
         ROW(OLD.id,OLD.conversation_id,OLD.policy_version,OLD.quiet_window_ms,
             OLD.max_wait_ms,OLD.opened_at,OLD.max_cutoff_at)
      OR OLD.closed_at IS NOT NULL
      OR (NEW.closed_at IS NOT NULL AND NEW.closed_at IS DISTINCT FROM OLD.ready_at)
      OR (OLD.closed_at IS NULL AND NEW.closed_at IS NULL
          AND NEW.ready_at < OLD.ready_at) THEN
        RAISE EXCEPTION 'Settling burst definition cannot be rewritten.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.execute("CREATE TRIGGER preserve_settling_burst_definition BEFORE UPDATE "
               "ON settling_bursts FOR EACH ROW EXECUTE FUNCTION preserve_burst_definition()")
    op.execute("""CREATE FUNCTION preserve_turn_settling_burst() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.settling_burst_id IS DISTINCT FROM OLD.settling_burst_id THEN
        RAISE EXCEPTION 'Turn settling membership is immutable.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.execute("CREATE TRIGGER preserve_turn_settling_burst BEFORE UPDATE ON agent_turns "
               "FOR EACH ROW EXECUTE FUNCTION preserve_turn_settling_burst()")

    # Reissue after the nullable discriminator exists so the frozen projection
    # trigger protects its exact persisted mode on later updates.
    op.execute("""CREATE OR REPLACE FUNCTION preserve_website_projection() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.managed_contract_version IS DISTINCT FROM OLD.managed_contract_version
      OR (OLD.managed_contract_version=2 AND ROW(
        NEW.id,NEW.conversation_id,NEW.selection_key,NEW.creation_fingerprint,
        NEW.input_first_sequence,NEW.input_last_sequence,NEW.context_last_sequence,
        NEW.input_revision,NEW.settling_revision,NEW.settling_burst_id,NEW.authority_revision,
        NEW.managed_contract_version,NEW.managed_context_first_sequence,
        NEW.managed_projection_agent_id,NEW.managed_projection_fingerprint) IS DISTINCT FROM ROW(
        OLD.id,OLD.conversation_id,OLD.selection_key,OLD.creation_fingerprint,
        OLD.input_first_sequence,OLD.input_last_sequence,OLD.context_last_sequence,
        OLD.input_revision,OLD.settling_revision,OLD.settling_burst_id,OLD.authority_revision,
        OLD.managed_contract_version,OLD.managed_context_first_sequence,
        OLD.managed_projection_agent_id,OLD.managed_projection_fingerprint)) THEN
        RAISE EXCEPTION 'Frozen Website projection cannot change.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")

    # Account identity is reconstructed from every retained MANAGED admission.
    op.create_table(
        "telegram_provider_accounts",
        sa.Column("owner_id", sa.String(255, collation="C"), primary_key=True),
        sa.Column("integration_id", sa.Uuid(), primary_key=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("bot_id", sa.String(64, collation="C"), nullable=True),
        sa.CheckConstraint(
            "(state='BOUND' AND bot_id IS NOT NULL "
            "AND bot_id ~ '^[1-9][0-9]{0,18}$') OR "
            "(state='CONFLICT' AND bot_id IS NULL)",
            name="ck_telegram_provider_account_shape",
        ),
    )
    op.execute("""
        INSERT INTO telegram_provider_accounts(owner_id,integration_id,state,bot_id)
        SELECT owner_id,integration_id,
               CASE WHEN count(DISTINCT bot_id)=1 THEN 'BOUND' ELSE 'CONFLICT' END,
               CASE WHEN count(DISTINCT bot_id)=1 THEN min(bot_id) ELSE NULL END
        FROM telegram_admissions WHERE route='MANAGED'
        GROUP BY owner_id,integration_id
    """)
    op.execute("""CREATE FUNCTION immutable_telegram_provider_account() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      RAISE EXCEPTION 'Telegram provider-account identity is immutable.' USING ERRCODE='23514';
    END $$""")
    op.execute("CREATE TRIGGER immutable_telegram_provider_account BEFORE UPDATE OR DELETE "
               "ON telegram_provider_accounts FOR EACH ROW "
               "EXECUTE FUNCTION immutable_telegram_provider_account()")
    # The historical Website writer authorized every retained response attempt.
    op.add_column("website_response_attempts", sa.Column("send_authorized", sa.Boolean(),
                                                            nullable=False,
                                                            server_default=sa.text("false")))
    op.execute("UPDATE website_response_attempts SET send_authorized=true")
    for table in ("telegram_response_attempts",):
        op.add_column(table, sa.Column("send_authorized", sa.Boolean(), nullable=False,
                                       server_default=sa.text("false")))
        op.execute(sa.text(f"""
            WITH ranked AS (
              SELECT a.id, row_number() OVER (
                PARTITION BY a.delivery_id ORDER BY a.created_at, a.id) AS attempt_rank
              FROM {table} a JOIN delivery_attempts d ON d.id=a.delivery_id
              WHERE d.status <> 'GENERATED'
            )
            UPDATE {table} a SET send_authorized=true FROM ranked r
            WHERE a.id=r.id AND r.attempt_rank=1
        """))


def downgrade() -> None:
    op.execute("LOCK TABLE conversations, messages, agent_turns, delivery_attempts, "
               "delivery_reconciliations, turn_input_messages, website_response_attempts, "
               "telegram_response_attempts, website_immediate_turns, telegram_immediate_turns, "
               "settling_bursts, telegram_provider_accounts, telegram_admissions "
               "IN ACCESS EXCLUSIVE MODE")
    bind = op.get_bind()
    guards = (
        ("SELECT EXISTS(SELECT 1 FROM conversations WHERE settling_revision <> 0)",
         "Conversation settling revisions prevent downgrade."),
        ("""WITH ranked AS (
             SELECT id, row_number() OVER (PARTITION BY conversation_id ORDER BY sequence) AS rev
             FROM messages WHERE actor='CUSTOMER' AND direction='INBOUND'
           ) SELECT EXISTS(
             SELECT 1 FROM messages m LEFT JOIN ranked r ON r.id=m.id
             WHERE m.settling_policy_version IS NOT NULL
                OR m.settling_quiet_window_ms IS NOT NULL
                OR m.settling_max_wait_ms IS NOT NULL
                OR (m.actor='CUSTOMER' AND m.direction='INBOUND' AND
                    (m.input_revision IS DISTINCT FROM r.rev OR
                     m.settling_revision IS DISTINCT FROM 0))
                OR ((m.actor <> 'CUSTOMER' OR m.direction <> 'INBOUND') AND
                    (m.input_revision IS NOT NULL OR m.settling_revision IS NOT NULL))
           )""", "Retained Message settling snapshots prevent downgrade."),
        ("SELECT EXISTS(SELECT 1 FROM agent_turns WHERE settling_revision <> 0)",
         "Retained Turn settling revisions prevent downgrade."),
        ("SELECT EXISTS(SELECT 1 FROM settling_bursts) OR EXISTS(SELECT 1 FROM messages "
         "WHERE settling_burst_id IS NOT NULL) OR EXISTS(SELECT 1 FROM agent_turns "
         "WHERE settling_burst_id IS NOT NULL)",
         "Retained settling burst membership prevents downgrade."),
        ("SELECT EXISTS(SELECT 1 FROM delivery_attempts WHERE status='SUPERSEDED')",
         "Retained supersede evidence prevents downgrade."),
        ("""SELECT EXISTS(SELECT 1 FROM website_response_attempts WHERE NOT send_authorized)
              OR EXISTS(SELECT 1 FROM telegram_response_attempts WHERE NOT send_authorized)""",
         "Retained response send-permission decisions prevent downgrade."),
        ("SELECT EXISTS(SELECT 1 FROM delivery_attempts WHERE "
         "expected_settling_revision IS NOT NULL OR fenced_settling_revision IS NOT NULL)",
         "Retained delivery settling fences prevent downgrade."),
        ("SELECT EXISTS(SELECT 1 FROM delivery_reconciliations WHERE "
         "expected_settling_revision IS NOT NULL OR result_settling_revision IS NOT NULL)",
         "Retained reconciliation settling evidence prevents downgrade."),
        ("""SELECT EXISTS(
             SELECT 1 FROM website_response_attempts GROUP BY delivery_id HAVING count(*) > 1
           ) OR EXISTS(
             SELECT 1 FROM telegram_response_attempts GROUP BY delivery_id HAVING count(*) > 1
           )""", "Multiple response attempts per delivery prevent downgrade."),
        ("""WITH candidates AS (
             SELECT m.id AS message_id, m.conversation_id, t.id AS turn_id, m.sequence,
                    count(t.id) OVER (PARTITION BY m.id) AS owner_count
             FROM messages m JOIN agent_turns t ON t.conversation_id=m.conversation_id
              AND m.sequence BETWEEN t.input_first_sequence AND t.input_last_sequence
             WHERE m.actor='CUSTOMER' AND m.direction='INBOUND'
           ), expected AS (
             SELECT message_id,conversation_id,turn_id,sequence,
                    row_number() OVER (PARTITION BY turn_id ORDER BY sequence)-1 AS ordinal
             FROM candidates WHERE owner_count=1
           ), different AS (
             (SELECT message_id,conversation_id,turn_id,sequence,ordinal FROM expected
              EXCEPT SELECT message_id,conversation_id,turn_id,sequence,ordinal
              FROM turn_input_messages)
             UNION ALL
             (SELECT message_id,conversation_id,turn_id,sequence,ordinal FROM turn_input_messages
              EXCEPT SELECT message_id,conversation_id,turn_id,sequence,ordinal FROM expected)
           ) SELECT EXISTS(SELECT 1 FROM different)""",
         "Turn input ownership cannot be safely reconstructed."),
        ("""SELECT EXISTS(SELECT 1 FROM website_immediate_turns
                            GROUP BY turn_id HAVING count(*) > 1)
              OR EXISTS(SELECT 1 FROM telegram_immediate_turns
                            GROUP BY turn_id HAVING count(*) > 1)""",
         "Multi-message immediate Turn mappings prevent downgrade."),
    )
    for query, message in guards:
        if bind.scalar(sa.text(query)):
            raise RuntimeError(message)
    account_difference = bind.scalar(sa.text("""
        WITH expected AS (
          SELECT owner_id,integration_id,
                 CASE WHEN count(DISTINCT bot_id)=1 THEN 'BOUND' ELSE 'CONFLICT' END AS state,
                 CASE WHEN count(DISTINCT bot_id)=1 THEN min(bot_id) ELSE NULL END AS bot_id
          FROM telegram_admissions WHERE route='MANAGED'
          GROUP BY owner_id,integration_id
        ), different AS (
          (SELECT owner_id,integration_id,state,bot_id FROM expected
           EXCEPT SELECT owner_id,integration_id,state,bot_id FROM telegram_provider_accounts)
          UNION ALL
          (SELECT owner_id,integration_id,state,bot_id FROM telegram_provider_accounts
           EXCEPT SELECT owner_id,integration_id,state,bot_id FROM expected)
        ) SELECT EXISTS(SELECT 1 FROM different)
    """))
    if account_difference:
        raise RuntimeError("Telegram provider-account authority cannot be safely reconstructed.")
    op.execute("DROP TRIGGER immutable_telegram_provider_account ON telegram_provider_accounts")
    op.execute("DROP FUNCTION immutable_telegram_provider_account()")
    op.execute("DROP TRIGGER preserve_turn_settling_burst ON agent_turns")
    op.execute("DROP FUNCTION preserve_turn_settling_burst()")
    op.execute("DROP TRIGGER preserve_settling_burst_definition ON settling_bursts")
    op.execute("DROP FUNCTION preserve_burst_definition()")
    op.execute("DROP TRIGGER preserve_message_settling_membership ON messages")
    op.execute("DROP FUNCTION preserve_settling_membership()")
    op.drop_constraint("ck_turn_settling_burst_shape", "agent_turns", type_="check")
    op.drop_constraint("fk_turn_settling_burst_conversation", "agent_turns",
                       type_="foreignkey")
    op.drop_column("agent_turns", "settling_burst_id")
    op.drop_constraint("ck_message_settling_burst_shape", "messages", type_="check")
    op.drop_constraint("fk_message_settling_burst_conversation", "messages",
                       type_="foreignkey")
    op.drop_column("messages", "settling_burst_id")
    op.drop_index("ix_settling_burst_due", table_name="settling_bursts")
    op.drop_index("uq_settling_burst_open", table_name="settling_bursts")
    op.drop_table("settling_bursts")
    op.drop_table("telegram_provider_accounts")
    op.drop_index("ix_turn_input_conversation_sequence", table_name="turn_input_messages")
    op.drop_table("turn_input_messages")
    op.drop_constraint("uq_message_identity_sequence", "messages", type_="unique")
    op.drop_index("ix_website_immediate_turn_id", table_name="website_immediate_turns")
    op.drop_index("ix_telegram_immediate_turn_id", table_name="telegram_immediate_turns")
    op.create_unique_constraint("website_immediate_turns_turn_id_key", "website_immediate_turns",
                                ["turn_id"])
    op.create_unique_constraint("telegram_immediate_turns_turn_id_key", "telegram_immediate_turns",
                                ["turn_id"])
    op.drop_constraint("ck_reconciliation_status", "delivery_reconciliations", type_="check")
    op.create_check_constraint("ck_reconciliation_status", "delivery_reconciliations",
                                "status IN ('GENERATED','DISPATCH_REQUESTED','PROVIDER_ACCEPTED',"
                                "'DELIVERED','READ','FAILED','UNKNOWN','PARTIAL','INTERRUPTED') "
                                "AND status <> 'GENERATED'")
    op.drop_constraint("ck_reconciliation_settling_fence", "delivery_reconciliations",
                       type_="check")
    op.drop_column("delivery_reconciliations", "result_settling_revision")
    op.drop_column("delivery_reconciliations", "expected_settling_revision")
    op.drop_constraint("ck_delivery_status", "delivery_attempts", type_="check")
    op.create_check_constraint("ck_delivery_status", "delivery_attempts",
                                "status IN ('GENERATED','DISPATCH_REQUESTED','PROVIDER_ACCEPTED',"
                                "'DELIVERED','READ','FAILED','UNKNOWN','PARTIAL','INTERRUPTED')")
    op.drop_constraint("ck_delivery_settling_fence", "delivery_attempts", type_="check")
    op.drop_column("delivery_attempts", "fenced_settling_revision")
    op.drop_column("delivery_attempts", "expected_settling_revision")
    op.drop_column("website_response_attempts", "send_authorized")
    op.drop_column("telegram_response_attempts", "send_authorized")
    op.drop_constraint("ck_turn_settling_revision", "agent_turns", type_="check")
    op.drop_column("agent_turns", "settling_revision")
    op.execute("""CREATE OR REPLACE FUNCTION preserve_website_projection() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.managed_contract_version IS DISTINCT FROM OLD.managed_contract_version
      OR (OLD.managed_contract_version=2 AND ROW(
        NEW.id,NEW.conversation_id,NEW.selection_key,NEW.creation_fingerprint,
        NEW.input_first_sequence,NEW.input_last_sequence,NEW.context_last_sequence,
        NEW.input_revision,NEW.authority_revision,NEW.managed_contract_version,
        NEW.managed_context_first_sequence,NEW.managed_projection_agent_id,
        NEW.managed_projection_fingerprint) IS DISTINCT FROM ROW(
        OLD.id,OLD.conversation_id,OLD.selection_key,OLD.creation_fingerprint,
        OLD.input_first_sequence,OLD.input_last_sequence,OLD.context_last_sequence,
        OLD.input_revision,OLD.authority_revision,OLD.managed_contract_version,
        OLD.managed_context_first_sequence,OLD.managed_projection_agent_id,
        OLD.managed_projection_fingerprint)) THEN
        RAISE EXCEPTION 'Frozen Website projection cannot change.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.drop_index("ix_messages_settling_pending", table_name="messages")
    op.drop_constraint("ck_message_settling_snapshot", "messages", type_="check")
    for column in ("settling_max_wait_ms", "settling_quiet_window_ms",
                   "settling_policy_version", "settling_revision", "input_revision"):
        op.drop_column("messages", column)
    op.drop_constraint("ck_conversation_counters", "conversations", type_="check")
    op.drop_column("conversations", "settling_revision")
    op.create_check_constraint("ck_conversation_counters", "conversations",
        "last_message_sequence >= 0 AND input_revision >= 0 AND "
        "input_revision <= last_message_sequence AND authority_revision >= 0")
