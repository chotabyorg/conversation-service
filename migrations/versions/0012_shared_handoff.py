"""Persist Telegram handoff cases, owner commands, grants, and ordered writers."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0012_shared_handoff"
down_revision = "0011_turn_catch_up"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_turn_catch_up_version", "agent_turns", type_="check")
    op.create_check_constraint(
        "ck_turn_catch_up_version",
        "agent_turns",
        "catch_up_version IN (0,1) AND (catch_up_version=0 OR "
        "(settling_burst_id IS NOT NULL AND managed_contract_version IN (2,3)))",
    )
    op.add_column("conversations", sa.Column("handoff_resume_floor_sequence", sa.BigInteger()))
    op.create_check_constraint(
        "ck_conversation_handoff_resume_floor",
        "conversations",
        "handoff_resume_floor_sequence IS NULL OR "
        "(handoff_resume_floor_sequence >= 0 AND "
        "handoff_resume_floor_sequence <= last_message_sequence)",
    )

    op.add_column("agent_turns", sa.Column("retired_at", sa.DateTime(timezone=True)))
    op.add_column("agent_turns", sa.Column("managed_v3_snapshot", JSONB(none_as_null=True)))
    op.add_column("agent_turns", sa.Column("managed_v3_request_fingerprint", sa.CHAR(64)))
    op.add_column("agent_turns", sa.Column("managed_v3_dispatch_id", sa.Uuid()))
    op.drop_constraint("ck_turn_managed_projection", "agent_turns", type_="check")
    op.create_check_constraint(
        "ck_turn_managed_projection",
        "agent_turns",
        "(managed_contract_version = 1 AND managed_context_first_sequence = 1 "
        "AND managed_projection_agent_id IS NULL AND managed_projection_fingerprint IS NULL) "
        "OR (managed_contract_version IN (2,3) AND managed_context_first_sequence >= 1 "
        "AND managed_context_first_sequence <= input_first_sequence "
        "AND managed_projection_agent_id IS NOT NULL "
        "AND managed_projection_fingerprint IS NOT NULL "
        "AND managed_projection_fingerprint ~ '^[0-9a-f]{64}$')",
    )
    op.create_check_constraint(
        "ck_turn_managed_v3_snapshot",
        "agent_turns",
        "(managed_v3_snapshot IS NULL AND managed_v3_request_fingerprint IS NULL "
        "AND managed_v3_dispatch_id IS NULL) OR "
        "(managed_v3_snapshot IS NOT NULL AND managed_v3_request_fingerprint IS NOT NULL "
        "AND managed_v3_dispatch_id IS NOT NULL "
        "AND managed_v3_request_fingerprint ~ '^[0-9a-f]{64}$' "
        "AND managed_contract_version = 3)",
    )
    op.drop_index("uq_turn_one_active", table_name="agent_turns")
    op.create_index(
        "uq_turn_one_active",
        "agent_turns",
        ["conversation_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('PENDING','CLAIMED') AND retired_at IS NULL"),
    )

    op.alter_column("delivery_attempts", "source_turn_id", nullable=True)
    op.add_column("delivery_attempts", sa.Column("source_message_id", sa.Uuid()))
    op.add_column("delivery_attempts", sa.Column("outbound_ordinal", sa.BigInteger()))
    op.add_column("delivery_attempts", sa.Column("case_id", sa.Uuid()))
    op.add_column("delivery_attempts", sa.Column("case_revision", sa.BigInteger()))
    op.add_column("delivery_attempts", sa.Column("assignment_revision", sa.BigInteger()))
    op.add_column("delivery_attempts", sa.Column("grant_generation", sa.BigInteger()))
    op.add_column(
        "delivery_attempts",
        sa.Column("writer_state", sa.String(16), server_default=sa.text("'NONE'"), nullable=False),
    )
    op.add_column("delivery_attempts", sa.Column("writer_supplier_instance_id", sa.Uuid()))
    op.add_column("delivery_attempts", sa.Column("writer_process_incarnation_id", sa.Uuid()))
    op.add_column("delivery_attempts", sa.Column("writer_started_at", sa.DateTime(timezone=True)))
    op.add_column("delivery_attempts", sa.Column("writer_ended_at", sa.DateTime(timezone=True)))
    # Historical attempts belong to the legacy reconciliation surface. Do not
    # assign them Telegram-v2 FIFO ordinals: that would make unrelated Website,
    # Voice, and legacy Telegram evidence look supplier-owned and could block a
    # newly enrolled Telegram writer forever.
    op.create_unique_constraint(
        "uq_delivery_source_message", "delivery_attempts", ["source_message_id"]
    )
    op.create_unique_constraint(
        "uq_delivery_outbound_ordinal",
        "delivery_attempts",
        ["conversation_id", "outbound_ordinal"],
    )
    op.create_foreign_key(
        "fk_delivery_source_message_conversation",
        "delivery_attempts",
        "messages",
        ["source_message_id", "conversation_id"],
        ["id", "conversation_id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_delivery_exact_source",
        "delivery_attempts",
        "((source_turn_id IS NOT NULL)::int + (source_message_id IS NOT NULL)::int) = 1",
    )
    op.create_check_constraint(
        "ck_delivery_outbound_ordinal", "delivery_attempts", "outbound_ordinal >= 1"
    )
    op.create_check_constraint(
        "ck_delivery_writer_state",
        "delivery_attempts",
        "writer_state IN ('NONE','LIVE','ENDED','RETIRED') AND "
        "((writer_state='NONE' AND writer_supplier_instance_id IS NULL "
        "AND writer_process_incarnation_id IS NULL AND writer_started_at IS NULL "
        "AND writer_ended_at IS NULL) OR (writer_state='LIVE' "
        "AND writer_supplier_instance_id IS NOT NULL "
        "AND writer_process_incarnation_id IS NOT NULL AND writer_started_at IS NOT NULL "
        "AND writer_ended_at IS NULL) OR (writer_state IN ('ENDED','RETIRED') "
        "AND writer_supplier_instance_id IS NOT NULL "
        "AND writer_process_incarnation_id IS NOT NULL AND writer_started_at IS NOT NULL "
        "AND writer_ended_at IS NOT NULL))",
    )
    op.create_check_constraint(
        "ck_delivery_human_binding",
        "delivery_attempts",
        "(case_id IS NULL AND case_revision IS NULL AND assignment_revision IS NULL "
        "AND ((source_turn_id IS NOT NULL AND "
        "(grant_generation IS NULL OR grant_generation >= 1)) OR "
        "(source_message_id IS NOT NULL AND grant_generation IS NULL))) OR "
        "(case_id IS NOT NULL AND case_revision >= 1 "
        "AND assignment_revision >= 0 AND grant_generation >= 1 "
        "AND source_message_id IS NOT NULL)",
    )

    op.create_table(
        "inbox_grant_scopes",
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32, collation="C"), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid()),
        sa.Column("agent_owner_id", sa.Uuid()),
        sa.Column("cp_binding_generation", sa.BigInteger()),
        sa.Column("agent_lifecycle_generation", sa.BigInteger()),
        sa.Column("cp_binding_high_water", sa.BigInteger()),
        sa.Column("agent_lifecycle_high_water", sa.BigInteger()),
        sa.Column("grant_generation", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(owner_id) BETWEEN 1 AND 255 AND octet_length(owner_id) <= 1020 "
            "AND owner_id <> '*'",
            name="ck_inbox_grant_owner_bounds",
        ),
        sa.CheckConstraint("channel = 'TELEGRAM'", name="ck_inbox_grant_channel"),
        sa.CheckConstraint(
            "(cp_binding_high_water IS NULL OR cp_binding_high_water >= 0) AND "
            "(agent_lifecycle_high_water IS NULL OR agent_lifecycle_high_water >= 0) "
            "AND grant_generation >= 0",
            name="ck_inbox_grant_generations",
        ),
        sa.CheckConstraint(
            "(active = false) OR (account_id IS NOT NULL AND agent_owner_id IS NOT NULL "
            "AND cp_binding_generation IS NOT NULL AND agent_lifecycle_generation IS NOT NULL "
            "AND (cp_binding_high_water IS NULL OR cp_binding_generation > cp_binding_high_water) "
            "AND (agent_lifecycle_high_water IS NULL OR "
            "agent_lifecycle_generation > agent_lifecycle_high_water) AND grant_generation >= 1)",
            name="ck_inbox_grant_active_shape",
        ),
        sa.PrimaryKeyConstraint(
            "owner_id", "integration_id", "channel", "agent_id", name="pk_inbox_grant_scopes"
        ),
    )

    op.create_table(
        "inbox_grant_lifecycle_receipts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32, collation="C"), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("command_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(24), nullable=False),
        sa.Column("request_fingerprint", sa.CHAR(64), nullable=False),
        sa.Column("acknowledgement", JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "char_length(owner_id) BETWEEN 1 AND 255 AND octet_length(owner_id) <= 1020 "
            "AND owner_id <> '*'",
            name="ck_inbox_grant_lifecycle_owner_bounds",
        ),
        sa.CheckConstraint("channel = 'TELEGRAM'", name="ck_inbox_grant_lifecycle_channel"),
        sa.CheckConstraint(
            "kind IN ('PROVISION','REVOKE_CONTROL_PLANE','REVOKE_AI_AGENT')",
            name="ck_inbox_grant_lifecycle_kind",
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_inbox_grant_lifecycle_fingerprint"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_inbox_grant_lifecycle_receipts"),
        sa.UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "agent_id",
            "command_id",
            name="uq_inbox_grant_lifecycle_command",
        ),
    )
    op.create_index(
        "ix_inbox_grant_lifecycle_scope",
        "inbox_grant_lifecycle_receipts",
        ["owner_id", "integration_id", "channel", "agent_id", "created_at"],
    )

    op.create_table(
        "handoff_cases",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32, collation="C"), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("control_request_id", sa.Uuid(), nullable=False),
        sa.Column("control_request_fingerprint", sa.CHAR(64), nullable=False),
        sa.Column("source_turn_id", sa.Uuid(), nullable=False),
        sa.Column("reason", sa.String(32), nullable=False),
        sa.Column("note", sa.Text()),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("case_revision", sa.BigInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("opened_authority_revision", sa.BigInteger(), nullable=False),
        sa.Column(
            "assignment_revision", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("assigned_account_id", sa.Uuid()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint("channel = 'TELEGRAM'", name="ck_handoff_case_channel"),
        sa.CheckConstraint(
            "reason IN ('CUSTOMER_REQUEST','AGENT_LIMIT','BUSINESS_UNCERTAIN',"
            "'POLICY_REQUIRES_HUMAN','OTHER')",
            name="ck_handoff_case_reason",
        ),
        sa.CheckConstraint(
            "status IN ('OPEN','CLAIMED','RETURNED')", name="ck_handoff_case_status"
        ),
        sa.CheckConstraint(
            "case_revision >= 1 AND authority_revision >= 1 AND "
            "opened_authority_revision >= 1 AND "
            "opened_authority_revision <= authority_revision AND "
            "assignment_revision >= 0",
            name="ck_handoff_case_revisions",
        ),
        sa.CheckConstraint(
            "control_request_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_handoff_control_fingerprint"
        ),
        sa.CheckConstraint(
            "note IS NULL OR (char_length(note) BETWEEN 1 AND 1000 AND octet_length(note) <= 4000)",
            name="ck_handoff_case_note",
        ),
        sa.CheckConstraint(
            "(status = 'CLAIMED' AND assigned_account_id IS NOT NULL) OR "
            "(status <> 'CLAIMED' AND assigned_account_id IS NULL)",
            name="ck_handoff_case_assignment",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
            ],
            name="fk_handoff_case_conversation_scope",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["source_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_handoff_case_source_turn",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_handoff_cases"),
        sa.UniqueConstraint("id", "conversation_id", name="uq_handoff_case_id_conversation"),
        sa.UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "conversation_id",
            "control_request_id",
            name="uq_handoff_control_request",
        ),
    )
    op.create_index(
        "uq_handoff_one_active_case",
        "handoff_cases",
        ["conversation_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('OPEN','CLAIMED')"),
    )
    op.create_index(
        "ix_handoff_case_owner_status", "handoff_cases", ["owner_id", "status", "updated_at"]
    )

    op.create_table(
        "inbox_command_receipts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("case_id", sa.Uuid(), nullable=False),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32, collation="C"), nullable=False),
        sa.Column("actor_account_id", sa.Uuid(), nullable=False),
        sa.Column("command_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(24), nullable=False),
        sa.Column("request_fingerprint", sa.CHAR(64), nullable=False),
        sa.Column("applied_case_revision", sa.BigInteger(), nullable=False),
        sa.Column("applied_authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("applied_assignment_revision", sa.BigInteger()),
        sa.Column("message_id", sa.Uuid()),
        sa.Column("outbound_intent_id", sa.Uuid()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint("channel = 'TELEGRAM'", name="ck_inbox_command_channel"),
        sa.CheckConstraint(
            "kind IN ('CLAIM','SEND_HUMAN','RETURN_TO_AGENT')", name="ck_inbox_command_kind"
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_inbox_command_fingerprint"
        ),
        sa.CheckConstraint(
            "applied_case_revision >= 1 AND applied_authority_revision >= 0 AND "
            "(applied_assignment_revision IS NULL OR applied_assignment_revision >= 0)",
            name="ck_inbox_command_revisions",
        ),
        sa.CheckConstraint(
            "(kind = 'SEND_HUMAN' AND message_id IS NOT NULL "
            "AND outbound_intent_id IS NOT NULL "
            "AND applied_assignment_revision IS NOT NULL) OR "
            "(kind <> 'SEND_HUMAN' AND message_id IS NULL "
            "AND outbound_intent_id IS NULL)",
            name="ck_inbox_command_message_shape",
        ),
        sa.ForeignKeyConstraint(
            ["case_id", "conversation_id"],
            ["handoff_cases.id", "handoff_cases.conversation_id"],
            name="fk_inbox_command_case",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            name="fk_inbox_command_message",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["outbound_intent_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            name="fk_inbox_command_delivery",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_inbox_command_receipts"),
        sa.UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "conversation_id",
            "actor_account_id",
            "command_id",
            name="uq_inbox_command_scope",
        ),
    )
    op.create_index("ix_inbox_command_case", "inbox_command_receipts", ["case_id", "created_at"])


def downgrade() -> None:
    connection = op.get_bind()
    retained = connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM handoff_cases) OR "
            "EXISTS (SELECT 1 FROM inbox_command_receipts) OR "
            "EXISTS (SELECT 1 FROM inbox_grant_lifecycle_receipts) OR "
            "EXISTS (SELECT 1 FROM inbox_grant_scopes WHERE grant_generation > 0 OR "
            "cp_binding_high_water IS NOT NULL OR agent_lifecycle_high_water IS NOT NULL) OR "
            "EXISTS (SELECT 1 FROM conversations WHERE handoff_resume_floor_sequence IS NOT NULL) "
            "OR EXISTS (SELECT 1 FROM agent_turns WHERE managed_contract_version = 3 "
            "OR managed_v3_snapshot IS NOT NULL OR managed_v3_dispatch_id IS NOT NULL "
            "OR retired_at IS NOT NULL) "
            "OR EXISTS (SELECT 1 FROM delivery_attempts WHERE source_message_id IS NOT NULL "
            "OR writer_state <> 'NONE')"
        )
    )
    if retained:
        raise RuntimeError("Retained handoff evidence prevents downgrade")
    op.drop_constraint("ck_turn_catch_up_version", "agent_turns", type_="check")
    op.create_check_constraint(
        "ck_turn_catch_up_version",
        "agent_turns",
        "catch_up_version IN (0,1) AND (catch_up_version=0 OR "
        "(settling_burst_id IS NOT NULL AND managed_contract_version=2))",
    )
    op.drop_index("ix_inbox_command_case", table_name="inbox_command_receipts")
    op.drop_table("inbox_command_receipts")
    op.drop_index("ix_inbox_grant_lifecycle_scope", table_name="inbox_grant_lifecycle_receipts")
    op.drop_table("inbox_grant_lifecycle_receipts")
    op.drop_index("ix_handoff_case_owner_status", table_name="handoff_cases")
    op.drop_index("uq_handoff_one_active_case", table_name="handoff_cases")
    op.drop_table("handoff_cases")
    op.drop_table("inbox_grant_scopes")
    op.drop_constraint("ck_delivery_human_binding", "delivery_attempts", type_="check")
    op.drop_constraint("ck_delivery_writer_state", "delivery_attempts", type_="check")
    op.drop_constraint("ck_delivery_outbound_ordinal", "delivery_attempts", type_="check")
    op.drop_constraint("ck_delivery_exact_source", "delivery_attempts", type_="check")
    op.drop_constraint(
        "fk_delivery_source_message_conversation", "delivery_attempts", type_="foreignkey"
    )
    op.drop_constraint("uq_delivery_outbound_ordinal", "delivery_attempts", type_="unique")
    op.drop_constraint("uq_delivery_source_message", "delivery_attempts", type_="unique")
    op.drop_column("delivery_attempts", "writer_ended_at")
    op.drop_column("delivery_attempts", "writer_started_at")
    op.drop_column("delivery_attempts", "writer_process_incarnation_id")
    op.drop_column("delivery_attempts", "writer_supplier_instance_id")
    op.drop_column("delivery_attempts", "writer_state")
    op.drop_column("delivery_attempts", "grant_generation")
    op.drop_column("delivery_attempts", "assignment_revision")
    op.drop_column("delivery_attempts", "case_revision")
    op.drop_column("delivery_attempts", "case_id")
    op.drop_column("delivery_attempts", "outbound_ordinal")
    op.drop_column("delivery_attempts", "source_message_id")
    op.alter_column("delivery_attempts", "source_turn_id", nullable=False)
    op.drop_index("uq_turn_one_active", table_name="agent_turns")
    op.drop_column("agent_turns", "retired_at")
    op.drop_constraint("ck_turn_managed_v3_snapshot", "agent_turns", type_="check")
    op.drop_constraint("ck_turn_managed_projection", "agent_turns", type_="check")
    # Restore the exact pre-Handoff constraint before dropping the v3 columns.
    op.create_check_constraint(
        "ck_turn_managed_projection",
        "agent_turns",
        "(managed_contract_version = 1 AND managed_context_first_sequence = 1 "
        "AND managed_projection_agent_id IS NULL AND managed_projection_fingerprint IS NULL) "
        "OR (managed_contract_version = 2 AND managed_context_first_sequence >= 1 "
        "AND managed_context_first_sequence <= input_first_sequence "
        "AND managed_projection_agent_id IS NOT NULL "
        "AND managed_projection_fingerprint IS NOT NULL "
        "AND managed_projection_fingerprint ~ '^[0-9a-f]{64}$')",
    )
    op.drop_column("agent_turns", "managed_v3_dispatch_id")
    op.drop_column("agent_turns", "managed_v3_request_fingerprint")
    op.drop_column("agent_turns", "managed_v3_snapshot")
    op.create_index(
        "uq_turn_one_active",
        "agent_turns",
        ["conversation_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('PENDING','CLAIMED')"),
    )
    op.drop_constraint("ck_conversation_handoff_resume_floor", "conversations", type_="check")
    op.drop_column("conversations", "handoff_resume_floor_sequence")
