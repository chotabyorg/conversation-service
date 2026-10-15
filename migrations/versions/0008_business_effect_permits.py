"""Add Conversation-owned immutable external-effect permits and epoch input floors."""

import sqlalchemy as sa
from alembic import op

revision = "0008_business_effect_permits"
down_revision = "0007_voice_bridge"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "conversations",
        sa.Column(
            "authority_input_floor_sequence",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.execute(sa.text(
        "UPDATE conversations SET authority_input_floor_sequence=last_message_sequence "
        "WHERE authority_revision > 0"
    ))
    op.create_check_constraint(
        "ck_conversation_authority_input_floor",
        "conversations",
        "authority_input_floor_sequence >= 0 AND "
        "authority_input_floor_sequence <= last_message_sequence",
    )

    op.create_index(
        "ix_settling_burst_conversation_tail",
        "settling_bursts",
        ["conversation_id", sa.text("tail_sequence DESC")],
    )
    op.create_index(
        "ix_message_burst_sequence",
        "messages",
        ["conversation_id", "settling_burst_id", "sequence"],
    )
    op.create_index(
        "ix_turn_conversation_settling_burst",
        "agent_turns",
        ["conversation_id", "settling_burst_id"],
    )

    op.create_table(
        "external_effect_permits",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.Uuid(), nullable=False),
        sa.Column("origin_turn_id", sa.Uuid(), nullable=False),
        sa.Column("operation_namespace", sa.String(64, collation="C"), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("operation_fingerprint", sa.CHAR(64), nullable=False),
        sa.Column("request_fingerprint", sa.CHAR(64), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("authority_input_floor_sequence", sa.BigInteger(), nullable=False),
        sa.Column("split_input_floor_sequence", sa.BigInteger(), nullable=False),
        sa.Column(
            "issued_at", sa.DateTime(timezone=True), server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "operation_namespace = 'main-chota.ai-business.v1'",
            name="ck_effect_permit_namespace",
        ),
        sa.CheckConstraint(
            "operation_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_effect_permit_operation_fingerprint",
        ),
        sa.CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_effect_permit_request_fingerprint",
        ),
        sa.CheckConstraint(
            "authority_revision >= 0 AND authority_input_floor_sequence >= 0 "
            "AND split_input_floor_sequence >= 0",
            name="ck_effect_permit_floors",
        ),
        sa.ForeignKeyConstraint(
            ["conversation_id"], ["conversations.id"],
            name="fk_effect_permit_conversation", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_effect_permit_turn", ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["origin_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_effect_permit_origin_turn", ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_external_effect_permits"),
        sa.UniqueConstraint("id", "conversation_id", name="uq_effect_permit_id_conversation"),
        sa.UniqueConstraint(
            "operation_namespace", "operation_id", name="uq_effect_permit_operation"
        ),
    )
    op.create_index(
        "ix_effect_permit_turn", "external_effect_permits",
        ["conversation_id", "turn_id", "issued_at"],
    )
    op.execute("""CREATE FUNCTION immutable_external_effect_permit() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      RAISE EXCEPTION 'External effect permit evidence is immutable.' USING ERRCODE='23514';
    END $$""")
    op.execute(
        "CREATE TRIGGER immutable_external_effect_permit BEFORE UPDATE OR DELETE "
        "ON external_effect_permits FOR EACH ROW "
        "EXECUTE FUNCTION immutable_external_effect_permit()"
    )


def downgrade() -> None:
    op.execute("LOCK TABLE external_effect_permits, conversations IN ACCESS EXCLUSIVE MODE")
    connection = op.get_bind()
    has_permits = connection.scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM external_effect_permits)")
    )
    has_floor_evidence = connection.scalar(sa.text(
        "SELECT EXISTS (SELECT 1 FROM conversations "
        "WHERE authority_input_floor_sequence <> 0)"
    ))
    if has_permits or has_floor_evidence:
        raise RuntimeError("Business effect authority evidence is retained; downgrade refused")
    op.execute("DROP TRIGGER immutable_external_effect_permit ON external_effect_permits")
    op.execute("DROP FUNCTION immutable_external_effect_permit()")
    op.drop_index("ix_effect_permit_turn", table_name="external_effect_permits")
    op.drop_table("external_effect_permits")
    op.drop_index("ix_turn_conversation_settling_burst", table_name="agent_turns")
    op.drop_index("ix_message_burst_sequence", table_name="messages")
    op.drop_index("ix_settling_burst_conversation_tail", table_name="settling_bursts")
    op.drop_constraint(
        "ck_conversation_authority_input_floor", "conversations", type_="check"
    )
    op.drop_column("conversations", "authority_input_floor_sequence")
