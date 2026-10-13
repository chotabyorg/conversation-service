"""Add sticky Telegram canary admission, immediate turns and provider evidence."""

import sqlalchemy as sa
from alembic import op

revision = "0005_telegram_canary"
down_revision = "0004_website_history"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "telegram_admissions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False, server_default="TELEGRAM"),
        sa.Column("provider_namespace", sa.String(128, collation="C"), nullable=False),
        sa.Column("provider_event_id", sa.String(512, collation="C"), nullable=False),
        sa.Column("provider_thread_id", sa.String(512, collation="C"), nullable=False),
        sa.Column("update_id", sa.BigInteger(), nullable=False),
        sa.Column("chat_id", sa.String(64, collation="C"), nullable=False),
        sa.Column("message_id", sa.String(64, collation="C"), nullable=False),
        sa.Column("sender_id", sa.String(64, collation="C"), nullable=False),
        sa.Column("bot_id", sa.String(64, collation="C"), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("route", sa.String(16), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=True),
        sa.Column("canonical_message_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
        sa.UniqueConstraint("owner_id", "integration_id", "channel", "provider_namespace",
                            "provider_event_id", name="uq_telegram_admission_identity"),
        sa.UniqueConstraint("canonical_message_id", name="uq_telegram_admission_message"),
        sa.UniqueConstraint("id", "conversation_id", name="uq_telegram_admission_parent"),
        sa.ForeignKeyConstraint(["conversation_id", "owner_id", "integration_id", "channel"],
                                ["conversations.id", "conversations.owner_id",
                                 "conversations.integration_id", "conversations.channel"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["canonical_message_id", "conversation_id"],
                                ["messages.id", "messages.conversation_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("char_length(owner_id) BETWEEN 1 AND 255 AND "
                           "octet_length(owner_id)<=1020 AND owner_id<>'*'",
                           name="ck_telegram_admission_owner"),
        sa.CheckConstraint("channel='TELEGRAM' AND "
                           "provider_namespace='main-chota.telegram.v1'",
                           name="ck_telegram_admission_namespace"),
        sa.CheckConstraint("fingerprint ~ '^[0-9a-f]{64}$'",
                           name="ck_telegram_admission_fingerprint"),
        sa.CheckConstraint("char_length(provider_event_id) BETWEEN 1 AND 512 AND "
                           "octet_length(provider_event_id)<=512",
                           name="ck_telegram_admission_event_bounds"),
        sa.CheckConstraint("char_length(provider_thread_id) BETWEEN 1 AND 512 AND "
                           "octet_length(provider_thread_id)<=512",
                           name="ck_telegram_admission_thread_bounds"),
        sa.CheckConstraint("update_id>=0", name="ck_telegram_admission_update_id"),
        sa.CheckConstraint("chat_id ~ '^[1-9][0-9]{0,18}$' AND "
                           "message_id ~ '^[1-9][0-9]{0,18}$' AND "
                           "sender_id ~ '^[1-9][0-9]{0,18}$' AND "
                           "bot_id ~ '^[1-9][0-9]{0,18}$'",
                           name="ck_telegram_admission_provider_ids"),
        sa.CheckConstraint("(route='LEGACY' AND conversation_id IS NULL AND "
                           "canonical_message_id IS NULL) OR (route='MANAGED' AND "
                           "conversation_id IS NOT NULL AND canonical_message_id IS NOT NULL)",
                           name="ck_telegram_admission_route"),
    )
    op.create_index("ix_telegram_admission_processing", "telegram_admissions", ["conversation_id"])
    op.create_table(
        "telegram_immediate_turns",
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.Uuid(), nullable=False, unique=True),
        sa.ForeignKeyConstraint(["message_id"], ["telegram_admissions.canonical_message_id"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["message_id", "conversation_id"],
                                ["messages.id", "messages.conversation_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["turn_id", "conversation_id"],
                                ["agent_turns.id", "agent_turns.conversation_id"],
                                ondelete="RESTRICT"),
    )
    op.create_table(
        "telegram_response_attempts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("admission_id", sa.Uuid(), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["admission_id", "conversation_id"],
                                ["telegram_admissions.id", "telegram_admissions.conversation_id"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["delivery_id", "conversation_id"],
                                ["delivery_attempts.id", "delivery_attempts.conversation_id"],
                                ondelete="RESTRICT"),
        sa.CheckConstraint("(outcome IS NULL AND finished_at IS NULL) OR "
                           "(outcome IN ('PROVIDER_ACCEPTED','FAILED','UNKNOWN') AND "
                           "finished_at IS NOT NULL)", name="ck_telegram_response_outcome"),
    )


def downgrade() -> None:
    op.execute("LOCK TABLE telegram_admissions IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(
            "SELECT EXISTS(SELECT 1 FROM telegram_admissions)")):
        raise RuntimeError("Retained Telegram admission prevents downgrade.")
    op.drop_table("telegram_response_attempts")
    op.drop_table("telegram_immediate_turns")
    op.drop_index("ix_telegram_admission_processing", table_name="telegram_admissions")
    op.drop_table("telegram_admissions")
