"""Add frozen Website arbitration, immediate selection and local HTTP evidence."""
import sqlalchemy as sa
from alembic import op

revision = "0003_website_canary"
down_revision = "0002_managed_execution"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_message_id_conversation", "messages", ["id", "conversation_id"])
    op.create_table(
        "website_admissions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.Uuid(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False, server_default="WEBSITE_WIDGET"),
        sa.Column("provider_namespace", sa.String(128), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.Uuid(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("route", sa.String(16), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=True),
        sa.Column("canonical_message_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
        sa.UniqueConstraint("owner_id", "integration_id", "channel", "provider_namespace",
                            "message_id", name="uq_website_admission_identity"),
        sa.UniqueConstraint("canonical_message_id", name="uq_website_admission_message"),
        sa.UniqueConstraint("id", "conversation_id", name="uq_website_admission_parent"),
        sa.ForeignKeyConstraint(["conversation_id", "owner_id", "integration_id", "channel"],
                                ["conversations.id", "conversations.owner_id",
                                 "conversations.integration_id", "conversations.channel"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["canonical_message_id", "conversation_id"],
                                ["messages.id", "messages.conversation_id"], ondelete="RESTRICT"),
        sa.CheckConstraint("char_length(owner_id) BETWEEN 1 AND 255 "
                           "AND octet_length(owner_id)<=1020 AND owner_id<>'*'",
                           name="ck_website_admission_owner"),
        sa.CheckConstraint("channel='WEBSITE_WIDGET' AND "
                           "provider_namespace='main-chota.website-widget.v1'",
                           name="ck_website_admission_namespace"),
        sa.CheckConstraint("fingerprint ~ '^[0-9a-f]{64}$'",
                           name="ck_website_admission_fingerprint"),
        sa.CheckConstraint("(route='LEGACY' AND conversation_id IS NULL "
                           "AND canonical_message_id IS NULL) OR (route='MANAGED' "
                           "AND conversation_id IS NOT NULL AND canonical_message_id IS NOT NULL)",
                           name="ck_website_admission_route"),
    )
    op.create_index("ix_website_admission_processing", "website_admissions", ["conversation_id"])
    op.create_table(
        "website_immediate_turns",
        sa.Column("message_id", sa.Uuid(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("turn_id", sa.Uuid(), nullable=False, unique=True),
        sa.ForeignKeyConstraint(["message_id"], ["website_admissions.canonical_message_id"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["message_id", "conversation_id"],
                                ["messages.id", "messages.conversation_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["turn_id", "conversation_id"],
                                ["agent_turns.id", "agent_turns.conversation_id"],
                                ondelete="RESTRICT"),
    )
    op.create_table(
        "website_response_attempts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("admission_id", sa.Uuid(), nullable=False),
        sa.Column("delivery_id", sa.Uuid(), nullable=False),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("clock_timestamp()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["admission_id", "conversation_id"],
                                ["website_admissions.id", "website_admissions.conversation_id"],
                                ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["delivery_id", "conversation_id"],
                                ["delivery_attempts.id", "delivery_attempts.conversation_id"],
                                ondelete="RESTRICT"),
        sa.CheckConstraint("(outcome IS NULL AND finished_at IS NULL) OR "
                           "(outcome IN ('WRITE_COMPLETED','WRITE_FAILED','UNKNOWN') "
                           "AND finished_at IS NOT NULL)", name="ck_website_response_outcome"),
    )


def downgrade() -> None:
    op.execute("LOCK TABLE website_admissions IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text("SELECT EXISTS(SELECT 1 FROM website_admissions)")):
        raise RuntimeError("Retained Website admission prevents downgrade.")
    op.drop_table("website_response_attempts")
    op.drop_table("website_immediate_turns")
    op.drop_table("website_admissions")
    op.drop_constraint("uq_message_id_conversation", "messages", type_="unique")
