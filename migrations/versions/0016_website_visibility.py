"""Retain bounded Website pagination across late original transport acceptance."""
import sqlalchemy as sa
from alembic import op

revision = "0016_website_visibility"
down_revision = "0015_website_grant_epoch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("website_visible_messages",
        sa.Column("conversation_id", sa.UUID(), primary_key=True),
        sa.Column("ordinal", sa.BigInteger(), primary_key=True),
        sa.Column("message_id", sa.UUID(), nullable=False),
        sa.Column("public_message_id", sa.UUID(), nullable=False),
        sa.Column("delivery_attempt_id", sa.UUID()),
        sa.UniqueConstraint("message_id", name="uq_website_visible_message"),
        sa.UniqueConstraint("public_message_id", name="uq_website_visible_public_id"),
        sa.CheckConstraint("ordinal > 0", name="ck_website_visible_ordinal"),
        sa.ForeignKeyConstraint(["conversation_id"],
            ["website_session_capabilities.conversation_id"],
            name="fk_website_visible_credential", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            name="fk_website_visible_message", ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["delivery_attempt_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            name="fk_website_visible_delivery", ondelete="RESTRICT"),
    )
    op.execute("""
        CREATE FUNCTION website_visibility_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'Website visibility positions are immutable' USING ERRCODE='23514';
        END $$;
    """)
    op.execute("""
        CREATE TRIGGER website_visibility_immutable BEFORE UPDATE OR DELETE
        ON website_visible_messages FOR EACH ROW EXECUTE FUNCTION website_visibility_immutable();
    """)


def downgrade() -> None:
    bind = op.get_bind()
    # Dropping a child FK also locks its parents. Match 0015's issuance/lifecycle
    # order before taking any child lock, including an empty visibility index.
    for table in ("inbox_grant_scopes", "inbox_grant_lifecycle_receipts",
                  "handoff_cases", "inbox_command_receipts"):
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE conversations IN SHARE ROW EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_session_capabilities IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_visible_messages IN ACCESS EXCLUSIVE MODE"))
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM website_visible_messages)")):
        raise RuntimeError("Retained Website visibility positions require a compatible reader")
    op.drop_table("website_visible_messages")
    op.execute("DROP FUNCTION website_visibility_immutable()")
