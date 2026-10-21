"""Bind create-only Website visitor credentials to exact canonical roots."""
import sqlalchemy as sa
from alembic import op

revision = "0014_website_session"
down_revision = "0013_writer_retirement"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_conversation_transport_identity", "conversations",
        ["id", "owner_id", "integration_id", "channel", "provider_thread_id"])
    op.create_table(
        "website_session_capabilities",
        sa.Column("conversation_id", sa.UUID(), primary_key=True),
        sa.Column("owner_id", sa.String(255, collation="C"), nullable=False),
        sa.Column("integration_id", sa.UUID(), nullable=False),
        sa.Column("channel", sa.String(32, collation="C"), nullable=False),
        sa.Column("provider_thread_id", sa.String(512, collation="C"), nullable=False),
        sa.Column("agent_id", sa.UUID(), nullable=False),
        sa.Column("binding_generation", sa.BigInteger(), nullable=False),
        sa.Column("token_digest", sa.CHAR(64), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True)),
        sa.ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel", "provider_thread_id"],
            ["conversations.id", "conversations.owner_id", "conversations.integration_id",
             "conversations.channel", "conversations.provider_thread_id"],
            name="fk_website_session_canonical_root"),
        sa.UniqueConstraint("token_digest", name="uq_website_session_digest"),
        sa.CheckConstraint("channel = 'WEBSITE_WIDGET'", name="ck_website_session_channel"),
        sa.CheckConstraint("binding_generation > 0", name="ck_website_session_generation"),
        sa.CheckConstraint("token_digest ~ '^[0-9a-f]{64}$'", name="ck_website_session_digest"),
        sa.CheckConstraint(
            "expires_at > issued_at AND expires_at <= issued_at + interval '24 hours' "
            "AND (revoked_at IS NULL OR revoked_at >= issued_at)",
            name="ck_website_session_clock"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Issuance inserts the canonical parent before its credential. Preserve that
    # lock order and stop concurrent root creation before checking emptiness.
    bind.execute(sa.text("LOCK TABLE conversations IN SHARE ROW EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_session_capabilities IN ACCESS EXCLUSIVE MODE"))
    if bind.execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM website_session_capabilities)"
    )).scalar():
        raise RuntimeError("Retained Website session credentials prevent downgrade")
    op.drop_table("website_session_capabilities")
    op.drop_constraint("uq_conversation_transport_identity", "conversations", type_="unique")
