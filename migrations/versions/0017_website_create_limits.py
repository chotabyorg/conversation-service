"""Durable anonymous Website creation admission, shared across workers/restart."""
import sqlalchemy as sa
from alembic import op

revision = "0017_website_create_limits"
down_revision = "0016_website_visibility"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_website_session_unexpired", "website_session_capabilities",
                    ["owner_id", "integration_id", "expires_at"])
    op.create_table("website_creation_buckets",
        sa.Column("owner_id", sa.String(255, collation="C"), primary_key=True),
        sa.Column("integration_id", sa.UUID(), primary_key=True),
        sa.Column("subject_digest", sa.CHAR(64), primary_key=True),
        sa.Column("theoretical_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("subject_digest ~ '^[0-9a-f]{64}$'",
                           name="ck_website_create_bucket_digest"),
    )
    op.create_index("ix_website_create_bucket_expiry", "website_creation_buckets",
                    ["owner_id", "integration_id", "theoretical_at"])


def downgrade() -> None:
    bind = op.get_bind()
    # Same lifecycle -> root -> credential order as 0015/0016, before child DDL.
    for table in ("inbox_grant_scopes", "inbox_grant_lifecycle_receipts",
                  "handoff_cases", "inbox_command_receipts"):
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE conversations IN SHARE ROW EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_session_capabilities IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_creation_buckets IN ACCESS EXCLUSIVE MODE"))
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM website_creation_buckets)")):
        raise RuntimeError("Retained Website creation admission requires a compatible reader")
    op.drop_table("website_creation_buckets")
    op.drop_index("ix_website_session_unexpired", "website_session_capabilities")
