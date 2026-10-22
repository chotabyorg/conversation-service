"""Current durable poll quota/reservation; no seed and no writer termination fact."""
import sqlalchemy as sa
from alembic import op

revision = "0018_website_poll_admission"
down_revision = "0017_website_create_limits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table("website_poll_states",
        sa.Column("conversation_id", sa.UUID(), sa.ForeignKey("conversations.id"),
                  primary_key=True),
        sa.Column("request_id", sa.UUID(), nullable=False),
        sa.Column("poll_id", sa.UUID(), nullable=False, unique=True),
        sa.Column("supplier_instance_id", sa.UUID(), nullable=False),
        sa.Column("process_incarnation_id", sa.UUID(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("theoretical_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("deadline_at > started_at AND "
            "deadline_at <= started_at + INTERVAL '20 seconds'", name="ck_website_poll_deadline"),
        sa.CheckConstraint("ended_at IS NULL OR ended_at >= started_at",
                           name="ck_website_poll_end"),
    )


def downgrade() -> None:
    bind = op.get_bind()
    for table in ("inbox_grant_scopes", "inbox_grant_lifecycle_receipts",
                  "handoff_cases", "inbox_command_receipts"):
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE conversations IN SHARE ROW EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_session_capabilities IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_poll_states IN ACCESS EXCLUSIVE MODE"))
    if bind.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM website_poll_states)")):
        raise RuntimeError("Retained Website poll admission requires a compatible reader")
    op.drop_table("website_poll_states")
