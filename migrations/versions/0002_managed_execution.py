"""Add same-Turn one-attempt execution metadata without changing Goal1 journals."""

import sqlalchemy as sa
from alembic import op

revision = "0002_managed_execution"
down_revision = "0001_foundation"
branch_labels = None
depends_on = None

MARKER_CHECK = """
 (managed_agent_id IS NULL AND managed_dispatch_epoch IS NULL
  AND managed_dispatch_started_at IS NULL AND managed_request_fingerprint IS NULL)
 OR (managed_agent_id IS NOT NULL AND managed_dispatch_epoch IS NOT NULL
  AND managed_dispatch_started_at IS NOT NULL AND managed_request_fingerprint IS NOT NULL
  AND managed_dispatch_epoch >= 1 AND managed_dispatch_epoch <= claim_epoch
  AND status <> 'PENDING' AND started_at IS NOT NULL
  AND managed_dispatch_started_at >= started_at
  AND managed_request_fingerprint ~ '^[0-9a-f]{64}$')
"""


def upgrade() -> None:
    op.add_column("agent_turns", sa.Column("managed_agent_id", sa.Uuid(), nullable=True))
    op.add_column(
        "agent_turns", sa.Column("managed_dispatch_epoch", sa.BigInteger(), nullable=True)
    )
    op.add_column(
        "agent_turns",
        sa.Column("managed_dispatch_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "agent_turns", sa.Column("managed_request_fingerprint", sa.CHAR(64), nullable=True)
    )
    op.create_check_constraint("ck_turn_managed_marker", "agent_turns", MARKER_CHECK)
    op.drop_constraint("ck_turn_failure", "agent_turns", type_="check")
    op.create_check_constraint(
        "ck_turn_failure",
        "agent_turns",
        "failure_code IN ('PROCESSING_ERROR','TIMEOUT','INVALID_RESULT','EXECUTION_UNKNOWN')",
    )


def downgrade() -> None:
    # Lock against concurrent writers for the check and entire DDL transaction.
    op.execute("LOCK TABLE agent_turns IN ACCESS EXCLUSIVE MODE")
    occupied = op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM agent_turns WHERE managed_agent_id IS NOT NULL "
            "OR managed_dispatch_epoch IS NOT NULL OR managed_dispatch_started_at IS NOT NULL "
            "OR managed_request_fingerprint IS NOT NULL OR failure_code='EXECUTION_UNKNOWN')"
        )
    )
    if occupied:
        raise RuntimeError("Managed execution evidence prevents downgrade.")
    op.drop_constraint("ck_turn_managed_marker", "agent_turns", type_="check")
    op.drop_constraint("ck_turn_failure", "agent_turns", type_="check")
    op.create_check_constraint(
        "ck_turn_failure",
        "agent_turns",
        "failure_code IN ('PROCESSING_ERROR','TIMEOUT','INVALID_RESULT')",
    )
    for name in (
        "managed_request_fingerprint",
        "managed_dispatch_started_at",
        "managed_dispatch_epoch",
        "managed_agent_id",
    ):
        op.drop_column("agent_turns", name)
