"""Retain supervisor retirement evidence on the original outbound intent."""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0013_writer_retirement"
down_revision = "0012_shared_handoff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("delivery_attempts", sa.Column("writer_retirement_command_id", sa.UUID()))
    op.add_column("delivery_attempts", sa.Column("writer_retirement_fingerprint", sa.CHAR(64)))
    op.add_column("delivery_attempts", sa.Column("writer_retirement_receipt", postgresql.JSONB()))
    op.create_unique_constraint("uq_delivery_writer_retirement_command", "delivery_attempts",
                                ["conversation_id", "writer_retirement_command_id"])
    op.create_check_constraint("ck_delivery_writer_retirement_receipt", "delivery_attempts",
        "(writer_retirement_command_id IS NULL AND writer_retirement_fingerprint IS NULL "
        "AND writer_retirement_receipt IS NULL) OR (writer_state='RETIRED' "
        "AND writer_retirement_command_id IS NOT NULL "
        "AND writer_retirement_fingerprint IS NOT NULL "
        "AND writer_retirement_fingerprint ~ '^[0-9a-f]{64}$' "
        "AND writer_retirement_receipt IS NOT NULL "
        "AND jsonb_typeof(writer_retirement_receipt)='object')")


def downgrade() -> None:
    if op.get_bind().execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM delivery_attempts "
        "WHERE writer_retirement_command_id IS NOT NULL)"
    )).scalar():
        raise RuntimeError("Retained writer retirement evidence prevents downgrade")
    op.drop_constraint("ck_delivery_writer_retirement_receipt", "delivery_attempts", type_="check")
    op.drop_constraint("uq_delivery_writer_retirement_command", "delivery_attempts", type_="unique")
    for name in ("writer_retirement_receipt", "writer_retirement_fingerprint",
                 "writer_retirement_command_id"):
        op.drop_column("delivery_attempts", name)
