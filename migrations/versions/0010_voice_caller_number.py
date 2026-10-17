"""Retained Voice caller number (ANI) for booking contact."""

import sqlalchemy as sa
from alembic import op

revision = "0010_voice_caller_number"
down_revision = "0009_voice_partial_drafts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("voice_calls", sa.Column("caller_number", sa.String(16), nullable=True))
    op.create_check_constraint(
        "ck_voice_caller_number",
        "voice_calls",
        r"caller_number IS NULL OR caller_number ~ '^\+375[0-9]{9}$'",
    )


def downgrade() -> None:
    op.drop_constraint("ck_voice_caller_number", "voice_calls", type_="check")
    op.drop_column("voice_calls", "caller_number")
