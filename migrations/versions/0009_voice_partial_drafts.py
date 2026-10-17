"""Voice partial draft column and per-lease response ordinals."""

import sqlalchemy as sa
from alembic import op

revision = "0009_voice_partial_drafts"
down_revision = "0008_business_effect_permits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_turns", sa.Column("partial_draft_text", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_turn_partial_draft_bounds",
        "agent_turns",
        "char_length(partial_draft_text) BETWEEN 1 AND 16384 AND "
        "octet_length(partial_draft_text) <= 32768",
    )

    op.add_column(
        "voice_responses",
        sa.Column("ordinal", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "voice_responses",
        sa.Column("is_partial", sa.Boolean(), server_default=sa.text("false"), nullable=False),
    )
    op.add_column("voice_responses", sa.Column("authorized_text", sa.Text(), nullable=True))
    op.execute(sa.text("ALTER TABLE voice_responses DROP CONSTRAINT voice_responses_turn_id_key"))
    op.create_unique_constraint(
        "uq_voice_response_turn_ordinal", "voice_responses", ["turn_id", "ordinal"]
    )
    op.create_check_constraint(
        "ck_voice_response_ordinal",
        "voice_responses",
        "ordinal >= 0",
    )
    op.create_check_constraint(
        "ck_voice_response_authorized_text",
        "voice_responses",
        "(authorized_text IS NULL) OR ("
        "char_length(authorized_text) BETWEEN 1 AND 16384 AND "
        "octet_length(authorized_text) <= 32768)",
    )
    op.create_check_constraint(
        "ck_voice_response_partial_shape",
        "voice_responses",
        "(is_partial = false) OR (send_authorized = true AND authorized_text IS NOT NULL)",
    )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(sa.text(
        "LOCK TABLE agent_turns, voice_responses IN ACCESS EXCLUSIVE MODE"
    ))
    multi = connection.scalar(sa.text(
        "SELECT EXISTS ("
        "SELECT 1 FROM voice_responses GROUP BY turn_id HAVING count(*) > 1)"
    ))
    partials = connection.scalar(sa.text(
        "SELECT EXISTS (SELECT 1 FROM agent_turns WHERE partial_draft_text IS NOT NULL)"
    ))
    if multi or partials:
        raise RuntimeError("Voice partial draft evidence is retained; downgrade refused")
    op.drop_constraint("ck_voice_response_partial_shape", "voice_responses", type_="check")
    op.drop_constraint("ck_voice_response_authorized_text", "voice_responses", type_="check")
    op.drop_constraint("ck_voice_response_ordinal", "voice_responses", type_="check")
    op.drop_constraint("uq_voice_response_turn_ordinal", "voice_responses", type_="unique")
    op.create_unique_constraint("voice_responses_turn_id_key", "voice_responses", ["turn_id"])
    op.drop_column("voice_responses", "authorized_text")
    op.drop_column("voice_responses", "is_partial")
    op.drop_column("voice_responses", "ordinal")
    op.drop_constraint("ck_turn_partial_draft_bounds", "agent_turns", type_="check")
    op.drop_column("agent_turns", "partial_draft_text")
