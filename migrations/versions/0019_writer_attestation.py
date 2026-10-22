"""Append-only lifecycle evidence; no historical proof or send authority backfill."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0019_writer_attestation"
down_revision = "0018_website_poll_admission"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "writer_registrations",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("intent_id", sa.UUID(), sa.ForeignKey("delivery_attempts.id"), nullable=False),
        sa.Column("producer_id", sa.String(128), nullable=False),
        sa.Column("supplier_instance_id", sa.UUID(), nullable=False),
        sa.Column("process_incarnation_id", sa.UUID(), nullable=False),
        sa.Column("descriptor", postgresql.JSONB(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "intent_id",
            "supplier_instance_id",
            "process_incarnation_id",
            name="uq_writer_registration_tuple",
        ),
    )
    op.create_table(
        "writer_witnesses",
        sa.Column(
            "registration_id", sa.UUID(), sa.ForeignKey("writer_registrations.id"), primary_key=True
        ),
        sa.Column("proof_ref", sa.UUID(), nullable=False, unique=True),
        sa.Column("witness", postgresql.JSONB(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "writer_recovery_commands",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "registration_id",
            sa.UUID(),
            sa.ForeignKey("writer_witnesses.registration_id"),
            nullable=False,
        ),
        sa.Column("expected_revision", sa.BigInteger(), nullable=False),
        sa.Column("command", postgresql.JSONB(), nullable=False),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "registration_id", "expected_revision", name="uq_writer_recovery_revision"
        ),
    )
    op.execute("""CREATE FUNCTION forbid_writer_evidence_mutation() RETURNS trigger AS $$
    BEGIN RAISE EXCEPTION 'immutable writer evidence'; END; $$ LANGUAGE plpgsql""")
    for table in ("writer_registrations", "writer_witnesses", "writer_recovery_commands"):
        op.execute(
            f"CREATE TRIGGER immutable_writer_evidence BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION forbid_writer_evidence_mutation()"
        )


def downgrade() -> None:
    for table in ("writer_registrations", "writer_witnesses", "writer_recovery_commands"):
        op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM writer_registrations)")):
        raise RuntimeError("Retained writer evidence requires a compatible reader")
    for table in ("writer_recovery_commands", "writer_witnesses", "writer_registrations"):
        op.drop_table(table)
    op.execute("DROP FUNCTION forbid_writer_evidence_mutation()")
