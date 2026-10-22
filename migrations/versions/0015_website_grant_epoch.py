"""Retain Website credentials in the existing Inbox grant epoch and proof floors."""

import sqlalchemy as sa
from alembic import op

revision = "0015_website_grant_epoch"
down_revision = "0014_website_session"
branch_labels = None
depends_on = None

CONSTRAINTS = (
    ("inbox_grant_scopes", "ck_inbox_grant_channel"),
    ("inbox_grant_lifecycle_receipts", "ck_inbox_grant_lifecycle_channel"),
    ("handoff_cases", "ck_handoff_case_channel"),
    ("inbox_command_receipts", "ck_inbox_command_channel"),
)


def upgrade() -> None:
    for table, name in CONSTRAINTS:
        op.drop_constraint(name, table, type_="check")
        op.create_check_constraint(name, table, "channel IN ('TELEGRAM','WEBSITE_WIDGET')")
    op.add_column("website_session_capabilities", sa.Column("grant_generation", sa.BigInteger()))
    op.add_column(
        "website_session_capabilities", sa.Column("agent_lifecycle_generation", sa.BigInteger())
    )
    op.add_column("website_session_capabilities", sa.Column("provision_command_id", sa.UUID()))
    # Existing foundation credentials stay unpromoted. No inferred epoch/backfill.
    op.create_check_constraint(
        "ck_website_session_grant_epoch",
        "website_session_capabilities",
        "(grant_generation IS NULL AND agent_lifecycle_generation IS NULL "
        "AND provision_command_id IS NULL) OR "
        "(grant_generation IS NOT NULL AND agent_lifecycle_generation IS NOT NULL "
        "AND grant_generation > 0 AND agent_lifecycle_generation >= 0 "
        "AND provision_command_id IS NOT NULL)",
    )
    op.create_foreign_key(
        "fk_website_session_grant_provision",
        "website_session_capabilities",
        "inbox_grant_lifecycle_receipts",
        ["owner_id", "integration_id", "channel", "agent_id", "provision_command_id"],
        ["owner_id", "integration_id", "channel", "agent_id", "command_id"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    # Match issuance/lifecycle scope-before-root ordering. A concurrent writer
    # must commit before the evidence check, or wait until the old schema exists.
    for table, _ in CONSTRAINTS:
        bind.execute(sa.text(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE conversations IN SHARE ROW EXCLUSIVE MODE"))
    bind.execute(sa.text("LOCK TABLE website_session_capabilities IN ACCESS EXCLUSIVE MODE"))
    retained = bind.execute(sa.text(
        "SELECT EXISTS (SELECT 1 FROM website_session_capabilities)"
    )).scalar()
    for table, _ in CONSTRAINTS:
        retained = retained or bind.execute(sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {table} WHERE channel='WEBSITE_WIDGET')"
        )).scalar()
    if retained:
        raise RuntimeError(
            "Retained Website session credentials and common grant epochs prevent downgrade"
        )
    op.drop_constraint("fk_website_session_grant_provision", "website_session_capabilities",
                       type_="foreignkey")
    op.drop_constraint("ck_website_session_grant_epoch", "website_session_capabilities",
                       type_="check")
    for name in ("provision_command_id", "agent_lifecycle_generation", "grant_generation"):
        op.drop_column("website_session_capabilities", name)
    for table, name in CONSTRAINTS:
        op.drop_constraint(name, table, type_="check")
        op.create_check_constraint(name, table, "channel = 'TELEGRAM'")
