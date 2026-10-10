"""Freeze new Website v2 windows and retain source-Turn assistant Messages."""
import sqlalchemy as sa
from alembic import op

revision = "0004_website_history"
down_revision = "0003_website_canary"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("agent_turns", sa.Column("managed_contract_version", sa.SmallInteger(),
                                        nullable=False, server_default="1"))
    op.add_column("agent_turns", sa.Column("managed_context_first_sequence", sa.BigInteger(),
                                        nullable=False, server_default="1"))
    op.add_column("agent_turns", sa.Column("managed_projection_agent_id", sa.Uuid()))
    op.add_column("agent_turns", sa.Column("managed_projection_fingerprint", sa.CHAR(64)))
    op.create_check_constraint("ck_turn_managed_projection", "agent_turns", """
        (managed_contract_version=1 AND managed_context_first_sequence=1
         AND managed_projection_agent_id IS NULL AND managed_projection_fingerprint IS NULL)
        OR (managed_contract_version=2 AND managed_context_first_sequence>=1
         AND managed_context_first_sequence<=input_first_sequence
         AND managed_projection_agent_id IS NOT NULL AND managed_projection_fingerprint IS NOT NULL
         AND managed_projection_fingerprint ~ '^[0-9a-f]{64}$')
    """)
    op.add_column("messages", sa.Column("source_agent_turn_id", sa.Uuid()))
    op.create_unique_constraint("uq_message_source_agent_turn", "messages",
                                ["source_agent_turn_id"])
    op.create_foreign_key("fk_message_source_agent_turn_conversation", "messages", "agent_turns",
                          ["source_agent_turn_id", "conversation_id"], ["id", "conversation_id"],
                          ondelete="RESTRICT")
    op.create_check_constraint("ck_message_agent_source_actor", "messages",
        "source_agent_turn_id IS NULL OR (actor='AGENT' AND direction='OUTBOUND' "
        "AND content_type='TEXT' AND source_inbound_event_id IS NULL)")
    # Once v2 exists, neither a version downgrade nor a coherent replacement of
    # all freeze fields can evade the retained fingerprint before dispatch.
    op.execute("""CREATE FUNCTION preserve_website_projection() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.managed_contract_version IS DISTINCT FROM OLD.managed_contract_version
      OR (OLD.managed_contract_version=2 AND ROW(
        NEW.id,NEW.conversation_id,NEW.selection_key,NEW.creation_fingerprint,
        NEW.input_first_sequence,NEW.input_last_sequence,NEW.context_last_sequence,
        NEW.input_revision,NEW.authority_revision,NEW.managed_contract_version,
        NEW.managed_context_first_sequence,NEW.managed_projection_agent_id,
        NEW.managed_projection_fingerprint) IS DISTINCT FROM ROW(
        OLD.id,OLD.conversation_id,OLD.selection_key,OLD.creation_fingerprint,
        OLD.input_first_sequence,OLD.input_last_sequence,OLD.context_last_sequence,
        OLD.input_revision,OLD.authority_revision,OLD.managed_contract_version,
        OLD.managed_context_first_sequence,OLD.managed_projection_agent_id,
        OLD.managed_projection_fingerprint)) THEN
        RAISE EXCEPTION 'Frozen Website projection cannot change.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.execute("CREATE TRIGGER preserve_website_projection BEFORE UPDATE ON agent_turns "
               "FOR EACH ROW EXECUTE FUNCTION preserve_website_projection()")


def downgrade() -> None:
    op.execute("LOCK TABLE agent_turns, messages IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text("SELECT EXISTS(SELECT 1 FROM agent_turns "
                                  "WHERE managed_contract_version=2) OR EXISTS(SELECT 1 "
                                  "FROM messages WHERE source_agent_turn_id IS NOT NULL)")):
        raise RuntimeError("Retained Website v2/history prevents downgrade.")
    op.execute("DROP TRIGGER preserve_website_projection ON agent_turns")
    op.execute("DROP FUNCTION preserve_website_projection()")
    op.drop_constraint("ck_message_agent_source_actor", "messages", type_="check")
    op.drop_constraint("fk_message_source_agent_turn_conversation", "messages", type_="foreignkey")
    op.drop_constraint("uq_message_source_agent_turn", "messages", type_="unique")
    op.drop_column("messages", "source_agent_turn_id")
    op.drop_constraint("ck_turn_managed_projection", "agent_turns", type_="check")
    for column in ("managed_projection_fingerprint", "managed_projection_agent_id",
                   "managed_context_first_sequence", "managed_contract_version"):
        op.drop_column("agent_turns", column)
