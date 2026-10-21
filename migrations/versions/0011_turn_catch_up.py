"""Add immutable bounded text selection evidence after the retained Voice schema."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "0011_turn_catch_up"
down_revision = "0010_voice_caller_number"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agent_turns",
        sa.Column(
            "catch_up_version", sa.SmallInteger(), nullable=False, server_default=sa.text("0")
        ),
    )
    op.create_check_constraint(
        "ck_turn_catch_up_version",
        "agent_turns",
        "catch_up_version IN (0,1) AND (catch_up_version=0 OR "
        "(settling_burst_id IS NOT NULL AND managed_contract_version=2))",
    )
    op.create_table(
        "catch_up_selections",
        sa.Column("turn_id", sa.Uuid(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("selection_version", sa.SmallInteger(), nullable=False),
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("authority_input_floor_sequence", sa.BigInteger(), nullable=False),
        sa.Column("input_revision", sa.BigInteger(), nullable=False),
        sa.Column("settling_revision", sa.BigInteger(), nullable=False),
        sa.Column("first_sequence", sa.BigInteger(), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), nullable=False),
        sa.Column("latest_customer_sequence", sa.BigInteger(), nullable=False),
        sa.Column("agent_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(32), nullable=False),
        sa.Column("member_count", sa.SmallInteger(), nullable=False),
        sa.Column("identity_proof", JSONB(), nullable=False),
        sa.Column("membership_fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("turn_id", "conversation_id", name="uq_catch_up_selection_parent"),
        sa.ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "selection_version=1 AND authority_revision>=0 AND input_revision>=1 "
            "AND settling_revision>=0 AND authority_input_floor_sequence>=0 "
            "AND first_sequence>authority_input_floor_sequence "
            "AND last_sequence>=first_sequence AND last_sequence-first_sequence<256 "
            "AND latest_customer_sequence>=last_sequence AND member_count BETWEEN 1 AND 256",
            name="ck_catch_up_selection_bounds",
        ),
        sa.CheckConstraint(
            "channel IN ('TELEGRAM','WEBSITE_WIDGET') "
            "AND membership_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(identity_proof)='array' "
            "AND jsonb_array_length(identity_proof)=last_sequence-first_sequence+1 "
            "AND octet_length(identity_proof::text)<=524288",
            name="ck_catch_up_selection_proof",
        ),
    )
    op.create_table(
        "catch_up_members",
        sa.Column("turn_id", sa.Uuid(), primary_key=True),
        sa.Column("ordinal", sa.SmallInteger(), primary_key=True),
        sa.Column("conversation_id", sa.Uuid(), nullable=False),
        sa.Column("burst_id", sa.Uuid(), nullable=False),
        sa.Column("first_sequence", sa.BigInteger(), nullable=False),
        sa.Column("last_sequence", sa.BigInteger(), nullable=False),
        sa.Column("burst_first_sequence", sa.BigInteger(), nullable=False),
        sa.Column("burst_tail_sequence", sa.BigInteger(), nullable=False),
        sa.Column("policy_version", sa.String(64), nullable=False),
        sa.Column("quiet_window_ms", sa.BigInteger(), nullable=False),
        sa.Column("max_wait_ms", sa.BigInteger(), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["catch_up_selections.turn_id", "catch_up_selections.conversation_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["burst_id", "conversation_id"],
            ["settling_bursts.id", "settling_bursts.conversation_id"],
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("turn_id", "burst_id", name="uq_catch_up_member_burst"),
        sa.CheckConstraint(
            "ordinal BETWEEN 0 AND 255 AND burst_first_sequence>=1 "
            "AND first_sequence>=burst_first_sequence "
            "AND last_sequence>=first_sequence AND burst_tail_sequence>=last_sequence "
            "AND quiet_window_ms>=1 AND max_wait_ms>=quiet_window_ms "
            "AND policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'",
            name="ck_catch_up_member_bounds",
        ),
    )
    op.execute("""CREATE FUNCTION immutable_catch_up_evidence() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      RAISE EXCEPTION 'Catch-up evidence is immutable.' USING ERRCODE='23514';
    END $$""")
    for table in ("catch_up_selections", "catch_up_members"):
        op.execute(
            f"CREATE TRIGGER immutable_{table} BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION immutable_catch_up_evidence()"
        )
    op.execute("""CREATE FUNCTION preserve_catch_up_version() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF NEW.catch_up_version IS DISTINCT FROM OLD.catch_up_version THEN
        RAISE EXCEPTION 'Turn catch-up version is immutable.' USING ERRCODE='23514';
      END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER preserve_catch_up_version BEFORE UPDATE ON agent_turns "
        "FOR EACH ROW EXECUTE FUNCTION preserve_catch_up_version()"
    )
    # A committed selection is sealed: there are no free member ordinals for
    # later inserts, and a v1 Turn can never commit without its complete evidence.
    op.execute("""CREATE FUNCTION check_catch_up_seal() RETURNS trigger
    LANGUAGE plpgsql AS $$
    DECLARE tid uuid; t agent_turns%ROWTYPE; s catch_up_selections%ROWTYPE;
            n bigint; lo bigint; hi bigint; positions bigint;
    BEGIN
      IF TG_TABLE_NAME='agent_turns' THEN tid:=NEW.id; ELSE tid:=NEW.turn_id; END IF;
      SELECT * INTO t FROM agent_turns WHERE id=tid;
      IF NOT FOUND THEN RETURN NULL; END IF;
      SELECT * INTO s FROM catch_up_selections WHERE turn_id=tid;
      IF t.catch_up_version=0 THEN
        IF FOUND THEN RAISE EXCEPTION 'Unexpected catch-up evidence.' USING ERRCODE='23514'; END IF;
        RETURN NULL;
      END IF;
      IF NOT FOUND OR ROW(s.conversation_id,s.authority_revision,s.input_revision,
         s.settling_revision,s.first_sequence,s.last_sequence,s.agent_id)
         IS DISTINCT FROM ROW(t.conversation_id,t.authority_revision,t.input_revision,
         t.settling_revision,t.input_first_sequence,t.input_last_sequence,t.managed_projection_agent_id)
      THEN RAISE EXCEPTION 'Incomplete catch-up selection.' USING ERRCODE='23514'; END IF;
      SELECT count(*),min(ordinal),max(ordinal) INTO n,lo,hi
        FROM catch_up_members WHERE turn_id=tid;
      IF n<>s.member_count OR lo<>0 OR hi<>n-1 THEN
        RAISE EXCEPTION 'Incomplete catch-up membership.' USING ERRCODE='23514'; END IF;
      IF EXISTS (SELECT 1 FROM (
        SELECT first_sequence,last_sequence,ordinal,
               lag(last_sequence) OVER (ORDER BY ordinal) AS previous_tail
        FROM catch_up_members WHERE turn_id=tid) AS members
        WHERE (ordinal=0 AND first_sequence<>s.first_sequence)
           OR (ordinal>0 AND first_sequence<>previous_tail+1)
           OR (ordinal=s.member_count-1 AND last_sequence<>s.last_sequence))
      THEN RAISE EXCEPTION 'Noncontiguous catch-up membership.' USING ERRCODE='23514'; END IF;
      SELECT count(*),min(sequence),max(sequence),count(DISTINCT ordinal) INTO n,lo,hi,positions
        FROM turn_input_messages WHERE turn_id=tid AND conversation_id=t.conversation_id;
      IF n<>s.last_sequence-s.first_sequence+1 OR lo<>s.first_sequence OR hi<>s.last_sequence
         OR positions<>n OR EXISTS(SELECT 1 FROM turn_input_messages
           WHERE turn_id=tid AND (ordinal<0 OR ordinal>=n))
      THEN RAISE EXCEPTION 'Incomplete catch-up ownership.' USING ERRCODE='23514'; END IF;
      RETURN NULL;
    END $$""")
    for table in ("agent_turns", "catch_up_selections", "catch_up_members", "turn_input_messages"):
        # turn_input_messages has a turn_id too; checking later inserts also
        # prevents committed ownership from expanding beyond the sealed range.
        op.execute(
            f"CREATE CONSTRAINT TRIGGER seal_{table}_catch_up AFTER INSERT ON {table} "
            "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_catch_up_seal()"
        )
    op.execute("""CREATE FUNCTION preserve_catch_up_ownership() RETURNS trigger
    LANGUAGE plpgsql AS $$ BEGIN
      IF EXISTS(SELECT 1 FROM agent_turns WHERE id=OLD.turn_id AND catch_up_version=1)
         OR (TG_OP='UPDATE' AND EXISTS(SELECT 1 FROM agent_turns
                                     WHERE id=NEW.turn_id AND catch_up_version=1)) THEN
        RAISE EXCEPTION 'Catch-up input ownership is immutable.' USING ERRCODE='23514';
      END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END $$""")
    op.execute(
        "CREATE TRIGGER preserve_catch_up_ownership BEFORE UPDATE OR DELETE ON turn_input_messages "
        "FOR EACH ROW EXECUTE FUNCTION preserve_catch_up_ownership()"
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE agent_turns,catch_up_selections,catch_up_members IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().scalar(
        sa.text(
            "SELECT EXISTS(SELECT 1 FROM catch_up_selections) "
            "OR EXISTS(SELECT 1 FROM catch_up_members) "
            "OR EXISTS(SELECT 1 FROM agent_turns WHERE catch_up_version<>0)"
        )
    ):
        raise RuntimeError("Retained catch-up evidence prevents downgrade.")
    for table in ("agent_turns", "catch_up_selections", "catch_up_members", "turn_input_messages"):
        op.execute(f"DROP TRIGGER seal_{table}_catch_up ON {table}")
    op.execute("DROP FUNCTION check_catch_up_seal()")
    op.execute("DROP TRIGGER preserve_catch_up_ownership ON turn_input_messages")
    op.execute("DROP FUNCTION preserve_catch_up_ownership()")
    op.execute("DROP TRIGGER preserve_catch_up_version ON agent_turns")
    op.execute("DROP FUNCTION preserve_catch_up_version()")
    for table in ("catch_up_members", "catch_up_selections"):
        op.execute(f"DROP TRIGGER immutable_{table} ON {table}")
        op.drop_table(table)
    op.execute("DROP FUNCTION immutable_catch_up_evidence()")
    op.drop_constraint("ck_turn_catch_up_version", "agent_turns", type_="check")
    op.drop_column("agent_turns", "catch_up_version")
