"""Conversation-owned immutable selection evidence; input ownership stays in TurnInputMessage."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    SmallInteger,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class CatchUpSelection(Base):
    __tablename__ = "catch_up_selections"
    turn_id: Mapped[UUID] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    selection_version: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    authority_input_floor_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    settling_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    latest_customer_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    member_count: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    identity_proof: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    membership_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        UniqueConstraint("turn_id", "conversation_id", name="uq_catch_up_selection_parent"),
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "selection_version=1 AND authority_revision>=0 AND input_revision>=1 "
            "AND settling_revision>=0 AND authority_input_floor_sequence>=0 "
            "AND first_sequence>authority_input_floor_sequence "
            "AND last_sequence>=first_sequence AND last_sequence-first_sequence<256 "
            "AND latest_customer_sequence>=last_sequence AND member_count BETWEEN 1 AND 256",
            name="ck_catch_up_selection_bounds",
        ),
        CheckConstraint(
            "channel IN ('TELEGRAM','WEBSITE_WIDGET') "
            "AND membership_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(identity_proof)='array' "
            "AND jsonb_array_length(identity_proof)=last_sequence-first_sequence+1 "
            "AND octet_length(identity_proof::text)<=524288",
            name="ck_catch_up_selection_proof",
        ),
    )


class CatchUpMember(Base):
    __tablename__ = "catch_up_members"
    turn_id: Mapped[UUID] = mapped_column(primary_key=True)
    ordinal: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    burst_id: Mapped[UUID] = mapped_column(nullable=False)
    first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    burst_first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    burst_tail_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64), nullable=False)
    quiet_window_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_wait_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    closed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["catch_up_selections.turn_id", "catch_up_selections.conversation_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["burst_id", "conversation_id"],
            ["settling_bursts.id", "settling_bursts.conversation_id"],
            ondelete="RESTRICT",
        ),
        UniqueConstraint("turn_id", "burst_id", name="uq_catch_up_member_burst"),
        CheckConstraint(
            "ordinal BETWEEN 0 AND 255 AND burst_first_sequence>=1 "
            "AND first_sequence>=burst_first_sequence "
            "AND last_sequence>=first_sequence AND burst_tail_sequence>=last_sequence "
            "AND quiet_window_ms>=1 AND max_wait_ms>=quiet_window_ms "
            "AND policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'",
            name="ck_catch_up_member_bounds",
        ),
    )
