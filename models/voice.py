"""Voice call ownership and immutable playback facts; no audio storage."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class VoiceCall(Base):
    __tablename__ = "voice_calls"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(unique=True)
    media_session_id: Mapped[UUID] = mapped_column()
    agent_id: Mapped[UUID] = mapped_column()
    caller_number: Mapped[str | None] = mapped_column(String(16))
    last_sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_reason: Mapped[str | None] = mapped_column(String(16))
    __table_args__ = (
        ForeignKeyConstraint(["conversation_id"], ["conversations.id"]),
        UniqueConstraint("id", "conversation_id", name="uq_voice_call_parent"),
        CheckConstraint("last_sequence >= 0", name="ck_voice_sequence"),
        CheckConstraint(
            r"caller_number IS NULL OR caller_number ~ '^\+375[0-9]{9}$'",
            name="ck_voice_caller_number",
        ),
        CheckConstraint("expires_at > started_at", name="ck_voice_lease"),
        CheckConstraint(
            "(ended_at IS NULL AND end_reason IS NULL) OR "
            "(ended_at IS NOT NULL AND ended_at >= started_at AND end_reason IS NOT NULL "
            "AND end_reason IN ('DISCONNECTED','TIMEOUT','CANCELLED','FAILED'))",
            name="ck_voice_end",
        ),
    )


class VoiceInput(Base):
    __tablename__ = "voice_inputs"
    call_id: Mapped[UUID] = mapped_column(primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column()
    message_id: Mapped[UUID] = mapped_column(unique=True)
    fingerprint: Mapped[str] = mapped_column(CHAR(64))
    __table_args__ = (
        ForeignKeyConstraint(["call_id", "conversation_id"],
                             ["voice_calls.id", "voice_calls.conversation_id"]),
        ForeignKeyConstraint(["message_id", "conversation_id"],
                             ["messages.id", "messages.conversation_id"]),
        CheckConstraint("sequence > 0", name="ck_voice_input_sequence"),
    )


class VoiceResponse(Base):
    __tablename__ = "voice_responses"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    call_id: Mapped[UUID] = mapped_column()
    conversation_id: Mapped[UUID] = mapped_column()
    turn_id: Mapped[UUID] = mapped_column()
    delivery_id: Mapped[UUID] = mapped_column(unique=True)
    send_authorized: Mapped[bool] = mapped_column(Boolean)
    ordinal: Mapped[int] = mapped_column(BigInteger, default=0)
    is_partial: Mapped[bool] = mapped_column(Boolean, default=False)
    authorized_text: Mapped[str | None] = mapped_column(Text)
    last_event_sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    generated_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    accepted_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    terminal: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (
        UniqueConstraint("id", "call_id", name="uq_voice_response_call"),
        UniqueConstraint("turn_id", "ordinal", name="uq_voice_response_turn_ordinal"),
        ForeignKeyConstraint(["call_id", "conversation_id"],
                             ["voice_calls.id", "voice_calls.conversation_id"]),
        ForeignKeyConstraint(["turn_id", "conversation_id"],
                             ["agent_turns.id", "agent_turns.conversation_id"]),
        ForeignKeyConstraint(["delivery_id", "conversation_id"],
                             ["delivery_attempts.id", "delivery_attempts.conversation_id"]),
        CheckConstraint("last_event_sequence >= 0 AND accepted_bytes >= 0 AND "
                        "generated_bytes >= accepted_bytes AND generated_bytes <= 8388608",
                        name="ck_voice_response_counters"),
        CheckConstraint("ordinal >= 0", name="ck_voice_response_ordinal"),
        CheckConstraint(
            "(authorized_text IS NULL) OR ("
            "char_length(authorized_text) BETWEEN 1 AND 16384 AND "
            "octet_length(authorized_text) <= 32768)",
            name="ck_voice_response_authorized_text",
        ),
        CheckConstraint(
            "(is_partial = false) OR (send_authorized = true AND authorized_text IS NOT NULL)",
            name="ck_voice_response_partial_shape",
        ),
    )


class VoicePlaybackEvent(Base):
    __tablename__ = "voice_playback_events"
    response_id: Mapped[UUID] = mapped_column(primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    call_id: Mapped[UUID] = mapped_column()
    fingerprint: Mapped[str] = mapped_column(CHAR(64))
    state: Mapped[str] = mapped_column(String(16))
    generated_bytes: Mapped[int] = mapped_column(BigInteger)
    accepted_bytes: Mapped[int] = mapped_column(BigInteger)
    terminal: Mapped[bool] = mapped_column(Boolean)
    accepted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        ForeignKeyConstraint(["response_id", "call_id"],
                             ["voice_responses.id", "voice_responses.call_id"]),
        CheckConstraint("sequence > 0 AND accepted_bytes >= 0 AND "
                        "generated_bytes >= accepted_bytes AND generated_bytes <= 8388608",
                        name="ck_voice_fact_counters"),
        CheckConstraint("state IN ('NOT_STARTED','STARTED','PARTIAL','COMPLETED',"
                        "'INTERRUPTED','FAILED','UNKNOWN')", name="ck_voice_fact_state"),
    )
