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
    last_sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    end_reason: Mapped[str | None] = mapped_column(String(16))
    __table_args__ = (
        ForeignKeyConstraint(["conversation_id"], ["conversations.id"]),
        UniqueConstraint("id", "conversation_id", name="uq_voice_call_parent"),
        CheckConstraint("last_sequence >= 0", name="ck_voice_sequence"),
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
    turn_id: Mapped[UUID] = mapped_column(unique=True)
    delivery_id: Mapped[UUID] = mapped_column(unique=True)
    send_authorized: Mapped[bool] = mapped_column(Boolean)
    last_event_sequence: Mapped[int] = mapped_column(BigInteger, default=0)
    generated_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    accepted_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    terminal: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (
        UniqueConstraint("id", "call_id", name="uq_voice_response_call"),
        ForeignKeyConstraint(["call_id", "conversation_id"],
                             ["voice_calls.id", "voice_calls.conversation_id"]),
        ForeignKeyConstraint(["turn_id", "conversation_id"],
                             ["agent_turns.id", "agent_turns.conversation_id"]),
        ForeignKeyConstraint(["delivery_id", "conversation_id"],
                             ["delivery_attempts.id", "delivery_attempts.conversation_id"]),
        CheckConstraint("last_event_sequence >= 0 AND accepted_bytes >= 0 AND "
                        "generated_bytes >= accepted_bytes AND generated_bytes <= 8388608",
                        name="ck_voice_response_counters"),
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
