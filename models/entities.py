"""Six durable journal tables; all cross-parent relationships are database fenced."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base

CHANNELS = "channel IN ('TELEGRAM','INSTAGRAM','WEBSITE_WIDGET','WHATSAPP','VOICE')"
OWNER_BOUNDS = (
    "char_length(owner_id) BETWEEN 1 AND 255 AND octet_length(owner_id) <= 1020 AND owner_id <> '*'"
)
DELIVERY_STATES = (
    "'GENERATED','DISPATCH_REQUESTED','PROVIDER_ACCEPTED','DELIVERED','"
    "READ','FAILED','UNKNOWN','PARTIAL','INTERRUPTED'"
)
ERROR_STATES = "'FAILED','UNKNOWN','PARTIAL','INTERRUPTED'"
ERROR_CODES = "'PROVIDER_REJECTED','TRANSPORT_ERROR','TIMEOUT','INTERRUPTED','UNSPECIFIED'"
REFS_CHECK = (
    "CASE WHEN jsonb_typeof(operation_refs) = 'array' THEN "
    "jsonb_array_length(operation_refs) <= 32 ELSE false END"
)
TURN_LIFECYCLE = """
 (status = 'PENDING' AND claim_epoch = 0 AND claim_token_digest IS NULL
  AND worker_ref IS NULL AND lease_expires_at IS NULL AND started_at IS NULL
  AND completed_at IS NULL AND outcome IS NULL AND draft_text IS NULL
  AND failure_code IS NULL AND terminal_fingerprint IS NULL AND operation_refs = '[]'::jsonb)
 OR (status = 'CLAIMED' AND claim_epoch >= 1 AND claim_token_digest IS NOT NULL
  AND worker_ref IS NOT NULL AND lease_expires_at IS NOT NULL AND started_at IS NOT NULL
  AND completed_at IS NULL AND outcome IS NULL AND draft_text IS NULL
  AND failure_code IS NULL AND terminal_fingerprint IS NULL AND operation_refs = '[]'::jsonb)
 OR (status = 'COMPLETED' AND claim_epoch >= 1 AND claim_token_digest IS NOT NULL
  AND worker_ref IS NOT NULL AND lease_expires_at IS NOT NULL AND started_at IS NOT NULL
  AND completed_at IS NOT NULL AND terminal_fingerprint IS NOT NULL AND failure_code IS NULL
  AND outcome IS NOT NULL AND ((outcome = 'DRAFT_READY' AND draft_text IS NOT NULL)
   OR (outcome = 'NO_RESPONSE' AND draft_text IS NULL)))
 OR (status = 'FAILED' AND claim_epoch >= 1 AND claim_token_digest IS NOT NULL
  AND worker_ref IS NOT NULL AND lease_expires_at IS NOT NULL AND started_at IS NOT NULL
  AND completed_at IS NOT NULL AND terminal_fingerprint IS NOT NULL AND failure_code IS NOT NULL
  AND outcome IS NULL AND draft_text IS NULL)
"""
DELIVERY_EVIDENCE = """
 (status = 'GENERATED' AND revision = 0 AND dispatch_requested_at IS NULL
  AND provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL
  AND provider_operation_id IS NULL AND provider_message_id IS NULL AND error_code IS NULL)
 OR (status <> 'GENERATED' AND revision >= 1 AND dispatch_requested_at IS NOT NULL
  AND (status <> 'PROVIDER_ACCEPTED' OR provider_accepted_at IS NOT NULL)
  AND (status <> 'DELIVERED' OR delivered_at IS NOT NULL)
  AND (status <> 'READ' OR (delivered_at IS NOT NULL AND read_at IS NOT NULL))
  AND (status IN ('FAILED','UNKNOWN','PARTIAL','INTERRUPTED') OR error_code IS NULL))
"""


def digest_check(column: str) -> str:
    return f"{column} ~ '^[0-9a-f]{{64}}$'"


def content_check(column: str) -> str:
    return f"char_length({column}) BETWEEN 1 AND 16384 AND octet_length({column}) <= 32768"


def provider_reference_check(column: str) -> str:
    return f"{column} ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{{0,254}}$'"


class Conversation(Base):
    __tablename__ = "conversations"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    provider_thread_id: Mapped[str] = mapped_column(String(512, collation="C"), nullable=False)
    last_message_sequence: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    input_revision: Mapped[int] = mapped_column(BigInteger, default=0, server_default=sql_text("0"))
    control_mode: Mapped[str] = mapped_column(
        String(16), default="AGENT", server_default=sql_text("'AGENT'")
    )
    authority_revision: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "provider_thread_id",
            name="uq_conversation_identity",
        ),
        UniqueConstraint(
            "id", "owner_id", "integration_id", "channel", name="uq_conversation_id_scope"
        ),
        CheckConstraint(OWNER_BOUNDS, name="ck_conversation_owner_bounds"),
        CheckConstraint(CHANNELS, name="ck_conversation_channel"),
        CheckConstraint(
            (
                "char_length(provider_thread_id) BETWEEN 1 AND 512 AND "
                "octet_length(provider_thread_id) <= 512"
            ),
            name="ck_conversation_thread_bounds",
        ),
        CheckConstraint(
            "control_mode IN ('AGENT','HUMAN_PENDING','HUMAN')", name="ck_conversation_control_mode"
        ),
        CheckConstraint(
            (
                "last_message_sequence >= 0 AND input_revision >= 0 AND "
                "input_revision <= last_message_sequence AND authority_revision >= "
                "0"
            ),
            name="ck_conversation_counters",
        ),
    )


class InboundEvent(Base):
    __tablename__ = "inbound_events"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    provider_namespace: Mapped[str] = mapped_column(String(128, collation="C"), nullable=False)
    provider_event_id: Mapped[str] = mapped_column(String(512, collation="C"), nullable=False)
    contract_version: Mapped[int] = mapped_column(
        SmallInteger, default=1, server_default=sql_text("1")
    )
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "provider_namespace",
            "provider_event_id",
            name="uq_inbound_event_scope",
        ),
        UniqueConstraint("id", "conversation_id", name="uq_inbound_event_id_conversation"),
        ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
            ],
            name="fk_inbound_event_conversation_scope",
            ondelete="RESTRICT",
        ),
        CheckConstraint(OWNER_BOUNDS, name="ck_inbound_event_owner_bounds"),
        CheckConstraint(CHANNELS, name="ck_inbound_event_channel"),
        CheckConstraint(
            "provider_namespace ~ '^[a-z0-9][a-z0-9._:-]{0,127}$'",
            name="ck_inbound_event_namespace",
        ),
        CheckConstraint(
            (
                "char_length(provider_event_id) BETWEEN 1 AND 512 AND "
                "octet_length(provider_event_id) <= 512"
            ),
            name="ck_inbound_event_id_bounds",
        ),
        CheckConstraint("contract_version = 1", name="ck_inbound_event_contract"),
        CheckConstraint(digest_check("fingerprint"), name="ck_inbound_event_fingerprint"),
        Index("ix_inbound_event_conversation", "conversation_id"),
    )


class Message(Base):
    __tablename__ = "messages"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actor: Mapped[str] = mapped_column(String(16), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    content_type: Mapped[str] = mapped_column(
        String(16), default="TEXT", server_default=sql_text("'TEXT'")
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source_inbound_event_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence", name="uq_message_sequence"),
        UniqueConstraint("source_inbound_event_id", name="uq_message_source_event"),
        ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_message_conversation",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["source_inbound_event_id", "conversation_id"],
            ["inbound_events.id", "inbound_events.conversation_id"],
            name="fk_message_source_event_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint("sequence >= 1", name="ck_message_sequence"),
        CheckConstraint("actor IN ('CUSTOMER','AGENT','HUMAN','SYSTEM')", name="ck_message_actor"),
        CheckConstraint("direction IN ('INBOUND','OUTBOUND')", name="ck_message_direction"),
        CheckConstraint("content_type = 'TEXT'", name="ck_message_content_type"),
        CheckConstraint(content_check("text"), name="ck_message_content_bounds"),
        CheckConstraint(
            "source_inbound_event_id IS NULL OR (actor = 'CUSTOMER' AND direction = 'INBOUND')",
            name="ck_message_source_actor",
        ),
    )


class AgentTurn(Base):
    __tablename__ = "agent_turns"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    selection_key: Mapped[UUID] = mapped_column(nullable=False)
    creation_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    input_first_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    context_last_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), default="PENDING", server_default=sql_text("'PENDING'")
    )
    claim_epoch: Mapped[int] = mapped_column(BigInteger, default=0, server_default=sql_text("0"))
    claim_token_digest: Mapped[str | None] = mapped_column(String(64))
    worker_ref: Mapped[str | None] = mapped_column(String(64))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(16))
    draft_text: Mapped[str | None] = mapped_column(Text)
    failure_code: Mapped[str | None] = mapped_column(String(32))
    operation_refs: Mapped[list[dict[str, str | None]]] = mapped_column(
        JSONB, default=list, server_default=sql_text("'[]'::jsonb")
    )
    terminal_fingerprint: Mapped[str | None] = mapped_column(String(64))
    __table_args__ = (
        UniqueConstraint("conversation_id", "selection_key", name="uq_turn_selection"),
        UniqueConstraint("id", "conversation_id", name="uq_turn_id_conversation"),
        ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_turn_conversation",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "input_first_sequence"],
            ["messages.conversation_id", "messages.sequence"],
            name="fk_turn_input_first",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "input_last_sequence"],
            ["messages.conversation_id", "messages.sequence"],
            name="fk_turn_input_last",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "context_last_sequence"],
            ["messages.conversation_id", "messages.sequence"],
            name="fk_turn_context_last",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            (
                "input_first_sequence >= 1 AND input_first_sequence <= "
                "input_last_sequence AND input_last_sequence <= "
                "context_last_sequence AND input_revision >= 1 AND "
                "authority_revision >= 0"
            ),
            name="ck_turn_boundaries",
        ),
        CheckConstraint(
            "status IN ('PENDING','CLAIMED','COMPLETED','FAILED')", name="ck_turn_status"
        ),
        CheckConstraint("claim_epoch >= 0", name="ck_turn_epoch"),
        CheckConstraint(digest_check("creation_fingerprint"), name="ck_turn_creation_fingerprint"),
        CheckConstraint(digest_check("claim_token_digest"), name="ck_turn_claim_digest"),
        CheckConstraint(digest_check("terminal_fingerprint"), name="ck_turn_terminal_fingerprint"),
        CheckConstraint("worker_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'", name="ck_turn_worker"),
        CheckConstraint("outcome IN ('DRAFT_READY','NO_RESPONSE')", name="ck_turn_outcome"),
        CheckConstraint(
            "failure_code IN ('PROCESSING_ERROR','TIMEOUT','INVALID_RESULT')",
            name="ck_turn_failure",
        ),
        CheckConstraint(content_check("draft_text"), name="ck_turn_draft_bounds"),
        CheckConstraint(REFS_CHECK, name="ck_turn_operation_refs"),
        CheckConstraint(TURN_LIFECYCLE, name="ck_turn_lifecycle"),
        Index(
            "uq_turn_one_active",
            "conversation_id",
            unique=True,
            postgresql_where=sql_text("status IN ('PENDING','CLAIMED')"),
        ),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_key: Mapped[UUID] = mapped_column(nullable=False)
    source_turn_id: Mapped[UUID] = mapped_column(nullable=False)
    creation_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), default="GENERATED", server_default=sql_text("'GENERATED'")
    )
    revision: Mapped[int] = mapped_column(BigInteger, default=0, server_default=sql_text("0"))
    provider_operation_id: Mapped[str | None] = mapped_column(String(255, collation="C"))
    provider_message_id: Mapped[str | None] = mapped_column(String(255, collation="C"))
    error_code: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    dispatch_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    provider_accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("conversation_id", "delivery_key", name="uq_delivery_key"),
        UniqueConstraint("source_turn_id", name="uq_delivery_source_turn"),
        UniqueConstraint("id", "conversation_id", name="uq_delivery_id_conversation"),
        ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_delivery_conversation",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["source_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_delivery_source_turn_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(f"status IN ({DELIVERY_STATES})", name="ck_delivery_status"),
        CheckConstraint("revision >= 0 AND authority_revision >= 0", name="ck_delivery_revisions"),
        CheckConstraint(
            digest_check("creation_fingerprint"), name="ck_delivery_creation_fingerprint"
        ),
        CheckConstraint(
            provider_reference_check("provider_operation_id"),
            name="ck_delivery_operation_reference",
        ),
        CheckConstraint(
            provider_reference_check("provider_message_id"), name="ck_delivery_message_reference"
        ),
        CheckConstraint(f"error_code IN ({ERROR_CODES})", name="ck_delivery_error_code"),
        CheckConstraint(DELIVERY_EVIDENCE, name="ck_delivery_evidence"),
    )


class DeliveryReconciliation(Base):
    __tablename__ = "delivery_reconciliations"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_attempt_id: Mapped[UUID] = mapped_column(nullable=False)
    reconciliation_key: Mapped[UUID] = mapped_column(nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    result_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_operation_id: Mapped[str | None] = mapped_column(String(255, collation="C"))
    provider_message_id: Mapped[str | None] = mapped_column(String(255, collation="C"))
    provider_occurred_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(String(32))
    expected_authority_revision: Mapped[int | None] = mapped_column(BigInteger)
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "delivery_attempt_id", "reconciliation_key", name="uq_delivery_reconciliation_key"
        ),
        UniqueConstraint(
            "delivery_attempt_id", "result_revision", name="uq_delivery_reconciliation_revision"
        ),
        ForeignKeyConstraint(
            ["delivery_attempt_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            name="fk_reconciliation_delivery_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(digest_check("fingerprint"), name="ck_reconciliation_fingerprint"),
        CheckConstraint(
            (
                "expected_revision >= 0 AND result_revision >= 1 AND "
                "result_revision = expected_revision + 1"
            ),
            name="ck_reconciliation_revision",
        ),
        CheckConstraint(
            f"status IN ({DELIVERY_STATES}) AND status <> 'GENERATED'",
            name="ck_reconciliation_status",
        ),
        CheckConstraint(
            provider_reference_check("provider_operation_id"),
            name="ck_reconciliation_operation_reference",
        ),
        CheckConstraint(
            provider_reference_check("provider_message_id"),
            name="ck_reconciliation_message_reference",
        ),
        CheckConstraint(
            f"error_code IN ({ERROR_CODES}) AND (status IN ({ERROR_STATES}) OR error_code IS NULL)",
            name="ck_reconciliation_error_code",
        ),
        CheckConstraint(
            (
                "(status = 'DISPATCH_REQUESTED' AND expected_authority_revision IS "
                "NOT NULL AND expected_authority_revision >= 0) OR (status <> "
                "'DISPATCH_REQUESTED' AND expected_authority_revision IS NULL)"
            ),
            name="ck_reconciliation_authority",
        ),
    )
