"""Six durable journal tables; all cross-parent relationships are database fenced."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CHAR,
    BigInteger,
    Boolean,
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
    "'GENERATED','DISPATCH_REQUESTED','SUPERSEDED','PROVIDER_ACCEPTED','DELIVERED','"
    "READ','FAILED','UNKNOWN','PARTIAL','INTERRUPTED'"
)
ERROR_STATES = "'FAILED','UNKNOWN','PARTIAL','INTERRUPTED'"
ERROR_CODES = "'PROVIDER_REJECTED','TRANSPORT_ERROR','TIMEOUT','INTERRUPTED','UNSPECIFIED'"
REFS_CHECK = (
    "CASE WHEN jsonb_typeof(operation_refs) = 'array' THEN "
    "jsonb_array_length(operation_refs) <= 32 ELSE false END"
)
MANAGED_MARKER = """
 (managed_agent_id IS NULL AND managed_dispatch_epoch IS NULL
  AND managed_dispatch_started_at IS NULL AND managed_request_fingerprint IS NULL)
 OR (managed_agent_id IS NOT NULL AND managed_dispatch_epoch IS NOT NULL
  AND managed_dispatch_started_at IS NOT NULL AND managed_request_fingerprint IS NOT NULL
  AND managed_dispatch_epoch >= 1 AND managed_dispatch_epoch <= claim_epoch
  AND status <> 'PENDING' AND started_at IS NOT NULL
  AND managed_dispatch_started_at >= started_at
  AND managed_request_fingerprint ~ '^[0-9a-f]{64}$')
"""
MANAGED_PROJECTION = """
 (managed_contract_version = 1 AND managed_context_first_sequence = 1
  AND managed_projection_agent_id IS NULL AND managed_projection_fingerprint IS NULL)
 OR (managed_contract_version IN (2, 3) AND managed_context_first_sequence >= 1
  AND managed_context_first_sequence <= input_first_sequence
  AND managed_projection_agent_id IS NOT NULL AND managed_projection_fingerprint IS NOT NULL
  AND managed_projection_fingerprint ~ '^[0-9a-f]{64}$')
"""
MANAGED_V3_SNAPSHOT = """
 (managed_v3_snapshot IS NULL AND managed_v3_request_fingerprint IS NULL
  AND managed_v3_dispatch_id IS NULL)
 OR (managed_v3_snapshot IS NOT NULL AND managed_v3_request_fingerprint IS NOT NULL
  AND managed_v3_dispatch_id IS NOT NULL
  AND managed_v3_request_fingerprint ~ '^[0-9a-f]{64}$'
  AND managed_contract_version = 3)
"""
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
    settling_revision: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    control_mode: Mapped[str] = mapped_column(
        String(16), default="AGENT", server_default=sql_text("'AGENT'")
    )
    authority_revision: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    authority_input_floor_sequence: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    handoff_resume_floor_sequence: Mapped[int | None] = mapped_column(BigInteger)
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
        UniqueConstraint(
            "id", "owner_id", "integration_id", "channel", "provider_thread_id",
            name="uq_conversation_transport_identity",
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
                "last_message_sequence >= 0 AND input_revision >= 0 AND settling_revision >= 0 AND "
                "input_revision <= last_message_sequence AND authority_revision >= 0 AND "
                "authority_input_floor_sequence >= 0 AND "
                "authority_input_floor_sequence <= last_message_sequence"
                " AND (handoff_resume_floor_sequence IS NULL OR "
                "(handoff_resume_floor_sequence >= 0 AND "
                "handoff_resume_floor_sequence <= last_message_sequence))"
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
    input_revision: Mapped[int | None] = mapped_column(BigInteger)
    settling_revision: Mapped[int | None] = mapped_column(BigInteger)
    settling_policy_version: Mapped[str | None] = mapped_column(String(64, collation="C"))
    settling_quiet_window_ms: Mapped[int | None] = mapped_column(BigInteger)
    settling_max_wait_ms: Mapped[int | None] = mapped_column(BigInteger)
    settling_burst_id: Mapped[UUID | None] = mapped_column()
    actor: Mapped[str] = mapped_column(String(16), nullable=False)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    content_type: Mapped[str] = mapped_column(
        String(16), default="TEXT", server_default=sql_text("'TEXT'")
    )
    text: Mapped[str] = mapped_column(Text, nullable=False)
    source_inbound_event_id: Mapped[UUID | None] = mapped_column()
    source_agent_turn_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint("conversation_id", "sequence", name="uq_message_sequence"),
        UniqueConstraint("source_inbound_event_id", name="uq_message_source_event"),
        UniqueConstraint("source_agent_turn_id", name="uq_message_source_agent_turn"),
        UniqueConstraint("id", "conversation_id", name="uq_message_id_conversation"),
        UniqueConstraint("id", "conversation_id", "sequence", name="uq_message_identity_sequence"),
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
        CheckConstraint(
            "(settling_burst_id IS NULL AND settling_policy_version IS NULL "
            "AND settling_quiet_window_ms IS NULL AND settling_max_wait_ms IS NULL) OR "
            "(settling_burst_id IS NOT NULL AND settling_policy_version IS NOT NULL "
            "AND settling_quiet_window_ms IS NOT NULL AND settling_max_wait_ms IS NOT NULL "
            "AND settling_policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$' "
            "AND settling_quiet_window_ms >= 1 AND settling_max_wait_ms >= "
            "settling_quiet_window_ms)",
            name="ck_message_settling_snapshot",
        ),
        ForeignKeyConstraint(
            ["settling_burst_id", "conversation_id"],
            ["settling_bursts.id", "settling_bursts.conversation_id"],
            name="fk_message_settling_burst_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "(settling_burst_id IS NULL AND settling_policy_version IS NULL "
            "AND settling_quiet_window_ms IS NULL AND settling_max_wait_ms IS NULL) OR "
            "(settling_burst_id IS NOT NULL AND actor='CUSTOMER' AND direction='INBOUND' "
            "AND settling_policy_version IS NOT NULL AND settling_quiet_window_ms IS NOT NULL "
            "AND settling_max_wait_ms IS NOT NULL AND input_revision IS NOT NULL "
            "AND input_revision >= 1 AND settling_revision IS NOT NULL "
            "AND settling_revision >= 0)",
            name="ck_message_settling_burst_shape",
        ),
        ForeignKeyConstraint(
            ["source_agent_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_message_source_agent_turn_conversation",
            ondelete="RESTRICT",
            use_alter=True,
        ),
        CheckConstraint(
            "source_agent_turn_id IS NULL OR (actor='AGENT' AND direction='OUTBOUND' "
            "AND content_type='TEXT' AND source_inbound_event_id IS NULL)",
            name="ck_message_agent_source_actor",
        ),
        CheckConstraint("actor IN ('CUSTOMER','AGENT','HUMAN','SYSTEM')", name="ck_message_actor"),
        CheckConstraint("direction IN ('INBOUND','OUTBOUND')", name="ck_message_direction"),
        CheckConstraint("content_type = 'TEXT'", name="ck_message_content_type"),
        CheckConstraint(content_check("text"), name="ck_message_content_bounds"),
        CheckConstraint(
            "source_inbound_event_id IS NULL OR (actor = 'CUSTOMER' AND direction = 'INBOUND')",
            name="ck_message_source_actor",
        ),
        Index("ix_message_burst_sequence", "conversation_id", "settling_burst_id", "sequence"),
    )


class SettlingBurst(Base):
    """Immutable policy snapshot and durable lifetime for one Conversation burst."""

    __tablename__ = "settling_bursts"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    policy_version: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    quiet_window_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_wait_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    max_cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_input_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    tail_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ready_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("id", "conversation_id", name="uq_settling_burst_id_conversation"),
        ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_settling_burst_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "policy_version ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$' "
            "AND quiet_window_ms >= 1 AND max_wait_ms >= quiet_window_ms "
            "AND tail_sequence >= 1 AND max_cutoff_at > opened_at "
            "AND last_input_at >= opened_at AND ready_at >= last_input_at "
            "AND ready_at <= max_cutoff_at "
            "AND (closed_at IS NULL OR closed_at = ready_at)",
            name="ck_settling_burst_shape",
        ),
        Index(
            "uq_settling_burst_open",
            "conversation_id",
            unique=True,
            postgresql_where=sql_text("closed_at IS NULL"),
        ),
        Index("ix_settling_burst_due", "ready_at", "conversation_id"),
        Index(
            "ix_settling_burst_conversation_tail", "conversation_id", sql_text("tail_sequence DESC")
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
    settling_revision: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    settling_burst_id: Mapped[UUID | None] = mapped_column()
    catch_up_version: Mapped[int] = mapped_column(
        SmallInteger, default=0, server_default=sql_text("0")
    )
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
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(16))
    draft_text: Mapped[str | None] = mapped_column(Text)
    partial_draft_text: Mapped[str | None] = mapped_column(Text)
    failure_code: Mapped[str | None] = mapped_column(String(32))
    operation_refs: Mapped[list[dict[str, str | None]]] = mapped_column(
        JSONB, default=list, server_default=sql_text("'[]'::jsonb")
    )
    terminal_fingerprint: Mapped[str | None] = mapped_column(String(64))
    managed_agent_id: Mapped[UUID | None] = mapped_column()
    managed_dispatch_epoch: Mapped[int | None] = mapped_column(BigInteger)
    managed_dispatch_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    managed_request_fingerprint: Mapped[str | None] = mapped_column(CHAR(64))
    managed_contract_version: Mapped[int] = mapped_column(
        SmallInteger, default=1, server_default=sql_text("1")
    )
    managed_context_first_sequence: Mapped[int] = mapped_column(
        BigInteger, default=1, server_default=sql_text("1")
    )
    managed_projection_agent_id: Mapped[UUID | None] = mapped_column()
    managed_projection_fingerprint: Mapped[str | None] = mapped_column(CHAR(64))
    managed_v3_snapshot: Mapped[dict[str, object] | None] = mapped_column(JSONB(none_as_null=True))
    managed_v3_request_fingerprint: Mapped[str | None] = mapped_column(CHAR(64))
    managed_v3_dispatch_id: Mapped[UUID | None] = mapped_column()
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
        ForeignKeyConstraint(
            ["settling_burst_id", "conversation_id"],
            ["settling_bursts.id", "settling_bursts.conversation_id"],
            name="fk_turn_settling_burst_conversation",
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
        CheckConstraint("settling_revision >= 0", name="ck_turn_settling_revision"),
        CheckConstraint(
            "catch_up_version IN (0,1) AND (catch_up_version=0 OR "
            "(settling_burst_id IS NOT NULL AND managed_contract_version IN (2,3)))",
            name="ck_turn_catch_up_version",
        ),
        CheckConstraint("claim_epoch >= 0", name="ck_turn_epoch"),
        CheckConstraint(digest_check("creation_fingerprint"), name="ck_turn_creation_fingerprint"),
        CheckConstraint(digest_check("claim_token_digest"), name="ck_turn_claim_digest"),
        CheckConstraint(digest_check("terminal_fingerprint"), name="ck_turn_terminal_fingerprint"),
        CheckConstraint("worker_ref ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$'", name="ck_turn_worker"),
        CheckConstraint("outcome IN ('DRAFT_READY','NO_RESPONSE')", name="ck_turn_outcome"),
        CheckConstraint(
            "failure_code IN ('PROCESSING_ERROR','TIMEOUT','INVALID_RESULT','EXECUTION_UNKNOWN')",
            name="ck_turn_failure",
        ),
        CheckConstraint(content_check("draft_text"), name="ck_turn_draft_bounds"),
        CheckConstraint(content_check("partial_draft_text"), name="ck_turn_partial_draft_bounds"),
        CheckConstraint(REFS_CHECK, name="ck_turn_operation_refs"),
        CheckConstraint(TURN_LIFECYCLE, name="ck_turn_lifecycle"),
        CheckConstraint(MANAGED_MARKER, name="ck_turn_managed_marker"),
        CheckConstraint(MANAGED_PROJECTION, name="ck_turn_managed_projection"),
        CheckConstraint(MANAGED_V3_SNAPSHOT, name="ck_turn_managed_v3_snapshot"),
        Index(
            "uq_turn_one_active",
            "conversation_id",
            unique=True,
            postgresql_where=sql_text("status IN ('PENDING','CLAIMED') AND retired_at IS NULL"),
        ),
        Index("ix_turn_conversation_settling_burst", "conversation_id", "settling_burst_id"),
    )


class TurnInputMessage(Base):
    """Canonical inbound messages frozen into a Turn; also the durable consumed marker."""

    __tablename__ = "turn_input_messages"
    message_id: Mapped[UUID] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    turn_id: Mapped[UUID] = mapped_column(nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ordinal: Mapped[int] = mapped_column(BigInteger, nullable=False)
    __table_args__ = (
        UniqueConstraint("turn_id", "sequence", name="uq_turn_input_sequence"),
        ForeignKeyConstraint(
            ["message_id", "conversation_id", "sequence"],
            ["messages.id", "messages.conversation_id", "messages.sequence"],
            name="fk_turn_input_exact_message",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_turn_input_turn_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint("sequence >= 1 AND ordinal >= 0", name="ck_turn_input_position"),
        Index("ix_turn_input_conversation_sequence", "conversation_id", "sequence"),
    )


class ExternalEffectPermit(Base):
    """Immutable Conversation-owned permission for one opaque Business operation."""

    __tablename__ = "external_effect_permits"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    turn_id: Mapped[UUID] = mapped_column(nullable=False)
    origin_turn_id: Mapped[UUID] = mapped_column(nullable=False)
    operation_namespace: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    operation_id: Mapped[UUID] = mapped_column(nullable=False)
    operation_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    authority_input_floor_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    split_input_floor_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        UniqueConstraint("id", "conversation_id", name="uq_effect_permit_id_conversation"),
        UniqueConstraint("operation_namespace", "operation_id", name="uq_effect_permit_operation"),
        ForeignKeyConstraint(
            ["conversation_id"],
            ["conversations.id"],
            name="fk_effect_permit_conversation",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_effect_permit_turn",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["origin_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_effect_permit_origin_turn",
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "operation_namespace = 'main-chota.ai-business.v1'", name="ck_effect_permit_namespace"
        ),
        CheckConstraint(
            "operation_fingerprint ~ '^[0-9a-f]{64}$'",
            name="ck_effect_permit_operation_fingerprint",
        ),
        CheckConstraint(
            "request_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_effect_permit_request_fingerprint"
        ),
        CheckConstraint(
            "authority_revision >= 0 AND authority_input_floor_sequence >= 0 "
            "AND split_input_floor_sequence >= 0",
            name="ck_effect_permit_floors",
        ),
        Index("ix_effect_permit_turn", "conversation_id", "turn_id", "issued_at"),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_key: Mapped[UUID] = mapped_column(nullable=False)
    source_turn_id: Mapped[UUID | None] = mapped_column()
    source_message_id: Mapped[UUID | None] = mapped_column()
    # Legacy Website/Telegram attempts retain their original API and may have no
    # Handoff ordering ordinal. Enrolled Handoff writers always set one.
    outbound_ordinal: Mapped[int | None] = mapped_column(BigInteger)
    case_id: Mapped[UUID | None] = mapped_column()
    case_revision: Mapped[int | None] = mapped_column(BigInteger)
    assignment_revision: Mapped[int | None] = mapped_column(BigInteger)
    grant_generation: Mapped[int | None] = mapped_column(BigInteger)
    writer_state: Mapped[str] = mapped_column(
        String(16), default="NONE", server_default=sql_text("'NONE'")
    )
    writer_supplier_instance_id: Mapped[UUID | None] = mapped_column()
    writer_process_incarnation_id: Mapped[UUID | None] = mapped_column()
    writer_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    writer_ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    writer_retirement_command_id: Mapped[UUID | None] = mapped_column()
    writer_retirement_fingerprint: Mapped[str | None] = mapped_column(CHAR(64))
    writer_retirement_receipt: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    ordinal: Mapped[int] = mapped_column(BigInteger, default=0, server_default=sql_text("0"))
    creation_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expected_settling_revision: Mapped[int | None] = mapped_column(BigInteger)
    fenced_settling_revision: Mapped[int | None] = mapped_column(BigInteger)
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
        UniqueConstraint("source_turn_id", "ordinal", name="uq_delivery_source_turn"),
        UniqueConstraint("source_message_id", name="uq_delivery_source_message"),
        UniqueConstraint("conversation_id", "writer_retirement_command_id",
                         name="uq_delivery_writer_retirement_command"),
        CheckConstraint(
            "(writer_retirement_command_id IS NULL AND writer_retirement_fingerprint IS NULL "
            "AND writer_retirement_receipt IS NULL) OR (writer_state='RETIRED' "
            "AND writer_retirement_command_id IS NOT NULL "
            "AND writer_retirement_fingerprint IS NOT NULL "
            "AND writer_retirement_fingerprint ~ '^[0-9a-f]{64}$' "
            "AND writer_retirement_receipt IS NOT NULL "
            "AND jsonb_typeof(writer_retirement_receipt)='object')",
            name="ck_delivery_writer_retirement_receipt",
        ),
        UniqueConstraint(
            "conversation_id", "outbound_ordinal", name="uq_delivery_outbound_ordinal"
        ),
        UniqueConstraint("id", "conversation_id", name="uq_delivery_id_conversation"),
        CheckConstraint("ordinal >= 0", name="ck_delivery_ordinal"),
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
        ForeignKeyConstraint(
            ["source_message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            name="fk_delivery_source_message_conversation",
            ondelete="RESTRICT",
        ),
        CheckConstraint(f"status IN ({DELIVERY_STATES})", name="ck_delivery_status"),
        CheckConstraint(
            "((source_turn_id IS NOT NULL)::int + (source_message_id IS NOT NULL)::int) = 1",
            name="ck_delivery_exact_source",
        ),
        CheckConstraint(
            "outbound_ordinal IS NULL OR outbound_ordinal >= 1", name="ck_delivery_outbound_ordinal"
        ),
        CheckConstraint(
            "writer_state IN ('NONE','LIVE','ENDED','RETIRED') AND "
            "((writer_state='NONE' AND writer_supplier_instance_id IS NULL "
            "AND writer_process_incarnation_id IS NULL AND writer_started_at IS NULL "
            "AND writer_ended_at IS NULL) OR (writer_state='LIVE' "
            "AND writer_supplier_instance_id IS NOT NULL "
            "AND writer_process_incarnation_id IS NOT NULL "
            "AND writer_started_at IS NOT NULL AND writer_ended_at IS NULL) OR "
            "(writer_state IN ('ENDED','RETIRED') AND writer_supplier_instance_id IS NOT NULL "
            "AND writer_process_incarnation_id IS NOT NULL AND writer_started_at IS NOT NULL "
            "AND writer_ended_at IS NOT NULL))",
            name="ck_delivery_writer_state",
        ),
        CheckConstraint(
            "(case_id IS NULL AND case_revision IS NULL AND assignment_revision IS NULL "
            "AND ((source_turn_id IS NOT NULL AND "
            "(grant_generation IS NULL OR grant_generation >= 1)) OR "
            "(source_message_id IS NOT NULL AND grant_generation IS NULL))) OR "
            "(case_id IS NOT NULL AND case_revision >= 1 "
            "AND assignment_revision >= 0 AND grant_generation >= 1 "
            "AND source_message_id IS NOT NULL)",
            name="ck_delivery_human_binding",
        ),
        CheckConstraint("revision >= 0 AND authority_revision >= 0", name="ck_delivery_revisions"),
        CheckConstraint(
            "(expected_settling_revision IS NULL AND fenced_settling_revision IS NULL) OR "
            "(expected_settling_revision IS NOT NULL AND expected_settling_revision >= 0 "
            "AND (fenced_settling_revision IS NULL OR (fenced_settling_revision IS NOT NULL "
            "AND fenced_settling_revision >= expected_settling_revision)))",
            name="ck_delivery_settling_fence",
        ),
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


class InboxGrantScope(Base):
    """Durable grant and independent CP/AI revocation floors, even pre-conversation."""

    __tablename__ = "inbox_grant_scopes"
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), primary_key=True)
    integration_id: Mapped[UUID] = mapped_column(primary_key=True)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), primary_key=True)
    agent_id: Mapped[UUID] = mapped_column(primary_key=True)
    account_id: Mapped[UUID | None] = mapped_column()
    agent_owner_id: Mapped[UUID | None] = mapped_column()
    cp_binding_generation: Mapped[int | None] = mapped_column(BigInteger)
    agent_lifecycle_generation: Mapped[int | None] = mapped_column(BigInteger)
    cp_binding_high_water: Mapped[int | None] = mapped_column(BigInteger)
    agent_lifecycle_high_water: Mapped[int | None] = mapped_column(BigInteger)
    grant_generation: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    active: Mapped[bool] = mapped_column(Boolean, default=False, server_default=sql_text("false"))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        CheckConstraint(OWNER_BOUNDS, name="ck_inbox_grant_owner_bounds"),
        CheckConstraint("channel IN ('TELEGRAM','WEBSITE_WIDGET')", name="ck_inbox_grant_channel"),
        CheckConstraint(
            "(cp_binding_high_water IS NULL OR cp_binding_high_water >= 0) AND "
            "(agent_lifecycle_high_water IS NULL OR agent_lifecycle_high_water >= 0) "
            "AND grant_generation >= 0",
            name="ck_inbox_grant_generations",
        ),
        CheckConstraint(
            "(active = false) OR (account_id IS NOT NULL AND agent_owner_id IS NOT NULL "
            "AND cp_binding_generation IS NOT NULL AND agent_lifecycle_generation IS NOT NULL "
            "AND (cp_binding_high_water IS NULL OR "
            "cp_binding_generation > cp_binding_high_water) "
            "AND (agent_lifecycle_high_water IS NULL OR "
            "agent_lifecycle_generation > agent_lifecycle_high_water) "
            "AND grant_generation >= 1)",
            name="ck_inbox_grant_active_shape",
        ),
    )


class InboxGrantLifecycleReceipt(Base):
    """Exact durable replay receipts for grant provisioning and revocation."""

    __tablename__ = "inbox_grant_lifecycle_receipts"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    command_id: Mapped[UUID] = mapped_column(nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    acknowledgement: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "agent_id",
            "command_id",
            name="uq_inbox_grant_lifecycle_command",
        ),
        CheckConstraint(OWNER_BOUNDS, name="ck_inbox_grant_lifecycle_owner_bounds"),
        CheckConstraint("channel IN ('TELEGRAM','WEBSITE_WIDGET')", name="ck_inbox_grant_lifecycle_channel"),
        CheckConstraint(
            "kind IN ('PROVISION','REVOKE_CONTROL_PLANE','REVOKE_AI_AGENT')",
            name="ck_inbox_grant_lifecycle_kind",
        ),
        CheckConstraint(
            digest_check("request_fingerprint"), name="ck_inbox_grant_lifecycle_fingerprint"
        ),
        Index(
            "ix_inbox_grant_lifecycle_scope",
            "owner_id",
            "integration_id",
            "channel",
            "agent_id",
            "created_at",
        ),
    )


class HandoffCase(Base):
    __tablename__ = "handoff_cases"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    control_request_id: Mapped[UUID] = mapped_column(nullable=False)
    control_request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    source_turn_id: Mapped[UUID] = mapped_column(nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    note: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    case_revision: Mapped[int] = mapped_column(BigInteger, default=1, server_default=sql_text("1"))
    authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    opened_authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    assignment_revision: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default=sql_text("0")
    )
    assigned_account_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint("id", "conversation_id", name="uq_handoff_case_id_conversation"),
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "conversation_id",
            "control_request_id",
            name="uq_handoff_control_request",
        ),
        ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
            ],
            name="fk_handoff_case_conversation_scope",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["source_turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            name="fk_handoff_case_source_turn",
            ondelete="RESTRICT",
        ),
        CheckConstraint("channel IN ('TELEGRAM','WEBSITE_WIDGET')", name="ck_handoff_case_channel"),
        CheckConstraint(
            "reason IN ('CUSTOMER_REQUEST','AGENT_LIMIT','BUSINESS_UNCERTAIN',"
            "'POLICY_REQUIRES_HUMAN','OTHER')",
            name="ck_handoff_case_reason",
        ),
        CheckConstraint("status IN ('OPEN','CLAIMED','RETURNED')", name="ck_handoff_case_status"),
        CheckConstraint(
            "case_revision >= 1 AND authority_revision >= 1 AND "
            "opened_authority_revision >= 1 AND "
            "opened_authority_revision <= authority_revision AND "
            "assignment_revision >= 0",
            name="ck_handoff_case_revisions",
        ),
        CheckConstraint(
            digest_check("control_request_fingerprint"), name="ck_handoff_control_fingerprint"
        ),
        CheckConstraint(
            "note IS NULL OR (char_length(note) BETWEEN 1 AND 1000 AND octet_length(note) <= 4000)",
            name="ck_handoff_case_note",
        ),
        CheckConstraint(
            "(status = 'CLAIMED' AND assigned_account_id IS NOT NULL) OR "
            "(status <> 'CLAIMED' AND assigned_account_id IS NULL)",
            name="ck_handoff_case_assignment",
        ),
        Index(
            "uq_handoff_one_active_case",
            "conversation_id",
            unique=True,
            postgresql_where=sql_text("status IN ('OPEN','CLAIMED')"),
        ),
        Index("ix_handoff_case_owner_status", "owner_id", "status", "updated_at"),
    )


class InboxCommandReceipt(Base):
    __tablename__ = "inbox_command_receipts"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    case_id: Mapped[UUID] = mapped_column(nullable=False)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    actor_account_id: Mapped[UUID] = mapped_column(nullable=False)
    command_id: Mapped[UUID] = mapped_column(nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    applied_case_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    applied_authority_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    applied_assignment_revision: Mapped[int | None] = mapped_column(BigInteger)
    message_id: Mapped[UUID | None] = mapped_column()
    outbound_intent_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "conversation_id",
            "actor_account_id",
            "command_id",
            name="uq_inbox_command_scope",
        ),
        ForeignKeyConstraint(
            ["case_id", "conversation_id"],
            ["handoff_cases.id", "handoff_cases.conversation_id"],
            name="fk_inbox_command_case",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            name="fk_inbox_command_message",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["outbound_intent_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            name="fk_inbox_command_delivery",
            ondelete="RESTRICT",
        ),
        CheckConstraint("channel IN ('TELEGRAM','WEBSITE_WIDGET')", name="ck_inbox_command_channel"),
        CheckConstraint(
            "kind IN ('CLAIM','SEND_HUMAN','RETURN_TO_AGENT')", name="ck_inbox_command_kind"
        ),
        CheckConstraint(digest_check("request_fingerprint"), name="ck_inbox_command_fingerprint"),
        CheckConstraint(
            "applied_case_revision >= 1 AND applied_authority_revision >= 0 AND "
            "(applied_assignment_revision IS NULL OR applied_assignment_revision >= 0)",
            name="ck_inbox_command_revisions",
        ),
        CheckConstraint(
            "(kind = 'SEND_HUMAN' AND message_id IS NOT NULL "
            "AND outbound_intent_id IS NOT NULL AND applied_assignment_revision IS NOT NULL) "
            "OR (kind <> 'SEND_HUMAN' AND message_id IS NULL "
            "AND outbound_intent_id IS NULL)",
            name="ck_inbox_command_message_shape",
        ),
        Index("ix_inbox_command_case", "case_id", "created_at"),
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
    expected_settling_revision: Mapped[int | None] = mapped_column(BigInteger)
    result_settling_revision: Mapped[int | None] = mapped_column(BigInteger)
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
        CheckConstraint(
            "(expected_settling_revision IS NULL AND result_settling_revision IS NULL) OR "
            "(expected_settling_revision IS NOT NULL AND result_settling_revision IS NOT NULL "
            "AND expected_settling_revision >= 0 AND result_settling_revision >= "
            "expected_settling_revision)",
            name="ck_reconciliation_settling_fence",
        ),
    )


class WebsiteAdmission(Base):
    """Frozen arbitration, including legacy reservations without canonical content."""

    __tablename__ = "website_admissions"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32), server_default="WEBSITE_WIDGET")
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    message_id: Mapped[UUID] = mapped_column(nullable=False)
    session_id: Mapped[UUID] = mapped_column(nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    route: Mapped[str] = mapped_column(String(16), nullable=False)
    conversation_id: Mapped[UUID | None] = mapped_column()
    canonical_message_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "provider_namespace",
            "message_id",
            name="uq_website_admission_identity",
        ),
        UniqueConstraint("canonical_message_id", name="uq_website_admission_message"),
        UniqueConstraint("id", "conversation_id", name="uq_website_admission_parent"),
        ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
            ],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["canonical_message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(OWNER_BOUNDS, name="ck_website_admission_owner"),
        CheckConstraint(
            "channel='WEBSITE_WIDGET' AND provider_namespace='main-chota.website-widget.v1'",
            name="ck_website_admission_namespace",
        ),
        CheckConstraint(digest_check("fingerprint"), name="ck_website_admission_fingerprint"),
        CheckConstraint(
            "(route='LEGACY' AND conversation_id IS NULL "
            "AND canonical_message_id IS NULL) OR (route='MANAGED' "
            "AND conversation_id IS NOT NULL AND canonical_message_id IS NOT NULL)",
            name="ck_website_admission_route",
        ),
        Index("ix_website_admission_processing", "conversation_id"),
    )


class WebsiteImmediateTurn(Base):
    __tablename__ = "website_immediate_turns"
    message_id: Mapped[UUID] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    turn_id: Mapped[UUID] = mapped_column(nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
            ["message_id"], ["website_admissions.canonical_message_id"], ondelete="RESTRICT"
        ),
        ForeignKeyConstraint(
            ["message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            ondelete="RESTRICT",
        ),
    )


class WebsiteResponseAttempt(Base):
    """Local HTTP write fact; never an application acknowledgement."""

    __tablename__ = "website_response_attempts"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    admission_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_id: Mapped[UUID] = mapped_column(nullable=False)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    send_authorized: Mapped[bool] = mapped_column(
        nullable=False, default=False, server_default=sql_text("false")
    )
    outcome: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        ForeignKeyConstraint(
            ["admission_id", "conversation_id"],
            ["website_admissions.id", "website_admissions.conversation_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["delivery_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "(outcome IS NULL AND finished_at IS NULL) OR "
            "(outcome IN ('WRITE_COMPLETED','WRITE_FAILED','UNKNOWN') "
            "AND finished_at IS NOT NULL)",
            name="ck_website_response_outcome",
        ),
    )


class TelegramAdmission(Base):
    """Sticky Telegram route and atomic link to accepted canonical input."""

    __tablename__ = "telegram_admissions"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32), server_default="TELEGRAM")
    provider_namespace: Mapped[str] = mapped_column(String(128), nullable=False)
    provider_event_id: Mapped[str] = mapped_column(String(512, collation="C"), nullable=False)
    provider_thread_id: Mapped[str] = mapped_column(String(512, collation="C"), nullable=False)
    update_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    chat_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    sender_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    bot_id: Mapped[str] = mapped_column(String(64, collation="C"), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    route: Mapped[str] = mapped_column(String(16), nullable=False)
    conversation_id: Mapped[UUID | None] = mapped_column()
    canonical_message_id: Mapped[UUID | None] = mapped_column()
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    __table_args__ = (
        UniqueConstraint(
            "owner_id",
            "integration_id",
            "channel",
            "provider_namespace",
            "provider_event_id",
            name="uq_telegram_admission_identity",
        ),
        UniqueConstraint("canonical_message_id", name="uq_telegram_admission_message"),
        UniqueConstraint("id", "conversation_id", name="uq_telegram_admission_parent"),
        ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
            ],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["canonical_message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(OWNER_BOUNDS, name="ck_telegram_admission_owner"),
        CheckConstraint(
            "channel='TELEGRAM' AND provider_namespace='main-chota.telegram.v1'",
            name="ck_telegram_admission_namespace",
        ),
        CheckConstraint(digest_check("fingerprint"), name="ck_telegram_admission_fingerprint"),
        CheckConstraint(
            "char_length(provider_event_id) BETWEEN 1 AND 512 AND "
            "octet_length(provider_event_id) <= 512",
            name="ck_telegram_admission_event_bounds",
        ),
        CheckConstraint(
            "char_length(provider_thread_id) BETWEEN 1 AND 512 AND "
            "octet_length(provider_thread_id) <= 512",
            name="ck_telegram_admission_thread_bounds",
        ),
        CheckConstraint("update_id >= 0", name="ck_telegram_admission_update_id"),
        CheckConstraint(
            "chat_id ~ '^[1-9][0-9]{0,18}$' AND "
            "message_id ~ '^[1-9][0-9]{0,18}$' AND "
            "sender_id ~ '^[1-9][0-9]{0,18}$' AND "
            "bot_id ~ '^[1-9][0-9]{0,18}$'",
            name="ck_telegram_admission_provider_ids",
        ),
        CheckConstraint(
            "(route='LEGACY' AND conversation_id IS NULL "
            "AND canonical_message_id IS NULL) OR (route='MANAGED' "
            "AND conversation_id IS NOT NULL AND canonical_message_id IS NOT NULL)",
            name="ck_telegram_admission_route",
        ),
        Index("ix_telegram_admission_processing", "conversation_id"),
    )


class TelegramProviderAccount(Base):
    """Immutable provider-account pin for a managed Telegram integration."""

    __tablename__ = "telegram_provider_accounts"
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), primary_key=True)
    integration_id: Mapped[UUID] = mapped_column(primary_key=True)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    bot_id: Mapped[str | None] = mapped_column(String(64, collation="C"))
    __table_args__ = (
        CheckConstraint(
            "(state='BOUND' AND bot_id IS NOT NULL "
            "AND bot_id ~ '^[1-9][0-9]{0,18}$') OR "
            "(state='CONFLICT' AND bot_id IS NULL)",
            name="ck_telegram_provider_account_shape",
        ),
    )


class TelegramImmediateTurn(Base):
    __tablename__ = "telegram_immediate_turns"
    message_id: Mapped[UUID] = mapped_column(primary_key=True)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    turn_id: Mapped[UUID] = mapped_column(nullable=False)
    __table_args__ = (
        ForeignKeyConstraint(
            ["message_id"], ["telegram_admissions.canonical_message_id"], ondelete="RESTRICT"
        ),
        ForeignKeyConstraint(
            ["message_id", "conversation_id"],
            ["messages.id", "messages.conversation_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["turn_id", "conversation_id"],
            ["agent_turns.id", "agent_turns.conversation_id"],
            ondelete="RESTRICT",
        ),
    )


class TelegramResponseAttempt(Base):
    """At most one Telegram provider write per logical managed draft."""

    __tablename__ = "telegram_response_attempts"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    admission_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_id: Mapped[UUID] = mapped_column(nullable=False)
    conversation_id: Mapped[UUID] = mapped_column(nullable=False)
    send_authorized: Mapped[bool] = mapped_column(
        nullable=False, default=False, server_default=sql_text("false")
    )
    outcome: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=sql_text("clock_timestamp()")
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        ForeignKeyConstraint(
            ["admission_id", "conversation_id"],
            ["telegram_admissions.id", "telegram_admissions.conversation_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["delivery_id", "conversation_id"],
            ["delivery_attempts.id", "delivery_attempts.conversation_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "(outcome IS NULL AND finished_at IS NULL) OR "
            "(outcome IN ('PROVIDER_ACCEPTED','FAILED','UNKNOWN') "
            "AND finished_at IS NOT NULL)",
            name="ck_telegram_response_outcome",
        ),
    )
