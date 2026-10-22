"""Visitor credential bindings on canonical roots; no separate conversation history."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    CHAR,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class WebsiteSessionCapability(Base):
    __tablename__ = "website_session_capabilities"

    conversation_id: Mapped[UUID] = mapped_column(primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), nullable=False)
    integration_id: Mapped[UUID] = mapped_column(nullable=False)
    channel: Mapped[str] = mapped_column(String(32, collation="C"), nullable=False)
    provider_thread_id: Mapped[str] = mapped_column(String(512, collation="C"), nullable=False)
    agent_id: Mapped[UUID] = mapped_column(nullable=False)
    binding_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    grant_generation: Mapped[int | None] = mapped_column(BigInteger)
    agent_lifecycle_generation: Mapped[int | None] = mapped_column(BigInteger)
    provision_command_id: Mapped[UUID | None] = mapped_column()
    token_digest: Mapped[str] = mapped_column(CHAR(64), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("token_digest", name="uq_website_session_digest"),
        ForeignKeyConstraint(
            ["conversation_id", "owner_id", "integration_id", "channel", "provider_thread_id"],
            [
                "conversations.id",
                "conversations.owner_id",
                "conversations.integration_id",
                "conversations.channel",
                "conversations.provider_thread_id",
            ],
            name="fk_website_session_canonical_root",
        ),
        ForeignKeyConstraint(
            ["owner_id", "integration_id", "channel", "agent_id", "provision_command_id"],
            [
                "inbox_grant_lifecycle_receipts.owner_id",
                "inbox_grant_lifecycle_receipts.integration_id",
                "inbox_grant_lifecycle_receipts.channel",
                "inbox_grant_lifecycle_receipts.agent_id",
                "inbox_grant_lifecycle_receipts.command_id",
            ],
            name="fk_website_session_grant_provision",
        ),
        CheckConstraint("channel = 'WEBSITE_WIDGET'", name="ck_website_session_channel"),
        CheckConstraint("binding_generation > 0", name="ck_website_session_generation"),
        CheckConstraint(
            "(grant_generation IS NULL AND agent_lifecycle_generation IS NULL "
            "AND provision_command_id IS NULL) OR "
            "(grant_generation IS NOT NULL AND agent_lifecycle_generation IS NOT NULL "
            "AND grant_generation > 0 AND agent_lifecycle_generation >= 0 "
            "AND provision_command_id IS NOT NULL)",
            name="ck_website_session_grant_epoch",
        ),
        CheckConstraint("token_digest ~ '^[0-9a-f]{64}$'", name="ck_website_session_digest"),
        CheckConstraint(
            "expires_at > issued_at AND expires_at <= issued_at + interval '24 hours' "
            "AND (revoked_at IS NULL OR revoked_at >= issued_at)",
            name="ck_website_session_clock",
        ),
    )
