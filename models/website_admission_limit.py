"""Mutable anonymous admission counters; no credentials, client IPs or content."""
from datetime import datetime
from uuid import UUID

from sqlalchemy import CHAR, CheckConstraint, DateTime, Index, String
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class WebsiteCreationBucket(Base):
    __tablename__ = "website_creation_buckets"
    owner_id: Mapped[str] = mapped_column(String(255, collation="C"), primary_key=True)
    integration_id: Mapped[UUID] = mapped_column(primary_key=True)
    subject_digest: Mapped[str] = mapped_column(CHAR(64), primary_key=True)
    theoretical_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        CheckConstraint("subject_digest ~ '^[0-9a-f]{64}$'",
                        name="ck_website_create_bucket_digest"),
        Index("ix_website_create_bucket_expiry", "owner_id", "integration_id", "theoretical_at"),
    )
