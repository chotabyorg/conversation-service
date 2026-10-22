"""Current poll resource reservation and rate debt, never a delivery/writer ledger."""
from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class WebsitePollState(Base):
    __tablename__ = "website_poll_states"
    conversation_id: Mapped[UUID] = mapped_column(ForeignKey("conversations.id"), primary_key=True)
    request_id: Mapped[UUID] = mapped_column(nullable=False)
    poll_id: Mapped[UUID] = mapped_column(nullable=False, unique=True)
    supplier_instance_id: Mapped[UUID] = mapped_column(nullable=False)
    process_incarnation_id: Mapped[UUID] = mapped_column(nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    theoretical_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        CheckConstraint("deadline_at > started_at AND "
                        "deadline_at <= started_at + INTERVAL '20 seconds'",
                        name="ck_website_poll_deadline"),
        CheckConstraint("ended_at IS NULL OR ended_at >= started_at",
                        name="ck_website_poll_end"),
    )
