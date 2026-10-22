"""Append-only visibility positions over canonical Messages, without copied content."""
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class WebsiteVisibleMessage(Base):
    __tablename__ = "website_visible_messages"
    conversation_id: Mapped[UUID] = mapped_column(primary_key=True)
    ordinal: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    message_id: Mapped[UUID] = mapped_column(nullable=False)
    public_message_id: Mapped[UUID] = mapped_column(nullable=False)
    delivery_attempt_id: Mapped[UUID | None] = mapped_column()

    __table_args__ = (
        UniqueConstraint("message_id", name="uq_website_visible_message"),
        UniqueConstraint("public_message_id", name="uq_website_visible_public_id"),
        CheckConstraint("ordinal > 0", name="ck_website_visible_ordinal"),
        ForeignKeyConstraint(["conversation_id"], ["website_session_capabilities.conversation_id"],
                             name="fk_website_visible_credential", ondelete="RESTRICT"),
        ForeignKeyConstraint(["message_id", "conversation_id"],
                             ["messages.id", "messages.conversation_id"],
                             name="fk_website_visible_message", ondelete="RESTRICT"),
        ForeignKeyConstraint(["delivery_attempt_id", "conversation_id"],
                             ["delivery_attempts.id", "delivery_attempts.conversation_id"],
                             name="fk_website_visible_delivery", ondelete="RESTRICT"),
    )
