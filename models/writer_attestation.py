"""Narrow lifecycle evidence linked to the existing delivery journal."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from db.base import Base


class WriterRegistration(Base):
    __tablename__ = "writer_registrations"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    intent_id: Mapped[UUID] = mapped_column(ForeignKey("delivery_attempts.id"), nullable=False)
    producer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    supplier_instance_id: Mapped[UUID] = mapped_column(nullable=False)
    process_incarnation_id: Mapped[UUID] = mapped_column(nullable=False)
    descriptor: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        UniqueConstraint(
            "intent_id",
            "supplier_instance_id",
            "process_incarnation_id",
            name="uq_writer_registration_tuple",
        ),
    )


class WriterWitness(Base):
    __tablename__ = "writer_witnesses"
    registration_id: Mapped[UUID] = mapped_column(
        ForeignKey("writer_registrations.id"), primary_key=True
    )
    proof_ref: Mapped[UUID] = mapped_column(nullable=False, unique=True)
    witness: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WriterRecoveryCommand(Base):
    __tablename__ = "writer_recovery_commands"
    id: Mapped[UUID] = mapped_column(primary_key=True)
    registration_id: Mapped[UUID] = mapped_column(
        ForeignKey("writer_witnesses.registration_id"), nullable=False
    )
    expected_revision: Mapped[int] = mapped_column(BigInteger, nullable=False)
    command: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    __table_args__ = (
        UniqueConstraint(
            "registration_id", "expected_revision", name="uq_writer_recovery_revision"
        ),
    )
