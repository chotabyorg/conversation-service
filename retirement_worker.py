"""Bounded recovery driver. Run separately with existing retirement/proof configuration."""

import asyncio
import signal

from sqlalchemy import select

from core.config import Settings, load_settings
from core.errors import DomainError
from core.logging import configure_logging, logger
from db.database import Database
from models import DeliveryAttempt, WriterRegistration, WriterWitness
from schemas.contracts import Principal
from services.writer_attestation import recovery
from services.writer_retirement import retire_writer


async def sweep(database: Database, settings: Settings) -> int:
    # A bounded snapshot is released before any proof network request.
    async with database.session_factory() as session:
        ids = (
            await session.scalars(
                select(WriterRegistration.id)
                .join(WriterWitness, WriterWitness.registration_id == WriterRegistration.id)
                .join(DeliveryAttempt, DeliveryAttempt.id == WriterRegistration.intent_id)
                .where(
                    DeliveryAttempt.writer_state == "LIVE",
                    DeliveryAttempt.writer_supplier_instance_id
                    == WriterRegistration.supplier_instance_id,
                    DeliveryAttempt.writer_process_incarnation_id
                    == WriterRegistration.process_incarnation_id,
                )
                .order_by(WriterWitness.created_at, WriterRegistration.id)
                .limit(100)
            )
        ).all()
    principal = Principal(
        principal_id="delivery-writer-supervisor", role="INBOX_LIFECYCLE", grants=()
    )
    completed = 0
    for registration_id in ids:
        try:
            result = await recovery(database, registration_id, settings)
            if result.command is not None:
                await retire_writer(database, principal, result.command, settings)
                completed += 1
        except DomainError:
            logger.warning(
                "", extra={"stage": "writer_recovery", "code": "WRITER_RECOVERY_PENDING"}
            )
    return completed


async def run() -> None:
    settings = load_settings()
    if (
        settings.writer_retirement_token is None
        or settings.writer_supervisor_token is None
        or settings.writer_supervisor_base_url is None
    ):
        raise RuntimeError("Writer recovery requires dedicated retirement and proof configuration")
    configure_logging(settings.log_level)
    database = Database(settings.database_url.get_secret_value())
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(name, stopped.set)
    try:
        while not stopped.is_set():
            if await database.ready():
                await sweep(database, settings)
            try:
                await asyncio.wait_for(stopped.wait(), timeout=5)
            except TimeoutError:
                pass
    finally:
        await database.dispose()


if __name__ == "__main__":
    asyncio.run(run())
