"""Fair bounded recovery; scan progress never grants authority or changes evidence."""

import asyncio
import signal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from core.config import Settings, load_settings
from core.errors import DomainError
from core.logging import configure_logging, logger
from db.database import Database
from models import DeliveryAttempt, WriterRecoveryScan, WriterRegistration, WriterWitness
from schemas.contracts import Principal
from services.writer_attestation import recovery
from services.writer_retirement import retire_writer

BATCH_SIZE = 100
RETRY_SECONDS = 5.0
RETRYABLE = (SQLAlchemyError, OSError, TimeoutError)


def diagnostic(code: str) -> None:
    logger.warning("", extra={"stage": "writer_recovery", "code": code})


async def page(database: Database) -> list[UUID]:
    # One short transaction serializes scan position only, never locks delivery
    # authority, and commits before proof I/O. A crash skips at most one bounded
    # page until the next wrap; failing rows cannot monopolize future pages.
    async with database.session_factory() as session, session.begin():
        await session.execute(
            insert(WriterRecoveryScan)
            .values(name="text-v1")
            .on_conflict_do_nothing(index_elements=["name"])
        )
        cursor = await session.scalar(
            select(WriterRecoveryScan)
            .where(WriterRecoveryScan.name == "text-v1")
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        assert cursor is not None
        query = (
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
            .order_by(WriterRegistration.id)
            .limit(BATCH_SIZE)
        )
        selected = query
        if cursor.after_registration_id is not None:
            selected = query.where(WriterRegistration.id > cursor.after_registration_id)
        ids = list((await session.scalars(selected)).all())
        if not ids and cursor.after_registration_id is not None:
            ids = list((await session.scalars(query)).all())
        cursor.after_registration_id = ids[-1] if ids else None
        return ids


async def sweep(
    database: Database, settings: Settings, stopped: asyncio.Event | None = None
) -> int:
    try:
        ids = await page(database)
    except RETRYABLE:
        diagnostic("WRITER_RECOVERY_RETRY")
        return 0
    principal = Principal(
        principal_id="delivery-writer-supervisor", role="INBOX_LIFECYCLE", grants=()
    )
    completed = 0
    for registration_id in ids:
        if stopped is not None and stopped.is_set():
            break
        try:
            result = await recovery(database, registration_id, settings)
            if result.command is not None:
                await retire_writer(database, principal, result.command, settings)
                completed += 1
        except DomainError:
            diagnostic("WRITER_RECOVERY_PENDING")
        except RETRYABLE:
            # The operation owns and closes its session on failure. Do not turn
            # unavailability into writer termination or replace a retained command.
            diagnostic("WRITER_RECOVERY_RETRY")
    return completed


async def drive(database: Database, settings: Settings, stopped: asyncio.Event) -> None:
    while not stopped.is_set():
        try:
            if await database.ready():
                await sweep(database, settings, stopped)
            else:
                diagnostic("WRITER_RECOVERY_RETRY")
        except RETRYABLE:
            diagnostic("WRITER_RECOVERY_RETRY")
        # Outage retries are bounded in time and the wait wakes immediately for
        # shutdown. CancelledError is never swallowed at any boundary.
        try:
            await asyncio.wait_for(stopped.wait(), timeout=RETRY_SECONDS)
        except TimeoutError:
            pass


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
        await drive(database, settings, stopped)
    finally:
        await database.dispose()


def main() -> int:
    # Startup, unexpected bugs and final disposal must never bypass the safe
    # formatter with raw exception/SQL/configuration tracebacks.
    configure_logging("INFO")
    try:
        asyncio.run(run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        return 0
    except Exception:
        diagnostic("WRITER_RECOVERY_STOPPED")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
