"""Durable 30/minute, burst2, single20s poll reservation under the canonical root."""
import math
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import WebsitePollState
from schemas.contracts import ErrorCode, Principal
from schemas.website_poll import (
    WebsitePollAccess,
    WebsitePollBegin,
    WebsitePollEnded,
    WebsitePollStarted,
)
from services import website_session
from services.common import db_clock


async def begin(database: Database, principal: Principal, request: WebsitePollBegin,
                settings: Settings) -> WebsitePollStarted:
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_access(session, principal, request, settings)
        row = await session.get(WebsitePollState, root.id)
        clock = await db_clock(session)
        if row is not None:
            if row.request_id == request.request_id:
                if (row.supplier_instance_id != request.supplier_instance_id
                        or row.process_incarnation_id != request.process_incarnation_id):
                    raise DomainError(ErrorCode.NOT_FOUND)
                return WebsitePollStarted(state="RECOVER_ONLY")
            if row.ended_at is None and row.deadline_at > clock:
                retry = math.ceil((row.deadline_at - clock).total_seconds())
                raise DomainError(ErrorCode.RATE_LIMITED,
                    retry_after=max(1, min(20, retry)))
            earliest = row.theoretical_at - timedelta(seconds=2)
            if clock < earliest:
                raise DomainError(ErrorCode.RATE_LIMITED,
                    retry_after=max(1, min(60, math.ceil((earliest - clock).total_seconds()))))
        theoretical = max(row.theoretical_at, clock) if row is not None else clock
        values = dict(request_id=request.request_id, poll_id=uuid4(),
            supplier_instance_id=request.supplier_instance_id,
            process_incarnation_id=request.process_incarnation_id,
            started_at=clock, deadline_at=clock + timedelta(seconds=20), ended_at=None,
            theoretical_at=theoretical + timedelta(seconds=2))
        if row is None:
            row = WebsitePollState(conversation_id=root.id, **values)
            session.add(row)
        else:
            for field, value in values.items():
                setattr(row, field, value)
        await session.flush()
        return WebsitePollStarted(state="ACQUIRED", poll_id=row.poll_id,
                                  deadline_at=row.deadline_at)


async def require_current(session: AsyncSession, conversation_id: UUID,
                          request: WebsitePollAccess) -> None:
    """Called under the root lock in the same transaction as work/acquisition."""
    row = await session.get(WebsitePollState, conversation_id)
    if (row is None or row.poll_id != request.poll_id or row.ended_at is not None
            or row.supplier_instance_id != request.supplier_instance_id
            or row.process_incarnation_id != request.process_incarnation_id
            or await db_clock(session) >= row.deadline_at):
        raise DomainError(ErrorCode.NOT_FOUND)


async def end(database: Database, principal: Principal,
              request: WebsitePollAccess) -> WebsitePollEnded:
    async with database.session_factory() as session, session.begin():
        # Original resource cleanup is metadata-only after expiry/revoke/config removal.
        root = await website_session.locked_original_writer(session, principal, request)
        row = await session.get(WebsitePollState, root.id)
        if (row is None or row.poll_id != request.poll_id
                or row.supplier_instance_id != request.supplier_instance_id
                or row.process_incarnation_id != request.process_incarnation_id):
            raise DomainError(ErrorCode.NOT_FOUND)
        replayed = row.ended_at is not None
        if not replayed:
            row.ended_at = await db_clock(session)
        await session.flush()
        return WebsitePollEnded(replayed=replayed)
