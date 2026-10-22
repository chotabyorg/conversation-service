"""PostgreSQL-clock GCRA admission; one integration lock, atomic quota reservation."""
import hashlib
import math
from datetime import datetime, timedelta
from ipaddress import IPv4Address, IPv6Address

from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.errors import DomainError
from models import WebsiteCreationBucket, WebsiteSessionCapability
from schemas.contracts import ErrorCode
from schemas.website_session import WebsiteSessionCreate
from services.common import db_clock, fingerprint

MAX_UNEXPIRED = 1000


def _subject(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


async def reserve(session: AsyncSession, request: WebsiteSessionCreate) -> datetime:
    # Caller already holds the common scope; no code takes this integration
    # advisory lock before a scope. Different retained agent epochs share quota.
    identity = dict(owner_id=request.owner_id, integration_id=request.integration_id)
    key = fingerprint(dict(namespace="website-creation-admission-v1",
                           owner=request.owner_id, integration=str(request.integration_id)))
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                          {"key": int(key[:16], 16) - (1 << 63)})
    clock = await db_clock(session)
    count = await session.scalar(select(func.count()).select_from(WebsiteSessionCapability).where(
        WebsiteSessionCapability.owner_id == request.owner_id,
        WebsiteSessionCapability.integration_id == request.integration_id,
        WebsiteSessionCapability.expires_at > clock))
    if count is not None and count >= MAX_UNEXPIRED:
        raise DomainError(ErrorCode.RATE_LIMITED, retry_after=60)
    # Prune before loading/updating bucket objects. A bulk DELETE after pending
    # ORM updates could remove their old database versions before the flush.
    await session.execute(delete(WebsiteCreationBucket).where(
        WebsiteCreationBucket.owner_id == request.owner_id,
        WebsiteCreationBucket.integration_id == request.integration_id,
        WebsiteCreationBucket.theoretical_at < clock - timedelta(minutes=2)))
    ip: IPv4Address | IPv6Address = request.client_ip
    if isinstance(ip, IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    rules = ((_subject("CREATE_TOTAL"), 1, 10), (_subject("CREATE_IP:" + str(ip)), 12, 2))
    updates: list[tuple[WebsiteCreationBucket, datetime]] = []
    for subject, seconds, burst in sorted(rules):
        row = await session.get(WebsiteCreationBucket,
                                (request.owner_id, request.integration_id, subject))
        current = row.theoretical_at if row is not None else clock
        earliest = current - timedelta(seconds=(burst - 1) * seconds)
        if clock < earliest:
            raise DomainError(ErrorCode.RATE_LIMITED,
                retry_after=max(1, min(60, math.ceil((earliest - clock).total_seconds()))))
        if row is None:
            row = WebsiteCreationBucket(**identity, subject_digest=subject, theoretical_at=clock)
            session.add(row)
        updates.append((row, max(current, clock) + timedelta(seconds=seconds)))
    # Failed admission rolls back all new buckets and creates no root/credential.
    for row, next_at in updates:
        row.theoretical_at = next_at
    return clock
