"""Shared scope-before-root fence for legacy text writers."""
from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from models import InboxGrantScope


async def lock_legacy_scopes(
    session: AsyncSession, *, owner_id: str, integration_id: UUID, channel: str,
    settings: Settings | None = None, extra_agent_ids: Iterable[UUID] = (),
) -> bool:
    """Lock retained/configured scopes in order and report any durable enrollment.

    The caller must still recheck enrollment after acquiring its canonical root:
    stale settings can miss an enrollment created while that root lock is pending.
    """
    if not session.in_transaction():
        raise RuntimeError("Text Handoff fencing requires the caller transaction")
    if channel not in {"TELEGRAM", "WEBSITE_WIDGET"}:
        return False
    from services import telegram

    agent_ids = set(await session.scalars(select(InboxGrantScope.agent_id).where(
        InboxGrantScope.owner_id == owner_id,
        InboxGrantScope.integration_id == integration_id,
        InboxGrantScope.channel == channel,
    ))) | set(extra_agent_ids)
    if settings is not None:
        agent_ids.update(binding.agent_id for binding in settings.managed_ai_bindings
            if binding.owner_id == owner_id and binding.integration_id == integration_id
            and binding.channel.value == channel)
        if channel == "WEBSITE_WIDGET":
            agent_ids.update(binding.agent_id for binding in settings.website_bindings
                if binding.owner_id == owner_id and binding.integration_id == integration_id)
    scopes = []
    for agent_id in sorted(agent_ids, key=str):
        row: InboxGrantScope | None
        if channel == "TELEGRAM":
            # Preserve the existing supplier hook and lock contract.
            row = await telegram._locked_handoff_scope_values(
                session, owner_id, integration_id, agent_id)
        else:
            # Legacy Website must not allocate a new common lifecycle row.
            # Enrollment locks the root before committing; the caller's root
            # recheck orders a concurrent first enrollment when no row exists.
            row = await session.scalar(select(InboxGrantScope).filter_by(owner_id=owner_id,
                integration_id=integration_id, channel=channel, agent_id=agent_id)
                .with_for_update().execution_options(populate_existing=True))
        if row is not None:
            scopes.append(row)
    return any(telegram._ever_enrolled(row) for row in scopes)
