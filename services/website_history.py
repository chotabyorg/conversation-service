"""Bounded credential history over canonical Messages and original delivery truth."""
from uuid import UUID, uuid5

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import AgentTurn, DeliveryAttempt, Message, WebsiteAdmission, WebsiteVisibleMessage
from schemas.contracts import ErrorCode, Principal
from schemas.website_history import WebsiteHistoryMessage, WebsiteHistoryPage, WebsiteHistoryRequest
from services import website_session
from services.website_delivery import PUBLIC_NAMESPACE

CUSTOMER_NAMESPACE = UUID("0ce33d10-a470-492a-a886-375ffce556ae")
MAX_PAGE_BYTES = 262_144
MAX_PAGE_ITEMS = 50
MAX_MATERIALIZE = 100


async def _materialize(session: AsyncSession, root_id: UUID, agent_id: UUID) -> bool:
    # Scope and canonical root are already locked. A late positive acceptance gets
    # a new visibility position even when its canonical message sequence is older
    # than the visitor's cursor. No text or parallel history is persisted here.
    eligible_customer = and_(Message.actor == "CUSTOMER", Message.direction == "INBOUND",
        WebsiteAdmission.id.is_not(None))
    eligible_outbound = and_(Message.direction == "OUTBOUND", DeliveryAttempt.id.is_not(None),
        or_(Message.actor == "HUMAN", and_(Message.actor == "AGENT",
            AgentTurn.managed_contract_version == 3,
            AgentTurn.managed_projection_agent_id == agent_id)))
    rows = (await session.execute(select(Message.id, DeliveryAttempt.id).outerjoin(
        WebsiteAdmission, and_(WebsiteAdmission.canonical_message_id == Message.id,
            WebsiteAdmission.conversation_id == root_id, WebsiteAdmission.agent_id == agent_id,
            WebsiteAdmission.route == "MANAGED")).outerjoin(DeliveryAttempt, and_(
            DeliveryAttempt.conversation_id == root_id,
            DeliveryAttempt.outbound_ordinal.is_not(None),
            DeliveryAttempt.provider_accepted_at.is_not(None),
            DeliveryAttempt.status.in_(("PROVIDER_ACCEPTED", "DELIVERED", "READ")),
            or_(DeliveryAttempt.source_message_id == Message.id,
                DeliveryAttempt.source_turn_id == Message.source_agent_turn_id))).outerjoin(
        AgentTurn, AgentTurn.id == Message.source_agent_turn_id).where(
            Message.conversation_id == root_id, Message.content_type == "TEXT",
            or_(eligible_customer, eligible_outbound),
            ~exists(select(WebsiteVisibleMessage.message_id).where(
                WebsiteVisibleMessage.message_id == Message.id)),
        ).order_by(Message.sequence).limit(MAX_MATERIALIZE + 1))).all()
    last = await session.scalar(select(func.max(WebsiteVisibleMessage.ordinal)).where(
        WebsiteVisibleMessage.conversation_id == root_id)) or 0
    for message_id, delivery_id in rows[:MAX_MATERIALIZE]:
        last += 1
        session.add(WebsiteVisibleMessage(conversation_id=root_id, ordinal=last,
            message_id=message_id, delivery_attempt_id=delivery_id,
            public_message_id=uuid5(PUBLIC_NAMESPACE if delivery_id else CUSTOMER_NAMESPACE,
                                   str(delivery_id or message_id))))
    await session.flush()
    return len(rows) > MAX_MATERIALIZE


async def history(database: Database, principal: Principal, request: WebsiteHistoryRequest,
                  settings: Settings) -> WebsiteHistoryPage:
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_access(session, principal, request, settings)
        after = 0
        if request.cursor is not None:
            position = await session.scalar(select(WebsiteVisibleMessage.ordinal).where(
                WebsiteVisibleMessage.conversation_id == root.id,
                WebsiteVisibleMessage.public_message_id == request.cursor))
            if position is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            after = position
        more_unindexed = await _materialize(session, root.id, request.agent_id)
        rows = (await session.execute(select(WebsiteVisibleMessage, Message,
            WebsiteAdmission.message_id).join(
                Message, Message.id == WebsiteVisibleMessage.message_id)
            .outerjoin(WebsiteAdmission, WebsiteAdmission.canonical_message_id == Message.id)
            .where(WebsiteVisibleMessage.conversation_id == root.id,
                   WebsiteVisibleMessage.ordinal > after)
            .order_by(WebsiteVisibleMessage.ordinal).limit(MAX_PAGE_ITEMS + 1))).all()
        items: list[WebsiteHistoryMessage] = []
        cursor = request.cursor
        for visible, message, client_id in rows[:MAX_PAGE_ITEMS]:
            item = WebsiteHistoryMessage.model_validate(dict(id=visible.public_message_id,
                actor=message.actor, text=message.text, client_message_id=client_id))
            candidate = WebsiteHistoryPage(items=[*items, item],
                                            next_cursor=visible.public_message_id)
            if len(candidate.model_dump_json().encode("utf-8")) > MAX_PAGE_BYTES:
                if not items:
                    return WebsiteHistoryPage(state="FRAME_TOO_LARGE", items=[],
                        next_cursor=request.cursor, oversized_message_id=visible.public_message_id)
                return WebsiteHistoryPage(items=items, next_cursor=cursor)
            items.append(item)
            cursor = visible.public_message_id
        more = len(rows) > MAX_PAGE_ITEMS or more_unindexed
        return WebsiteHistoryPage(items=items, next_cursor=cursor if more else None)
