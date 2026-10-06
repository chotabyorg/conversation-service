"""Thin internal routes; transactions and authorization filters are service-owned."""

from uuid import UUID

from fastapi import APIRouter, Query, Request, Response

from api.dependencies import AppDatabase, AppManagedPort, AppSettings, TrustedPrincipal
from core.errors import DomainError
from core.logging import logger
from schemas.contracts import (
    MAX_COUNTER,
    ControlTransition,
    ConversationRead,
    DeliveryAttemptRead,
    DeliveryCreate,
    DeliveryReconcile,
    DeliveryReconcileResult,
    DeliveryResult,
    ErrorCode,
    InboundEventCreate,
    InboundEventResult,
    MessagePage,
    MessageRead,
    TurnClaim,
    TurnClaimResult,
    TurnComplete,
    TurnCreate,
    TurnExecute,
    TurnFail,
    TurnRead,
    TurnRenew,
    TurnResult,
)
from services import authority, delivery, journal, managed_execution, turns

router = APIRouter(prefix="/internal/v1")


@router.post("/inbound-events", response_model=InboundEventResult, status_code=201)
async def ingest(
    request: InboundEventCreate,
    response: Response,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> InboundEventResult:
    result = await journal.ingest_event(database, principal, request)
    response.status_code = 200 if result.replayed else 201
    logger.info(
        "",
        extra={
            "stage": "journal",
            "conversation_id": result.conversation.id,
            "event_id": result.event.id,
            "message_id": result.message.id,
        },
    )
    return result


@router.get("/conversations/{conversation_id}", response_model=ConversationRead)
async def conversation(
    conversation_id: UUID,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> ConversationRead:
    return await journal.get_conversation(database, principal, conversation_id)


@router.get("/conversations/{conversation_id}/messages", response_model=MessagePage)
async def messages(
    conversation_id: UUID,
    database: AppDatabase,
    principal: TrustedPrincipal,
    after_sequence: str = Query(default="0", max_length=19, pattern=r"^[0-9]+$"),
    limit: str = Query(default="50", max_length=3, pattern=r"^[0-9]+$"),
) -> MessagePage:
    after = int(after_sequence)
    page_limit = int(limit)
    if after > MAX_COUNTER or not 1 <= page_limit <= 100:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    return await journal.list_messages(
        database, principal, conversation_id, after_sequence=after, limit=page_limit
    )


@router.get("/messages/{message_id}", response_model=MessageRead)
async def message(
    message_id: UUID,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> MessageRead:
    return await journal.get_message(database, principal, message_id)


@router.post("/conversations/{conversation_id}/turns", response_model=TurnResult, status_code=201)
async def create_turn(
    conversation_id: UUID,
    request: TurnCreate,
    response: Response,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> TurnResult:
    result = await turns.create_turn(database, principal, conversation_id, request)
    response.status_code = 200 if result.replayed else 201
    logger.info(
        "",
        extra={
            "stage": "turn",
            "conversation_id": result.turn.conversation_id,
            "turn_id": result.turn.id,
        },
    )
    return result


@router.get("/turns/{turn_id}", response_model=TurnRead)
async def turn(turn_id: UUID, database: AppDatabase, principal: TrustedPrincipal) -> TurnRead:
    return await turns.get_turn(database, principal, turn_id)


@router.post("/turns/{turn_id}/claim", response_model=TurnClaimResult)
async def claim(
    turn_id: UUID,
    request: TurnClaim,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> TurnClaimResult:
    result = await turns.claim_turn(database, principal, turn_id, request)
    logger.info(
        "",
        extra={
            "stage": "turn",
            "conversation_id": result.turn.conversation_id,
            "turn_id": result.turn.id,
        },
    )
    return result


@router.post("/turns/{turn_id}/renew", response_model=TurnRead)
async def renew(
    turn_id: UUID,
    request: TurnRenew,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> TurnRead:
    return await turns.renew_turn(database, principal, turn_id, request)


@router.post("/turns/{turn_id}/execute", response_model=TurnResult)
async def execute(
    turn_id: UUID,
    request: TurnExecute,
    http_request: Request,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
    port: AppManagedPort,
) -> TurnResult:
    return await managed_execution.execute_claimed_turn(
        database, principal, turn_id, request, settings, port,
        getattr(http_request.state, "request_id", ""),
    )


@router.post("/turns/{turn_id}/complete", response_model=TurnResult)
async def complete(
    turn_id: UUID,
    request: TurnComplete,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> TurnResult:
    result = await turns.complete_turn(database, principal, turn_id, request)
    logger.info(
        "",
        extra={
            "stage": "turn",
            "conversation_id": result.turn.conversation_id,
            "turn_id": result.turn.id,
        },
    )
    return result


@router.post("/turns/{turn_id}/fail", response_model=TurnResult)
async def fail(
    turn_id: UUID,
    request: TurnFail,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> TurnResult:
    result = await turns.fail_turn(database, principal, turn_id, request)
    logger.info(
        "",
        extra={
            "stage": "turn",
            "conversation_id": result.turn.conversation_id,
            "turn_id": result.turn.id,
        },
    )
    return result


@router.post("/conversations/{conversation_id}/control", response_model=ConversationRead)
async def control(
    conversation_id: UUID,
    request: ControlTransition,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> ConversationRead:
    result = await authority.transition_control(database, principal, conversation_id, request)
    logger.info("", extra={"stage": "authority", "conversation_id": result.id})
    return result


@router.post(
    "/conversations/{conversation_id}/delivery-attempts",
    response_model=DeliveryResult,
    status_code=201,
)
async def create_delivery(
    conversation_id: UUID,
    request: DeliveryCreate,
    response: Response,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> DeliveryResult:
    result = await delivery.create_delivery(database, principal, conversation_id, request)
    response.status_code = 200 if result.replayed else 201
    logger.info(
        "",
        extra={
            "stage": "delivery",
            "conversation_id": result.attempt.conversation_id,
            "turn_id": result.attempt.source_turn_id,
            "delivery_attempt_id": result.attempt.id,
        },
    )
    return result


@router.get("/delivery-attempts/{delivery_attempt_id}", response_model=DeliveryAttemptRead)
async def delivery_attempt(
    delivery_attempt_id: UUID,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> DeliveryAttemptRead:
    return await delivery.get_delivery(database, principal, delivery_attempt_id)


@router.post(
    "/delivery-attempts/{delivery_attempt_id}/reconcile", response_model=DeliveryReconcileResult
)
async def reconcile(
    delivery_attempt_id: UUID,
    request: DeliveryReconcile,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> DeliveryReconcileResult:
    result = await delivery.reconcile_delivery(database, principal, delivery_attempt_id, request)
    logger.info(
        "",
        extra={
            "stage": "delivery",
            "conversation_id": result.attempt.conversation_id,
            "delivery_attempt_id": result.attempt.id,
        },
    )
    return result
