"""Telegram provider routes for enrolled Handoff outbound intents."""

from fastapi import APIRouter

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from schemas.handoff import (
    TelegramBeginRequest,
    TelegramBeginResult,
    TelegramCompleteRequest,
    TelegramCompleteResult,
    TelegramWorkRequest,
    TelegramWorkResult,
)
from services import telegram_delivery_v2

router = APIRouter(prefix="/internal/v2/telegram/delivery")


@router.post("/work", response_model=TelegramWorkResult)
async def work_route(
    request: TelegramWorkRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> TelegramWorkResult:
    return await telegram_delivery_v2.work(database, principal, request, settings)


@router.post("/begin", response_model=TelegramBeginResult)
async def begin_route(
    request: TelegramBeginRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> TelegramBeginResult:
    return await telegram_delivery_v2.begin(database, principal, request, settings)


@router.post("/complete", response_model=TelegramCompleteResult)
async def complete_route(
    request: TelegramCompleteRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> TelegramCompleteResult:
    return await telegram_delivery_v2.complete(database, principal, request, settings)
