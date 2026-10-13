"""Authenticated, exact-scope Telegram canary ingress and delivery evidence."""

from fastapi import APIRouter

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from schemas.telegram import (
    TelegramAdmission,
    TelegramAdmissionResult,
    TelegramIdentity,
    TelegramResponseBegin,
    TelegramResponseBegun,
    TelegramResponseComplete,
    TelegramResponseCompleted,
    TelegramResult,
)
from services import telegram

router = APIRouter(prefix="/internal/v1/telegram")


@router.post("/admissions", response_model=TelegramAdmissionResult)
async def admit(request: TelegramAdmission, database: AppDatabase,
                principal: TrustedPrincipal, settings: AppSettings) -> TelegramAdmissionResult:
    return await telegram.admit(database, principal, request, settings)


@router.post("/results", response_model=TelegramResult)
async def result(request: TelegramIdentity, database: AppDatabase,
                 principal: TrustedPrincipal, settings: AppSettings) -> TelegramResult:
    return await telegram.result(database, principal, request, settings)


@router.post("/responses/begin", response_model=TelegramResponseBegun)
async def begin(request: TelegramResponseBegin, database: AppDatabase,
                principal: TrustedPrincipal, settings: AppSettings) -> TelegramResponseBegun:
    return await telegram.begin_response(database, principal, request, settings)


@router.post("/responses/complete", response_model=TelegramResponseCompleted)
async def complete(request: TelegramResponseComplete, database: AppDatabase,
                   principal: TrustedPrincipal,
                   settings: AppSettings) -> TelegramResponseCompleted:
    return await telegram.complete_response(database, principal, request, settings)
