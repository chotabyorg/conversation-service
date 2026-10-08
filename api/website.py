"""Narrow trusted Website routes, isolated from foundation execution authority."""
from uuid import UUID

from fastapi import APIRouter

from api.dependencies import AppDatabase, AppManagedPort, AppSettings, TrustedPrincipal
from schemas.website import (
    ManagedEvidenceResult,
    WebsiteAdmission,
    WebsiteAdmissionResult,
    WebsiteIdentity,
    WebsiteResponseBegin,
    WebsiteResponseBegun,
    WebsiteResponseComplete,
    WebsiteResponseCompleted,
    WebsiteResult,
)
from services import website
from services.website_evidence import operational_evidence

router = APIRouter(prefix="/internal/v1/website")


@router.post("/admissions", response_model=WebsiteAdmissionResult)
async def admit(request: WebsiteAdmission, database: AppDatabase,
                principal: TrustedPrincipal, settings: AppSettings) -> WebsiteAdmissionResult:
    return await website.admit(database, principal, request, settings)


@router.post("/results", response_model=WebsiteResult)
async def result(request: WebsiteIdentity, database: AppDatabase,
                 principal: TrustedPrincipal, settings: AppSettings) -> WebsiteResult:
    return await website.result(database, principal, request, settings)


@router.post("/responses/begin", response_model=WebsiteResponseBegun)
async def begin(request: WebsiteResponseBegin, database: AppDatabase,
                principal: TrustedPrincipal, settings: AppSettings) -> WebsiteResponseBegun:
    return await website.begin_response(database, principal, request, settings)


@router.post("/responses/complete", response_model=WebsiteResponseCompleted)
async def complete(request: WebsiteResponseComplete, database: AppDatabase,
                   principal: TrustedPrincipal, settings: AppSettings) -> WebsiteResponseCompleted:
    return await website.complete_response(database, principal, request, settings)


operations = APIRouter(prefix="/internal/v1/processing")


@operations.get("/turns/{turn_id}/evidence", response_model=ManagedEvidenceResult)
async def evidence(turn_id: UUID, database: AppDatabase, principal: TrustedPrincipal,
                   settings: AppSettings, port: AppManagedPort) -> ManagedEvidenceResult:
    return await operational_evidence(database, principal, turn_id, settings, port)
