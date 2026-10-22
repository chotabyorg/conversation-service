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
from schemas.website_delivery import (
    WebsiteDeliveryBegin,
    WebsiteDeliveryBegun,
    WebsiteDeliveryComplete,
    WebsiteDeliveryCompleted,
    WebsiteDeliveryWorkResult,
)
from schemas.website_session import (
    WebsiteSessionAccess,
    WebsiteSessionAdmission,
    WebsiteSessionCreated,
    WebsiteSessionScope,
)
from services import website, website_delivery, website_ingress
from services.website_evidence import operational_evidence
from services.website_session import create as create_session

router = APIRouter(prefix="/internal/v1/website")


@router.post("/sessions", response_model=WebsiteSessionCreated, status_code=201)
async def session_create(
    request: WebsiteSessionScope, database: AppDatabase,
    principal: TrustedPrincipal, settings: AppSettings,
) -> WebsiteSessionCreated:
    return await create_session(database, principal, request, settings)


@router.post("/admissions", response_model=WebsiteAdmissionResult)
async def admit(request: WebsiteAdmission, database: AppDatabase,
                principal: TrustedPrincipal, settings: AppSettings) -> WebsiteAdmissionResult:
    return await website.admit(database, principal, request, settings)


@router.post("/sessions/admissions", response_model=WebsiteAdmissionResult)
async def session_admit(
    request: WebsiteSessionAdmission, database: AppDatabase,
    principal: TrustedPrincipal, settings: AppSettings,
) -> WebsiteAdmissionResult:
    return await website_ingress.admit(database, principal, request, settings)


@router.post("/sessions/delivery/work", response_model=WebsiteDeliveryWorkResult)
async def session_delivery_work(
    request: WebsiteSessionAccess, database: AppDatabase,
    principal: TrustedPrincipal, settings: AppSettings,
) -> WebsiteDeliveryWorkResult:
    return await website_delivery.work(database, principal, request, settings)


@router.post("/sessions/delivery/begin", response_model=WebsiteDeliveryBegun)
async def session_delivery_begin(
    request: WebsiteDeliveryBegin, database: AppDatabase,
    principal: TrustedPrincipal, settings: AppSettings,
) -> WebsiteDeliveryBegun:
    return await website_delivery.begin(database, principal, request, settings)


@router.post("/sessions/delivery/complete", response_model=WebsiteDeliveryCompleted)
async def session_delivery_complete(
    request: WebsiteDeliveryComplete, database: AppDatabase, principal: TrustedPrincipal,
) -> WebsiteDeliveryCompleted:
    return await website_delivery.complete(database, principal, request)


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
