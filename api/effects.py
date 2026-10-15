"""Private Conversation endpoints for Business external-effect authority."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from api.dependencies import AppDatabase, TrustedPrincipal
from schemas.effects import (
    ExternalEffectEligibilityRequest,
    ExternalEffectEligibilityResult,
    ExternalEffectPermitRequest,
    ExternalEffectPermitResult,
)
from services import effects

router = APIRouter(prefix="/internal/v1")


@router.post(
    "/external-effect-eligibility",
    response_model=ExternalEffectEligibilityResult,
)
async def check_eligibility(
    request: ExternalEffectEligibilityRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> ExternalEffectEligibilityResult:
    try:
        return await effects.eligibility(database, principal, request)
    except effects.EffectServiceConflict as error:
        return JSONResponse(  # type: ignore[return-value]
            status_code=409,
            content={"detail": {"code": error.code}},
        )


@router.post(
    "/external-effect-permits",
    response_model=ExternalEffectPermitResult,
)
async def acquire_permit(
    request: ExternalEffectPermitRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
) -> ExternalEffectPermitResult:
    try:
        return await effects.permit(database, principal, request)
    except effects.EffectServiceConflict as error:
        detail: dict[str, str] = {"code": error.code}
        if error.reason is not None:
            detail["reason"] = error.reason.value
        return JSONResponse(  # type: ignore[return-value]
            status_code=409,
            content={"detail": detail},
        )
