"""Liveness and migrated-database readiness expose only bounded state."""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from api.dependencies import AppDatabase

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "conversation-service"}


@router.get("/ready")
async def ready(database: AppDatabase) -> JSONResponse:
    try:
        available = await database.ready()
    except Exception:
        available = False
    return JSONResponse(
        status_code=200 if available else 503,
        content={
            "status": "ready" if available else "not_ready",
            "service": "conversation-service",
        },
    )


@router.get("/internal/openapi.json", include_in_schema=False)
async def openapi(request: Request) -> JSONResponse:
    return JSONResponse(request.app.openapi())
