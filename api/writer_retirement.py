"""Internal supervisor-only writer retirement; no owner/browser ingress."""
from fastapi import APIRouter

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from schemas.writer_retirement import WriterRetireCommand, WriterRetired
from services.writer_retirement import retire_writer

router = APIRouter(prefix="/internal/v1/outbound-writers")


@router.post("/retire", response_model=WriterRetired)
async def retire_route(request: WriterRetireCommand, database: AppDatabase,
                       principal: TrustedPrincipal, settings: AppSettings) -> WriterRetired:
    return await retire_writer(database, principal, request, settings)
