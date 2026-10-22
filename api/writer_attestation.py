"""Dedicated narrow lifecycle producer, proof reader and recovery ports."""

from fastapi import APIRouter

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from schemas.writer_attestation import (
    AttestationReceipt,
    RecoveryRequest,
    RecoveryResult,
    WriterRegistration,
    WriterWitness,
)
from schemas.writer_retirement import (
    RetireCommand,
    WebsiteWriterTerminationProof,
    WriterTerminationProof,
)
from services import writer_attestation

router = APIRouter()


@router.post("/internal/v1/writer-attestations/register", response_model=AttestationReceipt)
async def register(
    request: WriterRegistration,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> AttestationReceipt:
    return await writer_attestation.register(database, principal, request, settings)


@router.post("/internal/v1/writer-attestations/witness", response_model=AttestationReceipt)
async def witness(
    request: WriterWitness,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> AttestationReceipt:
    return await writer_attestation.witness(database, principal, request, settings)


@router.post(
    "/internal/v1/writer-termination/proof",
    response_model=WriterTerminationProof | WebsiteWriterTerminationProof,
)
async def proof(
    request: RetireCommand,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> WriterTerminationProof | WebsiteWriterTerminationProof:
    return await writer_attestation.proof(database, request, settings)


@router.post("/internal/v1/outbound-writers/recovery", response_model=RecoveryResult)
async def recovery(
    request: RecoveryRequest,
    database: AppDatabase,
    principal: TrustedPrincipal,
    settings: AppSettings,
) -> RecoveryResult:
    return await writer_attestation.recovery(database, request.registration_id, settings)
