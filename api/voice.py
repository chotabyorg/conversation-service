"""Private Voice endpoints; scope and agent identity come from trusted configuration."""

from fastapi import APIRouter, Request

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from schemas.voice import (
    CallEnd,
    CallIdentity,
    CallResult,
    CallStart,
    Playback,
    PlaybackResult,
    ResponseBegin,
    ResponseCandidate,
    ResponsePermission,
    Utterance,
    UtteranceResult,
)
from services import voice

router = APIRouter(prefix="/internal/v1/voice")


@router.post("/calls/start", response_model=CallResult)
async def start(request: CallStart, database: AppDatabase, principal: TrustedPrincipal,
                settings: AppSettings, http_request: Request) -> CallResult:
    result = await voice.start(database, principal, request, settings)
    if result.state == "ACTIVE" and not result.replayed:
        http_request.app.state.voice_warmups.start(request, principal)
    return result


@router.post("/calls/heartbeat", response_model=CallResult)
async def heartbeat(request: CallIdentity, database: AppDatabase, principal: TrustedPrincipal,
                    settings: AppSettings) -> CallResult:
    return await voice.heartbeat(database, principal, request, settings)


@router.post("/calls/end", response_model=CallResult)
async def end(request: CallEnd, database: AppDatabase, principal: TrustedPrincipal,
              settings: AppSettings, http_request: Request) -> CallResult:
    result = await voice.end(database, principal, request, settings)
    http_request.app.state.voice_warmups.stop(request.call_id)
    return result


@router.post("/utterances", response_model=UtteranceResult)
async def utterance(request: Utterance, database: AppDatabase, principal: TrustedPrincipal,
                    settings: AppSettings, http_request: Request) -> UtteranceResult:
    result = await voice.admit(database, principal, request, settings)
    http_request.app.state.voice_warmups.stop(request.call_id)
    return result


@router.post("/responses/next", response_model=ResponseCandidate)
async def next_response(request: CallIdentity, database: AppDatabase, principal: TrustedPrincipal,
                        settings: AppSettings) -> ResponseCandidate:
    return await voice.next_response(database, principal, request, settings)


@router.post("/responses/begin", response_model=ResponsePermission)
async def begin(request: ResponseBegin, database: AppDatabase, principal: TrustedPrincipal,
                settings: AppSettings) -> ResponsePermission:
    return await voice.begin(database, principal, request, settings)


@router.post("/playback", response_model=PlaybackResult)
async def playback(request: Playback, database: AppDatabase, principal: TrustedPrincipal,
                   settings: AppSettings) -> PlaybackResult:
    return await voice.playback(database, principal, request, settings)
