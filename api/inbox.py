"""Browser-only owner Inbox routes backed by the live User session."""

import re
from uuid import UUID

from fastapi import APIRouter, Request

from api.dependencies import AppDatabase, TrustedHuman
from core.errors import DomainError
from schemas.contracts import ErrorCode
from schemas.handoff import (
    CaseDetail,
    CasePage,
    ClaimCommand,
    InboxCommandResult,
    ReturnToAgentCommand,
    SendHumanCommand,
)
from services import inbox

router = APIRouter(prefix="/public/v1/inbox")


def _single_query(request: Request, name: str, default: str | None = None) -> str | None:
    values = request.query_params.getlist(name)
    if not values:
        return default
    if len(values) != 1:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    return values[0]


def _strict_int(value: str | None, default: int, lower: int, upper: int) -> int:
    if value is None:
        return default
    if re.fullmatch(r"(?:0|[1-9][0-9]{0,18})", value) is None:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    result = int(value)
    if not lower <= result <= upper:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    return result


@router.get("/cases", response_model=CasePage)
async def list_cases_route(
    request: Request,
    database: AppDatabase,
    principal: TrustedHuman,
) -> CasePage:
    limit = _strict_int(_single_query(request, "limit"), 20, 1, 50)
    cursor = _single_query(request, "cursor")
    return await inbox.list_cases(database, principal, limit, cursor)


@router.get("/cases/{case_id}", response_model=CaseDetail)
async def case_detail_route(
    case_id: UUID,
    request: Request,
    database: AppDatabase,
    principal: TrustedHuman,
) -> CaseDetail:
    after_sequence = _strict_int(
        _single_query(request, "after_sequence"), 0, 0, 9_223_372_036_854_775_807
    )
    limit = _strict_int(_single_query(request, "limit"), 50, 1, 100)
    return await inbox.case_detail(database, principal, case_id, after_sequence, limit)


@router.post("/cases/{case_id}/claim", response_model=InboxCommandResult)
async def claim_route(
    case_id: UUID,
    command: ClaimCommand,
    database: AppDatabase,
    principal: TrustedHuman,
) -> InboxCommandResult:
    return await inbox.claim_case(database, principal, case_id, command)


@router.post("/cases/{case_id}/messages", response_model=InboxCommandResult)
async def send_human_route(
    case_id: UUID,
    command: SendHumanCommand,
    database: AppDatabase,
    principal: TrustedHuman,
) -> InboxCommandResult:
    return await inbox.send_human(database, principal, case_id, command)


@router.post("/cases/{case_id}/return-to-agent", response_model=InboxCommandResult)
async def return_to_agent_route(
    case_id: UUID,
    command: ReturnToAgentCommand,
    database: AppDatabase,
    principal: TrustedHuman,
) -> InboxCommandResult:
    return await inbox.return_to_agent(database, principal, case_id, command)
