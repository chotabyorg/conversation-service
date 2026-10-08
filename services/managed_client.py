"""The only managed AI network port: bounded, correlated and never retried."""

from __future__ import annotations

import asyncio
import json
import re
from typing import Protocol

import httpx

from core.config import ManagedBinding, Settings
from schemas.contracts import TurnFailureCode
from schemas.managed import MAX_RESULT_BYTES, ManagedTurnResult, PreparedManagedRequest
from schemas.website import ManagedEvidenceRequest, ManagedEvidenceResult
from services.managed_http_owner import NativeHTTPResourceOwner

MANAGED_PATH = "/internal/v1/managed-turns/execute"
# Only an exact provider boundary envelope is affirmative preexecution evidence.
PREEXECUTION_ERRORS: dict[int, tuple[str, str, TurnFailureCode]] = {
    401: ("UNAUTHENTICATED", "Managed authentication required.", TurnFailureCode.PROCESSING_ERROR),
    404: ("AGENT_NOT_FOUND", "Agent not found.", TurnFailureCode.PROCESSING_ERROR),
    413: ("PAYLOAD_TOO_LARGE", "Managed request exceeds limit.", TurnFailureCode.INVALID_RESULT),
    415: (
        "UNSUPPORTED_MEDIA_TYPE",
        "Managed request requires uncompressed JSON.",
        TurnFailureCode.INVALID_RESULT,
    ),
    422: ("INVALID_CONTEXT", "Invalid managed context.", TurnFailureCode.INVALID_RESULT),
}


class ManagedPortFailure(Exception):
    """Closed safe mapping only; no original exception/body/URL is retained."""

    def __init__(self, failure_code: TurnFailureCode = TurnFailureCode.EXECUTION_UNKNOWN) -> None:
        self.failure_code = failure_code
        super().__init__(failure_code.value)


class ManagedAIPort(Protocol):
    async def execute(
        self, prepared: PreparedManagedRequest, binding: ManagedBinding, request_id: str = ""
    ) -> ManagedTurnResult: ...


def unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("invalid JSON")
        value[key] = item
    return value


def reject_constant(value: str) -> object:
    raise ValueError("invalid JSON")


def parse_response(body: bytes, status: int, prepared: PreparedManagedRequest) -> ManagedTurnResult:
    try:
        value = json.loads(
            body, object_pairs_hook=unique_json_object, parse_constant=reject_constant
        )
        if status != 200:
            known = PREEXECUTION_ERRORS.get(status)
            if known is not None:
                code, message, failure = known
                if value == {"detail": {"code": code, "message": message}}:
                    raise ManagedPortFailure(failure)
            raise ManagedPortFailure()
        result = ManagedTurnResult.model_validate(value)
        result.correlate(prepared.request)
        return result
    except ManagedPortFailure:
        raise
    except Exception:
        raise ManagedPortFailure() from None


class ManagedAIClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def execute(
        self, prepared: PreparedManagedRequest, binding: ManagedBinding, request_id: str = ""
    ) -> ManagedTurnResult:
        resources = NativeHTTPResourceOwner()
        owner = asyncio.create_task(
            self._execute_owned(prepared, binding, request_id, resources),
            name="managed-ai-http-owner",
        )
        cancellation = None
        while True:
            try:
                result = await asyncio.shield(owner)
                break
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
                    # Signal work only. Raw Task.cancel can interrupt an already
                    # running native close despite its AnyIO shield.
                    resources.cancel_operation()
                if owner.done():
                    # Retrieve failure without changing the original caller's cancel.
                    try:
                        owner.result()
                    except BaseException:
                        pass
                    raise cancellation
            except BaseException:
                if cancellation is not None:
                    raise cancellation
                raise
        if cancellation is not None:
            raise cancellation
        return result

    async def _execute_owned(
        self, prepared: PreparedManagedRequest, binding: ManagedBinding, request_id: str,
        resources: NativeHTTPResourceOwner,
    ) -> ManagedTurnResult:
        settings = self.settings
        if settings.managed_ai_base_url is None:
            raise ManagedPortFailure()
        headers = {
            "X-Service-Token": binding.service_token.get_secret_value(),
            "Content-Type": "application/json",
        }
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", request_id):
            headers["X-Request-ID"] = request_id
        timeout = httpx.Timeout(
            connect=settings.managed_ai_connect_seconds,
            write=settings.managed_ai_write_seconds,
            read=settings.managed_ai_read_seconds,
            pool=settings.managed_ai_pool_seconds,
        )
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        transport = httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False)
        result = None
        try:
            resources.own_pool(transport._pool)
            with resources.operation(settings.managed_ai_total_seconds) as operation:
                async with httpx.AsyncClient(
                    timeout=timeout,
                    limits=limits,
                    trust_env=False,
                    follow_redirects=False,
                    transport=transport,
                ) as client:
                    async with client.stream(
                        "POST",
                        settings.managed_ai_base_url + MANAGED_PATH,
                        content=prepared.body,
                        headers=headers,
                    ) as response:
                        media = response.headers.get_list("content-type")
                        encoding = response.headers.get_list("content-encoding")
                        if (
                            len(media) != 1
                            or media[0].split(";", 1)[0].strip().lower() != "application/json"
                            or any(v.strip().lower() != "identity" for v in encoding)
                        ):
                            raise ManagedPortFailure()
                        body = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > MAX_RESULT_BYTES:
                                raise ManagedPortFailure()
                            body.extend(chunk)
                        result = parse_response(bytes(body), response.status_code, prepared)
            if operation.cancel_called or result is None:
                raise ManagedPortFailure()
            return result
        except ManagedPortFailure:
            raise
        except Exception:
            raise ManagedPortFailure() from None
        finally:
            # Native TLS can be cancelled before HTTPcore installs a connection,
            # leaving its pool empty. The acquisition owner still holds the socket.
            try:
                await resources.aclose()
            except Exception:
                raise ManagedPortFailure() from None

    async def evidence(self, request: ManagedEvidenceRequest,
                       binding: ManagedBinding) -> ManagedEvidenceResult:
        resources = NativeHTTPResourceOwner()
        owner = asyncio.create_task(self._evidence_owned(request, binding, resources),
                                    name="managed-evidence-http-owner")
        cancellation = None
        while True:
            try:
                result = await asyncio.shield(owner)
                break
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
                resources.cancel_operation()
                if owner.done():
                    try:
                        owner.result()
                    except BaseException:
                        pass
                    raise cancellation
            except BaseException:
                if cancellation is not None:
                    raise cancellation
                raise
        if cancellation is not None:
            raise cancellation
        return result

    async def _evidence_owned(self, request: ManagedEvidenceRequest, binding: ManagedBinding,
                              resources: NativeHTTPResourceOwner) -> ManagedEvidenceResult:
        if self.settings.managed_ai_base_url is None:
            raise ManagedPortFailure()
        transport = httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        resources.own_pool(transport._pool)
        result = None
        try:
            total = min(self.settings.managed_ai_total_seconds, 10.0)
            with resources.operation(total) as operation:
                async with httpx.AsyncClient(transport=transport, trust_env=False,
                                             follow_redirects=False, timeout=10.0) as client:
                    async with client.stream(
                        "POST", self.settings.managed_ai_base_url +
                        "/internal/v1/managed-turns/evidence",
                        content=request.model_dump_json().encode("utf-8"),
                        headers={"Content-Type": "application/json", "X-Service-Token":
                                 binding.service_token.get_secret_value()},
                    ) as response:
                        if (response.status_code != 200 or response.headers.get_list(
                                "content-type") != ["application/json"]
                                or response.headers.get_list("content-encoding")):
                            raise ManagedPortFailure()
                        body = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > 32768:
                                raise ManagedPortFailure()
                            body.extend(chunk)
                        value = json.loads(body, object_pairs_hook=unique_json_object,
                                           parse_constant=reject_constant)
                        result = ManagedEvidenceResult.model_validate(value)
            if operation.cancel_called or result is None:
                raise ManagedPortFailure()
            return result
        except Exception:
            raise ManagedPortFailure() from None
        finally:
            await resources.aclose()
