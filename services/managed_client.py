"""The only managed AI network port: bounded, correlated and never retried."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID, uuid5

import httpx

from core.config import ManagedBinding, Settings
from core.logging import logger
from schemas.contracts import TurnFailureCode
from schemas.managed import MAX_RESULT_BYTES, ManagedTurnResult, PreparedManagedRequest
from schemas.managed_v3 import (
    ControlEvidenceRequest,
    ControlEvidenceResult,
    ManagedTurnV3Request,
    ManagedTurnV3Result,
)
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

    async def execute_v3(
        self,
        prepared: PreparedManagedV3Request,
        binding: ManagedBinding,
        request_id: str = "",
    ) -> ManagedTurnV3Result: ...

    async def control_evidence_v3(
        self,
        request: ControlEvidenceRequest,
        binding: ManagedBinding,
    ) -> ControlEvidenceResult: ...


@dataclass(frozen=True)
class PreparedManagedV3Request:
    request: ManagedTurnV3Request
    body: bytes
    request_fingerprint: str
    sidecar_fingerprint: str
    original_dispatch_id: UUID


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def prepare_v3_request(request: ManagedTurnV3Request) -> PreparedManagedV3Request:
    fields = request.model_dump(mode="json")
    body = _canonical_bytes(fields)
    if len(body) > 1_048_576:
        raise ValueError("managed request exceeds limit")
    direct = hashlib.sha256(body).hexdigest()
    sidecar = hashlib.sha256(
        _canonical_bytes(
            {
                "execution_namespace": "main-chota.conversation.v1",
                "request": fields,
            }
        )
    ).hexdigest()
    namespace = UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
    dispatch_name = _canonical_bytes(
        [
            "main-chota.managed-execution.v3",
            request.owner_id,
            str(request.integration_id),
            request.channel,
            str(request.conversation_id),
            str(request.turn_id),
            direct,
        ]
    ).decode("utf-8")
    dispatch_id = uuid5(namespace, dispatch_name)
    return PreparedManagedV3Request(request, body, direct, sidecar, dispatch_id)


def handoff_request_id(request: ManagedTurnV3Request) -> UUID:
    namespace = UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
    name = _canonical_bytes(
        [
            "main-chota.handoff.v1",
            request.owner_id,
            str(request.integration_id),
            request.channel,
            str(request.conversation_id),
            str(request.turn_id),
            0,
        ]
    ).decode("utf-8")
    return uuid5(namespace, name)


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
                    if status in {401, 404}:
                        logger.warning(
                            "",
                            extra={
                                "stage": "turn",
                                "turn_id": prepared.request.turn_id,
                                "code": f"MANAGED_PREEXECUTION_{code}",
                                "status_code": status,
                            },
                        )
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

    async def execute_v3(
        self,
        prepared: PreparedManagedV3Request,
        binding: ManagedBinding,
        request_id: str = "",
    ) -> ManagedTurnV3Result:
        resources = NativeHTTPResourceOwner()
        owner = asyncio.create_task(
            self._execute_v3_owned(prepared, binding, request_id, resources),
            name="managed-ai-v3-http-owner",
        )
        cancellation = None
        while True:
            try:
                result = await asyncio.shield(owner)
                break
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
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

    async def _execute_v3_owned(
        self,
        prepared: PreparedManagedV3Request,
        binding: ManagedBinding,
        request_id: str,
        resources: NativeHTTPResourceOwner,
    ) -> ManagedTurnV3Result:
        if self.settings.managed_ai_base_url is None:
            raise ManagedPortFailure()
        headers = {
            "X-Service-Token": binding.service_token.get_secret_value(),
            "Content-Type": "application/json",
        }
        if re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", request_id):
            headers["X-Request-ID"] = request_id
        timeout = httpx.Timeout(
            connect=self.settings.managed_ai_connect_seconds,
            write=self.settings.managed_ai_write_seconds,
            read=self.settings.managed_ai_read_seconds,
            pool=self.settings.managed_ai_pool_seconds,
        )
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
        transport = httpx.AsyncHTTPTransport(retries=0, limits=limits, trust_env=False)
        result = None
        try:
            resources.own_pool(transport._pool)
            with resources.operation(self.settings.managed_ai_total_seconds) as operation:
                async with httpx.AsyncClient(
                    timeout=timeout,
                    limits=limits,
                    trust_env=False,
                    follow_redirects=False,
                    transport=transport,
                ) as client:
                    async with client.stream(
                        "POST",
                        self.settings.managed_ai_base_url + "/internal/v3/managed-turns/execute",
                        content=prepared.body,
                        headers=headers,
                    ) as response:
                        media = response.headers.get_list("content-type")
                        encoding = response.headers.get_list("content-encoding")
                        if (
                            response.status_code != 200
                            or len(media) != 1
                            or media[0].split(";", 1)[0].strip().lower() != "application/json"
                            or any(value.strip().lower() != "identity" for value in encoding)
                        ):
                            raise ManagedPortFailure()
                        body = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > 262_144:
                                raise ManagedPortFailure()
                            body.extend(chunk)
                        value = json.loads(
                            body,
                            object_pairs_hook=unique_json_object,
                            parse_constant=reject_constant,
                        )
                        result = ManagedTurnV3Result.model_validate(value)
                        result.correlate(
                            prepared.request,
                            prepared.original_dispatch_id,
                            prepared.request_fingerprint,
                        )
            if operation.cancel_called or result is None:
                raise ManagedPortFailure()
            return result
        except ManagedPortFailure:
            raise
        except Exception:
            raise ManagedPortFailure() from None
        finally:
            try:
                await resources.aclose()
            except Exception:
                raise ManagedPortFailure() from None

    async def control_evidence_v3(
        self,
        request: ControlEvidenceRequest,
        binding: ManagedBinding,
    ) -> ControlEvidenceResult:
        if self.settings.managed_ai_base_url is None:
            raise ManagedPortFailure()
        body = _canonical_bytes(request.model_dump(mode="json"))
        transport = httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        resources = NativeHTTPResourceOwner()
        resources.own_pool(transport._pool)
        try:
            with resources.operation(
                min(self.settings.managed_ai_total_seconds, 10.0)
            ) as operation:
                async with httpx.AsyncClient(
                    transport=transport,
                    trust_env=False,
                    follow_redirects=False,
                    timeout=httpx.Timeout(connect=2, read=5, write=2, pool=2),
                ) as client:
                    async with client.stream(
                        "POST",
                        self.settings.managed_ai_base_url
                        + "/internal/v3/managed-turns/control-evidence",
                        content=body,
                        headers={
                            "Content-Type": "application/json",
                            "X-Service-Token": binding.service_token.get_secret_value(),
                        },
                    ) as response:
                        media = response.headers.get_list("content-type")
                        encoding = response.headers.get_list("content-encoding")
                        if (
                            response.status_code != 200
                            or len(media) != 1
                            or media[0].split(";", 1)[0].strip().lower() != "application/json"
                            or any(value.strip().lower() != "identity" for value in encoding)
                        ):
                            raise ManagedPortFailure()
                        payload = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(payload) + len(chunk) > 32_768:
                                raise ManagedPortFailure()
                            payload.extend(chunk)
                        value = json.loads(
                            payload,
                            object_pairs_hook=unique_json_object,
                            parse_constant=reject_constant,
                        )
                        result = ControlEvidenceResult.model_validate(value)
                        result.correlate(request)
            if operation.cancel_called:
                raise ManagedPortFailure()
            return result
        except ManagedPortFailure:
            raise
        except Exception:
            raise ManagedPortFailure() from None
        finally:
            try:
                await resources.aclose()
            except Exception:
                raise ManagedPortFailure() from None

    async def _execute_owned(
        self,
        prepared: PreparedManagedRequest,
        binding: ManagedBinding,
        request_id: str,
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

    async def evidence(
        self, request: ManagedEvidenceRequest, binding: ManagedBinding
    ) -> ManagedEvidenceResult:
        resources = NativeHTTPResourceOwner()
        owner = asyncio.create_task(
            self._evidence_owned(request, binding, resources), name="managed-evidence-http-owner"
        )
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

    async def _evidence_owned(
        self,
        request: ManagedEvidenceRequest,
        binding: ManagedBinding,
        resources: NativeHTTPResourceOwner,
    ) -> ManagedEvidenceResult:
        if self.settings.managed_ai_base_url is None:
            raise ManagedPortFailure()
        transport = httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        resources.own_pool(transport._pool)
        result = None
        try:
            total = min(self.settings.managed_ai_total_seconds, 10.0)
            with resources.operation(total) as operation:
                async with httpx.AsyncClient(
                    transport=transport, trust_env=False, follow_redirects=False, timeout=10.0
                ) as client:
                    async with client.stream(
                        "POST",
                        self.settings.managed_ai_base_url + "/internal/v1/managed-turns/evidence",
                        content=request.model_dump_json().encode("utf-8"),
                        headers={
                            "Content-Type": "application/json",
                            "X-Service-Token": binding.service_token.get_secret_value(),
                        },
                    ) as response:
                        if (
                            response.status_code != 200
                            or response.headers.get_list("content-type") != ["application/json"]
                            or response.headers.get_list("content-encoding")
                        ):
                            raise ManagedPortFailure()
                        body = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(body) + len(chunk) > 32768:
                                raise ManagedPortFailure()
                            body.extend(chunk)
                        value = json.loads(
                            body,
                            object_pairs_hook=unique_json_object,
                            parse_constant=reject_constant,
                        )
                        result = ManagedEvidenceResult.model_validate(value)
            if operation.cancel_called or result is None:
                raise ManagedPortFailure()
            return result
        except Exception:
            raise ManagedPortFailure() from None
        finally:
            await resources.aclose()
