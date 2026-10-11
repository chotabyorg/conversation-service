import json
import logging
from typing import get_args
from uuid import UUID, uuid4

import pytest

from core.logging import SafeJsonFormatter, safe_handle
from schemas.managed import ManagedErrorCode
from tests.test_managed_client import fake_http, install_client, success
from tests.test_managed_execution import execute, response_for
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture
SENTINEL = "private-message-token-prompt-exception-sentinel"


def diagnostics(caplog):
    return [record for record in caplog.records
            if getattr(record, "code", "").startswith("MANAGED_")]


def assert_private(caplog, *secrets):
    for record in caplog.records:
        assert record.msg == "" and record.args == ()
        assert record.exc_info is None and record.exc_text is None
        assert record.stack_info is None
        for secret in (SENTINEL, *secrets):
            assert secret not in repr(record.__dict__)
            assert secret not in SafeJsonFormatter().format(record)


@pytest.mark.parametrize("status", ["FAILED", "UNKNOWN"])
@pytest.mark.parametrize("code", get_args(ManagedErrorCode))
async def test_validated_closed_diagnostic_preserves_mapping_and_replay(
    h, managed, caplog, status, code
):
    settings, port = managed
    port.status, port.error_code = status, code
    turn, _ = await h.select(await h.ingest(text=SENTINEL))
    claim = await h.claim(turn)
    result = await execute(h, turn, claim)
    expected = ("EXECUTION_UNKNOWN" if status == "UNKNOWN" else
                "TIMEOUT" if code in {"MODEL_TIMEOUT", "EXECUTION_TIMEOUT"} else
                "INVALID_RESULT" if code in {"CONTEXT_LIMIT", "INVALID_RESULT"} else
                "PROCESSING_ERROR")
    assert result.status_code == 200
    assert result.json()["turn"]["failure_code"] == expected
    records = diagnostics(caplog)
    assert len(records) == 1
    assert records[0].code == f"MANAGED_{status}_{code}"
    assert records[0].turn_id == UUID(turn["id"]) and records[0].stage == "turn"
    assert json.loads(SafeJsonFormatter().format(records[0]))["code"] == records[0].code
    repeat = await execute(h, turn, claim)
    assert repeat.json()["replayed"] and len(port.calls) == 1
    assert len(diagnostics(caplog)) == 1
    assert_private(caplog, settings.managed_ai_bindings[0].service_token.get_secret_value())


@pytest.mark.parametrize("status,code,message", [
    (401, "UNAUTHENTICATED", "Managed authentication required."),
    (404, "AGENT_NOT_FOUND", "Agent not found."),
])
async def test_exact_preexecution_diagnostic_never_retries(
    h, managed, network_allowlist, caplog, status, code, message
):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest(text=SENTINEL))
    claim = await h.claim(turn)
    payload = {"detail": {"code": code, "message": message}}
    async with fake_http(network_allowlist, "normal", status, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        result = await execute(h, turn, claim)
        assert result.json()["turn"]["failure_code"] == "PROCESSING_ERROR"
        assert (await execute(h, turn, claim)).json()["replayed"]
        assert len(calls) == 1
    records = diagnostics(caplog)
    assert len(records) == 1
    assert records[0].code == f"MANAGED_PREEXECUTION_{code}"
    assert records[0].status_code == status and records[0].turn_id == UUID(turn["id"])
    assert_private(caplog, settings.managed_ai_bindings[0].service_token.get_secret_value())


@pytest.mark.parametrize("mode", ["malformed", "correlation", "code", "spoof401", "spoof404"])
async def test_untrusted_result_emits_no_diagnostic(h, managed, network_allowlist, caplog, mode):
    settings, _ = managed
    turn, _ = await h.select(await h.ingest(text=SENTINEL))
    claim = await h.claim(turn)

    def payload(request):
        value = success(request) | dict(status="FAILED", draft_text=None, error_code="MODEL_ERROR")
        if mode == "malformed":
            return ('{"private":"' + SENTINEL).encode()
        if mode == "correlation":
            value["turn_id"] = str(uuid4())
        if mode == "code":
            value["error_code"] = SENTINEL
        if mode.startswith("spoof"):
            return {"detail": {"code": "UNAUTHENTICATED" if mode == "spoof401"
                               else "AGENT_NOT_FOUND", "message": SENTINEL}}
        return value

    status = 401 if mode == "spoof401" else 404 if mode == "spoof404" else 200
    async with fake_http(network_allowlist, "normal", status, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        result = await execute(h, turn, claim)
        assert result.json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
        assert (await execute(h, turn, claim)).json()["replayed"] and len(calls) == 1
    assert not diagnostics(caplog)
    assert_private(caplog, settings.managed_ai_bindings[0].service_token.get_secret_value())


async def test_arbitrary_exception_emits_no_diagnostic(h, managed, caplog):
    _, port = managed
    port.failure = RuntimeError(SENTINEL)
    turn, _ = await h.select(await h.ingest(text=SENTINEL))
    claim = await h.claim(turn)
    assert (await execute(h, turn, claim)).json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
    assert not diagnostics(caplog)
    assert_private(caplog)


@pytest.mark.parametrize("invalid", ["code", "correlation"])
async def test_terminal_boundary_revalidates_before_logging(h, managed, caplog, invalid):
    _, port = managed

    async def untrusted_result(prepared, binding, request_id=""):
        port.calls.append((prepared, binding))
        result = response_for(prepared, "FAILED", "MODEL_ERROR")
        return result.model_copy(update={"error_code": SENTINEL} if invalid == "code"
                                 else {"turn_id": uuid4()})

    port.execute = untrusted_result
    turn, _ = await h.select(await h.ingest(text=SENTINEL))
    claim = await h.claim(turn)
    assert (await execute(h, turn, claim)).json()["turn"]["failure_code"] == "EXECUTION_UNKNOWN"
    assert (await execute(h, turn, claim)).json()["replayed"] and len(port.calls) == 1
    assert not diagnostics(caplog)
    assert_private(caplog)


@pytest.mark.parametrize("code", [
    "MANAGED_FAILED_MODEL_ERROR_" + SENTINEL,
    "MANAGED_UNKNOWN_" + SENTINEL,
    "MANAGED_PREEXECUTION_AGENT_NOT_FOUND_" + SENTINEL,
])
def test_matching_prefix_does_not_expand_logging_whitelist(code):
    record = logging.makeLogRecord(dict(
        name="conversation", levelname="WARNING", levelno=logging.WARNING,
        msg=SENTINEL, args=(SENTINEL,), code=code, stage="turn", turn_id=SENTINEL,
        private=SENTINEL, exc_info=(RuntimeError, RuntimeError(SENTINEL), None),
        exc_text=SENTINEL, stack_info=SENTINEL,
    ))
    assert "code" not in json.loads(SafeJsonFormatter().format(record))

    class Sink(logging.Handler):
        def emit(self, received):
            self.received = received

    sink = Sink()
    safe_handle(sink, record)
    assert sink.received is record
    assert record.msg == "" and record.args == () and record.exc_info is None
    assert "code" not in record.__dict__ and "turn_id" not in record.__dict__
    assert "private" not in record.__dict__
    assert SENTINEL not in repr(record.__dict__)
    assert SENTINEL not in sink.format(record)
