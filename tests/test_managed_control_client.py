"""The control-evidence reader retains its HTTP owner through caller cancellation."""

import asyncio
from uuid import uuid4

import pytest

from schemas.managed_v3 import ControlEvidenceRequest
from services.managed_client import ManagedAIClient
from services.managed_http_owner import NativeHTTPResourceOwner
from tests.test_managed_client import fake_http
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture


async def test_control_reader_joins_native_close_after_repeated_cancellation(
    managed, network_allowlist, monkeypatch
):
    settings, _ = managed
    binding = settings.managed_ai_bindings[0]
    request = ControlEvidenceRequest(
        contract_version=3,
        owner_id=binding.owner_id,
        integration_id=binding.integration_id,
        channel="TELEGRAM",
        agent_id=binding.agent_id,
        conversation_id=uuid4(),
        turn_id=uuid4(),
        input_revision=1,
        authority_revision=1,
        input_first_sequence=1,
        input_last_sequence=1,
        context_last_sequence=1,
        request_fingerprint="a" * 64,
        original_dispatch_id=uuid4(),
    )
    closed, release = asyncio.Event(), asyncio.Event()
    owners = []
    original_close = NativeHTTPResourceOwner.aclose

    async def observe_close(owner):
        owners.append(asyncio.current_task())
        await original_close(owner)
        closed.set()
        await release.wait()

    monkeypatch.setattr(NativeHTTPResourceOwner, "aclose", observe_close)
    async with fake_http(network_allowlist, "timeout") as (origin, calls, received):
        client = ManagedAIClient(settings.model_copy(update={"managed_ai_base_url": origin}))
        caller = asyncio.create_task(client.control_evidence_v3(request, binding))
        try:
            await asyncio.wait_for(received.wait(), 2)
            caller.cancel("original control cancellation")
            await asyncio.wait_for(closed.wait(), 2)
            caller.cancel("repeated control cancellation")
            await asyncio.sleep(0)
            assert not caller.done()
        finally:
            release.set()
            if not closed.is_set():
                caller.cancel("test cleanup")
            with pytest.raises(asyncio.CancelledError) as cancelled:
                await caller
        assert cancelled.value.args == ("original control cancellation",)
        assert len(calls) == len(owners) == 1
        assert calls[0][0] == "POST /internal/v3/managed-turns/control-evidence HTTP/1.1"
        assert owners[0].done()
        assert owners[0].get_name() == "managed-control-evidence-http-owner"
