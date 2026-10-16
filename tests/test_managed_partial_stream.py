"""Opt-in partial drafts over server-sent events; buffered path unchanged."""

import json
import secrets

import pytest
from pydantic import ValidationError

from core.config import ManagedBinding
from schemas.managed import (
    ECHO_FIELDS,
    ManagedTurnRequest,
    ManagedTurnResult,
    prepare_request,
)
from services.managed_client import ManagedAIClient, ManagedPortFailure, parse_sse_event
from tests.test_managed_client import fake_http, install_client
from tests.test_managed_contracts import wire_request
from tests.test_managed_execution import managed as managed_fixture

managed = managed_fixture


def sse_bytes(*events):
    lines = [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]
    return ("\n\n".join(lines) + "\n\n").encode()


def final_event(request, **overrides):
    value = dict(
        {name: request[name] for name in ECHO_FIELDS},
        status="DRAFT_READY",
        draft_text="HTTP draft",
        error_code=None,
        operation_refs=[],
    )
    value.update(overrides)
    return value


def test_parse_sse_event_ignores_blanks_and_control():
    assert parse_sse_event("") is None
    assert parse_sse_event("event: partial") is None
    assert parse_sse_event("data: [DONE]") is None
    assert parse_sse_event("data: ") is None


def test_parse_sse_event_rejects_malformed_payloads():
    with pytest.raises(ManagedPortFailure):
        parse_sse_event("data: {broken")
    with pytest.raises(ManagedPortFailure):
        parse_sse_event("data: [1, 2]")
    with pytest.raises(ManagedPortFailure):
        parse_sse_event('data: {"a": 1, "a": 2}')


def test_streaming_request_contract_defaults_and_version_gate():
    value = wire_request()
    assert ManagedTurnRequest.model_validate(value).stream_partials is False
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate({**value, "stream_partials": True})
    streamed = ManagedTurnRequest.model_validate(
        {**value, "contract_version": 2, "stream_partials": True, "max_partial_chars": 50}
    )
    assert streamed.stream_partials is True
    with pytest.raises(ValidationError):
        ManagedTurnRequest.model_validate(
            {**value, "stream_partials": True, "max_partial_chars": 0}
        )
    result = ManagedTurnResult.model_validate(
        dict(
            contract_version=2,
            conversation_id=streamed.conversation_id,
            turn_id=streamed.turn_id,
            agent_id=streamed.agent_id,
            input_revision=1,
            authority_revision=0,
            input_first_sequence=1,
            input_last_sequence=1,
            context_last_sequence=1,
            status="DRAFT_READY",
            draft_text="full answer",
            error_code=None,
            operation_refs=[],
            partial_text="first sentence. ",
            partial_final=False,
        )
    )
    assert result.partial_text == "first sentence. "


def streaming_request():
    value = wire_request()
    value.update(contract_version=2, stream_partials=True, max_partial_chars=500)
    return ManagedTurnRequest.model_validate(value)


async def test_event_stream_delivers_partials_then_authoritative_final(
    h, managed, network_allowlist
):
    settings, _ = managed
    request = streaming_request()

    def payload(body):
        assert body["stream_partials"] is True
        return sse_bytes(
            {"partial_text": "Hello. ", "partial_final": False},
            {"partial_text": "How can I help?", "partial_final": False},
            {**final_event(body), "partial_text": None, "partial_final": True},
        )

    async with fake_http(network_allowlist, "event-stream", 200, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        client = h.client._transport.app.state.managed_port
        assert isinstance(client, ManagedAIClient)
        partials = []

        async def collect(text):
            partials.append(text)

        binding = ManagedBinding(
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            service_token=secrets.token_urlsafe(32),
        )
        result = await client.execute(prepare_request(request), binding, on_partial=collect)
    assert partials == ["Hello. ", "How can I help?"]
    assert result.status == "DRAFT_READY" and result.draft_text == "HTTP draft"
    assert result.partial_final is True
    assert len(calls) == 1


async def test_event_stream_without_callback_fails_explicitly(h, managed, network_allowlist):
    settings, _ = managed
    request = streaming_request()

    def payload(body):
        return sse_bytes({"partial_text": "Hello. ", "partial_final": False})

    async with fake_http(network_allowlist, "event-stream", 200, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        client = h.client._transport.app.state.managed_port
        binding = ManagedBinding(
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            service_token=secrets.token_urlsafe(32),
        )
        with pytest.raises(ManagedPortFailure):
            await client.execute(prepare_request(request), binding)
    assert len(calls) == 1


async def test_buffered_answer_with_callback_stays_single_result(h, managed, network_allowlist):
    settings, _ = managed
    request = streaming_request()

    def payload(body):
        return dict(
            {name: body[name] for name in ECHO_FIELDS},
            status="DRAFT_READY",
            draft_text="HTTP draft",
            error_code=None,
            operation_refs=[],
        )

    async with fake_http(network_allowlist, "success", 200, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        client = h.client._transport.app.state.managed_port
        binding = ManagedBinding(
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            service_token=secrets.token_urlsafe(32),
        )
        partials = []

        async def collect(text):
            partials.append(text)

        result = await client.execute(prepare_request(request), binding, on_partial=collect)
    assert partials == []
    assert result.status == "DRAFT_READY" and result.draft_text == "HTTP draft"
    assert len(calls) == 1


@pytest.mark.parametrize("mode", ["event-stream-truncated", "event-stream-malformed"])
async def test_broken_partial_stream_fails_without_silent_result(
    h, managed, network_allowlist, mode
):
    settings, _ = managed
    request = streaming_request()

    def payload(body):
        if mode == "event-stream-malformed":
            return b'data: {"partial_text": "Hello. "\n\ndata: {broken\n\n'
        return sse_bytes({"partial_text": "Hello. ", "partial_final": False})

    async with fake_http(network_allowlist, mode, 200, payload) as (origin, calls, _):
        install_client(h, settings, origin)
        client = h.client._transport.app.state.managed_port
        binding = ManagedBinding(
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            service_token=secrets.token_urlsafe(32),
        )
        seen = []

        async def collect(text):
            seen.append(text)

        with pytest.raises(ManagedPortFailure):
            await client.execute(prepare_request(request), binding, on_partial=collect)
    assert len(calls) == 1
