import json
import logging
import socket
from uuid import uuid4

import pytest
from asgi_lifespan import LifespanManager
from pydantic import ValidationError
from sqlalchemy import text

from tests.conftest import PREFIX, error


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [("X-Service-Token", "")],
        [("X-Service-Token", "wrong")],
        [("X-Service-Token", "x" * 257)],
        [("X-Service-Token", "first"), ("X-Service-Token", "second")],
    ],
)
async def test_invalid_credentials_precede_malformed_and_oversized_body(h, headers, sql):
    for body in [b"{malformed-secret-body", b"x" * 70000]:
        response = await h.client.post(PREFIX + "/inbound-events", content=body, headers=headers)
        error(response, "UNAUTHENTICATED", 401)
        assert "secret-body" not in response.text and "first" not in response.text
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM conversations")) == 0


async def test_duplicate_valid_tokens_still_rejected_and_user_headers_do_not_authorize(h):
    headers = [("X-Service-Token", h.tokens["a"])] * 2
    error(
        await h.client.post(PREFIX + "/inbound-events", json=h.event(), headers=headers),
        "UNAUTHENTICATED",
        401,
    )
    headers = dict(h.headers(), **{"X-User-Id": "owner-b", "X-User-Role": "admin"})
    error(
        await h.client.post(
            PREFIX + "/inbound-events", json=h.event(owner_id="owner-b"), headers=headers
        ),
        "NOT_FOUND",
        404,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"contract_version": True},
        {"contract_version": 2},
        {"integration_id": "bad-uuid"},
        {"owner_id": "*"},
        {"owner_id": " "},
        {"owner_id": "x" * 256},
        {"owner_id": "x\n"},
        {"provider_thread_id": "😀" * 129},
        {"provider_event_id": "x" * 513},
        {"provider_namespace": "Account"},
        {"channel": " telegram"},
        {"channel": "unknown"},
        {"text": ""},
        {"text": "x" * 16385},
        {"text": "😀" * 8193},
        {"text": "nul\x00"},
        {"text": "\ud800"},
        {"provider_occurred_at": 123},
        {"provider_occurred_at": "2026-10-06"},
        {"provider_occurred_at": "2026-10-06T12:00:00"},
        {"provider_occurred_at": "2026-10-06T12:00:00.1234567Z"},
        {"provider_occurred_at": "2026-10-06T12:00:00+03:60"},
        {"fingerprint": "caller-must-not-control-hash"},
        {"metadata": {"secret": "private"}},
        {"reasoning": "hidden reasoning"},
        {"credentials": "password"},
    ],
)
async def test_strict_payload_validation_and_safe_error(h, changes):
    payload = h.event(**changes)
    # Explicit serialization can represent an invalid surrogate as escaped JSON.
    escaped = changes.get("text") == "\ud800"
    response = await h.client.post(
        PREFIX + "/inbound-events",
        content=json.dumps(payload, ensure_ascii=escaped).encode(),
        headers=h.headers(),
    )
    error(response, "VALIDATION_ERROR", 422)
    serialized = response.text
    assert "hidden reasoning" not in serialized and "caller-must-not-control-hash" not in serialized
    assert "input" not in response.json()["detail"] and "ctx" not in serialized


async def test_maximum_text_byte_boundary_and_preserved_whitespace(h):
    result = await h.ingest(text="😀" * 8192)
    assert len(result["message"]["text"].encode()) == 32768
    result = await h.ingest(text=" " * 16384)
    assert result["message"]["text"] == " " * 16384


@pytest.mark.parametrize("false_length", [None, "1"])
async def test_actual_streamed_byte_cap_false_or_absent_content_length(h, false_length, sql):
    async def chunks():
        for _ in range(8):
            yield b"sensitive-stream-marker" + b"x" * 9000

    headers = h.headers()
    if false_length:
        headers["Content-Length"] = false_length
    response = await h.client.post(PREFIX + "/inbound-events", content=chunks(), headers=headers)
    error(response, "PAYLOAD_TOO_LARGE", 413)
    assert "sensitive-stream-marker" not in response.text
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 0


async def test_valid_multichunk_body_without_length_and_malformed_json(h):
    body = json.dumps(h.event(text="split multibyte é"), ensure_ascii=False).encode()

    async def chunks():
        for index in range(0, len(body), 3):
            yield body[index : index + 3]

    response = await h.client.post(
        PREFIX + "/inbound-events", content=chunks(), headers=h.headers()
    )
    assert response.status_code == 201 and response.json()["message"]["text"] == "split multibyte é"
    error(
        await h.client.post(PREFIX + "/inbound-events", content=b"{", headers=h.headers()),
        "VALIDATION_ERROR",
        422,
    )


async def test_finite_multi_grants_cannot_be_combined_attribute_wise(h, sql):
    valid_a = h.event()
    valid_b = h.event("b")
    for payload in [valid_a, valid_b]:
        assert (await h.call("POST", "/inbound-events", payload, "multi")).status_code == 201
    for field in ["owner_id", "integration_id", "channel", "provider_namespace"]:
        mixed = dict(valid_a, provider_event_id=str(uuid4()))
        mixed[field] = valid_b[field]
        error(await h.call("POST", "/inbound-events", mixed, "multi"), "NOT_FOUND", 404)
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 2


@pytest.mark.parametrize("foreign", ["b", "other_integration", "other_owner"])
async def test_every_entity_read_and_mutation_returns_same_404_for_foreign_and_guessed(
    h, foreign, sql
):
    accepted = await h.ingest(foreign)
    conversation_id = accepted["conversation"]["id"]
    selection = dict(
        selection_key=str(uuid4()),
        input_first_sequence=1,
        input_last_sequence=1,
        expected_input_revision=1,
        expected_authority_revision=0,
    )
    turn_response = await h.call(
        "POST", f"/conversations/{conversation_id}/turns", selection, foreign
    )
    assert turn_response.status_code == 201
    turn_id = turn_response.json()["turn"]["id"]
    claim = (
        await h.call(
            "POST", f"/turns/{turn_id}/claim", dict(worker_ref="foreign", lease_seconds=30), foreign
        )
    ).json()
    assert (
        await h.call(
            "POST",
            f"/turns/{turn_id}/complete",
            dict(h.proof(claim), outcome="DRAFT_READY", draft_text="foreign private draft"),
            foreign,
        )
    ).status_code == 200
    creation = dict(
        delivery_key=str(uuid4()), source_turn_id=turn_id, expected_authority_revision=0
    )
    attempt_response = await h.call(
        "POST", f"/conversations/{conversation_id}/delivery-attempts", creation, foreign
    )
    assert attempt_response.status_code == 201
    attempt_id = attempt_response.json()["attempt"]["id"]
    ids = dict(
        conversation=conversation_id,
        message=accepted["message"]["id"],
        turn=turn_id,
        attempt=attempt_id,
    )
    guessed = dict.fromkeys(ids, str(uuid4()))
    for scope_ids in [ids, guessed]:
        c, m, t, d = [scope_ids[key] for key in ["conversation", "message", "turn", "attempt"]]
        routes = [
            ("GET", f"/conversations/{c}", None),
            ("GET", f"/conversations/{c}/messages", None),
            ("GET", f"/messages/{m}", None),
            ("POST", f"/conversations/{c}/turns", selection),
            ("GET", f"/turns/{t}", None),
            ("POST", f"/turns/{t}/claim", dict(worker_ref="attacker", lease_seconds=30)),
            ("POST", f"/turns/{t}/renew", dict(h.proof(claim), lease_seconds=30)),
            ("POST", f"/turns/{t}/complete", dict(h.proof(claim), outcome="NO_RESPONSE")),
            ("POST", f"/turns/{t}/fail", dict(h.proof(claim), failure_code="TIMEOUT")),
            (
                "POST",
                f"/conversations/{c}/control",
                dict(expected_authority_revision=0, control_mode="HUMAN"),
            ),
            ("POST", f"/conversations/{c}/delivery-attempts", creation),
            ("GET", f"/delivery-attempts/{d}", None),
            (
                "POST",
                f"/delivery-attempts/{d}/reconcile",
                dict(
                    reconciliation_key=str(uuid4()),
                    expected_revision=0,
                    status="DISPATCH_REQUESTED",
                    expected_authority_revision=0,
                ),
            ),
        ]
        for method, path, payload in routes:
            response = await h.call(method, path, payload)
            error(response, "NOT_FOUND", 404)
            assert (
                "foreign private draft" not in response.text
                and claim["claim_token"] not in response.text
            )
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT authority_revision FROM conversations")) == 0
        assert await connection.scalar(text("SELECT count(*) FROM agent_turns")) == 1
        assert await connection.scalar(text("SELECT revision FROM delivery_attempts")) == 0


async def test_foreign_source_turn_reference_is_404(h):
    mine = await h.ingest()
    foreign = await h.ingest("b")
    selection = dict(
        selection_key=str(uuid4()),
        input_first_sequence=1,
        input_last_sequence=1,
        expected_input_revision=1,
        expected_authority_revision=0,
    )
    turn = (
        await h.call(
            "POST", f"/conversations/{foreign['conversation']['id']}/turns", selection, "b"
        )
    ).json()["turn"]
    error(
        await h.call(
            "POST",
            f"/conversations/{mine['conversation']['id']}/delivery-attempts",
            dict(
                delivery_key=str(uuid4()), source_turn_id=turn["id"], expected_authority_revision=0
            ),
        ),
        "NOT_FOUND",
        404,
    )


async def test_request_id_errors_logs_and_claim_reads_are_redacted(h, sql, caplog):
    caplog.set_level(logging.DEBUG)
    private = "sensitive-message-body-marker"
    payload = h.event(
        text=private,
        provider_event_id="private-event-marker",
        provider_thread_id="private-thread-marker",
    )
    response = await h.client.post(
        PREFIX + "/inbound-events",
        json=payload,
        headers=dict(h.headers(), **{"X-Request-ID": "request-good:12"}),
    )
    assert response.status_code == 201 and response.headers["X-Request-ID"] == "request-good:12"
    accepted = response.json()
    turn, _ = await h.select(accepted)
    claim = await h.claim(turn)
    error_response = await h.call(
        "POST", f"/turns/{turn['id']}/renew", dict(h.proof(claim), lease_seconds=True)
    )
    error(error_response, "VALIDATION_ERROR", 422)
    invalid = await h.client.post(
        PREFIX + "/inbound-events",
        json=dict(payload, credentials="database-secret-marker"),
        headers=dict(h.headers(), **{"X-Request-ID": "invalid\nrequest-marker"}),
    )
    error(invalid, "VALIDATION_ERROR", 422)
    assert (
        "request-marker" not in invalid.headers["X-Request-ID"]
        and len(invalid.headers["X-Request-ID"]) <= 64
    )
    read = await h.call("GET", f"/turns/{turn['id']}")
    assert (
        "claim_token" not in read.text
        and "digest" not in read.text
        and claim["claim_token"] not in read.text
    )
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "ALTER TABLE messages ADD CONSTRAINT test_logging_fault "
                "CHECK (text <> 'fault-private-marker')"
            )
        )
    try:
        failure = await h.call("POST", "/inbound-events", h.event(text="fault-private-marker"))
        error(failure, "INTERNAL_ERROR", 500)
    finally:
        async with sql.begin() as connection:
            await connection.execute(
                text("ALTER TABLE messages DROP CONSTRAINT test_logging_fault")
            )
    logged = caplog.text
    for marker in [
        private,
        "private-event-marker",
        "private-thread-marker",
        "database-secret-marker",
        "fault-private-marker",
        claim["claim_token"],
        *h.tokens.values(),
    ]:
        assert marker not in logged
        assert marker not in error_response.text and marker not in failure.text
    assert "postgresql+asyncpg://" not in logged


async def test_health_ready_and_authenticated_openapi_only(h):
    assert (await h.client.get("/health")).json() == dict(
        status="ok", service="conversation-service"
    )
    assert (await h.client.get("/ready")).json() == dict(
        status="ready", service="conversation-service"
    )
    schema = await h.client.get("/internal/openapi.json", headers=h.headers())
    assert schema.status_code == 200
    assert "/internal/v1/inbound-events" in schema.json()["paths"]
    error(await h.client.get("/internal/openapi.json"), "UNAUTHENTICATED", 401)
    for path in ["/docs", "/redoc", "/openapi.json"]:
        assert (await h.client.get(path, headers=h.headers())).status_code == 404


@pytest.mark.parametrize(
    "mutation",
    ["empty", "duplicate_id", "duplicate_token", "duplicate_grant", "wildcard", "empty_token"],
)
def test_invalid_auth_config_fails_closed_and_settings_repr_redacted(h, mutation, database_url):
    from core.config import Settings

    registry = [dict(principal_id="one", token=h.tokens["a"], grants=[h.grants["a"]])]
    if mutation == "empty":
        registry = []
    elif mutation == "duplicate_id":
        registry.append(dict(principal_id="one", token=h.tokens["b"], grants=[h.grants["b"]]))
    elif mutation == "duplicate_token":
        registry.append(dict(principal_id="two", token=h.tokens["a"], grants=[h.grants["b"]]))
    elif mutation == "duplicate_grant":
        registry[0]["grants"].append(dict(h.grants["a"]))
    elif mutation == "wildcard":
        registry[0]["grants"][0] = dict(h.grants["a"], owner_id="*")
    else:
        registry[0]["token"] = ""
    with pytest.raises(ValidationError):
        Settings(database_url=database_url, auth_principals=registry, _env_file=None)
    assert h.tokens["a"] not in repr(h.settings) and database_url not in repr(h.settings)


def test_network_guard_denies_provider_access():
    with socket.socket() as sock, pytest.raises(AssertionError, match="Unexpected non-PostgreSQL"):
        sock.connect(("203.0.113.1", 443))


async def test_invalid_secret_environment_fails_startup_without_secret_echo(
    monkeypatch, database_url
):
    from main import create_app

    marker = "private-invalid-registry-marker"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("CONVERSATION_AUTH_PRINCIPALS", marker)
    with pytest.raises(
        RuntimeError, match="Invalid conversation-service configuration"
    ) as captured:
        async with LifespanManager(create_app()):
            pytest.fail("An invalid credential registry must never start")
    assert marker not in str(captured.value) and database_url not in str(captured.value)


async def test_non_ascii_token_rejected_before_validation(h):
    response = await h.client.post(
        PREFIX + "/inbound-events", content=b"{", headers=[(b"X-Service-Token", b"\xff" * 32)]
    )
    error(response, "UNAUTHENTICATED", 401)
