import hashlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import error


async def test_identity_owner_integration_namespace_and_exact_unicode(h):
    first = await h.ingest(provider_event_id="same-event")
    next_event = await h.ingest()
    assert first["conversation"]["id"] == next_event["conversation"]["id"]
    for principal in ["other_owner", "other_integration", "b"]:
        isolated = await h.ingest(principal, provider_event_id="same-event")
        assert isolated["conversation"]["id"] != first["conversation"]["id"]
    namespace_payload = h.event(provider_namespace="account-extra", provider_event_id="same-event")
    result = await h.call("POST", "/inbound-events", namespace_payload, "namespace")
    assert result.status_code == 201
    assert result.json()["conversation"]["id"] == first["conversation"]["id"]
    unicode_a = await h.ingest(provider_thread_id="é")
    unicode_b = await h.ingest(provider_thread_id="e\u0301")
    assert unicode_a["conversation"]["id"] != unicode_b["conversation"]["id"]
    spaced = await h.ingest(provider_thread_id=" thread-123 ")
    assert spaced["conversation"]["id"] != first["conversation"]["id"]


async def test_fingerprint_server_canonicalization_and_stable_replay(h, sql):
    payload = h.event(
        text=" é\r\n e\u0301 ",
        channel="telegram",
        integration_id=h.grants["a"]["integration_id"].upper(),
        provider_occurred_at="2026-10-06T12:00:01.1234+03:00",
    )
    created = await h.call("POST", "/inbound-events", payload)
    assert created.status_code == 201
    first = created.json()
    canonical = dict(
        payload,
        contract_version=1,
        channel="TELEGRAM",
        integration_id=payload["integration_id"].lower(),
        provider_occurred_at="2026-10-06T09:00:01.123400Z",
        event_kind="TEXT",
        actor="CUSTOMER",
        direction="INBOUND",
    )
    expected = hashlib.sha256(
        json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()
    assert first["event"]["fingerprint"] == expected
    replay = await h.call(
        "POST",
        "/inbound-events",
        dict(
            reversed(
                list(dict(payload, provider_occurred_at="2026-10-06T09:00:01.123400Z").items())
            )
        ),
    )
    assert replay.status_code == 200
    assert replay.json() == dict(first, replayed=True)
    assert first["message"]["text"] == payload["text"]
    assert first["event"]["accepted_at"] == first["message"]["created_at"]
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 1


async def test_absent_null_timestamp_equivalent(h):
    payload = h.event()
    created = await h.call("POST", "/inbound-events", payload)
    replay = await h.call("POST", "/inbound-events", dict(payload, provider_occurred_at=None))
    assert created.status_code == 201 and replay.status_code == 200
    assert replay.json()["message"]["id"] == created.json()["message"]["id"]


@pytest.mark.parametrize(
    "change",
    [
        {"text": "changed"},
        {"provider_thread_id": "phantom-thread"},
        {"provider_occurred_at": "2026-01-01T00:00:00Z"},
    ],
)
async def test_conflicting_event_rolls_back_identity_and_counter(h, sql, change):
    payload = h.event(provider_event_id="shared")
    first = (await h.call("POST", "/inbound-events", payload)).json()
    response = await h.call("POST", "/inbound-events", dict(payload, **change))
    error(response, "EVENT_CONFLICT")
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM conversations")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM inbound_events")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 1
    read = (await h.call("GET", f"/conversations/{first['conversation']['id']}")).json()
    assert read == first["conversation"]
    accepted = await h.ingest()
    assert accepted["message"]["sequence"] == 2
    assert accepted["conversation"]["input_revision"] == 2


async def test_unicode_text_composition_remains_immutable(h):
    payload = h.event(text="é")
    assert (await h.call("POST", "/inbound-events", payload)).status_code == 201
    error(await h.call("POST", "/inbound-events", dict(payload, text="e\u0301")), "EVENT_CONFLICT")


async def test_sequence_pagination_uses_sequence_not_provider_time(h):
    accepted = []
    for timestamp in ["2026-10-06T00:00:00Z", "2020-01-01T00:00:00Z", None, "2026-10-06T00:00:00Z"]:
        accepted.append(await h.ingest(provider_occurred_at=timestamp))
    conversation = accepted[-1]["conversation"]
    response = await h.call("GET", f"/conversations/{conversation['id']}/messages?limit=2")
    assert response.status_code == 200
    assert [item["sequence"] for item in response.json()["items"]] == [1, 2]
    assert response.json()["next_after_sequence"] == 2
    rest = (
        await h.call(
            "GET", f"/conversations/{conversation['id']}/messages?after_sequence=2&limit=2"
        )
    ).json()
    assert [item["sequence"] for item in rest["items"]] == [3, 4]
    assert rest["next_after_sequence"] is None
    assert conversation["last_message_sequence"] == conversation["input_revision"] == 4
    for item in accepted:
        read = await h.call("GET", f"/messages/{item['message']['id']}")
        assert read.json() == item["message"]


async def test_constraint_failure_is_atomic_and_next_append_has_no_gap(h, sql):
    # A DB-level guard is a meaningful fault after the ingestion transaction begins.
    async with sql.begin() as connection:
        await connection.execute(
            text("ALTER TABLE messages ADD CONSTRAINT test_reject_text CHECK (text <> 'reject-me')")
        )
    try:
        response = await h.call("POST", "/inbound-events", h.event(text="reject-me"))
        error(response, "INTERNAL_ERROR", 500)
        async with sql.connect() as connection:
            for table in ["conversations", "messages", "inbound_events"]:
                assert await connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        async with sql.begin() as connection:
            await connection.execute(text("ALTER TABLE messages DROP CONSTRAINT test_reject_text"))
    accepted = await h.ingest()
    assert accepted["message"]["sequence"] == 1


async def test_scope_rejection_never_creates_identity(h, sql):
    for change in [
        {"owner_id": "unknown"},
        {"integration_id": str(uuid4())},
        {"channel": "VOICE"},
        {"provider_namespace": "ungranted"},
    ]:
        error(await h.call("POST", "/inbound-events", h.event(**change)), "NOT_FOUND", 404)
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM conversations")) == 0
