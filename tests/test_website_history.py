"""Credential history never publishes pending Human work or loses late acceptance."""
import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text

from models import WebsiteVisibleMessage
from schemas.contracts import ErrorCode
from services import website_history
from tests.conftest import error
from tests.test_common_text_delivery import human_intents
from tests.test_database import violation
from tests.test_managed_migration import migrate
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_idle_delivery import call, completion, drafted, identity, visitor, work
from tests.test_website_session import issued
from tests.test_website_session import sh as session_fixture
from tests.website_common_fixture import append, common_website

sh, suppliers, wh = session_fixture, supplier_fixture, common_website
PATH = "/website/sessions/history"


async def page(wh, cursor=None, **changes):
    return await wh.call("POST", PATH, dict(visitor(wh), cursor=cursor, **changes))


async def test_unknown_human_is_hidden_and_late_acceptance_survives_advanced_cursor(wh, suppliers):
    await human_intents(wh, suppliers)
    initial = (await page(wh)).json()
    assert [item["actor"] for item in initial["items"]] == ["CUSTOMER"]
    value = await work(wh)
    request = identity(wh, value)
    assert (await call(wh, "begin", request)).json()["send"]
    unknown = await call(wh, "complete", completion(request, "UNKNOWN", joined=False))
    assert unknown.status_code == 200
    await append(wh, "later customer input")
    advanced = (await page(wh)).json()
    assert [item["actor"] for item in advanced["items"]] == ["CUSTOMER", "CUSTOMER"]
    cursor = advanced["items"][-1]["id"]
    assert (await page(wh, cursor)).json()["items"] == []
    # Same original writer supplies positive evidence; no re-acquisition or resend.
    assert (await call(wh, "complete", completion(request))).status_code == 200
    late = (await page(wh, cursor)).json()
    assert len(late["items"]) == 1 and late["items"][0]["actor"] == "HUMAN"
    assert late["items"][0]["id"] == value["frame"]["delivery_id"]
    assert late["items"][0]["client_message_id"] is None
    assert (await page(wh, cursor)).json() == late
    assert len((await page(wh)).json()["items"]) == 3  # second unacquired reply stays hidden


async def test_agent_history_uses_poll_identity_and_original_customer_reconciliation(wh):
    await drafted(wh)
    value = await work(wh)
    request = identity(wh, value)
    assert len((await page(wh)).json()["items"]) == 1
    assert (await call(wh, "begin", request)).json()["send"]
    assert (await call(wh, "complete", completion(request))).status_code == 200
    first, second = await asyncio.gather(page(wh), page(wh))
    assert first.json() == second.json()
    customer, agent = first.json()["items"]
    assert customer["client_message_id"] is not None and agent["client_message_id"] is None
    assert agent["id"] == value["frame"]["delivery_id"]
    assert agent["id"] != request["source_id"] and agent["id"] != request["outbound_intent_id"]
    assert wh.created["session_token"] not in first.text
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WebsiteVisibleMessage)) == 2


async def test_pagination_fifty_and_materialization_limit_never_skip_eligible_input(wh):
    for index in range(105):
        await append(wh, f"input {index}")
    cursor, ids, sizes = None, [], []
    for _ in range(3):
        response = await page(wh, cursor)
        assert response.status_code == 200 and len(response.content) <= 262144
        value = response.json()
        sizes.append(len(value["items"]))
        ids.extend(item["id"] for item in value["items"])
        cursor = value["next_cursor"]
    assert sizes == [50, 50, 5] and cursor is None and len(set(ids)) == 105


async def test_full_serialized_json_byte_budget_keeps_escaped_content_and_cursor(wh):
    payload = "\x01" * 10000
    for _ in range(5):
        await append(wh, payload)
    first = await page(wh)
    assert len(first.content) <= 262144 and len(first.json()["items"]) == 4
    cursor = first.json()["next_cursor"]
    assert cursor == first.json()["items"][-1]["id"]
    last = await page(wh, cursor)
    assert len(last.json()["items"]) == 1 and last.json()["next_cursor"] is None
    assert all(item["text"] == payload for item in first.json()["items"] + last.json()["items"])


async def test_oversized_first_item_keeps_cursor_and_never_truncates_or_skips(wh, monkeypatch):
    await append(wh)
    cursor = (await page(wh)).json()["items"][0]["id"]
    await append(wh, "x" * 1000)
    monkeypatch.setattr(website_history, "MAX_PAGE_BYTES", 512)
    blocked = (await page(wh, cursor)).json()
    assert blocked["state"] == "FRAME_TOO_LARGE" and blocked["items"] == []
    assert blocked["next_cursor"] == cursor and blocked["oversized_message_id"] != cursor
    monkeypatch.setattr(website_history, "MAX_PAGE_BYTES", 262144)
    retained = (await page(wh, cursor)).json()["items"]
    assert retained[0]["id"] == blocked["oversized_message_id"]
    assert retained[0]["text"] == "x" * 1000


@pytest.mark.parametrize("field", ["session_id", "session_token", "owner_id", "agent_id",
                                   "binding_generation", "integration_id"])
async def test_wrong_visitor_history_has_no_supplier_io(wh, suppliers, field):
    value = visitor(wh)
    value[field] = ("x" * 43 if field == "session_token" else "foreign-owner"
        if field == "owner_id" else 8 if field == "binding_generation" else str(uuid4()))
    suppliers.calls.clear()
    error(await wh.call("POST", PATH, value), "NOT_FOUND", 404)
    assert suppliers.calls == []


async def test_unknown_cursor_proof_outage_and_closed_http_shape(wh, suppliers):
    await append(wh)
    error(await page(wh, str(uuid4())), "NOT_FOUND", 404)
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    error(await page(wh), "SERVICE_UNAVAILABLE", 503)
    suppliers.failure = None
    assert (await page(wh, internal_id=str(uuid4()))).status_code == 422


async def test_visibility_positions_are_immutable(wh, sql):
    await append(wh)
    await page(wh)
    await violation(sql, "UPDATE website_visible_messages SET ordinal=ordinal+1", {}, "23514")
    await violation(sql, "DELETE FROM website_visible_messages", {}, "23514")
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM website_visible_messages")) == 1


async def test_second_credential_cannot_use_first_visitors_cursor_or_history(wh):
    await append(wh, "first visitor input")
    original = (await page(wh)).json()["items"][0]
    wh.created = await issued(wh)
    assert (await page(wh)).json()["items"] == []
    error(await page(wh, original["id"]), "NOT_FOUND", 404)
    await append(wh, "second visitor input")
    separate = (await page(wh)).json()["items"]
    assert len(separate) == 1 and separate[0]["id"] != original["id"]
    assert separate[0]["client_message_id"] != original["client_message_id"]


async def test_downgrade_keeps_retained_visibility_and_restart_cursor(wh, database_url):
    await append(wh)
    first = (await page(wh)).json()
    cursor = first["items"][0]["id"]
    rejected = migrate(database_url, "downgrade", "0015_website_grant_epoch", expected=1)
    assert "compatible reader" in rejected.stderr
    assert await wh.database.ready()
    assert (await page(wh)).json() == first
    assert (await page(wh, cursor)).json()["items"] == []
