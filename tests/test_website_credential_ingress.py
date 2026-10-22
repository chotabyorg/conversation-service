"""Private credential ingress reuses canonical admissions and common grant proofs."""
import asyncio
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text

from core.config import SettlingPolicy
from models import Conversation, InboxGrantScope, Message, WebsiteAdmission
from schemas.contracts import ErrorCode
from services.authority import advance_authority
from tests.conftest import error, observed_waiters
from tests.test_website_grant import enroll
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_session import issued, scope
from tests.test_website_session import sh as session_fixture

sh, suppliers = session_fixture, supplier_fixture
PATH = "/internal/v1/website/sessions/admissions"


def request(sh, created, **changes):
    return dict(scope(sh), session_id=created["session_id"],
        session_token=created["session_token"], message_id=str(uuid4()), text="customer input",
        **changes)


async def post(sh, value):
    return await sh.client.post(PATH, json=value, headers=sh.headers())


async def test_private_ingress_commits_one_canonical_message_and_exact_receipt(sh, suppliers, sql):
    await enroll(sh, suppliers)
    created = await issued(sh)
    value = request(sh, created)
    first, second = await asyncio.gather(post(sh, value), post(sh, value))
    assert first.status_code == second.status_code == 200
    assert first.json()["admission_id"] == second.json()["admission_id"]
    assert sorted([first.json()["replayed"], second.json()["replayed"]]) == [False, True]
    async with sql.connect() as conn:
        for table in ("inbound_events", "messages", "website_admissions"):
            assert await conn.scalar(text(f"SELECT count(*) FROM {table}")) == 1
            retained = await conn.scalar(text(f"SELECT row_to_json(row)::text FROM {table} row"))
            assert created["session_token"] not in retained
        root = (await conn.execute(text("SELECT * FROM conversations"))).mappings().one()
        assert root["provider_thread_id"] == created["session_id"]
        assert root["input_revision"] == root["last_message_sequence"] == 1
    changed = dict(value, text="different input")
    error(await post(sh, changed), "EVENT_CONFLICT", 409)


@pytest.mark.parametrize("field", ["session_token", "session_id", "agent_id",
                                    "integration_id", "owner_id", "binding_generation"])
async def test_wrong_visitor_or_scope_cannot_append_or_call_suppliers(sh, suppliers, field):
    await enroll(sh, suppliers)
    value = request(sh, await issued(sh))
    value[field] = ("x" * 43 if field == "session_token" else "foreign-owner"
                    if field == "owner_id" else 8 if field == "binding_generation"
                    else str(uuid4()))
    suppliers.calls.clear()
    error(await post(sh, value), "NOT_FOUND", 404)
    assert suppliers.calls == []
    async with sh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)) == 0


async def test_proof_outage_and_revocation_fail_before_input_append(sh, suppliers):
    await enroll(sh, suppliers)
    value = request(sh, await issued(sh))
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    error(await post(sh, value), "SERVICE_UNAVAILABLE", 503)
    suppliers.failure = None
    async with sh.database.session_factory() as session, session.begin():
        grant = await session.scalar(select(InboxGrantScope).with_for_update())
        grant.active = False
        grant.cp_binding_high_water = grant.cp_binding_generation
    error(await post(sh, value), "NOT_FOUND", 404)
    async with sh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WebsiteAdmission)) == 0


async def test_input_waiter_observes_revoke_before_canonical_append(sh, suppliers, sql):
    await enroll(sh, suppliers)
    value = request(sh, await issued(sh))
    async with sh.database.session_factory() as blocker, blocker.begin():
        grant = await blocker.scalar(select(InboxGrantScope).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(post(sh, value))
        try:
            await observed_waiters(sql, pid, 1, "credential_input_after_common_revoke")
            grant.active = False
        finally:
            await blocker.commit()
    error(await pending, "NOT_FOUND", 404)
    async with sh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)) == 0


async def test_human_input_is_canonical_and_post_return_burst_has_new_authority(sh, suppliers):
    await enroll(sh, suppliers)
    created = await issued(sh)
    settings = sh.settings.model_copy(update={"turn_settling_policies": (SettlingPolicy(
        integration_id=UUID(str(scope(sh)["integration_id"])), channel="WEBSITE_WIDGET",
        version="credential-ingress-v1", quiet_window_ms=1000, max_wait_ms=2000),)})
    sh.client._transport.app.state.settings = settings
    async with sh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        advance_authority(root)
        root.control_mode = "HUMAN"
    assert (await post(sh, request(sh, created))).status_code == 200
    async with sh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        assert root.control_mode == "HUMAN"
        advance_authority(root)
        root.control_mode = "AGENT"
        root.handoff_resume_floor_sequence = root.last_message_sequence
    assert (await post(sh, request(sh, created))).status_code == 200
    async with sh.database.session_factory() as session:
        messages = list(await session.scalars(select(Message).order_by(Message.sequence)))
        assert len(messages) == 2 and messages[0].settling_burst_id != messages[1].settling_burst_id
        assert (await session.scalar(select(Conversation))).authority_revision == 2


async def test_foundation_credential_cannot_be_promoted_into_content_ingress(sh, suppliers):
    created = await issued(sh)
    await enroll(sh, suppliers)
    suppliers.calls.clear()
    error(await post(sh, request(sh, created)), "NOT_FOUND", 404)
    assert suppliers.calls == []
