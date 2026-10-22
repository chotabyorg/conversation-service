"""Real PostgreSQL poll admission, restart, lease fences and independent writers."""
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, select

from models import Conversation, DeliveryAttempt, InboxGrantScope, WebsitePollState
from schemas.contracts import ErrorCode
from tests.conftest import error
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_idle_delivery import call, completion, drafted, identity, visitor, work
from tests.test_website_session import sh as session_fixture
from tests.website_common_fixture import common_website

sh, suppliers, wh = session_fixture, supplier_fixture, common_website
BASE = "/website/sessions/polls/"


def request(wh):
    return dict(visitor(wh), request_id=str(uuid4()), supplier_instance_id=str(uuid4()),
                process_incarnation_id=str(uuid4()))


async def begin(wh, value=None):
    return await wh.call("POST", BASE + "begin", value or request(wh))


def access(wh, original, result):
    return dict(visitor(wh), poll_id=result["poll_id"],
        supplier_instance_id=original["supplier_instance_id"],
        process_incarnation_id=original["process_incarnation_id"])


async def end(wh, value):
    return await wh.call("POST", BASE + "end", value)


async def test_two_workers_cannot_start_two_inflight_polls(wh):
    results = await asyncio.gather(begin(wh), begin(wh))
    assert sorted(value.status_code for value in results) == [200, 429]
    admitted = next(value for value in results if value.status_code == 200).json()
    assert admitted["state"] == "ACQUIRED"
    deadline = datetime.fromisoformat(admitted["deadline_at"].replace("Z", "+00:00"))
    assert deadline == wh.clock.current + timedelta(seconds=20)
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WebsitePollState)) == 1
        assert await session.scalar(select(func.count()).select_from(DeliveryAttempt)) == 0


async def test_lost_begin_reply_returns_no_second_executable_grant(wh):
    value = request(wh)
    created = await begin(wh, value)
    assert created.status_code == 200
    async with wh.database.session_factory() as session:
        before = (await session.scalar(select(WebsitePollState))).theoretical_at
    replay = await begin(wh, value)
    assert replay.status_code == 200
    assert replay.json() == {"state": "RECOVER_ONLY", "poll_id": None, "deadline_at": None}
    assert len(replay.content) <= 4096
    changed = dict(value, process_incarnation_id=str(uuid4()))
    error(await begin(wh, changed), "NOT_FOUND", 404)
    original = access(wh, value, created.json())
    assert (await end(wh, original)).json() == {"replayed": False}
    assert (await end(wh, original)).json() == {"replayed": True}
    assert (await begin(wh, value)).json()["state"] == "RECOVER_ONLY"
    async with wh.database.session_factory() as session:
        assert (await session.scalar(select(WebsitePollState))).theoretical_at == before


async def test_rate_debt_survives_release_new_worker_and_refills_on_db_clock(wh):
    for _ in range(2):
        original = request(wh)
        result = await begin(wh, original)
        assert result.status_code == 200
        assert (await end(wh, access(wh, original, result.json()))).status_code == 200
    denied = await begin(wh)  # Different supplier/process, same durable visitor budget.
    error(denied, "RATE_LIMITED", 429)
    assert denied.headers["Retry-After"] == "2"
    wh.clock.advance_to(wh.clock.current + timedelta(seconds=2))
    assert (await begin(wh)).status_code == 200


async def test_expired_poll_cannot_begin_write_or_end_replacement_poll(wh):
    await drafted(wh)
    first = await work(wh)
    old = identity(wh, first)
    old_access = dict(visitor(wh), **wh.poll_access)
    wh.clock.advance_to(wh.poll_deadline)
    fresh = request(wh)
    next_poll = await begin(wh, fresh)
    assert next_poll.status_code == 200
    error(await call(wh, "begin", old), "NOT_FOUND", 404)
    error(await call(wh, "work", old_access), "NOT_FOUND", 404)
    error(await end(wh, old_access), "NOT_FOUND", 404)
    current = access(wh, fresh, next_poll.json())
    result = await call(wh, "work", current)
    assert result.status_code == 200 and result.json()["state"] == "SEND_CANDIDATE"
    begun = await call(wh, "begin", dict(current, **result.json()["item"], recover_only=False))
    assert begun.status_code == 200 and begun.json()["send"]


async def test_poll_expiry_and_release_never_end_original_unknown_writer(wh):
    await drafted(wh)
    old = identity(wh, await work(wh))
    assert (await call(wh, "begin", old)).json()["send"]
    assert (await call(wh, "complete", completion(old, "UNKNOWN", joined=False))).status_code == 200
    wh.clock.advance_to(wh.poll_deadline)
    value = request(wh)
    fresh = await begin(wh, value)
    current = access(wh, value, fresh.json())
    result = await call(wh, "work", current)
    assert result.status_code == 200 and result.json()["state"] == "RECOVER_ONLY"
    assert result.json()["frame"] is None
    assert (await end(wh, current)).status_code == 200
    async with wh.database.session_factory() as session:
        original = await session.scalar(select(DeliveryAttempt))
        assert original.writer_state == "LIVE" and original.status == "UNKNOWN"
    joined = await call(wh, "complete", completion(old, "UNKNOWN"))
    assert joined.status_code == 200 and joined.json()["recovery"]["writer_state"] == "ENDED"


async def test_poll_cleanup_after_revoke_and_config_removal_is_original_metadata_only(
    wh, suppliers,
):
    original = request(wh)
    value = access(wh, original, (await begin(wh, original)).json())
    async with wh.database.session_factory() as session, session.begin():
        grant = await session.scalar(select(InboxGrantScope).with_for_update())
        grant.active = False
        await session.scalar(select(Conversation).with_for_update())
    wh.client._transport.app.state.settings = wh.settings.model_copy(update={
        "website_session_capabilities_enabled": False, "website_bindings": ()})
    suppliers.calls.clear()
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    error(await end(wh, dict(value, process_incarnation_id=str(uuid4()))), "NOT_FOUND", 404)
    result = await end(wh, value)
    assert result.status_code == 200 and result.json() == {"replayed": False}
    error(await begin(wh), "NOT_FOUND", 404)
    assert suppliers.calls == []


async def test_invalid_credential_and_proof_outage_do_not_reserve_polls(wh, suppliers):
    invalid = dict(request(wh), session_token="x" * 43)
    suppliers.calls.clear()
    error(await begin(wh, invalid), "NOT_FOUND", 404)
    assert suppliers.calls == []
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    error(await begin(wh), "SERVICE_UNAVAILABLE", 503)
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WebsitePollState)) == 0


async def test_original_poll_supplier_and_process_bind_work_and_writer(wh):
    await drafted(wh)
    value = await work(wh)
    current = identity(wh, value)
    changed = dict(current, process_incarnation_id=str(uuid4()))
    error(await call(wh, "begin", changed), "NOT_FOUND", 404)
    assert (await call(wh, "begin", current)).json()["send"]
