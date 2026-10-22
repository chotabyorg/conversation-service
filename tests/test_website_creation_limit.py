"""Durable anonymous creation admission: concurrency, restart, quota and rollback."""
import asyncio
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select, text

from models import Conversation, WebsiteCreationBucket, WebsiteSessionCapability
from services import website_creation_limit, website_session
from tests.conftest import app_client, error
from tests.test_website_session import creation
from tests.test_website_session import sh as session_fixture

sh = session_fixture
PATH = "/website/sessions"


async def create(sh, ip="192.0.2.1"):
    return await sh.call("POST", PATH, dict(creation(sh), client_ip=ip))


async def counts(sh):
    async with sh.database.session_factory() as session:
        return [await session.scalar(select(func.count()).select_from(model))
                for model in (Conversation, WebsiteSessionCapability, WebsiteCreationBucket)]


async def test_ip_burst_is_shared_across_restart_and_refills_by_database_clock(
    sh, database_url, test_database_clock,
):
    assert (await create(sh)).status_code == (await create(sh)).status_code == 201
    blocked = await create(sh)
    error(blocked, "RATE_LIMITED", 429)
    assert blocked.headers["Retry-After"] == "12"
    assert await counts(sh) == [2, 2, 2]
    async with app_client(sh.settings, database_url) as (client, _):
        restored = await client.post("/internal/v1" + PATH, json=creation(sh), headers=sh.headers())
        error(restored, "RATE_LIMITED", 429)
    test_database_clock.advance_to(test_database_clock.current + timedelta(seconds=12))
    assert (await create(sh)).status_code == 201


async def test_concurrent_same_ip_issues_two_and_no_denied_root_or_credential(
    sh, test_database_clock,
):
    replies = await asyncio.gather(*(create(sh) for _ in range(6)))
    assert sorted(response.status_code for response in replies) == [201, 201, 429, 429, 429, 429]
    assert await counts(sh) == [2, 2, 2]


async def test_aggregate_burst_survives_different_client_ips_and_does_not_retain_raw_ip(
    sh, test_database_clock, sql,
):
    replies = await asyncio.gather(*(create(sh, f"192.0.2.{index}") for index in range(1, 13)))
    assert sorted(response.status_code for response in replies) == [201] * 10 + [429] * 2
    assert await counts(sh) == [10, 10, 11]
    delays = {response.headers["Retry-After"] for response in replies
              if response.status_code == 429}
    assert delays == {"1"}
    async with sql.connect() as connection:
        records = await connection.scalars(text(
            "SELECT row_to_json(bucket)::text FROM website_creation_buckets bucket"))
        assert all("192.0.2." not in value for value in records)
    test_database_clock.advance_to(test_database_clock.current + timedelta(seconds=1))
    assert (await create(sh, "192.0.2.13")).status_code == 201


async def test_ipv4_mapped_address_uses_the_same_client_bucket(sh, test_database_clock):
    assert (await create(sh, "192.0.2.1")).status_code == 201
    assert (await create(sh, "::ffff:192.0.2.1")).status_code == 201
    error(await create(sh, "192.0.2.1"), "RATE_LIMITED", 429)
    assert await counts(sh) == [2, 2, 2]


async def test_unexpired_quota_counts_revoked_credentials_and_expiry_frees_capacity(
    sh, test_database_clock, monkeypatch,
):
    monkeypatch.setattr(website_creation_limit, "MAX_UNEXPIRED", 2)
    assert (await create(sh, "192.0.2.1")).status_code == 201
    assert (await create(sh, "192.0.2.2")).status_code == 201
    async with sh.database.session_factory() as session, session.begin():
        for capability in await session.scalars(select(WebsiteSessionCapability)):
            capability.revoked_at = test_database_clock.current
    error(await create(sh, "192.0.2.3"), "RATE_LIMITED", 429)
    assert await counts(sh) == [2, 2, 3]
    test_database_clock.advance_to(test_database_clock.current + timedelta(days=1))
    assert (await create(sh, "192.0.2.3")).status_code == 201
    # Expiry frees capacity, but canonical credential/revocation evidence is retained.
    assert (await counts(sh))[:2] == [3, 3]


async def test_invalid_client_context_and_scope_have_no_admission_side_effects(sh):
    missing = creation(sh)
    missing.pop("client_ip")
    error(await sh.call("POST", PATH, missing), "VALIDATION_ERROR", 422)
    error(await create(sh, "untrusted-forwarded-value"), "VALIDATION_ERROR", 422)
    error(await sh.call("POST", PATH, dict(creation(sh), owner_id="different")), "NOT_FOUND", 404)
    assert await counts(sh) == [0, 0, 0]


async def test_root_collision_rolls_back_both_rate_reservations(
    sh, monkeypatch, test_database_clock,
):
    first = await create(sh)
    original = website_session.uuid4
    values = iter([UUID(first.json()["session_id"]), uuid4()])
    with monkeypatch.context() as patch:
        patch.setattr(website_session, "uuid4", lambda: next(values))
        error(await create(sh), "NOT_FOUND", 404)
    assert website_session.uuid4 is original
    assert (await create(sh)).status_code == 201
    error(await create(sh), "RATE_LIMITED", 429)
    assert await counts(sh) == [2, 2, 2]
