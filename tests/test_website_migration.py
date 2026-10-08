"""Additive populated0002 migration preserves all retained foundation evidence."""
from uuid import uuid4

from sqlalchemy import text

from tests.test_managed_migration import migrate, rows
from tests.test_website import admit, payload
from tests.test_website import wh as website_fixture

wh = website_fixture


async def test_populated0002_to_head_keeps_ids_fingerprints_leases_drafts_and_refs(
    h, sql, database_url
):
    accepted, turn = await h.draft()
    async with sql.begin() as conn:
        await conn.execute(text(
            "UPDATE agent_turns SET managed_agent_id=:agent,managed_dispatch_epoch=claim_epoch,"
            "managed_dispatch_started_at=started_at,managed_request_fingerprint=:digest "
            "WHERE id=:id"
        ), {"agent": str(uuid4()), "digest": "b" * 64, "id": turn["id"]})
    before = await rows(sql)
    await h.database.dispose()
    migrate(database_url, "downgrade", "0002_managed_execution")
    assert not await h.database.ready()
    assert await rows(sql) == before
    migrate(database_url, "upgrade", "head")
    assert await h.database.ready()
    assert await rows(sql) == before
    assert (await h.call("GET", f"/messages/{accepted['message']['id']}")).status_code == 200


async def test_retained_legacy_reservation_blocks_schema_downgrade(wh, sql, database_url):
    value = payload(wh, desired_route="LEGACY")
    result = await admit(wh, value)
    await wh.database.dispose()
    rejected = migrate(database_url, "downgrade", "0002_managed_execution", expected=1)
    assert "Retained Website admission prevents downgrade." in rejected.stderr
    assert await wh.database.ready()
    assert await admit(wh, dict(value, desired_route="MANAGED")) == dict(result, replayed=True)
