import asyncio

from sqlalchemy import text

from tests.conftest import PREFIX, app_client, error
from tests.test_turns import expire


async def test_new_app_and_engine_recover_same_claimed_turn_after_public_lease_expiry(
    h, sql, database_url
):
    payload = h.event()
    accepted = (await h.call("POST", "/inbound-events", payload)).json()
    turn, selection = await h.select(accepted)
    claim = await h.claim(turn, lease_seconds=1)
    await h.database.dispose()
    async with app_client(h.settings, database_url) as (client, fresh_database):
        assert fresh_database is not h.database and fresh_database.engine is not h.database.engine

        async def call(method, path, body=None):
            return await client.request(method, PREFIX + path, json=body, headers=h.headers())

        assert (await call("GET", f"/messages/{accepted['message']['id']}")).json() == accepted[
            "message"
        ]
        persisted = (await call("GET", f"/turns/{turn['id']}")).json()
        assert persisted == claim["turn"] and persisted["status"] == "CLAIMED"
        assert (await call("POST", "/inbound-events", payload)).json()["message"]["id"] == accepted[
            "message"
        ]["id"]
        assert (
            await call("POST", f"/conversations/{turn['conversation_id']}/turns", selection)
        ).json()["turn"]["id"] == turn["id"]
        deadline = asyncio.get_running_loop().time() + 4
        while True:
            async with sql.connect() as connection:
                expired = await connection.scalar(
                    text(
                        "SELECT clock_timestamp() >= lease_expires_at FROM agent_turns WHERE id=:id"
                    ),
                    {"id": turn["id"]},
                )
            if expired:
                break
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.02)
        recovered = await call(
            "POST", f"/turns/{turn['id']}/claim", dict(worker_ref="after-restart", lease_seconds=30)
        )
        assert recovered.status_code == 200
        new = recovered.json()
        assert new["turn"]["id"] == turn["id"] and new["turn"]["claim_epoch"] == 2
        assert (
            new["claim_token"] != claim["claim_token"]
            and new["turn"]["started_at"] == claim["turn"]["started_at"]
        )
        error(
            await call(
                "POST", f"/turns/{turn['id']}/complete", dict(h.proof(claim), outcome="NO_RESPONSE")
            ),
            "STALE_CLAIM",
        )
        assert (
            await call(
                "POST", f"/turns/{turn['id']}/complete", dict(h.proof(new), outcome="NO_RESPONSE")
            )
        ).status_code == 200


async def test_pending_turn_and_terminal_replay_survive_independent_app_restart(
    h, sql, database_url
):
    accepted = await h.ingest()
    turn, selection = await h.select(accepted)
    await h.database.dispose()
    async with app_client(h.settings, database_url) as (client, database):
        assert database.engine is not h.database.engine
        response = await client.get(PREFIX + f"/turns/{turn['id']}", headers=h.headers())
        assert response.json() == turn and response.json()["status"] == "PENDING"
        claim = (
            await client.post(
                PREFIX + f"/turns/{turn['id']}/claim",
                json=dict(worker_ref="restart", lease_seconds=30),
                headers=h.headers(),
            )
        ).json()
        terminal = dict(h.proof(claim), outcome="DRAFT_READY", draft_text="durable draft")
        done = await client.post(
            PREFIX + f"/turns/{turn['id']}/complete", json=terminal, headers=h.headers()
        )
        assert done.status_code == 200
        persisted = done.json()["turn"]
    await expire(sql, turn["id"])
    await h.ingest()
    assert (
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/control",
            dict(expected_authority_revision=0, control_mode="HUMAN"),
        )
    ).status_code == 200
    async with app_client(h.settings, database_url) as (client, _):
        replay = await client.post(
            PREFIX + f"/turns/{turn['id']}/complete", json=terminal, headers=h.headers()
        )
        assert replay.status_code == 200 and replay.json()["replayed"]
        assert (
            replay.json()["turn"]["id"] == persisted["id"]
            and replay.json()["turn"]["completed_at"] == persisted["completed_at"]
        )
        replay_create = await client.post(
            PREFIX + f"/conversations/{turn['conversation_id']}/turns",
            json=selection,
            headers=h.headers(),
        )
        assert replay_create.status_code == 200 and replay_create.json()["turn"]["id"] == turn["id"]


async def test_delivery_reconciliation_history_and_old_key_survive_restart(h, database_url):
    accepted, attempt, creation = await h.delivery()
    attempt, dispatch = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    attempt, unknown = await h.reconcile(attempt, "UNKNOWN", error_code="TIMEOUT")
    attempt, _ = await h.reconcile(attempt, "DELIVERED")
    await h.database.dispose()
    async with app_client(h.settings, database_url) as (client, database):
        assert database.engine is not h.database.engine
        for payload, applied in [(dispatch, 1), (unknown, 2)]:
            replay = await client.post(
                PREFIX + f"/delivery-attempts/{attempt['id']}/reconcile",
                json=payload,
                headers=h.headers(),
            )
            assert replay.status_code == 200 and replay.json() == dict(
                attempt=attempt, applied_revision=applied, replayed=True
            )
        replay = await client.post(
            PREFIX + f"/conversations/{accepted['conversation']['id']}/delivery-attempts",
            json=creation,
            headers=h.headers(),
        )
        assert replay.status_code == 200 and replay.json()["attempt"] == attempt
