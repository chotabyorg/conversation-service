from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import error


async def expire(sql, turn_id):
    async with sql.begin() as connection:
        await connection.execute(
            text(
                "UPDATE agent_turns SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE id=:id"
            ),
            {"id": turn_id},
        )


async def test_selection_freezes_context_and_replay_precedes_new_history_and_mode(h):
    accepted = await h.ingest()
    turn, payload = await h.select(accepted)
    claim = await h.claim(turn)
    await h.ingest()
    result = await h.call(
        "POST",
        f"/conversations/{turn['conversation_id']}/control",
        dict(expected_authority_revision=0, control_mode="HUMAN"),
    )
    assert result.status_code == 200
    done = await h.call(
        "POST", f"/turns/{turn['id']}/complete", dict(h.proof(claim), outcome="NO_RESPONSE")
    )
    assert done.status_code == 200
    replay = await h.call("POST", f"/conversations/{turn['conversation_id']}/turns", payload)
    assert replay.status_code == 200 and replay.json()["replayed"]
    frozen = replay.json()["turn"]
    assert frozen["id"] == turn["id"] and frozen["status"] == "COMPLETED"
    for field in [
        "input_first_sequence",
        "input_last_sequence",
        "context_last_sequence",
        "input_revision",
        "authority_revision",
        "created_at",
    ]:
        assert frozen[field] == turn[field]
    error(
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/turns",
            dict(payload, expected_input_revision=2),
        ),
        "TURN_SELECTION_CONFLICT",
    )


@pytest.mark.parametrize(
    "changes,code",
    [
        ({"expected_input_revision": 0}, "INPUT_REVISION_CONFLICT"),
        ({"expected_authority_revision": 1}, "AUTHORITY_REVISION_CONFLICT"),
        ({"input_last_sequence": 2}, "INVALID_INPUT_BOUNDARY"),
    ],
)
async def test_selection_rejects_stale_or_missing_boundary(h, changes, code):
    accepted = await h.ingest()
    payload = dict(
        selection_key=str(uuid4()),
        input_first_sequence=1,
        input_last_sequence=1,
        expected_input_revision=1,
        expected_authority_revision=0,
        **{},
    )
    payload.update(changes)
    error(
        await h.call("POST", f"/conversations/{accepted['conversation']['id']}/turns", payload),
        code,
    )


async def test_pending_claimed_and_expired_share_single_active_slot(h, sql):
    accepted = await h.ingest()
    turn, payload = await h.select(accepted)
    for state in ["PENDING", "CLAIMED", "EXPIRED"]:
        if state == "CLAIMED":
            await h.claim(turn)
        elif state == "EXPIRED":
            await expire(sql, turn["id"])
        error(
            await h.call(
                "POST",
                f"/conversations/{turn['conversation_id']}/turns",
                dict(payload, selection_key=str(uuid4())),
            ),
            "ACTIVE_TURN_EXISTS",
        )


async def test_recovery_rotates_proof_same_identity_and_stale_owner_rejected(h, sql):
    accepted = await h.ingest()
    turn, _ = await h.select(accepted)
    initial = await h.claim(turn)
    assert len(initial["claim_token"]) == 43 and initial["turn"]["claim_epoch"] == 1
    error(
        await h.call(
            "POST", f"/turns/{turn['id']}/claim", dict(worker_ref="worker-b", lease_seconds=30)
        ),
        "LEASE_HELD",
    )
    renewed = await h.call(
        "POST", f"/turns/{turn['id']}/renew", dict(h.proof(initial), lease_seconds=60)
    )
    assert renewed.status_code == 200 and renewed.json()["claim_epoch"] == 1
    assert (
        "claim_token" not in renewed.json()
        and renewed.json()["lease_expires_at"] >= initial["turn"]["lease_expires_at"]
    )
    await expire(sql, turn["id"])
    recovered_response = await h.call(
        "POST", f"/turns/{turn['id']}/claim", dict(worker_ref="worker-b", lease_seconds=30)
    )
    assert recovered_response.status_code == 200
    recovered = recovered_response.json()
    assert recovered["turn"]["id"] == turn["id"]
    assert recovered["turn"]["claim_epoch"] == 2
    assert recovered["claim_token"] != initial["claim_token"]
    assert recovered["turn"]["started_at"] == initial["turn"]["started_at"]
    for field in [
        "input_first_sequence",
        "input_last_sequence",
        "context_last_sequence",
        "input_revision",
        "authority_revision",
    ]:
        assert recovered["turn"][field] == turn[field]
    for suffix, extra in [
        ("renew", {"lease_seconds": 30}),
        ("complete", {"outcome": "NO_RESPONSE"}),
        ("fail", {"failure_code": "TIMEOUT"}),
    ]:
        error(
            await h.call("POST", f"/turns/{turn['id']}/{suffix}", dict(h.proof(initial), **extra)),
            "STALE_CLAIM",
        )
    async with sql.connect() as connection:
        digest = await connection.scalar(
            text("SELECT claim_token_digest FROM agent_turns WHERE id=:id"), {"id": turn["id"]}
        )
    import hashlib

    assert digest == hashlib.sha256(recovered["claim_token"].encode()).hexdigest()


@pytest.mark.parametrize(
    "suffix,extra",
    [
        ("renew", {"lease_seconds": 30}),
        ("complete", {"outcome": "NO_RESPONSE"}),
        ("fail", {"failure_code": "TIMEOUT"}),
    ],
)
async def test_expired_owner_rejected_even_without_takeover(h, sql, suffix, extra):
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    await expire(sql, turn["id"])
    error(
        await h.call("POST", f"/turns/{turn['id']}/{suffix}", dict(h.proof(claim), **extra)),
        "LEASE_EXPIRED",
    )
    read = (await h.call("GET", f"/turns/{turn['id']}")).json()
    assert read["status"] == "CLAIMED" and read["claim_epoch"] == 1


@pytest.mark.parametrize(
    "suffix,result",
    [
        ("complete", {"outcome": "DRAFT_READY", "draft_text": "answer"}),
        ("complete", {"outcome": "NO_RESPONSE"}),
        ("fail", {"failure_code": "PROCESSING_ERROR"}),
    ],
)
async def test_terminal_winning_replay_expiry_changed_result_wrong_proof(h, sql, suffix, result):
    turn, _ = await h.select(await h.ingest())
    claim = await h.claim(turn)
    refs = [
        {"origin": "AI_AGENT_SERVICE", "operation_id": "operation-b", "outcome": "UNKNOWN"},
        {"origin": "AI_AGENT_SERVICE", "operation_id": "operation-a", "outcome": "SUCCEEDED"},
    ]
    payload = dict(h.proof(claim), operation_refs=refs, **result)
    response = await h.call("POST", f"/turns/{turn['id']}/{suffix}", payload)
    assert response.status_code == 200 and not response.json()["replayed"]
    saved = response.json()["turn"]
    assert [ref["operation_id"] for ref in saved["operation_refs"]] == [
        "operation-a",
        "operation-b",
    ]
    await expire(sql, turn["id"])
    replay = await h.call(
        "POST", f"/turns/{turn['id']}/{suffix}", dict(payload, operation_refs=list(reversed(refs)))
    )
    assert replay.status_code == 200 and replay.json()["replayed"]
    assert replay.json()["turn"]["id"] == saved["id"]
    assert replay.json()["turn"]["completed_at"] == saved["completed_at"]
    error(
        await h.call("POST", f"/turns/{turn['id']}/{suffix}", dict(payload, operation_refs=[])),
        "TERMINAL_RESULT_CONFLICT",
    )
    error(
        await h.call("POST", f"/turns/{turn['id']}/{suffix}", dict(payload, claim_token="x" * 43)),
        "STALE_CLAIM",
    )
    error(
        await h.call(
            "POST", f"/turns/{turn['id']}/claim", dict(worker_ref="worker", lease_seconds=1)
        ),
        "TURN_TERMINAL",
    )
    messages = (await h.call("GET", f"/conversations/{turn['conversation_id']}/messages")).json()[
        "items"
    ]
    assert len(messages) == 1 and messages[0]["direction"] == "INBOUND"


async def test_control_same_mode_advances_stale_same_mode_rejected_and_claim_human_denied(h):
    turn, _ = await h.select(await h.ingest())
    path = f"/conversations/{turn['conversation_id']}/control"
    for revision, mode in enumerate(["AGENT", "HUMAN_PENDING", "HUMAN", "AGENT"]):
        response = await h.call(
            "POST", path, dict(expected_authority_revision=revision, control_mode=mode)
        )
        assert response.status_code == 200 and response.json()["authority_revision"] == revision + 1
        error(
            await h.call(
                "POST", path, dict(expected_authority_revision=revision, control_mode=mode)
            ),
            "AUTHORITY_REVISION_CONFLICT",
        )
        if mode in {"HUMAN", "HUMAN_PENDING"}:
            error(
                await h.call(
                    "POST", f"/turns/{turn['id']}/claim", dict(worker_ref="w", lease_seconds=30)
                ),
                "CONTROL_MODE_CONFLICT",
            )


async def test_displaced_owner_cannot_replay_winners_terminal_result(h, sql):
    turn, _ = await h.select(await h.ingest())
    old = await h.claim(turn)
    await expire(sql, turn["id"])
    winner = (
        await h.call("POST", f"/turns/{turn['id']}/claim", dict(worker_ref="new", lease_seconds=30))
    ).json()
    assert (
        await h.call(
            "POST", f"/turns/{turn['id']}/complete", dict(h.proof(winner), outcome="NO_RESPONSE")
        )
    ).status_code == 200
    error(
        await h.call(
            "POST", f"/turns/{turn['id']}/complete", dict(h.proof(old), outcome="NO_RESPONSE")
        ),
        "STALE_CLAIM",
    )


async def test_terminal_frees_active_slot_new_key_but_human_mode_rejects_new_selection(h):
    accepted = await h.ingest()
    turn, payload = await h.select(accepted)
    claim = await h.claim(turn)
    assert (
        await h.call(
            "POST", f"/turns/{turn['id']}/complete", dict(h.proof(claim), outcome="NO_RESPONSE")
        )
    ).status_code == 200
    new_turn, _ = await h.select(accepted)
    assert new_turn["id"] != turn["id"] and new_turn["status"] == "PENDING"
    assert (
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/control",
            dict(expected_authority_revision=0, control_mode="HUMAN"),
        )
    ).status_code == 200
    error(
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/turns",
            dict(payload, selection_key=str(uuid4()), expected_authority_revision=1),
        ),
        "CONTROL_MODE_CONFLICT",
    )
