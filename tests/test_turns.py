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


async def test_completed_turn_owns_input_and_different_selection_is_atomic_conflict(h, sql):
    accepted = await h.ingest()
    turn, _ = await h.select(accepted)
    claim = await h.claim(turn)
    completed = await h.call(
        "POST", f"/turns/{turn['id']}/complete",
        dict(h.proof(claim), outcome="NO_RESPONSE"),
    )
    assert completed.status_code == 200

    competing = dict(
        selection_key=str(uuid4()), input_first_sequence=1, input_last_sequence=1,
        expected_input_revision=1, expected_authority_revision=0,
    )
    response = await h.call(
        "POST", f"/conversations/{turn['conversation_id']}/turns", competing
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "TURN_SELECTION_CONFLICT"
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM agent_turns")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM turn_input_messages")) == 1


async def test_partially_owned_multimessage_range_rolls_back_second_turn(h, sql):
    await h.ingest()
    second = await h.ingest()
    turn, _ = await h.select(second, input_first_sequence=1, input_last_sequence=1)
    claim = await h.claim(turn)
    assert (await h.call(
        "POST", f"/turns/{turn['id']}/complete",
        dict(h.proof(claim), outcome="NO_RESPONSE"),
    )).status_code == 200

    conversation = second["conversation"]
    response = await h.call(
        "POST", f"/conversations/{conversation['id']}/turns",
        dict(selection_key=str(uuid4()), input_first_sequence=1, input_last_sequence=2,
             expected_input_revision=2, expected_authority_revision=0),
    )
    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "TURN_SELECTION_CONFLICT"
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM agent_turns")) == 1
        assert await connection.scalar(text(
            "SELECT count(*) FROM turn_input_messages")) == 1


async def test_concurrent_competing_turn_creates_keep_one_complete_mapping(h, sql):
    import asyncio

    accepted = await h.ingest()
    conversation_id = accepted["conversation"]["id"]
    base = dict(input_first_sequence=1, input_last_sequence=1,
                expected_input_revision=1, expected_authority_revision=0)
    responses = await asyncio.gather(*(
        h.call("POST", f"/conversations/{conversation_id}/turns",
               dict(base, selection_key=str(uuid4())))
        for _ in range(2)
    ))
    assert sorted(response.status_code for response in responses) == [201, 409]
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM agent_turns")) == 1
        assert await connection.scalar(text("SELECT count(*) FROM turn_input_messages")) == 1


async def test_actual_turn_input_primary_key_conflict_is_translated_after_rollback(h, sql):
    accepted = await h.ingest()
    conversation_id = accepted["conversation"]["id"]
    function_name = "goal5_inject_duplicate_turn_input_owner_fn"
    trigger_name = "goal5_inject_duplicate_turn_input_owner"
    async with sql.connect() as connection:
        primary_key = await connection.scalar(text(
            "SELECT conname FROM pg_constraint WHERE conrelid='turn_input_messages'::regclass "
            "AND contype='p'"
        ))
    assert primary_key == "pk_turn_input_messages"
    async with sql.begin() as connection:
        await connection.execute(text(f"""
            CREATE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF pg_trigger_depth() = 1 THEN
                INSERT INTO turn_input_messages(message_id,conversation_id,turn_id,sequence,ordinal)
                VALUES(NEW.message_id,NEW.conversation_id,NEW.turn_id,NEW.sequence,NEW.ordinal);
              END IF;
              RETURN NEW;
            END $$
        """))
        await connection.execute(text(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT ON turn_input_messages "
            f"FOR EACH ROW EXECUTE FUNCTION {function_name}()"
        ))
    try:
        response = await h.call(
            "POST", f"/conversations/{conversation_id}/turns",
            dict(selection_key=str(uuid4()), input_first_sequence=1, input_last_sequence=1,
                 expected_input_revision=1, expected_authority_revision=0),
        )
        assert response.status_code == 409, response.text
        assert response.json()["detail"]["code"] == "TURN_SELECTION_CONFLICT"
    finally:
        async with sql.begin() as connection:
            await connection.execute(text(
                f"DROP TRIGGER IF EXISTS {trigger_name} ON turn_input_messages"
            ))
            await connection.execute(text(f"DROP FUNCTION IF EXISTS {function_name}()"))

    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM agent_turns")) == 0
        assert await connection.scalar(text("SELECT count(*) FROM turn_input_messages")) == 0


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

    error(
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/turns",
            dict(payload, selection_key=str(uuid4())),
        ),
        "TURN_SELECTION_CONFLICT",
    )
    second = await h.ingest()
    new_turn, _ = await h.select(second, input_first_sequence=2, input_last_sequence=2)
    assert new_turn["id"] != turn["id"] and new_turn["status"] == "PENDING"
    assert (
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/control",
            dict(expected_authority_revision=0, control_mode="HUMAN"),
        )
    ).status_code == 200
    third = await h.ingest()
    error(
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/turns",
            dict(
                selection_key=str(uuid4()),
                input_first_sequence=3,
                input_last_sequence=3,
                expected_input_revision=third["conversation"]["input_revision"],
                expected_authority_revision=1,
            ),
        ),
        "CONTROL_MODE_CONFLICT",
    )
