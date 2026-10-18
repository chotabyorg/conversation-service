"""Failed managed turns remain failed while the caller can continue speaking."""

import asyncio
from uuid import UUID, uuid4

import pytest
from pydantic import SecretStr
from sqlalchemy import select

from models import AgentTurn, Conversation, Message, VoiceCall
from schemas.contracts import TurnClaim, TurnComplete, TurnCreate, TurnFail
from schemas.managed import ManagedTurnResult
from services import turns
from tests.test_voice import begin, fact, post, start
from tests.test_voice import vh as vh_fixture

vh = vh_fixture


def terminal(request, status):
    return ManagedTurnResult(
        **{name: getattr(request, name) for name in (
            "contract_version", "conversation_id", "turn_id", "agent_id", "input_revision",
            "authority_revision", "input_first_sequence", "input_last_sequence",
            "context_last_sequence",
        )},
        status=status, draft_text=None,
        error_code=("TOOL_EXECUTION_UNKNOWN" if status == "UNKNOWN" else
                    "MODEL_ERROR" if status == "FAILED" else None),
        operation_refs=([dict(origin="AI_AGENT_SERVICE", operation_id="retained-operation",
                              outcome="UNKNOWN")] if status == "UNKNOWN" else []),
    )


@pytest.mark.parametrize("status", ["FAILED", "UNKNOWN"])
@pytest.mark.parametrize("partial_mode", ["none", "candidate", "played"])
async def test_failure_is_spoken_once_without_rewriting_turn_or_ending_call(
    vh, status, partial_mode,
):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="Запишите меня")
    original = vh["port"].execute
    entered, release = asyncio.Event(), asyncio.Event()

    async def fail(prepared, binding, request_id="", on_partial=None, caller_phone=""):
        if partial_mode != "none":
            await on_partial("Проверяю данные. ")
            entered.set()
            await release.wait()
        return terminal(prepared.request, status)

    vh["port"].execute = fail
    task = asyncio.create_task(vh["worker"].run_once())
    if partial_mode != "none":
        await asyncio.wait_for(entered.wait(), 5)
        turn_id = (await post(vh, "responses/next")).json()["turn_id"]
        assert turn_id is not None
        if partial_mode == "played":
            partial = await begin(vh, turn_id)
            assert partial["is_partial"]
            assert (await fact(vh, partial)).status_code == 200
        release.set()
    assert await asyncio.wait_for(task, 5)
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    permission = await begin(vh, turn_id)
    assert permission["send"] and not permission["is_partial"]
    assert "Я на линии" in permission["text"]
    assert "Проверяю данные" not in permission["text"]
    if status == "UNKNOWN":
        assert "без проверки не буду" in permission["text"]
    else:
        assert permission["text"] == "Извините, произошёл технический сбой. Я на линии."
    assert not (await begin(vh, turn_id))["send"]
    for _ in range(2):
        assert (await fact(vh, permission)).status_code == 200
    assert (await post(vh, "responses/next")).json()["turn_id"] is None
    assert (await post(vh, "calls/heartbeat")).json()["state"] == "ACTIVE"
    async with vh["database"].session_factory() as session:
        turn = await session.get(AgentTurn, UUID(turn_id))
        assert turn.status == "FAILED" and turn.draft_text is None
        assert turn.failure_code == ("EXECUTION_UNKNOWN" if status == "UNKNOWN"
                                     else "PROCESSING_ERROR")
        if status == "UNKNOWN":
            assert turn.operation_refs == [dict(
                origin="AI_AGENT_SERVICE", operation_id="retained-operation", outcome="UNKNOWN",
            )]
        messages = list((await session.scalars(select(Message).order_by(Message.sequence))).all())
        assert [row.actor for row in messages] == ["CUSTOMER", "AGENT"]
        assert messages[-1].text == permission["text"]
        assert (await session.scalar(select(VoiceCall))).ended_at is None
    vh["port"].execute = original
    admitted = await post(vh, "utterances", sequence=2, text="Какие услуги доступны?")
    assert admitted.status_code == 200
    assert await vh["worker"].run_once()
    next_id = (await post(vh, "responses/next")).json()["turn_id"]
    assert next_id and next_id != turn_id
    assert (await begin(vh, next_id))["send"]


@pytest.mark.parametrize("fence", ["input", "authority", "end"])
async def test_failure_fallback_cannot_bypass_new_input_takeover_or_call_end(vh, fence):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="Запишите меня")

    async def fail(prepared, *args, **kwargs):
        return terminal(prepared.request, "UNKNOWN")

    vh["port"].execute = fail
    assert await vh["worker"].run_once()
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    if fence == "input":
        await post(vh, "utterances", sequence=2, text="Другой вопрос")
    elif fence == "end":
        await post(vh, "calls/end", reason="DISCONNECTED")
    else:
        async with vh["database"].session_factory() as session, session.begin():
            conversation = await session.scalar(select(Conversation))
            conversation.authority_revision += 1
    permission = await begin(vh, turn_id)
    assert not permission["send"] and permission["text"] is None


async def test_partial_candidate_that_finishes_without_response_does_not_fail_boundary(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="Спасибо")
    entered, release = asyncio.Event(), asyncio.Event()

    async def finish(prepared, binding, request_id="", on_partial=None, caller_phone=""):
        await on_partial("Промежуточный текст.")
        entered.set()
        await release.wait()
        return terminal(prepared.request, "NO_RESPONSE")

    vh["port"].execute = finish
    task = asyncio.create_task(vh["worker"].run_once())
    await asyncio.wait_for(entered.wait(), 5)
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    release.set()
    assert await asyncio.wait_for(task, 5)
    permission = await begin(vh, turn_id)
    assert not permission["send"] and permission["text"] is None
    assert (await post(vh, "calls/heartbeat")).json()["state"] == "ACTIVE"


@pytest.mark.parametrize("status", ["FAILED", "COMPLETED"])
async def test_generic_turn_without_voice_projection_cannot_block_response_queue(vh, status):
    """A generic terminal turn must not hide the next valid Voice response."""
    await start(vh)
    await post(vh, "utterances", sequence=1, text="Первый вопрос")
    database, principal = vh["database"], vh["worker"].principal
    async with database.session_factory() as session:
        conversation = await session.scalar(select(Conversation))
    selected = await turns.create_turn(database, principal, conversation.id, TurnCreate(
        selection_key=uuid4(), input_first_sequence=1, input_last_sequence=1,
        expected_input_revision=conversation.input_revision,
        expected_authority_revision=conversation.authority_revision,
    ))
    claim = await turns.claim_turn(database, principal, selected.turn.id, TurnClaim(
        worker_ref="generic-worker", lease_seconds=30,
    ))
    proof = dict(claim_epoch=claim.turn.claim_epoch, claim_token=SecretStr(claim.claim_token))
    result = (TurnFail(**proof, failure_code="PROCESSING_ERROR") if status == "FAILED" else
              TurnComplete(**proof, outcome="DRAFT_READY", draft_text="Generic answer"))
    await turns.finish_turn(database, principal, selected.turn.id, result)
    assert (await post(vh, "responses/next")).json()["turn_id"] is None
    assert (await post(vh, "utterances", sequence=2, text="Следующий вопрос")).status_code == 200
    assert await vh["worker"].run_once()
    next_id = (await post(vh, "responses/next")).json()["turn_id"]
    assert next_id and next_id != str(selected.turn.id)
    permission = await begin(vh, next_id)
    assert permission["send"] and permission["text"] != "Generic answer"
    assert (await fact(vh, permission)).status_code == 200
    assert (await post(vh, "calls/heartbeat")).json()["state"] == "ACTIVE"
