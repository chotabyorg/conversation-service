"""Current v3 Handoff uses the same immutable catch-up/input proof as text."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update

from models import AgentTurn, Conversation, HandoffCase, TelegramAdmission
from schemas.managed_v3 import HandoffControlRequest, ManagedTurnV3Result
from services import telegram
from services.managed_client import handoff_request_id
from services.telegram_worker import TelegramWorker
from tests.test_catch_up import add, enable
from tests.test_catch_up import telegram_harness as telegram_fixture

telegram_harness = telegram_fixture


class PausedHandoffPort:
    def __init__(self):
        self.requests = []
        self.started, self.release = asyncio.Event(), asyncio.Event()

    async def execute_v3(self, prepared, binding, request_id=""):
        request = prepared.request
        self.requests.append(request)
        self.started.set()
        await self.release.wait()
        echoes = request.model_dump(include={
            "contract_version", "conversation_id", "turn_id", "agent_id", "input_revision",
            "authority_revision", "input_first_sequence", "input_last_sequence",
            "context_last_sequence",
        })
        return ManagedTurnV3Result(
            **echoes,
            original_dispatch_id=prepared.original_dispatch_id,
            request_fingerprint=prepared.request_fingerprint,
            result_kind="PLATFORM_CONTROL_REQUEST",
            processing_outcome="NO_RESPONSE",
            draft_text=None,
            error_code=None,
            operation_refs=[],
            control_request=HandoffControlRequest(
                type="HUMAN_HANDOFF", request_id=handoff_request_id(request),
                reason="CUSTOMER_REQUEST", note=None,
            ),
        )


@pytest.mark.parametrize("drift", ["none", "new_input", "admission_identity"])
async def test_grouped_v3_handoff_requires_current_input_and_full_member_proof(
    telegram_harness, sql, drift
):
    h = telegram_harness
    settings = enable(h)
    binding = settings.managed_ai_bindings[0]
    settings = settings.model_copy(update={
        "handoff_cohort_integration_ids": (binding.integration_id,),
    })
    h["settings"], h["app"].state.settings = settings, settings
    async with h["database"].session_factory() as session, session.begin():
        scope = await telegram._locked_handoff_scope_values(
            session, binding.owner_id, binding.integration_id, binding.agent_id)
        scope.account_id = scope.agent_owner_id = uuid4()
        scope.cp_binding_generation = scope.agent_lifecycle_generation = 1
        scope.grant_generation, scope.active = 1, True
    for n in range(3):
        await add(h, sql, n)
    port = PausedHandoffPort()
    worker = TelegramWorker(h["database"], settings, port)
    task = asyncio.create_task(worker.run_once())
    try:
        await asyncio.wait_for(port.started.wait(), 8)
        request = port.requests[0]
        assert request.contract_version == 3
        assert (request.input_first_sequence, request.input_last_sequence) == (1, 3)
        assert len(request.selected_input_message_ids) == 3
        async with h["database"].session_factory() as session:
            turn = await session.get(AgentTurn, request.turn_id)
            assert turn is not None
            assert turn.catch_up_version == 1 and turn.managed_contract_version == 3
        if drift == "new_input":
            await add(h, sql, 3)
        elif drift == "admission_identity":
            async with sql.begin() as connection:
                await connection.execute(update(TelegramAdmission).where(
                    TelegramAdmission.message_id == "8").values(update_id=9999))
    finally:
        port.release.set()
        await task
    assert task.result()
    async with h["database"].session_factory() as session:
        conversation = await session.scalar(select(Conversation))
        turn = await session.get(AgentTurn, request.turn_id)
        cases = await session.scalar(select(func.count()).select_from(HandoffCase))
        assert conversation is not None and turn is not None
        assert turn.status == "COMPLETED" and turn.outcome == "NO_RESPONSE"
        assert conversation.control_mode == ("HUMAN" if drift == "none" else "AGENT")
        assert cases == (1 if drift == "none" else 0)
        assert conversation.authority_revision == (1 if drift == "none" else 0)
    if drift == "none":
        assert await worker.run_once() is False
