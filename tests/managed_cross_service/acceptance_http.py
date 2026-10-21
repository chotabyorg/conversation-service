"""Explicit paired gate; filename intentionally outside default service collection."""

import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from sqlalchemy import text

from schemas.managed import EXECUTION_NAMESPACE
from tests.managed_cross_service.conftest import unknown
from tests.managed_cross_service.fake_http import call, completion, descriptor

SENTINEL = "SYNTHETIC_SECRET_LOST_REPLY_DO_NOT_FORWARD"


def draft(response, expected="Fixture draft"):
    assert response.status_code == 200
    turn = response.json()["turn"]
    assert turn["status"] == "COMPLETED" and turn["outcome"] == "DRAFT_READY"
    assert turn["draft_text"] == expected and turn["operation_refs"] == []
    return turn


async def assert_separated(cross, conversation_id, before):
    assert await cross.snapshot(conversation_id) == before
    assert before[2:4] == (0, 0)
    assert cross.observations().get("managed_forbidden_store", 0) == 0


async def _create_website_turn(cross, *, pre_stream_profile=False):
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from models import (
        AgentTurn,
        Conversation,
        Message,
        TurnInputMessage,
        WebsiteAdmission,
        WebsiteImmediateTurn,
    )
    from schemas.managed import ManagedMessage, ManagedTurnRequest
    from services.common import db_clock, fingerprint
    from services.history_projection import freeze_website_projection

    owner_id = cross.grants["a"]["owner_id"]
    integration_id = cross.ids["integration_a"]
    agent_id = cross.ids["agent_a"]
    session_id, message_id = str(uuid4()), str(uuid4())
    admission_response = await cross.request("POST", "/website/admissions", {
        "owner_id": owner_id,
        "integration_id": integration_id,
        "agent_id": agent_id,
        "session_id": session_id,
        "message_id": message_id,
        "text": "Historical Website input",
        "desired_route": "MANAGED",
    })
    assert admission_response.status_code == 200
    admission_id = admission_response.json()["admission_id"]
    async_session = async_sessionmaker(
        bind=cross.sql, class_=AsyncSession, expire_on_commit=False
    )
    async with async_session() as session, session.begin():
        admission = await session.get(WebsiteAdmission, admission_id)
        assert admission is not None and admission.route == "MANAGED"
        conversation = await session.get(Conversation, admission.conversation_id)
        message = await session.get(Message, admission.canonical_message_id)
        assert conversation is not None and message is not None
        clock = await db_clock(session)
        fields = {
            "contract_version": 1,
            "conversation_id": str(conversation.id),
            "selection_key": str(message.id),
            "input_first_sequence": message.sequence,
            "input_last_sequence": message.sequence,
            "context_last_sequence": message.sequence,
            "expected_input_revision": conversation.input_revision,
            "expected_authority_revision": conversation.authority_revision,
        }
        turn = AgentTurn(
            id=uuid4(), conversation_id=conversation.id, selection_key=message.id,
            creation_fingerprint=fingerprint(fields),
            input_first_sequence=message.sequence, input_last_sequence=message.sequence,
            context_last_sequence=message.sequence, input_revision=conversation.input_revision,
            authority_revision=conversation.authority_revision,
            settling_revision=conversation.settling_revision, settling_burst_id=None,
            status="PENDING", claim_epoch=0, operation_refs=[], created_at=clock,
        )
        await freeze_website_projection(session, conversation, turn, admission.agent_id)
        historical_digest = None
        if pre_stream_profile:
            legacy_request = ManagedTurnRequest(
                contract_version=turn.managed_contract_version,
                conversation_id=conversation.id,
                turn_id=turn.id,
                owner_id=conversation.owner_id,
                integration_id=conversation.integration_id,
                agent_id=admission.agent_id,
                channel=conversation.channel,
                input_revision=turn.input_revision,
                authority_revision=turn.authority_revision,
                input_first_sequence=turn.input_first_sequence,
                input_last_sequence=turn.input_last_sequence,
                context_last_sequence=turn.context_last_sequence,
                messages=(ManagedMessage.model_validate(message),),
                selected_input_message_ids=(message.id,),
                stream_partials=False,
                max_partial_chars=2000,
            )
            fields = legacy_request.model_dump(mode="json")
            fields.pop("stream_partials")
            fields.pop("max_partial_chars")
            identity = json.dumps(
                {"execution_namespace": EXECUTION_NAMESPACE, "request": fields},
                sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
            ).encode("utf-8")
            historical_digest = hashlib.sha256(identity).hexdigest()
            # Seed the old immutable hash before the insert; no trigger or
            # persisted snapshot is changed after creation.
            turn.managed_projection_fingerprint = historical_digest
        session.add(turn)
        await session.flush()
        session.add(WebsiteImmediateTurn(
            message_id=message.id, conversation_id=conversation.id, turn_id=turn.id,
        ))
        session.add(TurnInputMessage(
            message_id=message.id, conversation_id=conversation.id, turn_id=turn.id,
            sequence=message.sequence, ordinal=0,
        ))
        turn_id = str(turn.id)
    read_response = await cross.request("GET", f"/turns/{turn_id}")
    assert read_response.status_code == 200
    return read_response.json(), historical_digest


async def test_A_simple_same_turn_draft_and_terminal_replay(cross):
    from tests.managed_cross_service.conftest import private_json

    cross.model.queue(completion("<thinking>private reasoning</thinking> Готово"))
    accepted, claim = await cross.selected()
    before = await cross.snapshot(accepted["conversation"]["id"])
    redis_before = cross.read("redis.json")
    for identifier in [accepted["conversation"]["id"], claim["turn"]["id"]]:
        redis_before["chat_history:" + identifier] = {
            "value": '[{"role":"user","content":"POISON_NOT_CANONICAL"}]',
            "ttl": 999,
        }
    private_json(cross.root / "redis.json", redis_before)
    response = await cross.execute(claim)
    assert response.status_code == 200
    turn = response.json()["turn"]
    assert turn["id"] == claim["turn"]["id"]
    assert turn["status"] == "COMPLETED" and turn["outcome"] == "DRAFT_READY"
    assert turn["draft_text"] == "Готово" and turn["operation_refs"] == []
    assert turn["managed_agent_id"] == cross.ids["agent_a"]
    assert turn["managed_dispatch_epoch"] == 1
    observations = cross.observations()
    projection = observations["managed_projections"][0]
    projection_fields = observations["managed_projection_fields_set"][0]
    assert "stream_partials" in projection_fields and "max_partial_chars" in projection_fields
    assert projection["stream_partials"] is False
    assert projection["max_partial_chars"] == 2000
    for key in [
        "conversation_id",
        "input_revision",
        "authority_revision",
        "input_first_sequence",
        "input_last_sequence",
        "context_last_sequence",
    ]:
        assert projection[key] == claim["turn"][key]
    assert projection["turn_id"] == turn["id"]
    assert projection["messages"][0]["id"] == accepted["message"]["id"]
    assert projection["selected_input_message_ids"] == [accepted["message"]["id"]]
    read = await cross.request("GET", f"/turns/{turn['id']}")
    assert read.json() == turn
    replay = await cross.execute(claim)
    assert replay.json() == {"turn": turn, "replayed": True}
    assert len(cross.model.calls) == cross.observations()["managed_http_requests"] == 1
    await assert_separated(cross, turn["conversation_id"], before)
    assert cross.read("redis.json") == redis_before
    assert "POISON_NOT_CANONICAL" not in json.dumps(cross.model.calls)
    assert cross.observations()["guard_denials"] == 2
    assert cross.read("cs-guard.json")["denials"] == 2
    cross.safe_logs(SENTINEL)


async def test_current_website_v2_profile_crosses_real_cs_ai_http_and_replays(cross):
    from schemas.managed import ManagedTurnRequest, prepare_request

    cross.model.queue(completion("Current Website profile draft"))
    turn, historical_digest = await _create_website_turn(cross)
    assert historical_digest is None
    claim = await cross.claim(turn)

    response = await cross.execute(claim)
    result = draft(response, "Current Website profile draft")
    observations = cross.observations()
    assert observations["managed_http_requests"] == 1
    assert len(observations["managed_projections"]) == 1

    projection = observations["managed_projections"][0]
    assert projection["channel"] == "WEBSITE_WIDGET"
    assert projection["contract_version"] == 2
    assert projection["stream_partials"] is False
    assert projection["max_partial_chars"] == 2000
    assert {"stream_partials", "max_partial_chars"} <= set(
        observations["managed_projection_fields_set"][0]
    )

    prepared = prepare_request(ManagedTurnRequest.model_validate(projection))
    expected_body_hash = hashlib.sha256(prepared.body).hexdigest()
    async with cross.sql.connect() as connection:
        retained_hash = await connection.scalar(text(
            "SELECT managed_projection_fingerprint FROM agent_turns WHERE id=:id"
        ), {"id": claim["turn"]["id"]})
        contract_version = await connection.scalar(text(
            "SELECT managed_contract_version FROM agent_turns WHERE id=:id"
        ), {"id": claim["turn"]["id"]})
    assert contract_version == 2
    assert retained_hash == prepared.fingerprint
    assert retained_hash == result["managed_request_fingerprint"]
    assert observations["managed_request_body_sha256"] == [expected_body_hash]
    assert observations["managed_request_canonical_sha256"] == [expected_body_hash]
    assert observations["managed_request_body_sizes"] == [{
        "raw": len(prepared.body), "canonical": len(prepared.body),
    }]
    assert cross.model.calls[0].get("stream", False) is False

    replay = await cross.execute(claim)
    assert replay.json() == {"turn": result, "replayed": True}
    assert cross.observations()["managed_http_requests"] == 1
    assert len(cross.model.calls) == 1


async def test_historical_pre_stream_projection_crosses_real_cs_ai_http_after_restart(cross):
    cross.model.queue(completion("Legacy-profile draft"))
    turn, legacy_fingerprint = await _create_website_turn(cross, pre_stream_profile=True)
    claim = await cross.claim(turn)

    # A fresh Conversation worker restores the persisted profile. It must send
    # the exact pre-stream JSON body and the real AI process must parse both
    # extension keys as omitted before executing the request.
    await cross.restart_cs()
    response = await cross.execute(claim)
    result = draft(response, "Legacy-profile draft")
    assert result["managed_request_fingerprint"] == legacy_fingerprint
    observations = cross.observations()
    assert observations["managed_http_requests"] == 1
    field_sets = observations["managed_projection_fields_set"]
    assert len(field_sets) == 1
    assert "stream_partials" not in field_sets[0]
    assert "max_partial_chars" not in field_sets[0]

    replay = await cross.execute(claim)
    assert replay.json() == {"turn": result, "replayed": True}
    assert cross.observations()["managed_http_requests"] == 1


@pytest.mark.parametrize("selection", [(3, 3), (1, 1), (1, 2)])
async def test_B_canonical_order_suffix_nonsuffix_multiple(cross, selection):
    cross.model.queue(completion())
    accepted = await cross.ingest("Меня зовут Влад", thread="history")
    await cross.historical(accepted["conversation"]["id"], "AGENT", "Приятно познакомиться")
    latest = await cross.ingest("Как меня зовут?", thread="history")
    turn = await cross.turn(latest, *selection)
    claim = await cross.claim(turn)
    before = await cross.snapshot(turn["conversation_id"])
    draft(await cross.execute(claim))
    model_messages = cross.model.calls[0]["messages"]
    assert model_messages[-3:] == [
        {"role": "user", "content": "Меня зовут Влад"},
        {"role": "assistant", "content": "Приятно познакомиться"},
        {"role": "user", "content": "Как меня зовут?"},
    ]
    selectors = [
        message["content"]
        for message in model_messages
        if message["role"] == "system" and "communication positions" in message["content"]
    ]
    if selection == (3, 3):
        assert selectors == []
    else:
        assert len(selectors) == 1
        expected = "1, 2" if selection == (1, 2) else "1"
        assert f"positions {expected}." in selectors[0]
    expected_query = "\n\n".join(row["text"] for row in before[0][selection[0] - 1 : selection[1]])
    assert cross.kb.calls[0]["query"] == expected_query
    assert cross.kb.calls[0]["owner_id"] == cross.ids["owner_a"]
    projection = cross.observations()["managed_projections"][0]
    assert projection["messages"] == [
        {key: row[key] for key in ["id", "sequence", "actor", "direction", "content_type", "text"]}
        for row in json.loads(json.dumps(before[0], default=str))
    ]
    await assert_separated(cross, turn["conversation_id"], before)
    assert "POISON_NOT_CANONICAL" not in json.dumps(cross.model.calls)


async def test_B_human_system_roles_and_frozen_prefix_during_inflight(cross):
    cross.model.queue(completion(), barrier=True)
    accepted = await cross.ingest("Initial", thread="frozen")
    conversation_id = accepted["conversation"]["id"]
    await cross.historical(conversation_id, "HUMAN", "Operator history")
    await cross.historical(conversation_id, "SYSTEM", "Unprivileged event")
    claim = await cross.claim(await cross.turn(accepted, 2, 3))
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        # This real HTTP ingest proves no Conversation row lock spans the AI await.
        later = await asyncio.wait_for(cross.ingest("Later excluded", thread="frozen"), 2)
        assert later["message"]["sequence"] == 4
        cross.model.release.set()
        draft(await running)
    finally:
        cross.model.release.set()
        if not running.done():
            await running
    assert cross.model.calls[0]["messages"][-3:] == [
        {"role": "user", "content": "Initial"},
        {"role": "assistant", "content": "Operator history"},
        {"role": "user", "content": "[System communication event]\nUnprivileged event"},
    ]
    projection = cross.observations()["managed_projections"][0]
    assert projection["context_last_sequence"] == 3 and len(projection["messages"]) == 3


def kb_context():
    return {
        "contexts": [
            {
                "chunk_id": str(uuid4()),
                "document_id": str(uuid4()),
                "knowledge_base_id": str(uuid4()),
                "content": "Company fixture fact",
                "score": 0.9,
                "source_filename": "fixture.txt",
                "chunk_index": 1,
                "metadata": {"private": SENTINEL},
            }
        ],
        "total": 1,
    }


@pytest.mark.parametrize("mode", ["no_tool", "kb", "tools"])
async def test_D_E_F_legacy_managed_deepcopy_golden_parity(cross, mode):
    if mode == "kb":
        context = kb_context()
        cross.kb.scripts.clear()
        cross.kb.queue(context, context)
    if mode == "tools":
        cross.scenario(
            tools={"bitrix": [descriptor("read_fixture")]},
            results={
                "read_fixture": {
                    "content": [{"type": "text", "text": '{"value":42}'}],
                    "isError": False,
                }
            },
        )
    responses = (
        [completion(None, [call("read_fixture")]), completion("Final fixture")]
        if mode == "tools"
        else [completion("Final fixture")]
    )
    cross.model.queue(*responses, *responses)
    history = [
        {"role": "user", "content": "Old input"},
        {"role": "assistant", "content": "Old answer"},
    ]
    # Create/reuse the real legacy Session and real history adapter first.
    legacy_payload = {
        "agent_id": cross.ids["agent_a"],
        "external_client_id": "fixture-legacy",
        "channel": "telegram",
        "message": "Current input",
    }
    async with cross.ai_sql.begin() as connection:
        from sqlalchemy import text

        session_id = str(uuid4())
        await connection.execute(
            text(
                "INSERT INTO conversation_sessions "
                "(id,agent_id,external_client_id,channel) VALUES(:id,:agent,:external,'telegram')"
            ),
            {"id": session_id, "agent": cross.ids["agent_a"], "external": "fixture-legacy"},
        )
    redis = cross.read("redis.json")
    redis["chat_history:" + session_id] = {"value": json.dumps(history), "ttl": 100}
    from tests.managed_cross_service.conftest import private_json

    private_json(cross.root / "redis.json", redis)
    legacy = await cross.client.post(
        cross.url("ai") + "/v1/chat/completions",
        json=legacy_payload,
        headers={"X-Service-Token": cross.tokens["chat"], "X-User-Id": cross.ids["owner_a"]},
    )
    assert legacy.status_code == 200 and legacy.json() == {
        "session_id": session_id,
        "answer": "Final fixture",
    }
    saved = cross.read("redis.json")["chat_history:" + session_id]
    assert json.loads(saved["value"]) == history + [
        {"role": "user", "content": "Current input"},
        {"role": "assistant", "content": "Final fixture"},
    ]
    assert saved["ttl"] == 86400
    legacy_captures = json.loads(json.dumps(cross.model.calls))
    accepted = await cross.ingest("Old input", thread="equivalent")
    await cross.historical(accepted["conversation"]["id"], "AGENT", "Old answer")
    current = await cross.ingest("Current input", thread="equivalent")
    claim = await cross.claim(await cross.turn(current, 3, 3))
    before = await cross.snapshot(current["conversation"]["id"])
    redis_before = cross.read("redis.json")
    draft(await cross.execute(claim), "Final fixture")
    assert cross.model.calls[len(responses) :] == legacy_captures
    assert legacy_captures[0]["messages"][-1] == {"role": "user", "content": "Current input"}
    if mode == "tools":
        assert not any(message["role"] == "tool" for message in legacy_captures[0]["messages"])
        assert any(message["role"] == "tool" for message in legacy_captures[1]["messages"])
    if mode == "kb":
        assert "Company fixture fact" in legacy_captures[0]["messages"][1]["content"]
        assert SENTINEL not in json.dumps(cross.model.calls)
    await assert_separated(cross, current["conversation"]["id"], before)
    assert cross.read("redis.json") == redis_before


@pytest.mark.parametrize("kb_failure", ["http", "format", "timeout"])
async def test_E_existing_kb_fail_open_safe_logs(cross, kb_failure):
    cross.kb.scripts.clear()
    cross.kb.queue(
        {"error": SENTINEL},
        status=503 if kb_failure == "http" else 200,
        barrier=kb_failure == "timeout",
    )
    cross.model.queue(completion())
    accepted, claim = await cross.selected()
    draft(await cross.execute(claim))
    assert len(cross.model.calls[0]["messages"]) == 2
    assert SENTINEL not in json.dumps(cross.model.calls)
    cross.safe_logs(SENTINEL)


async def test_C_stale_real_inflight_recovery_unknown_without_second_ai(cross):
    cross.model.queue(completion("Late draft must not win"), barrier=True)
    accepted, claim = await cross.selected()
    before = await cross.snapshot(accepted["conversation"]["id"])
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        duplicate = await cross.execute(claim)
        assert duplicate.status_code == 409
        assert duplicate.json()["detail"]["code"] == "MANAGED_EXECUTION_STARTED"
        await cross.expire(claim["turn"]["id"])
        recovered = await cross.claim(claim["turn"])
        assert recovered["turn"]["id"] == claim["turn"]["id"]
        assert recovered["turn"]["claim_epoch"] == 2
        winning = unknown(await cross.execute(recovered))
        cross.model.release.set()
        stale = await running
        assert stale.status_code == 409 and stale.json()["detail"]["code"] == "STALE_CLAIM"
        read = await cross.request("GET", f"/turns/{winning['id']}")
        assert read.json() == winning
        assert cross.observations()["managed_http_requests"] == len(cross.model.calls) == 1
        await assert_separated(cross, winning["conversation_id"], before)
    finally:
        cross.model.release.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_C_expiry_without_replacement_retains_marker(cross):
    cross.model.queue(completion("Expired draft"), barrier=True)
    _, claim = await cross.selected()
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        await cross.expire(claim["turn"]["id"])
        cross.model.release.set()
        response = await running
        assert response.status_code == 409 and response.json()["detail"]["code"] == "LEASE_EXPIRED"
        persisted = (await cross.request("GET", f"/turns/{claim['turn']['id']}")).json()
        assert persisted["status"] == "CLAIMED" and persisted["draft_text"] is None
        assert persisted["managed_dispatch_epoch"] == 1
        unknown(await cross.execute(await cross.claim(claim["turn"])))
        assert cross.observations()["managed_http_requests"] == 1
    finally:
        cross.model.release.set()
        await asyncio.gather(running, return_exceptions=True)


ERROR_FORMS = [
    "bitrix-structured",
    "bitrix-text",
    "bitrix-singleton",
    "bitrix-wrapper",
    "google-structured",
    "google-text",
    "google-wrapper",
    "conflict",
    "protocol",
]


def lost_result(form):
    family = "google_workspace" if form.startswith("google") else "bitrix"
    envelope = (
        {"ok": False, "error": {"message": SENTINEL, "recoverable": True}}
        if family == "google_workspace"
        else {"error": SENTINEL}
    )
    if "singleton" in form:
        envelope = [envelope]
    if "wrapper" in form:
        envelope = {"result": envelope}
    result = {"content": [{"type": "text", "text": json.dumps(envelope)}], "isError": False}
    if "structured" in form:
        result["structuredContent"] = envelope
        result["content"] = [{"type": "text", "text": '{"ok":true}'}]
    if form == "conflict":
        result["structuredContent"] = {"ok": True}
    if form == "protocol":
        result["isError"] = True
    return family, result


@pytest.mark.parametrize("form", ERROR_FORMS)
async def test_F_G_R1_application_errors_stop_before_any_next_model_or_tool(cross, form):
    family, result = lost_result(form)
    cross.scenario(
        tools={family: [descriptor("mutate_fixture"), descriptor("mutate_second")]},
        results={"mutate_fixture": result},
    )
    cross.model.queue(
        completion(
            None,
            [call("mutate_fixture", '{"value":1}'), call("mutate_second", "{}", "second-tool")],
        ),
        completion(None, [call("mutate_fixture", '{ "value" : 1 }')]),
        completion("Should not be consumed"),
    )
    accepted, claim = await cross.selected("Synthetic mutation")
    before = await cross.snapshot(accepted["conversation"]["id"])
    terminal = unknown(await cross.execute(claim))
    assert len(cross.model.calls) == 1 and len(cross.model.scripts) == 2
    observations = cross.observations()
    assert observations["effects"] == len(observations["tool_calls"]) == 1
    assert observations["manager_close"] == observations["manager_enter"]
    assert SENTINEL not in json.dumps(cross.model.calls)
    assert SENTINEL not in json.dumps(terminal)
    cross.safe_logs(SENTINEL)
    await cross.restart_cs()
    replay = await cross.execute(claim)
    assert replay.json()["turn"] == terminal and replay.json()["replayed"] is True
    assert cross.observations()["managed_http_requests"] == 1
    assert cross.observations()["effects"] == 1
    await assert_separated(cross, terminal["conversation_id"], before)


@pytest.mark.parametrize("family", ["bitrix", "google_workspace"])
async def test_D_R1_legacy_consumes_retry_and_original_error_text(cross, family):
    _, result = lost_result("google-text" if family == "google_workspace" else "bitrix-text")
    cross.scenario(
        tools={family: [descriptor("mutate_fixture")]}, results={"mutate_fixture": result}
    )
    cross.model.queue(
        completion(None, [call("mutate_fixture", '{"value":1}')]),
        completion(None, [call("mutate_fixture", '{ "value" : 1 }')]),
        completion("Legacy done"),
    )
    response = await cross.client.post(
        cross.url("ai") + "/v1/chat/completions",
        json={
            "agent_id": cross.ids["agent_a"],
            "external_client_id": "legacy-error",
            "channel": "telegram",
            "message": "Synthetic mutation",
        },
        headers={"X-Service-Token": cross.tokens["chat"], "X-User-Id": cross.ids["owner_a"]},
    )
    assert response.status_code == 200 and response.json()["answer"] == "Legacy done"
    assert len(cross.model.calls) == 3 and cross.observations()["effects"] == 2
    assert SENTINEL in json.dumps(cross.model.calls[1]["messages"])


async def test_F_successful_multistep_and_raw_call_dedupe(cross):
    cross.scenario(tools={"bitrix": [descriptor("mutate_fixture")]})
    cross.model.queue(
        completion(None, [call("mutate_fixture")]),
        completion(None, [call("mutate_fixture", identifier="repeat")]),
        completion(None, [call("mutate_fixture", '{"later":true}', "later")]),
        completion("Success"),
    )
    _, claim = await cross.selected()
    response = await cross.execute(claim)
    assert response.status_code == 200 and response.json()["turn"]["draft_text"] == "Success"
    assert cross.observations()["effects"] == 2
    assert len(cross.model.calls) == 4
    assert "identical tool call blocked" in cross.model.calls[2]["messages"][-1]["content"]


@pytest.mark.parametrize("failure", ["tool_exception", "model_after_tool"])
async def test_F_failure_after_effect_is_unknown_and_sentinel_safe(cross, failure):
    cross.scenario(
        tools={"bitrix": [descriptor("mutate_fixture")]},
        **({"tool_exception": SENTINEL} if failure == "tool_exception" else {}),
    )
    cross.model.queue(completion(None, [call("mutate_fixture")]))
    if failure == "model_after_tool":
        cross.model.queue({"error": {"message": SENTINEL, "type": "fixture"}}, status=500)
    _, claim = await cross.selected()
    unknown(await cross.execute(claim))
    assert cross.observations()["effects"] == 1
    assert len(cross.model.calls) == (1 if failure == "tool_exception" else 2)
    assert SENTINEL not in json.dumps(cross.model.calls)
    cross.safe_logs(SENTINEL)


@pytest.fixture
def cs_deadline(request):
    return 0.8 if "timeout" in request.node.name or "cancel" in request.node.name else 8.0


@pytest.mark.parametrize("operation", ["timeout", "cancel"])
async def test_G_timeout_or_disconnected_client_retains_no_retry_after_restart(cross, operation):
    cross.scenario(tools={"bitrix": [descriptor("mutate_fixture")]})
    cross.model.queue(completion(None, [call("mutate_fixture")]))
    cross.model.queue(completion("Held after synthetic effect"), barrier=True)
    accepted, claim = await cross.selected()
    before = await cross.snapshot(accepted["conversation"]["id"])
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        if operation == "cancel":
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
            deadline = asyncio.get_running_loop().time() + 4
            while True:
                terminal = (await cross.request("GET", f"/turns/{claim['turn']['id']}")).json()
                if terminal["status"] == "FAILED":
                    break
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.03)
            assert terminal["failure_code"] == "EXECUTION_UNKNOWN"
        else:
            terminal = unknown(await running)
        cross.model.release.set()
        await cross.restart_cs()
        replay = await cross.execute(claim)
        assert replay.json()["turn"] == terminal and replay.json()["replayed"] is True
        assert cross.observations()["managed_http_requests"] == 1
        assert cross.observations()["effects"] == 1
        assert len(cross.model.calls) == 2
        await assert_separated(cross, terminal["conversation_id"], before)
    finally:
        cross.model.release.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_G_abrupt_conversation_process_restart_recovers_retained_marker(cross):
    cross.scenario(tools={"bitrix": [descriptor("mutate_fixture")]})
    cross.model.queue(completion(None, [call("mutate_fixture")]))
    cross.model.queue(completion("Held after effect"), barrier=True)
    accepted, claim = await cross.selected()
    before = await cross.snapshot(accepted["conversation"]["id"])
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        await cross.restart_cs(abrupt=True)
        lost = await asyncio.gather(running, return_exceptions=True)
        assert isinstance(lost[0], Exception)
        retained = (await cross.request("GET", f"/turns/{claim['turn']['id']}")).json()
        assert retained["status"] == "CLAIMED" and retained["managed_dispatch_epoch"] == 1
        await cross.expire(retained["id"])
        recovered = await cross.claim(retained)
        winning = unknown(await cross.execute(recovered))
        assert winning["id"] == retained["id"] and winning["claim_epoch"] == 2
        assert winning["managed_request_fingerprint"] == retained["managed_request_fingerprint"]
        assert cross.observations()["managed_http_requests"] == 1
        assert cross.observations()["effects"] == 1
        await assert_separated(cross, winning["conversation_id"], before)
    finally:
        cross.model.release.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_G_R1_application_error_result_lost_at_real_pg_commit_wait(cross):

    from tests.conftest import observed_waiters

    _, result = lost_result("bitrix-text")
    cross.scenario(
        tools={"bitrix": [descriptor("mutate_fixture")]}, results={"mutate_fixture": result}
    )
    cross.model.queue(completion(None, [call("mutate_fixture", '{"value":1}')]), barrier=True)
    cross.model.queue(completion(None, [call("mutate_fixture", '{ "value" : 1 }')]))
    accepted, claim = await cross.selected()
    before = await cross.snapshot(accepted["conversation"]["id"])
    running = asyncio.create_task(cross.execute(claim))
    try:
        await cross.latch()
        # Freeze only the real CS result transaction, after the committed marker.
        async with cross.sql.begin() as blocker:
            pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
            await blocker.execute(
                text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                {"id": claim["turn"]["conversation_id"]},
            )
            cross.model.release.set()
            await observed_waiters(cross.sql, pid, 1, "real CS result commit after AI R1 UNKNOWN")
            assert cross.observations()["managed_results"][-1]["status"] == "UNKNOWN"
            assert (
                cross.observations()["managed_results"][-1]["error_code"]
                == "TOOL_EXECUTION_UNKNOWN"
            )
            await cross.stop("cs", abrupt=True)
        await cross.start("cs")
        await asyncio.gather(running, return_exceptions=True)
        retained = (await cross.request("GET", f"/turns/{claim['turn']['id']}")).json()
        assert retained["status"] == "CLAIMED" and retained["managed_dispatch_epoch"] == 1
        await cross.expire(retained["id"])
        recovered = await cross.claim(retained)
        terminal = unknown(await cross.execute(recovered))
        assert terminal["id"] == retained["id"] and terminal["claim_epoch"] == 2
        assert cross.observations()["managed_http_requests"] == 1
        assert cross.observations()["effects"] == len(cross.observations()["tool_calls"]) == 1
        assert len(cross.model.calls) == 1 and len(cross.model.scripts) == 1
        assert SENTINEL not in json.dumps(cross.model.calls)
        cross.safe_logs(SENTINEL)
        await assert_separated(cross, terminal["conversation_id"], before)
    finally:
        cross.model.release.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_H_owner_agent_scope_and_dedicated_token_boundary(cross):
    accepted = await cross.ingest()
    value = {
        "contract_version": 1,
        "conversation_id": accepted["conversation"]["id"],
        "turn_id": str(uuid4()),
        "owner_id": cross.grants["a"]["owner_id"],
        "integration_id": cross.ids["integration_a"],
        "agent_id": cross.ids["agent_a"],
        "channel": "TELEGRAM",
        "input_revision": 1,
        "authority_revision": 0,
        "input_first_sequence": 1,
        "input_last_sequence": 1,
        "context_last_sequence": 1,
        "messages": [
            {
                key: accepted["message"][key]
                for key in ["id", "sequence", "actor", "direction", "content_type", "text"]
            }
        ],
        "selected_input_message_ids": [accepted["message"]["id"]],
    }
    url = cross.url("ai") + "/internal/v1/managed-turns/execute"
    for token in [None, "wrong", cross.tokens["chat"], cross.tokens["internal"]]:
        response = await cross.client.post(
            url, json=value, headers={} if token is None else {"X-Service-Token": token}
        )
        assert response.status_code == 401
    duplicate = await cross.client.post(
        url,
        json=value,
        headers=[
            ("X-Service-Token", cross.tokens["managed"]),
            ("X-Service-Token", cross.tokens["managed"]),
        ],
    )
    assert duplicate.status_code == 401
    bodies = []
    for changes in [
        {"agent_id": cross.ids["agent_b"]},
        {"agent_id": str(uuid4())},
        {"owner_id": cross.ids["owner_a"]},
        {"integration_id": cross.ids["integration_b"]},
        {"channel": "INSTAGRAM"},
    ]:
        response = await cross.client.post(
            url,
            json=dict(value, **changes),
            headers={"X-Service-Token": cross.tokens["managed"], "X-User-Id": cross.ids["owner_b"]},
        )
        assert response.status_code == 404
        bodies.append(response.json())
    assert all(body == bodies[0] for body in bodies)
    # A real, granted UUID whose DB ownership no longer matches remains the same404.
    async with cross.ai_sql.begin() as connection:
        from sqlalchemy import text

        await connection.execute(
            text("UPDATE agent_templates SET user_id=:owner WHERE id=:id"),
            {"owner": cross.ids["owner_b"], "id": cross.ids["agent_a"]},
        )
    foreign = await cross.client.post(
        url, json=value, headers={"X-Service-Token": cross.tokens["managed"]}
    )
    assert foreign.status_code == 404 and foreign.json() == bodies[0]
    extra = await cross.client.post(
        url,
        json=dict(value, system_prompt=SENTINEL),
        headers={"X-Service-Token": cross.tokens["managed"]},
    )
    assert extra.status_code == 422 and SENTINEL not in extra.text
    claim = await cross.claim(await cross.turn(accepted))
    invalid = await cross.request(
        "POST",
        f"/turns/{claim['turn']['id']}/execute",
        dict(cross.proof(claim), agent_id=cross.ids["agent_b"]),
    )
    assert invalid.status_code == 422
    await cross.restart_cs(binding_agent=cross.ids["agent_b"])
    failed = await cross.execute(claim)
    assert failed.status_code == 200
    assert failed.json()["turn"]["failure_code"] == "PROCESSING_ERROR"
    assert len(cross.model.calls) == 0 and cross.observations().get("tool_calls", []) == []
    assert cross.observations().get("managed_forbidden_store", 0) == 0
    async with cross.ai_sql.connect() as connection:
        from sqlalchemy import text

        assert await connection.scalar(text("SELECT count(*) FROM conversation_sessions")) == 0
    cross.safe_logs(SENTINEL)
