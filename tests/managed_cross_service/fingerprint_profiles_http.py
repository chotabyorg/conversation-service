"""Actual paired HTTP proof for the current Voice-v2 fingerprint profile."""

import hashlib
import json
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import func, select, text

from core.config import Settings
from db.database import Database
from models import Conversation, InboundEvent, Message, VoiceCall, VoiceInput
from schemas.managed import ManagedTurnRequest, prepare_request
from services.common import fingerprint
from services.voice import VOICE_NAMESPACE, select_turn


def _add_voice_scope(cross):
    integration_id = cross.ids["integration_a"]
    owner_id = cross.grants["a"]["owner_id"]
    voice_grant = {
        "owner_id": owner_id,
        "integration_id": integration_id,
        "channel": "VOICE",
        "provider_namespace": VOICE_NAMESPACE,
    }
    ai_grant = {
        "owner_id": owner_id,
        "integration_id": integration_id,
        "channel": "VOICE",
        "agent_id": cross.ids["agent_a"],
        "agent_owner_id": cross.ids["owner_a"],
    }

    principals = json.loads(cross.ai_env["MANAGED_AI_AUTH_PRINCIPALS"])
    principals[0]["grants"].append(ai_grant)
    cross.ai_env["MANAGED_AI_AUTH_PRINCIPALS"] = json.dumps(principals)

    auth = json.loads(cross.cs_env["CONVERSATION_AUTH_PRINCIPALS"])
    next(item for item in auth if item["principal_id"] == "a")["grants"].append(voice_grant)
    cross.cs_env["CONVERSATION_AUTH_PRINCIPALS"] = json.dumps(auth)

    bindings = json.loads(cross.cs_env["MANAGED_AI_BINDINGS"])
    bindings.append({
        "owner_id": owner_id,
        "integration_id": integration_id,
        "channel": "VOICE",
        "agent_id": cross.ids["agent_a"],
        "service_token": cross.tokens["managed"],
    })
    cross.cs_env["MANAGED_AI_BINDINGS"] = json.dumps(bindings)
    cross.cs_env["VOICE_CANARY_INTEGRATION_IDS"] = json.dumps([integration_id])


async def _seed_voice_input_and_select_turn(cross):
    owner_id = cross.grants["a"]["owner_id"]
    integration_id = UUID(cross.ids["integration_a"])
    agent_id = UUID(cross.ids["agent_a"])
    call_id, media_session_id = uuid4(), uuid4()
    conversation_id, event_id, message_id = uuid4(), uuid4(), uuid4()
    event_fingerprint = fingerprint({"fixture_voice_event": str(event_id)})
    database = Database(cross.cs_url)
    try:
        async with database.session_factory() as session, session.begin():
            clock = await session.scalar(select(func.clock_timestamp()))
            assert clock is not None
            conversation = Conversation(
                id=conversation_id,
                owner_id=owner_id,
                integration_id=integration_id,
                channel="VOICE",
                provider_thread_id=str(call_id),
                last_message_sequence=1,
                input_revision=1,
                settling_revision=1,
            )
            session.add(conversation)
            await session.flush()
            session.add(InboundEvent(
                id=event_id,
                conversation_id=conversation_id,
                owner_id=owner_id,
                integration_id=integration_id,
                channel="VOICE",
                provider_namespace=VOICE_NAMESPACE,
                provider_event_id=f"{call_id}:1",
                fingerprint=event_fingerprint,
                accepted_at=clock,
            ))
            await session.flush()
            session.add(Message(
                id=message_id,
                conversation_id=conversation_id,
                sequence=1,
                input_revision=1,
                settling_revision=1,
                actor="CUSTOMER",
                direction="INBOUND",
                content_type="TEXT",
                text="Synthetic Voice-v2 input",
                source_inbound_event_id=event_id,
                created_at=clock,
            ))
            session.add(VoiceCall(
                id=call_id,
                conversation_id=conversation_id,
                media_session_id=media_session_id,
                agent_id=agent_id,
                last_sequence=1,
                started_at=clock,
                expires_at=clock + timedelta(minutes=10),
            ))
            await session.flush()
            session.add(VoiceInput(
                call_id=call_id,
                sequence=1,
                conversation_id=conversation_id,
                message_id=message_id,
                fingerprint=event_fingerprint,
            ))
            await session.flush()

        voice_grant = {
            "owner_id": owner_id,
            "integration_id": integration_id,
            "channel": "VOICE",
            "provider_namespace": VOICE_NAMESPACE,
        }
        settings = Settings(
            _env_file=None,
            database_url=cross.cs_url,
            auth_principals=[{
                "principal_id": "fixture-voice-selector",
                "token": cross.tokens["a"],
                "grants": [voice_grant],
            }],
            managed_ai_bindings=[{
                "owner_id": owner_id,
                "integration_id": integration_id,
                "channel": "VOICE",
                "agent_id": agent_id,
                "service_token": cross.tokens["managed"],
            }],
            managed_ai_base_url=cross.url("ai"),
            voice_cohort_integration_ids=(integration_id,),
        )
        principal = settings.auth_principals[0].principal()
        turn_id = await select_turn(database, principal, settings)
        assert turn_id is not None
        return conversation_id, turn_id
    finally:
        await database.dispose()


async def _business_rows_for_turn(cross, conversation_id, turn_id):
    async with cross.ai_sql.connect() as connection:
        return {
            "scope": await connection.scalar(text(
                "SELECT count(*) FROM business_scopes WHERE conversation_id=:conversation_id"
            ), {"conversation_id": conversation_id}),
            "context": await connection.scalar(text(
                "SELECT count(*) FROM business_turn_contexts WHERE turn_id=:turn_id"
            ), {"turn_id": turn_id}),
            "plan": await connection.scalar(text(
                "SELECT count(*) FROM business_plans "
                "WHERE origin_turn_id=:turn_id OR confirmed_turn_id=:turn_id"
            ), {"turn_id": turn_id}),
            "plan_link": await connection.scalar(text(
                "SELECT count(*) FROM business_turn_plan_links WHERE turn_id=:turn_id"
            ), {"turn_id": turn_id}),
            "operation": await connection.scalar(text(
                "SELECT count(*) FROM business_operations "
                "WHERE origin_turn_id=:turn_id OR effect_request_turn_id=:turn_id"
            ), {"turn_id": turn_id}),
        }


async def test_current_voice_v2_body_stream_and_terminal_replay_cross_real_http(cross):
    _add_voice_scope(cross)
    await cross.stop("cs")
    await cross.stop("ai")
    await cross.start("ai")
    await cross.start("cs")

    cross.model.queue_stream("Здравствуйте! ", "Чем помочь?")
    conversation_id, turn_id = await _seed_voice_input_and_select_turn(cross)
    read = await cross.request("GET", f"/turns/{turn_id}")
    assert read.status_code == 200
    turn = read.json()
    assert turn["status"] == "PENDING"

    claim = await cross.claim(turn)
    business_before = await _business_rows_for_turn(cross, conversation_id, turn_id)
    response = await cross.execute(claim)
    assert response.status_code == 200, response.text
    completed = response.json()["turn"]
    assert completed["status"] == "COMPLETED"
    assert completed["outcome"] == "DRAFT_READY"
    assert completed["draft_text"] == "Здравствуйте! Чем помочь?"
    assert completed["operation_refs"] == []
    assert completed["conversation_id"] == str(conversation_id)

    observations = cross.observations()
    assert observations["managed_http_requests"] == 1
    fields_set = observations["managed_projection_fields_set"][0]
    assert {"stream_partials", "max_partial_chars"} <= set(fields_set)
    projection = observations["managed_projections"][0]
    assert projection["channel"] == "VOICE"
    assert projection["contract_version"] == 2
    assert projection["stream_partials"] is True
    assert projection["max_partial_chars"] == 2000
    assert projection["conversation_id"] == str(conversation_id)
    assert projection["turn_id"] == str(turn_id)
    assert projection["messages"][0]["text"] == "Synthetic Voice-v2 input"

    prepared = prepare_request(ManagedTurnRequest.model_validate(projection))
    async with cross.sql.connect() as connection:
        retained_fingerprint = await connection.scalar(text(
            "SELECT managed_projection_fingerprint FROM agent_turns WHERE id=:id"
        ), {"id": turn_id})
        contract_version = await connection.scalar(text(
            "SELECT managed_contract_version FROM agent_turns WHERE id=:id"
        ), {"id": turn_id})
        retained_partial = await connection.scalar(text(
            "SELECT partial_draft_text FROM agent_turns WHERE id=:id"
        ), {"id": turn_id})
        effect_permits = await connection.scalar(text(
            "SELECT count(*) FROM external_effect_permits"
        ))
    assert retained_fingerprint == prepared.fingerprint
    assert contract_version == 2
    assert retained_fingerprint == completed["managed_request_fingerprint"]
    expected_body_hash = hashlib.sha256(prepared.body).hexdigest()
    assert observations["managed_request_canonical_sha256"] == [expected_body_hash]
    assert observations["managed_request_body_sizes"] == [{
        "raw": len(prepared.body), "canonical": len(prepared.body),
    }]
    assert observations["managed_request_body_sha256"] == [expected_body_hash]
    assert retained_partial == "Здравствуйте! "
    assert observations["managed_stream_summaries"] == [{
        "media_type": "text/event-stream",
        "event_count": 2,
        "partial_count": 1,
        "final_count": 1,
        "final_status": "DRAFT_READY",
    }]
    assert cross.model.calls[0]["stream"] is True
    assert effect_permits == 0
    assert cross.observations().get("effects", 0) == 0
    assert cross.observations().get("tool_calls", []) == []

    replay = await cross.execute(claim)
    assert replay.json() == {"turn": completed, "replayed": True}
    assert cross.observations()["managed_http_requests"] == 1
    assert len(cross.model.calls) == 1
    assert business_before == {
        "scope": 0,
        "context": 0,
        "plan": 0,
        "plan_link": 0,
        "operation": 0,
    }
    assert await _business_rows_for_turn(cross, conversation_id, turn_id) == business_before
