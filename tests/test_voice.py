"""Voice boundary behavior on migrated PostgreSQL, including independent writers."""

import asyncio
import secrets
from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select, text

from core.config import Settings
from db.database import Database
from main import create_app
from models import AgentTurn, Conversation, DeliveryAttempt, Message, VoiceCall, VoiceInput
from schemas.managed import ManagedTurnResult
from services import turns, voice
from services.voice_worker import VoiceWorker
from tests.test_telegram import DeterministicManagedPort

OWNER = "voice-owner"
INTEGRATION = UUID("00000000-0000-4000-8000-000000000091")
AGENT = UUID("00000000-0000-4000-8000-000000000092")


@pytest_asyncio.fixture
async def vh(sql, database_url):
    token, worker_token, ai_token = (secrets.token_urlsafe(32) for _ in range(3))
    grant = dict(owner_id=OWNER, integration_id=INTEGRATION, channel="VOICE",
                 provider_namespace="main-chota.voice.v1")
    settings = Settings(
        database_url=database_url, _env_file=None,
        auth_principals=[dict(principal_id="voice", role="VOICE", token=token, grants=[grant]),
                         dict(principal_id="voice-worker", role="VOICE_PROCESSING",
                              token=worker_token, grants=[grant])],
        managed_ai_bindings=[dict(owner_id=OWNER, integration_id=INTEGRATION, channel="VOICE",
                                 agent_id=AGENT, service_token=ai_token)],
        managed_ai_base_url="http://127.0.0.1:55570",
        voice_cohort_integration_ids=(INTEGRATION,), voice_worker_enabled=True,
        voice_worker_principal_id="voice-worker",
    )
    database = Database(database_url)
    port = DeterministicManagedPort()
    app = create_app(settings, database, port)
    async with LifespanManager(app), httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
        headers={"X-Service-Token": token},
    ) as client:
        identity = dict(call_id=str(uuid4()), media_session_id=str(uuid4()))
        h = dict(client=client, identity=identity, database=database, settings=settings,
                 worker=VoiceWorker(database, settings, port), port=port, app=app)
        yield h


async def post(h, path, **values):
    return await h["client"].post("/internal/v1/voice/" + path,
                                  json=dict(h["identity"], **values))


async def start(h):
    result = await post(h, "calls/start", owner_id=OWNER,
                        integration_id=str(INTEGRATION), agent_id=str(AGENT))
    assert result.status_code == 200, result.text
    return result.json()


async def draft(h):
    await start(h)
    result = await post(h, "utterances", sequence=1, text="Привет")
    assert result.status_code == 200, result.text
    assert await h["worker"].run_once()
    candidate = await post(h, "responses/next")
    assert candidate.status_code == 200, candidate.text
    return candidate.json()["turn_id"]


async def begin(h, turn_id):
    result = await post(h, "responses/begin", turn_id=turn_id)
    assert result.status_code == 200, result.text
    return result.json()


async def fact(h, permission, state="COMPLETED", **changes):
    values = dict(response_id=permission["response_id"], event_sequence=1, state=state,
                  generated_bytes=1600, transport_accepted_bytes=1600, terminal=True)
    values.update(changes)
    return await post(h, "playback", **values)


async def test_complete_call_history_and_no_repeat_send(vh):
    turn_id = await draft(vh)
    permission = await begin(vh, turn_id)
    assert permission["send"] is True
    replay = await begin(vh, turn_id)
    assert replay["send"] is False and replay["text"] is None
    for _ in range(2):
        result = await fact(vh, permission)
        assert result.status_code == 200, result.text
    async with vh["database"].session_factory() as session:
        rows = list((await session.scalars(select(Message).order_by(Message.sequence))).all())
        assert [row.actor for row in rows] == ["CUSTOMER", "AGENT"]
        assert (await session.scalar(select(DeliveryAttempt.status))) == "PROVIDER_ACCEPTED"
    ended = await post(vh, "calls/end", reason="DISCONNECTED")
    assert ended.status_code == 200
    async with vh["database"].session_factory() as session:
        conversation = await session.scalar(select(Conversation))
        assert conversation is not None
        assert conversation.authority_revision == 1
        assert (
            conversation.authority_input_floor_sequence
            == conversation.last_message_sequence == 2
        )
    assert (await post(vh, "calls/end", reason="DISCONNECTED")).json()["replayed"]
    assert (await post(vh, "utterances", sequence=2, text="late")).status_code == 409
    assert not await vh["worker"].run_once()
    assert vh["port"].calls == 1


async def test_concurrent_admission_exact_replay_and_conflict(vh):
    await start(vh)
    results = await asyncio.gather(*(post(vh, "utterances", sequence=1, text="one")
                                     for _ in range(4)))
    assert all(r.status_code == 200 for r in results)
    assert sum(not r.json()["replayed"] for r in results) == 1
    assert (await post(vh, "utterances", sequence=1, text="changed")).status_code == 409
    assert (await post(vh, "utterances", sequence=3, text="gap")).status_code == 409
    async with vh["database"].session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(VoiceInput)) == 1


@pytest.mark.parametrize("fence", ["input", "authority", "end", "expiry", "cohort"])
async def test_stale_or_closed_call_never_authorizes_playback(vh, fence):
    turn_id = await draft(vh)
    if fence == "input":
        assert (await post(vh, "utterances", sequence=2, text="newer")).status_code == 200
    elif fence == "end":
        assert (await post(vh, "calls/end", reason="DISCONNECTED")).status_code == 200
    elif fence == "cohort":
        vh["app"].state.settings = vh["settings"].model_copy(
            update={"voice_cohort_integration_ids": ()})
    else:
        async with vh["database"].session_factory() as session, session.begin():
            if fence == "authority":
                conversation = await session.scalar(select(Conversation))
                conversation.authority_revision += 1
            else:
                call = await session.scalar(select(VoiceCall))
                call.started_at -= timedelta(seconds=100)
                call.expires_at = call.started_at + timedelta(seconds=1)
    permission = await begin(vh, turn_id)
    assert not permission["send"] and permission["text"] is None
    assert (await fact(vh, permission)).status_code == 404
    async with vh["database"].session_factory() as session:
        assert await session.scalar(select(DeliveryAttempt.status)) == "SUPERSEDED"


@pytest.mark.parametrize("state", ["INTERRUPTED", "FAILED", "UNKNOWN", "NOT_STARTED"])
async def test_partial_or_failed_audio_is_not_full_assistant_history(vh, state):
    permission = await begin(vh, await draft(vh))
    result = await fact(vh, permission, state, transport_accepted_bytes=(0 if state == "NOT_STARTED"
                                                                       else 320))
    assert result.status_code == 200, result.text
    assert (await fact(vh, permission)).status_code == 409
    async with vh["database"].session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)) == 1


async def test_playback_progress_monotonic_replay_and_late_completion(vh):
    permission = await begin(vh, await draft(vh))
    result = await fact(vh, permission, "STARTED", transport_accepted_bytes=320, terminal=False)
    assert result.status_code == 200, result.text
    assert (await fact(vh, permission, "STARTED", transport_accepted_bytes=320,
                       terminal=False)).json()["replayed"]
    assert (await fact(vh, permission, "PARTIAL", event_sequence=2,
                       transport_accepted_bytes=160, terminal=False)).status_code == 409
    async with vh["database"].session_factory() as session:
        assert await session.scalar(select(DeliveryAttempt.status)) == "DISPATCH_REQUESTED"
    assert (await post(vh, "calls/end", reason="DISCONNECTED")).status_code == 200
    # Already accepted audio evidence remains recordable after call/control closure.
    result = await fact(vh, permission, event_sequence=2)
    assert result.status_code == 200, result.text


async def test_expired_heartbeat_does_not_resurrect_call(vh):
    await start(vh)
    async with vh["database"].session_factory() as session, session.begin():
        call = await session.scalar(select(VoiceCall))
        call.started_at -= timedelta(seconds=100)
        call.expires_at = call.started_at + timedelta(seconds=1)
        old_expiry = call.expires_at
    assert (await post(vh, "calls/heartbeat")).json()["state"] == "EXPIRED"
    async with vh["database"].session_factory() as session:
        assert (await session.scalar(select(VoiceCall))).expires_at == old_expiry


async def test_wrong_call_session_and_agent_are_rejected(vh):
    assert (await post(vh, "calls/start", owner_id=OWNER, integration_id=str(INTEGRATION),
                       agent_id=str(uuid4()))).status_code == 404
    await start(vh)
    original = vh["identity"]
    vh["identity"] = dict(original, media_session_id=str(uuid4()))
    assert (await post(vh, "calls/heartbeat")).status_code == 404
    vh["identity"] = original
    result = await vh["client"].post("/internal/v1/inbound-events", json={})
    assert result.status_code == 404


async def test_new_input_can_arrive_while_ai_runs_and_fences_old_response(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="old")
    original = vh["port"].execute
    entered, release = asyncio.Event(), asyncio.Event()

    async def held(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    vh["port"].execute = held
    task = asyncio.create_task(vh["worker"].run_once())
    await asyncio.wait_for(entered.wait(), 5)
    result = await post(vh, "utterances", sequence=2, text="new")
    release.set()
    assert result.status_code == 200 and await task
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    assert not (await begin(vh, turn_id))["send"]
    assert await vh["worker"].run_once()
    newest = (await post(vh, "responses/next")).json()["turn_id"]
    assert (await begin(vh, newest))["send"]


async def test_lost_managed_result_recovery_never_reexecutes_ai(vh):
    from schemas.contracts import TurnClaim

    await start(vh)
    await post(vh, "utterances", sequence=1, text="one")
    worker = vh["worker"]
    turn_id = await voice.select_turn(vh["database"], worker.principal, vh["settings"])
    claim = await turns.claim_turn(vh["database"], worker.principal, turn_id,
                                   TurnClaim(worker_ref="lost-worker", lease_seconds=30))
    async with vh["database"].session_factory() as session, session.begin():
        turn = await session.get(AgentTurn, turn_id)
        turn.managed_agent_id = AGENT
        turn.managed_dispatch_epoch = claim.turn.claim_epoch
        turn.managed_dispatch_started_at = turn.started_at
        turn.managed_request_fingerprint = "a" * 64
        turn.lease_expires_at = turn.started_at
    await post(vh, "calls/end", reason="DISCONNECTED")
    assert await worker.run_once()
    assert vh["port"].calls == 0
    async with vh["database"].session_factory() as session:
        assert (await session.get(AgentTurn, turn_id)).failure_code == "EXECUTION_UNKNOWN"


async def test_schema_readiness_and_retained_downgrade_guard(vh, sql):
    from tests.test_managed_migration import migrate

    assert await vh["database"].ready()
    await start(vh)
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM voice_calls")) == 1
    migrate(vh["settings"].database_url.get_secret_value(), "downgrade", "0006_turn_settling",
            expected=1)
    assert await vh["database"].ready()


async def test_expired_call_persists_unknown_and_never_resumes_audio(vh):
    await begin(vh, await draft(vh))
    async with vh["database"].session_factory() as session, session.begin():
        call = await session.scalar(select(VoiceCall))
        call.started_at -= timedelta(seconds=100)
        call.expires_at = call.started_at + timedelta(seconds=1)
    await vh["worker"].run_once()
    async with vh["database"].session_factory() as session:
        assert (await session.scalar(select(VoiceCall))).end_reason == "TIMEOUT"
        assert await session.scalar(select(DeliveryAttempt.status)) == "UNKNOWN"
    assert (await post(vh, "calls/heartbeat")).json()["state"] == "ENDED"


async def test_active_playback_blocks_second_permission_until_terminal_fact(vh):
    permission = await begin(vh, await draft(vh))
    await post(vh, "utterances", sequence=2, text="new")
    assert await vh["worker"].run_once()
    assert (await post(vh, "responses/next")).json()["turn_id"] is None
    async with vh["database"].session_factory() as session:
        newest = await session.scalar(select(AgentTurn.id).order_by(
            AgentTurn.input_first_sequence.desc()))
    assert (await post(vh, "responses/begin", turn_id=str(newest))).status_code == 409
    interrupted = await fact(vh, permission, "INTERRUPTED", transport_accepted_bytes=320)
    assert interrupted.status_code == 200
    assert (await begin(vh, str(newest)))["send"]


async def test_admission_and_permission_share_actual_database_lock(vh, sql):
    from tests.conftest import locked_race

    turn_id = await draft(vh)
    async with vh["database"].session_factory() as session:
        parent = await session.scalar(select(Conversation.id))
    permission, admitted = await locked_race(sql, str(parent), [
        post(vh, "responses/begin", turn_id=turn_id),
        post(vh, "utterances", sequence=2, text="racing input"),
    ], "voice admission versus playback permission")
    assert permission.status_code == admitted.status_code == 200
    async with vh["database"].session_factory() as session:
        delivery = await session.scalar(select(DeliveryAttempt))
        assert delivery.status == ("DISPATCH_REQUESTED" if permission.json()["send"]
                                   else "SUPERSEDED")
        assert delivery.fenced_settling_revision == (1 if permission.json()["send"] else 2)


async def test_call_end_fences_an_unmarked_selected_turn_without_ai(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="one")
    worker = vh["worker"]
    turn_id = await voice.select_turn(vh["database"], worker.principal, vh["settings"])
    await post(vh, "calls/end", reason="DISCONNECTED")
    assert await worker.run_once()
    assert vh["port"].calls == 0
    async with vh["database"].session_factory() as session:
        turn = await session.get(AgentTurn, turn_id)
        assert turn.status == "FAILED" and turn.managed_dispatch_epoch is None


async def test_voice_input_ownership_can_authorize_an_opaque_business_permit(vh):
    """Voice's immediate TurnInputMessage path follows the same authority proof."""
    from core.config import AuthPrincipal

    await start(vh)
    assert (await post(vh, "utterances", sequence=1, text="proposal")).status_code == 200
    assert await vh["worker"].run_once()
    async with vh["database"].session_factory() as session:
        origin = await session.scalar(select(AgentTurn))
        conversation = await session.scalar(select(Conversation))
    assert origin is not None and origin.status == "COMPLETED"
    assert conversation is not None

    business_token = secrets.token_urlsafe(32)
    business = AuthPrincipal.model_validate({
        "principal_id": "voice-business-effect",
        "role": "BUSINESS_EFFECT",
        "token": business_token,
        "grants": [{"owner_id": OWNER, "integration_id": INTEGRATION,
                    "channel": "VOICE", "provider_namespace": "main-chota.voice.v1"}],
    })
    app_settings = vh["settings"].model_copy(update={
        "auth_principals": (*vh["settings"].auth_principals, business),
    })
    vh["app"].state.settings = app_settings

    assert (await post(vh, "utterances", sequence=2, text="confirmation")).status_code == 200
    entered, release = asyncio.Event(), asyncio.Event()
    original_execute = vh["port"].execute

    async def hold_after_marker(prepared, binding, request_id="", on_partial=None):
        entered.set()
        await release.wait()
        return await original_execute(prepared, binding, request_id, on_partial=on_partial)

    vh["port"].execute = hold_after_marker
    running = asyncio.create_task(vh["worker"].run_once())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        async with vh["database"].session_factory() as session:
            current = await session.scalar(select(AgentTurn).where(
                AgentTurn.status == "CLAIMED"))
            conversation = await session.get(Conversation, conversation.id)
        assert current is not None and current.managed_request_fingerprint is not None
        assert conversation is not None
        assert await voice.select_turn(
            vh["database"], vh["worker"].principal, vh["settings"]
        ) is None
        context = {
            "contract_version": 1,
            "conversation_id": str(conversation.id),
            "turn_id": str(current.id),
            "origin_turn_id": str(origin.id),
            "owner_id": OWNER,
            "integration_id": str(INTEGRATION),
            "channel": "VOICE",
            "agent_id": str(AGENT),
            "authority_revision": conversation.authority_revision,
            "managed_request_fingerprint": current.managed_request_fingerprint,
        }
        headers = {"X-Service-Token": business_token}
        eligibility = await vh["client"].post(
            "/internal/v1/external-effect-eligibility", json=context, headers=headers)
        assert eligibility.status_code == 200, eligibility.text
        assert eligibility.json()["eligible"] is True
        request = {
            **context,
            "operation_namespace": "main-chota.ai-business.v1",
            "operation_id": str(uuid4()),
            "operation_fingerprint": "d" * 64,
        }
        issued = await vh["client"].post(
            "/internal/v1/external-effect-permits", json=request, headers=headers)
        assert issued.status_code == 200, issued.text
        assert issued.json()["replayed"] is False
    finally:
        release.set()
    assert await running
    replay = await vh["client"].post(
        "/internal/v1/external-effect-permits", json=request, headers=headers)
    assert replay.status_code == 200, replay.text
    assert replay.json()["replayed"] is True
    assert replay.json()["permit_id"] == issued.json()["permit_id"]


@pytest.mark.parametrize("permit_first", [False, True], ids=["close-first", "permit-first"])
async def test_voice_close_and_business_permit_share_the_parent_lock(vh, sql, permit_first):
    """Call closure and permit issuance serialize on the same Conversation row."""
    from core.config import AuthPrincipal
    from schemas.contracts import TurnClaim
    from tests.conftest import observed_waiters

    origin_id = UUID(await draft(vh))
    assert (await post(vh, "utterances", sequence=2, text="new customer input")).status_code == 200
    current_id = await voice.select_turn(vh["database"], vh["worker"].principal, vh["settings"])
    assert current_id is not None
    claim = await turns.claim_turn(
        vh["database"], vh["worker"].principal, current_id,
        TurnClaim(worker_ref="effect-race", lease_seconds=30),
    )
    async with vh["database"].session_factory() as session, session.begin():
        current = await session.get(AgentTurn, current_id)
        origin = await session.get(AgentTurn, origin_id)
        conversation = await session.scalar(select(Conversation))
        assert current is not None and origin is not None and conversation is not None
        assert origin.status == "COMPLETED"
        current.managed_agent_id = AGENT
        current.managed_dispatch_epoch = claim.turn.claim_epoch
        current.managed_dispatch_started_at = current.started_at
        current.managed_request_fingerprint = "a" * 64
        context = {
            "contract_version": 1,
            "conversation_id": str(conversation.id),
            "turn_id": str(current.id),
            "origin_turn_id": str(origin.id),
            "owner_id": OWNER,
            "integration_id": str(INTEGRATION),
            "channel": "VOICE",
            "agent_id": str(AGENT),
            "authority_revision": current.authority_revision,
            "managed_request_fingerprint": current.managed_request_fingerprint,
        }
        conversation_id = conversation.id

    token = secrets.token_urlsafe(32)
    business = AuthPrincipal.model_validate({
        "principal_id": "voice-effect-lock-race",
        "role": "BUSINESS_EFFECT",
        "token": token,
        "grants": [{"owner_id": OWNER, "integration_id": INTEGRATION,
                    "channel": "VOICE", "provider_namespace": "main-chota.voice.v1"}],
    })
    vh["app"].state.settings = vh["settings"].model_copy(update={
        "auth_principals": (*vh["settings"].auth_principals, business),
    })
    headers = {"X-Service-Token": token}
    request = {
        **context,
        "operation_namespace": "main-chota.ai-business.v1",
        "operation_id": str(uuid4()),
        "operation_fingerprint": "e" * 64,
    }

    async def issue_permit():
        return await vh["client"].post(
            "/internal/v1/external-effect-permits", json=request, headers=headers,
        )

    async def close_call():
        return await post(vh, "calls/end", reason="DISCONNECTED")

    async with sql.begin() as blocker:
        blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
            {"id": conversation_id},
        )
        first = asyncio.create_task(issue_permit() if permit_first else close_call())
        await observed_waiters(sql, blocker_pid, 1, "Voice close versus effect permit")
        second = asyncio.create_task(close_call() if permit_first else issue_permit())
        await observed_waiters(sql, blocker_pid, 2, "Voice close versus effect permit")
    first_result, second_result = await asyncio.gather(first, second)
    permit_result, close_result = (
        (first_result, second_result) if permit_first else (second_result, first_result)
    )
    assert close_result.status_code == 200, close_result.text

    if permit_first:
        assert permit_result.status_code == 200, permit_result.text
        assert permit_result.json()["replayed"] is False
        replay = await issue_permit()
        assert replay.status_code == 200, replay.text
        assert replay.json()["replayed"] is True
        assert replay.json()["permit_id"] == permit_result.json()["permit_id"]
    else:
        assert permit_result.status_code == 409, permit_result.text
        assert permit_result.json()["detail"] == {
            "code": "EXTERNAL_EFFECT_DENIED", "reason": "AUTHORITY_CHANGED"
        }

    async with vh["database"].session_factory() as session:
        call = await session.scalar(select(VoiceCall))
        assert call is not None and call.ended_at is not None
    async with sql.connect() as connection:
        permit_count = await connection.scalar(
            text("SELECT count(*) FROM external_effect_permits")
        )
    assert permit_count == (1 if permit_first else 0)


async def test_invalid_candidate_does_not_starve_a_later_call(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="first")
    vh["identity"] = dict(call_id=str(uuid4()), media_session_id=str(uuid4()))
    await start(vh)
    await post(vh, "utterances", sequence=1, text="second")
    async with vh["database"].session_factory() as session, session.begin():
        parents = list((await session.scalars(select(Conversation.id).order_by(
            Conversation.id))).all())
        first = await session.scalar(select(Message).where(Message.conversation_id == parents[0]))
        # Simulate a malformed retained history with a gap before the selected
        # input. It must not affect the other caller or silently truncate history.
        first.sequence = 3
        parent = await session.get(Conversation, parents[0])
        parent.last_message_sequence = 3
        session.add(Message(id=uuid4(), conversation_id=parents[0], sequence=1,
                            actor="HUMAN", direction="OUTBOUND", content_type="TEXT",
                            text="retained context"))
    assert await vh["worker"].run_once()
    async with vh["database"].session_factory() as session:
        turns = list((await session.scalars(select(AgentTurn))).all())
        assert len(turns) == 1 and turns[0].conversation_id == parents[1]
        assert turns[0].status == "COMPLETED"
    assert vh["port"].calls == 1


async def test_cohort_removal_fences_selected_unmarked_execution(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="one")
    worker = vh["worker"]
    turn_id = await voice.select_turn(vh["database"], worker.principal, vh["settings"])
    worker.settings = vh["settings"].model_copy(update={"voice_cohort_integration_ids": ()})
    assert await worker.run_once()
    assert vh["port"].calls == 0
    async with vh["database"].session_factory() as session:
        turn = await session.get(AgentTurn, turn_id)
        assert turn.status == "FAILED" and turn.managed_dispatch_epoch is None


async def test_human_control_stops_media_heartbeat(vh):
    await start(vh)
    async with vh["database"].session_factory() as session, session.begin():
        parent = await session.scalar(select(Conversation))
        parent.control_mode = "HUMAN"
        parent.authority_revision += 1
    assert (await post(vh, "calls/heartbeat")).status_code == 409


async def test_worker_token_and_wrong_namespace_have_no_voice_http_access(vh):
    from core.config import AuthPrincipal

    await start(vh)
    worker = vh["settings"].auth_principals[1]
    result = await vh["client"].post("/internal/v1/voice/calls/heartbeat",
        json=vh["identity"], headers={"X-Service-Token": worker.token.get_secret_value()})
    assert result.status_code == 404
    original = vh["settings"].auth_principals[0]
    wrong = AuthPrincipal.model_validate(dict(
        principal_id=original.principal_id, role="VOICE", token=original.token,
        grants=[dict(owner_id=OWNER, integration_id=INTEGRATION, channel="VOICE",
                     provider_namespace="wrong.voice.namespace")]))
    with pytest.raises(voice.DomainError) as exc:
        await voice.heartbeat(vh["database"], wrong.principal(),
            voice.CallIdentity.model_validate(vh["identity"]), vh["settings"])
    assert exc.value.code == voice.ErrorCode.NOT_FOUND
    assert await voice.select_turn(vh["database"], wrong.principal(), vh["settings"]) is None
    async with vh["database"].session_factory() as session, session.begin():
        call = await session.scalar(select(VoiceCall))
        call.started_at -= timedelta(seconds=100)
        call.expires_at = call.started_at + timedelta(seconds=1)
    await voice.expire_calls(vh["database"], wrong.principal())
    async with vh["database"].session_factory() as session:
        assert (await session.scalar(select(VoiceCall))).ended_at is None


async def test_voice_requests_stream_partials_on_managed_projection(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="hi")
    assert await vh["worker"].run_once()
    assert vh["port"].prepared[-1].request.stream_partials is True
    assert vh["port"].prepared[-1].request.contract_version == 2


async def test_partial_draft_next_begin_before_completed_and_final_remainder(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="hi")
    first_partial, release = asyncio.Event(), asyncio.Event()

    async def streaming(prepared, binding, request_id="", on_partial=None):
        assert prepared.request.stream_partials is True
        assert on_partial is not None
        await on_partial("First sentence. ")
        first_partial.set()
        await release.wait()
        await on_partial("First sentence. Second sentence.")
        request = prepared.request
        return ManagedTurnResult(
            contract_version=request.contract_version,
            conversation_id=request.conversation_id,
            turn_id=request.turn_id,
            agent_id=request.agent_id,
            input_revision=request.input_revision,
            authority_revision=request.authority_revision,
            input_first_sequence=request.input_first_sequence,
            input_last_sequence=request.input_last_sequence,
            context_last_sequence=request.context_last_sequence,
            status="DRAFT_READY",
            draft_text="First sentence. Second sentence.",
            error_code=None,
            operation_refs=[],
        )

    vh["port"].execute = streaming
    task = asyncio.create_task(vh["worker"].run_once())
    await asyncio.wait_for(first_partial.wait(), 5)
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    assert turn_id is not None
    permission = await begin(vh, turn_id)
    assert permission["send"] is True and permission["text"] == "First sentence. "
    async with vh["database"].session_factory() as session:
        turn = await session.get(AgentTurn, UUID(turn_id))
        assert turn is not None
        assert turn.status == "CLAIMED" and turn.draft_text is None
        assert turn.partial_draft_text == "First sentence. "
    assert (await fact(vh, permission)).status_code == 200
    async with vh["database"].session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)) == 1
    release.set()
    assert await task
    assert (await post(vh, "responses/next")).json()["turn_id"] == turn_id
    final = await begin(vh, turn_id)
    assert final["send"] is True and final["text"] == "Second sentence."
    assert final["response_id"] != permission["response_id"]
    assert (await begin(vh, turn_id))["send"] is False
    assert (await fact(vh, final)).status_code == 200
    async with vh["database"].session_factory() as session:
        rows = list((await session.scalars(select(Message).order_by(Message.sequence))).all())
        assert [row.actor for row in rows] == ["CUSTOMER", "AGENT"]
        assert rows[1].text == "First sentence. Second sentence."


async def test_partial_draft_appends_new_sentences_from_ai_events(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="hi")
    first_partial, release = asyncio.Event(), asyncio.Event()

    async def streaming(prepared, binding, request_id="", on_partial=None):
        await on_partial("First sentence. ")
        first_partial.set()
        await release.wait()
        await on_partial("Second sentence.")
        request = prepared.request
        return ManagedTurnResult(
            contract_version=request.contract_version,
            conversation_id=request.conversation_id,
            turn_id=request.turn_id,
            agent_id=request.agent_id,
            input_revision=request.input_revision,
            authority_revision=request.authority_revision,
            input_first_sequence=request.input_first_sequence,
            input_last_sequence=request.input_last_sequence,
            context_last_sequence=request.context_last_sequence,
            status="DRAFT_READY",
            draft_text="First sentence. Second sentence.",
            error_code=None,
            operation_refs=[],
        )

    vh["port"].execute = streaming
    task = asyncio.create_task(vh["worker"].run_once())
    await asyncio.wait_for(first_partial.wait(), 5)
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    permission = await begin(vh, turn_id)
    assert permission["text"] == "First sentence. "
    assert (await fact(vh, permission)).status_code == 200
    release.set()
    assert await task
    async with vh["database"].session_factory() as session:
        turn = await session.get(AgentTurn, UUID(turn_id))
        assert turn.partial_draft_text == "First sentence. Second sentence."
    assert (await post(vh, "responses/next")).json()["turn_id"] == turn_id
    final = await begin(vh, turn_id)
    assert final["text"] == "Second sentence."


async def test_partial_covering_full_draft_seals_history_without_extra_speech(vh):
    await start(vh)
    await post(vh, "utterances", sequence=1, text="hi")
    first_partial, release = asyncio.Event(), asyncio.Event()

    async def streaming(prepared, binding, request_id="", on_partial=None):
        await on_partial("Only sentence.")
        first_partial.set()
        await release.wait()
        request = prepared.request
        return ManagedTurnResult(
            contract_version=request.contract_version,
            conversation_id=request.conversation_id,
            turn_id=request.turn_id,
            agent_id=request.agent_id,
            input_revision=request.input_revision,
            authority_revision=request.authority_revision,
            input_first_sequence=request.input_first_sequence,
            input_last_sequence=request.input_last_sequence,
            context_last_sequence=request.context_last_sequence,
            status="DRAFT_READY",
            draft_text="Only sentence.",
            error_code=None,
            operation_refs=[],
        )

    vh["port"].execute = streaming
    task = asyncio.create_task(vh["worker"].run_once())
    await asyncio.wait_for(first_partial.wait(), 5)
    turn_id = (await post(vh, "responses/next")).json()["turn_id"]
    permission = await begin(vh, turn_id)
    assert permission["text"] == "Only sentence."
    assert (await fact(vh, permission)).status_code == 200
    release.set()
    assert await task
    final = await begin(vh, turn_id)
    assert final["send"] is False and final["text"] is None and final["replayed"] is False
    async with vh["database"].session_factory() as session:
        rows = list((await session.scalars(select(Message).order_by(Message.sequence))).all())
        assert [row.actor for row in rows] == ["CUSTOMER", "AGENT"]
        assert rows[1].text == "Only sentence."
