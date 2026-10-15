"""Focused real-PostgreSQL tests for the Telegram owner Inbox boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json
import secrets
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from pydantic import SecretStr
from sqlalchemy import func, select, text

import services.authority as authority
import services.delivery as delivery
import services.human_auth as human_auth
import services.inbox_lifecycle as inbox_lifecycle
import services.managed_execution as managed_execution
import services.telegram as telegram
from core.errors import DomainError
from db.database import Database
from main import create_app
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    ExternalEffectPermit,
    HandoffCase,
    InboxCommandReceipt,
    InboxGrantScope,
    Message,
    TelegramAdmission,
    TelegramProviderAccount,
    TurnInputMessage,
)
from schemas.contracts import (
    ControlTransition,
    DeliveryCreate,
    ErrorCode,
    Principal,
    TurnClaim,
    TurnComplete,
    TurnCreate,
    TurnExecute,
)
from schemas.effects import ExternalEffectPermitRequest
from schemas.handoff import (
    AiAgentProof,
    AiGrantRevoke,
    CpBindingProof,
    CpGrantRevoke,
    InboxGrantProvision,
)
from schemas.managed_v3 import (
    ControlEvidenceResult,
    HandoffControlRequest,
    ManagedTurnV3Request,
    ManagedTurnV3Result,
    V3Message,
    V3OperationReference,
)
from services import effects, turns
from services.managed_client import handoff_request_id, prepare_v3_request
from services.managed_execution_v3 import _build_request, _finish_handoff, execute_claimed_turn_v3
from services.telegram_delivery_v2 import response_attempt_id
from tests.conftest import observed_waiters

OWNER = "handoff-owner"
INTEGRATION = UUID("00000000-0000-4000-8000-000000000241")
AGENT = UUID("00000000-0000-4000-8000-000000000242")
OTHER_AGENT = UUID("00000000-0000-4000-8000-000000000243")
ACCOUNT = UUID("00000000-0000-4000-8000-000000000244")
FOREIGN_ACCOUNT = UUID("00000000-0000-4000-8000-000000000245")
NAMESPACE = "main-chota.telegram.v1"


@pytest_asyncio.fixture
async def inbox_harness(database_url: str, sql):
    from core.config import Settings

    service_token = secrets.token_urlsafe(32)
    full_token = secrets.token_urlsafe(32)
    ai_token = secrets.token_urlsafe(32)
    cp_lifecycle = secrets.token_urlsafe(32)
    ai_lifecycle = secrets.token_urlsafe(32)
    settings = Settings(
        database_url=database_url,
        auth_principals=[
            {
                "principal_id": "telegram-gateway",
                "role": "TELEGRAM",
                "token": service_token,
                "grants": [
                    {
                        "owner_id": OWNER,
                        "integration_id": INTEGRATION,
                        "channel": "TELEGRAM",
                        "provider_namespace": NAMESPACE,
                    }
                ],
            },
            {
                "principal_id": "full-internal-test",
                "role": "FULL",
                "token": full_token,
                "grants": [
                    {
                        "owner_id": OWNER,
                        "integration_id": INTEGRATION,
                        "channel": "TELEGRAM",
                        "provider_namespace": NAMESPACE,
                    }
                ],
            },
        ],
        managed_ai_bindings=[
            {
                "owner_id": OWNER,
                "integration_id": INTEGRATION,
                "channel": "TELEGRAM",
                "agent_id": AGENT,
                "service_token": ai_token,
            },
        ],
        telegram_cohort_integration_ids=(INTEGRATION,),
        handoff_cohort_integration_ids=(INTEGRATION,),
        user_service_base_url="http://user-service.test",
        cp_handoff_base_url="http://control-plane.test",
        ai_handoff_base_url="http://ai-agent.test",
        inbox_lifecycle_token=cp_lifecycle,
        handoff_ai_lifecycle_token=ai_lifecycle,
        _env_file=None,
    )
    database = Database(database_url)
    app = create_app(settings=settings, database=database)
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://conversation.test"
        ) as client:
            yield SimpleNamespace(
                app=app,
                client=client,
                database=database,
                settings=settings,
                service_headers={"X-Service-Token": service_token},
                full_headers={"X-Service-Token": full_token},
                cp_lifecycle_headers={"X-Inbox-Lifecycle-Token": cp_lifecycle},
                ai_lifecycle_headers={"X-Service-Token": ai_lifecycle},
            )


@pytest.fixture
def user_me_transport(monkeypatch: pytest.MonkeyPatch):
    """Return real-shaped `/me` JSON over HTTPX without opening a network socket."""
    real_httpx = httpx
    outcomes: dict[str, tuple[int, bytes]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/api/v1/me"
        cookie = request.headers.get("cookie", "")
        token = cookie.removeprefix("access_token=").split(".", 1)[0]
        status, payload = outcomes.get(
            token,
            (
                200,
                json.dumps(
                    {
                        "id": str(ACCOUNT),
                        "status": "active",
                        "email_verified": True,
                        "consent_required": False,
                    }
                ).encode(),
            ),
        )
        headers = {"content-type": "application/json"}
        return real_httpx.Response(
            status, stream=real_httpx.ByteStream(payload), headers=headers, request=request
        )

    class MockUserClient:
        def __init__(self, **kwargs):
            self._client = real_httpx.AsyncClient(
                transport=real_httpx.MockTransport(handler), **kwargs
            )

        async def __aenter__(self):
            return await self._client.__aenter__()

        async def __aexit__(self, *args):
            return await self._client.__aexit__(*args)

    monkeypatch.setattr(
        human_auth,
        "httpx",
        SimpleNamespace(
            AsyncClient=MockUserClient,
            Timeout=real_httpx.Timeout,
            HTTPError=real_httpx.HTTPError,
        ),
    )
    return outcomes


async def seed_case(database: Database, *, control_mode: str = "HUMAN") -> dict[str, UUID]:
    now = datetime.now(UTC)
    conversation_id, customer_message_id = uuid4(), uuid4()
    turn_id, case_id = uuid4(), uuid4()
    async with database.session_factory() as session, session.begin():
        session.add(
            Conversation(
                id=conversation_id,
                owner_id=OWNER,
                integration_id=INTEGRATION,
                channel="TELEGRAM",
                provider_thread_id=f"thread-{conversation_id}",
                last_message_sequence=1,
                input_revision=1,
                control_mode=control_mode,
                authority_revision=1 if control_mode == "HUMAN" else 0,
                authority_input_floor_sequence=0,
            )
        )
        await session.flush()
        session.add(
            Message(
                id=customer_message_id,
                conversation_id=conversation_id,
                sequence=1,
                input_revision=1,
                actor="CUSTOMER",
                direction="INBOUND",
                text="customer asked for a person",
                created_at=now,
            )
        )
        await session.flush()
        session.add(
            AgentTurn(
                id=turn_id,
                conversation_id=conversation_id,
                selection_key=uuid4(),
                creation_fingerprint="1" * 64,
                input_first_sequence=1,
                input_last_sequence=1,
                context_last_sequence=1,
                input_revision=1,
                authority_revision=0,
                status="COMPLETED",
                claim_epoch=1,
                claim_token_digest=hashlib.sha256(b"test-claim-token").hexdigest(),
                worker_ref="test-worker",
                lease_expires_at=now + timedelta(minutes=1),
                started_at=now,
                completed_at=now,
                outcome="DRAFT_READY",
                draft_text="retained generated draft",
                operation_refs=[],
                terminal_fingerprint="2" * 64,
                managed_contract_version=3,
                managed_context_first_sequence=1,
                managed_projection_agent_id=AGENT,
                managed_projection_fingerprint="3" * 64,
            )
        )
        session.add(
            InboxGrantScope(
                owner_id=OWNER,
                integration_id=INTEGRATION,
                channel="TELEGRAM",
                agent_id=AGENT,
                account_id=ACCOUNT,
                agent_owner_id=ACCOUNT,
                cp_binding_generation=1,
                agent_lifecycle_generation=1,
                grant_generation=1,
                active=True,
            )
        )
        session.add(
            TelegramProviderAccount(
                owner_id=OWNER,
                integration_id=INTEGRATION,
                state="BOUND",
                bot_id="999",
            )
        )
        if control_mode == "HUMAN":
            session.add(
                HandoffCase(
                    id=case_id,
                    conversation_id=conversation_id,
                    owner_id=OWNER,
                    integration_id=INTEGRATION,
                    channel="TELEGRAM",
                    agent_id=AGENT,
                    control_request_id=uuid4(),
                    control_request_fingerprint="4" * 64,
                    source_turn_id=turn_id,
                    reason="CUSTOMER_REQUEST",
                    note=None,
                    status="OPEN",
                    case_revision=1,
                    authority_revision=1,
                    opened_authority_revision=1,
                    assignment_revision=0,
                    assigned_account_id=None,
                    created_at=now,
                    updated_at=now,
                )
            )
    return {
        "conversation_id": conversation_id,
        "message_id": customer_message_id,
        "turn_id": turn_id,
        "case_id": case_id,
    }


def browser_headers(token: str, *, write: bool = False) -> dict[str, str]:
    headers = {"Cookie": f"access_token={token}.payload.signature"}
    if write:
        headers["Origin"] = "https://agents.chota.by"
    return headers


def lifecycle_proofs(
    request: InboxGrantProvision, *, binding_generation: int = 1, agent_generation: int = 1
):
    return (
        CpBindingProof(
            contract_version=1,
            account_id=ACCOUNT,
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            binding_generation=binding_generation,
        ),
        AiAgentProof(
            contract_version=1,
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=request.channel,
            agent_id=request.agent_id,
            agent_owner_id=ACCOUNT,
            agent_lifecycle_generation=agent_generation,
        ),
    )


@pytest.mark.asyncio
async def test_same_owner_claim_race_and_return_floor(inbox_harness, user_me_transport):
    ids = await seed_case(inbox_harness.database)
    client = inbox_harness.client
    path = f"/public/v1/inbox/cases/{ids['case_id']}/claim"
    commands = [
        {"command_id": str(uuid4()), "expected_authority_revision": 1, "expected_case_revision": 1},
        {"command_id": str(uuid4()), "expected_authority_revision": 1, "expected_case_revision": 1},
    ]
    results = await asyncio.gather(
        *(
            client.post(path, json=command, headers=browser_headers(session, write=True))
            for command, session in zip(commands, ("session-a", "session-b"), strict=True)
        )
    )
    assert sorted(response.status_code for response in results) == [200, 409]
    winner = next(response for response in results if response.status_code == 200)
    winner_index = results.index(winner)
    winner_command = commands[winner_index]
    applied = winner.json()
    assert applied["receipt"]["replayed"] is False
    assert applied["case"]["status"] == "CLAIMED"
    assert applied["case"]["assigned_account_id"] == str(ACCOUNT)

    replay = await client.post(
        path,
        json=winner_command,
        headers=browser_headers(("session-a", "session-b")[winner_index], write=True),
    )
    assert replay.status_code == 200
    assert replay.json()["receipt"]["replayed"] is True
    assert replay.json()["receipt"]["command_id"] == winner_command["command_id"]

    case = applied["case"]
    send_command = {
        "command_id": str(uuid4()),
        "expected_authority_revision": case["authority_revision"],
        "expected_case_revision": case["case_revision"],
        "expected_assignment_revision": case["assignment_revision"],
        "text": "A person will help you shortly.",
    }
    sent = await client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/messages",
        json=send_command,
        headers=browser_headers("session-a", write=True),
    )
    assert sent.status_code == 200, sent.text
    assert sent.json()["receipt"]["message_id"]
    assert sent.json()["receipt"]["outbound_intent_id"]
    assert sent.json()["case"]["case_revision"] == 3

    returned = await client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/return-to-agent",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": 1,
            "expected_case_revision": 3,
        },
        headers=browser_headers("session-a", write=True),
    )
    assert returned.status_code == 200, returned.text
    assert returned.json()["case"]["status"] == "RETURNED"
    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        assert conversation.control_mode == "AGENT"
        assert conversation.authority_revision == 2
        assert conversation.handoff_resume_floor_sequence == 2
        turns = (
            await session.scalars(
                select(AgentTurn).where(AgentTurn.conversation_id == conversation.id)
            )
        ).all()
        assert len(turns) == 1, "Return must not synthesize an AgentTurn"


@pytest.mark.asyncio
async def test_foreign_owner_is_hidden_and_user_service_failures_are_distinct(
    inbox_harness,
    user_me_transport,
):
    ids = await seed_case(inbox_harness.database)
    client = inbox_harness.client
    user_me_transport["foreign-session"] = (
        200,
        json.dumps(
            {
                "id": str(FOREIGN_ACCOUNT),
                "status": "active",
                "email_verified": True,
                "consent_required": False,
            }
        ).encode(),
    )
    foreign = await client.get(
        f"/public/v1/inbox/cases/{ids['case_id']}",
        headers=browser_headers("foreign-session"),
    )
    assert foreign.status_code == 404

    user_me_transport["invalid-session"] = (401, b'{"detail":"invalid session"}')
    invalid = await client.get("/public/v1/inbox/cases", headers=browser_headers("invalid-session"))
    assert invalid.status_code == 401

    user_me_transport["service-outage"] = (503, b'{"detail":"unavailable"}')
    unavailable = await client.get(
        "/public/v1/inbox/cases", headers=browser_headers("service-outage")
    )
    assert unavailable.status_code == 503

    user_me_transport["malformed-user"] = (200, b'{"id":')
    malformed = await client.get(
        "/public/v1/inbox/cases", headers=browser_headers("malformed-user")
    )
    assert malformed.status_code == 503


@pytest.mark.asyncio
async def test_authorized_second_agent_cannot_borrow_another_agents_delivery(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    # Model a rebind: the current exact Telegram principal is authorized for the
    # second agent while the retained root/turn still belongs to the first.
    old_binding = inbox_harness.settings.managed_ai_bindings[0]
    inbox_harness.app.state.settings = inbox_harness.settings.model_copy(
        update={
            "managed_ai_bindings": (old_binding.model_copy(update={"agent_id": OTHER_AGENT}),),
        }
    )
    intent_id = uuid4()
    now = datetime.now(UTC)
    async with inbox_harness.database.session_factory() as session, session.begin():
        session.add(
            DeliveryAttempt(
                id=intent_id,
                conversation_id=ids["conversation_id"],
                delivery_key=ids["turn_id"],
                source_turn_id=ids["turn_id"],
                source_message_id=None,
                outbound_ordinal=1,
                grant_generation=1,
                creation_fingerprint="5" * 64,
                authority_revision=0,
                status="GENERATED",
                revision=0,
                created_at=now,
                updated_at=now,
            )
        )

    payload = {
        "contract_version": 2,
        "owner_id": OWNER,
        "integration_id": str(INTEGRATION),
        "agent_id": str(OTHER_AGENT),
        "conversation_id": str(ids["conversation_id"]),
        "provider_thread_id": f"thread-{ids['conversation_id']}",
        "bot_id": 999,
        "outbound_intent_id": str(intent_id),
        "outbound_ordinal": 1,
        "source_kind": "AGENT_TURN",
        "source_id": str(ids["turn_id"]),
        "response_attempt_id": str(response_attempt_id(intent_id)),
        "work_kind": "SEND_CANDIDATE",
        "supplier_instance_id": str(uuid4()),
        "process_incarnation_id": str(uuid4()),
    }
    response = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/begin",
        json=payload,
        headers=inbox_harness.service_headers,
    )
    assert response.status_code == 409
    assert "retained generated draft" not in response.text


@pytest.mark.asyncio
async def test_telegram_ingress_releases_human_and_return_floor_admissions(inbox_harness):
    ids = await seed_case(inbox_harness.database)
    identity = {
        "owner_id": OWNER,
        "integration_id": str(INTEGRATION),
        "agent_id": str(AGENT),
        "provider_thread_id": "123",
        "provider_event_id": "b999c123m9",
    }
    now = datetime.now(UTC)
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.provider_thread_id = "123"
        session.add(
            TelegramAdmission(
                id=uuid4(),
                owner_id=OWNER,
                integration_id=INTEGRATION,
                agent_id=AGENT,
                channel="TELEGRAM",
                provider_namespace=NAMESPACE,
                provider_event_id="b999c123m9",
                provider_thread_id="123",
                update_id=9,
                chat_id="123",
                message_id="9",
                sender_id="123",
                bot_id="999",
                fingerprint="6" * 64,
                route="MANAGED",
                conversation_id=conversation.id,
                canonical_message_id=ids["message_id"],
                created_at=now,
            )
        )

    during_human = await inbox_harness.client.post(
        "/internal/v1/telegram/results",
        json=identity,
        headers=inbox_harness.service_headers,
    )
    assert during_human.status_code == 200, during_human.text
    assert during_human.json()["progress_disposition"] == "AUTHORITY_FENCED"
    assert during_human.json()["state"] == "PENDING"
    assert during_human.json()["turn_id"] is None
    assert during_human.json()["delivery_state"] == "NOT_STARTED"

    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.control_mode = "AGENT"
        conversation.authority_revision = 2
        conversation.handoff_resume_floor_sequence = 1

    after_return = await inbox_harness.client.post(
        "/internal/v1/telegram/results",
        json=identity,
        headers=inbox_harness.service_headers,
    )
    assert after_return.status_code == 200, after_return.text
    assert after_return.json()["progress_disposition"] == "AUTHORITY_FENCED"
    assert after_return.json()["turn_id"] is None


@pytest.mark.asyncio
async def test_generic_full_control_cannot_bypass_enrolled_owner_return(inbox_harness):
    ids = await seed_case(inbox_harness.database)
    response = await inbox_harness.client.post(
        f"/internal/v1/conversations/{ids['conversation_id']}/control",
        json={"expected_authority_revision": 1, "control_mode": "AGENT"},
        headers=inbox_harness.full_headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "HANDOFF_CONFLICT"
    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        case = await session.get(HandoffCase, ids["case_id"])
        assert conversation is not None and case is not None
        assert conversation.control_mode == "HUMAN"
        assert conversation.authority_revision == 1
        assert conversation.handoff_resume_floor_sequence is None
        assert case.status == "OPEN"


@pytest.mark.asyncio
async def test_generic_full_delivery_cannot_create_legacy_intent_for_enrolled_draft(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    response = await inbox_harness.client.post(
        f"/internal/v1/conversations/{ids['conversation_id']}/delivery-attempts",
        json={
            "delivery_key": str(uuid4()),
            "source_turn_id": str(ids["turn_id"]),
            "expected_authority_revision": 0,
        },
        headers=inbox_harness.full_headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "HANDOFF_CONFLICT"
    async with inbox_harness.database.session_factory() as session:
        assert await session.scalar(
            select(func.count()).select_from(DeliveryAttempt).where(
                DeliveryAttempt.source_turn_id == ids["turn_id"]
            )
        ) == 0


@pytest.mark.asyncio
async def test_legacy_reconcile_remains_available_for_ordinal_only_attempt(inbox_harness):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    now = datetime.now(UTC)
    attempt_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        session.add(
            DeliveryAttempt(
                id=attempt_id,
                conversation_id=ids["conversation_id"],
                delivery_key=uuid4(),
                source_turn_id=ids["turn_id"],
                source_message_id=None,
                outbound_ordinal=1,
                grant_generation=None,
                creation_fingerprint="8" * 64,
                authority_revision=0,
                status="GENERATED",
                revision=0,
                created_at=now,
                updated_at=now,
            )
        )
    dispatched = await inbox_harness.client.post(
        f"/internal/v1/delivery-attempts/{attempt_id}/reconcile",
        json={
            "reconciliation_key": str(uuid4()),
            "expected_revision": 0,
            "status": "DISPATCH_REQUESTED",
            "expected_authority_revision": 0,
        },
        headers=inbox_harness.full_headers,
    )
    assert dispatched.status_code == 200, dispatched.text
    failed = await inbox_harness.client.post(
        f"/internal/v1/delivery-attempts/{attempt_id}/reconcile",
        json={
            "reconciliation_key": str(uuid4()),
            "expected_revision": 1,
            "status": "FAILED",
            "error_code": "TIMEOUT",
        },
        headers=inbox_harness.full_headers,
    )
    assert failed.status_code == 200, failed.text
    assert failed.json()["attempt"]["status"] == "FAILED"


@pytest.mark.asyncio
async def test_generic_full_new_turn_is_blocked_for_post_enrollment_input(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    now = datetime.now(UTC)
    message_id = uuid4()
    admission_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.provider_thread_id = "123"
        conversation.last_message_sequence = 2
        conversation.input_revision = 2
        session.add(
            Message(
                id=message_id,
                conversation_id=conversation.id,
                sequence=2,
                input_revision=2,
                actor="CUSTOMER",
                direction="INBOUND",
                text="A new customer message after enrollment",
                created_at=now,
            )
        )
        session.add(
            TelegramAdmission(
                id=admission_id,
                owner_id=OWNER,
                integration_id=INTEGRATION,
                agent_id=AGENT,
                channel="TELEGRAM",
                provider_namespace=NAMESPACE,
                provider_event_id="b999c123m2",
                provider_thread_id="123",
                update_id=2,
                chat_id="123",
                message_id="2",
                sender_id="123",
                bot_id="999",
                fingerprint="9" * 64,
                route="MANAGED",
                conversation_id=conversation.id,
                canonical_message_id=message_id,
                created_at=now,
            )
        )

    forbidden = await inbox_harness.client.post(
        f"/internal/v1/conversations/{ids['conversation_id']}/turns",
        json={
            "selection_key": str(uuid4()),
            "input_first_sequence": 2,
            "input_last_sequence": 2,
            "expected_input_revision": 2,
            "expected_authority_revision": 0,
        },
        headers=inbox_harness.full_headers,
    )
    assert forbidden.status_code == 409, forbidden.text
    assert forbidden.json()["detail"]["code"] == "HANDOFF_CONFLICT"

    selected = await telegram.select_immediate(
        inbox_harness.database,
        inbox_harness.settings.auth_principals[0].principal(),
        inbox_harness.settings,
    )
    assert selected is not None
    async with inbox_harness.database.session_factory() as session:
        fresh = await session.get(AgentTurn, selected)
        assert fresh is not None
        assert fresh.managed_contract_version == 3
        assert fresh.input_first_sequence == 2


@pytest.mark.asyncio
async def test_exact_legacy_selection_replay_survives_later_enrollment(inbox_harness):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    async with inbox_harness.database.session_factory() as session, session.begin():
        await session.execute(
            text(
                "DELETE FROM inbox_grant_scopes WHERE owner_id=:owner "
                "AND integration_id=:integration AND agent_id=:agent"
            ),
            {"owner": OWNER, "integration": INTEGRATION, "agent": AGENT},
        )
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.last_message_sequence = 2
        conversation.input_revision = 2
        session.add(
            Message(
                id=uuid4(),
                conversation_id=conversation.id,
                sequence=2,
                input_revision=2,
                actor="CUSTOMER",
                direction="INBOUND",
                text="Legacy selection before enrollment",
                created_at=datetime.now(UTC),
            )
        )
    selection = {
        "selection_key": str(uuid4()),
        "input_first_sequence": 2,
        "input_last_sequence": 2,
        "expected_input_revision": 2,
        "expected_authority_revision": 0,
    }
    created = await inbox_harness.client.post(
        f"/internal/v1/conversations/{ids['conversation_id']}/turns",
        json=selection,
        headers=inbox_harness.full_headers,
    )
    assert created.status_code == 201, created.text
    turn_id = UUID(created.json()["turn"]["id"])
    principal = inbox_harness.settings.auth_principals[1].principal()
    claim = await turns.claim_turn(
        inbox_harness.database,
        principal,
        turn_id,
        TurnClaim(worker_ref="legacy-before-enrollment", lease_seconds=30),
    )
    await turns.finish_turn(
        inbox_harness.database,
        principal,
        turn_id,
        TurnComplete(
            claim_epoch=claim.turn.claim_epoch,
            claim_token=SecretStr(claim.claim_token),
            outcome="NO_RESPONSE",
        ),
    )
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert scope is not None
        scope.account_id = ACCOUNT
        scope.agent_owner_id = ACCOUNT
        scope.cp_binding_generation = 1
        scope.agent_lifecycle_generation = 1
        scope.grant_generation = 1
        scope.active = True

    replay = await inbox_harness.client.post(
        f"/internal/v1/conversations/{ids['conversation_id']}/turns",
        json=selection,
        headers=inbox_harness.full_headers,
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["replayed"] is True


@pytest.mark.parametrize("active_shape", ["PENDING_V1", "EXPIRED_MARKED_V2"])
@pytest.mark.asyncio
async def test_provision_refuses_active_legacy_turn_without_retiring_recovery_state(
    inbox_harness, monkeypatch, active_shape
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    now = datetime.now(UTC)
    legacy_turn_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert scope is not None
        await session.delete(scope)
        marked = active_shape == "EXPIRED_MARKED_V2"
        session.add(
            AgentTurn(
                id=legacy_turn_id,
                conversation_id=ids["conversation_id"],
                selection_key=uuid4(),
                creation_fingerprint="a" * 64,
                input_first_sequence=1,
                input_last_sequence=1,
                context_last_sequence=1,
                input_revision=1,
                authority_revision=0,
                status="CLAIMED" if marked else "PENDING",
                claim_epoch=1 if marked else 0,
                claim_token_digest=(
                    hashlib.sha256(b"old-claim").hexdigest() if marked else None
                ),
                worker_ref="old-worker" if marked else None,
                lease_expires_at=now - timedelta(seconds=1) if marked else None,
                started_at=now - timedelta(minutes=1) if marked else None,
                managed_contract_version=2 if marked else 1,
                managed_context_first_sequence=1,
                managed_projection_agent_id=AGENT if marked else None,
                managed_projection_fingerprint="b" * 64 if marked else None,
                managed_agent_id=AGENT if marked else None,
                managed_dispatch_epoch=1 if marked else None,
                managed_dispatch_started_at=now if marked else None,
                managed_request_fingerprint="c" * 64 if marked else None,
            )
        )

    async def current_proofs(_settings, request):
        return (
            CpBindingProof(
                contract_version=1,
                account_id=ACCOUNT,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                binding_generation=request.expected_binding_generation,
            ),
            AiAgentProof(
                contract_version=1,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                agent_owner_id=ACCOUNT,
                agent_lifecycle_generation=1,
            ),
        )

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current_proofs)
    request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=1,
    )
    with pytest.raises(DomainError) as rejected:
        await inbox_lifecycle.provision_grant(
            inbox_harness.database, inbox_harness.settings, request
        )
    assert rejected.value.code == ErrorCode.ACTIVE_TURN_EXISTS
    async with inbox_harness.database.session_factory() as session:
        assert await session.get(
            InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT)
        ) is None
        retained = await session.get(AgentTurn, legacy_turn_id)
        assert retained is not None
        assert retained.status == ("PENDING" if active_shape == "PENDING_V1" else "CLAIMED")
        assert retained.retired_at is None


@pytest.mark.parametrize("first_lock", ["LEGACY_SELECTION", "PROVISION"])
@pytest.mark.asyncio
async def test_legacy_selection_and_grant_provision_linearize_on_scope_lock(
    inbox_harness, sql, monkeypatch, first_lock
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert scope is not None and conversation is not None
        await session.delete(scope)
        conversation.last_message_sequence = 2
        conversation.input_revision = 2
        session.add(
            Message(
                id=uuid4(),
                conversation_id=conversation.id,
                sequence=2,
                input_revision=2,
                actor="CUSTOMER",
                direction="INBOUND",
                text="Selection/provision ordering input",
                created_at=datetime.now(UTC),
            )
        )

    async def current_proofs(_settings, request):
        return (
            CpBindingProof(
                contract_version=1,
                account_id=ACCOUNT,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                binding_generation=request.expected_binding_generation,
            ),
            AiAgentProof(
                contract_version=1,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                agent_owner_id=ACCOUNT,
                agent_lifecycle_generation=1,
            ),
        )

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current_proofs)
    provision_request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=1,
    )
    turn_request = TurnCreate(
        selection_key=uuid4(),
        input_first_sequence=2,
        input_last_sequence=2,
        expected_input_revision=2,
        expected_authority_revision=0,
    )
    full_principal = inbox_harness.settings.auth_principals[1].principal()
    scope_held = asyncio.Event()
    release_holder = asyncio.Event()
    blocker_pid: int | None = None

    original_scope_lock = telegram._locked_handoff_scope_values
    original_active_check = inbox_lifecycle._has_active_agent_turn

    async def pause_legacy_after_scope(session, owner_id, integration_id, agent_id):
        nonlocal blocker_pid
        scope = await original_scope_lock(session, owner_id, integration_id, agent_id)
        if first_lock == "LEGACY_SELECTION":
            blocker_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            scope_held.set()
            await release_holder.wait()
        return scope

    async def pause_provision_after_scope(session, request):
        nonlocal blocker_pid
        blocker_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        scope_held.set()
        await release_holder.wait()
        return await original_active_check(session, request)

    monkeypatch.setattr(telegram, "_locked_handoff_scope_values", pause_legacy_after_scope)
    monkeypatch.setattr(
        inbox_lifecycle,
        "_has_active_agent_turn",
        pause_provision_after_scope,
    )

    if first_lock == "LEGACY_SELECTION":
        legacy_task = asyncio.create_task(
            turns.create_turn(
                inbox_harness.database,
                full_principal,
                ids["conversation_id"],
                turn_request,
                settings=inbox_harness.settings,
            )
        )
        await asyncio.wait_for(scope_held.wait(), timeout=3)
        provision_task = asyncio.create_task(
            inbox_lifecycle.provision_grant(
                inbox_harness.database, inbox_harness.settings, provision_request
            )
        )
        assert blocker_pid is not None
        await observed_waiters(sql, blocker_pid, 1, "legacy-selection-before-provision")
        release_holder.set()
        created = await legacy_task
        with pytest.raises(DomainError) as rejected:
            await provision_task
        async with inbox_harness.database.session_factory() as session:
            retained = await session.get(AgentTurn, created.turn.id)
            assert retained is not None and retained.managed_contract_version == 1
        assert rejected.value.code == ErrorCode.ACTIVE_TURN_EXISTS
    else:
        provision_task = asyncio.create_task(
            inbox_lifecycle.provision_grant(
                inbox_harness.database, inbox_harness.settings, provision_request
            )
        )
        await asyncio.wait_for(scope_held.wait(), timeout=3)
        selection_task = asyncio.create_task(
            turns.create_turn(
                inbox_harness.database,
                full_principal,
                ids["conversation_id"],
                turn_request,
                settings=inbox_harness.settings,
            )
        )
        assert blocker_pid is not None
        await observed_waiters(sql, blocker_pid, 1, "provision-before-legacy-selection")
        release_holder.set()
        provisioned = await provision_task
        assert provisioned.active is True
        with pytest.raises(DomainError) as rejected:
            await selection_task
        assert rejected.value.code == ErrorCode.HANDOFF_CONFLICT


@pytest.mark.parametrize("legacy_operation", ["turn", "delivery", "control"])
@pytest.mark.asyncio
async def test_stale_settings_legacy_writers_recheck_enrollment_after_parent_lock(
    inbox_harness, sql, monkeypatch, legacy_operation
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    legacy_source_turn_id = ids["turn_id"]
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        turn = await session.get(AgentTurn, ids["turn_id"])
        assert scope is not None and turn is not None
        await session.delete(scope)
        if legacy_operation == "delivery":
            # A pre-Handoff FULL draft has no immutable managed agent projection
            # from which the legacy service could discover the missing scope.
            legacy_source_turn_id = uuid4()
            session.add(
                AgentTurn(
                    id=legacy_source_turn_id,
                    conversation_id=turn.conversation_id,
                    selection_key=uuid4(),
                    creation_fingerprint="d" * 64,
                    input_first_sequence=1,
                    input_last_sequence=1,
                    context_last_sequence=1,
                    input_revision=1,
                    authority_revision=0,
                    status="COMPLETED",
                    claim_epoch=1,
                    claim_token_digest=hashlib.sha256(b"legacy-draft").hexdigest(),
                    worker_ref="legacy-worker",
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=1),
                    started_at=datetime.now(UTC),
                    completed_at=datetime.now(UTC),
                    outcome="DRAFT_READY",
                    draft_text="legacy draft without an agent projection",
                    terminal_fingerprint="e" * 64,
                    managed_contract_version=1,
                    managed_context_first_sequence=1,
                    operation_refs=[],
                )
            )

    async def current_proofs(_settings, request):
        return (
            CpBindingProof(
                contract_version=1,
                account_id=ACCOUNT,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                binding_generation=1,
            ),
            AiAgentProof(
                contract_version=1,
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                agent_owner_id=ACCOUNT,
                agent_lifecycle_generation=1,
            ),
        )

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current_proofs)
    original_active_check = inbox_lifecycle._has_active_agent_turn
    parent_held = asyncio.Event()
    release_provision = asyncio.Event()
    blocker_pid: int | None = None

    async def pause_provision_after_parent(session, request):
        nonlocal blocker_pid
        active = await original_active_check(session, request)
        blocker_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        parent_held.set()
        await release_provision.wait()
        return active

    monkeypatch.setattr(inbox_lifecycle, "_has_active_agent_turn", pause_provision_after_parent)
    provision_request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=1,
    )
    provision_task = asyncio.create_task(
        inbox_lifecycle.provision_grant(
            inbox_harness.database, inbox_harness.settings, provision_request
        )
    )
    await asyncio.wait_for(parent_held.wait(), timeout=3)
    principal = inbox_harness.settings.auth_principals[1].principal()
    if legacy_operation == "turn":
        legacy_task = asyncio.create_task(
            turns.create_turn(
                inbox_harness.database,
                principal,
                ids["conversation_id"],
                TurnCreate(
                    selection_key=uuid4(),
                    input_first_sequence=1,
                    input_last_sequence=1,
                    expected_input_revision=1,
                    expected_authority_revision=0,
                ),
                settings=None,
            )
        )
    elif legacy_operation == "delivery":
        legacy_task = asyncio.create_task(
            delivery.create_delivery(
                inbox_harness.database,
                principal,
                ids["conversation_id"],
                DeliveryCreate(
                    delivery_key=uuid4(),
                    source_turn_id=legacy_source_turn_id,
                    expected_authority_revision=0,
                ),
                settings=None,
            )
        )
    else:
        stale_settings = inbox_harness.settings.model_copy(update={"managed_ai_bindings": ()})
        legacy_task = asyncio.create_task(
            authority.transition_control(
                inbox_harness.database,
                principal,
                ids["conversation_id"],
                ControlTransition(expected_authority_revision=0, control_mode="HUMAN"),
                stale_settings,
            )
        )
    assert blocker_pid is not None
    await observed_waiters(sql, blocker_pid, 1, f"stale-settings-{legacy_operation}-writer")
    release_provision.set()
    provisioned = await provision_task
    assert provisioned.active is True
    with pytest.raises(DomainError) as rejected:
        await legacy_task
    assert rejected.value.code == ErrorCode.HANDOFF_CONFLICT
    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        assert conversation.control_mode == "AGENT"
        assert conversation.authority_revision == 0
        if legacy_operation == "turn":
            assert await session.scalar(
                select(AgentTurn.id).where(
                    AgentTurn.conversation_id == conversation.id,
                    AgentTurn.id != ids["turn_id"],
                )
            ) is None
        elif legacy_operation == "delivery":
            assert await session.scalar(
                select(DeliveryAttempt.id).where(
                    DeliveryAttempt.conversation_id == conversation.id
                )
            ) is None


@pytest.mark.asyncio
async def test_provision_refuses_legacy_human_root_without_handoff_case(
    inbox_harness, monkeypatch
):
    ids = await seed_case(inbox_harness.database, control_mode="HUMAN")
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert scope is not None
        await session.delete(scope)
        await session.delete(await session.get(HandoffCase, ids["case_id"]))
    request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=1,
    )

    async def current_proofs(_settings, proof_request):
        return lifecycle_proofs(proof_request)

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current_proofs)
    with pytest.raises(DomainError) as rejected:
        await inbox_lifecycle.provision_grant(
            inbox_harness.database, inbox_harness.settings, request
        )
    assert rejected.value.code == ErrorCode.HANDOFF_CONFLICT
    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert conversation is not None and conversation.control_mode == "HUMAN"
        assert scope is None


@pytest.mark.asyncio
async def test_generic_full_control_first_cannot_strand_root_before_initial_enrollment(
    inbox_harness, sql, monkeypatch
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    async with inbox_harness.database.session_factory() as session, session.begin():
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert scope is not None
        await session.delete(scope)

    async def current_proofs(_settings, request):
        return lifecycle_proofs(request)

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", current_proofs)
    original_scope_lock = telegram._locked_handoff_scope_values
    scope_held = asyncio.Event()
    release_control = asyncio.Event()
    blocker_pid: int | None = None

    async def pause_control_after_scope(session, owner_id, integration_id, agent_id):
        nonlocal blocker_pid
        scope = await original_scope_lock(session, owner_id, integration_id, agent_id)
        blocker_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        scope_held.set()
        await release_control.wait()
        return scope

    monkeypatch.setattr(telegram, "_locked_handoff_scope_values", pause_control_after_scope)
    principal = inbox_harness.settings.auth_principals[1].principal()
    control_task = asyncio.create_task(
        authority.transition_control(
            inbox_harness.database,
            principal,
            ids["conversation_id"],
            ControlTransition(expected_authority_revision=0, control_mode="HUMAN"),
            inbox_harness.settings,
        )
    )
    await asyncio.wait_for(scope_held.wait(), timeout=3)
    provision_task = asyncio.create_task(
        inbox_lifecycle.provision_grant(
            inbox_harness.database,
            inbox_harness.settings,
            InboxGrantProvision(
                contract_version=1,
                command_id=uuid4(),
                owner_id=OWNER,
                integration_id=INTEGRATION,
                channel="TELEGRAM",
                agent_id=AGENT,
                expected_binding_generation=1,
            ),
        )
    )
    assert blocker_pid is not None
    await observed_waiters(sql, blocker_pid, 1, "generic-control-before-first-provision")
    release_control.set()
    controlled = await control_task
    assert controlled.control_mode == "HUMAN"
    with pytest.raises(DomainError) as rejected:
        await provision_task
    assert rejected.value.code == ErrorCode.HANDOFF_CONFLICT
    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert conversation is not None and conversation.control_mode == "HUMAN"
        assert scope is not None and not scope.active and scope.grant_generation == 0
        assert await session.get(HandoffCase, ids["case_id"]) is None


@pytest.mark.asyncio
async def test_cp_revoke_hides_case_and_denies_applied_owner_command_replay(
    inbox_harness, user_me_transport
):
    ids = await seed_case(inbox_harness.database)
    claimed = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/claim",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": 1,
            "expected_case_revision": 1,
        },
        headers=browser_headers("session-a", write=True),
    )
    assert claimed.status_code == 200, claimed.text
    case = claimed.json()["case"]
    send_command = {
        "command_id": str(uuid4()),
        "expected_authority_revision": case["authority_revision"],
        "expected_case_revision": case["case_revision"],
        "expected_assignment_revision": case["assignment_revision"],
        "text": "The operator is taking over.",
    }
    sent = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/messages",
        json=send_command,
        headers=browser_headers("session-a", write=True),
    )
    assert sent.status_code == 200, sent.text
    intent_id = UUID(sent.json()["receipt"]["outbound_intent_id"])

    revoke_request = CpGrantRevoke(
        contract_version=1,
        lifecycle_source="CONTROL_PLANE_BINDING",
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        source_binding_generation=1,
        target_binding_generation=2,
    )
    path = "/internal/v1/inbox-grants/revoke"
    unauthorized = await inbox_harness.client.post(
        path,
        json=revoke_request.model_dump(mode="json"),
        headers=inbox_harness.full_headers,
    )
    assert unauthorized.status_code == 404
    revoked = await inbox_harness.client.post(
        path,
        json=revoke_request.model_dump(mode="json"),
        headers=inbox_harness.cp_lifecycle_headers,
    )
    assert revoked.status_code == 200, revoked.text
    assert revoked.json()["active"] is False
    assert revoked.json()["grant_generation"] == 2
    replay = await inbox_harness.client.post(
        path,
        json=revoke_request.model_dump(mode="json"),
        headers=inbox_harness.cp_lifecycle_headers,
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["replayed"] is True
    assert replay.json()["grant_generation"] == 2

    hidden_list = await inbox_harness.client.get(
        "/public/v1/inbox/cases", headers=browser_headers("session-a")
    )
    hidden_case = await inbox_harness.client.get(
        f"/public/v1/inbox/cases/{ids['case_id']}",
        headers=browser_headers("session-a"),
    )
    denied_replay = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/messages",
        json=send_command,
        headers=browser_headers("session-a", write=True),
    )
    assert hidden_list.status_code == 200 and hidden_list.json()["items"] == []
    assert hidden_case.status_code == 404
    assert denied_replay.status_code == 404
    for response in (hidden_list, hidden_case, denied_replay):
        assert response.headers["cache-control"] == "no-store"

    async with inbox_harness.database.session_factory() as session:
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        case_row = await session.get(HandoffCase, ids["case_id"])
        intent = await session.get(DeliveryAttempt, intent_id)
        message_count = await session.scalar(
            select(func.count()).select_from(Message).where(
                Message.conversation_id == ids["conversation_id"]
            )
        )
        receipt_count = await session.scalar(
            select(func.count()).select_from(InboxCommandReceipt).where(
                InboxCommandReceipt.conversation_id == ids["conversation_id"]
            )
        )
        assert scope is not None and not scope.active
        assert scope.cp_binding_high_water == 2 and scope.grant_generation == 2
        assert case_row is not None and case_row.status == "OPEN"
        assert case_row.assigned_account_id is None
        assert intent is not None and intent.status == "SUPERSEDED"
        assert intent.writer_state == "NONE"
        assert message_count == 2
        assert receipt_count == 2


@pytest.mark.parametrize("managed_version", [1, 2])
@pytest.mark.asyncio
async def test_revoke_retires_unmarked_legacy_claim_before_first_dispatch(
    inbox_harness, managed_version
):
    ids = await seed_case(inbox_harness.database)
    turn_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.control_mode = "AGENT"
        session.add(
            AgentTurn(
                id=turn_id,
                conversation_id=conversation.id,
                selection_key=uuid4(),
                creation_fingerprint="a" * 64,
                input_first_sequence=1,
                input_last_sequence=1,
                context_last_sequence=1,
                input_revision=1,
                authority_revision=conversation.authority_revision,
                status="PENDING",
                managed_contract_version=managed_version,
                managed_context_first_sequence=1,
                managed_projection_agent_id=AGENT if managed_version == 2 else None,
                managed_projection_fingerprint="b" * 64 if managed_version == 2 else None,
            )
        )
    principal = inbox_harness.settings.auth_principals[1].principal()
    claim = await turns.claim_turn(
        inbox_harness.database,
        principal,
        turn_id,
        TurnClaim(worker_ref="legacy-dispatch-worker", lease_seconds=60),
    )

    revoked = await inbox_harness.client.post(
        "/internal/v1/inbox-grants/revoke",
        json=CpGrantRevoke(
            contract_version=1,
            lifecycle_source="CONTROL_PLANE_BINDING",
            command_id=uuid4(),
            owner_id=OWNER,
            integration_id=INTEGRATION,
            channel="TELEGRAM",
            agent_id=AGENT,
            source_binding_generation=1,
            target_binding_generation=2,
        ).model_dump(mode="json"),
        headers=inbox_harness.cp_lifecycle_headers,
    )
    assert revoked.status_code == 200, revoked.text
    async with inbox_harness.database.session_factory() as session:
        retired = await session.get(AgentTurn, turn_id)
        assert retired is not None
        assert retired.retired_at is not None
        assert retired.managed_dispatch_epoch is None

    class NoDispatchPort:
        async def execute(self, *_args, **_kwargs):
            pytest.fail("a revoked unmarked Telegram Turn must not call the managed AI")

    proof = TurnExecute(
        claim_epoch=claim.turn.claim_epoch,
        claim_token=SecretStr(claim.claim_token),
    )
    with pytest.raises(DomainError) as rejected:
        await managed_execution.execute_claimed_turn(
            inbox_harness.database,
            principal,
            turn_id,
            proof,
            inbox_harness.settings,
            NoDispatchPort(),
        )
    assert rejected.value.code == ErrorCode.CONTROL_MODE_CONFLICT
    async with inbox_harness.database.session_factory() as session:
        retained = await session.get(AgentTurn, turn_id)
        assert retained is not None
        assert retained.status == "CLAIMED"
        assert retained.retired_at is not None
        assert retained.managed_dispatch_epoch is None


@pytest.mark.parametrize("lifecycle_source", ["CONTROL_PLANE_BINDING", "AI_AGENT"])
@pytest.mark.asyncio
async def test_revoke_high_water_rejects_paused_stale_proof_then_allows_fresh_rebind(
    inbox_harness, user_me_transport, monkeypatch, lifecycle_source
):
    ids = await seed_case(inbox_harness.database)
    claim = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/claim",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": 1,
            "expected_case_revision": 1,
        },
        headers=browser_headers("session-a", write=True),
    )
    assert claim.status_code == 200, claim.text
    old_case = claim.json()["case"]
    stale_assignment_send = {
        "command_id": str(uuid4()),
        "expected_authority_revision": old_case["authority_revision"],
        "expected_case_revision": old_case["case_revision"],
        "expected_assignment_revision": old_case["assignment_revision"],
        "text": "An old assignment must not send this.",
    }
    stale_request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=1,
    )
    proof_ready = asyncio.Event()
    release_provision = asyncio.Event()

    async def pause_after_current_proofs(_settings, request):
        pair = lifecycle_proofs(request)
        proof_ready.set()
        await release_provision.wait()
        return pair

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", pause_after_current_proofs)
    stale_provision = asyncio.create_task(
        inbox_lifecycle.provision_grant(
            inbox_harness.database, inbox_harness.settings, stale_request
        )
    )
    await asyncio.wait_for(proof_ready.wait(), timeout=3)
    if lifecycle_source == "CONTROL_PLANE_BINDING":
        revoke_request = CpGrantRevoke(
            contract_version=1,
            lifecycle_source="CONTROL_PLANE_BINDING",
            command_id=uuid4(),
            owner_id=OWNER,
            integration_id=INTEGRATION,
            channel="TELEGRAM",
            agent_id=AGENT,
            source_binding_generation=1,
            target_binding_generation=2,
        )
    else:
        revoke_request = AiGrantRevoke(
            contract_version=1,
            lifecycle_source="AI_AGENT",
            command_id=uuid4(),
            owner_id=OWNER,
            integration_id=INTEGRATION,
            channel="TELEGRAM",
            agent_id=AGENT,
            agent_owner_id=ACCOUNT,
            source_agent_lifecycle_generation=1,
            target_agent_lifecycle_generation=2,
        )
    revoked = await inbox_lifecycle.revoke_grant(
        inbox_harness.database, inbox_harness.settings, revoke_request
    )
    assert revoked.active is False
    release_provision.set()
    with pytest.raises(DomainError) as stale:
        await stale_provision
    assert stale.value.code == ErrorCode.GRANT_CONFLICT
    async with inbox_harness.database.session_factory() as session:
        scope = await session.get(InboxGrantScope, (OWNER, INTEGRATION, "TELEGRAM", AGENT))
        assert scope is not None and not scope.active
        if lifecycle_source == "CONTROL_PLANE_BINDING":
            assert scope.cp_binding_high_water == 2
        else:
            assert scope.agent_lifecycle_high_water == 2

    fresh_request = InboxGrantProvision(
        contract_version=1,
        command_id=uuid4(),
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        expected_binding_generation=(3 if lifecycle_source == "CONTROL_PLANE_BINDING" else 1),
    )

    async def fresh_proofs(_settings, request):
        return lifecycle_proofs(
            request,
            binding_generation=(3 if lifecycle_source == "CONTROL_PLANE_BINDING" else 1),
            agent_generation=(3 if lifecycle_source == "AI_AGENT" else 1),
        )

    monkeypatch.setattr(inbox_lifecycle, "_current_proofs", fresh_proofs)
    provisioned = await inbox_lifecycle.provision_grant(
        inbox_harness.database, inbox_harness.settings, fresh_request
    )
    assert provisioned.active is True
    assert provisioned.grant_generation == 3
    details = await inbox_harness.client.get(
        f"/public/v1/inbox/cases/{ids['case_id']}",
        headers=browser_headers("session-a"),
    )
    assert details.status_code == 200, details.text
    current_case = details.json()["case"]
    reclaimed = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/claim",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": current_case["authority_revision"],
            "expected_case_revision": current_case["case_revision"],
        },
        headers=browser_headers("session-a", write=True),
    )
    assert reclaimed.status_code == 200, reclaimed.text
    stale_send = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/messages",
        json=stale_assignment_send,
        headers=browser_headers("session-a", write=True),
    )
    assert stale_send.status_code == 409, stale_send.text
    async with inbox_harness.database.session_factory() as session:
        human_messages = await session.scalar(
            select(func.count()).select_from(Message).where(
                Message.conversation_id == ids["conversation_id"],
                Message.actor == "HUMAN",
            )
        )
        assert human_messages == 0


@pytest.mark.asyncio
async def test_browser_consent_email_and_write_origin_matrix(
    inbox_harness, user_me_transport
):
    ids = await seed_case(inbox_harness.database)
    case_path = f"/public/v1/inbox/cases/{ids['case_id']}"
    user_me_transport["consent-required"] = (
        200,
        json.dumps({
            "id": str(ACCOUNT),
            "status": "active",
            "email_verified": True,
            "consent_required": True,
        }).encode(),
    )
    user_me_transport["unverified-email"] = (
        200,
        json.dumps({
            "id": str(ACCOUNT),
            "status": "active",
            "email_verified": False,
            "consent_required": False,
        }).encode(),
    )
    user_me_transport["inactive-user"] = (
        200,
        json.dumps({
            "id": str(ACCOUNT),
            "status": "inactive",
            "email_verified": True,
            "consent_required": False,
        }).encode(),
    )
    consent = await inbox_harness.client.get(
        case_path, headers=browser_headers("consent-required")
    )
    unverified = await inbox_harness.client.get(
        case_path, headers=browser_headers("unverified-email")
    )
    inactive = await inbox_harness.client.get(
        case_path, headers=browser_headers("inactive-user")
    )
    assert consent.status_code == 403
    assert unverified.status_code == 403
    assert inactive.status_code == 401
    for response in (consent, unverified, inactive):
        assert response.headers["cache-control"] == "no-store"

    inbox_harness.app.state.settings = inbox_harness.settings.model_copy(
        update={"inbox_require_verified_email": False}
    )
    allowed_unverified = await inbox_harness.client.get(
        case_path, headers=browser_headers("unverified-email")
    )
    assert allowed_unverified.status_code == 200

    claim_command = {
        "command_id": str(uuid4()),
        "expected_authority_revision": 1,
        "expected_case_revision": 1,
    }
    path = f"/public/v1/inbox/cases/{ids['case_id']}/claim"
    missing_origin = await inbox_harness.client.post(
        path,
        json=claim_command,
        headers=browser_headers("session-a"),
    )
    foreign_origin = await inbox_harness.client.post(
        path,
        json=claim_command,
        headers={
            **browser_headers("session-a", write=True),
            "Origin": "https://evil.example",
        },
    )
    duplicate_origin = await inbox_harness.client.post(
        path,
        json=claim_command,
        headers=[
            ("Cookie", "access_token=session-a.payload.signature"),
            ("Origin", "https://agents.chota.by"),
            ("Origin", "https://agents.chota.by"),
        ],
    )
    assert [
        missing_origin.status_code,
        foreign_origin.status_code,
        duplicate_origin.status_code,
    ] == [403, 403, 403]
    for response in (missing_origin, foreign_origin, duplicate_origin):
        assert response.headers["cache-control"] == "no-store"
    async with inbox_harness.database.session_factory() as session:
        case = await session.get(HandoffCase, ids["case_id"])
        receipt_count = await session.scalar(
            select(func.count()).select_from(InboxCommandReceipt).where(
                InboxCommandReceipt.conversation_id == ids["conversation_id"]
            )
        )
        assert case is not None and case.status == "OPEN"
        assert receipt_count == 0


async def claim_and_send_human(inbox_harness) -> tuple[dict[str, UUID], dict[str, object]]:
    ids = await seed_case(inbox_harness.database)
    claim = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/claim",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": 1,
            "expected_case_revision": 1,
        },
        headers=browser_headers("session-a", write=True),
    )
    assert claim.status_code == 200, claim.text
    summary = claim.json()["case"]
    sent = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/messages",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": summary["authority_revision"],
            "expected_case_revision": summary["case_revision"],
            "expected_assignment_revision": summary["assignment_revision"],
            "text": "I will take over this conversation.",
        },
        headers=browser_headers("session-a", write=True),
    )
    assert sent.status_code == 200, sent.text
    return ids, sent.json()


async def add_fresh_agent_turn(
    inbox_harness, conversation_id: UUID, *, context_first_sequence: int | None = None
) -> UUID:
    now = datetime.now(UTC)
    message_id, turn_id = uuid4(), uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, conversation_id)
        assert conversation is not None
        assert conversation.control_mode == "AGENT"
        assert conversation.handoff_resume_floor_sequence == conversation.last_message_sequence
        conversation.last_message_sequence += 1
        conversation.input_revision += 1
        sequence = conversation.last_message_sequence
        session.add(
            Message(
                id=message_id,
                conversation_id=conversation.id,
                sequence=sequence,
                input_revision=conversation.input_revision,
                actor="CUSTOMER",
                direction="INBOUND",
                text="Here is the additional information.",
                created_at=now,
            )
        )
        await session.flush()
        session.add(
            AgentTurn(
                id=turn_id,
                conversation_id=conversation.id,
                selection_key=uuid4(),
                creation_fingerprint="7" * 64,
                input_first_sequence=sequence,
                input_last_sequence=sequence,
                context_last_sequence=sequence,
                input_revision=conversation.input_revision,
                authority_revision=conversation.authority_revision,
                status="COMPLETED",
                claim_epoch=1,
                claim_token_digest=hashlib.sha256(b"fresh-agent-claim").hexdigest(),
                worker_ref="test-agent-worker",
                lease_expires_at=now + timedelta(minutes=1),
                started_at=now,
                completed_at=now,
                outcome="DRAFT_READY",
                draft_text="The agent can continue from the new information.",
                operation_refs=[],
                terminal_fingerprint="8" * 64,
                managed_contract_version=3,
                managed_context_first_sequence=context_first_sequence or sequence,
                managed_projection_agent_id=AGENT,
                managed_projection_fingerprint="9" * 64,
            )
        )
    return turn_id


async def telegram_work(inbox_harness):
    response = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/work",
        json={
            "contract_version": 2,
            "owner_id": OWNER,
            "integration_id": str(INTEGRATION),
            "agent_id": str(AGENT),
            "bot_id": 999,
        },
        headers=inbox_harness.service_headers,
    )
    assert response.status_code == 200, response.text
    return response.json()["items"]


def begin_request(item: dict[str, object]) -> dict[str, object]:
    payload = dict(item)
    payload.pop("text")
    payload["supplier_instance_id"] = str(uuid4())
    payload["process_incarnation_id"] = str(uuid4())
    return payload


@pytest.mark.asyncio
async def test_return_retires_never_acquired_human_intent_before_fresh_agent_delivery(
    inbox_harness, user_me_transport,
):
    ids, sent = await claim_and_send_human(inbox_harness)
    human_items = await telegram_work(inbox_harness)
    assert len(human_items) == 1
    assert human_items[0]["source_kind"] == "OUTBOUND_MESSAGE"
    human_intent_id = UUID(human_items[0]["outbound_intent_id"])

    case = sent["case"]
    returned = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/return-to-agent",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": case["authority_revision"],
            "expected_case_revision": case["case_revision"],
        },
        headers=browser_headers("session-a", write=True),
    )
    assert returned.status_code == 200, returned.text
    async with inbox_harness.database.session_factory() as session:
        intent = await session.get(DeliveryAttempt, human_intent_id)
        assert intent is not None
        assert intent.writer_state == "NONE"
        assert intent.status == "SUPERSEDED"

    await add_fresh_agent_turn(inbox_harness, ids["conversation_id"])
    agent_items = await telegram_work(inbox_harness)
    assert len(agent_items) == 1
    assert agent_items[0]["source_kind"] == "AGENT_TURN"


@pytest.mark.asyncio
async def test_post_return_agent_projection_retains_human_message_and_delivery_fact(
    inbox_harness, user_me_transport
):
    ids, sent = await claim_and_send_human(inbox_harness)
    returned = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/return-to-agent",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": sent["case"]["authority_revision"],
            "expected_case_revision": sent["case"]["case_revision"],
        },
        headers=browser_headers("session-a", write=True),
    )
    assert returned.status_code == 200, returned.text
    turn_id = await add_fresh_agent_turn(
        inbox_harness, ids["conversation_id"], context_first_sequence=1
    )

    async with inbox_harness.database.session_factory() as session:
        conversation = await session.get(Conversation, ids["conversation_id"])
        turn = await session.get(AgentTurn, turn_id)
        human_message = await session.scalar(
            select(Message).where(
                Message.conversation_id == ids["conversation_id"],
                Message.actor == "HUMAN",
                Message.direction == "OUTBOUND",
            )
        )
        assert conversation is not None and turn is not None and human_message is not None
        prepared = await _build_request(
            session,
            conversation,
            turn,
            inbox_harness.settings.managed_ai_bindings[0],
        )
    assert any(
        item.id == human_message.id and item.actor == "HUMAN"
        for item in prepared.request.messages
    )
    assert any(
        item.kind == "DELIVERY_OBSERVATION"
        and item.message_id == human_message.id
        and item.status == "SUPERSEDED"
        for item in prepared.request.communication_facts
    )
    assert any(
        item.kind == "AUTHORITY_EVENT" and item.to_mode == "AGENT"
        for item in prepared.request.communication_facts
    )


@pytest.mark.asyncio
async def test_live_human_writer_blocks_fresh_agent_until_original_task_joined(
    inbox_harness, user_me_transport
):
    ids, sent = await claim_and_send_human(inbox_harness)
    human_items = await telegram_work(inbox_harness)
    assert len(human_items) == 1
    human_item = human_items[0]
    begin = begin_request(human_item)
    begun = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/begin",
        json=begin,
        headers=inbox_harness.service_headers,
    )
    assert begun.status_code == 200, begun.text
    assert begun.json()["send"] is True
    assert begun.json()["writer_state"] == "LIVE"

    case = sent["case"]
    returned = await inbox_harness.client.post(
        f"/public/v1/inbox/cases/{ids['case_id']}/return-to-agent",
        json={
            "command_id": str(uuid4()),
            "expected_authority_revision": case["authority_revision"],
            "expected_case_revision": case["case_revision"],
        },
        headers=browser_headers("session-a", write=True),
    )
    assert returned.status_code == 200, returned.text
    async with inbox_harness.database.session_factory() as session:
        intent = await session.get(DeliveryAttempt, UUID(human_item["outbound_intent_id"]))
        assert intent is not None
        assert intent.writer_state == "LIVE"

    await add_fresh_agent_turn(inbox_harness, ids["conversation_id"])
    blocked_items = await telegram_work(inbox_harness)
    assert all(item["source_kind"] != "AGENT_TURN" for item in blocked_items)

    complete = dict(begin)
    complete.update(outcome="UNKNOWN", error_code="TIMEOUT")
    complete["writer_end"] = {
        "kind": "TASK_JOINED",
        "supplier_instance_id": begin["supplier_instance_id"],
        "process_incarnation_id": begin["process_incarnation_id"],
    }
    ended = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/complete",
        json=complete,
        headers=inbox_harness.service_headers,
    )
    assert ended.status_code == 200, ended.text
    assert ended.json()["outcome"] == "UNKNOWN"
    assert ended.json()["writer_state"] == "ENDED"
    agent_items = await telegram_work(inbox_harness)
    assert len(agent_items) == 1
    assert agent_items[0]["source_kind"] == "AGENT_TURN"


@pytest.mark.asyncio
async def test_legacy_reconcile_cannot_forge_live_agent_writer_result_after_handoff(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    work = await telegram_work(inbox_harness)
    assert len(work) == 1 and work[0]["source_kind"] == "AGENT_TURN"
    begin = begin_request(work[0])
    begun = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/begin",
        json=begin,
        headers=inbox_harness.service_headers,
    )
    assert begun.status_code == 200, begun.text
    assert begun.json()["writer_state"] == "LIVE"

    # Model the valid state after another managed Turn atomically hands this
    # conversation to the owner while the original supplier task is still live.
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.control_mode = "HUMAN"
        conversation.authority_revision += 1
        conversation.authority_input_floor_sequence = conversation.last_message_sequence

    generic = await inbox_harness.client.post(
        f"/internal/v1/delivery-attempts/{work[0]['outbound_intent_id']}/reconcile",
        json={
            "reconciliation_key": str(uuid4()),
            "expected_revision": 1,
            "status": "FAILED",
            "error_code": "TIMEOUT",
        },
        headers=inbox_harness.full_headers,
    )
    assert generic.status_code == 404, generic.text
    async with inbox_harness.database.session_factory() as session:
        intent = await session.get(DeliveryAttempt, UUID(work[0]["outbound_intent_id"]))
        assert intent is not None
        assert intent.status == "DISPATCH_REQUESTED"
        assert intent.writer_state == "LIVE"

    wrong_supplier = dict(begin)
    wrong_supplier["supplier_instance_id"] = str(uuid4())
    wrong_supplier["process_incarnation_id"] = str(uuid4())
    wrong_supplier.update(
        outcome="UNKNOWN",
        error_code="TIMEOUT",
        writer_end={
            "kind": "TASK_JOINED",
            "supplier_instance_id": wrong_supplier["supplier_instance_id"],
            "process_incarnation_id": wrong_supplier["process_incarnation_id"],
        },
    )
    wrong = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/complete",
        json=wrong_supplier,
        headers=inbox_harness.service_headers,
    )
    assert wrong.status_code == 409, wrong.text

    exact = dict(begin)
    exact.update(
        outcome="UNKNOWN",
        error_code="TIMEOUT",
        writer_end={
            "kind": "TASK_JOINED",
            "supplier_instance_id": begin["supplier_instance_id"],
            "process_incarnation_id": begin["process_incarnation_id"],
        },
    )
    completed = await inbox_harness.client.post(
        "/internal/v2/telegram/delivery/complete",
        json=exact,
        headers=inbox_harness.service_headers,
    )
    assert completed.status_code == 200, completed.text
    assert completed.json()["outcome"] == "UNKNOWN"
    assert completed.json()["writer_state"] == "ENDED"


async def _claimed_v3_turn_for_effect_race(inbox_harness, conversation_id: UUID) -> tuple[
    UUID, TurnExecute, Principal, ManagedTurnV3Request, ExternalEffectPermitRequest
]:
    claim_token = secrets.token_urlsafe(32)
    now = datetime.now(UTC)
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, conversation_id)
        assert conversation is not None
        turn = await session.scalar(
            select(AgentTurn).where(AgentTurn.conversation_id == conversation_id)
        )
        message = await session.scalar(
            select(Message).where(
                Message.conversation_id == conversation_id,
                Message.actor == "CUSTOMER",
                Message.direction == "INBOUND",
            )
        )
        assert turn is not None and message is not None
        request = ManagedTurnV3Request(
            contract_version=3,
            conversation_id=conversation.id,
            turn_id=turn.id,
            owner_id=OWNER,
            integration_id=INTEGRATION,
            agent_id=AGENT,
            channel="TELEGRAM",
            input_revision=1,
            authority_revision=0,
            input_first_sequence=1,
            input_last_sequence=1,
            context_last_sequence=1,
            messages=[
                V3Message(
                    id=message.id,
                    sequence=1,
                    actor="CUSTOMER",
                    direction="INBOUND",
                    content_type="TEXT",
                    text=message.text,
                )
            ],
            selected_input_message_ids=[message.id],
            communication_facts=[],
            prior_operation_refs=[],
        )
        prepared = prepare_v3_request(request)
        turn.status = "CLAIMED"
        turn.claim_epoch = 1
        turn.claim_token_digest = hashlib.sha256(claim_token.encode("ascii")).hexdigest()
        turn.worker_ref = "effect-race-worker"
        turn.lease_expires_at = now + timedelta(minutes=5)
        turn.started_at = now
        turn.completed_at = None
        turn.outcome = None
        turn.draft_text = None
        turn.failure_code = None
        turn.terminal_fingerprint = None
        turn.operation_refs = []
        turn.managed_agent_id = AGENT
        turn.managed_dispatch_epoch = 1
        turn.managed_dispatch_started_at = now
        turn.managed_request_fingerprint = prepared.sidecar_fingerprint
        turn.managed_v3_snapshot = request.model_dump(mode="json")
        turn.managed_v3_request_fingerprint = prepared.request_fingerprint
        turn.managed_v3_dispatch_id = prepared.original_dispatch_id
        session.add(
            TurnInputMessage(
                message_id=message.id,
                conversation_id=conversation.id,
                turn_id=turn.id,
                sequence=1,
                ordinal=0,
            )
        )
        turn_id = turn.id

    control_principal = inbox_harness.settings.auth_principals[0].principal()
    execution = TurnExecute(claim_epoch=1, claim_token=claim_token)
    permit_request = ExternalEffectPermitRequest(
        contract_version=1,
        conversation_id=conversation_id,
        turn_id=turn_id,
        origin_turn_id=turn_id,
        owner_id=OWNER,
        integration_id=INTEGRATION,
        channel="TELEGRAM",
        agent_id=AGENT,
        authority_revision=0,
        managed_request_fingerprint=prepared.sidecar_fingerprint,
        operation_namespace="main-chota.ai-business.v1",
        operation_id=uuid4(),
        operation_fingerprint="a" * 64,
    )
    return turn_id, execution, control_principal, request, permit_request


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["permit", "handoff"])
async def test_goal5_permit_and_handoff_share_one_postgres_linearization_order(
    inbox_harness, sql, first: str
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    turn_id, proof, principal, snapshot, permit_request = await _claimed_v3_turn_for_effect_race(
        inbox_harness, ids["conversation_id"]
    )
    control = HandoffControlRequest(
        type="HUMAN_HANDOFF",
        request_id=handoff_request_id(snapshot),
        reason="CUSTOMER_REQUEST",
        note=None,
    )
    effect_principal = Principal(
        principal_id="business-effect-test",
        role="BUSINESS_EFFECT",
        grants=principal.grants,
    )

    async with sql.begin() as blocker:
        blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        await blocker.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
            {"id": ids["conversation_id"]},
        )
        if first == "permit":
            permit_task = asyncio.create_task(
                effects.permit(inbox_harness.database, effect_principal, permit_request)
            )
            await observed_waiters(sql, int(blocker_pid), 1, "Goal5 permit before Handoff")
            handoff_task = asyncio.create_task(
                _finish_handoff(inbox_harness.database, principal, turn_id, proof, control, [])
            )
        else:
            handoff_task = asyncio.create_task(
                _finish_handoff(inbox_harness.database, principal, turn_id, proof, control, [])
            )
            await observed_waiters(sql, int(blocker_pid), 1, "Handoff before Goal5 permit")
            permit_task = asyncio.create_task(
                effects.permit(inbox_harness.database, effect_principal, permit_request)
            )
        await observed_waiters(sql, int(blocker_pid), 2, f"{first} wins Handoff permit race")

    handoff_result, permit_result = await asyncio.gather(
        handoff_task, permit_task, return_exceptions=True
    )
    assert not isinstance(handoff_result, Exception), repr(handoff_result)
    assert handoff_result.turn.status == "COMPLETED"
    assert handoff_result.turn.outcome == "NO_RESPONSE"
    if first == "permit":
        assert not isinstance(permit_result, Exception), repr(permit_result)
        assert permit_result.replayed is False
        replay = await effects.permit(
            inbox_harness.database,
            effect_principal,
            permit_request,
        )
        assert replay.replayed is True
        async with inbox_harness.database.session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(ExternalEffectPermit))
        assert count == 1
    else:
        assert isinstance(permit_result, effects.EffectServiceConflict)
        assert permit_result.reason.value == "AUTHORITY_CHANGED"
        async with inbox_harness.database.session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(ExternalEffectPermit))
        assert count == 0


class SealedUnknownPort:
    def __init__(self) -> None:
        self.dispatch_fingerprints: list[str] = []
        self.operation = V3OperationReference(
            origin="AI_AGENT_SERVICE", operation_id="tool-run-unknown", outcome="UNKNOWN"
        )

    @staticmethod
    def _correlation(prepared):
        request = prepared.request
        return {
            "contract_version": 3,
            "original_dispatch_id": prepared.original_dispatch_id,
            "request_fingerprint": prepared.request_fingerprint,
            "conversation_id": request.conversation_id,
            "turn_id": request.turn_id,
            "agent_id": request.agent_id,
            "input_revision": request.input_revision,
            "authority_revision": request.authority_revision,
            "input_first_sequence": request.input_first_sequence,
            "input_last_sequence": request.input_last_sequence,
            "context_last_sequence": request.context_last_sequence,
        }

    async def execute_v3(self, prepared, binding, request_id=""):
        self.dispatch_fingerprints.append(prepared.request_fingerprint)
        return ManagedTurnV3Result(
            **self._correlation(prepared),
            result_kind="EXECUTION_RESULT",
            processing_outcome="UNKNOWN",
            draft_text=None,
            error_code="TOOL_EXECUTION_UNKNOWN",
            operation_refs=[self.operation],
            control_request=None,
        )

    async def control_evidence_v3(self, request, binding):
        return ControlEvidenceResult(
            contract_version=3,
            evidence_kind="SEALED_NO_CONTROL",
            conversation_id=request.conversation_id,
            turn_id=request.turn_id,
            agent_id=request.agent_id,
            input_revision=request.input_revision,
            authority_revision=request.authority_revision,
            input_first_sequence=request.input_first_sequence,
            input_last_sequence=request.input_last_sequence,
            context_last_sequence=request.context_last_sequence,
            original_dispatch_id=request.original_dispatch_id,
            request_fingerprint=request.request_fingerprint,
            execution_state="SEALED",
            processing_outcome="UNKNOWN",
            error_code="TOOL_EXECUTION_UNKNOWN",
            control_request=None,
            operation_refs=[self.operation],
        )


@pytest.mark.asyncio
async def test_sealed_unknown_replay_is_terminal_and_fresh_customer_can_be_claimed(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    turn_id, proof, principal, _snapshot, _permit_request = await _claimed_v3_turn_for_effect_race(
        inbox_harness, ids["conversation_id"]
    )
    port = SealedUnknownPort()
    managed_settings = inbox_harness.settings.model_copy(
        update={"managed_ai_base_url": "http://ai-agent.test"}
    )
    result = await execute_claimed_turn_v3(
        inbox_harness.database,
        principal,
        turn_id,
        proof,
        managed_settings,
        port,
    )
    assert result.turn.status == "FAILED"
    assert result.turn.failure_code == "EXECUTION_UNKNOWN"
    assert [item.model_dump(mode="json") for item in result.turn.operation_refs] == [
        {"origin": "AI_AGENT_SERVICE", "operation_id": "tool-run-unknown", "outcome": "UNKNOWN"}
    ]
    assert len(port.dispatch_fingerprints) == 2
    assert len(set(port.dispatch_fingerprints)) == 1

    now = datetime.now(UTC)
    new_message_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        conversation.provider_thread_id = "123"
        conversation.last_message_sequence = 2
        conversation.input_revision = 2
        session.add(
            Message(
                id=new_message_id,
                conversation_id=conversation.id,
                sequence=2,
                input_revision=2,
                actor="CUSTOMER",
                direction="INBOUND",
                text="A fresh question after the uncertain tool run",
                created_at=now,
            )
        )
        session.add(
            TelegramAdmission(
                id=uuid4(),
                owner_id=OWNER,
                integration_id=INTEGRATION,
                agent_id=AGENT,
                channel="TELEGRAM",
                provider_namespace=NAMESPACE,
                provider_event_id="b999c123m2",
                provider_thread_id="123",
                update_id=2,
                chat_id="123",
                message_id="2",
                sender_id="123",
                bot_id="999",
                fingerprint="a" * 64,
                route="MANAGED",
                conversation_id=conversation.id,
                canonical_message_id=new_message_id,
                created_at=now,
            )
        )

    principal = inbox_harness.settings.auth_principals[0].principal()
    new_turn_id = await telegram.select_immediate(
        inbox_harness.database,
        principal,
        inbox_harness.settings,
    )
    assert new_turn_id is not None
    claimed = await turns.claim_turn(
        inbox_harness.database,
        principal,
        new_turn_id,
        TurnClaim(worker_ref="fresh-after-unknown", lease_seconds=30),
    )
    assert claimed.turn.status == "CLAIMED"


@pytest.mark.asyncio
async def test_more_than_64_human_outbound_facts_fails_before_ai_and_releases_turn(
    inbox_harness,
):
    ids = await seed_case(inbox_harness.database, control_mode="AGENT")
    now = datetime.now(UTC)
    turn_id = uuid4()
    async with inbox_harness.database.session_factory() as session, session.begin():
        conversation = await session.get(Conversation, ids["conversation_id"])
        assert conversation is not None
        for sequence in range(2, 67):
            session.add(
                Message(
                    id=uuid4(),
                    conversation_id=conversation.id,
                    sequence=sequence,
                    actor="HUMAN",
                    direction="OUTBOUND",
                    text=f"Historical operator reply {sequence}",
                    created_at=now,
                )
            )
        customer_id = uuid4()
        session.add(
            Message(
                id=customer_id,
                conversation_id=conversation.id,
                sequence=67,
                input_revision=2,
                actor="CUSTOMER",
                direction="INBOUND",
                text="Can the agent continue after a long operator history?",
                created_at=now,
            )
        )
        conversation.last_message_sequence = 67
        conversation.input_revision = 2
        await session.flush()
        turn = AgentTurn(
            id=turn_id,
            conversation_id=conversation.id,
            selection_key=uuid4(),
            creation_fingerprint="d" * 64,
            input_first_sequence=67,
            input_last_sequence=67,
            context_last_sequence=67,
            input_revision=2,
            authority_revision=conversation.authority_revision,
            settling_revision=conversation.settling_revision,
            status="PENDING",
            claim_epoch=0,
            operation_refs=[],
            created_at=now,
            managed_contract_version=3,
            managed_context_first_sequence=1,
            managed_projection_agent_id=AGENT,
            managed_projection_fingerprint="c" * 64,
        )
        session.add(turn)
        session.add(
            TurnInputMessage(
                message_id=customer_id,
                conversation_id=conversation.id,
                turn_id=turn.id,
                sequence=67,
                ordinal=0,
            )
        )

    principal = inbox_harness.settings.auth_principals[0].principal()
    claimed = await turns.claim_turn(
        inbox_harness.database,
        principal,
        turn_id,
        TurnClaim(worker_ref="context-limit-test", lease_seconds=30),
    )
    proof = TurnExecute(
        claim_epoch=claimed.turn.claim_epoch,
        claim_token=claimed.claim_token,
    )
    port = SealedUnknownPort()
    managed_settings = inbox_harness.settings.model_copy(
        update={"managed_ai_base_url": "http://ai-agent.test"}
    )
    result = await execute_claimed_turn_v3(
        inbox_harness.database,
        principal,
        turn_id,
        proof,
        managed_settings,
        port,
    )
    assert result.turn.status == "FAILED"
    assert result.turn.failure_code == "INVALID_RESULT"
    assert port.dispatch_fingerprints == []
    async with inbox_harness.database.session_factory() as session:
        current = await session.get(AgentTurn, turn_id)
        assert current is not None
        assert current.status == "FAILED"
        assert current.managed_v3_snapshot is None
