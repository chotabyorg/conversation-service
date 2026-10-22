"""Private HTTP credential adapter: one frame, exact writer, bounded honest evidence."""
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from core.errors import DomainError
from models import (
    Conversation,
    DeliveryAttempt,
    DeliveryReconciliation,
    InboxGrantScope,
    Message,
    WebsiteSessionCapability,
)
from schemas.contracts import ErrorCode
from schemas.handoff import ReturnToAgentCommand
from services import inbox, website_delivery
from services.common import db_clock
from services.website_worker import WebsiteWorker
from tests.conftest import error
from tests.test_common_text_delivery import human_intents, load
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_session import scope
from tests.test_website_session import sh as session_fixture
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import append, common_website, due

sh, suppliers, wh = session_fixture, supplier_fixture, common_website
BASE = "/website/sessions/delivery/"


def visitor(wh):
    return dict(scope(wh), session_id=wh.created["session_id"],
                 session_token=wh.created["session_token"])


async def call(wh, action, value):
    return await wh.call("POST", BASE + action, value)


async def work(wh):
    await poll(wh)
    response = await call(wh, "work", dict(visitor(wh), **wh.poll_access))
    assert response.status_code == 200
    assert len(response.content) <= 65536
    assert wh.created["session_token"] not in response.text
    return response.json()


async def poll(wh):
    if hasattr(wh, "poll_access") and wh.clock.current < wh.poll_deadline:
        return wh.poll_access
    request = dict(visitor(wh), request_id=str(uuid4()), supplier_instance_id=str(uuid4()),
                   process_incarnation_id=str(uuid4()))
    response = await wh.call("POST", "/website/sessions/polls/begin", request)
    assert response.status_code == 200
    value = response.json()
    assert value["state"] == "ACQUIRED"
    wh.poll_access = dict(poll_id=value["poll_id"],
        supplier_instance_id=request["supplier_instance_id"],
        process_incarnation_id=request["process_incarnation_id"])
    wh.poll_deadline = datetime.fromisoformat(value["deadline_at"].replace("Z", "+00:00"))
    return wh.poll_access


def identity(wh, value):
    return dict(visitor(wh), **value["item"], **wh.poll_access, recover_only=False)


def completion(value, outcome="WRITE_COMPLETED", *, joined=True):
    return dict({key: item for key, item in value.items() if key != "poll_id"}, outcome=outcome,
        error_code="TIMEOUT" if outcome == "UNKNOWN" else None,
        writer_end=dict(kind="TASK_JOINED", supplier_instance_id=value["supplier_instance_id"],
            process_incarnation_id=value["process_incarnation_id"]) if joined else None)


class DraftPort(WebsiteV3Port):
    def __init__(self, text):
        super().__init__("DRAFT")
        self.text = text

    async def execute_v3(self, prepared, binding, request_id=""):
        result = await super().execute_v3(prepared, binding, request_id)
        return type(result).model_validate(dict(result.model_dump(), draft_text=self.text))


async def drafted(wh, text="common reply"):
    await append(wh)
    await due(wh)
    assert await WebsiteWorker(wh.database, wh.settings, DraftPort(text)).run_once()


async def test_idle_human_delivery_unknown_join_return_and_fresh_agent_input(wh, suppliers):
    principal, case, ids = await human_intents(wh, suppliers)
    value = await work(wh)
    assert value["state"] == "SEND_CANDIDATE" and value["frame"]["actor"] == "HUMAN"
    assert value["item"]["outbound_intent_id"] == str(ids[0])
    assert value["frame"]["delivery_id"] != str(ids[0])
    request = identity(wh, value)
    a, b = await asyncio.gather(call(wh, "begin", request), call(wh, "begin", request))
    assert a.status_code == b.status_code == 200
    assert sorted([a.json()["send"], b.json()["send"]]) == [False, True]
    denied = a.json() if not a.json()["send"] else b.json()
    assert denied["frame"] is None and denied["state"] == "RECOVER_ONLY"
    uncertain = await call(wh, "complete", completion(request, "UNKNOWN", joined=False))
    assert uncertain.status_code == 200 and len(uncertain.content) <= 4096
    assert uncertain.json()["recovery"]["status"] == "UNKNOWN"
    assert uncertain.json()["recovery"]["writer_state"] == "LIVE"
    recovery = await work(wh)
    assert recovery["state"] == "RECOVER_ONLY" and recovery["frame"] is None
    assert recovery["recovery"]["delivery_id"] == value["frame"]["delivery_id"]
    command = ReturnToAgentCommand(command_id=uuid4(),
        expected_authority_revision=case.authority_revision,
        expected_case_revision=case.case_revision)
    with pytest.raises(DomainError):
        await inbox.return_to_agent(wh.database, principal, case.case_id, command)
    joined = await call(wh, "complete", completion(request, "UNKNOWN"))
    assert joined.status_code == 200 and joined.json()["recovery"]["writer_state"] == "ENDED"
    returned = await inbox.return_to_agent(wh.database, principal, case.case_id, command)
    assert returned.case.status == "RETURNED" and (await work(wh))["state"] == "EMPTY"
    await drafted(wh)
    next_value = await work(wh)
    assert next_value["state"] == "SEND_CANDIDATE" and next_value["frame"]["actor"] == "AGENT"
    assert next_value["frame"]["delivery_id"] != value["frame"]["delivery_id"]
    next_request = identity(wh, next_value)
    assert (await call(wh, "begin", next_request)).json()["send"]
    accepted = await call(wh, "complete", completion(next_request))
    assert accepted.status_code == 200
    assert accepted.json()["recovery"]["status"] == "TRANSPORT_ACCEPTED"
    replay = await call(wh, "complete", completion(next_request))
    assert replay.status_code == 200 and replay.json()["replayed"]
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)
                                    .where(Message.actor == "AGENT")) == 1


@pytest.mark.parametrize("text", ["\x01" * 10000, "я" * 16000],
                         ids=["escaped-controls", "utf8-multibyte"])
async def test_serialized_work_and_begin_frames_fit_full_json_limit(wh, text):
    await drafted(wh, text)
    value = await work(wh)
    assert value["state"] == "SEND_CANDIDATE" and value["frame"]["text"] == text
    begun = await call(wh, "begin", identity(wh, value))
    assert begun.status_code == 200 and begun.json()["send"]
    assert len(begun.content) <= 65536 and begun.json()["frame"]["text"] == text


async def test_escaped_oversized_frame_never_acquires_writer_or_truncates(wh):
    text = "\x01" * 12000
    await drafted(wh, text)
    value = await work(wh)
    assert value["state"] == "FRAME_TOO_LARGE" and value["frame"] is None
    begun = await call(wh, "begin", identity(wh, value))
    assert begun.status_code == 200 and len(begun.content) <= 4096
    assert begun.json()["state"] == "RECOVER_ONLY" and not begun.json()["send"]
    assert value["recovery"]["status"] == begun.json()["recovery"]["status"] == "SUPERSEDED"
    async with wh.database.session_factory() as session:
        intent = await session.scalar(select(DeliveryAttempt))
        assert intent.writer_state == "NONE" and intent.status == "SUPERSEDED"
        assert intent.error_code is None


@pytest.mark.parametrize("path", ["work", "begin"])
async def test_oversized_first_human_intent_is_terminal_and_next_fifo_can_proceed(
    wh, suppliers, path,
):
    text = "\x01" * 12000
    _principal, _case, ids = await human_intents(wh, suppliers, (text, "short owner reply"))
    async with wh.database.session_factory() as session, session.begin():
        _grant, _root, first, source = await load(session, ids[0])
        descriptor = website_delivery._descriptor(first, source).model_dump(mode="json")
        assert source[2] == text
    await poll(wh)
    request = identity(wh, {"item": descriptor})
    result = await call(wh, "work", dict(visitor(wh), **wh.poll_access)) if path == "work" else (
        await call(wh, "begin", request))
    assert result.status_code == 200 and len(result.content) <= 4096
    assert result.json()["state"] == "FRAME_TOO_LARGE"
    assert result.json()["frame"] is None and result.json()["recovery"]["status"] == "SUPERSEDED"
    repeated = await call(wh, "begin", request)
    assert repeated.status_code == 200 and not repeated.json()["send"]
    next_value = await work(wh)
    assert next_value["item"]["outbound_intent_id"] == str(ids[1])
    assert next_value["frame"]["text"] == "short owner reply"
    assert (await call(wh, "begin", identity(wh, next_value))).json()["send"]
    async with wh.database.session_factory() as session:
        first = await session.get(DeliveryAttempt, ids[0])
        assert first.status == "SUPERSEDED" and first.writer_state == "NONE"
        assert first.error_code is None
        assert first.writer_started_at is first.writer_supplier_instance_id is None
        assert await session.scalar(select(func.count()).select_from(DeliveryReconciliation).where(
            DeliveryReconciliation.delivery_attempt_id == ids[0],
            DeliveryReconciliation.status == "SUPERSEDED")) == 1


@pytest.mark.parametrize("field", ["session_id", "session_token", "owner_id", "agent_id",
                                   "binding_generation", "integration_id"])
async def test_wrong_visitor_work_has_no_supplier_io_or_content(wh, suppliers, field):
    value = dict(visitor(wh), **await poll(wh))
    value[field] = ("x" * 43 if field == "session_token" else "foreign-owner"
        if field == "owner_id" else 8 if field == "binding_generation" else str(uuid4()))
    suppliers.calls.clear()
    error(await call(wh, "work", value), "NOT_FOUND", 404)
    assert suppliers.calls == []


async def test_proof_outage_fails_begin_without_acquisition(wh, suppliers):
    await drafted(wh)
    request = identity(wh, await work(wh))
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    error(await call(wh, "begin", request), "SERVICE_UNAVAILABLE", 503)
    async with wh.database.session_factory() as session:
        assert (await session.scalar(select(DeliveryAttempt))).writer_state == "NONE"
    suppliers.failure = None
    assert (await call(wh, "begin", request)).json()["send"]


async def test_original_completion_after_expiry_revoke_and_config_removal_is_metadata_only(
    wh, suppliers,
):
    await drafted(wh)
    request = identity(wh, await work(wh))
    assert (await call(wh, "begin", request)).json()["send"]
    async with wh.database.session_factory() as session, session.begin():
        grant = await session.scalar(select(InboxGrantScope).with_for_update())
        grant.active = False
        root = await session.scalar(select(Conversation).with_for_update())
        cap = await session.get(WebsiteSessionCapability, root.id)
        cap.revoked_at = await db_clock(session)
        wh.clock.advance_to(cap.expires_at + timedelta(seconds=1))
    settings = wh.settings.model_copy(update={"website_session_capabilities_enabled": False,
        "website_bindings": (), "managed_ai_bindings": ()})
    wh.client._transport.app.state.settings = settings
    suppliers.calls.clear()
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    wrong = completion(dict(request, process_incarnation_id=str(uuid4())))
    error(await call(wh, "complete", wrong), "INVALID_DELIVERY_TRANSITION", 409)
    result = await call(wh, "complete", completion(request))
    assert result.status_code == 200 and len(result.content) <= 4096
    assert set(result.json()) == {"recovery", "replayed"}
    assert result.json()["recovery"]["writer_state"] == "ENDED" and suppliers.calls == []
    error(await call(wh, "work", dict(visitor(wh), **wh.poll_access)), "NOT_FOUND", 404)
    error(await call(wh, "begin", request), "NOT_FOUND", 404)


async def test_private_descriptor_cannot_move_to_another_attempt_or_source(wh):
    await drafted(wh)
    request = identity(wh, await work(wh))
    for field in ("response_attempt_id", "delivery_id", "source_id"):
        changed = dict(request, **{field: str(uuid4())})
        error(await call(wh, "begin", changed), "DELIVERY_IDENTITY_CONFLICT", 409)
    assert (await call(wh, "begin", request)).json()["send"]


async def test_higher_live_writer_also_blocks_fresh_acquisition(wh, suppliers):
    _principal, _case, ids = await human_intents(wh, suppliers)
    first = await work(wh)
    async with wh.database.session_factory() as session, session.begin():
        root = await session.scalar(select(Conversation).with_for_update())
        later = await session.get(DeliveryAttempt, ids[1])
        later.writer_state, later.writer_started_at = "LIVE", await db_clock(session)
        later.writer_supplier_instance_id = later.writer_process_incarnation_id = uuid4()
        assert later.conversation_id == root.id
    begun = await call(wh, "begin", identity(wh, first))
    assert begun.status_code == 200 and not begun.json()["send"]
    assert (await work(wh))["state"] == "EMPTY"


async def test_unknown_fields_and_chunked_oversize_are_rejected_before_work(wh, suppliers):
    suppliers.calls.clear()
    error(await call(wh, "work", dict(visitor(wh), unexpected=True)), "VALIDATION_ERROR", 422)
    async def chunks():
        yield b" " * 32768
        yield b" " * 32769
    response = await wh.client.post("/internal/v1" + BASE + "work",
        content=chunks(), headers={**wh.headers(), "Content-Type": "application/json"})
    error(response, "PAYLOAD_TOO_LARGE", 413)
    assert suppliers.calls == []
