"""Website AI/HUMAN use the same intent, writer and evidence rules as Telegram."""
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from core.errors import DomainError
from models import AgentTurn, Conversation, DeliveryAttempt, HandoffCase, InboxGrantScope, Message
from schemas.handoff import ClaimCommand, HumanPrincipal, ReturnToAgentCommand, SendHumanCommand
from services import inbox
from services.common import db_clock
from services.text_delivery import (
    WriterCompletionEvidence,
    _ensure_agent_intent,
    _source,
    _valid_authority,
    begin_writer,
    complete_writer,
)
from services.website_worker import WebsiteWorker
from tests.test_website_grant import suppliers as supplier_fixture
from tests.test_website_session import sh as session_fixture
from tests.test_website_v3_execution import WebsiteV3Port
from tests.website_common_fixture import append, common_website, due

sh, suppliers, wh = session_fixture, supplier_fixture, common_website


async def run(wh, mode):
    await append(wh)
    await due(wh)
    assert await WebsiteWorker(wh.database, wh.settings, WebsiteV3Port(mode)).run_once()


async def load(session, intent_id):
    grant = await session.scalar(select(InboxGrantScope).with_for_update())
    root = await session.scalar(select(Conversation).with_for_update())
    intent = await session.scalar(select(DeliveryAttempt).where(DeliveryAttempt.id == intent_id)
                                   .with_for_update())
    source = await _source(session, intent, root, grant, grant.agent_id)
    assert source is not None
    return grant, root, intent, source


async def acquire(session, values, supplier, process, *, recover=False):
    grant, root, intent, source = values
    return await begin_writer(session, intent, root, source, recover_only=recover,
        current=_valid_authority(intent, source, root, grant),
        supplier_instance_id=supplier, process_incarnation_id=process,
        superseded_key=uuid4(), dispatch_key=uuid4())


async def test_website_agent_writer_is_one_shot_and_canonical_output_is_unique(wh):
    await run(wh, "DRAFT")
    async with wh.database.session_factory() as session, session.begin():
        grant = await session.scalar(select(InboxGrantScope).with_for_update())
        root = await session.scalar(select(Conversation).with_for_update())
        turn = await session.scalar(select(AgentTurn).with_for_update())
        intent = await _ensure_agent_intent(session, root, turn, grant, await db_clock(session))
        intent_id = intent.id
    supplier, process = uuid4(), uuid4()
    async with wh.database.session_factory() as session, session.begin():
        values = await load(session, intent_id)
        first = await acquire(session, values, supplier, process)
        retry = await acquire(session, values, supplier, process)
        assert first.send and not retry.send and retry.replayed
    evidence = WriterCompletionEvidence(supplier, process, "PROVIDER_ACCEPTED",
        "original-transport-acceptance", None, True)
    for replayed in (False, True):
        async with wh.database.session_factory() as session, session.begin():
            _grant, root, intent, source = await load(session, intent_id)
            result = await complete_writer(session, intent, root, source, evidence,
                recover_only=True, reconciliation_key=uuid4())
            assert result.replayed is replayed and intent.writer_state == "ENDED"
    async with wh.database.session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Message)
                                    .where(Message.actor == "AGENT")) == 1


async def human_intents(wh, suppliers, texts=("owner reply", "owner reply")):
    await run(wh, "HANDOFF")
    async with wh.database.session_factory() as session:
        case = await session.scalar(select(HandoffCase))
    principal = HumanPrincipal(account_id=suppliers.account, email_verified=True,
                                consent_required=False)
    claimed = await inbox.claim_case(wh.database, principal, case.id, ClaimCommand(
        command_id=uuid4(), expected_case_revision=case.case_revision,
        expected_authority_revision=case.authority_revision))
    current, intents = claimed.case, []
    for text in texts:
        sent = await inbox.send_human(wh.database, principal, case.id, SendHumanCommand(
            command_id=uuid4(), expected_case_revision=current.case_revision,
            expected_authority_revision=current.authority_revision,
            expected_assignment_revision=current.assignment_revision, text=text))
        current = sent.case
        intents.append(sent.receipt.outbound_intent_id)
    return principal, current, intents


async def test_website_human_unknown_does_not_end_writer_or_authorize_resend(wh, suppliers):
    principal, case, ids = await human_intents(wh, suppliers)
    supplier, process = uuid4(), uuid4()
    async with wh.database.session_factory() as session, session.begin():
        assert (await acquire(session, await load(session, ids[0]), supplier, process)).send
    unknown = WriterCompletionEvidence(supplier, process, "UNKNOWN", None, "TIMEOUT", False)
    async with wh.database.session_factory() as session, session.begin():
        _grant, root, intent, source = await load(session, ids[0])
        result = await complete_writer(session, intent, root, source, unknown,
                                       recover_only=True, reconciliation_key=uuid4())
        assert not result.replayed
        assert intent.writer_state == "LIVE" and intent.status == "UNKNOWN"
        second = await load(session, ids[1])
        deferred = await acquire(session, second, uuid4(), uuid4())
        assert not deferred.send and not deferred.replayed
        assert second[2].status == "GENERATED" and second[2].writer_state == "NONE"
        assert not (await acquire(session, (_grant, root, intent, source), supplier, process)).send
    command = ReturnToAgentCommand(command_id=uuid4(),
        expected_authority_revision=case.authority_revision,
        expected_case_revision=case.case_revision)
    with pytest.raises(DomainError):
        await inbox.return_to_agent(wh.database, principal, case.case_id, command)
    joined = WriterCompletionEvidence(supplier, process, "UNKNOWN", None, "TIMEOUT", True)
    async with wh.database.session_factory() as session, session.begin():
        _grant, root, intent, source = await load(session, ids[0])
        result = await complete_writer(session, intent, root, source, joined,
                                       recover_only=True, reconciliation_key=uuid4())
        assert not result.replayed
        assert intent.writer_state == "ENDED" and intent.status == "UNKNOWN"
    async with wh.database.session_factory() as session, session.begin():
        _grant, root, intent, source = await load(session, ids[0])
        repeated = await complete_writer(session, intent, root, source, joined,
                                         recover_only=True, reconciliation_key=uuid4())
        assert repeated.replayed
    returned = await inbox.return_to_agent(wh.database, principal, case.case_id, command)
    assert returned.case.status == "RETURNED"
    async with wh.database.session_factory() as session:
        assert (await session.get(DeliveryAttempt, ids[1])).status == "SUPERSEDED"


async def test_website_writer_completion_rejects_another_process(wh, suppliers):
    _principal, _case, ids = await human_intents(wh, suppliers)
    supplier, process = uuid4(), uuid4()
    async with wh.database.session_factory() as session, session.begin():
        await acquire(session, await load(session, ids[0]), supplier, process)
    async with wh.database.session_factory() as session, session.begin():
        _grant, root, intent, source = await load(session, ids[0])
        with pytest.raises(DomainError):
            await complete_writer(session, intent, root, source,
                WriterCompletionEvidence(supplier, uuid4(), "UNKNOWN", None, "TIMEOUT", True),
                recover_only=True, reconciliation_key=uuid4())
        assert intent.writer_state == "LIVE" and intent.status == "DISPATCH_REQUESTED"
