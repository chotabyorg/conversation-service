"""Website credentials consume the real common ledger; suppliers are local fakes."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from core.errors import DomainError
from models import InboxGrantLifecycleReceipt, InboxGrantScope, WebsiteSessionCapability
from schemas.contracts import ErrorCode
from schemas.handoff import InboxGrantProvision, InboxGrantProvisioned
from schemas.website_session import WebsiteSessionScope
from services import inbox_lifecycle, website_session
from tests.conftest import app_client, error, observed_waiters
from tests.test_website_session import access, issued, scope

pytest_plugins = ["tests.test_website_session"]


@pytest_asyncio.fixture
async def suppliers(sh, monkeypatch):
    state = SimpleNamespace(
        account=uuid4(), cp=7, ai=11, calls=[], failure=None, channel="WEBSITE_WIDGET"
    )
    for field, value in dict(
        cp_handoff_base_url="https://cp.invalid",
        ai_handoff_base_url="https://ai.invalid",
        inbox_lifecycle_token=SecretStr("local-cp-fixture"),
        handoff_ai_lifecycle_token=SecretStr("local-ai-fixture"),
        website_session_ttl_seconds=86400,
    ).items():
        monkeypatch.setattr(sh.settings, field, value)

    async def proof(url, path, headers, request, response_model):
        state.calls.append(request)
        if state.failure is not None:
            raise DomainError(state.failure)
        value = dict(
            contract_version=1,
            owner_id=request.owner_id,
            integration_id=request.integration_id,
            channel=state.channel,
            agent_id=request.agent_id,
        )
        if path.endswith("integration-proof"):
            value.update(account_id=state.account, binding_generation=state.cp)
        else:
            value.update(agent_owner_id=state.account, agent_lifecycle_generation=state.ai)
        return response_model.model_validate(value)

    monkeypatch.setattr(inbox_lifecycle, "_post_proof", proof)
    return state


async def enroll(sh, suppliers, *, generation=1):
    """Seed an acknowledged common enrollment; the production Website gate stays off."""
    values = dict(
        scope(sh),
        contract_version=1,
        command_id=uuid4(),
        channel="WEBSITE_WIDGET",
        expected_binding_generation=suppliers.cp,
    )
    del values["binding_generation"]
    request = InboxGrantProvision.model_validate(values)
    fingerprint = inbox_lifecycle._wire_fingerprint(request)
    ack = InboxGrantProvisioned(
        contract_version=1,
        command_id=request.command_id,
        account_id=suppliers.account,
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        channel=request.channel,
        agent_id=request.agent_id,
        binding_generation=suppliers.cp,
        agent_owner_id=suppliers.account,
        agent_lifecycle_generation=suppliers.ai,
        grant_generation=generation,
        active=True,
        request_fingerprint=fingerprint,
        replayed=False,
    )
    async with sh.database.session_factory() as session, session.begin():
        row = await inbox_lifecycle._lock_scope(
            session,
            dict(
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
            ),
        )
        row.account_id = row.agent_owner_id = suppliers.account
        row.cp_binding_generation, row.agent_lifecycle_generation = suppliers.cp, suppliers.ai
        row.grant_generation, row.active = generation, True
        session.add(
            InboxGrantLifecycleReceipt(
                owner_id=request.owner_id,
                integration_id=request.integration_id,
                channel=request.channel,
                agent_id=request.agent_id,
                command_id=request.command_id,
                kind="PROVISION",
                request_fingerprint=fingerprint,
                acknowledgement=ack.model_dump(mode="json"),
            )
        )
    return request


async def content(sh, created, *, settings=None, database=None, **changes):
    settings = settings or sh.settings
    async with (database or sh.database).session_factory() as session, session.begin():
        root = await website_session.locked_access(
            session,
            settings.auth_principals[0].principal(),
            access(sh, created, **changes),
            settings,
        )
        return root.id


async def denied(sh, created, **changes):
    with pytest.raises(DomainError) as captured:
        await content(sh, created, **changes)
    assert captured.value.code == ErrorCode.NOT_FOUND


async def test_common_receipt_epoch_fresh_proofs_and_restart(sh, suppliers, database_url, sql):
    provision = await enroll(sh, suppliers)
    created = await issued(sh)
    root_id = await content(sh, created)
    async with sh.database.session_factory() as session:
        credential = await session.get(WebsiteSessionCapability, root_id)
        assert (
            credential.grant_generation,
            credential.agent_lifecycle_generation,
            credential.provision_command_id,
        ) == (1, 11, provision.command_id)
    cp_calls = [r for r in suppliers.calls if hasattr(r, "command_id")]
    assert len(cp_calls) == 2 and {r.command_id for r in cp_calls} == {provision.command_id}
    assert all(r.channel == "WEBSITE_WIDGET" for r in suppliers.calls)
    async with app_client(sh.settings, database_url) as (_, database):
        assert await content(sh, created, database=database) == root_id
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM conversations")) == 1
        for table in ("messages", "agent_turns", "handoff_cases", "delivery_attempts"):
            assert await conn.scalar(text(f"SELECT count(*) FROM {table}")) == 0


async def test_foundation_credential_is_never_promoted_by_later_enrollment(sh, suppliers):
    old = await issued(sh)
    await enroll(sh, suppliers)
    await denied(sh, old)
    assert suppliers.calls == []
    fresh = await issued(sh)
    await content(sh, fresh)


@pytest.mark.parametrize(
    "field,value",
    [
        ("owner_id", "other"),
        ("integration_id", "uuid"),
        ("agent_id", "uuid"),
        ("binding_generation", 8),
        ("session_id", "uuid"),
        ("session_token", "x" * 43),
        ("session_token", "bad"),
    ],
)
async def test_wrong_visitor_rejected_before_supplier_io(sh, suppliers, field, value):
    await enroll(sh, suppliers)
    created = await issued(sh)
    suppliers.calls.clear()
    await denied(sh, created, **{field: str(uuid4()) if value == "uuid" else value})
    assert suppliers.calls == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("cp", 8),
        ("ai", 12),
        ("account", "uuid"),
        ("channel", "TELEGRAM"),
        ("failure", ErrorCode.NOT_FOUND),
        ("failure", ErrorCode.GRANT_CONFLICT),
    ],
)
async def test_each_fresh_supplier_dimension_fails_closed(sh, suppliers, field, value, sql):
    await enroll(sh, suppliers)
    created = await issued(sh)
    setattr(suppliers, field, uuid4() if value == "uuid" else value)
    await denied(sh, created)
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM messages")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM agent_turns")) == 0


async def test_unavailable_proof_does_not_fall_back_to_credential(sh, suppliers):
    await enroll(sh, suppliers)
    created = await issued(sh)
    suppliers.failure = ErrorCode.SERVICE_UNAVAILABLE
    with pytest.raises(DomainError) as captured:
        await content(sh, created)
    assert captured.value.code == ErrorCode.SERVICE_UNAVAILABLE


async def test_revoke_floors_and_reenrollment_cannot_reactivate_old_credential(sh, suppliers):
    await enroll(sh, suppliers)
    old = await issued(sh)
    async with sh.database.session_factory() as session, session.begin():
        row = await session.scalar(select(InboxGrantScope).with_for_update())
        row.active, row.grant_generation = False, 2
        row.cp_binding_high_water, row.agent_lifecycle_high_water = 8, 12
    await denied(sh, old)
    response = await sh.call("POST", "/website/sessions", scope(sh))
    error(response, "NOT_FOUND", 404)
    suppliers.cp, suppliers.ai = 9, 13
    await enroll(sh, suppliers, generation=3)
    # Even with the old static binding still present, the durable epoch rejects it.
    await denied(sh, old)
    settings = sh.settings.model_copy(
        update={
            "website_bindings": (
                sh.settings.website_bindings[0].model_copy(update={"capability_generation": 9}),
            )
        }
    )
    fresh = await website_session.create(
        sh.database,
        settings.auth_principals[0].principal(),
        WebsiteSessionScope.model_validate(dict(scope(sh), binding_generation=9)),
        settings,
    )
    value = dict(
        session_id=str(fresh.session_id), session_token=fresh.session_token.get_secret_value()
    )
    await content(sh, value, settings=settings, binding_generation=9)
    await denied(sh, old, settings=settings, binding_generation=9)


async def test_scope_lock_wait_observes_revocation_before_protected_use(sh, suppliers, sql):
    await enroll(sh, suppliers)
    created = await issued(sh)
    async with sh.database.session_factory() as blocker, blocker.begin():
        row = await blocker.scalar(select(InboxGrantScope).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(content(sh, created))
        try:
            await observed_waiters(sql, pid, 1, "website_grant_revocation_before_access")
            row.active, row.grant_generation = False, 2
        finally:
            await blocker.commit()
    with pytest.raises(DomainError) as captured:
        await pending
    assert captured.value.code == ErrorCode.NOT_FOUND


@pytest.mark.parametrize("mutation", ["fingerprint", "ack_epoch", "missing"])
async def test_retained_provision_receipt_is_required_and_verified(sh, suppliers, mutation):
    await enroll(sh, suppliers)
    created = await issued(sh)
    async with sh.database.session_factory() as session, session.begin():
        row = await session.scalar(select(InboxGrantLifecycleReceipt))
        if mutation == "fingerprint":
            row.request_fingerprint = "0" * 64
        else:
            row.acknowledgement = dict(
                row.acknowledgement,
                grant_generation=2 if mutation == "missing" else 1,
                agent_lifecycle_generation=12,
            )
    suppliers.calls.clear()
    await denied(sh, created)
    assert suppliers.calls == []


@pytest.mark.parametrize("change", ["partial", "foreign_command"])
async def test_db_rejects_partial_epoch_and_cross_scope_provision(sh, suppliers, sql, change):
    await enroll(sh, suppliers)
    created = await issued(sh)
    root_id = await content(sh, created)
    with pytest.raises(IntegrityError):
        async with sql.begin() as conn:
            if change == "partial":
                await conn.execute(
                    text(
                        "UPDATE website_session_capabilities "
                        "SET grant_generation=NULL WHERE conversation_id=:id"
                    ),
                    {"id": root_id},
                )
            else:
                # An existing receipt belonging to another agent must not satisfy the FK.
                command_id = uuid4()
                await conn.execute(
                    text(
                        "INSERT INTO inbox_grant_lifecycle_receipts "
                        "(id,owner_id,integration_id,channel,agent_id,command_id,kind,"
                        "request_fingerprint,acknowledgement) SELECT :id,owner_id,integration_id,"
                        "channel,:agent,:command,kind,request_fingerprint,acknowledgement "
                        "FROM inbox_grant_lifecycle_receipts LIMIT 1"
                    ),
                    {"id": uuid4(), "agent": uuid4(), "command": command_id},
                )
                await conn.execute(
                    text(
                        "UPDATE website_session_capabilities "
                        "SET provision_command_id=:command WHERE conversation_id=:id"
                    ),
                    {"id": root_id, "command": command_id},
                )


async def test_common_website_enrollment_stays_disabled_before_transport(sh, suppliers):
    payload = dict(
        scope(sh),
        contract_version=1,
        command_id=str(uuid4()),
        channel="WEBSITE_WIDGET",
        expected_binding_generation=7,
    )
    del payload["binding_generation"]
    with pytest.raises(DomainError) as captured:
        await inbox_lifecycle.provision_grant(
            sh.database, sh.settings, InboxGrantProvision.model_validate(payload)
        )
    assert captured.value.code == ErrorCode.NOT_FOUND
    assert suppliers.calls == []
