"""Real PG visitor isolation, root collisions and expiry across lock contention."""
import asyncio
import hashlib
import secrets
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from core.config import Settings
from core.errors import DomainError
from models import Conversation, WebsiteSessionCapability
from schemas.contracts import ErrorCode
from schemas.website_session import WebsiteSessionAccess
from services import website_session
from tests.conftest import Harness, app_client, error, observed_waiters


@pytest_asyncio.fixture
async def sh(sql, database_url):
    grant = dict(owner_id="visitor-owner", integration_id=str(uuid4()),
                 channel="WEBSITE_WIDGET", provider_namespace="main-chota.website-widget.v1")
    tokens = {name: secrets.token_urlsafe(32) for name in ("a", "worker")}
    settings = Settings(database_url=database_url, auth_principals=[
        dict(principal_id="a", token=tokens["a"], role="WEBSITE", grants=[grant]),
        dict(principal_id="worker", token=tokens["worker"], role="PROCESSING", grants=[grant]),
    ], website_bindings=[dict(owner_id=grant["owner_id"], integration_id=grant["integration_id"],
                             agent_id=str(uuid4()), capability_generation=7)],
        website_session_capabilities_enabled=True, website_session_ttl_seconds=60,
        _env_file=None)
    async with app_client(settings, database_url) as (client, database):
        yield Harness(client, database, settings, tokens, {"a": grant})


def scope(sh):
    binding = sh.settings.website_bindings[0]
    return dict(owner_id=binding.owner_id, integration_id=str(binding.integration_id),
                agent_id=str(binding.agent_id), binding_generation=7)


async def issued(sh):
    response = await sh.call("POST", "/website/sessions", scope(sh))
    assert response.status_code == 201, response.text
    assert response.headers["Cache-Control"] == "no-store"
    return response.json()


def access(sh, created, **changes):
    value = dict(scope(sh), session_id=created["session_id"],
                 session_token=created["session_token"])
    value.update(changes)
    return WebsiteSessionAccess.model_validate(value)


async def authorized(sh, request, settings=None):
    async with sh.database.session_factory() as session, session.begin():
        return await website_session.locked_credential(
            session, sh.settings.auth_principals[0].principal(), request, settings or sh.settings,
        )


async def test_create_only_no_history_raw_secret_or_recoverable_response(sh, sql, database_url):
    first, second = await issued(sh), await issued(sh)
    assert first["session_id"] != second["session_id"]
    assert first["session_token"] != second["session_token"]
    assert len(first["session_token"]) == 43
    root = await authorized(sh, access(sh, first))
    assert root.provider_thread_id == first["session_id"]
    async with sql.connect() as conn:
        row = (await conn.execute(text(
            "SELECT token_digest,expires_at-issued_at AS ttl FROM website_session_capabilities "
            "WHERE conversation_id=:root"
        ), {"root": root.id})).one()
        assert row.token_digest == hashlib.sha256(first["session_token"].encode()).hexdigest()
        assert row.ttl.total_seconds() == 60
        assert await conn.scalar(text("SELECT count(*) FROM messages")) == 0
        assert await conn.scalar(text("SELECT count(*) FROM agent_turns")) == 0
        columns = set((await conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='website_session_capabilities'"
        ))).scalars())
        assert "session_token" not in columns
    # A new app/engine sees retained authorization; no in-process token registry.
    async with app_client(sh.settings, database_url) as (_, database):
        async with database.session_factory() as session, session.begin():
            restored = await website_session.locked_credential(
                session, sh.settings.auth_principals[0].principal(), access(sh, first), sh.settings)
            assert restored.id == root.id
    response = await sh.call("POST", "/website/sessions", dict(
        scope(sh), session_id=first["session_id"], session_token=first["session_token"]))
    error(response, "VALIDATION_ERROR", 422)
    assert first["session_token"] not in response.text
    assert first["session_token"] not in repr(access(sh, first))


@pytest.mark.parametrize("field,value", [
    ("owner_id", "another-owner"), ("integration_id", "uuid"), ("agent_id", "uuid"),
    ("binding_generation", 8), ("session_id", "uuid"),
    ("session_token", "x" * 43), ("session_token", "invalid"),
])
async def test_wrong_secret_or_any_identity_dimension_cannot_read_root(sh, field, value):
    created = await issued(sh)
    with pytest.raises(DomainError) as captured:
        changes = {field: str(uuid4()) if value == "uuid" else value}
        await authorized(sh, access(sh, created, **changes))
    assert captured.value.code == ErrorCode.NOT_FOUND


async def test_generation_advance_disable_binding_removal_and_role_deny(sh):
    created = await issued(sh)
    for settings in (
        sh.settings.model_copy(update={"website_session_capabilities_enabled": False}),
        sh.settings.model_copy(update={"website_bindings": ()}),
        sh.settings.model_copy(update={"website_bindings": (
            sh.settings.website_bindings[0].model_copy(update={"capability_generation": 8}),)}),
    ):
        with pytest.raises(DomainError) as captured:
            await authorized(sh, access(sh, created), settings)
        assert captured.value.code == ErrorCode.NOT_FOUND
    response = await sh.call("POST", "/website/sessions", scope(sh), principal="worker")
    error(response, "NOT_FOUND", 404)
    with pytest.raises(ValidationError):
        Settings(database_url=sh.settings.database_url,
                 auth_principals=sh.settings.auth_principals,
                 website_bindings=[sh.settings.website_bindings[0].model_copy(
                     update={"capability_generation": None})],
                 website_session_capabilities_enabled=True, _env_file=None)


async def test_default_off_has_no_root_side_effects(sh, sql, database_url):
    settings = sh.settings.model_copy(update={"website_session_capabilities_enabled": False})
    async with app_client(settings, database_url) as (client, _):
        response = await client.post("/internal/v1/website/sessions", json=scope(sh),
                                     headers=sh.headers())
        error(response, "NOT_FOUND", 404)
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM conversations")) == 0


async def test_collision_never_attaches_old_root_or_changes_credential(sh, sql, monkeypatch):
    created = await issued(sh)
    values = iter([UUID(created["session_id"]), uuid4()])
    monkeypatch.setattr(website_session, "uuid4", lambda: next(values))
    response = await sh.call("POST", "/website/sessions", scope(sh))
    error(response, "NOT_FOUND", 404)
    await authorized(sh, access(sh, created))
    async with sql.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM website_session_capabilities")) == 1
        assert await conn.scalar(text("SELECT count(*) FROM conversations")) == 1


async def test_old_uuid_only_root_is_not_credentialed(sh):
    session_id = uuid4()
    async with sh.database.session_factory() as session, session.begin():
        session.add(Conversation(owner_id=scope(sh)["owner_id"],
            integration_id=UUID(scope(sh)["integration_id"]), channel="WEBSITE_WIDGET",
            provider_thread_id=str(session_id)))
    with pytest.raises(DomainError) as captured:
        await authorized(sh, WebsiteSessionAccess.model_validate(dict(
            scope(sh), session_id=session_id, session_token=secrets.token_urlsafe(32))))
    assert captured.value.code == ErrorCode.NOT_FOUND


async def test_expiry_is_checked_after_actual_root_lock_wait(sh, sql):
    created = await issued(sh)
    root = await authorized(sh, access(sh, created))
    async with sql.begin() as conn:
        await conn.execute(text("UPDATE website_session_capabilities "
            "SET expires_at=clock_timestamp()+interval '600 milliseconds' "
            "WHERE conversation_id=:root"), {"root": root.id})
    async with sh.database.session_factory() as blocker, blocker.begin():
        await blocker.scalar(select(Conversation).where(
            Conversation.id == root.id).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(authorized(sh, access(sh, created)))
        try:
            await observed_waiters(sql, pid, 1, "website_session_expiry_after_root_lock")
            # Wait for DB time, not machine time or a guessed lease timeout.
            async with sql.connect() as observer:
                for _ in range(100):
                    expired = await observer.scalar(text(
                        "SELECT clock_timestamp()>=expires_at FROM website_session_capabilities "
                        "WHERE conversation_id=:root"), {"root": root.id})
                    if expired:
                        break
                    await asyncio.sleep(.02)
                assert expired
        except BaseException:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            raise
    with pytest.raises(DomainError) as captured:
        await pending
    assert captured.value.code == ErrorCode.NOT_FOUND


async def test_revocation_serializes_against_waiting_access(sh, sql):
    created = await issued(sh)
    root = await authorized(sh, access(sh, created))
    async with sh.database.session_factory() as blocker, blocker.begin():
        await blocker.scalar(select(Conversation).where(
            Conversation.id == root.id).with_for_update())
        pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
        pending = asyncio.create_task(authorized(sh, access(sh, created)))
        try:
            await observed_waiters(sql, pid, 1, "website_session_revocation_root_lock")
            capability = await blocker.get(WebsiteSessionCapability, root.id)
            capability.revoked_at = await website_session.db_clock(blocker)
        except BaseException:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            raise
    with pytest.raises(DomainError) as captured:
        await pending
    assert captured.value.code == ErrorCode.NOT_FOUND


async def test_database_rejects_cross_root_binding_and_excess_retention(sh, sql):
    created = await issued(sh)
    root = await authorized(sh, access(sh, created))
    for mutation in (
        "provider_thread_id='wrong-session'",
        "expires_at=issued_at+interval '25 hours'",
        "binding_generation=0",
    ):
        with pytest.raises(IntegrityError):
            async with sql.begin() as conn:
                await conn.execute(text("UPDATE website_session_capabilities SET " + mutation
                                        + " WHERE conversation_id=:root"), {"root": root.id})
