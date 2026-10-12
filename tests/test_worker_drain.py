"""Real PostgreSQL shutdown admission, lease retention and deadline behavior."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
from sqlalchemy import text

import services.website_worker as coordinator
import worker as lifecycle
from services.website_worker import WebsiteWorker
from tests import test_website
from tests.test_website import FakeAI, admit, payload, query

wh = test_website.wh


class HeldAI(FakeAI):
    def __init__(self):
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(self, prepared, binding, request_id=""):
        self.started.set()
        await self.release.wait()
        return await super().execute(prepared, binding, request_id)


async def test_stop_idle_wakes_poll_and_prevents_admission(wh):
    instance = WebsiteWorker(wh.database, wh.settings.model_copy(
        update={"website_worker_poll_seconds": 10}), wh.ai)
    task = asyncio.create_task(instance.run())
    await asyncio.sleep(0.05)
    instance.request_stop()
    await asyncio.wait_for(task, 1)
    assert not instance.running and not await instance.ready()
    await admit(wh, payload(wh))
    assert not await instance.run_once()
    assert not wh.ai.calls


async def test_stop_during_selection_does_not_claim(wh, monkeypatch, sql):
    await admit(wh, payload(wh))
    selected = asyncio.Event()
    release = asyncio.Event()
    original = coordinator.select_immediate

    async def delayed(*args):
        result = await original(*args)
        selected.set()
        await release.wait()
        return result

    monkeypatch.setattr(coordinator, "select_immediate", delayed)
    task = asyncio.create_task(wh.worker.run())
    await asyncio.wait_for(selected.wait(), 2)
    wh.worker.request_stop()
    release.set()
    await asyncio.wait_for(task, 2)
    async with sql.connect() as connection:
        assert (await connection.execute(text(
            "SELECT status, claim_epoch FROM agent_turns"))).one() == ("PENDING", 0)
    assert not wh.ai.calls


async def test_owned_execution_drains_with_lease_and_no_second_claim(wh, sql):
    first = payload(wh)
    await admit(wh, first)
    second = payload(wh, session_id=first["session_id"])
    await admit(wh, second)
    ai = HeldAI()
    instance = WebsiteWorker(wh.database, wh.settings, ai)
    task = asyncio.create_task(instance.run())
    await asyncio.wait_for(ai.started.wait(), 2)
    async with sql.connect() as connection:
        initial = await connection.scalar(text("SELECT lease_expires_at FROM agent_turns"))
    draining = asyncio.create_task(lifecycle.stop_and_drain(instance, task))
    await asyncio.sleep(0)
    assert not await instance.ready()
    await asyncio.sleep(1.9)  # Fixture lease5 renews every5/3, even while draining.
    async with sql.connect() as connection:
        renewed = await connection.scalar(text("SELECT lease_expires_at FROM agent_turns"))
    assert renewed > initial and not draining.done()
    ai.release.set()
    await asyncio.wait_for(draining, 2)
    assert len(ai.calls) == 1 and not instance.running
    assert (await query(wh, first))["state"] == "DRAFT_READY"
    assert (await query(wh, second))["state"] == "PENDING"
    async with sql.connect() as connection:
        assert (await connection.execute(text(
            "SELECT status, claim_epoch FROM agent_turns"))).all() == [("COMPLETED", 1)]
    # Retrieving the completed logical result does not execute a model a second time.
    assert (await query(wh, first))["state"] == "DRAFT_READY"
    assert len(ai.calls) == 1


async def test_claim_started_before_stop_completes_as_owned_work(wh, monkeypatch):
    value = payload(wh)
    await admit(wh, value)
    started = asyncio.Event()
    release = asyncio.Event()
    original = coordinator.turns.claim_turn

    async def delayed(*args):
        started.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(coordinator.turns, "claim_turn", delayed)
    task = asyncio.create_task(wh.worker.run())
    await asyncio.wait_for(started.wait(), 2)
    wh.worker.request_stop()
    release.set()
    await asyncio.wait_for(task, 2)
    assert (await query(wh, value))["state"] == "DRAFT_READY"
    assert len(wh.ai.calls) == 1


async def test_drain_deadline_retains_unknown_and_restart_never_redispatches(wh, sql, monkeypatch):
    value = payload(wh)
    await admit(wh, value)
    ai = HeldAI()
    instance = WebsiteWorker(wh.database, wh.settings, ai)
    task = asyncio.create_task(instance.run())
    await asyncio.wait_for(ai.started.wait(), 2)
    monkeypatch.setattr(lifecycle, "MAX_DRAIN_SECONDS", 0.05)
    await asyncio.wait_for(lifecycle.stop_and_drain(instance, task), 2)
    assert task.done() and task.cancelled()
    async with sql.connect() as connection:
        row = (await connection.execute(text(
            "SELECT status, failure_code, managed_dispatch_epoch FROM agent_turns"))).one()
    assert row == ("FAILED", "EXECUTION_UNKNOWN", 1)
    restarted = WebsiteWorker(wh.database, wh.settings, ai)
    assert not await restarted.run_once()
    assert ai.started.is_set() and not ai.calls


async def test_startup_shutdown_never_enters_run_and_disposes_after_join(wh, monkeypatch):
    database = SimpleNamespace(ready=AsyncMock(return_value=False), dispose=AsyncMock())
    entered = AsyncMock()
    monkeypatch.setattr(WebsiteWorker, "run", entered)
    app = lifecycle.create_worker_app(wh.settings, database, wh.ai)
    async with app.router.lifespan_context(app):
        await asyncio.sleep(0.02)
        assert not app.state.worker_task.done()
    assert app.state.worker_task.done()
    entered.assert_not_awaited()
    database.dispose.assert_awaited_once()


async def test_stop_readiness_is_false_before_execution_exits(wh):
    app = lifecycle.create_worker_app(wh.settings, wh.database, wh.ai)
    async with app.router.lifespan_context(app):
        app.state.worker.request_stop()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://test") as client:
            assert (await client.get("/ready")).status_code == 503


async def test_repeated_shutdown_cancellation_cannot_abandon_owned_execution(wh):
    value = payload(wh)
    await admit(wh, value)
    ai = HeldAI()
    instance = WebsiteWorker(wh.database, wh.settings, ai)
    task = asyncio.create_task(instance.run())
    await asyncio.wait_for(ai.started.wait(), 2)
    draining = asyncio.create_task(lifecycle.stop_and_drain(instance, task))
    await asyncio.sleep(0)
    draining.cancel()
    await asyncio.sleep(0)
    draining.cancel()
    await asyncio.sleep(0)
    assert not draining.done() and not task.done()
    ai.release.set()
    await asyncio.wait_for(draining, 2)
    assert task.done() and len(ai.calls) == 1
    assert (await query(wh, value))["state"] == "DRAFT_READY"


async def test_stop_during_ready_probe_cannot_report_ready(wh, monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_ready():
        entered.set()
        await release.wait()
        return True

    monkeypatch.setattr(wh.database, "ready", delayed_ready)
    instance = wh.worker
    instance.running = True
    await instance.run_once()  # Establish a current empty-queue poll timestamp.
    probe = asyncio.create_task(instance.ready())
    await asyncio.wait_for(entered.wait(), 2)
    instance.request_stop()
    release.set()
    assert not await asyncio.wait_for(probe, 2)
