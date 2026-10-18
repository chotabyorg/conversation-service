"""Bounded independent Voice execution against real disposable PostgreSQL."""

import asyncio
import time
from collections import Counter
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import text

import services.voice_worker as coordinator
import voice_worker as lifecycle
from services.voice_worker import VoiceWorker
from tests import test_voice
from tests.test_telegram import DeterministicManagedPort

vh = test_voice.vh


class HeldPort(DeterministicManagedPort):
    def __init__(self):
        super().__init__()
        self.release = asyncio.Event()
        self.started = asyncio.Queue()
        self.active = Counter()
        self.maximum = 0
        self.seen = []

    async def execute(self, prepared, binding, request_id="", on_partial=None, caller_phone=""):
        key = prepared.request.conversation_id
        self.active[key] += 1
        assert self.active[key] == 1, "A conversation must never execute overlapping turns"
        self.maximum = max(self.maximum, sum(self.active.values()))
        self.seen.append((key, prepared.request.input_first_sequence))
        self.started.put_nowait(key)
        try:
            await self.release.wait()
            return await super().execute(prepared, binding, request_id, on_partial, caller_phone)
        finally:
            self.active[key] -= 1


async def admit(h, count):
    calls = []
    for _ in range(count):
        call = dict(h, identity=dict(call_id=str(uuid4()), media_session_id=str(uuid4())))
        await test_voice.start(call)
        response = await test_voice.post(call, "utterances", sequence=1, text="synthetic input")
        assert response.status_code == 200
        calls.append(call)
    return calls


def instance(h, port, concurrency):
    settings = h["settings"].model_copy(update={
        "voice_worker_concurrency": concurrency, "voice_worker_lease_seconds": 5,
    })
    return VoiceWorker(h["database"], settings, port)


@pytest.mark.parametrize(("calls", "capacity"), [(5, 1), (5, 5), (8, 8), (8, 3)])
async def test_independent_calls_progress_with_bounded_execution_and_drain(
    vh, sql, calls, capacity,
):
    await admit(vh, calls)
    port = HeldPort()
    worker = instance(vh, port, capacity)
    task = asyncio.create_task(worker.run())
    try:
        started = {await asyncio.wait_for(port.started.get(), 5) for _ in range(capacity)}
        assert len(started) == capacity
        await asyncio.sleep(0.15)
        assert port.started.empty() and port.maximum == capacity
        assert await worker.ready()
        async with sql.connect() as connection:
            initial = (await connection.execute(text(
                "SELECT id, lease_expires_at FROM agent_turns WHERE status='CLAIMED'"
            ))).all()
        assert len(initial) == capacity
        worker.request_stop()
        assert not await worker.ready()
        # The drain owns all claims and renews them; no waiting call is admitted.
        await asyncio.sleep(1.9)
        async with sql.connect() as connection:
            renewed = dict((await connection.execute(text(
                "SELECT id, lease_expires_at FROM agent_turns WHERE status='CLAIMED'"
            ))).all())
        assert all(renewed[key] > expiry for key, expiry in initial)
        assert not task.done() and port.started.empty()
        port.release.set()
        await asyncio.wait_for(task, 5)
        assert port.calls == capacity and not worker.running
        async with sql.connect() as connection:
            assert await connection.scalar(text(
                "SELECT count(*) FROM agent_turns WHERE status='COMPLETED' AND claim_epoch=1"
            )) == capacity
    finally:
        worker.request_stop()
        port.release.set()
        await lifecycle.stop_and_drain(worker, task)


async def test_slow_call_does_not_hold_an_independent_call(vh):
    calls = await admit(vh, 2)
    port = HeldPort()
    worker = instance(vh, port, 2)
    task = asyncio.create_task(worker.run())
    try:
        first = await asyncio.wait_for(port.started.get(), 3)
        second = await asyncio.wait_for(port.started.get(), 3)
        assert first != second and port.calls == 0
        # A later utterance remains ordered behind its own active turn.
        for call in calls:
            assert (await test_voice.post(call, "utterances", sequence=2,
                                          text="synthetic second input")).status_code == 200
        await asyncio.sleep(0.15)
        assert port.started.empty() and len(port.seen) == 2
        port.release.set()
        later = {await asyncio.wait_for(port.started.get(), 3) for _ in range(2)}
        assert later == {first, second}
        assert [sequence for _, sequence in port.seen] == [1, 1, 2, 2]
    finally:
        worker.request_stop()
        port.release.set()
        await lifecycle.stop_and_drain(worker, task)
    assert port.calls == 4


async def test_two_workers_preserve_claim_uniqueness(vh, sql):
    await admit(vh, 5)
    port = HeldPort()
    workers = [instance(vh, port, 3) for _ in range(2)]
    tasks = [asyncio.create_task(worker.run()) for worker in workers]
    try:
        started = [await asyncio.wait_for(port.started.get(), 5) for _ in range(5)]
        await asyncio.sleep(0.15)
        assert len(set(started)) == 5 and port.started.empty()
    finally:
        for worker in workers:
            worker.request_stop()
        port.release.set()
        await asyncio.gather(*(lifecycle.stop_and_drain(w, t) for w, t in zip(workers, tasks)))
    assert port.calls == 5
    async with sql.connect() as connection:
        assert (await connection.execute(text(
            "SELECT count(*), max(claim_epoch) FROM agent_turns"
        ))).one() == (5, 1)


async def test_drain_deadline_settles_all_dispatched_turns_unknown_without_replay(
    vh, sql, monkeypatch,
):
    await admit(vh, 5)
    port = HeldPort()
    worker = instance(vh, port, 5)
    task = asyncio.create_task(worker.run())
    try:
        for _ in range(5):
            await asyncio.wait_for(port.started.get(), 5)
        monkeypatch.setattr(lifecycle, "MAX_DRAIN_SECONDS", 0.05)
        await asyncio.wait_for(lifecycle.stop_and_drain(worker, task), 5)
        async with sql.connect() as connection:
            rows = (await connection.execute(text(
                "SELECT status, failure_code, managed_dispatch_epoch FROM agent_turns"
            ))).all()
        assert rows == [("FAILED", "EXECUTION_UNKNOWN", 1)] * 5
        assert task.done() and not worker.running and not any(port.active.values())
        assert not await instance(vh, port, 5).run_once()
        assert port.calls == 0
    finally:
        port.release.set()
        await lifecycle.stop_and_drain(worker, task)


async def test_stop_wakes_idle_dispatcher(vh):
    worker = VoiceWorker(vh["database"], vh["settings"].model_copy(
        update={"voice_worker_poll_seconds": 10, "voice_worker_concurrency": 5}), vh["port"])
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)
    started = time.monotonic()
    await asyncio.wait_for(lifecycle.stop_and_drain(worker, task), 1)
    assert time.monotonic() - started < 1


def test_concurrency_is_opt_in_and_bounded(vh):
    settings = vh["settings"]
    assert settings.voice_worker_concurrency == 1
    for invalid in (0, 65):
        with pytest.raises(ValidationError):
            type(settings)(**{**settings.model_dump(by_alias=True),
                              "voice_worker_concurrency": invalid}, _env_file=None)


@pytest.mark.parametrize("boundary", ["select", "claim"])
async def test_stop_during_admission_owns_only_committed_claims(vh, sql, monkeypatch, boundary):
    await admit(vh, 2)
    worker = instance(vh, vh["port"], 2)
    entered, release = asyncio.Event(), asyncio.Event()
    module, name = ((coordinator.voice, "select_turn") if boundary == "select"
                    else (coordinator.turns, "claim_turn"))
    original = getattr(module, name)

    async def held(*args):
        entered.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(module, name, held)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(entered.wait(), 3)
        worker.request_stop()
        release.set()
        await asyncio.wait_for(task, 3)
        assert vh["port"].calls == (1 if boundary == "claim" else 0)
        async with sql.connect() as connection:
            assert (await connection.execute(text(
                "SELECT status, claim_epoch FROM agent_turns"
            ))).all() == [("COMPLETED", 1) if boundary == "claim" else ("PENDING", 0)]
    finally:
        release.set()
        await lifecycle.stop_and_drain(worker, task)
