"""Separately owned Website worker lifecycle; HTTP serves bounded probes only."""
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from core.config import Settings, load_settings
from core.logging import configure_logging
from db.database import Database
from services.managed_client import ManagedAIClient, ManagedAIPort
from services.website_worker import WebsiteWorker

# Current production client total 120 + 10 for claim/result bookkeeping. Keep 20
# seconds of the existing Pod grace 150 for cancellation/finalization and exit.
# This cap does not make longer client budgets operationally supported.
MAX_DRAIN_SECONDS = 130.0
DRAIN_HEADROOM_SECONDS = 10.0


async def stop_and_drain(worker: WebsiteWorker, task: asyncio.Task[None]) -> None:
    worker.request_stop()

    async def drain() -> None:
        try:
            await asyncio.wait({task}, timeout=min(
                worker.settings.managed_ai_total_seconds + DRAIN_HEADROOM_SECONDS,
                MAX_DRAIN_SECONDS))
        finally:
            if not task.done():
                # Existing conservative cancellation/UNKNOWN behavior at deadline.
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    owner = asyncio.create_task(drain(), name="website-worker-drain")
    while not owner.done():
        try:
            await asyncio.shield(owner)
        except asyncio.CancelledError:
            # Repeated lifecycle cancellation cannot dispose DB under owned work.
            continue
    owner.result()


def create_worker_app(settings: Settings | None = None, database: Database | None = None,
                      port: ManagedAIPort | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        active = settings if settings is not None else load_settings()
        configure_logging(active.log_level)
        db = database if database is not None else Database(active.database_url.get_secret_value())
        worker = WebsiteWorker(db, active, port if port is not None else ManagedAIClient(active))
        app.state.worker = worker

        async def start() -> None:
            # Worker never migrates and never consumes against an older schema.
            while not worker.stop_requested.is_set():
                if await db.ready():
                    if not worker.stop_requested.is_set():
                        await worker.run()
                    return
                await worker.wait_for_poll()
        task = asyncio.create_task(start(), name="website-coordinator")
        app.state.worker_task = task
        try:
            yield
        finally:
            await stop_and_drain(worker, task)
            await db.dispose()

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    @app.get("/health")
    async def health() -> JSONResponse:
        available = not app.state.worker_task.done()
        return JSONResponse(status_code=200 if available else 503,
                            content={"status": "ok" if available else "not_live",
                                     "service": "conversation-worker"})

    @app.get("/ready")
    async def ready() -> JSONResponse:
        available = await app.state.worker.ready()
        return JSONResponse(status_code=200 if available else 503,
                            content={"status": "ready" if available else "not_ready"})
    return app


if __name__ == "__main__":
    active_settings = load_settings()
    configure_logging(active_settings.log_level)
    uvicorn.run(create_worker_app(active_settings), host="0.0.0.0",
                port=active_settings.website_worker_health_port, access_log=False, log_config=None)
