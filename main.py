"""Main Chota Conversation application; no provider or AI execution."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.handlers import install_error_handlers
from api.health import router as health_router
from api.internal import router as internal_router
from api.middleware import BoundaryMiddleware
from core.config import Settings, load_settings
from core.logging import configure_logging, logger
from db.database import Database


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        active_settings = settings if settings is not None else load_settings()
        configure_logging(active_settings.log_level)
        # Injectable resources support independent application/restart tests.
        try:
            active_database = (
                database
                if database is not None
                else Database(active_settings.database_url.get_secret_value())
            )
        except Exception:
            raise RuntimeError("Invalid conversation-service database configuration.") from None
        application.state.settings = active_settings
        application.state.database = active_database
        logger.info("", extra={"stage": "startup", "code": "STARTED"})
        try:
            yield
        finally:
            await active_database.dispose()
            logger.info("", extra={"stage": "shutdown", "code": "STOPPED"})

    application = FastAPI(
        title="Main Chota Platform — Conversation Service",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    application.add_middleware(BoundaryMiddleware)
    install_error_handlers(application)
    application.include_router(health_router)
    application.include_router(internal_router)
    return application


app = create_app()
