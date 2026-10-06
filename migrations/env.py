"""Dedicated PostgreSQL migration runner; no auth config or external services."""

import asyncio
import os

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

import models  # noqa: F401
from db.base import Base
from db.database import validated_url


def run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=Base.metadata,
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def online() -> None:
    engine = create_async_engine(
        validated_url(os.environ.get("DATABASE_URL", "")),
        poolclass=NullPool,
        echo=False,
        hide_parameters=True,
        connect_args={"timeout": 5, "server_settings": {"timezone": "UTC"}},
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    context.configure(
        dialect_name="postgresql",
        target_metadata=Base.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(online())
