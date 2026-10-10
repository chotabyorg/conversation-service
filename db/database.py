"""App-owned engine; domain operations open an independent session per call."""

from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError, SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def validated_url(value: str) -> URL:
    """Reject unsafe/unsupported configuration without including the supplied DSN."""
    try:
        url = make_url(value)
        if url.drivername != "postgresql+asyncpg" or not url.database or not url.host:
            raise ValueError
        if not url.username or (url.port is not None and not 1 <= url.port <= 65535):
            raise ValueError
    except (ArgumentError, ValueError, TypeError):
        raise ValueError("DATABASE_URL must be a dedicated PostgreSQL asyncpg URL") from None
    return url


class Database:
    def __init__(self, url: str) -> None:
        self.engine: AsyncEngine = create_async_engine(
            validated_url(url),
            echo=False,
            hide_parameters=True,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=10,
            isolation_level="READ COMMITTED",
            connect_args={
                "timeout": 5,
                "server_settings": {
                    "timezone": "UTC",
                    "statement_timeout": "15000",
                    "lock_timeout": "10000",
                },
            },
        )
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False, autoflush=False
        )

    async def dispose(self) -> None:
        await self.engine.dispose()

    async def ready(self, expected_revision: str = "0004_website_history") -> bool:
        try:
            async with self.engine.connect() as connection:
                if await connection.scalar(text("SELECT 1")) != 1:
                    return False
                versions = (
                    (await connection.execute(text("SELECT version_num FROM alembic_version")))
                    .scalars()
                    .all()
                )
                if versions != [expected_revision]:
                    return False
                await connection.execute(
                    text(
                        "SELECT id, last_message_sequence, authority_revision "
                        "FROM conversations LIMIT 1"
                    )
                )
                await connection.execute(text(
                    "SELECT a.id,t.turn_id,r.id,m.source_agent_turn_id, "
                    "v.managed_contract_version,v.managed_context_first_sequence, "
                    "v.managed_projection_agent_id,v.managed_projection_fingerprint "
                    "FROM website_admissions a "
                    "LEFT JOIN website_immediate_turns t ON t.message_id=a.canonical_message_id "
                    "LEFT JOIN website_response_attempts r ON r.admission_id=a.id "
                    "LEFT JOIN agent_turns v ON v.id=t.turn_id "
                    "LEFT JOIN messages m ON m.source_agent_turn_id=v.id LIMIT 1"
                ))
                return True
        except (SQLAlchemyError, OSError, TimeoutError):
            return False
