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

    async def ready(self, expected_revision: str = "0010_voice_caller_number") -> bool:
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
                await connection.execute(text(
                    "SELECT c.id, i.message_id, r.delivery_id, r.ordinal, r.is_partial, "
                    "r.authorized_text, c.caller_number, e.sequence FROM voice_calls c "
                    "LEFT JOIN voice_inputs i ON i.call_id=c.id "
                    "LEFT JOIN voice_responses r ON r.call_id=c.id "
                    "LEFT JOIN voice_playback_events e ON e.response_id=r.id LIMIT 1"
                ))
                await connection.execute(text(
                    "SELECT id, draft_text, partial_draft_text FROM agent_turns LIMIT 1"
                ))
                await connection.execute(
                    text(
                        "SELECT id, last_message_sequence, authority_revision, "
                        "authority_input_floor_sequence, settling_revision "
                        "FROM conversations LIMIT 1"
                    )
                )
                await connection.execute(text(
                    "SELECT id,conversation_id,turn_id,origin_turn_id,operation_namespace,"
                    "operation_id,operation_fingerprint,request_fingerprint,agent_id,"
                    "authority_revision,authority_input_floor_sequence,split_input_floor_sequence "
                    "FROM external_effect_permits LIMIT 1"
                ))
                await connection.execute(text(
                    "SELECT id,input_revision,settling_revision,settling_policy_version,"
                    "settling_quiet_window_ms,settling_max_wait_ms FROM messages LIMIT 1"
                ))
                await connection.execute(text(
                    "SELECT a.id,t.turn_id,r.id,m.source_agent_turn_id, "
                    "s.ordinal,v.settling_revision,d.expected_settling_revision, "
                    "q.expected_settling_revision,q.result_settling_revision, "
                    "v.managed_contract_version,v.managed_context_first_sequence, "
                    "v.managed_projection_agent_id,v.managed_projection_fingerprint "
                    "FROM website_admissions a "
                    "LEFT JOIN website_immediate_turns t ON t.message_id=a.canonical_message_id "
                    "LEFT JOIN turn_input_messages s ON s.message_id=a.canonical_message_id "
                    "LEFT JOIN website_response_attempts r ON r.admission_id=a.id "
                    "LEFT JOIN agent_turns v ON v.id=t.turn_id "
                    "LEFT JOIN delivery_attempts d ON d.source_turn_id=v.id "
                    "LEFT JOIN delivery_reconciliations q ON q.delivery_attempt_id=d.id "
                    "LEFT JOIN messages m ON m.source_agent_turn_id=v.id LIMIT 1"
                ))
                await connection.execute(text(
                    "SELECT a.id,t.turn_id,r.id,m.source_inbound_event_id,s.ordinal, "
                    "v.settling_revision,d.expected_settling_revision, "
                    "q.expected_settling_revision,q.result_settling_revision "
                    "FROM telegram_admissions a "
                    "LEFT JOIN telegram_immediate_turns t "
                    "ON t.message_id=a.canonical_message_id "
                    "LEFT JOIN turn_input_messages s ON s.message_id=a.canonical_message_id "
                    "LEFT JOIN telegram_response_attempts r ON r.admission_id=a.id "
                    "LEFT JOIN agent_turns v ON v.id=t.turn_id "
                    "LEFT JOIN delivery_attempts d ON d.source_turn_id=v.id "
                    "LEFT JOIN delivery_reconciliations q ON q.delivery_attempt_id=d.id "
                    "LEFT JOIN messages m ON m.id=a.canonical_message_id LIMIT 1"
                ))
                return True
        except (SQLAlchemyError, OSError, TimeoutError):
            return False
