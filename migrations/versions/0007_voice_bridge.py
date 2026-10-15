"""Private Voice call ownership and playback facts."""
import sqlalchemy as sa
from alembic import op

revision = "0007_voice_bridge"
down_revision = "0006_turn_settling"
branch_labels = None
depends_on = None

TABLE_DDL = [
    """
CREATE TABLE voice_calls (
id UUID NOT NULL,
conversation_id UUID NOT NULL,
media_session_id UUID NOT NULL,
agent_id UUID NOT NULL,
last_sequence BIGINT NOT NULL,
started_at TIMESTAMP WITH TIME ZONE NOT NULL,
expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
ended_at TIMESTAMP WITH TIME ZONE,
end_reason VARCHAR(16),
CONSTRAINT pk_voice_calls PRIMARY KEY (id),
FOREIGN KEY(conversation_id) REFERENCES conversations (id),
CONSTRAINT uq_voice_call_parent UNIQUE (id, conversation_id),
CONSTRAINT ck_voice_sequence CHECK (last_sequence >= 0),
CONSTRAINT ck_voice_lease CHECK (expires_at > started_at),
CONSTRAINT ck_voice_end CHECK ((ended_at IS NULL AND end_reason IS NULL) OR (ended_at IS NOT
NULL AND ended_at >= started_at AND end_reason IS NOT NULL AND end_reason IN
('DISCONNECTED','TIMEOUT','CANCELLED','FAILED'))),
UNIQUE (conversation_id)
)
    """,
    """
CREATE TABLE voice_inputs (
call_id UUID NOT NULL,
sequence BIGINT NOT NULL,
conversation_id UUID NOT NULL,
message_id UUID NOT NULL,
fingerprint CHAR(64) NOT NULL,
CONSTRAINT pk_voice_inputs PRIMARY KEY (call_id, sequence),
FOREIGN KEY(call_id, conversation_id) REFERENCES voice_calls (id, conversation_id),
FOREIGN KEY(message_id, conversation_id) REFERENCES messages (id, conversation_id),
CONSTRAINT ck_voice_input_sequence CHECK (sequence > 0),
UNIQUE (message_id)
)
    """,
    """
CREATE TABLE voice_responses (
id UUID NOT NULL,
call_id UUID NOT NULL,
conversation_id UUID NOT NULL,
turn_id UUID NOT NULL,
delivery_id UUID NOT NULL,
send_authorized BOOLEAN NOT NULL,
last_event_sequence BIGINT NOT NULL,
generated_bytes BIGINT NOT NULL,
accepted_bytes BIGINT NOT NULL,
terminal BOOLEAN NOT NULL,
CONSTRAINT pk_voice_responses PRIMARY KEY (id),
CONSTRAINT uq_voice_response_call UNIQUE (id, call_id),
FOREIGN KEY(call_id, conversation_id) REFERENCES voice_calls (id, conversation_id),
FOREIGN KEY(turn_id, conversation_id) REFERENCES agent_turns (id, conversation_id),
FOREIGN KEY(delivery_id, conversation_id) REFERENCES delivery_attempts (id,
conversation_id),
CONSTRAINT ck_voice_response_counters CHECK (last_event_sequence >= 0 AND accepted_bytes >=
0 AND generated_bytes >= accepted_bytes AND generated_bytes <= 8388608),
UNIQUE (turn_id),
UNIQUE (delivery_id)
)
    """,
    """
CREATE TABLE voice_playback_events (
response_id UUID NOT NULL,
sequence BIGINT NOT NULL,
call_id UUID NOT NULL,
fingerprint CHAR(64) NOT NULL,
state VARCHAR(16) NOT NULL,
generated_bytes BIGINT NOT NULL,
accepted_bytes BIGINT NOT NULL,
terminal BOOLEAN NOT NULL,
accepted_at TIMESTAMP WITH TIME ZONE NOT NULL,
CONSTRAINT pk_voice_playback_events PRIMARY KEY (response_id, sequence),
FOREIGN KEY(response_id, call_id) REFERENCES voice_responses (id, call_id),
CONSTRAINT ck_voice_fact_counters CHECK (sequence > 0 AND accepted_bytes >= 0 AND
generated_bytes >= accepted_bytes AND generated_bytes <= 8388608),
CONSTRAINT ck_voice_fact_state CHECK (state IN
('NOT_STARTED','STARTED','PARTIAL','COMPLETED','INTERRUPTED','FAILED','UNKNOWN'))
)
    """,
]


def upgrade() -> None:
    for statement in TABLE_DDL:
        op.execute(sa.text(statement))

def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(sa.text(
        "LOCK TABLE voice_calls, voice_inputs, voice_responses, "
        "voice_playback_events IN ACCESS EXCLUSIVE MODE"
    ))
    if connection.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM voice_calls)")):
        raise RuntimeError("Voice journal is retained; downgrade refused")
    op.drop_table('voice_playback_events')
    op.drop_table('voice_responses')
    op.drop_table('voice_inputs')
    op.drop_table('voice_calls')
