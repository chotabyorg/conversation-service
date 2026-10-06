# Main Chota Platform — Conversation Service

This repository implements the Main Chota communication foundation. It is separate
from the paused Chota 2.0 product (`chotabyorg/chota`, Project #6). Read README and
docs before changes. The accepted Main ADR and Project #3 are planning authority.

Dependencies remain exact `==` pins in requirements files; pyproject configures
tools only. Python 3.11, FastAPI/Pydantic 2, async SQLAlchemy 2, Alembic and real
PostgreSQL 16 form the foundation. Each concurrent operation owns its own session.

Preserve complete trusted grants, database uniqueness, Conversation-local locks,
post-lock database clock, claim fencing, immutable journal and replay precedence.
Change shared contracts in schemas/contracts.py deliberately with behavior tests.
Never expose/log tokens, DSNs, text, raw payloads, SQL parameters or exception repr.

Use only explicitly disposable local databases for tests. Set TEST_DATABASE_URL and
TEST_DATABASE_DISPOSABLE=yes; never fall back to shared/runtime DATABASE_URL. Run
migrations, pytest, Ruff, mypy and container build for changes affecting behavior.
Source authoring does not replace independent review or final verification.

Goal 1 has no production deployment or channel consumer. Do not add registry push,
deploy workflows, GitOps resources/image tags, public routes, AI/provider calls,
settling, BusinessOperation ledger, CRM or Voice implementation. Existing Main
runtime services remain outside this repository's changes. Goal 2 requires its own
authorization and preserves the legacy AI path.
