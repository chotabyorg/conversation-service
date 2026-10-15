# Main Chota Platform — Conversation Service

This repository implements the Main Chota communication foundation. It is separate
from the paused Chota 2.0 product (`chotabyorg/chota`, Project #6). Read README and
docs before changes. The accepted Main ADR and Project #3 are planning authority.

The user explicitly authorized the Voice/Conversation source integration on
2026-10-15, including both sides of the boundary, on a separate branch above
Goal5. This supersedes the historical no-Voice source scope below. It does not
authorize production activation, number routing, GitOps changes, or merging
Goal5. Keep the candidate Voice contract reviewable until Goal5 acceptance.
Use native Python/PostgreSQL verification; the user prohibited local Docker.

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

Historical Goal1 scope was foundation-only. Current explicit Goal4A authorizes
narrow Website admission/FIFO worker/transport/evidence-read, additive migrations,
manual image publication and independently reviewed GitOps production rollout.
Source authoring alone never authorizes a live mutation or smoke; preserve exact
Main-cluster/release/logging/widget-binding/no-business-effect gates. Production
cohorts start empty and only the dedicated technical session may later be selected.
Never add public routes, Goal5 settling, a BusinessOperation ledger, CRM policy,
Telegram migration or Voice work. Managed v1 DTOs remain the stable boundary.
