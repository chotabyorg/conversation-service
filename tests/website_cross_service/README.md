# Goal4A Website process acceptance

This explicit suite starts the actual candidate Website entry point, Conversation
API and worker in separate Python processes. A separate local HTTP fake serves
Control Plane, managed AI, legacy AI and evidence-read. A real disposable
PostgreSQL16 database is required. It never invokes production/provider systems.

```sh
TEST_DATABASE_URL='<private numeric-loopback disposable PostgreSQL16 URL>' \
TEST_DATABASE_DISPOSABLE=yes \
GOAL4A_WEBSITE_REPO='<absolute reviewed Website worktree>' \
GOAL4A_WEBSITE_PYTHON='<absolute Website virtualenv Python>' \
python -m pytest tests/website_cross_service/acceptance_http.py -v
```

No dotenv or inherited production settings are allowed. Child processes receive
only fresh synthetic credentials and explicit fixture environment. Every child
has an exact loopback socket allowlist and verifies external `connect` and
`connect_ex` are denied. Test files/configuration are private temporary fixtures.
The harness's fake effect counter deliberately records **every** managed HTTP
execution, so fake idempotency cannot conceal a duplicate coordinator execution.
It establishes Conversation orchestration behavior, not Business adapter safety.

Bootstrap wrappers pause after positively committed production methods at A–E,
before Website begins transport at F, after intent COMMIT at G, and before
reconciliation after actual HTTP bytes at H. The test kills the actual process
and starts an unmodified instance against the same database. Production source
contains no fault injection mechanism. D pauses after durable dispatch marker
before the AI call, so call/effect count is zero and recovery is still UNKNOWN.
E pauses after the real fake AI HTTP result/effect before terminalization; count
is one, genuine refs are read and the same Turn becomes UNKNOWN without retry.
F–H resend the retained draft with fresh physical transport evidence, preserving
unconfirmed crash intents and never treating socket completion as browser ACK.

Concurrency tests hold real PostgreSQL row/advisory locks and inspect independent
`pg_stat_activity` PIDs/wait edges while public HTTP requests and worker processes
overlap. Scheduling tasks alone is not the concurrency evidence. Cases include
same event ID, FIFO Messages, two workers, two sessions, unrelated integration,
and old/new gateway policy races. Legacy routing preserves existing behavior and
does not gain an AI execution deduplication guarantee from a route reservation.
