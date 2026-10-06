# Goal 2 actual two-service HTTP acceptance

This folder starts real Conversation and AI apps in separate Python processes,
uses real repositories and migrations on two distinct disposable PostgreSQL16
databases, and drives the real Conversation client through the real authenticated
AI managed route/shared core. Only model/KB/MCP providers, Redis and prompt clock
are test seams. No real provider credential or external provider socket is used.

Run from this Conversation candidate using its Python dependency environment.
Provide all inputs explicitly in private local configuration; never use shared or
runtime databases:

| Environment | Required value |
| --- | --- |
| TEST_DATABASE_URL | CS postgresql+asyncpg URL, 127.0.0.1, database name containing test |
| TEST_DATABASE_DISPOSABLE | yes |
| GOAL2_AI_TEST_DATABASE_URL | distinct AI postgresql+asyncpg URL, 127.0.0.1, database name containing test |
| GOAL2_AI_TEST_DATABASE_DISPOSABLE | yes |
| GOAL2_AI_REPO | absolute independently checked AI candidate path containing tests/goal2_cross_service_bootstrap.py |
| GOAL2_AI_PYTHON | absolute Python executable with that candidate's dependencies |

```text
python -m pytest -q -p no:cacheprovider tests/managed_cross_service/acceptance_http.py
```

Missing inputs fail setup. There is no auto-fetch/main fallback, test skip, .env
loading, SQLite, real Redis, xdist or production compose. Real CS and AI migrations
run before seed. The two DBs must be PG16; per-case cleanup truncates only these
explicitly designated DBs. Provision the Docker DB resources before runtime socket
guards; clean up only the resources created for your run after tests finish.

`acceptance_http.py` intentionally sits outside pytest's default `test_*.py` filename
pattern. The standard `python -m pytest` service gate and unchanged Quality workflow
therefore run the ordinary service suite without requiring the paired AI checkout.
The exact explicit-file command above is the separate mandatory paired gate and
must run before Goal2 acceptance; it never silently skips missing inputs.

Fixtures bind fresh listeners before spawning children and pass owned descriptors
to uvicorn, avoiding port reuse races. Both app processes have independent exact
numeric-loopback destination guards; pytest extends the foundation guard with only
these fresh ports and the two PG endpoints. Each child self-checks denial before
connecting to a forbidden TEST-NET destination. No provider DNS or sockets are
allowed. All fixture tokens/Fernet keys are generated privately at runtime.

The suite covers A-H: stable Turn/Message projection and draft separation, ordered
non-suffix/multiple/HUMAN/SYSTEM inputs, frozen prefix during in-flight execution,
deep-copy legacy/no-tool/KB/tool envelope parity, real stale lease/recovery/409,
KB fail-open, successful fake tool loops, current Bitrix/Google protocol-success
application-error envelopes, legacy continuation controls, unknown after a fake
accepted effect, timeout/client disconnect, abrupt CS process/engine restart and
complete scope isolation. The R1 retained-marker test loses the actual AI UNKNOWN
result while CS waits on an observed independent PG row lock, then kills/restarts
the owned process and recovers the same Turn without another AI HTTP/effect.

Private synthetic captures and safe process logs live under pytest's per-case temp
directory. Captures omit auth headers; logs are checked for synthetic error sentinel,
tokens, database passwords, Fernet key and synthetic model key. Canonical Messages,
counters, both delivery tables, AI Session count and Redis keys/method calls are
checked. Processes, requests/listeners and fixture files are owned by this run.

This author suite complements the service tests. It does not replace real pinned
stdio R2 same-task/LIFO/acquisition/cancellation/resource cleanup tests, auth-before-
receive/parser probes, DTO/context/tool caps, local corrupted response/commit
uncertainty tests, migrations/catalog, full legacy suite or independent review and
fresh final verification. A browser client disconnect is checked through actual
HTTP; repeated raw request-task cancellation remains an explicit service unit gate.
