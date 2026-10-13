# Goal4B Telegram process acceptance

This explicit acceptance test exercises a real aiogram polling process, the
Conversation API, a separately restarted Conversation worker, a disposable
PostgreSQL 16 database, and networked deterministic peers for Control Plane,
Managed AI, legacy AI, and the Telegram Bot API. It does not use production
credentials or real Telegram.

Run it from `conversation-service` after starting an empty disposable local
PostgreSQL 16 database whose database name contains `test`:

```sh
TEST_DATABASE_URL='postgresql+asyncpg://<user>:<password>@127.0.0.1:<port>/<test_database>' \
TEST_DATABASE_DISPOSABLE=yes \
GOAL4B_TELEGRAM_REPO='/absolute/path/to/backend/telegram-gateway-service' \
GOAL4B_TELEGRAM_PYTHON='/absolute/path/to/backend/telegram-gateway-service/.venv/bin/python' \
.venv/bin/python -m pytest tests/telegram_cross_service/acceptance_http.py -v
```

The Conversation API and worker use the service-pinned Python 3.11 virtualenv;
the real Telegram Gateway uses its own service virtualenv. Child processes
receive a fresh allowlist containing only loopback ports for PostgreSQL and the
fixture peers. A guard probe confirms non-loopback connections are rejected.
The fake Bot API supports retained update replay, provider rejection, accepted
writes with dropped replies, and a held response for process-restart coverage.

The matrix checks managed and legacy routing, canonical identity and turn
retention, duplicate and conflicting provider identities, concurrent admission,
Gateway/API/worker restarts, sticky rollback, HTML parity, and truthful success,
failure, and unknown Telegram delivery evidence. It also verifies that a
harmless deterministic managed response creates no Business operation.
