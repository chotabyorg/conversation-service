# Real Website Gateway and Conversation HTTP profile

`credential_gateway_http.py` is an explicit opt-in profile, outside ordinary pytest
discovery. Supply `TRACK_WEBSITE_FIXTURE_INPUT` from `prepare_gateway_fixture.py`
and the ordinary disposable PostgreSQL 16 test variables. Sources must be clean;
the fixture pins and checks the Gateway revision before and after each case.

Gateway runs in its own dependency environment. Its production session client
verifies the ephemeral loopback certificate and IP identity, performs actual
HTTPS calls to Conversation, and owns its real aiohttp public writer. The test
client represents the explicitly trusted loopback ingress and sets HTTPS/client-IP
headers. No production ingress, certificate or browser is exercised.

The common grant is provisioned through the real coordinator. PostgreSQL,
credentials, public and private DTOs, visitor isolation, creation rate admission,
Handoff, owner Inbox commands, HUMAN idempotency, delivery history and Return to AI
are real. AI execution, User authentication and Control Plane identity are
synthetic. The multi-message case holds the first execution while three separate
settled inputs arrive, then asserts one bounded current Turn and one visible reply.

The original Gateway must commit joined writer completion before the test accepts
delivery. Reading a response body is not used as termination proof. The imported
network guard blocks remote connections/DNS in the Gateway child; the parent
permits only PostgreSQL and its two exact owned HTTP ports. No provider credentials,
paid experiments, production enrollment or permanent-writer proof daemon are used.
