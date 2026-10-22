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

Every case starts the actual Gateway with
`COMMON_WEBSITE_MESSAGE_SESSION_ALLOWLIST={}`. A genuine server-created session
with its valid capability receives no-store 404 on message admission while own
history and idle poll stay available. PostgreSQL must contain its empty root and
no Message, AgentTurn or DeliveryAttempt. The harness restarts the Gateway with
only that issued session UUID selected; an invalid token still fails. The extra
idle poll consumes an ordinary durable rate token, so the existing test clock
advances by its two-second refill interval before the positive flow. Before any
restart it also waits for the original Gateway to commit idle-poll cleanup;
public response bytes alone do not prove that cleanup.

A second genuine session remains unselected: no input journal or AI execution
can originate from it, and it cannot read the selected visitor's history. The
existing same-message replay and creation-quota checks remain. Removing the
selection by restarting with the empty map rejects fresh input to the original
session without adding another AI execution. This tests an ingress fence, not
revocation of previously admitted work.

The original Gateway must commit joined writer completion before the test accepts
delivery. Reading a response body is not used as termination proof. The imported
network guard blocks remote connections/DNS in the Gateway child; the parent
permits only PostgreSQL and its two exact owned HTTP ports. No provider credentials,
paid experiments, production enrollment or permanent-writer proof daemon are used.

`credential_widget_http.py` additionally runs the actual Widget from an exact clean
Site revision in Chromium. Supply explicit `widget_source`, `widget_revision`,
`node`, `playwright_module` and `browser` fixture inputs. Every page request is
intercepted: static fixture HTML/Widget source or a strict public-port allowlist
proxied to the exact local Gateway. It verifies visible HUMAN and AGENT replies,
Return and fresh-input ownership, reload/history deduplication, both original
writers ENDED, and settled 320/1440 layouts with a reachable send control. It is
local composed acceptance; hosted ingress, real User/CP/model and other browser
engines remain separate acceptance gates.

The Widget first creates its own genuine capability with the empty policy. A
bounded request in the browser harness proves message denial using that token;
it does not send through the Widget UI because its documented 404 handling would
discard the capability. Only the issued session UUID crosses the harness event
pipe. The Widget closes its chat after an actual empty poll, then reopens
after selection. After the database no-work assertion and actual Gateway restart, the same
Widget credential drives the existing UI Handoff/Return/reload flow. No client
UUID seed, artificial grant, token export or CP capability bypass is used.

Run both explicit files (`credential_gateway_http.py` and
`credential_widget_http.py`) with pytest, `TEST_DATABASE_DISPOSABLE=yes`, the
private task-owned loopback `TEST_DATABASE_URL` and prepared
`TRACK_WEBSITE_FIXTURE_INPUT`. See the preparation script for exact inputs.

The paired CI pins Website `6f4db23bfbca840e6178eb7bc46e82e91e0d1dc6`, which
implements this empty-default policy. Widget source and browser dependencies are
explicit local fixture inputs; the Gateway CI job does not claim browser coverage.
