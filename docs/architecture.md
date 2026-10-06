# Main Chota Conversation foundation

This is the Main Platform communication authority defined by the merged
[ADR at GitOps 05867486](https://github.com/chotabyorg/gitops/blob/05867486c8de6292f6b5679391e84c19d3421557/docs/main-chota-conversation-foundation.md).
The paused Chota 2.0 fashion/e-commerce repository and Project #6 are separate.

Goal 1 supplies a durable journal and processing/authority primitives. Existing
production channels and legacy AI calls have no consumer of this service. Goal2
adds one protected managed AI HTTP port and no provider/CP networking,
Redis authority, broker, workflow framework,
public API, webhook, deploy workflow or GitOps service resources.

| Entity | Foundation ownership |
| --- | --- |
| Conversation | Stable UUID, exact owner/integration/channel/thread identity, local sequence/input counters, control mode and authority revision |
| InboundEvent | Scoped normalized immutable provider event, deterministic server fingerprint, accepted receipt and replay/conflict proof |
| Message | Canonical text communication in durable Conversation-local order; one Message per accepted inbound event |
| AgentTurn | Frozen selected input/context, one active logical slot, stable UUID, fenced lease, immutable completed draft/failure |
| DeliveryAttempt | One journal intent per completed draft, explicit delivery evidence state, CAS revision and provider identifier references |
| DeliveryReconciliation | Immutable update identity/fingerprint and original applied revision, preserving old-key replay/conflict evidence |

Owner is an exact opaque Control Plane subject string rather than AI's UUID user
model. Integration is Control Plane's UUID, independent of agent identity.
`(owner_id,integration_id,channel,provider_thread_id)` is unique; PostgreSQL text
identity fields use deterministic `COLLATE "C"`. ASCII channel case normalizes;
all other allowed identity text preserves case, whitespace and Unicode composition.
No provider-specific thread derivation, customer merge, CRM customer or Call is
required. WHATSAPP/VOICE enum values reserve generic identity, not integrations.

Private configuration binds each dedicated service credential to finite complete
grants. Event scope includes the namespace. History/read scope is the grant's
owner/integration/channel triple; namespace does not create a separate journal.
Multiple grants must not be mixed into a new tuple. UUID access always resolves
through its authorized Conversation before child read/mutation.

PostgreSQL is the only durable authority. All journal mutations commit inside
short READ COMMITTED transactions with independent AsyncSessions. The lock order
is Conversation then Turn/Delivery then reconciliation. DB uniqueness and FKs are
defenses independent of in-process code; no Python lock participates in safety.

The immutable completed Turn draft is the foundation's future outbound-intent
relation. Completing a Turn, creating a delivery or journaling dispatch does not
create an outbound Message and does not mean that a provider was called. Future
managed dispatch must add explicit input freshness/supersede and promotion policy.

Bounded operation references identify AI-owned operations only. They carry no
business request, receipt or reasoning. Conversation coordination **does not own
Business operation coordination**; there is no BusinessOperation/confirmation
ledger or UNKNOWN business-write reconciliation here.

See [states and transactions](states-and-transactions.md) for durable primitives
and [API](api.md) for the internal contracts. [Managed execution](managed-execution.md)
uses canonical Messages and same-Turn claims while AI remains prompt/KB/model/tool
authority. Existing legacy `/v1/chat/completions` callers remain unchanged.
