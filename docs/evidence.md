# Design and implementation provenance

This record separates inspected source/design evidence from executable review and
verification. It contains no database endpoint, credential or private payload.

Fresh discovery on 2026-10-06 read the complete Goal 1, Main ADR, Project #3 item
263003543, AI #18/#43 and current Main source. Chota 2.0 was neither read nor used
as implementation template. Existing Main runtime repositories were read only.

Discovery artifact SHA-256:
`165b4924bc5a2b73a4583cbf9f1d989673e4c021bd77e143369f146fe158f6f6`.
Architecture discovery artifact SHA-256:
`eab5ab56834434503ca7f9bbe302ddd64066fb550d52d415610acc3d5644bc8f`.

| Source | Inspected main revision |
| --- | --- |
| GitOps/Main ADR | 05867486c8de6292f6b5679391e84c19d3421557 |
| ai-agent-service | 3eafb97726fd33e614e443bbac5c10f9fce72a81 |
| control-plane-service | 96a4b40daeb113f435f5cd02a6f7a2fc12502b24 |
| telegram-gateway-service | 591d713302f06dd15fd583db1ca5629b240b4bf7 |
| instagram-gateway-service | f366b46cee2c4e845da150f81df573af467503b6 |
| website-gateway-service | fda632c0f694ae1b5ffc74ce4abd92d87ebeb04b |
| whatsapp-gateway-service | ac47fea4a5c630622cee218747c0f0b9ec551d69 |
| notification-service | 145c3a6f3ab55d894ea631a8d6b590fbb2c819b0 |
| subscription-service | b664172db619c74e6bc708c47c4ca8685b032789 |
| payment-service | eef4a62ba24e773f23745b783b25023b0ee6e58f |

Main AI/subscription informed layout and async ORM; notification informed exact
dependency pins, settings and app factory; payment informed bounded errors and
request correlation; CP informed owner/integration identity; gateways informed
provider-neutral normalization; GitOps informed deployment prohibition. Readiness,
Ruff and mypy are deliberate Goal 1 additions, not a claim of universal Main usage.

AI PR #43 was OPEN/DRAFT/unmerged, base 3eafb97726fd33e614e443bbac5c10f9fce72a81,
head f4fdcc9c3b4b64ad89d947a17791312847b1a598, updated 2026-10-03T17:22:53Z.
This matched Goal 0; no dependency/merge/deployment assumption was made.

The reconciled FINAL design SHA-256 was
`5ea42a40a2ef62994c266b28ee01f4878199d9bffa752cb284f81d393d456f13`.
Fresh independent architecture context `/root/design_review` returned **PASS for
implementation**, zero blocker/important design findings, on 2026-10-06. That
review approved the contract only; runtime code did not yet exist. It explicitly
called for current locked ORM state, null-safe DDL and server/DB log redaction.
Design review artifact SHA-256:
`06ca12fb98e87a3a0c04091f60b578ba587492339f4b05bc09b11c0ba30e3f33`.

Implementation is split into API/config/DTO/docs, database/migrations/transactions,
and test-author contexts. Authors cannot provide final independent review or
verification. Candidate review/fix/verification history belongs to the final Goal 1
delivery record and must identify exact reviewed/verified commits. A source check,
test-author run or design PASS is never final acceptance evidence.

The dependency closure was resolved in an isolated Python 3.11 environment from
the approved baseline. Requirements files retain exact direct/transitive pins;
Linux image build checks the platform's actual installation rather than assuming
a macOS environment proves Linux compatibility.

API author checks on the working candidate, 2026-10-06: Ruff passed for
core/schemas/api/main.py; mypy passed for those 14 source files; pip check found no
broken requirements. A separate disposable API-author PostgreSQL database was
migrated to head and app lifespan/readiness, auth-first rejection, new/replayed
ingestion, UTC serialization, Turn selection/claim/read/completion, authenticated
OpenAPI and strict pagination inputs passed. A local Linux aarch64 image build
installed the full runtime pins, passed pip check and exited0. These are author
checks, not independent final verification and not a claim of all tests passing.

At this authoring stage final code reviews, independent verification, merge and
tracker reconciliation remain orchestrator gates. No result for those gates is
claimed here. There is no production/provider traffic, deployment, GitOps runtime
change, existing gateway modification or AI runtime change in this foundation.

## Goal 2 Conversation author checks

On 2026-10-06 the Conversation author implemented Issue3 in its isolated service
worktree after the independently approved revised design and the AI provider's
stable schema/security checkpoint. The sole AI HTTP port is bounded and makes one
attempt. Conversation freezes canonical context and commits the same-Turn marker
before dispatch; existing winning-proof terminal fencing remains authoritative.
Migration 0002 is additive and refuses downgrade after managed history exists.

The initial unmodified service baseline passed 230 tests. The authored service
candidate passed 355 tests in 54.98s with
`python -m pytest -q`. These checks used an
explicitly designated, disposable PostgreSQL 16 database, including observed real
lock waits for snapshot/marker/result post-lock expiry and joined cancellation
finalization. Populated 0001 upgrade, exact catalog/constraint negatives, refused
downgrade, retained database restart, ambiguous marker/result commit outcomes,
non-suffix ordered context and bounded local HTTP failure responses all passed.
No critical service test was skipped. The separately authored, mandatory
`tests/managed_cross_service/acceptance_http.py` gate requires explicit
AI/runtime/database inputs;
its results belong to its own author record and fresh final verification.

Ruff, strict mypy, pip check, explicit Alembic upgrade, readiness and Alembic check
passed. The migration drift result was `No new upgrade operations detected.`
A local Python3.11 Linux image build installed the exact runtime dependency closure
and passed pip check. Author image:
`sha256:20eb4fdda2a61e317e9a7e6395335d9c7b41935a8a6ca55064260eb2b952268c`.
Exact authored source commit, commands, disposable environment ownership and safe
logs are recorded in the external Goal2 implementation note. These are source
author checks, not independent review, final verification or global acceptance.

## Goal4A source author verification

Author candidate is isolated on `codex/goal4a-website-canary`, based on current Main
`d238064215d3875c5d2061bba5a00b55dc5fad0e`. Existing checkout was not switched.
This record is source author evidence, not independent review or runtime rollout.

- Final complete standard service suite: **404 passed in109.77s**, real PostgreSQL16
  in a separately created disposable author database. Existing374 tests retained.
- Ruff: PASS. Strict mypy: PASS,35 source files. pip check: PASS.
- New behavior tests cover positive sticky arbitration in both policy directions,
  observed independent PG advisory-lock waits, atomic rollback on failed admission,
  different IDs/same text, FIFO and frozen prefix, stable pending/reclaimed Turn,
  marked UNKNOWN without execution, genuine and late refs, post-I/O lease fencing,
  context bound known failure, lease renewal, narrow roles/scope, authority changes,
  immutable local write facts, response replay and exact worker startup bindings.
- Migrations: empty→head via the standard fixture; populated0002→0003 preserves
  journal IDs, fingerprints, leases and drafts. Retained LEGACY arbitration blocks
  downgrade; older populated0001 upgrade/evidence refusal regressions still pass.
- Safe-log sink sentinel tests include descendants, independently attached handlers,
  DEBUG, raw exception/stack fields and logging reconfiguration. Actual process
  logging/live Website/CP credential protection remains a separate release gate.
- Final local Docker image:
  `sha256:bbfe2eba96b9557706a41f7c76b3c0fea4aac2dac945264104d02044d90e1787`.
  API and separate worker both reached readiness200 in read-only containers at
  UID10001 with capability drop/no-new-privileges, using a separate disposable
  lifecycle DB. Both gracefully exited0; captured logs contained none of the
  synthetic dedicated registry/AI/database credentials. This local image was not
  published. Main GHCR build/digest proof is a later reviewed publishing gate.

Orchestrator owns `tests/website_cross_service` real-process A–H/overlap acceptance;
its immutable source/evidence is committed and reported separately. No real widget
key, production admission, provider mutation, image push, deployment or source
merge was performed by this author lane. Independent full source review remains
required. Full demo activation additionally requires the separately reviewed
assistant-history/context-limit policy and stable frontend message identities.
