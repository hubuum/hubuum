# Lifecycle audit and sink-event contract

Proposed contract for [#550](https://github.com/hubuum/hubuum/issues/550),
extending the shared design in [#503](https://github.com/hubuum/hubuum/issues/503)
and public contracts in [#545](https://github.com/hubuum/hubuum/issues/545).
The proposal is based on server commit `beb37f8a`.

This document specifies future lifecycle behavior. It does not enable a new
event, endpoint, retention policy, or destructive operation. Feature owners must
implement and qualify their applicable transitions before advertising support.
Consumer review is still required; the review table below records inspected
source, not consumer approval. Unrelated foundations and task/SSE work can ship
independently.

## Existing guarantees and gaps

Reuse the [canonical event stream](events.md), `AuditDocument`, event catalog,
audit visibility, subscriptions, delivery workers, and protected deletion
authority. There is no second audit log or deletion ledger.

| Existing evidence | What it establishes | Remaining lifecycle work |
| --- | --- | --- |
| `crates/hubuum-events-core/src/lib.rs`: `AuditDocument`, `NewEvent`, `EventEnvelope` | Validated catalog pairs, object-shaped documents, provenance and envelope versions | Lifecycle catalog, transition-specific evidence and safe outbound projection |
| `crates/hubuum-storage-postgres/src/operations/event_record.rs` and ordinary audited mutation capabilities | Append on the caller's transaction; committed receipts | Explicit atomic lifecycle capabilities, including membership and required evidence |
| `src/tests/storage_contract.rs`: `every_registered_backend_satisfies_the_audited_mutation_contract` | Shared committed mutation, rollback and fan-out certification | Equivalent lifecycle fault coverage for every selectable backend |
| `tests/api_platform_suite/events.rs` | Collection `ReadAudit`, scoped tokens and relation snapshot redaction | Stable resource/version lookup and lifecycle evidence retention |
| `src/tests/storage_contract/event_delivery.rs` | Fenced delivery admission and grant/configuration rechecks | Lifecycle projection before templates, preview, test and every transport |
| `crates/hubuum-storage-postgres/src/operations/event_retention.rs` | Durable bounded retention claims and idempotent completion | Protected evidence lifetimes; ordinary event retention is insufficient |

Existing related-collection visibility preserves metadata while redacting
snapshots. It is therefore insufficient for a mixed-collection lifecycle batch.
Never put another collection's members, counts, operation identity, or provider
references in a collection-visible event's metadata or summary.

## Catalog and versioning

Propose one new catalog entity, `lifecycle`, using the existing coarse actions
below. `metadata.lifecycle.name` supplies the specific transition name;
`contract_version: 1` versions its semantics. The fully qualified event name is
`<name>.v1`, for example `lifecycle.archive.published.v1`. A subscription still
uses the existing entity/action validation, with an additive validated
`lifecycle_names` filter for precise selection. An unknown name in a write
request is rejected. An unknown optional reason in a read is preserved.

`lifecycle_names` selects fully qualified names including the major version,
for example `lifecycle.archive.published.v1`. Lifecycle delivery requires explicit
entity and name selection; legacy empty/wildcard subscriptions do not opt in.
Reject empty lifecycle-name selection and audit-only names on a lifecycle
subscription. A future name or major version therefore needs a subscription
update and review rather than silently expanding an existing export contract.

The envelope's `schema_version` remains separate: lifecycle examples use version
1 with null snapshots; existing revision-aware documents remain version 2.
Do not change historical event meaning or infer lifecycle completion from a
generic `task.succeeded` event. No lifecycle catalog entry is activated by this
proposal. Adding it later requires exhaustive server/adapter matches, OpenAPI,
backup-reader compatibility and the consumer checks below.

Each row below is a named v1 event. **Eligible** means an explicit subscription
may select its safe sink projection; no subscription is created automatically.
**Audit** means audit-only, including for system subscriptions. State and audit
facts commit together at the stated emission point. Repeating an unchanged
transition returns its original event identity and emits nothing new.

| Name after `lifecycle.` | Action | Owner | Durable fact and emission point | Sink |
| --- | --- | --- | --- | --- |
| `policy.created` | created | #513 | Policy identity, initial revision, scope and original time anchor committed | Eligible |
| `policy.changed` | updated | #513 | Expected revision and prospective/retrospective effect committed | Eligible |
| `policy.reviewed` | updated | #513/#518 | Review identity, fingerprint, coverage, expiry and authorizing revision committed; reserves nothing | Audit |
| `hold.created` | created | #513 | Hold identity, scope and hold generation committed | Eligible |
| `hold.released` | updated | #513 | Explicit release and new generation committed | Eligible |
| `hold.expired` | updated | #513 | Effective expiry settled under the current generation | Eligible |
| `run.requested` | queued | #513/#515/#520 | Admitted manual or scheduled run and fixed scope recorded | Eligible |
| `run.started` | started | #513/#515/#520 | Fenced execution ownership committed | Eligible |
| `run.completed` | succeeded | #513/#515/#520 | Run's declared action finished with exact batch evidence; unresolved cleanup is explicit | Eligible |
| `run.failed` | failed | #513/#515/#520 | Terminal bounded failure and retained obligations committed | Eligible |
| `run.cancelled` | cancelled | #513/#515/#520 | Cancellation settled; existing physical obligations retained | Eligible |
| `archive.transfer_started` | started | #515/#520 | Attempt, immutable source membership and destination identity recorded before I/O | Audit |
| `archive.verified` | updated | #515/#520 | Read-back/checksum observation bound to immutable object version and verification receipt committed | Audit |
| `archive.published` | updated | #515/#520 | Verified destination becomes authoritative after generation/fence recheck | Eligible |
| `archive.source_retired` | updated | #515/#520 | Exact source retired after holds, pins, expiry and authority rechecks | Eligible |
| `archive.cleanup_completed` | succeeded | #515/#520 | Remaining managed source-copy obligations confirmed removed | Eligible |
| `archive.blocked` | updated | #515/#520 | Blocker category changes, including hold, provider lock or unavailable evidence | Eligible |
| `archive.failed` | failed | #515/#520 | Attempt/run failure recorded without inventing destination publication | Eligible |
| `archive.integrity_failed` | failed | #514/#515/#517/#519 | Missing/corrupt/dependency-incomplete archive observation and restricted coverage committed | Eligible |
| `data_request.requested` | queued | #516/#517 | Principal-owned discoverable request accepted, independent of its login session | Eligible |
| `data_request.preparing` | running | #516/#517 | Warm preparation or active retrieval processing phase changes | Audit |
| `data_request.provider_wait` | updated | #516/#517 | Durable provider checkpoint committed and execution capacity released | Audit |
| `data_request.ready` | succeeded | #516/#517 | Verified result and actual readiness/availability window committed | Eligible |
| `data_request.failed` | failed | #516/#517 | Failure settled; residual retrieval/cache obligations retained | Eligible |
| `data_request.cancelled` | cancelled | #516/#517 | This consumer cancelled without cancelling other authorized consumers | Eligible |
| `data_request.result_expired` | updated | #516/#517 | Temporary result availability ended; retained source lifetime unchanged | Eligible |
| `cache.evicted` | deleted | #516/#517 | Bounded temporary-copy eviction confirmed; retained source is not erased | Audit |
| `erasure.requested` | queued | #511/#518 | Minimal protected intent and exact reviewed scope committed | Eligible |
| `erasure.access_restricted` | updated | #511/#518 | Current read/publication fence takes effect; physical erasure is not implied | Eligible |
| `erasure.recovery_protected` | updated | #511/#518 | Latest independent checkpoint read-back covers the intent | Audit |
| `erasure.copy_removed` | updated | #511/#518 | Exact managed-copy removal receipt committed after verified observation | Audit |
| `erasure.blocked` | updated | #511/#518 | Managed obligation changes to a bounded blocker | Eligible |
| `erasure.completed` | succeeded | #511/#518 | Defined managed scope and final receipt independently protected before acknowledgement | Eligible |
| `evidence.expired` | deleted | #513/#550 | A bounded evidence-retention batch and its accounting committed | Audit |
| `result.access_authorized` | invoked | #516/#517/#545 | Optional access-audit policy records authorization, before opening the body | Audit |
| `result.transfer_completed` | succeeded | #516/#517/#545 | Optional access audit records verified EOF and bytes handed to server transport | Audit |
| `result.transfer_failed` | failed | #516/#517/#545 | Optional access audit records interrupted/corrupt server transfer | Audit |

Same-tier migration and cross-tier movement use the same archive transitions;
the source and destination tiers identify which occurred. Provider preparation,
pending physical deletion and cache eviction cannot be renamed as permanent
expiry. Scheduled expiry uses erasure transitions under the approved policy and
does not require fresh human approval for each batch.

Rejected validation, authorization and admission attempts do not emit a
committed lifecycle success fact. Existing bounded security/operational logs may
record them with secret-safe categories. A real provider attempt records intent
and its observed outcome; retries with additional physical work get a fresh
attempt identity. Polls without a state change, ordinary reads and genuine
no-ops emit no lifecycle audit event. Access audit is explicitly opt-in and
bounded per transfer, never per chunk. Authorization, response opening and
server transfer completion prove different facts; none proves a person read,
stored or retained the result. A crash can leave an access attempt without a
completion fact.

## Identity, authority and exact membership

Keep canonical `event_id` UUIDs and integer audit row IDs. Row IDs, timestamps
and sequence allocation do not establish commit order or a global watermark.
Assign an operation UUID and monotonic revision to each authorization-scoped
operation; atomically advance that revision with its state and event. Retrying
the same committed revision returns the existing event. Conflicting or stale
claims cannot allocate a success revision. Provider attempts, execution runs
and immutable batches have separate UUIDs. `caused_by_event_id` is optional and
must point only within the same visible scope.

A root operation spanning collections remains internal. It owns separate
collection operation IDs, batches and canonical events, each with its own
`event_id`. Only an unscoped administrator may read a separate system summary.
Collection events never carry the root's identity, global counts, hidden
collection list or cross-scope causation. Pagination and totals operate on the
authorized projection. System-only operations use a null collection and remain
unscoped-admin-only. A deleted collection cannot be resolved through a reused
integer ID: visibility uses stable collection identity and explicit retained
audit authority, otherwise access fails closed.

Audit attribution includes the immediate actor and root initiator, stable
principal UUIDs where available, trigger (`manual`, `scheduled`, `recovery`),
policy/review references and revisions, hold/deletion generations, request
correlation and causal event. Preserve the existing actor/initiator provenance;
names remain permission-scoped read-time decoration. Missing historical
identities are explicitly unavailable, never guessed from current integer IDs.
Pin the referenced immutable policy revision and review fingerprint/scope for
the evidence lifetime. A pointer to today's mutable policy cannot explain a
historical decision after the policy changes or disappears. Retain those minimal
authority records under the owning policy/receipt capability, without copying
sensitive selector values into sink payloads.
`occurred_at` retains the existing naive-UTC wire format and database recording
semantics. `observed_at` is an optional RFC 3339 UTC timestamp for a separately
recorded observation, not a claimed provider commit time. Neither is advertised
as a transaction's commit timestamp. New monotonic revisions/generations use
positive decimal strings to avoid JavaScript precision loss.

Counts alone are not sufficient membership evidence. Before moving, retiring
or erasing a batch, persist a fixed, scope-homogeneous manifest of stable
resource UUID/version UUID pairs and its format version, digest, bounded count
and retention deadline. Store an indexed association from each pair to the
manifest and event/operation UUIDs. Exact resource/version lookup must work
without a live resource or original task. Catalog/segment/artifact UUIDs and
configured destination UUIDs are distinct references; object keys, signed URLs,
provider versions and recovery material stay private.

Audit copy references identify both source and destination configured stores
and artifact UUIDs when applicable. A hot row source has null source store and
artifact references; a same-tier migration retains both immutable store
identities even after defaults change. Raw provider locations remain private.

Use at most 1,000 pairs and 1 MiB of encoded membership per batch. Split larger
operations into independently evidenced batches; no unbounded event array.
One event is at most 64 KiB encoded UTF-8, with lifecycle metadata at most 16
KiB; the safe sink envelope is at most 8 KiB. Membership pages default to 100
and cap at 250 pairs. Reject over-budget claims before I/O; never truncate a
manifest and claim completeness. These are proposed initial hard limits;
feature measurements must validate them against
[lifecycle budgets](performance/lifecycle_budgets.md). Counters are scoped,
nonnegative JSON-safe integers. Logical bytes, physically verified removed
bytes and transferred bytes have separate nullable fields. Unknown physical
bytes are null, never substituted with logical length or zero.
Shared physical objects must not expose another scope's total size: report null
unless an isolated, measured contribution can be disclosed safely.

Membership is immutable while retained. Minimum evidence contains identities,
not snapshots, names, selector values or erased payload. Apply authorization,
holds, minimization and erasure rules to manifests and indexes too. If identities
must be removed, retire the original evidence under protected authority and
return `minimized`; do not rewrite it under the old digest or claim exact
membership is still available. A digest without retrievable authorized
membership is insufficient to answer which versions were affected.

The v1 digest covers this ASCII byte stream: `hubuum-lifecycle-membership-v1`
and LF; manifest UUID and LF; collection UUID and LF; scoped operation UUID and
LF; then one `resource_uuid`, TAB, `version_uuid`, LF line per member, sorted
lexicographically by the UUID pair with duplicates rejected. UUIDs use canonical
lowercase hyphenated form. There is no BOM, CR or trailing whitespace. The
encoded byte count includes the header and final LF. JSON pages are views over
that complete immutable manifest; a single partial page cannot verify its
whole-manifest digest. The shared complete two-member example includes the
matching SHA-256 and length.

## Atomicity and recovery

Each complete backend must implement an operation-shaped lifecycle transition
capability. Its validated request carries the scoped operation/revision,
current claim, authority generations and required membership/verification proof.
State, exact membership/index changes, canonical event and returned event receipt
commit in the same transaction. Types carry validated facts across services
and storage; database constraints and fencing still enforce concurrency.
Ordinary `append_event` followed by a separate domain write is not sufficient.
Failure to persist required audit evidence rolls back the transition.

Provider I/O occurs outside transactions. Commit attempt intent first; perform
I/O with bounded resources; then commit verified observations under the current
claim. A crash after provider success but before observation leaves a pending
attempt for reconciliation, not a fabricated failure or committed success.
Reconciliation reuses the attempt and immutable object identity until it proves
the outcome; additional physical work uses a new attempt. Destination verified,
destination published, source retired and residual cleanup confirmed are
independent facts. A hold or provider lock may block a later fact without
undoing earlier facts. A stale owner may report an observation for diagnostic
reconciliation but cannot publish, retire or acknowledge completion.

For destructive work, #511 remains the only protected authority. Intent and
required exact membership are independently recoverable before irreversible
cleanup or acknowledgement that deletion started. Completion receipts must
include the required audit event identities and scoped membership coverage;
publish and authenticate independent coverage before acknowledging final
completion. Event retention alone cannot provide this guarantee. After logical
restore or supported physical recovery, quarantine exposure, recover authority,
reapply restrictions and reconcile incomplete evidence before reopening access.
Reconstructed audit projections preserve original event identities and mark
their recovery provenance; they do not emit a second deletion success.

Task purge and principal/resource deletion cannot cascade away required
receipts, member indexes or event attribution. Ordinary audit events may expire
under their separate policy, but protected acknowledgements and minimum lookup
evidence must remain discoverable for their promised lifetime. A missing,
wrong-lineage, stale or unverifiable checkpoint is an explicit recovery gap.
There is no claim of tamper-proof storage, arbitrary rollback detection, or
removal from backups/WAL/external recipients outside the declared managed scope.

## Safe sink projection and delivery

Build the sink projection from an allowlist **before** subscription templates,
preview/test, delivery persistence or transport serialization. No template can
recover the privileged audit document. The v1 sink view includes only the
canonical event UUID, scoped operation/revision, coarse actor kind, event name,
scope, committed outcome, bounded reason codes and this scope's counts. Use a
fixed catalog summary; null names, actor/initiator principal details, task ID,
request/correlation IDs, snapshots, authority details, membership links and
physical destination/artifact references. An administrator's system subscription
also receives the safe view. Privileged audit export is a separate explicit
integration, not an implicit sink option.

The existing numeric audit row ID and occurrence time remain envelope metadata
for compatibility. They are not counts, commit-order proofs or catch-up
watermarks; consumers deduplicate with the event UUID.

The v1 fixed sink summary is `Lifecycle state changed`; consumers display the
specific transition from the typed name. No source-generated free text crosses
this boundary.

Collection subscription matching requires the event's direct stable collection
scope. Do not use related-collection metadata to route lifecycle batches. Keep
existing `ReadAudit`, `ManageEventSubscription`, sink grants, enabled state,
configuration revision and scoped-token checks. At delivery admission recheck
the subscription's current authority and sink grants, including retries; a
revoked/disabled destination does not send. The already-admitted in-flight
request can finish. Redaction is not a way to bypass revoked authority.

Reuse existing at-least-once delivery, bounded attempts/backoff, claim fencing,
dead-letter inspection and manual retry. Dedupe by canonical `event_id` per
subscription/consumer. Different scoped events have different IDs. Preserve
operation revision and causation for out-of-order reconciliation, but promise
neither globally ordered nor exactly-once delivery. Sink acknowledgement proves
transport delivery only; it cannot complete a migration or erasure.

Bound fan-out using existing batch settings and one delivery per matching
subscription/event. Add measured per-scope/deployment pending-row and byte
ceilings before activation. When the delivery allowance is exhausted, record a
bounded delivery-gap interval/reason and keep the canonical audit fact; do not
silently lose audit evidence, create an unbounded outbox, or wait for a webhook
to acknowledge physical cleanup. Resume consumers through an authorized,
bounded audit query and explicit coverage. A gap is not a promise that missing
notifications can always be replayed. Dead letters expire under a documented
delivery policy, and retry after expiry cannot invent a missing source event.

## Retention and public API proposal

| Evidence | Lifetime and access |
| --- | --- |
| Canonical audit events | Independent configured audit window, with `ReadAudit`; append-only except bounded authorized retention |
| Exact membership and resource/version index | At least the promised event evidence window and any outstanding protected obligation; advertised expiry; no task cascade |
| Protected deletion receipts/checkpoints | #511's recovery/restorable-copy horizon, independent of event/task retention; explicit minimum identity minimization rules |
| Delivery rows and safe payload | Independent retry/dead-letter window; bounded backlog; no privileged audit snapshots |
| Operational logs and metrics | Independent operational retention, secret-safe categories, no exact membership or correctness authority |

Keep the existing event retention defaults until a feature supplies qualified
integration. Audit-data retention records one bounded `evidence.expired` summary
per committed batch, with exact retention-batch identity and scoped counts;
purging such summaries does not recursively emit another summary. Its durable
retention completion receipt supplies idempotency/accounting. It is audit-only
and does not delay on a sink. Protected evidence cannot be purged merely because
its ordinary event or task expired.

The following additions are **proposed routes and filters**, not currently
implemented API claims. Implement them with the owning feature, shared response
helpers and `ApiError`; then add annotations and regenerate `docs/openapi.json`.
The downloadable [audit schema](contracts/lifecycle-audit/audit-event.schema.json),
[sink schema](contracts/lifecycle-audit/sink-event.schema.json),
[membership schema](contracts/lifecycle-audit/membership-page.schema.json),
[unavailable-evidence schema](contracts/lifecycle-audit/unavailable.schema.json)
and [shared examples](contracts/lifecycle-audit/examples.json) define these views
before rollout. Schemas validate structure; typed constructors must additionally
enforce UUID relationships, member ordering/uniqueness, digest/count agreement,
time ordering and proof authenticity. The examples are independent state samples,
except the paired published audit/sink views and their complete membership page.

| Surface | Contract |
| --- | --- |
| `GET /api/v1/events` | Add validated `lifecycle_name`, `operation_id`, `resource_uuid`, `version_uuid` filters. Resource/version predicates use the retained membership index, not today's integer resource tables. Preserve existing integer filters. |
| `GET /api/v1/events/{event_uuid}` | Authorized audit detail and evidence availability after task disappearance. A scoped view has no global count or hidden-operation identity. |
| `GET /api/v1/events/{event_uuid}/members` | Bounded exact membership page with manifest UUID, scoped operation UUID, coverage, advertised expiry and opaque next cursor. Bind cursor to manifest generation, filter and authority scope; recheck access on every page. |
| `GET /api/v1/lifecycle-receipts/{receipt_uuid}` | #511/#545 durable, permission-scoped acknowledgement and exact evidence lookup independent of transient task/event detail. Distinguish access restricted, recovery pending, cleanup pending, blocked copies and confirmed managed-scope completion. |
| Existing subscription, preview/test and delivery routes | Add validated lifecycle-name selection and safe-view fixtures; never treat delivery state as domain completion. |

A fully available empty membership page means complete empty evidence. Expired,
missing, minimized, corrupt, unsupported-format or dependency-incomplete evidence
must not become an empty success. Known authorized expired evidence returns
`410` with a typed bounded reason and safe receipt reference when retained;
transient evidence unavailability returns `503`; corrupt/dependency-incomplete
evidence returns a typed failure without complete coverage. A nonexistent or
unauthorized identity returns the same `404` shape. Never reveal expiry,
counts or receipt links before authorization. A retained tombstone is needed to
distinguish authorized expiry; once it expires, ordinary `404` is correct.

Readers preserve unknown optional fields/reasons and display unsupported names
or major contract versions without claiming completion. Producers validate the
closed v1 schemas and allowlists. Additive optional read fields do not authorize
new outbound fields. A changed required meaning needs a new contract version and
reviewed migration. Keep event names and evidence availability separate from
task phase, SSE updates, metrics and consumer wait timeouts.

`examples.json` separates valid producer examples from `reader_cases` that
deliberately extend the closed v1 grammar. The latter specify preservation or
unsupported outcomes for consumer review. `delivery_cases` supplies actual
duplicated/out-of-order safe envelopes, including a revision above JavaScript's
exact-number range. Validating their structure does not prove a consumer's
reconciliation or interpretation; its own tests must run those vectors.

## Consumer compatibility review

Inspected default-branch sources on 2026-10-30. This is an implementation review,
not execution of those repositories' tests or maintainer sign-off.

| Consumer and pinned source | Finding | Required acceptance |
| --- | --- | --- |
| [Rust client `170f4c9e`, `src/types/event.rs`](https://github.com/hubuum/hubuum-client-rust/blob/170f4c9e4d181eea14283267ee9c61ee068328c8/src/types/event.rs) | Envelope entity/action are strings and metadata is JSON; raw envelope acceptance is not typed lifecycle interpretation | #144: typed audit/sink/evidence models, async/blocking fixture parity and unknown-version behavior |
| [Python client `f65ed9db`, `src/hubuum_client/api/models.py`](https://github.com/hubuum/hubuum-client-python/blob/f65ed9db387e6792c04d56028e101d83e2f9f010/src/hubuum_client/api/models.py) | Generated `EventResponse` uses string entity/action and JSON metadata; the response base allows extra fields | #91: regenerate wire models, prove sync/async parsing of new catalog entries, bounded reasons and evidence failures |
| [Frontend `49790431`, `src/lib/event-options.ts`](https://github.com/hubuum/hubuum-frontend/blob/49790431aa3e5a6dcd93f3d00d21f3c530486054/src/lib/event-options.ts) | Catalog options are explicit; audit workspace uses generated models | #193: regenerate models, add catalog/filter/detail UI, preserve expiry/redaction and separate notification preview |
| [CLI `8ae0368d`, `src/domain/events.rs`](https://github.com/hubuum/hubuum-cli/blob/8ae0368da0f16ca77b315442c4ea2ce3c4f3e97c/src/domain/events.rs) | Audit records wrap the Rust client; tabular output is generic | #155: release/pin the corresponding SDK, typed evidence commands, structured completeness and nonzero unavailable outcomes |

Review each consuming feature using the same fixtures: additional optional field,
unknown optional reason, unsupported name/version, scope separation, exact
membership after task/resource deletion, expiry, duplicated/out-of-order sink
delivery, and protected recovery. Consumer implementation remains blocked on its
feature-specific accepted server contract; fixture review can begin now. Do not
quietly add `lifecycle` to a closed enum or assume a string envelope proves all
generated models and rendering paths compatible.

## Qualification and remaining work

[Scenario contracts](contracts/lifecycle-audit/scenarios.json) define required
outcomes and implementation ownership. Rust fixture tests validate the proposal's
schemas, positive examples, projection restrictions and rejected malformed
evidence. They do **not** execute future lifecycle transitions or establish
provider, transaction, restore, retention or performance guarantees.

| Gate | Required evidence before the owning feature is accepted |
| --- | --- |
| Atomicity and audit outage | Backend-native rollback/failpoint: no state transition without required event/membership, no success event after rollback |
| Transfer boundaries | Crash before/after intent, provider success, observation, publication and source retirement; exact receipt/event dedupe on recovery |
| Deletion boundaries | Crash before/after independent protection, physical removal and completion protection; no premature acknowledgement |
| Fencing and concurrent change | Stale leases, changed generation, new hold, lock/outage, late commit behind earlier selection, and authorization loss |
| Scope and disclosure | Two-collection operation with one-collection reader/subscriber; every template/preview/transport excludes hidden identities, counts and provider data |
| Delivery | Sink outage, duplicate and out-of-order delivery, revoked retry authority, bounded backlog/gap and dead-letter expiry |
| Evidence lifetime | Task purge, principal/resource deletion, membership expiry/minimization, corrupt manifest and unsupported reader |
| Recovery | Logical restore with reused integers, supported primary rollback, wrong-lineage/stale checkpoint and duplicate reconciliation |
| Consumer acceptance | Pinned frontend/BFF, Rust async/blocking, Python sync/async and released-SDK CLI fixture results and explicit review |

Consumer approval and runtime qualification remain unproven by this proposal.
Record accepted portions in #503/#545; keep remaining #550 checklist items open
or explicitly assign them to the implementation owners during review. Each
owning feature supplies its runtime evidence before acceptance. Design review
can proceed independently; a documentation merge does not claim all lifecycle
runtime behavior has shipped.
