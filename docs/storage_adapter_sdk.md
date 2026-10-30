# Storage Adapter SDK Compatibility

Status: accepted; current experimental release train is `0.15`.

## Supported Crate Graph

The statically linked storage adapter SDK consists of these crates:

| Crate | Supported purpose | Supported features |
| --- | --- | --- |
| `hubuum-budget` | Immutable byte policies, cumulative accounting, and bounded writers | Default only |
| `hubuum-computed-fields` | Validated computed-field definitions and deterministic evaluation | Default only |
| `hubuum-schema-diagnostics` | Generic bounded diagnostics for `jsonschema` errors | Default; `openapi` |
| `hubuum-domain` | Validated identifiers, revisions, JSON Patch, and domain values | Default; `openapi` |
| `hubuum-events-core` | Event catalog, envelopes, filters, and mutation provenance | Default; `schema` |
| `hubuum-regex` | Bounded portable regex language, compiler interface, and conformance cases | Default only |
| `hubuum-query` | Bounded, backend-neutral query parsing and values | Default only |
| `hubuum-task-core` | Task identity, idempotency, validated execution limits and cooperative stop contexts | Default only |
| `hubuum-storage-core` | Complete capability traits, DTOs, errors, transactions, and aggregate | Default; `openapi` |
| `hubuum-storage-conformance` | Reusable behavioral certification for complete adapters | Default only |

The diagnostic crate is independently usable and has no Hubuum dependencies.
It is currently a publishable prerequisite in this train; actual standalone
publication and a separate release cadence require a later review.

Their publication graph is closed:

```text
hubuum-storage-conformance
        |          |
        |          +-- hubuum-domain
        v
hubuum-storage-core
    |       |       |       |       |
    |       |       |       |       +-- hubuum-task-core
    |       |       |       +---------- hubuum-query
    |       |       +------------------ hubuum-events-core
    |       +-------------------------- hubuum-domain
    +---------------------------------- hubuum-computed-fields

hubuum-storage-core --> hubuum-budget
hubuum-storage-conformance --> hubuum-events-core
hubuum-storage-conformance --> hubuum-query
hubuum-domain --> hubuum-schema-diagnostics
hubuum-query --> hubuum-regex
```

Every in-graph dependency uses an exact version requirement. The ten packages
share one version and are released together. The root application and
`hubuum-storage-postgres` are not part of the supported SDK. PostgreSQL remains
the in-repository reference implementation, while an external adapter depends
only on the graph above.

The SDK is a Rust source compatibility contract for static Cargo composition.
It is not a dynamic plugin ABI, wire protocol, runtime capability handshake, or
server embedding API.

## Versioning and Aggregate Evolution

The SDK remains `experimental-public` during the `0.x` series:

- a patch release is source compatible with its minor line;
- a minor release may make a documented breaking change;
- all ten crates still advance together, even when only one crate changes; and
- adapter manifests use an exact requirement for `hubuum-storage-core` and the
  matching `hubuum-storage-conformance` release.

At `1.0.0`, ordinary Semantic Versioning applies: compatible additions use a
minor release, fixes use a patch release, and incompatible changes use a major
release.

`StorageBackend` is the mandatory aggregate. Adding a required supertrait or
trait method, removing or changing a method, changing a carrier or result, or
changing a closed semantic vocabulary is incompatible for adapter authors. It
therefore requires a coordinated minor release before `1.0.0` and a major
release afterward. The project will not hide a required capability behind an
unsupported default. A versioned parallel aggregate is introduced only if a
future migration genuinely requires two application contract generations to
coexist; it is not the routine evolution mechanism.

`cargo-semver-checks` runs against the latest crates.io release for every SDK
crate. Its result supplements the policy above: a change that is behaviorally
breaking still requires the coordinated incompatible release even if a source
API checker cannot detect it.

## MSRV and Features

The minimum supported Rust version is Rust 1.88 for the entire release train.
Every documented feature combination must build and be usable on that version.
Raising the MSRV requires an incompatible coordinated release and migration
notice during `0.x`; after `1.0.0`, it follows the project's documented major
release policy unless a future policy explicitly establishes an MSRV window.

The optional schema features are additive:

- `hubuum-domain/openapi` adds Utoipa schema implementations;
- `hubuum-storage-core/openapi` adds schemas for task policy and accounting values; and
- `hubuum-events-core/schema` adds Utoipa schema implementations and enables
  `hubuum-domain/openapi`.

Default behavior must not change when optional features are disabled. Removing or
renaming a feature, making it mandatory, or changing a feature so it removes
an API is incompatible. New additive features are compatible when existing
feature combinations keep their behavior.

## Enum Evolution

Public SDK enums have an explicit closed or extensible policy.

`hubuum-storage-core::StorageCapability` is extensible and carries
`#[non_exhaustive]`. Downstream diagnostics must include a wildcard match arm;
new capability labels are compatible additions.

Every other public SDK enum is a closed semantic vocabulary. Adding, removing,
renaming, or reinterpreting a variant is an incompatible change. The audited
closed set is:

- `hubuum-computed-fields`: `DefinitionError`, `FieldErrorCode`, `Operation`, and
  `ResultType`;
- `hubuum-domain`: `EventDeliveryStatus`, `IdentityReference`, `JsonPatchErrorKind`,
  `JsonSchemaErrorKind`, `MaintenanceState`, `PrincipalKind`,
  `ResourceRevisionError`, and `StorageJsonValidationError`;
- `hubuum-schema-diagnostics`: `SchemaActualValue`, `SchemaDiagnosticInspection`,
  `SchemaDiagnosticOmission`, and `SchemaExpectedValue`;
- `hubuum-events-core`: `Action`, `ActorKind`, `EntityType`,
  `EventCatalogError`, `EventFilterError`, and `EventSinkSecretError`;
- `hubuum-regex`: `ExpressionKind` and `Assertion`;
- `hubuum-query`: `ComputedFieldScope`, `ComputedQueryValueType`,
  `CursorCodecError`, `CursorValue`, `DataType`, `Operator`,
  `QueryError`, `QueryScalarType`, `RelatedClassField`,
  `RelatedFilterTarget`, `RelatedObjectField`, `SearchOperator`,
  `StructuredQueryExpression`, and `StructuredQueryField`;
- `hubuum-task-core`: `IdempotencyKeyError`, `TaskControlError`, and `TaskStopReason`; and
- `hubuum-storage-core`: all public enums except `StorageCapability`, including
  authorization permissions, errors, lifecycle selectors, query dimensions,
  task and restore states, import policies, mutation outcomes, notifications,
  and execution call sites.

Error enums are deliberately closed because their classifications are part of
the portable contract. A new failure category requires adapter and caller
review rather than being silently absorbed by a wildcard. Private adapter
enums and application HTTP enums are outside this SDK policy.

## Deprecation and Removal

A supported item is normally deprecated for at least one coordinated minor
release before removal. Removal occurs only in an incompatible release and the
changelog gives the replacement and migration action. A required aggregate
method is not deprecated until its replacement can express the complete
semantics and the conformance suite covers the migration.

An urgent security or soundness problem may require immediate removal or
restriction. That release must contain an explicit security note, affected
versions, and the safest available migration.

## Release Process

SDK releases are distinct from server `vX.Y.Z` releases. Maintainers:

1. choose one version for all ten packages and update every exact in-graph
   dependency plus `Cargo.lock`;
2. record supported additions, changes, deprecations, and every breaking
   migration in `CHANGELOG.md`;
3. update the crate policy documents, storage contract, method registry,
   semantic evidence, and conformance behavior together;
4. run the Rust API policy, package, rustdoc, SemVer, formatting, lint, and full
   repository suites;
5. publish in dependency order, waiting for crates.io index visibility between
   layers: `hubuum-budget`, `hubuum-schema-diagnostics`, `hubuum-computed-fields`, `hubuum-regex`,
   and `hubuum-task-core` first; `hubuum-domain` and `hubuum-query` second;
   `hubuum-events-core` third; `hubuum-storage-core` fourth; and
   `hubuum-storage-conformance` last; and
6. create an annotated `storage-sdk-vX.Y.Z` tag and release notes only after all
   ten immutable crate versions are visible.

The local publication checks are:

```bash
python3 scripts/check-rust-api-policy.py
python3 tests/python/run.py unit policies.test_rust_api
cargo package --locked \
  --package hubuum-computed-fields \
  --package hubuum-budget \
  --package hubuum-schema-diagnostics \
  --package hubuum-domain \
  --package hubuum-events-core \
  --package hubuum-query \
  --package hubuum-regex \
  --package hubuum-task-core \
  --package hubuum-storage-core \
  --package hubuum-storage-conformance
```

CI builds rustdoc with warnings denied and all features enabled. It runs
`cargo-semver-checks` once a registry baseline exists. Publication itself is a
maintainer-controlled operation because crates.io versions cannot be replaced.

## Adapter Upgrade Process

### Deletion authority foundation: 0.9 to 0.10

Update all ten supported SDK crates together to 0.10.0. The complete operational
family now requires `DeletionControlStorage`. Implement privileged, bounded
capture and atomic exact-predecessor transitions for `DeletionControlState`.
Keep its records outside logical application replacement. Retained decisions
must prevent restored exposure until restriction reapplication is implemented;
the initial adapters conservatively reject replacement whenever intents exist.
See [deletion recovery](deletion_recovery.md) for the independent repository
contract and the remaining activation gates. This is a breaking Rust SDK change;
it does not enable a deletion endpoint or promise permanent-erasure completion.

Adapter authors should upgrade `hubuum-storage-core` and
`hubuum-storage-conformance` to the same exact SDK version, update every direct
SDK dependency to that version, and then:

1. compile the complete `StorageBackend` aggregate;
2. run the unchanged storage conformance suite and the `hubuum-regex` corpus
   through the adapter's native compiler and execution path;
3. run backend-native transaction, consistency, migration, failure, and
   connection-loss tests; and
4. review the SDK changelog for behavior changes that compile-time checks do
   not express.

An adapter version supports exactly the SDK version it declares. Applications
select adapters statically and do not negotiate compatibility at runtime.

## Data, Errors, Runtime, and Security

Crate-specific policy documents under `docs/rust_api/` define supported
entrypoints, error and panic behavior, runtime and cancellation expectations,
serialization guarantees, and secret-redaction requirements. The
[storage contract](storage_boundary/contract.md),
[query semantics](storage_boundary/query-semantics.md), and
[testing contract](storage_boundary/testing.md) are normative for adapter
behavior.

## Upgrading from 0.1 to 0.2

Update all seven SDK dependencies together. Relation query constructors now
require `TraversalBudget`; adapters must enforce its depth and generated-work
limits before final sorting and pagination. Implement the claimed import methods
with atomic domain changes and item receipts, and reject an expired or replaced
claim at commit.

The [generated inventory](generated/project_inventory.md) records current package
versions and minimum Rust versions. The server's configuration and deployment
upgrade actions are in the [runtime hardening guide](runtime_hardening.md).

## Upgrading from 0.2 to 0.3

Update all SDK dependencies to exactly 0.3.0 together. Implement every required
method of the new `SchemaEvolutionStorage` capability in the workflow
family. Carry `SchemaReference`, compiled policy proof, authorized collection,
object resource revision, and task lease through the operation boundary. Publish
schema/evidence changes and their audit/outbox records atomically.

The required `get_schema_work_report` method returns a separate complete report
projection; `get_schema_work` remains a bounded cursor checkpoint.
`StorageSchemaWork::record_impact` now returns an optional typed finding. Append
that finding atomically with the batch checkpoint, without loading or rewriting
prior findings. Assemble reports with `StorageSchemaWorkReport::builder`, feeding
findings in object-ID order from the checkpoint's consistent snapshot, and call
`finish` to verify completeness. Preserve findings through cancellation and lease
recovery, and discard them with their work during retention or restore replacement.

Handle the closed `schema_validation` task kind, `class_schema` and
`object_validation` event entities, and optional `StorageImportSchemaActivation`
in class imports. Map the three new logical schema state sections and schema
history section for backup format 6, validate them before restore replacement,
and recreate revalidation work without restoring active leases or impact proofs.

`BackupSnapshotStorage::capture_backup_replay` requires a validated
`StorageBackupBudget` in addition to the history selection. Pass deployment
limits through unchanged and enforce both logical bytes and enumerated-row work
before retaining snapshot rows. `StorageBackupCaptureProgress` supplies checked
accounting and content-free diagnostics. Every selectable adapter must abort on
budget exhaustion with `StorageErrorKind::InputTooLarge`, while preserving a
consistent snapshot and the full restore contract. Do not implement a fallback
that captures an unbounded document and checks its size afterward.
Use `verify_backup_budget_rejected` from `hubuum-storage-conformance` with
fixtures exceeding each limit, and retain native probes proving early database
or iterator termination and bounded retained state.

The materialized `capture_backup_snapshot`, `stage_restore`, `get_restore_job`,
and `apply_restore` methods have been removed. Implement `capture_backup_replay`,
`stage_restore_artifact`, `get_restore_artifact`, and `apply_restore_replay`
directly; these are mandatory and have no materializing fallback. Replace
`StorageRestoreStageCreate`, `StorageRestoreJob`, `StorageRestoreDocument`, and
`StorageRestoreApply` with `StorageRestoreStageArtifact`,
`StorageRestoreArtifactJob`, and `StorageRestoreReplayApply` as appropriate.
Use `StorageRestoreArtifact::from_reader` for exact-byte staging and
`StorageRestoreReplayApply::at_restore_boundary` to prepare baseline history.
Replay I/O failures must abort the same transaction as ordinary write failures.
Provide writable temporary storage through `TMPDIR`; see
[backup spool storage](deployment.md#backup-spool-storage).

Run the portable conformance suite and native transaction, lease, concurrency,
and query-budget tests. The shared implementation scenarios and exact method
inventory are in `docs/storage_boundary/semantic-coverage.toml`; behavioral
requirements and client migration are in [class schema evolution](schema_evolution.md).

Schema work reporting also applies through generic task APIs. Adapters must honor
`StorageTaskListQuery::excluded_kind()` before totals and pagination; task initiator
attribution does not grant access to class-wide counts. Task failure must atomically
persist the schema checkpoint's terminal `failed` status and release its active-work
slot. Schema provenance timestamps use UTC microsecond precision across adapters.

## Cancellation contract migration

The next incompatible SDK release adds five required `TaskExecutionStorage`
operations: `request_task_cancellation`, `admit_task_execution`,
`poll_task_execution`, `acknowledge_task_stop`, and `begin_remote_dispatch`.
Adapters must preserve private-fielded `StorageTaskControl` across projections
and logical backups, pin the original deadline at first admission, and honor
`StorageExecutionScope::with_task_execution` for actual work and cleanup.

Requests persist independently of acknowledgement. Queue withdrawal and terminal
bookkeeping are atomic. Active work keeps its live lease through cleanup, and
all terminal writes arbitrate cancellation against completion under that lease.
Strict imports fence the effect/receipt commit against the stop decision;
remote dispatch persists conservative evidence before attempting HTTP. Export
and backup artifacts are published only with their successful terminal state.
`TaskCancelled` and `TaskDeadlineExceeded` are distinct storage errors and must
not be converted into per-item import failures. Update exhaustive error/action
matches for these variants and `Action::CancelRequested`.

The shared application contract tests exercise every registered adapter. Keep
adapter-native coverage for SQL cancellation, receipt commit races, database
locking, replica recovery and connection cleanup alongside those shared tests.

## Fresh credential approvals

The next server revision requires `TokenStorage::create_credential_approval` and
`get_credential_approval`, plus `StorageRestoreConfirmation` on
`RestoreStorage::start_restore_draining`. Identity write requests may carry a
`StorageCredentialClaim`; task and restore requests carry `StorageCredentialUse`
to preserve the claim with its event attribution. Internal login/bootstrap
issuance remains explicit and does not attach a claim.

Adapters must consume attached claims atomically with the protected write, reject
expired/replayed/mismatched claims with `ReauthenticationRequired`, recheck the
originating unscoped human token, and retain consumed approval metadata. A failed
mutation or event append must roll back consumption. Emit the
`CredentialApproval` entity's `Created` and `Succeeded` events through normal
fanout. Matching idempotent task retrieval performs no new write or consumption.

Restores preserve local approval records outside logical backup state, invalidate
unused approvals, and attach consumed restore approval evidence to completion
provenance. PostgreSQL and memory conformance tests cover single use, concurrency,
rollback, expiry, origin revocation, and request binding. See the
[wire protocol and deployment guide](credential_approvals.md).

## Upgrading from 0.3 to 0.4

Update all ten SDK dependencies to exactly 0.4.0 together. Subscription
requests, projections and lookups now carry `EventSubscriptionScope`, which
preserves either a collection ID or system scope. Use `scope().collection_id()`
when an optional collection is required. `TaskKind` is shared through
`hubuum-domain`; subscription filters add `task_kinds`.

Regex queries use the bounded portable language in `hubuum-regex`. Implement
its `Compiler` trait and support every accepted normalized expression through
native execution. `ParsedQueryParam` constructors now return validated regex
operands; `from_parts` is fallible and fields are private. Replace struct
literals with constructors and field access with accessors. Use `with_field`
for aliases and related-field remapping that preserves the operand format.
Consume `regex_pattern()` instead of interpreting the raw value in a native
dialect. Revalidate saved patterns against the
[portable contract](querying.md#regular-expressions).

Implement `load_event_notification`, `enqueue_event_notification_test` and
`finish_event_delivery` on complete adapters. Preserve test purpose and deferral
reason, validate event scope and sink identity before preview/test, and audit
actual test requests. Atomically fence sink admission by the current unexpired
claim. Persist per-sink spacing and provider cooldown across workers. Deferrals
must leave attempts unchanged, clear claims, and honor their next attempt time;
permanent errors terminate delivery. Claim batches must allow other sinks to
make progress. Logical backups preserve delivery policy, system subscriptions
and terminal test deliveries; transient admission state resets on restore.

The built-in PostgreSQL and memory backends share application contract tests in
`src/tests/storage_contract/webhook_notifications.rs`; retain adapter-specific
locking and lease-loss tests when implementing these guarantees.

## Upgrading from 0.4 to 0.5

Update all ten SDK dependencies to exactly `0.5.0` together. This is a breaking
Rust source release; HTTP resource representations and database schemas retain
their existing formats.

Class-relation creation and import inputs now carry `TemplateAlias` and
`ObjectRelationLimit` from `hubuum-domain`. Construct aliases with
`TemplateAlias::new` at input boundaries, or `from_canonical` when reading
persisted canonical values. Construct limits with `ObjectRelationLimit::new`.
Keep these values typed through command preparation and persistence. Use
`StorageClassRelation::with_relation_details` to retain existing proofs;
raw database rows may use the fallible `try_with_*` constructors.
`StorageClassRelation::into_parts` now returns `StorageClassRelationParts` with
named accessors instead of a positional tuple.

`SessionPolicy::deadlines` returns `SessionDeadlines`, which guarantees
`issued < access <= refresh <= absolute`. `StorageSessionIssued::new` takes
this value after its three identifiers. Use `SessionLifetimes::try_new` and
`deadlines` for rotation with persisted policy and the fixed family deadline.
Retain the adapter's transaction, replay detection, and concurrency fencing.

Restore upload validation uses `StorageBackupReplayBuilder::finish_for_restore`
to obtain a validated replay. The older materialized validation constructors and
normalization helpers are removed; apply destination schema limits to the replay. Shared resource checks and legacy normalization
now live in storage-core; application artifact metadata and manifest checks
remain the caller's responsibility. Adapters still enforce cross-row and
transactional restore invariants.

## Upgrading from 0.5 to 0.6

Update all ten SDK dependencies to exactly `0.6.0` together. This is a breaking
supported Rust crate change: the closed task-kind vocabulary adds
`SchemaProfiling`, and `TaskMetadataDetails` and `StorageTaskCompletionPayload`
add matching variants. `StorageSchemaProfileArtifact` carries a bounded report
and expiry timestamp.

Implement both new `TaskExecutionStorage` methods: `get_schema_profile_output`
and `purge_expired_schema_profile_outputs`. Publish the artifact atomically with
successful task completion under the existing lease and cancellation fence.
Retain output expiry metadata after cleanup, and include profiling in task
capacity, discovery, cancellation, recovery, and output-state filtering. Profiling
reports are transient and need not enter backup snapshots; restore must clear
local reports. Run the complete adapter conformance and task-control suites.

The PostgreSQL adapter requires migration
`2026-10-25-000002_schema_profiling`. Drain old workers before submitting the new
task kind. The server uses `json-schema-profiler` 0.0.1; the SDK does not depend
on it and retains its Rust 1.88 minimum version.

## Upgrading from 0.6 to 0.7

Upgrade all ten SDK crates together. `QueryStorage` now requires
`LogicalIdentityStorage`; every selectable adapter must implement the complete
contract. Resolve `StorageIdentityTarget` to `StorageIdentityEvidence` inside a
consistent backend view, preserving typed resource, principal, task and version
identities across projections and backup replay. Assign new identities at writes,
keep historical versions immutable on interval closure, and capture reference
identities before integer IDs can be reused. Return `NotFound` for absent targets.

`HistoryStorage::resolve_history_principal_names` now accepts
`Vec<PrincipalIdentity>` and returns names keyed by that identity. Preserve the
identity evidence in history and task-event projections. Resolve actor and
initiator names only from captured, known UUIDs. Unresolved legacy attribution
must remain unnamed even if a current principal has the same integer ID.

`StorageRestoreReplayApply::at_restore_boundary` takes a
`StorageRestoreIdentityContext` obtained from the confirmed artifact job. It
prepares a `StorageIdentityReplay` before entering the adapter's fenced restore
transaction. Preserve that proof until replay. Format 11 requires identity
metadata; formats 6–10 undergo a quarantined upgrade with a stable per-stage
cohort. Restore existing identities, mint fresh versions only for synthesized
history baselines, and keep retained control-plane requester evidence across
application replacement. The context does not replace the adapter's transaction,
maintenance ownership, or execution fences.

Use `verify_collection_version_identity` with an isolated collection, its read
capabilities and mutation capability. `BackupRestoreFixture::restore` now returns
the confirmed `StorageRestoreIdentityContext`, allowing conformance checks to
verify synthesized baseline identities against the actual restore stage. See the full
[identity coverage and acceptance matrix](logical_identity.md) for retained
relationships, schema dependencies, task outcomes, migration and restore tests.

## Upgrading from 0.7 to 0.8

This is a breaking coordinated SDK release. Advance all ten SDK dependencies
and implement the required `TaskCapacityStorage` capability in every complete
adapter. Policy initialization is atomic and first-writer-wins; later startup
configuration cannot replace a committed revision. Explicit policy updates use
`TaskPolicyRevision` as a compare-and-replace token.

`StorageTaskCreateRequestBuilder::try_build()` no longer accepts a caller-chosen
active-task limit. Remove that argument and initialize the shared
`TaskAdmissionPolicy` instead. Enforce outstanding work, execution occupancy and
resident bytes transactionally against the committed policy. Keep cumulative
work owned by immutable task identities outside logical application restore.
Follow the [task accounting contract](task_admission.md) for restoration,
internal repair admission and administrative diagnostics.

Preserve `StorageError::task_capacity_rejection()` when translating adapter
errors. `into_parts()` now returns the optional structured capacity reason as
its fourth element; update error destructuring and preserve that reason across
adapter boundaries. Only application code translates the bounded rejection
into HTTP.

## Upgrading from 0.8 to 0.9

Update all ten SDK dependencies together. `StorageBackend` now requires
`artifacts::TaskArtifactStore`: implement immutable staged chunks, independent
read-back verification, task-incarnation ownership, atomic reference publication,
resident-byte admission, and idempotent cleanup. The memory and PostgreSQL
adapters implement the complete contract. The standalone `ArtifactStore` trait
remains usable without task execution or an archive provider.
Local producers must stop within `LOCAL_SPOOL_CLEANUP_GRACE` after ownership
loss. Keep their reservation through that interval after fencing an abandoned
attempt; a cleanup call returning zero can still have deferred physical work.

Export and backup completion can carry `VerifiedArtifact` rather than complete
values. Preserve the proof through the completion transaction and fence the
claim, cancellation, output retention and attempt state before publishing it.
`StorageBackupOutput::document()` is optional; use its artifact reference for new
outputs. Continue supporting the inline variants for retained legacy outputs.

Backups use format 12 and include `history.export_artifacts` as bounded manifest
and block frames. Restore these into fresh physical identities, independently
verify destination bytes, and remap output references. Retain prior cleanup
obligations and shared capacity policy across restore. Format 11 identities remain
native; formats 6–10 still require the quarantined legacy identity upgrade.

## Upgrading from 0.10 to 0.11

This is a breaking coordinated SDK release. Upgrade all ten SDK dependencies
and handle `MaintenanceState::RecoveryQuarantine` in exhaustive matches. It
restricts exposure independently of restore draining; restore status endpoints
must not bypass this restriction. Return this state from maintenance and readiness
checks while any deletion intent remains, including completed intents. Block
new task/event claims and retention work under the same restriction.

`DeletionControlStorage::read_deletion_control()` provides minimal authority
without physical backup coordinates. Its default implementation uses capture;
adapters with privileged capture must override it with a read-only runtime
capability. `DeletionControlState::recover_from()` constructs an exact-predecessor
transition that preserves newer local decisions and rejects conflicting histories.
Adapters must accept validated recovery transitions that advance several revisions
under the same exact-predecessor transaction, rather than requiring an increment
of exactly one.
Only trusted services holding authenticated latest evidence may use it for recovery.
See the [recovery procedure](deletion_recovery.md) for the offline boundary.

## Upgrading from 0.11 to 0.12

This is a breaking coordinated SDK release. Upgrade all ten SDK crates and
implement the four new `DeletionControlStorage` operations: `register_runtime`,
`unregister_runtime`, `prepare_hot_object_history_deletion`, and
`cleanup_hot_object_history`.

Runtime registration is atomic against deletion authority and bounded to 1,024
owners. Return the non-cloneable `RuntimeRegistration` capability only after
commit; remove a registration only with its matching ownership token. Dropping a
token, heartbeat age, process failure, and logical restore must not remove a
registration. Keep ownership tokens out of logs and public reports.

Prepare and cleanup lock authority before other rows, compare the exact expected
state, and require every protected registration to be absent in that same
transaction. A racing startup must either register first and block cleanup or
observe the retained intent and refuse startup. Preserve registrations outside
application replacement independently of ordinary restore coordinator heartbeats.

The managed-copy request names one resource/version UUID pair and one explicitly
bound hot object-history copy. Reject open, unresolved, mismatched or ambiguous
history. Delete the exact copy and record its outcome atomically. A retained
`Removed` outcome still requires checking physical presence on retry: an older
application restore can reintroduce that row. Never authorize cleanup from reused
integers or infer a binding for an existing opaque obligation.

`DeletionCheckpointIdentity` validates a content address; it is not evidence of
independent publication or durability. Only the trusted administrator service
holding the authenticated repository session may invoke managed cleanup. Final
completion requires independent publication and readback covering the adapter's
receipt. Keep application exposure quarantined afterward.

Control format 2 adds explicit copy bindings and receipts. Continue decoding and
encoding format 1 byte-for-byte: checkpoint authentication depends on its original
canonical representation. Unknown legacy copies remain unexecutable and retain
their original outcomes. See [managed cleanup](deletion_recovery.md#offline-managed-copy-cleanup).

## Upgrading from 0.12 to 0.13

Upgrade all ten SDK crates together. Every complete adapter now implements
application-incarnation fencing, runtime-owner binding, exact drain
acknowledgements, transactional decision reapplication, and exposure release.
`ExecutionStorage` exposes the current scope and synchronous scope execution for
streaming response polling; nested scopes preserve the outer application permit.
`StorageTaskLease` carries the captured application fence. Adapters persist and
verify the task claim's original fence before productive transitions, even when
another caller supplies a freshly admitted execution scope.

Logical replacement preserves authority and runtime registrations, rotates the
application incarnation, and quarantines retained obligations until independently
protected reapplication completes. A checkpoint's optional reapplication receipt
never imports local release permission. An explicitly unbound adapter remains
available for privileged administration and focused tests; production composition
binds every capability port and provider to its registered owner and rejects
missing, different-owner, stale, or revoked permits.

See [deletion recovery](deletion_recovery.md) for the coordinated offline upgrade,
process-isolation contract, checkpoint ordering, and conservative failure states.

## Upgrading from 0.13 to 0.14

Upgrade all ten SDK crates together to `0.14.0` for the required protected audit
contract and expanded lifecycle event vocabulary.

`DeletionControlStorage::observe_deletion_checkpoint` commits an independently
verified checkpoint observation, its required canonical event and minimum protected
evidence atomically. Use the adapter's recording clock. Audited intent and copy
removal capabilities must preserve original event UUIDs, exact membership and
recording times; failure to persist required audit rolls back their state changes.
Format 3 retains this minimum evidence without erased payloads. Ordinary event
expiry must not remove it. Lifecycle audit is initially system-only and cannot
enter notification subscription, preview or delivery paths.

Completion evidence may advance an already recorded physical reapplication receipt
only through `ApplicationControlState::with_completed_audit`, which rejects changed
decisions, bindings or copy outcomes. Protect the updated receipt and audit facts
before release; do not redo physical cleanup or infer a new drain acknowledgement
from an audit-only revision. Missing required completion audit blocks exposure.

## Upgrading from 0.14 to 0.15

Update all ten exact-version SDK dependencies to `=0.15.0` together.
`TaskExecutionStorage::claim_next_task` now accepts `StorageTaskClaimRequest`,
which carries a stable per-slot `TaskWorkerId`, supported `TaskWorkerKinds`,
and the validated lease duration. Generate the identity once per worker loop,
not once per poll. Preserve the complete policy's required `scheduling` field
and provide `TaskSchedulingSnapshot` when constructing capacity diagnostics.

Adapters must persist weighted rotation, principal service history and promotion
spacing atomically with claims, maintain worker availability with backend time,
and fence/reset worker leases during restore while preserving service history.
Execution caps and small-pool reservation fallback are part of the complete
contract; see [shared scheduling](task_scheduling.md). Rerun conformance and
production-adapter transaction/concurrency tests before deployment.

## Upgrading from 0.15 to 0.16

Upgrade all ten SDK crates together to 0.16.0. This is a breaking adapter change.
Implement the complete `TaskRetentionStorage` contract, protected submission
lookup and activation, and the typed `StorageTaskSubmissionOutcome` creation
result. Preserve stable principal/task identities, original request fingerprints,
terminal-anchored windows and exact admission tickets across adapter boundaries.

Preserve `StorageErrorKind::IdempotencyConflict` across adapter error conversion
for changed protected request fingerprints and task kinds. The application maps
it to HTTP 409 with a bounded reason; do not collapse it into a generic conflict
whose meaning a consumer would have to recover from prose.

`TaskRetentionPolicyChange` compares a `TaskRetentionPolicyRevision`, independently
of the task-control revision. Under the adapter's control lock, reject a competing
policy revision and apply the replacement to the latest authority state, preserving
concurrent receipt changes. Receipt settlement alone must not invalidate the edit.

Export and backup artifact `into_parts` now accepts an adapter-owned readiness
clock and returns a fallible result. `StorageOutputAvailability` preserves either
an existing fixed deadline or a validated post-ready lifetime. Resolve it inside
atomic completion and update physical artifact ownership to the same resulting
deadline. Restore keeps its recorded deadline and must not start a new lifetime.

Checkpoint capture now combines independent deletion and task authority through
`ControlCheckpointCapture`. Exposure release takes `ApplicationExposureProof`;
protected task authority requires an exact current-incarnation task reapplication
proof. Retain historical reconciliation proofs during ordinary task revision
changes, but require exact current revisions at exposure-release CAS.

Implement both task-detail and expired-output purge scopes, aggregate row/byte
budgets, child-write fencing, output/hold/dependency blockers, terminal expiry
protection and recovery of retained decisions. Preserve audit provenance after
parent deletion and grandfather legacy detail-retention anchors. No empty or
unsupported fallback satisfies these required methods. Run the shared protected
admission verifier and adapter-native transaction, locking, migration, recovery
and bounded-purge tests. See [task retention](task_retention.md) for operational
activation and client-visible retry behavior.

Published task-owned artifact chunks, manifests and reservations are part of the
bounded purge contract. Their verification proof fences ordinary physical
cleanup, including while already marked deleting. Abandoned unpublished staging
retains lease-fenced cleanup with current holds pinned, preserving retry capacity.
Recheck holds and immutable task ownership even when its catalog row is
absent, preserve producer grace, and acknowledge completion only after all
physical obligations in the selected scope have been retired.
