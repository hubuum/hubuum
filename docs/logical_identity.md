# Logical identity across restore

Implementation contract for [#505](https://github.com/hubuum/hubuum/issues/505),
under [#503](https://github.com/hubuum/hubuum/issues/503). This coverage matrix is
committed before schema and backfill changes. It describes required behavior;
completion of the implementation and its acceptance tests is tracked separately.

## Identity rules

An integer REST identifier locates a record in one application timeline. An
immutable resource UUID identifies its incarnation across backup and restore.
Each retained historical version has a separate immutable version UUID. Closing
an interval changes its end time without replacing either UUID. Restoring a
record preserves its UUIDs; subsequent inserts and versions mint fresh UUIDs,
even when their integers or revision numbers occur in a discarded timeline.

Use private-field domain types to distinguish resource, principal, task, version,
and legacy-cohort identities. Database UUID representations stay inside adapters.
A reference has one of three meanings: known identity, explicitly unresolved
legacy identity, or absent/system attribution. An unresolved integer is never a
fallback lookup key for today's live records. A name is display data, not proof.

## Coverage matrix

The storage names below identify existing persistence locations, not a new
table-shaped public API. Backend-neutral records and operations carry the
validated identity facts needed by their consumers.

| Family | Resource identity | Version identity | References captured with the record |
| --- | --- | --- | --- |
| Collections | `collections` and every snapshot in `collections_history` share the incarnation UUID. | Each history row owns a distinct UUID, stable on closure. | Historical parent collection, actor, initiator and task. |
| Classes | `hubuumclass` and `hubuumclass_history` share the incarnation UUID. | One UUID per history row. | Snapshot collection, exact schema dependency, actor, initiator and task. |
| Objects | `hubuumobject` and `hubuumobject_history` share the incarnation UUID. | One UUID per history row. | Snapshot class and collection, schema evidence, actor, initiator and task; movement cannot rewrite older memberships. |
| Class relations | Relation incarnation shared by live and history rows. | One UUID per history row. | Both endpoint class identities, including deleted endpoints and endpoints outside the current collection; actor, initiator and task. |
| Object relations | Relation incarnation shared by live and history rows. | One UUID per history row. | Both endpoint objects and the represented class relation; actor, initiator and task. |
| Export templates | Template incarnation shared by `export_templates` and its history. | One UUID per history row. | Snapshot collection, optional class, actor, initiator and task. Related-object selectors capture class and class-relation identities; relation-context depth is an option, not a resource reference. |
| Remote targets | Target incarnation shared by `remote_targets` and its history. | One UUID per history row. | Snapshot collection, optional class, actor, initiator and task. Template text remains user data. |
| Principals | `principals` owns the human/service-account identity; `users` and `service_accounts` share that identity. | No synthetic temporal principal version is implied. | Identity scope and any principal attribution retained after deletion. Deleted principals remain distinguishable from later integer reuse. |
| Identity scopes and groups | Stable identities for scopes and groups referenced by durable policy or membership. | Existing revisions remain revision counters. | Principal/group membership and its source retain their endpoint identities. Composite associations do not need unrelated resource UUIDs. |
| Authorization | Collection and principal/group identities identify grants and policy scope. | Authorization revisions remain concurrency tokens. | Collection hierarchy and grants retain the identities represented by their source state. Rebuilt closure/cache rows do not acquire independent logical identities. |
| Schema revisions | The owning class identity plus an immutable schema-revision identity identifies `class_schema_revisions`. | Schema history entries have their own identities; embedded snapshots preserve the schema-revision identity. | Class, creator/actor/task and exact schema dependency. `(class integer, revision integer)` alone is insufficient across restores. |
| Object schema evidence | The object identity identifies the evidence owner. | Bind evidence to the represented object version/revision and schema-revision identity. | Exact class/schema dependency; restoring evidence must not reinterpret it through today's schema. |
| Computed definitions | Each `computed_field_definitions` incarnation has a UUID. | Each definition revision and each complete definition-set history version has a distinct identity. | Owning class, personal owner where applicable, and embedded definition identities/revisions. Preserve the existing shared-history exclusion of personal owner/actor data. |
| Tasks | Stable task identity survives independently of the task's rotating execution lease. | Lease/claim tokens are fencing, not task or historical version identities. | Submitter, initiator, canceller, deleter and typed resource references in requests/checkpoints. |
| Task outcomes/artifacts | Results bind to stable task identity; artifact/write-attempt identity remains a separate contract owned by #508. | Retained outcomes must preserve the task and represented resource/version dependencies. | Import results, export/backup outputs, remote-call results, schema outputs/findings and provenance must remain attributable after task purge. |
| Events | Preserve existing `events.event_id`; it is already an event UUID. | The event UUID does not identify a referenced resource version. | Resource before/after, actor, initiator, task, collection and typed metadata references; keep arbitrary user JSON untouched. |
| Configured integrations | Event sinks and subscriptions have stable resource identities. | Existing revisions retain their existing semantics. | Collection/grant scope and subscription-to-sink references. Delivery records preserve event and subscription identities when retained. |
| Query usage | Declarations refer to stable resource identities. | Existing physical-index ownership UUIDs keep their separate meaning. | Declared class/resource dependencies; never reuse `query_usage_resources.identity` as a logical resource UUID. |
| Restore control | Restore job/receipt identity remains independent of replaceable application rows. | Existing job/maintenance generations retain their fencing role. | Known initiator identity survives application replacement; stored integer, scope and name cannot prove a new principal is the original requester. |
| Sessions, credentials and caches | Session IDs, token material, server IDs and claim tokens retain their existing purpose. | No new resource/version identity for regenerated caches or execution leases. | Principal/resource identities are required only where a retained reference crosses the restore boundary; backup exclusions remain explicit. |

Import-result `identifier` values and export-output `template_name` values are
display data. They do not establish resource correspondence and must never be
resolved into archive or erasure targets by name. Results retain their task UUID;
the task's canonical typed discovery metadata retains its explicit target,
template, remote-target and schema bindings. Import-local named references are
resolved during execution and are not durable resource identities.

Active tasks and schema work checkpoints are excluded from logical restore.
Schema profiling and repair artifacts retain their task binding in native
storage, but this identity migration does not add them to the backup format or
make work from the discarded timeline resumable. Artifact attempt identities
and retention rules remain the separate contract in #508.

## Backfill and legacy evidence

One authoritative mapping owns each established legacy incarnation. Its key
includes a documented cohort/lineage and family plus the evidence establishing
that incarnation. A family/integer pair alone cannot distinguish discarded
restore timelines. A deployment-wide lineage alone cannot do so either.

Include history-only resources and identities present only in references. All
versions of one proven incarnation use its one mapping. Assign a version UUID
once; repeated batches never replace an existing UUID. When available evidence
cannot distinguish incarnations, record unresolved coverage rather than merging
them or minting unrelated resource UUIDs independently for every snapshot.

Backfill uses bounded transactions and database uniqueness to arbitrate mapping
creation against concurrent inserts, updates, deletes and retries. It must not
bump user-visible revisions, change creation/update times, or synthesize history
merely to add identity metadata. Newly written data records identities at the
write boundary; old references are resolved from their captured evidence, never
through a convenient current-row join. Activation requires checked uniqueness,
immutability and reference coverage, including explicit unresolved counts.

Legacy restore runs an identity upgrade in quarantine before destructive
replacement or exposure. A fresh import cohort can identify the imported data
without claiming correspondence to existing archives or deletion receipts.
Unresolved correspondence cannot activate cross-restore control references or
archive publication in an existing lineage. Report the missing coverage and
preserve existing supported backup readers, including explicit versions 6, 7,
8, 9 and 10 when a new format is introduced.

The upgraded replay is itself a validated value. Services and adapters consume
that value; they do not independently assign missing UUIDs while inserting rows.
Use the replayable bounded backup spool. History-free restore preserves resource
identities and gives genuinely synthesized baseline versions fresh, retry-stable
version identities. Existing historical versions pass through unchanged.

## Implementation boundaries and compatibility traps

| Boundary | Required implementation |
| --- | --- |
| `hubuum-domain` | Crate-owned identity types and explicit known/unresolved states with validating constructors; no Diesel, pools, application errors or global configuration. |
| `hubuum-storage-core` | Backend-neutral record/reference carriers, backup validation and replay upgrade contracts. Preserve proof types through operation-shaped interfaces. |
| `hubuum-storage-postgres` | One new migration version, generated Diesel schema, constraints, trigger-safe writes and bounded backfill. PostgreSQL errors convert at the adapter boundary. |
| `hubuum-storage-memory` | The same identity, historical-reference and backup/restore semantics in the complete selectable adapter. Its explicit row parsers must preserve every new field. |
| Root application | Keep REST integers/revisions compatible, integrate backup format validation and restore quarantine, and make history/name enrichment use captured identity. |
| `hubuum-storage-conformance` | Shared behavioral assertions for every selectable adapter, plus adapter-specific transaction/concurrency/recovery tests in PostgreSQL. |

Before changing the implementation, account for these existing behaviors:

- `reset_restore_sequence` already preserves integers retained by several history
  families. Retain that protection; it does not identify a discarded timeline.
- PostgreSQL restore explicitly projects physical columns through
  `jsonb_populate_record`. Missing legacy fields become null; column defaults
  alone do not upgrade old backups.
- Generic temporal triggers copy base columns dynamically. Introduce compatible
  history representations before making new base fields writable; interval
  closure must preserve the historical version identity.
- Restore uses `TRUNCATE ... RESTART IDENTITY CASCADE`. Protected control data
  must have no cascading foreign keys into application tables. Test survival
  through real restore, not just omission from a truncate allowlist.
- Historical name/provenance enrichment currently resolves some integer IDs
  through current principals/tasks. Update those readers with the writers.
- Computed definitions and schema snapshots contain typed references inside
  JSON, including structures that reject unknown fields. Update those contracts
  deliberately; do not rewrite arbitrary numeric values in user payloads.
- Adding a required storage capability or changing a closed SDK vocabulary is
  an incompatible experimental SDK change. Follow the coordinated release-train
  policy and document upgrade actions when that boundary changes.

## Acceptance matrix

| Scenario | Required evidence |
| --- | --- |
| Older backup followed by integer reuse | Restored records retain UUIDs; discarded-timeline resources, versions, tasks and principals cannot match new data. |
| Multiple versions and closure | Resource identity is shared; version identities are distinct and stable when validity ends. |
| Deleted or moved references | Memberships, relation endpoints and actor/task attribution use captured identities, including deleted targets. |
| Ambiguous legacy data | Explicit unresolved coverage, deterministic retry within one upgrade, no guessed attachment to live resources or retained external state. |
| Legacy format compatibility | Parameterized versions 6–10 plus the new format; history present/absent; resolved and quarantined outcomes; both complete adapters. |
| Backfill interruption and racing writes | Bounded work, convergence on retry, no duplicate/overwritten identity and no user-visible revision/history churn. |
| Definition/schema dependencies | Embedded references and synthesized restore baselines retain the correct resource and version identities. |
| Protected references and failure | Real restore preserves control attribution; failed replacement rolls back; runtime roles cannot bypass identity immutability. |
| Packaging and API compatibility | SDK policy, full tests, clippy, formatting, generated schema/OpenAPI and required container checks pass; changes have upgrade/changelog notes. |

Archive providers, schedulers and erasure executors remain outside #505. Its
identity proof is a prerequisite for their cross-restore references, not a claim
that those features have been implemented.

## Upgrade and backup compatibility

This change requires an offline upgrade. Stop API and worker replicas running an
older binary and take a database backup before applying the migration. Run
`hubuum-admin --migrate` with the migration credentials. The command reconciles
role ownership, then commits identity backfill in bounded batches and checks
completion. If interrupted, rerun the same command; assigned identities remain
unchanged. Start matching binaries after the command completes. Keep the
pre-upgrade database snapshot for rollback to an older binary.

Logical backup format 11 carries identity evidence. Older binaries cannot read
that format. The new reader continues accepting formats 6, 7, 8, 9 and 10. A
confirmed restore of a legacy format receives an isolated upgrade cohort;
retries of that stage retain its assignments. A separately staged legacy import
receives a different cohort and does not establish correspondence to an existing
archive or erasure receipt. Ambiguous historical references remain explicitly
unresolved, including historical attribution that cannot be proved from the
backup. A format 11 document missing required identity evidence is rejected
before application replacement. Staging reports `identity_quarantine` and
`unresolved_identity_references` in its validation summary. Legacy staging checks
the entire upgrade in a disposable cohort; confirmation binds final assignments
to the retained restore stage. Neither preview nor unresolved references establish
correspondence with an existing external archive.

Internal UUIDs do not replace REST integer IDs or revision counters. Consumers
must not treat a restored integer or revision counter as proof of correspondence
with a previously retained reference.
