# Class schema evolution

Every class has an immutable schema revision, including classes without an
object-validation requirement. Schema changes can be staged, analyzed, and
activated without rewriting existing object JSON. PostgreSQL and the experimental
process-local memory backend implement the same required storage capability.

## Revisions and object compliance

`SchemaRevision` is a positive number allocated monotonically within one class.
`SchemaReference` binds that number to a `ClassId`. Neither is the class or
object's ordinary `ResourceRevision`. Revision documents, enforcement flags, and
creation provenance are immutable. Lifecycle transitions are `staged` to
`active` or `abandoned`, then `active` to `retired`. Activation moves forward;
to restore an earlier document, stage it as a new revision. Staging an equivalent
active or staged policy returns the existing revision.

The existing class `json_schema` and `validate_schema` fields project the active
revision. PostgreSQL triggers and deferred constraints prevent divergence and
reject direct changes to retained documents. The memory adapter checks the same
rules in its mutation boundary. Enforced revisions carry a compiled schema
proof. External schema references remain unsupported.

Evidence records the exact schema reference, inspected object resource revision,
validation result, and actual validation time. Effective status is calculated
from that evidence:

| Active policy and evidence | Status |
| --- | --- |
| Absent or advisory schema | `not_required` |
| Enforced, matching object and schema revisions, successful check | `valid` |
| Enforced, matching revisions, failed check | `invalid` |
| Enforced, missing or outdated evidence | `pending` |

Ordinary object creation, update, JSON Patch, and import writes validate and
record evidence atomically. Failed writes retain the previous object. Native
writes and metadata changes conservatively invalidate older evidence through
the resource revision fence. Validation jobs never change object JSON, resource
revisions, or user timestamps. Invalid objects remain readable under normal
permissions; subsequent writes must satisfy the active schema.

The migration gives existing classes revision 1 and preserves their policy.
Existing enforced objects start pending; other objects are not required.
Administrators can request revalidation after upgrading.

Deploy the migration during a quiet period. Indexes and the expanded task-kind
constraint commit atomically with the schema state; lock acquisition is limited
to five seconds and each statement to sixty seconds. A timeout rolls back the
whole migration, so retry it after reducing load. The task-kind replacement is
validated before commit and preserves every previously accepted task kind.

## API workflow

All routes below are relative to `/api/v1/classes/{class_id}/schema`.

| Method and path | Operation |
| --- | --- |
| `GET /revisions` | Page retained revisions with `after` and `limit` |
| `POST /revisions` | Stage `json_schema` and required `validate_schema` |
| `GET /revisions/{revision}` | Read a revision |
| `DELETE /revisions/{revision}` | Abandon a staged revision |
| `POST /revisions/{revision}/impact` | Queue bounded impact analysis |
| `POST /revisions/{revision}/activate` | Activate with an explicit policy |
| `POST /revisions/{revision}/revalidate` | Queue revalidation of the active revision |
| `GET /tasks/{task_id}` | Read progress and bounded findings |
| `DELETE /tasks/{task_id}` | Cancel work and fence later batch commits |
| `GET` | Read active revision, population epoch, and compliance counts |
| `GET /objects` | Page compliance metadata, optionally filtered by `status` |

For example, stage an enforced revision:

```json
{
  "json_schema": {
    "type": "object",
    "required": ["hostname"],
    "properties": {"hostname": {"type": "string"}}
  },
  "validate_schema": true
}
```

Request impact for the returned revision and poll its task. Then activate:

```json
{
  "expected_active_revision": 1,
  "policy": "reject_incompatible",
  "impact_task_id": 123
}
```

`reject_incompatible` requires an empty class or a completed compatible impact
analysis whose population epoch still matches at activation. Invalid,
uninspectable, or stale rows prevent proof. Concurrent object writes, inserts,
deletes, and moves invalidate the proof, including changes to already scanned
rows. An outdated expected revision or proof returns `409 Conflict`.

`allow_pending` requires administrator authority and permits activation without
compatibility proof. It immediately projects older evidence as pending for an
enforced schema. Both policies atomically queue revalidation; disabling
validation immediately projects `not_required`. Activation returns the active
revision, revalidation task ID, and any dependent rebuild task ID. It does not scan or update the object population
inside the PostgreSQL activation transaction.

Class read permission permits revision reads. Class update permission permits
staging, abandonment, and strict activation. Class counts, impact and revalidation
requests, progress reports, and cancellation additionally require an unscoped token and administrator
authority because aggregate findings can reveal hidden objects. Generic task
read and event endpoints enforce this same report restriction; task listings
exclude schema work before counting or pagination for other callers. Initiator
attribution remains intact. Class permission
checks still use the configured authorization backend. Mutations recheck the
authorized collection at the storage boundary to reject a concurrent class move.

Object compliance pages apply `ReadObject` authorization individually and omit
totals. `next_after` advances past inspected candidates, including hidden ones;
a page may therefore contain fewer visible results. Limits are 1–100, with a
default of 50. Ordinary class and object response shapes remain unchanged;
these routes expose their schema metadata separately.

## Bounded work and recovery

### Planning with an impact report

Stage either a first schema or a replacement with `POST /revisions`, then request
`POST /revisions/{revision}/impact` and poll `GET /tasks/{task_id}`. The worker
evaluates each object snapshot against both the proposed policy and the immutable
active revision captured when the task was queued. Existing evidence is not used
as a substitute for evaluating the baseline. Object data and active evidence stay
unchanged; mismatch events and audit entries still record advisory findings.

The `impact.baseline` reference identifies the comparison policy. The disjoint
`impact.counts` fields sum to `examined`:

| Count | Meaning |
| --- | --- |
| `newly_invalid` | Previously valid or not required, now invalid |
| `newly_valid` | Previously invalid, now valid |
| `still_invalid` | Invalid under both policies |
| `still_valid` | Valid under both enforced policies |
| `newly_required_valid` | Previously not required, now valid under enforcement |
| `no_longer_required` | Previously enforced and inspected, now not required |
| `unchanged_not_required` | Neither policy requires validation |
| `uninspectable` | Either comparison could not run, or the object changed before commit |

`impact.failures` groups the first failing constraint per object, with at most
20 groups and five object IDs per group. Each group includes `objects` and a
`reason` containing the JSON Schema `keyword` (`falseSchema` for a boolean-false
schema), a schema-owned `schema_path`,
and, for missing required properties, `missing_property`. No instance paths,
unexpected property names, instance values, or validator messages are included.
Schema paths exceeding 512 bytes and property names exceeding 128 bytes are
omitted. `ungrouped_failures` counts failures whose group did not fit the limit;
the overall invalid count remains complete. These are first-failure counts,
not an exhaustive list of everything that must be repaired in each object.

For example, a report could show 43 `newly_invalid` objects, with one group of
38 failures for missing `hostname` and another of five `type` failures at
`/properties/hostname/type`. Use the sample IDs to inspect authorized objects,
repair them under the active policy, or stage a revised proposal. Then request
a fresh impact task before activation.

The response's `readiness` is recomputed against the class state at read time:

| Readiness | Meaning |
| --- | --- |
| `compatible` | Complete, fully inspected, current comparison with no candidate failures |
| `incompatible` | Complete, fully inspected, current comparison with candidate failures |
| `inconclusive` | Incomplete or failed work, changed population or baseline, unavailable candidate, or an uninspectable comparison |

`current_epoch` and `current_active_schema` identify the observed state. A report
can become outdated immediately after it is read; strict activation checks the
population and baseline again inside its transaction. Budget or regex execution
failures are uninspectable, not proof of a schema mismatch. Increasing admission
budgets may allow a new task to inspect those objects. Impact checkpoints created
before comparison metadata was available remain inconclusive and need a fresh
task. Revalidation responses have no impact comparison or readiness.
Polling reads only the task checkpoint and indexed schema state; it does not
scan objects or recompute compliance totals.

### Checkpoints and execution

Impact and revalidation share the `schema_validation` task kind and use distinct
work kinds. Requests deduplicate running work by class, revision, and kind.
After completion, cancellation, or failure, another request starts a fresh scan
of the class, including stale and invalid objects.

A checkpoint retains the target, initial population epoch, maximum object ID,
cursor, counters, up to 20 invalid object IDs, batch count, and elapsed batch
time. Matching start/end epochs make completed impact exact at completion;
activation rechecks that epoch again. An analysis with population changes is
advisory. Impact checkpoints also retain the baseline, comparison counts, and
bounded failure groups described above. Event and audit findings retain fixed
categories; their payloads omit validator messages, instance values, and paths.

Each batch reads snapshots in object-ID order, validates outside its write
transaction, then rechecks the lease, checkpoint, active revision, and inspected
object revisions before publishing results. Evidence, progress, task completion,
events, and audit records commit together. A superseded revalidation terminates
without satisfying the new schema. Historical impact can finish but cannot
satisfy another revision. Cancellation preserves already committed results and
prevents later batches from publishing. Worker errors and graceful-shutdown
interruptions atomically mark both the task and schema checkpoint as `failed`,
preserving already committed findings. Class deletion cancels queued work.

Workers default to 64 rows, 8 MiB of serialized JSON per batch, and 2 MiB per
object. The object ceiling follows `HUBUUM_SCHEMA_MAX_INSTANCE_BYTES`; the batch
byte ceiling grows to fit one configured object, up to 16 MiB. The typed storage limits allow at most 100 rows and 16 MiB per batch.
An oversized object is counted as uninspectable and remains pending under an
enforced schema; it cannot support strict activation. The schema and instance
admission budgets are [deployment settings](json_schema_validation.md), shared
by writes, imports, workers, and restore validation. Batch duration depends on schema complexity;
there is no claim of a hard validator execution deadline. PostgreSQL queries
avoid returning oversized object JSON. The memory worker copies only snapshots
within the byte budget.

Lease recovery preserves the committed cursor and counters. Expired or replaced
claims cannot commit. A retry uses a fresh task after a terminal failure. A counter records dependent rebuilds queued by explicit activation. Generic
task metrics expose backlog and recovery; reports provide per-task batch and
elapsed statistics. Database query statistics remain in storage observation
and the native regression capture, rather than a caller-supplied proof field.

## Events, audit, and history

Schema staging, abandonment, activation, work requests, cancellation, and deletion
emit `class_schema` events and audit documents. Activation includes the previous
and new schema identities, selected policy, projected object effect, revalidation
task, and any dependent computed rebuild task.

Committed validation findings emit `object_validation` events with `succeeded`
for valid evidence, `failed` for mismatches, and `updated` when validation becomes
not required. They record object/schema revisions and safe result categories.
Impact mismatches also emit findings, explicitly marked as advisory with
`compliance_changed: false`; they do not publish active-schema evidence. Event
publication and the audit document use the existing durable event/outbox
transaction. Lease rejection and rolled-back import writes publish neither.

Schema lifecycle history retains immutable document snapshots independently of
live-class deletion. Object resource history remains unchanged by background
validation; its evidence changes are recorded in validation audit documents.
The class activation record accounts for the immediate population-wide pending
or not-required projection. Individual object records follow as batches inspect
them, avoiding an unbounded activation transaction.

## Imports and backups

New classes start at revision 1, and objects created in the same import use that
revision. An overwrite on a nonempty class must select a previously staged
revision through optional `schema_activation` on the class input:

```json
{
  "revision": 2,
  "expected_active_revision": 1,
  "policy": "allow_pending",
  "impact_task_id": null
}
```

The imported class policy must exactly match that staged revision. Import
activation requires administrator authority. Strict mode commits activation,
queued work, objects, evidence, and audit together or rolls them all back.
Best-effort mode reports a class activation failure separately from object item
failures. Dry-run details include activation intent and any referenced impact
report, while preflight checks the real operation in a rollback transaction.
Timestamp preservation cannot supply schema evidence or validation timestamps.
Idempotent task replay retains the existing import replay contract; equivalent
legacy policies do not allocate revisions.

Backup format 6 adds `class_schema_revisions`, `class_schema_state`, and
`object_schema_evidence` state sections and `class_schema_history` when history
is included. Pre-replacement validation checks references, lifecycle, compiled
policies, active projection, population counts, and evidence integrity. Current
successful evidence is rechecked against its object document. Work checkpoints,
impact proofs, and active leases are not restored; fresh revalidation tasks are
queued for enforced classes. History retains documents for deleted classes.
Reconstructed jobs are covered by the restore completion audit entry; subsequent
validation findings emit the usual object events and audit entries.

Schema provenance uses UTC timestamps at microsecond precision in live revisions,
validation evidence, and history snapshots. Logical restore preserves timestamp
offsets for PostgreSQL's timezone-aware schema columns, independently of the
restore connection's configured timezone.

Version 5 and older artifacts must be restored with their matching old release,
then the database upgraded and a new format 6 backup created. No automatic
artifact conversion is provided. Install matching server, administrator, and
restore-executor binaries, drain old workers, and run migrations before starting
new processes. See [backup and restore](backup-restore.md).

## Storage architecture and dependencies

`SchemaEvolutionStorage` is required by `WorkflowStorage`. Domain identities and
compiled proof live in `hubuum-domain`; private-fielded requests, checkpoints,
and evidence live in `hubuum-storage-core`. Application services use the opaque
observed storage handle. PostgreSQL SQL, locking, triggers, and adapter errors
remain in `hubuum-storage-postgres`; memory implements the complete contract.

Activation invalidates shared computed-field evaluation generations and queues
existing fenced rebuild work. `SchemaReference` is the reusable dependency
identity for future indexes and effective/inherited schemas. This change does
not add declarative indexes, inheritance, or automatic JSON transformations.

The seven coordinated SDK crates move from 0.2 to 0.3 because the mandatory
capability, task/event vocabularies, import DTO, and backup sections change.
Adapters must implement all eleven schema methods, handle `schema_validation`,
map the new event entities and logical sections, and preserve the schema
transaction and lease semantics. See the storage boundary inventories.

## Verification and performance

Shared backend tests cover policies, atomic evidence and imports, redaction,
authorization, concurrent activation, cancellation, recovery, and batch bounds.
Native PostgreSQL tests retain direct constraint violations, an object write
between inspection and commit, and a 128-object workload with 256 KiB JSON rows.
That workload asserts constant activation query count, two checkouts per batch,
and the aggregate byte bound. Ordinary backup/restore conformance exercises the
new sections with both adapters.

Run `cargo bench --bench schema_validation_criterion` for deterministic compiled
validation and budget-rejection throughput without database or global config.
Separate groups cover accepted 16/256/1024 KiB payloads and rejected 2/3/4 MiB
payloads, each with 128 integer samples. Fixtures check their expected outcome
before timing. Batches contain at most 64 documents and 8 MiB of serialized JSON;
reported throughput counts documents inspected. Admission also charges actual
JSON escaping with conservative punctuation estimates and schema complexity, so a serialized object below the
worker's byte limit can still exceed the validation budget.
The native storage regression measures the database behavior; benchmark numbers
are hardware-dependent and do not establish a worst-case lock or CPU deadline.
