# Shared task admission and accounting

The backend-neutral storage contract exposes one validated `TaskAdmissionPolicy`
and a required `TaskCapacityStorage` capability. PostgreSQL and memory enforce
the same logical contract. Application configuration and HTTP errors remain
outside the SDK. This implements [#506](https://github.com/hubuum/hubuum/issues/506)
on the durable identity foundation from #505.

Fair selection, worker-pool availability and reserved execution scheduling are
described in [shared task scheduling](task_scheduling.md). External artifact/provider obligations are added with #508/#516, rather
than represented by unused placeholder tables here.

## Ownership and restore reconciliation

| Record family | Durable owner | Restore transaction and reconciliation |
| --- | --- | --- |
| Shared policy revision | Deployment control state | Preserve outside application replacement; publish updates using the expected revision. |
| Logical outstanding work | Immutable task identity, captured principal and collection identities | Rebuild reservations from authoritative nonterminal work after application replacement, before admission resumes; do not match integer IDs. |
| Execution occupancy | Task identity, unique lease token and control generation | Invalidate pre-restore execution ownership; only a fresh post-restore claim can occupy a slot. |
| Resident request/output bytes | Actual application rows associated with task identity | Reconcile inside the replacement transaction; release only bytes whose managed rows were removed. |
| Cumulative execution work | Immutable task identity | Preserve across terminal transitions, task disappearance, retries and logical restore. A new attempt adds work rather than replacing earlier charges. |
| Future external physical obligations | Owning artifact/provider attempt | Added with the consuming feature; they must survive task disappearance until independently verified cleanup. |
| Future deduplication and deletion decisions | Protected control authority | Added by #511/#512 in the same control foundation, with their independent recovery and expiry rules. |

Ambiguous identity mappings cannot activate an identity-bound reservation.
Restoring older application data cannot establish ownership by matching an
integer principal, collection or task ID. Reconciliation and application replay
must commit together; failure leaves maintenance in control of recovery.

## Policy changes

Admission and execution occupancy are separate limits. Queued requests and
retained outputs also consume resident bytes; cumulative work is never a proxy
for resident storage. Lowering a limit does not cancel or preempt existing work.
The affected scope drains before new work can be admitted or claimed.

Internal computed/schema repair has a distinct admission allowance that ordinary
submissions cannot consume. An ordinary caller cannot select the internal lane.
Finite internal limits still apply. Execution reservations and their single-worker
fallback follow [shared task scheduling](task_scheduling.md).

Only administrator diagnostics disclose deployment totals. Public capacity
rejections use bounded reason codes and do not include another principal's
identity, usage or queued work.

## Effective settings and defaults

The first API or worker startup initializes the shared policy from the existing
per-principal import, export, backup and remote-call settings. Later starts
receive the persisted policy even if their environment differs. Updates are
immediately authoritative for every admission and claim transaction.

| Scope | Outstanding tasks | Executing tasks | Resident bytes |
| --- | --- | --- | --- |
| Ordinary deployment | 10,000 | 64 | 4 GiB |
| Principal | 500 | 16 | 1 GiB |
| Kind | 5,000 | 64 | 4 GiB |
| Applicable collection | 1,000 | 32 | 2 GiB |
| Internal repair | 10,000 | 64 | 1 GiB |

Per-principal kind defaults remain 100 for imports, exports and remote calls,
1 for backups and 2 for schema profiling. Internal reindex and schema validation
use the separate internal lane. Ordinary HTTP submissions cannot select that
lane. Collection scopes are captured from an explicit discovery target, including
both relation endpoints; broad unscoped queries and imports use deployment,
principal and kind budgets. Moving a resource later does not rewrite the
captured admission owner.

New work resolves its captured resource UUID against the current resource row
and captures that row's current collection membership. This permits repair work
after a legacy restore without claiming that the legacy archive's unresolved
historical references are known.

All limits are positive integers bounded by signed 64-bit storage. Byte and
count limits have distinct private-field newtypes. The policy must contain each
of the seven task kinds exactly once. The per-task cumulative input-byte ceiling
is initially 1 TiB. A queued retry that would exceed it remains admitted but
cannot claim; an administrator can raise the ceiling or cancel the work.

`GET /api/v1/admin/task-capacity` returns the shared policy revision, control
generation, ordinary/internal usage, unresolved owners and work-budget-blocked
counts. `PUT` accepts `expected_revision` and a complete `policy`; a concurrent
update returns 409 and requires a refetch. Both require unscoped human
administrator access. `/api/v1/admin/config` reads the same live snapshot and
projects its effective per-kind values into the existing compatibility fields.

Capacity rejections return 429 with a bounded `task_capacity_*` reason.
Unavailable identity mapping or incomplete reconciliation returns 503. Neither
response contains usage, task IDs or another principal's information. Policy
and usage reads are administrator-only; they are not added to client config.

## Accounting and transaction boundaries

PostgreSQL uses one short control-row lock to serialize policy changes and
capacity acquisition. Terminalization, redaction and output removal only lock
their accounting row, avoiding lock inversions during cascading class deletion.
Until those releases commit, admission conservatively sees the old occupancy.
Claim selection takes the control lock before task
rows and uses `SKIP LOCKED`; it never waits for a task row while holding the
control lock. Eligibility aggregates occupied scopes once per decision.
Completion and cancellation update accounting in the task transaction. Memory
uses its state write lock and commits output publication and accounting together.

Existing idempotency lookup runs before reservation creation and is rechecked
under the admission lock. A recognized duplicate returns the existing task and
charges nothing, even when its scope is full. This is the integration point for
issue #512's protected receipts; task-row deduplication still has its existing lifetime.

Resident accounting measures encoded request JSON and retained output content:
export content/metadata/warnings, backup bytes, schema reports and remote response
headers/previews. PostgreSQL measures JSONB's text encoding; memory measures
compact JSON. These are application-content budgets, not estimates of heap,
indexes, WAL or filesystem allocation. Output publication checks peak retained
input plus output; terminal transitions release the request bytes. Artifact
expiry releases resident bytes only when its rows are actually removed.

Each new claim charges its retained input bytes. Output content publication
charges new content even when replacement is smaller. Progress adds positive
processed-item increments. Cumulative counters survive retries, completion,
removal of application task rows and logical restore; replay itself does not
charge another execution. They describe these measured operations, rather than
estimating database scanning or provider traffic. #508/#516 attach their actual
transfer/staging obligations when those consumers are introduced.

Lost leases retain occupancy until the existing recovery transaction fences
and settles or requeues them. Requeued repair work retains its logical owner;
a fresh claim adds cumulative work. Restore advances the control generation,
invalidates old leases and reconstructs current reservations in the same fenced
transaction. Required recreated repairs are registered even above a lowered
internal limit; new admission waits for that scope to drain. Original UUID
owners and cumulative charges remain detached when a task is absent.

## Durable waiting foundation

The first slice of [#516](https://github.com/hubuum/hubuum/issues/516) adds
`wait_task` and `resume_task` to the complete task execution storage contract.
PostgreSQL and memory implement the same transitions. Existing built-in export
and backup producers do not call them. No provider SDK, notification endpoint,
polling loop, or shared retrieval service is enabled by this foundation.

A live, uncommitted export or backup claim can enter `waiting` after execution
has admitted its absolute deadline. The transition persists a validated private
checkpoint and a fresh opaque wait token while clearing the execution lease.
Cancellation, an expired deadline, committed effects, and unsupported task kinds
cannot create a wait. In particular, remote-call dispatch never becomes safely
replayable through this API.

Checkpoints contain a format name, a positive format version, and an object
payload. The complete UTF-8 encoding is limited to 64 KiB; format names contain
at most 64 lowercase ASCII letters, digits, dots, underscores, or hyphens and
start with a letter or digit. Constructors validate this representation once.
Checkpoint payload data and wait tokens remain outside public responses, event
payloads, and diagnostic logs. Wait events include only bounded checkpoint
metadata: format, version, and encoded byte count.

Waiting retains logical admission and request/output storage charges. Checkpoint
bytes join resident input accounting, while execution occupancy is released.
Publishing a checkpoint does not invent cumulative execution work. A later
fresh claim charges the retained request and checkpoint bytes together; retries
preserve earlier charges. The original deadline, start time, and progress remain
unchanged across wait and resume.

A notification supplies the task ID and matching opaque token. The first
matching notification makes the task `queued` and consumes that token. It does
not acquire execution capacity: normal queue claiming grants a fresh lease.
Missing, stale, and already-consumed tokens produce `Unchanged` without an event.
This also fences notifications from an earlier wait of the same task. Old lease
tokens cannot renew, update, or finalize the resumed task.

Cancellation and disabling the submitting service account finalize waiting work
without acquiring execution. The bounded recovery sweep settles expired waits;
an unexpired wait is not a lost lease. A matching notification also settles an
expired or stopped task before considering maintenance. Otherwise, maintenance
blocks a matching resume; entering wait under an existing live lease can release
execution while the deployment drains. Terminal transitions remove the private
checkpoint and release its resident bytes atomically.

Current built-in executors reject any stored checkpoint before ordinary
execution. A consumer must implement its exact checkpoint format before it can
use resumption. Provider reservations, independently owned physical storage,
polling limits, shared-consumer authorization, and cleanup obligations that
outlive a task remain later #516 work. The foundation does not certify those
resource or recovery guarantees.

Logical backups retain terminal task history only. Pending waits and their
checkpoint tokens are not exported or restarted by logical restore. Reusing an
integer task ID does not restore its old token. Applications requiring a wait
to survive logical replacement need a later durable provider-obligation contract;
process restart alone preserves the currently stored wait.

Migration `20261029000001` can be installed during a rolling upgrade while this
foundation is dormant. Before enabling any consumer, drain old workers and deploy
matching API/worker binaries. Closed REST task-status enums
must accept `waiting`; regenerate clients from OpenAPI. Administrator queue
diagnostics expose `waiting_tasks` separately, leaving `active_tasks` limited to
occupied validating/running work. Before downgrading, finish or cancel all tasks
with checkpoints. The reverse migration refuses to discard a remaining wait or
checkpoint into an older worker's ordinary execution path.

## Upgrade and recovery

The task-capacity migration requires stopped API and worker replicas. Take a database backup
and, for split roles, reapply generated role setup/grants SQL as the database
administrator so the owner can create the protected control schema. Run
`hubuum-admin --migrate` to complete the resumable identity and capacity
backfills and reconcile runtime grants, then start matching binaries. Startup
refuses to initialize admission if application task rows lack their accounting
owners. Unknown identity mappings appear as blocked diagnostics and cannot
activate reservations; cancellation or a validated recovery must resolve them.
Existing resident content is measured during backfill; historical byte work is
not invented retroactively.

Control tables have no cascading foreign keys into application data and are
excluded from logical backups. Runtime roles can invoke the narrow policy and
claim functions but cannot modify protected control tables directly. Trusted
restore reconciliation requires owner authority and maintenance fencing. The
same foundation is extended by #511; a whole-database rollback can roll back
these tables too and is outside this logical-restore guarantee.

SDK authors must implement the complete capability and move caller-local quota
arguments into policy initialization. See the
[0.8 SDK upgrade guide](storage_adapter_sdk.md#upgrading-from-07-to-08).
