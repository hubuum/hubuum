# Task receipts and retention

Protected task receipts preserve submission deduplication after task details are
removed. They apply to imports, backups, direct exports, template runs, remote
target invocations and schema profiles submitted with an idempotency key.

## Retry behavior

A receipt binds a stable principal UUID and a protected idempotency-key digest
to the original request fingerprint, task UUID, kind and bounded outcome. It
contains no request body, raw key, credentials, result body or output bytes.
The independent repository key authenticates checkpoints and derives receipt
digests in separate domains. Changing that key does not create a fresh namespace:
an activated deployment rejects a mismatched or missing key.

New keyed tasks remain unclaimable until their exact admission generation has
been independently protected. A duplicate consumes no new task capacity,
credential approval or external effect. Matching retries return the existing
task while its details are available. A changed request conflicts during the
receipt window, even when dependencies have since changed or disappeared.

After details are removed, an authorized matching retry returns HTTP 410 with
only the stable task UUID, kind, expiry and reason. The caller must still own the
receipt and pass the selected authorization backend's task-read check. The
response grants no old integer task location, output access or cancellation
rights. External policy uses the resource identity `receipt:<task UUID>`.

The deduplication duration is captured at admission and anchored once at the
first known terminal outcome. Retries and restore do not extend protected terminal
deadlines; active work never
expires merely because time has passed. Terminal anchors are independently
protected before receipt retirement advances the monotonic expiry floor. Only
then can a reused key create another generation. An old admission ticket cannot
activate that generation.

## Upgrade and activation

Stop older API, worker, restore and administrator binaries. Apply migration
`2026-10-29-000006_task_retention`, reconcile database grants, and deploy matching
binaries with the same independent control repository and authentication key.
Keep these outside supported database rollback, following
[deletion recovery](deletion_recovery.md). The migration leaves receipt protection
disabled until explicit activation; existing behavior remains available during
this initial state.

Every command below requires `--control-recovery-directory` and
`--control-recovery-key`, or their `HUBUUM_CONTROL_RECOVERY_DIRECTORY` and
`HUBUUM_CONTROL_RECOVERY_KEY` environment variables. Initialize a new repository
using the existing control-checkpoint bootstrap procedure before activation.

```bash
hubuum-admin --task-retention status
hubuum-admin --task-retention preview
hubuum-admin --task-retention enable --task-retention-confirmation '<preview confirmation>'
```

Review the preview before enabling: the first finite legacy deduplication window
uses original terminal timestamps, so an old terminal receipt can become eligible
for retirement immediately. The preview reports a bounded sample, not an exact
whole-database count. Its confirmation binds the current authority and policy.
Repeat `enable` while its report says `more: true`; each invocation handles a
bounded backfill batch and protects its progress. New keyed submissions are
unavailable during backfill. The final transition to protected operation requires
all legacy candidates and pending acknowledgements to be resolved and the new
activation checkpoint to be durably read back.

Backfill preserves original identity and clocks. It never reconstructs an
unprovable original request from a redacted or expanded payload. Those legacy
requests return a conflict rather than executing again during their window.
Existing tasks are grandfathered out of detail expiry; activation does not
silently apply a new deletion deadline to old details.

The default deduplication window is seven days after terminal completion; detail
expiry is disabled. Policy changes affect subsequent admissions. For example:

```bash
hubuum-admin --task-retention set-policy \
  --task-retention-deduplication-seconds 604800 \
  --task-retention-details-seconds 86400
hubuum-admin --task-retention run
```

Omitting `--task-retention-details-seconds` in `set-policy` disables detail expiry
for subsequent admissions. The `run` command performs one bounded maintenance
pass; normal workers continue eligible work automatically.

## Holds, outputs and bounded cleanup

Use stable UUIDs when retrying hold commands:

```bash
hubuum-admin --task-retention hold --task-hold-id '<hold UUID>' \
  --task-hold-task '<task UUID>' --task-hold-reason legal
hubuum-admin --task-retention release-hold --task-hold-id '<hold UUID>'
```

Holds can have a fixed `--task-hold-expires-at` timestamp; otherwise they remain
until explicitly released. Released hold decisions remain in protected authority.
Publishing their release is required before a previously protected purge may use
that release. A new hold still blocks deletion immediately.

Cleanup protects an immutable manifest before deleting its selected scope and
protects completion before acknowledging removal. One manifest contains at most
64 distinct task UUIDs. Each pass has an aggregate budget of 1,000 persisted rows
and 16 MiB of measured row input, including owned children. Oversized indivisible
rows block explicitly. Held or blocked targets do not prevent progress on other
eligible targets.

Detail cleanup preserves available outputs, repair reports without an expiry,
active dependencies and required provenance. Expired export, backup and profile
outputs use a separate manifest scope, so their cleanup does not depend on detail
expiry. Global audit records retain captured stable task identities even after a
task row is removed. Scoped task reads disappear when their purge intent becomes
effective. Reads already started under an earlier database snapshot may finish;
retention cannot retract bytes already delivered.

Receipt authority has independent limits of 16,384 records and 12 MiB. Admission
reserves each record's worst-case terminal growth, so the byte limit can be reached
before the count limit. Each receipt must fit 1,024 encoded bytes. Retention
authority has a separate 4 MiB limit, at most 1,024 manifests, 64 policy revisions
and 1,024 retained hold decisions. These are safety limits, not promises that all
maximum populations fit simultaneously. Capacity exhaustion refuses new protected
work instead of discarding applicable obligations.

## Recovery and retained evidence

The signed checkpoint contains independent deletion and task-control revisions.
Ordinary admission, terminal settlement and retention advance task authority
without invalidating unrelated application's deletion fences. A checkpoint keeps
the latest complete state; it is not an unbounded operation log.

After physical recovery, `--recover-control` restores compatible authority while
leaving the application quarantined. `--reapply-control` settles old nonterminal
tasks and orphaned unresolved receipts as recovery-unknown, establishes their
fixed settlement anchor, reapplies retained output/detail purges, independently
protects the exact completion and only then releases the recovered incarnation.
Logical restore preserves authority and follows the same reapplication rules.
A prior removal receipt is never proof that an older restored copy is absent.

Backups predating stable task and principal identities can synthesize new UUIDs
on restore. Such derived identities cannot prove that an old purge target is
absent or that an old receipt belongs to a different person. With retained purge
decisions or receipt ownership, unresolved correspondence keeps recovery
quarantined. Use a backup carrying matching stable identity evidence; removing
the independent evidence would discard the promised recovery protection.

The repository retains the current checkpoint and its predecessor. It collects
only authenticated superseded files after durable latest-reference readback.
Unknown, conflicting and unrelated partial files consume capacity and are not
silently removed. Publication may complete an interrupted candidate only when
its existing contents are the exact prefix of the expected unpublished bytes.
Repository entry and byte caps include staged files and temporary references.

Checkpoint metrics use bounded operation labels and include actual encoded and
verified work, file reads/writes, synchronization, collection and lock contention.
Set `HUBUUM_CONTROL_CHECKPOINT_MEASURE=true` to include a cumulative
`checkpoint_measurement` object in administrator retention reports. It measures
this administrator process only and changes no lifecycle behavior; aggregate
separate invocations explicitly when measuring a multi-command operation.

Purge decisions and hold releases currently remain for the full lineage. This
does not erase other backups, WAL, artifacts outside the selected scope or prior
downloads. Receipts keep sensitive matching fields in explicit bounded records
for a later authorized targeted-erasure operation; retention alone does not claim
that separate erasure capability.
