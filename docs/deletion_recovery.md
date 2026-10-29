# Deletion authority and independent checkpoint foundation

Protected control records, independent checkpoints, runtime fencing, and restore
reapplication implement [#511](https://github.com/hubuum/hubuum/issues/511).
The enabled destructive scope is one exact closed object-history copy. General
resource erasure and discovery remain separate consumers of this foundation.
Unknown copy obligations, ambiguous identities and unsupported selectors remain
quarantined; they are never interpreted as proof of absence.

## Authority and ownership

`DeletionControlState` contains a deployment lineage, recovery epoch, monotonic
revision, and exact UUID selectors. Resource selectors include their family;
version selectors use an immutable version identity. Integer IDs, names, erased
payloads, request bodies, and free-form diagnostics are absent. Retrying an intent
with the same identity and scope preserves its generation; changed scope conflicts.

Each intent retains explicit physical-copy identities. Copy outcomes distinguish
pending, blocked, and removed. A blocked copy remains an obligation. All copies
must be removed before physical completion is representable; that state alone
does not acknowledge completion to a caller. Completion requires a new independent
checkpoint covering the recorded outcomes.

PostgreSQL stores the bounded material in `hubuum_control.deletion_authority`,
without foreign keys into application tables. Runtime roles can read minimal
authority through an allowlisted function, but have no direct control-table
access or mutation authority.
Privileged transitions compare the entire expected predecessor in one transaction.
The complete memory adapter implements the same state transitions and preserves
control state across application replay. Common storage tracing and metrics cover
both adapters.

The initial ceiling is 1,024 retained intents, 64 copies per intent, and 4 MiB of
encoded control material. Reaching a ceiling rejects new work. There is no receipt
expiry, compaction, or eviction in this slice: the supported recovery horizon is
the entire retained lineage. A future retention policy must account for every
restorable copy before it can shorten that horizon.

## Independent repository contract

The first repository is an administrator-owned Unix directory on durable storage
outside the primary database's supported rollback boundary. It must remain intact
when PostgreSQL data, WAL, and the database's own control ledger are restored.
Putting it in the same restorable database volume does not meet this contract.
The administrator must also retain the signing key independently. No always-on
ledger service is required.

The trusted writer has filesystem ownership and privileged database access. The
directory must have mode 0700; key, lock, checkpoint, and latest-reference files
must be ordinary owner-only files. Symlink files and multiply linked files are
rejected. The signing key contains exactly 32 random bytes. Files contain only
minimal authority and PostgreSQL cluster/WAL coordinates; the key is never written
to a checkpoint or report.

All writers and readers use a nonblocking advisory lock. A busy repository fails
closed and can be retried. Only filesystems that provide local Unix locking,
atomic rename, and durable file/directory synchronization are supported. This
slice does not claim NFS, distributed-filesystem, or multi-host writer support.
Repository ownership, the host administrator, and the filesystem are trusted.

Publication proceeds in this order:

1. Capture committed control state and adapter recovery coordinates.
2. Authenticate the latest reference and its referenced recovery material.
3. Reject another lineage/epoch, an older revision, missing decisions, or
   conflicting content at an existing revision.
4. Write a content-addressed checkpoint with HMAC-SHA-256 authentication, sync
   the file and directory, and independently read it back.
5. Write and sync a signed `HEAD` reference, atomically replace `HEAD`, sync the
   directory, and reread the published checkpoint before returning success.

The signed reference binds lineage, epoch, revision, and the SHA-256 identity of
the complete authenticated recovery material. That material includes exact
intent generations, copy outcomes, and PostgreSQL system identifier, WAL filename
(including timeline), and insertion LSN. These coordinates identify the captured
physical context; an LSN alone is not proof of recovered control coverage, and
positions from different timelines are not compared numerically.

A retry after lost acknowledgement can return the identical published checkpoint.
A crash before latest-reference replacement leaves unacknowledged staged material;
it cannot grant cleanup authority. Missing or corrupt staged bytes fail closed.
An uncertain rename/synchronization result requires rereading the latest durable
evidence before any acknowledgement or cleanup. The managed-copy command below
retains this proof through cleanup and completion protection.

The repository itself must not be rolled back. HMAC authentication detects
modification, not replay of an older valid copy of the entire independent
repository. A missing, unavailable, or uncertain latest reference is an operator
recovery problem, never permission to initialize a fresh empty ledger.

## Administrator commands

Apply the migration with `hubuum-admin --migrate` and reconcile split-role grants.
Checkpoint commands use the privileged migration connection when configured.
The database credential must be able to read the protected control table and
execute `pg_control_system()`; grant that PostgreSQL function only to the trusted
checkpoint operator where required. Ordinary runtime credentials must not receive
these permissions.

Provision the directory and signing key through the deployment's normal durable
storage and secret-management procedures. Initialize exactly once, before any
deletion intent exists:

```bash
hubuum-admin --control-checkpoint /independent/control \
  --control-checkpoint-key /independent-secrets/control.key \
  --initialize-control-checkpoint
```

Subsequent publication omits the initialization flag:

```bash
hubuum-admin --control-checkpoint /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

After physical recovery, keep API and worker processes stopped and the deployment
quarantined. Check control coverage against the independently retained latest
reference:

```bash
hubuum-admin --verify-control-recovery /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

The JSON report includes `authority_covered` and always reports
`application_quarantine_released: false`. Successful coverage is necessary but
does not reapply restrictions, authorize cleanup, or release traffic. A stale or
wrong-lineage database returns an error. Preserve the database and independent
repository and recover the missing control material before retrying. Use the
offline recovery command below when the authenticated checkpoint is a compatible
successor. Conflicting lineages/epochs or divergent decisions require
operator recovery of the correct deployment; they cannot be reset or merged.

Initialization requires empty control authority and an empty directory. It cannot
repair a missing `HEAD` in an existing repository. An interrupted first bootstrap
requires an operator to establish that no checkpoint was acknowledged before
reprovisioning; do not delete files to bypass an uncertain recovery history.

## Offline control recovery and runtime gates

Apply the recovery-gate migration and reconcile split-role grants with all API
and worker replicas stopped. Configure both settings on every runtime replica,
keeping configuration outside the primary database rollback boundary:

```bash
HUBUUM_CONTROL_RECOVERY_DIRECTORY=/independent/control
HUBUUM_CONTROL_RECOVERY_KEY=/independent-secrets/control.key
```

The equivalent flags are `--control-recovery-directory` and
`--control-recovery-key`. The trusted runtime and administrator must share the
Unix owner account for this repository; the existing owner-only files remain
required. Treat that account and host as trusted checkpoint writers because
HMAC verification uses the signing key. Paths and key contents are omitted from
administrator configuration; `control_recovery_required` reports activation.

Every runtime role checks authority before it binds an HTTP listener, initializes
application workers, or joins the restore coordinator. Missing/unavailable latest
evidence, invalid authentication, a different lineage/epoch, or a rolled-back
primary prevents startup. Omitting both settings is supported only for deployments
without physical-recovery guarantees; configuration is an operator trust boundary
and must not be removed to bypass a failed gate. Retained local intents without independently protected reapplication block
startup even without these settings.

With all replicas stopped, recover missing control material:

```bash
hubuum-admin --recover-control /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

Recovery holds the repository publication lock from authenticated latest lookup
through the primary transaction and independent read-back. An exact-predecessor
comparison rejects competing primary changes. A compatible newer local ledger is
preserved. Different lineage/epoch, conflicting decisions, and same-revision
conflicts fail without replacing authority. Recovery neither adopts another
lineage nor resets the epoch. Physical WAL coordinates remain evidence of the
captured context; recovery does not compare LSNs across timelines.

A crash before commit leaves the primary unchanged. A crash after commit and
before acknowledgement is retried idempotently. The OS releases the publication
lock when the process dies; the next attempt authenticates the latest evidence
again. Do not copy an arbitrary older checkpoint into `HEAD` to make recovery pass.

Importing authority creates a new local application incarnation and quarantines
it, even when the recovered authority already matched the checkpoint. It does
not import the checkpoint's old exposure permission. Maintenance/readiness reports
`recovery_quarantine`, ordinary application requests return 503, and new productive
work is denied. Health and metrics remain available.

After recovery, reapply every retained decision and protect the resulting receipt:

```bash
hubuum-admin --reapply-control /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

The command holds the independent publication session while it protects all
pending intents, reapplies known bindings transactionally, publishes and reads
back the exact completion receipt, then releases the matching application
incarnation and authority revision. A failure leaves quarantine in place. Retry
uses the same decisions and never considers an old `Removed` outcome sufficient
proof that a recovered copy is absent. A matching live object version, unresolved
provenance, open history, or unsupported obligation blocks release.

The completion receipt contains only the application incarnation and control
revision. It is covered by checkpoint authentication. The local release marker
and its checkpoint identity are excluded from exported control material, so
restoring that material cannot manufacture permission to expose another image.
Optional receipt fields preserve canonical authentication of older checkpoints.

Physical rollback and control recovery require all producers stopped. Live
database replacement remains unsupported. Logical application restore holds
maintenance through replacement and reapplication as described below.

### Productive task transactions and request draining

Each admitted unit of application work carries a registered owner, application
incarnation and deletion-authority revision. Nested storage calls inherit that
permit; they cannot silently capture a newer revision. PostgreSQL takes an
authority shared lock before domain locks and holds it through the transaction.
Intent/recovery transitions take the conflicting write lock first. The memory
adapter validates the same facts while holding its state lock.

PostgreSQL also holds a key-share lock on the admitted runtime's registration.
Retiring that owner waits for its own transactions, while unrelated runtimes can
start and stop during a long backup. Registration serializes only its bounded
capacity check. Retired owners remain invalid even in older repeatable-read
snapshots; retirement cannot make previously admitted work valid again.

Task claims persist their original fence. Reopening the application does not
make an old claim eligible to publish. Cancellation, failure settlement, lease
renewal and control coordination remain available under quarantine, with their
own ownership checks. Snapshot reads hold the same barrier; PostgreSQL repeatable
read rejects an authority row changed while the snapshot waited for its lock.

An intent closes admission before cleanup. Each registered runtime acknowledges
the exact pending fence only after its ordinary work settles. HTTP bodies remain
counted through actual completion; archive/profile blocking jobs and template
subprocesses retain activity until they finish. Abandoned work keeps a conservative
uncertainty latch. Heartbeat expiry, a dropped future, or a returned streaming
response is insufficient proof. Health/readiness/metrics polling is control work
and does not prevent ordinary work from draining.

These fences cannot retract bytes already delivered or cancel a remote action
already received. The deletion guarantee describes the explicitly managed copy.

## Offline managed-copy cleanup

The first executor removes one **closed object-history row** selected by its
stable resource and version UUIDs. It does not erase live objects, open history,
backups, PostgreSQL MVCC/WAL, artifacts, or bytes already delivered to consumers.
Completion describes this one managed hot copy, not erasure of every possible
copy of the data. Existing opaque copy obligations cannot be guessed into an
executable target.

Before upgrading, stop every older API and worker process. Apply the managed
cleanup migration and reconcile split-role grants, then start only matching
binaries. Its UUID index rejects malformed or ambiguous historical version
identities; repair their provenance before retrying. Older binaries do not
participate in the new runtime registration fence and must never remain active
when cleanup is enabled.

Each runtime registers before bootstrap writes, background work or listeners.
These bounded protected registrations survive logical restore and are separate
from restore-coordinator heartbeats. A successful shutdown removes its own
registration only after HTTP, workers and template processing have settled.
Cancelled handlers, incomplete responses, worker panic/timeout, lost shutdown
acknowledgement and process death (including abrupt signals such as `SIGQUIT`)
can leave uncertainty; their registrations stay
blocking. Heartbeat expiry does not prove that a disconnected writer stopped.
There is no force-clear command. Configured runtimes hold a shared lock on the
repository's stable `RUNTIME_LOCK` file for their entire lifetime. The protected
registration records its random nonce and local device/inode identity. Offline
recovery requires an exclusive lock on that same file, then drains old SQL under
the authority write lock before revoking owners. Revoked owners cannot capture a
new permit. Unknown or mismatched domains block recovery; age is never proof.

The lock file must never be replaced, unlinked or restored from a copied
repository to clear owners. A copied repository has a different file identity.
This protocol supports the trusted single-host Unix repository boundary above,
not multi-host file locking. An uncertain shutdown retains its shared descriptor
until process exit. Older registrations without a matching isolation domain
require the documented offline upgrade boundary; they are not force-retired.

Provision and initialize the independent repository as described above, then
stop all API and worker replicas cleanly. Generate an intent UUID once and reuse
it for every retry of the same target:

```bash
hubuum-admin \
  --delete-object-history-version VERSION_UUID \
  --history-resource RESOURCE_UUID \
  --deletion-intent INTENT_UUID \
  --control-repository /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

The command uses the privileged migration connection. It authenticates the latest
independent checkpoint and requires the primary to cover it. Prepare then locks
authority, validates the exact closed history copy and commits a minimal bound
intent. Active runtimes then drain and acknowledge that exact fence. New runtime registration now
fails under quarantine.

The owned repository session remains locked while the command publishes and
reads back the intent checkpoint. Only its private protection proof can reach
the cleanup service. The adapter locks authority again, compares the exact
protected state, checks that every runtime acknowledged the fence, and verifies
the UUID pair. Removing the row and recording its protecting checkpoint identity commit
together. No database transaction spans filesystem synchronization.

Final acknowledgement requires another independent checkpoint covering the
committed removal receipt. If that publication fails, the command reports pending
completion protection instead of success. Retry with the same intent identity;
it preserves the existing copy identity and receipt. Retries check physical
presence even when authority already says `Removed`, because recovery may have
reintroduced the same version. A recovered open or mismatched row remains blocked.
Every successful report keeps `application_quarantine_released: false`.

Bound intents use control format 2. Readers preserve the canonical bytes of
existing format 1 checkpoints and keep their unknown obligations unchanged.
Downgrade refuses format 2 authority or retained runtime registrations instead of
discarding them. Retain the repository and key for the entire supported recovery
horizon; no control eviction or receipt expiry is enabled.

## Logical restore and recovery horizon

Application backups exclude protected authority, runtime ownership and exposure
permission. Replacement locks authority before domain data, preserves retained
decisions, and rotates the application incarnation. Empty-authority replacement
can resume immediately. With retained obligations, maintenance stays draining
while the coordinator invokes protected reapplication. Only exact checkpointed
completion releases maintenance and application access. A post-commit failure
keeps the restored image quarantined for an administrator retry.

A history-free backup is not evidence that deleted versions are absent: current
object identities are checked too. Synthesized baseline versions retain their
original source version and all earlier restore sources in validated identity
references. Subsequent backups and restores preserve that ancestry. A live or
closed derived copy of a retained target blocks release; exact hot-copy cleanup
does not silently expand to erase a different version. At most 64 source versions
are retained per baseline. A restore that would exceed this limit fails rather
than discarding older evidence.

Older derived version-8 identities without source evidence cannot establish that
another version of the same resource is unrelated to a retained target. Such
ambiguity keeps the image quarantined, and later baseline synthesis preserves
the uncertainty. Known unrelated version-4 identities remain usable. Legacy or
unresolved identities likewise cannot prove that a protected selector is absent.
Reused integer IDs never select a copy; cleanup compares immutable resource and
version UUIDs.

The supported control horizon is the entire deployment lineage. No retained
intent or receipt expires or is evicted. Restorable copies remain obligations
until explicitly resolved. A backup without sufficient identity evidence, from
another lineage/epoch, or whose independently required control material is
missing stays quarantined. Reaching the bounded control ledger limit rejects new
deletion work; it never shortens the horizon to admit more work. Later lifecycle
consumers must provide an explicit bounded retirement policy before changing it.

## Upgrade and rollback

Stop every older API, worker, restore executor and administrator producer before
applying `2026-10-29-000006_application_reapplication`. Apply runtime grants and
start only matching binaries. The migration must not discard retained owners.
Configure the same independent repository and key on every producer before
activating recovery guarantees. Do not roll back to an older executable that
ignores application permits or the completion receipt.

After physical recovery, run `--recover-control`, then `--reapply-control`, then
verify matching runtime startup before admitting traffic. Missing or mismatched
process-isolation evidence requires restoring the original trusted repository
and stopping its processes; it cannot be bypassed by deleting registrations.
Retain the original repository, signing key and supported backups throughout the
recovery horizon.

## Machine-readable operational status

The offline administrator interface supplies the recovery-status contract for
this milestone of [#545](https://github.com/hubuum/hubuum/issues/545). Run it with
the trusted host account, administrative database credentials, and access to the
owner-only independent repository and key:

```bash
hubuum-admin --control-status /independent/control \
  --control-checkpoint-key /independent-secrets/control.key
```

This read-only command works while application startup is quarantined. It does
not expose an HTTP route that bypasses application authentication or recovery
fencing. Its JSON status is independent of task rows and task/result expiry.
Intent identifiers remain inspectable for the deployment's indefinite control
retention horizon. An unavailable primary produces an error, never an empty list
claiming there are no retained decisions.

The format-1 response contains these fields:

| Field | Meaning |
| --- | --- |
| `control_revision`, `checkpoint_revision` | Observed primary revision and authenticated latest checkpoint revision; the latter is null when evidence cannot be authenticated. |
| `recovery_protection` | `pending` means current authority needs publication; `protected` means independent evidence covers it; `blocked` means latest evidence is unavailable or primary recovery is required. |
| `block_reason` | Null, `evidence_unavailable`, `lineage_or_epoch_mismatch`, or `recovery_required`; no raw repository error or path is returned. |
| `access_restricted` | True unless the current application is released against matching independently protected reapplication. |
| `next_action` | Bounded operator action; consumers must display unknown future actions conservatively rather than infer permission. |
| `intents` | At most 1,024 identity/generation summaries with protection state, copy counters, completion and next action. This is the primary observation; when protection is blocked it cannot establish complete recovered scope. |

Each intent reports `pending_copies`, `blocked_copies`,
`recorded_removed_copies`, and `unbound_copies`. Blocked and unbound copies remain
obligations. These counters do not expose subject identities, provider locations,
copy lists, authentication material or erased payloads.
`confirmed_managed_scope_completion` becomes true only when every copy has an
implemented binding and recorded removal, the outcome is independently protected,
and the current application has matching protected reapplication and release.
Physical recovery resets this confirmation until fresh reapplication. A recorded
removal alone does not prove that an older restore has not reintroduced bytes.
No response establishes erasure of unmanaged or undiscovered copies.

The [shared JSON fixture](https://github.com/hubuum/hubuum/blob/main/tests/fixtures/lifecycle/control_status.json) is
checked against the server's serialized observation and is available for CLI and
other consumer contract tests. For example, a primary intent committed ahead of
its checkpoint reports:

```json
{
  "format": 1,
  "control_revision": 1,
  "checkpoint_revision": 0,
  "recovery_protection": "pending",
  "block_reason": null,
  "access_restricted": true,
  "next_action": "publish_checkpoint",
  "intents": [{
    "intent": "bcdf80ed-4ba2-435a-ab11-db89ead89a33",
    "generation": 1,
    "recovery_protection": "pending",
    "pending_copies": 1,
    "blocked_copies": 0,
    "recorded_removed_copies": 0,
    "unbound_copies": 0,
    "confirmed_managed_scope_completion": false,
    "next_action": "publish_checkpoint"
  }]
}
```

`restore_independent_evidence` requires repairing or restoring trusted latest
evidence, not retrying cleanup without it. `recover_control` requires the offline
recovery procedure. `resolve_control_lineage` requires investigating the primary
and repository lineage/epoch and restoring the correct trusted pair; importing
another deployment's authority is prohibited. `publish_checkpoint` requires checkpoint publication and
readback. `resolve_copy_obligations` requires operator investigation of blocked or
unsupported copies. `retry_managed_cleanup` means repeat the exact managed-copy
command with the same intent UUID. `reapply_control` requires protected
reapplication; `none` means this observation requests no additional action.

Exit success means the status observation was encoded successfully, including a
`blocked` observation. It is not a recovery verification or cleanup
acknowledgement. Use `--verify-control-recovery` for strict verification and the
documented cleanup/reapplication commands for mutations. Status polling reserves
no execution capacity and grants no authority to delete or reopen access.
