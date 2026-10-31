# Deletion authority and independent checkpoint foundation

This implements protected control records, independent checkpoint publication,
offline control recovery, and runtime recovery quarantine for
[#511](https://github.com/hubuum/hubuum/issues/511). It does **not** enable permanent
deletion or selective restriction reapplication. Retained decisions restrict the
entire deployment, including completed deletions, so recovering authority cannot
expose data that an older application backup may contain. The issue remains open.

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
evidence before any acknowledgement or cleanup. No cleanup executor is enabled
by this foundation.

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
and must not be removed to bypass a failed gate. Retained local intents block
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

Importing authority reapplies the conservative **deployment-wide restriction**:
maintenance/readiness reports `recovery_quarantine`, application requests return
503 (including restore-status requests), and task/event claims and retention work
remain stopped. Health and metrics probes remain available on already-running
processes. Startup rejects retained intents; recovery reports always keep
`application_quarantine_released: false`. Selective reapplication is still required
before a deployment with retained intents can resume useful application traffic.

Physical rollback and control recovery are offline operations. These checks do
not fence in-flight operations across a live database replacement, and they do not
authorize online deletion producers. Keep such producers disabled until transactional
restriction and execution fencing are implemented. Logical application restore
continues to reject retained intents before replacing any data.

## Logical restore gate and remaining activation work

Application backups exclude the control table. Restore checks protected authority
inside the replacement transaction and holds its row against concurrent changes.
If any retained intent exists, replacement fails before application data changes.
This conservative gate includes completed decisions because an older backup may
contain their erased data. The memory adapter applies the same gate.

Before #511 can be completed or any deletion consumer activated, implement:

- restriction/publication fencing when each intent commits, with proof types
  carried into cleanup and acknowledgement operations;
- independently protected intent and completion acknowledgements around real
  physical-copy operations;
- explicit recovery-epoch transitions and selective exposure release after
  restriction reapplication;
- restriction reapplication after logical restore, replacing the conservative
  rejection only when every affected exposure is fenced; and
- process-crash, physical-rollback, and copy-retention-horizon integration tests
  covering those enabled execution paths.

Do not infer freshness from the recovered primary database, match a selector by
reused integer ID, or treat an old authenticated checkpoint as the latest one.
