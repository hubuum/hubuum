# Verified task artifacts

Exports and backups use bounded producers, PostgreSQL chunks, independent
read-back, and atomic task publication. This implements
[#508](https://github.com/hubuum/hubuum/issues/508) using the identities and
lifecycle from [#503](https://github.com/hubuum/hubuum/issues/503).
The [resource contracts](performance/lifecycle_budgets.md) distinguish acceptance
profiles from production configuration.

## Ownership and publication

The storage SDK's `artifacts` module owns the backend-neutral contract. Every
selectable `StorageBackend` implements `TaskArtifactStore`; application code uses
its configured opaque handle. Common tracing and metrics surround adapter calls.
`hubuum-artifacts` owns the synchronous-producer/async-destination bridge.

Artifact, attempt, destination and object-version UUIDs are distinct validated
types. The manifest binds all four. The PostgreSQL destination is named
`primary-artifacts`; changing credentials does not change its identity.
Destination names and IDs cannot be rebound. There is no external provider or
historical archive query dependency.

Task ownership records bind an attempt to the stable task identity and exact
lease. They deliberately have no foreign key to replaceable task rows. Deleting
a task, restoring an older database snapshot, or reusing its integer ID cannot
remove physical cleanup obligations or transfer ownership to a different task.

| State | Allowed work | Recovery |
| --- | --- | --- |
| Staging | Append the next bounded block; retry identical bytes at an existing ordinal. | Abort on producer failure; lost workers fail without producer replay. |
| Sealed | Read immutable blocks and the expected manifest for independent verification. | A replica with the live claim can reload the manifest and verify without a local spool. |
| Published | Read through the catalog reference. | Task output and terminal success commit in the same fenced transaction. |
| Deleting | Remove at most 16 blocks per cleanup transaction. | Repeat after interruption; retain reservations until the final physical block and manifest are removed. |
| Discarded | Retain a small tombstone without content. | Delayed retries cannot recreate the cancelled attempt or reuse its object version. |

A staging attempt has a fixed one-hour lifetime. Retries do not extend it.
Publication requires a private-field verification proof and rechecks identity,
manifest, state, live lease and cancellation in the task transaction. A failure
in task finalization rolls back both publication and output metadata. Byte-store
publication alone is forbidden for task-owned attempts.

## Producer bounds

Exports fetch small pages, apply authorization and includes, hydrate one page,
and append rows to an indexed temporary file. The row index is also on disk and
charged to the spool limit. JSON serialization replays one row at a time. Text
templates use the existing isolated renderer with lazy indexed rows and bounded
file output. The input files remain pinned until the renderer is reaped.
Related-object templates expose an aggregate graph and have a separate finite
8 MiB source bound; every final hydrated row must still fit 4 MiB.

Backups reuse `StorageBackupReplayBuilder`. PostgreSQL captures rows through
bounded cursors in one consistent snapshot and checks each encoded source row
against 4 MiB. Serialization reads that exact snapshot row by row. It does not
materialize a complete backup document before writing the output.

`stage_producer` transfers one awaited block at a time through a one-block channel.
The bridge retains at most five data blocks, the bounded manifest, and producer
input state. Slow storage applies backpressure to serialization. A serializer
error, panic, or exhausted output limit poisons the write; ignoring a sink error
cannot publish a shorter successful prefix. Upload is followed by full independent
read-back. Provider metadata and echoed checksums are never verification evidence.

Spools are disposable. After process/host loss, ordinary export and backup tasks
fail when their lease expires; the worker does not replay arbitrary producer
side effects or claim to continue an old source snapshot. Staged bytes are then
reconciled independently. Explicitly resubmitting creates a new task and snapshot.
Template files are anonymous on Unix and use OS delete-on-close on Windows.
The renderer inherits only its three spool descriptors on Unix and enforces its
own five-second lifetime even if its parent dies. Kernel file ownership reclaims
local bytes after abrupt exit; durable destination cleanup remains independent.
Abandoned attempts retain their full reservation for at least ten seconds after cleanup
fences them, allowing an orphaned renderer to exit before any capacity is released.
Cancellation waits for active producer/render work to stop before acknowledging
the task. Dropping a blocked bridge disconnects its channel; a callback performing
arbitrary computation must still return cooperatively. Durable expiry remains
the backstop for abandoned writes.

## Integrity and downloads

The version-1 canonical binary manifest contains a five-byte magic/version,
four 16-byte identities, four-byte block size, eight-byte total encoded length,
32-byte full-stream SHA-256, and four-byte block count. Each ordered entry contains
its four-byte length and 32-byte SHA-256. Integers are little endian. Ordinal and
offset follow from position and block size; only the final block may be short.
An empty artifact has no blocks and the SHA-256 of an empty stream.

The catalog retains the manifest digest independently of destination bytes.
Readers validate the bounded manifest, identity, length and ordered blocks before
release. They hold the last block until full-stream SHA-256 and exact EOF pass.
Truncation, extra blocks, reordered/substituted content and manifest corruption
cannot finish successfully. A failed or panicking reader cannot become a clean
EOF. Range reads are outside this contract.

Existing output authorization remains at the download boundary. Backups recheck
administrator authorization before publication, and exports recheck template
access. New downloads stream through a bounded channel and cancel further reads
when the consumer disconnects.

**Legacy exception:** outputs stored inline by older binaries remain readable
through their existing JSON/text/byte readers. Those readers materialize the old
stored value and retain the old creation-time finite output bound; the new
streaming memory guarantee applies to newly produced artifact-backed outputs.
Lowering current write settings does not make previously stored output unreadable.

## Admission and production settings

| Setting | Default | Purpose |
| --- | --- | --- |
| `HUBUUM_ARTIFACT_MAX_SPOOL_BYTES` | 256 MiB | Maximum export row/index spool. |
| `HUBUUM_ARTIFACT_WORKER_RESIDENT_BYTES` | 4 GiB | Per-process reservation ceiling for concurrent staging and local spools. |
| `HUBUUM_EXPORT_MAX_OUTPUT_BYTES` | 256 KiB | Admission ceiling for a new export; requests may choose a smaller limit. |
| `HUBUUM_BACKUP_MAX_OUTPUT_BYTES` | 256 MiB | Backup capture/serialization ceiling. |
| `HUBUUM_EXPORT_STAGE_TIMEOUT_MS` | 10 seconds | Query/hydration and render/transfer stage deadline; tune for large, slow output. |

Reservations precede producer capture. An export reserves its output ceiling plus
`max(spool ceiling, output ceiling)`; a backup reserves twice its output ceiling.
The manifest ceiling is reserved in addition to those payload/spool bytes.
This covers overlapping spool and destination bytes. Publication drops the local
reservation to the actual durable payload plus encoded manifest size. Cancellation, task purge and output
expiry retain the physical reservation until cleanup finishes. The worker runs
bounded cleanup independently of output metadata expiry scheduling. It drains
one batch per worker tick while batches remove data. After an empty batch, it
waits for `HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS` (300 seconds by default)
before checking again. Failures remain eligible for retry on the next tick.
Newly eligible or grace-fenced copies can therefore remain charged until the
next scheduled check; lower the interval when faster reclamation is required.

The authoritative deployment/principal/collection/kind limits from
[task admission](task_admission.md) also include these obligations. Configure them
consistently with artifact limits and concurrency. Admission may reject a new task
output when an applicable quota is exhausted; raising a per-output ceiling alone
does not grant more shared capacity. Published outputs cease occupying a worker's
staging budget but remain charged to shared capacity until reclaimed.

Write ceilings are configurable within a stable finite format envelope: 64 GiB
artifact, 1 MiB block, 65,536 blocks, 4 MiB manifest. The default transfer profile
is 1 GiB, 1,024 blocks and 256 KiB manifest. Readers use the retained format
envelope, independently of the current write policy. The 4 MiB row bound remains
fixed. Production tuning changes admission, not integrity or cancellation fences.
The isolated renderer retains its fixed 128 MiB heap and five-second execution
limits. A larger output allowance does not grant a template more computation.

## Backup, restore and upgrades

Backup format 12 adds portable export-artifact records: a bounded manifest header
followed by bounded base64 blocks. Restore validates ownership, ordering, all
hashes and exact EOF before replacing application state. It writes fresh physical
identities, independently reads the newly stored destination, and remaps output
references. Existing destination obligations survive logical restore and remain
charged until cleanup. Restore reserves space for the additional physical copy
against the deployment resident limit before writing its blocks; finish cleanup
or raise that shared limit if the copy cannot fit. Backups do not recursively
embed retained backup outputs.
Formats 6–11 remain readable; format 11 preserves its native logical identities.

Stop older API/worker processes, take a database backup, and run
`hubuum-admin --migrate` before starting matching binaries. This applies the new
artifact-output migration and reconciles grants. Storage SDK implementers must
follow the [0.9 upgrade guide](storage_adapter_sdk.md#upgrading-from-08-to-09).
Before restarting, reduce `HUBUUM_EXPORT_MAX_OUTPUT_BYTES` and
`HUBUUM_BACKUP_MAX_OUTPUT_BYTES` settings above `68719476736` (64 GiB); upgraded
processes reject them. Partition larger workloads and tune shared capacity
consistently. This format ceiling is not a measured resource qualification.
Do not downgrade while artifact-backed output or cleanup obligations remain:
expire/purge the output and finish cleanup first. The down migration refuses to
discard them. Artifacts above the older format ceiling also prevent downgrade.

## Verification

Protocol tests exercise empty/final-partial output, manifest and block corruption,
identity substitution, poisoned writers/readers, limits, cancellation and retries.
Cross-adapter contracts test atomic task publication, rollback, replica handoff,
worker loss and cancellation at staging/upload/seal/verification boundaries.
Portable restore tests check fresh identities and preserved cleanup obligations.

The explicit resource driver runs actual producers and streamed downloads in two
fresh worker processes with slow PostgreSQL writes/read-back and slow consumers:

```bash
source .env
python3 -I -S tests/python/run.py integration artifact-pipeline \
  --release --output /tmp/artifact-pipeline.jsonl
```

It creates and drops isolated databases using the test connection settings,
requires `CREATEDB`, and does not require `CREATEROLE`. It records every warm-up
and measured sample, RSS including renderer children, shared reservations, and
producer/upload/verification/publication/download timings. Resource results and
any revised acceptance profile must be recorded with the lifecycle budget ledger;
passing protocol tests alone is not resource acceptance.

## Filesystem byte-store adapter

`hubuum_artifacts::FilesystemArtifactStore` implements the immutable
`ArtifactStore` contract on local Unix filesystems. Configure it explicitly with
`FilesystemArtifactStore::open(root).await`, then register a stable
`Destination` before staging. The existing `ArtifactWriter`, `stage_producer`,
verifier and `ArtifactReader` use the adapter without a separate transfer
protocol. The implementation is internal and Unix-only; Windows builds retain
the existing PostgreSQL path.

This is a byte-store capability, **not a production task-output selector**.
Exports, backups, task completion, authorization, restore and protected task
retention continue using PostgreSQL's `TaskArtifactStore` and its atomic
primary-store publication. This adapter does not implement task binding, shared
admission reservations, deletion authority, task-result expiry or external
publication reconciliation. Switching production task outputs requires those
coordinated guarantees; configuring this root alone cannot switch them.

### Root and destination ownership

Pre-create an absolute root directory owned by the effective server user with
mode `0700`. Use the real path: root opening rejects symlinks in every component
and rejects parent traversal. Do not canonicalize untrusted paths to bypass
these checks. On macOS, for example, use `/private/tmp` rather than `/tmp`.
Keep ancestors under trusted administrative control, and keep the root private
from other services running under the same Unix identity. The adapter rejects
files with multiple hard links, non-regular files and group/world permissions;
files are created with mode `0600` and subdirectories with mode `0700`.
All runtime lookups use pinned directory descriptors, generated UUID keys and
`NOFOLLOW`, rather than user-controlled names or check-then-open paths.

Keep the root and its complete metadata together across process restarts. Each
root supports at most 1,024 immutable destination ID/name bindings. Provision a
new destination ID when changing physical roots; changing a default or credentials
must not rebind existing references. There is no automatic copying or migration
between roots, and no provider locator or filesystem path appears in a public
artifact reference. Adapter error projections omit filesystem paths.

### Durability and recovery

One advisory root lock serializes each operation across clones and processes.
All filesystem I/O runs on Tokio blocking workers. Cancelling an async call does
not cancel an already running filesystem operation; its durable outcome remains
inspectable and identical retries are safe. Independent read-back still creates
the verification proof; successful file writes alone do not permit publication.

Blocks, manifests and lifecycle metadata are written through an owner-only
pending file, synchronized, atomically renamed within the same directory, then
followed by directory synchronization. macOS additionally uses `F_FULLFSYNC`
for regular files. Synchronization errors prevent a successful acknowledgement;
an error after rename can leave a committed operation that an identical retry
reconciles. Filesystems must support directory synchronization, atomic rename
and process-wide advisory locking with the documented durability semantics.
Hardware and mount settings must honor synchronization; process-exit tests do
not establish power-loss behavior for every storage device.

Staging has a fixed one-hour wall-clock deadline; retries preserve it. Sealed
attempts can be verified and published after reopening the root. Abort and expiry
fence later publication before cleanup begins. Cleanup removes at most 16 blocks
per call and synchronizes removals before recording progress. It reconciles
interrupted pending files, block renames and deletions. Discarded attempts retain
small tombstones permanently so delayed retries cannot recreate them. Published
independent artifacts remain retained; this contract has no public operation for
deleting them or expiring task-owned results.

Cleanup streams the catalog with bounded memory, but scan time grows with the
number of retained attempts and tombstones. The root lock also serializes reads
and writes; partition independent workloads into distinct roots if contention
becomes significant. Do not remove tombstones to regain capacity while delayed
retries remain possible.

### Deployment limitations and verification

The supported deployment is a single owner on a local Unix filesystem. Multiple
processes on the same host may use the same physical root and Unix identity.
Cross-host replicas require the *same* files and proven cross-host locking,
rename, cache coherence and file/directory synchronization semantics; NFS, SMB,
FUSE and distributed filesystems are not qualified by local temporary-directory
tests. A replica with its own local path does not share artifacts. Do not enable
multi-host operation on the strength of this adapter's local tests.

The native filesystem suite exercises the shared writer, producer, verifier and
reader against temporary roots, including restart handoff, publication/abort
races, bounded cleanup, expiry, path and link attacks, permission failures,
corruption and injected synchronization failures. A child exits without running
destructors after a block rename while holding the root lock; a fresh instance
recovers the append and completes independent verification. Run it with:

```bash
cargo test -p hubuum-artifacts --locked
```

For an adapter-only PR, keep drafts free of broad CI labels. `ci:validate` requests
early full correctness and production/container checks without forcing all
benchmarks; `ci:full` also forces every performance scope. Narrow
`ci:bench:scope:*` or `ci:bench:tag:*` labels select an investigation, with effort
set by `ci:bench:profile:*`; a profile alone does not start draft benchmarks.
Remove temporary selectors after recording their tested source and results.
Ready-for-review triggers normal full affected-path validation.

The adapter requires filesystem durability/security/recovery tests, the full
repository runner, Clippy, formatting, documentation lint and CI-classification
checks. Manifest/lockfile changes also require workspace-manifest parity and a
real both-TLS production image. Existing conservative benchmark selection may
expand for these shared build inputs; retain that final validation. Scheduler
P3/P4 (`ci:scheduler`), task-retention P6, large-scale lifecycle runs, S3/cold-provider
qualification and unrelated schema/template resource investigations are not
acceptance evidence for this local byte-store change. Production producer RSS
and end-to-end filesystem task-output performance must be qualified when that
routing is implemented; local protocol tests make no throughput or RSS claim.

This internal adapter introduces no database migration, supported public SDK
change, server configuration flag or deployment upgrade action. It does not
change the stable release's supported storage choices.
