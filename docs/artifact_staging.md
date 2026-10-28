# Artifact staging foundation

This is the first implementation slice of
[#508](https://github.com/hubuum/hubuum/issues/508), following the immutable
artifact contract in [#503](https://github.com/hubuum/hubuum/issues/503).
It provides a bounded transfer protocol and PostgreSQL staging. Application
output offloading is **not enabled**. Existing export and backup outputs keep
their current representation and readers. There is no HTTP, backup-format,
configuration, or supported storage-SDK change in this slice.

## Ownership and compatibility

`hubuum-artifacts` is an unpublished workspace-internal crate. Its byte-store
contract is separate from the complete hot `StorageBackend`. The PostgreSQL
adapter implements it with its existing runtime and adapter error conversion;
application callers do not acquire or select a PostgreSQL pool.

Artifact, write-attempt, destination, and object-version IDs are distinct
private-field types. A manifest binds all four. They are independent of
resource/principal/task identities supplied by #505. The artifact tables have
no foreign keys into replaceable application tables. Aborting or losing a
task cannot cascade away a physical cleanup obligation.

Destination registration is idempotent for an identical ID/name pair. Neither
can be rebound, and the database rejects removal of a referenced destination.
This slice has one adapter; configured default routing, credential rotation,
and external provider composition belong to later integrations.

## Lifecycle

| State | Allowed work | Recovery |
| --- | --- | --- |
| Staging | Append the next bounded block; retry identical bytes at an existing ordinal. | A failed producer explicitly aborts, or the one-hour staging deadline expires. |
| Sealed | Read immutable blocks and the expected manifest for independent verification. | Another replica reloads the bounded manifest and repeats full read-back; no local spool is required. |
| Published | Read verified blocks through the published catalog reference. | An identical publication is idempotent. Staging expiry no longer applies. |
| Deleting | Remove at most 16 physical blocks per cleanup transaction. | Repeat cleanup until all physical bytes and the manifest are gone. |
| Discarded | Retain a small attempt tombstone with no content or manifest. | A delayed stage retry cannot recreate the cancelled attempt or reuse its object version. |

The deadline is fixed when an attempt starts; retries do not extend it.
Cancellation and expiry fence publication through a locked attempt row.
Publication requires a private-field verification proof and rechecks persisted
identity, state, deadline, and manifest under the same lock. PostgreSQL triggers
also reject post-seal chunk writes, identity changes, manifest replacement,
invalid transitions, and removal of an obligation before its chunks are gone.
Attempt tombstones are retained in this foundation; a future reclamation policy
must prove that old owners cannot return before removing their cancellation
fences. Cleanup does not silently turn a cancelled identity into a new write.

One awaited block write provides producer backpressure. No transaction spans
the complete upload or verification pass. An interrupted write poisons the
local writer; ignoring an error cannot publish a shorter prefix. A verified
sealed attempt is independently replayable, but this does not permit replaying
producer side effects. Unsealed attempts are discarded on abort/expiry in
this initial integration.

## Encoded bytes and verification

The version-1 manifest is a canonical binary record: five-byte magic/version,
four 16-byte identities, a four-byte block size, an eight-byte total length,
32-byte full-stream SHA-256, and a four-byte block count. Each ordered entry
contains its four-byte exact length and 32-byte SHA-256. Integers use little
endian encoding. Ordinal and offset follow from entry order and block size;
only the final block may be short. An empty artifact has no blocks and the
SHA-256 of an empty stream. Readers reject trailing manifest bytes.

The manifest digest is retained in trusted catalog state and in the published
reference. Decoding checks the encoded byte budget, identities, count, exact
length, and positions before allocating the bounded hash vector. Every block
is verified before release. The final block is withheld until the complete
stream digest and exact EOF pass. A corrupt, truncated, substituted, duplicated,
or reordered stream never produces a successful completion; a failed reader
cannot become successful on retry. Range reads are not supported.

The encoded plaintext is the application digest input. Provider checksums,
echoed upload metadata, and encryption metadata are not independent read-back
evidence. No compression or encryption format is introduced here.

## Producer integration

`stage_producer` runs a synchronous serializer on a blocking worker and passes
bounded blocks through a one-block channel. Slow destinations apply backpressure
to serialization. The producer, channel, and consumer retain at most five data
blocks in total, plus the bounded manifest and the producer's own input state.
The callback returns metadata separately from output bytes. Verification returns
a private-field proof; this operation never publishes the artifact or completes
a task.

`BackupArtifactProducer` captures an authorized backup using the existing
`StorageBackupReplayBuilder` spool. Its staging method serializes that exact
snapshot row by row into the channel, without an intermediate complete artifact.
The spool is disposable and is released after serialization. A producer failure,
panic, byte-limit violation, seal failure, or verification failure aborts the
attempt after the blocking producer exits. Ignoring a sink limit error cannot
seal a shorter successful output. Failed aborts remain explicit cleanup
obligations and the staging deadline remains a recovery backstop.

Dropping the staging future disconnects a producer blocked on output, but cannot
interrupt arbitrary computation inside its callback or run asynchronous cleanup.
Owners must await cooperative cancellation before releasing reservations.
Abandoned attempts require deadline-based cleanup. This bridge does not yet
implement task cancellation, task ownership, or physical-space accounting.

The production backup executor still uses the explicitly named legacy
serialization method and its existing finite output limit. Production export
query/hydration, template rendering, publication, and downloads are unchanged.
Neither the bridge's block bound nor a replay round trip establishes H1/H2/P7
acceptance for the complete producer pipeline. Capture validation, maximum-row
memory, export production, and real resource measurements remain activation gates.

## Resource and activation gates

The [#504 budgets](https://github.com/hubuum/hubuum/issues/504) supply the H3
ceilings enforced here: 1 GiB per artifact, 1 MiB per block, 1,024 blocks, and
256 KiB per manifest. Smaller explicit limits are supported. The fixed header
is 117 bytes and each block adds 36 bytes, so a full 1,024-block manifest is
36,981 bytes. Bounds use checked arithmetic and include framing overhead.

The remaining #508 work is required before routing production outputs here:

- Connect real export hydration/rendering and backup serialization with bounded
  producer memory. Reuse `StorageBackupReplayBuilder`; do not first materialize
  an artifact and then spool it.
- Atomically associate a verified artifact with a fenced task completion,
  stable task ownership, and the existing output-authorization boundary.
  Preserve finite legacy-output readers and backup/restore compatibility.
- Integrate admission/resident-space accounting, independent cleanup scheduling,
  task cancellation and replica recovery, and common metrics/tracing outside
  individual adapters. Reconcile obligations before enabling restored work.
- Measure real concurrent workers, slow destinations/consumers, largest rows,
  cancellation, and spool loss against the named `artifact-slow-v1` profile,
  including H1/H2/P7. Protocol tests alone are not resource acceptance evidence.

The new migration only adds unused tables, functions, and constraints. Apply
normal migration and role reconciliation. The previous output paths remain
readable while this foundation is dormant. Its down migration removes the
foundation; it is suitable before feature activation, not a future procedure
for discarding published application data.

Changelog review: this dormant internal foundation has no user-facing entry.
Its activation PR must describe resulting behavior and any upgrade actions.
