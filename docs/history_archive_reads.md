# Collection history archive read foundation

This internal service slice of [#514](https://github.com/hubuum/hubuum/issues/514)
persists catalogs and query captures in both complete storage adapters. It builds
on the retained [segment format](performance/history_segment_prototype.md).
Public archive tasks/routes and primary-copy retirement remain disabled. There
is no archive movement, cold provider or predicate search.

## Supported mode

Every catalog, capture and result entry requires an original application/deletion
permit and its live registered runtime owner. A missing, retired, mismatched
or stale owner fails closed. Captures preserve their original application image,
deletion revision, runtime owner, stable principal/resource/version identities,
catalog generation and deadline across segment I/O. They never refresh an old
permit. Configured permission checks apply before admission, execution and result
access, including current and captured collection names. Changing authorization
invalidates access without returning payloads or coverage to the rejected caller.
The original runtime and application fence are rechecked after external
authorization completes, before admitting a handle or returning a result.

Migration `2026-11-03-000004_history_archive_reads` initially installed the
protected, fixed-true `hubuum_archive_history_requires_legacy_runtime()` hook.
The subsequent `2026-11-19-000002_history_lifecycle` migration replaces that
restriction with the shared history-authority and original publication/producer
entry. Both adapters accept Legacy before retained expiry and OnlineV1 with its
original history scope. Unknown protocols, schema or permission errors fail
closed. There is no configuration or TTL bypass. A retained collection-expiry
member makes archive capture unavailable for that resource; a descriptor cannot
restore access through archive fallback.

Production still registers Legacy. These adapter boundaries do not establish
complete production transport or online-expiry acceptance. Never edit shipped
migration history to activate expiry. Rollback conservatively refuses any of the
lifecycle control, expiry-membership or publication tables, even when empty, as
well as retained catalog records or query pins.

## PostgreSQL catalog upgrade

Stop older API, worker and recovery processes and take a database backup.
Apply `2026-11-27-000001_packed_history_catalogs` as the migrator, reconcile
runtime grants, and deploy matching readers and publishers. Existing JSONB
catalogs remain readable. A successful publication normally converts that
resource's catalog to the packed representation. Catalogs above the packed
row ceiling use the bounded JSONB representation. Both go through the same
owner and generation checks.

A packed row contains the resource and generation, version count, compressed
metadata, and the complete UUID and SHA-256 commitment of each version.
The database checks that the envelope matches the row and that its declared
length covers every commitment. The reader reconstructs and validates the full
logical catalog. Query captures keep their existing format.

An older reader reports an upgrade error when it encounters a packed row.
The down migration refuses to proceed while any packed row exists. Roll
forward, or follow the documented [physical recovery procedure](deletion_recovery.md)
using the pre-upgrade backup, matching binaries and independent authority.

## Catalog and captured inputs

A per-resource catalog persists its generation and explicit
`PrimaryCopiesRequired` coverage. Each descriptor preserves the logical version,
original closed half-open interval, payload commitment, dependency version UUIDs,
format version and immutable artifact reference. Locations contain destination,
object version, attempt and trusted manifest digest. Catalog descriptors contain
no payload. Publication checks the exact retained primary version and uses a
short generation-checked transaction. A stale generation requires replanning.
Within one publication, repeated entries for a version must agree on every
descriptor field, including its format and immutable location. Conflicting
entries reject the entire batch and preserve the previous catalog generation.
A later generation can still relocate a version whose logical content matches.

Admission takes a short snapshot of at most one matching hot collection version
and pins the selected catalog descriptor in the same transaction. PostgreSQL uses
one bounded statement for candidate selection and canonical collection presence.
A concurrent hot closure cannot alter the captured row. Catalog replacement
cannot rebind an admitted pin: the pin retains its original immutable location
and generation. Publication does not delete or move any artifact or primary row.

Segment work happens after that transaction ends. Readers resolve durable artifact
publication and verify the trusted manifest, index blocks, selected record and
captured content commitment. The pinned catalog format must match the header and
footer. Readers accept JSON indexes (v1), fixed-width indexes (v2), and bounded
compressed indexes with segment-local version keys (v3). A v3 lookup verifies
the selected record's complete UUID before returning an exact match. Upgrade
readers and recovery tools before publishing v3 segments.
As-of execution reads the exact version selected at
admission; it does not run a newer as-of search. A corrupt or missing archive
cannot fall back to a hot row and claim complete coverage.

Catalogs and captures live outside application logical replacement. Memory restore
preserves them explicitly; PostgreSQL stores them in the protected control schema.
Restoring application data changes the application fence, invalidating older
query handles. A missing source version or restore gap cannot become a complete
empty result, and an older archived version is never a fallback.

## Coverage and lifetime

Execution consumes its query handle; access consumes the opaque result. Neither
handle is cloneable or deserializable, and durable task resumption is not active.
The result is opaque until fresh principal, original-fence and configured
permission checks succeed. Its authorized projection includes the immutable capture
ID and captured selection,
generation, capture time, original availability deadline, coverage and charged
read work. Coverage states are `Complete`, `Unavailable`, `Expired`, `Corrupt`,
`DependencyIncomplete`, `BudgetLimited` and `Unsupported`. Incomplete states
return no payload. Oversized rows retain only bounded historical authorization
facts; those facts must pass configured authorization before overflow coverage
is exposed. Authorization or authority failures fail the operation instead
of exposing a coverage distinction to an inaccessible caller.

This slice supports collection history with no version dependencies. Other
families, unknown formats and dependency-bearing descriptors are explicit
incomplete outcomes. A catalog miss by itself proves nothing about completeness.
An authorized as-of query before the canonical resource's preserved creation time
can produce a complete empty result. Unknown exact versions and uncovered time
intervals are unavailable. Deleted collections are outside this first service.

The caller supplies a positive deadline at most 48 hours after capture. Reads and
catalog changes never renew it. PostgreSQL observes expiry using its own clock.
Expired capture staging is reaped in bounded admission work; a reaped or unknown
handle fails access with the existing gone/unavailable operation error. This is
not the later durable task-discovery or post-ready availability-window contract.
PostgreSQL commits each bounded cleanup batch even when admission still lacks
capacity, so later attempts can reclaim further expired captures.

## Bounds and remaining qualification

| Input or obligation | Enforced bound |
| --- | --- |
| One encoded hot row | 4 MiB, including capture framing; JSON escaping counts |
| Catalog allocation per selected resource | 4 MiB and 10,000 version candidates |
| Publication batch | 1,000 descriptors and 16 MiB descriptor/source input |
| One captured input manifest | 5 MiB |
| Retained captures | 1,024 and 1 GiB staging reservations per backend |
| Expired capture cleanup during admission | At most 16 captures |
| Segment read attempt | 16 MiB reserved before I/O |
| Cumulative read work per capture | 64 MiB; repeated attempts retain charges |
| Successful result | At most one bounded captured row |

Memory candidate examination is capped cumulatively at 10,000 per operation.
PostgreSQL uses stable identity/time indexes and fetches at most two bounded
candidates to reject ambiguity. Catalog and staging exhaustion stop admission;
there is no unbounded page or whole-history capture. Staging reservations include
fixed headroom for cumulative-work metadata changes and remain charged until
capture record removal. PostgreSQL MVCC allocation and vacuum costs are distinct
and remain unqualified. Successful manifest calls charge actual bytes after a
preflight envelope check; failed calls charge the retained manifest ceiling.
Fetched index/data blocks always charge their full physical size.

The [#504 budgets](performance/lifecycle_budgets.md) remain unchanged. Functional
bounds are distinct from H1 RSS and H2 deployment accounting qualification or
P1/P2 performance acceptance. Full 10k/100k catalog allocation, worker RSS,
constrained profiles, complete pipeline amplification and subset-rewrite costs
remain unqualified. No cold-provider or source-retirement acceptance is claimed.

A published mixed segment can stage a fresh replacement from exact resource and
version UUIDs. Before staging, the batch admits up to 1,000 selected frames
and 16 MiB across all supplied sources. A resource version present at two
immutable destinations consumes two frame slots and the bytes of both copies.
The coordinator requires coverage of every selected resource version and
accepts each published source once. The worker authenticates and decodes
every frame, including selected frames. Surviving frames retain their original
bytes. An absent version, a
survivor depending on a selected version, corruption or a depleted read budget
stops staging. After a restart the same owned attempt can recover a sealed or
published replacement. The worker verifies the destination and compares every
surviving frame with the original source before returning its provider state.
Mixed and untouched segments can be checked against all versions in the batch.
When every frame in a segment is selected, the worker verifies the complete
resource and version UUID in each authenticated frame and returns the original
artifact reference and selection commitment. This produces no replacement.
The same 16 MiB selected-frame limit applies before reading the frames.
The owning operation must account for every source and fence the catalog
generation before publishing the handoff and retiring the old copy. See the
[local subset-rewrite measurements](performance/history_segment_prototype.md#local-mixed-segment-rewrite).

Capture staging exhaustion preserves at most 4 KiB of encoded historical
authorization facts plus fixed original request/fence/lifetime metadata in a
private validated error proof. Both adapters preserve this proof across their
error boundary. The service checks its bindings, configured historical policy
and the original registered authority before exposing budget-limited admission; bare size
errors do not establish authorized coverage. Error display/debug and transport
serialization omit these facts. This does not establish a publication fence or
activate the service alongside retained online expiry.

## Preserving superseded archive locations

Erasure review can look up the immutable version of an artifact that a catalog
previously named, even when no query ever captured it. Publication records the
former location and manifest digest with the catalog generation change.
Replaying a location or rejecting a stale publication adds no evidence. The
operator lookup returns the current catalog generation and the exact version’s
active location, together with query pins and references retained after query
reclamation or catalog replacement. A single snapshot covers all of these reads.
A relocation appears either before or after its commit, with the corresponding
set of immutable copies. Physical retirement requires independent evidence.

Rewrite planning observes each selected version at the supplied application fence.
Versions of one resource must share a catalog generation. The planner groups
selected frames by immutable artifact before opening an index, charges repeated
physical copies to the batch, and carries the observed generations forward for
the catalog handoff. Selected versions with no recorded archive copies still
participate in dependency checks on surviving frames.

Stop API and worker processes, back up PostgreSQL, apply the catalog-displacement
migration with `hubuum-admin --migrate`, reconcile runtime grants, and start
matching writers. PostgreSQL rejects an unaccounted replacement at commit.
After the first displaced location is recorded, use roll-forward recovery.
Earlier catalog replacements cannot be recovered from this migration.

## Synchronizing runtime migrations

Apply `2026-11-11-000001_archive_runtime_compatibility` after bringing the runtime
settings migrations into the project branch. The subsequent migration
`2026-11-19-000001_idle_task_scheduling` qualifies its scheduler-only change
and restores the prior checkpoint on rollback. Each migration updates the exact
supported checkpoint for unversioned Legacy registrations. Both preserve the fixed
Legacy restriction and original owner/application checks. Unknown future
checkpoints still fail closed. Both upgrade and rollback preserve the gate of an
already installed versioned history protocol. The later history lifecycle
migration retains the scheduler's bounded idle check and restores Legacy entry at
the scheduler checkpoint on rollback. Rolling back the scheduler migration then
restores the November 11 checkpoint.
Rolling back the November 11 compatibility migration while the newer runtime
settings remain installed blocks archive calls until that compatibility
migration is reapplied. Follow the runtime-policy offline upgrade and rollback
procedure for the combined deployment.
