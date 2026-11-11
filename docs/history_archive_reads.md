# Collection history archive read foundation

This internal service slice of [#514](https://github.com/hubuum/hubuum/issues/514)
persists catalogs and query captures in both complete storage adapters. It builds
on the retained [segment format](performance/history_segment_prototype.md).
Public archive tasks/routes and primary-copy retirement remain disabled. There
is no archive movement, cold provider or predicate search.

## Supported mode

Every catalog, capture and result entry requires an original application/deletion
permit and a live registered Legacy runtime owner. A missing, retired, mismatched
or stale owner fails closed. Captures preserve their original application image,
deletion revision, runtime owner, stable principal/resource/version identities,
catalog generation and deadline across segment I/O. They never refresh an old
permit. Configured permission checks apply before admission, execution and result
access, including current and captured collection names. Changing authorization
invalidates access without returning payloads or coverage to the rejected caller.
The original runtime and application fence are rechecked after external
authorization completes, before admitting a handle or returning a result.

Migration `2026-11-03-000004_history_archive_reads` owns the protected, fixed-true
`hubuum_archive_history_requires_legacy_runtime()` hook. It has no configuration
or TTL bypass. The archive entry guard accepts unversioned registrations only
under the explicitly supported migration version. If the history protocol column
exists, it requires protocol zero. Unknown values, schema or permission errors
fail closed. The SDK exports the equivalent fixed Legacy requirement for memory
composition.

The eventual combined runtime must refuse OnlineV1 registration while this hook
is true, including an empty catalog. #513 owns that integration and the future
archive publication/read/restore gate. This standalone slice does not establish
combined-fleet or online-expiry acceptance. Replacing the restriction requires a
later reviewed migration after integration and qualification; never edit shipped
migration history to activate expiry. Rollback conservatively refuses any of the
lifecycle control, expiry-membership or publication tables, even when empty, as
well as retained catalog records or query pins.

## Catalog and captured inputs

A per-resource catalog persists its generation and explicit
`PrimaryCopiesRequired` coverage. Each descriptor preserves the logical version,
original closed half-open interval, payload commitment, dependency version UUIDs,
format version and immutable artifact reference. Locations contain destination,
object version, attempt and trusted manifest digest. Catalog descriptors contain
no payload. Publication checks the exact retained primary version and uses a
short generation-checked transaction. A stale generation requires replanning.

Admission takes a short snapshot of at most one matching hot collection version
and pins the selected catalog descriptor in the same transaction. PostgreSQL uses
one bounded statement for candidate selection and canonical collection presence.
A concurrent hot closure cannot alter the captured row. Catalog replacement
cannot rebind an admitted pin: the pin retains its original immutable location
and generation. Publication does not delete or move any artifact or primary row.

Segment work happens after that transaction ends. Readers resolve durable artifact
publication and verify the trusted manifest, index blocks, selected record and
captured content commitment. As-of execution reads the exact version selected at
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

Capture staging exhaustion preserves at most 4 KiB of encoded historical
authorization facts plus fixed original request/fence/lifetime metadata in a
private validated error proof. Both adapters preserve this proof across their
error boundary. The service checks its bindings, configured historical policy
and fresh Legacy authority before exposing budget-limited admission; bare size
errors do not establish authorized coverage. Error display/debug and transport
serialization omit these facts. This does not establish a publication fence or
activate the service alongside retained online expiry.

## Synchronizing runtime migrations

Apply `2026-11-11-000001_archive_runtime_compatibility` after bringing the runtime
settings migrations into the project branch. It updates the exact supported
checkpoint for unversioned Legacy registrations; it preserves the fixed Legacy
restriction, original owner/application checks, and refusal of unknown future
checkpoints. Both upgrade and rollback leave an already installed versioned
history protocol unchanged, so application order cannot replace its gate.
Rollback on an unversioned registry restores the original archive checkpoint
restriction; with newer runtime migrations still present, archive calls then
refuse until the compatible migration is reapplied. Follow the runtime-policy
offline upgrade and rollback procedure for the combined deployment.
