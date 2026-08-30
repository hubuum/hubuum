# Storage Query Semantics

This document defines the common query behavior that every complete storage
backend must preserve. Method-specific query types may narrow the accepted
filters, sorts, limits, and visibility inputs, but they must not reinterpret
these rules.

The machine-readable [method contract registry](method-contracts.toml) is the
normative, exhaustive method-level companion to these common rules. Every
pageable, searchable, batched, and complete-collection method names its exact
input carrier and one profile. Query profiles define supported filter and sort
keys, cursor, visibility, count, snapshot, bound, and error behavior.
Collection profiles define ordering, duplicate and missing input behavior,
multiplicity, completeness, bounds, visibility, snapshot consistency, and
portable errors.

## Naming and Result Shapes

`get_*` names one point lookup or named snapshot. A required point lookup
returns `NotFound` when absent; a method whose result type is `Option<T>` or a
domain lookup enum exposes absence as part of its documented protocol.

`list_*` returns a collection. Pageable list methods return `StoragePage<T>` or
a documented capability-specific page. A bounded or naturally scoped complete
collection may return `Vec<T>` when the trait documents that shape; therefore
the `list_*` prefix alone does not promise pagination.

`load_*` is reserved for a complete, non-paged projection used for policy
evaluation or application composition. It never silently applies cursor
pagination. `resolve_*` normalizes or combines an address, resolves names in a
batch, or loads an endpoint aggregate before returning the authoritative
result; its result type defines whether absence is optional or `NotFound`.

Legacy `list_all_*` names are explicit complete reads. New complete projections
should use `load_*`; new aggregate observations should use a domain-specific
`*_snapshot` name. Renaming the remaining stable legacy methods solely for
style is deferred until the external crate publication surface is selected.

## Common Page Contract

For every pageable operation:

1. Authorization and resource visibility are applied before filtering,
   counting, sorting, and paging.
2. Filters, the optional exact total, and page items use the same predicate. If
   `include_total` is false, `StoragePage::total` is `None`; it is never a
   sentinel value.
3. The backend evaluates an exact total and its page in one consistent native
   read snapshot when both are requested.
4. Sort order is deterministic. The adapter appends a stable unique
   identifier as a tie-breaker when the requested fields are not unique.
5. A cursor continues the exact sort projection that produced it. Callers must
   not reuse it with different sorts, visibility, or resource scope, and an
   adapter must not expose or require a native cursor representation.
6. Malformed cursors, unknown filters, unknown sorts, and operators that are
   invalid for a recognized field return `InvalidInput`. Every documented
   query behavior is mandatory for a complete backend.
7. Limits and cursor byte budgets are enforced before native query execution.
   Contract-specific oversized input uses `InputTooLarge` where documented.
8. An empty match returns an empty page and an exact total of zero when a total
   was requested. It is not `NotFound`.
9. A field or combination absent from the method's registry profile is
   unsupported and returns `InvalidInput`. A recognized field with a malformed
   value or invalid operator also returns `InvalidInput`; neither case may be
   silently ignored.

`QueryOptions` is a validated carrier, not a promise that every `FilterField`
is valid for every operation. Each capability owns its accepted subset.

### Candidate Page Contract

`StorageCandidatePageLimit` validates a positive per-read bound no greater than
512. `StorageCandidatePage<T>` contains no more than that many rows and reports
whether the stable operation-specific cursor can continue the enumeration. An
empty candidate page cannot report more rows.

Delegated authorization applies ordinary filters and stable sorting in storage,
authorizes each bounded candidate page, and applies the public page limit only
after authorization. A skipped total permits early termination after one
authorized response page plus look-ahead. An exact total visits all candidate
pages but retains only that bounded response window. Unified-search candidates
carry the adapter-owned rank cursor with every row so application code does not
reconstruct native ordering.

An operation that must return a complete authorized set requires an explicit,
validated total candidate bound. It returns an error when the enumeration would
exceed that bound; it does not silently truncate or fall back to an unbounded
`Vec<T>`.

Two operation-shaped carriers preserve application-prepared pagination forms.
`StorageGroupListQuery` carries one record query; the adapter derives an
optional exact count from the same filters without applying its sorting, limit,
or cursor. `StorageComputedObjectQueryOptions::try_new` requires the execution
form to use the normalized requested sort with an ID tie-breaker, expand the
effective page limit by exactly one row, and retain the requested filters,
cursor, and count intent. The effective limit is resolved by application policy
and may be lower than an explicit client limit when that limit is capped, but
it must never be higher than an explicit requested limit.

## Identity and Collection-Authorization Matrix

This convenience matrix is extracted from the normative registry for the
membership and collection-authorization page operations. Aliases separated by
`/` address the same logical field.

| Operation | Filter fields | Sort fields |
| --- | --- | --- |
| `list_groups` | `id`, `name`/`groupname`, `identity_scope`, `description`, `created_at`, `updated_at`, `revision` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_principal_groups` | `id`, `name`/`groupname`, `identity_scope`, `description`, `created_at`, `updated_at` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_group_members` | `id`, `name`/`username`, `created_at`, `updated_at`, `revision` | `id`, `name`/`username`, `created_at`, `updated_at`, `revision` |
| `list_principal_collection_permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at` |
| `list_groups_with_collection_permission` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_local_collection_grants` | `id`, `name`/`groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at` |

The `permissions` filter selects grants containing the requested permission. It
is a filter only and is not a cursor sort field.

## Registry and Publication Status

The common rules and exhaustive registry are part of the workspace contract.
The application-boundary guard derives the required registry keys from the
method effect inventory, verifies every entry's result shape and portable error
mapping, rejects unused profiles, and requires method-specific semantic
evidence. Adding or reclassifying a collection-shaped method therefore requires
updating its observable contract in the same change.

This completes the method-level query and collection specification; it does
not by itself publish `hubuum-storage-core` as a supported standalone adapter
SDK. Crate publication, compatibility promises, versioning, and the supported
certification entry point are a separate policy decision.
