# Storage Query Semantics

This document defines the common query behavior that every complete storage
backend must preserve. Method-specific query types may narrow the accepted
filters, sorts, limits, and visibility inputs, but they must not reinterpret
these rules.

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

`QueryOptions` is a validated carrier, not a promise that every `FilterField`
is valid for every operation. Each capability owns its accepted subset.

## Identity and Collection-Authorization Matrix

This matrix is normative for the membership and collection-authorization page
operations. Aliases separated by `/` address the same logical field.

| Operation | Filter fields | Sort fields |
| --- | --- | --- |
| `list_groups` | `id`, `name`/`groupname`, `identity_scope`, `description`, `created_at`, `updated_at`, `revision` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_principal_groups` | `id`, `name`/`groupname`, `identity_scope`, `description`, `created_at`, `updated_at` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_group_members` | `id`, `name`/`username`, `created_at`, `updated_at`, `revision` | `id`, `name`/`username`, `created_at`, `updated_at`, `revision` |
| `list_principal_collection_permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at` |
| `list_groups_with_collection_permission` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` | `id`, `name`/`groupname`, `description`, `created_at`, `updated_at`, `revision` |
| `list_collection_group_permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at` |
| `list_local_collection_grants` | `id`, `name`/`groupname`, `created_at`, `updated_at`, `permissions` | `id`, `name`/`groupname`, `created_at`, `updated_at` |

The `permissions` filter selects grants containing the requested permission. It
is a filter only and is not a cursor sort field.

## Publication Status

The common rules above are part of the workspace contract. The table is the
first method-specific support inventory. Exact filter, sort, cursor, and
consistency matrices for the remaining pageable capabilities still live across
trait documentation and compatibility tests and are not yet a supported
external-crate promise.

Before `hubuum-storage-core` is published as an independently certifiable
adapter SDK, maintainers must complete those method-specific matrices and make
their drift machine-checkable. Until then, an out-of-tree adapter can compile
against the boundary, but certification still requires the Hubuum workspace
compatibility suite.
