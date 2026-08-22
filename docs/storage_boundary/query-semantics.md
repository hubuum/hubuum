# Storage Query Semantics

This document defines the common query behavior that every complete storage
backend must preserve. Method-specific query types may narrow the accepted
filters, sorts, limits, and visibility inputs, but they must not reinterpret
these rules.

## Naming and Result Shapes

Pageable operations use `list_*` and return `StoragePage<T>` or a documented
capability-specific page. Operations named `load_*` intentionally return a
complete non-paged projection for policy evaluation or application composition;
they do not silently apply cursor pagination. Point reads use `get_*` when a
missing durable resource is `NotFound` and `resolve_*` when the operation
normalizes or combines an address before loading it.

Legacy `list_all_*` names are explicit complete reads. New contract methods
should use `load_*` or a domain-specific snapshot name instead.

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
   invalid for a recognized field return `InvalidInput`. `UnsupportedOperation`
   does not mean that an adapter omitted required query behavior.
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
