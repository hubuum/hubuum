# Collection Hierarchy

Hubuum collections form a tree. The system creates one root collection named
`root`, and every other collection has exactly one parent. Classes, objects,
templates, remote targets, and event subscriptions still belong to one concrete
collection; the hierarchy affects permission resolution, not resource ownership.

## User Model

When a group receives permissions on a collection, those permissions apply to
that collection and all descendant collections. This inheritance is additive:

- child collections do not override or deny parent grants
- every permission flag inherits, including `DelegateCollection`, `ReadAudit`,
  template permissions, and remote-target permissions
- token scopes still narrow by permission type only
- combined checks do not union different rows

The last rule is important. If an operation requires both `ReadCollection` and
`UpdateCollection`, one permission row on the target collection or on one
ancestor must contain both flags. A `ReadCollection` grant on a parent plus an
`UpdateCollection` grant on the child does not satisfy that combined check.

## Creating Collections

`POST /api/v1/collections` accepts `parent_collection_id`.

```json
{
  "name": "mathematics",
  "description": "Department of Mathematics",
  "group_id": 42,
  "parent_collection_id": 7
}
```

If `parent_collection_id` is omitted or `null`, Hubuum creates the collection
under `root`.

Collection names are unique among siblings. Two departments can both have a
child named `assets` if those children have different parents.

Import requests that refer to existing collections by natural key can use
`CollectionKey.path` to disambiguate duplicate names in different branches. A
bare `CollectionKey.name` is accepted only when that name resolves to exactly
one collection.

## Inspecting and Moving

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/collections/{collection_id}/children` | List direct child collections. |
| `GET /api/v1/collections/{collection_id}/ancestors` | List ancestors, nearest parent first. |
| `PUT /api/v1/collections/{collection_id}/parent` | Move a collection to a new parent. |

Move requests use this body:

```json
{
  "parent_collection_id": 12
}
```

Moving a collection requires effective `UpdateCollection` on the moved
collection and effective `DelegateCollection` on both the old parent and the new
parent. Admin users bypass those checks.

Hubuum rejects moves that would make a collection its own ancestor. The root
collection cannot be moved or deleted. Collections with child collections cannot
be deleted; move or delete the children first.

## Direct and Effective Permissions

Permission management endpoints are direct-row endpoints. They grant, replace,
revoke, and list rows stored on the named collection only:

```text
GET /api/v1/collections/{collection_id}/permissions
POST /api/v1/collections/{collection_id}/permissions/group/{group_id}
PUT /api/v1/collections/{collection_id}/permissions/group/{group_id}
DELETE /api/v1/collections/{collection_id}/permissions/group/{group_id}
```

Use the effective endpoints to explain inherited access:

```text
GET /api/v1/collections/{collection_id}/permissions/effective/group/{group_id}
GET /api/v1/collections/{collection_id}/permissions/effective/principal/{principal_id}
GET /api/v1/collections/{collection_id}/has_permissions/{permission}
```

The two `permissions/effective` endpoints return explanation rows. Each row
includes the target collection, source collection, granting group, permission
row, depth, and an `inherited` boolean.

The `has_permissions` endpoint returns groups that have the requested
permission directly on the collection or through an ancestor collection.

## Developer Notes

The database stores hierarchy in two places:

- `collections.parent_collection_id` stores the direct tree edge
- `collection_closure` stores every ancestor-to-descendant path, including the
  self row at depth `0`

Authorization joins permission rows through `collection_closure`: a permission
row on an ancestor is matched to the descendant collection being checked. The
authorization path preserves the existing combined-permission invariant by
filtering a single permission row for all requested flags and then counting the
distinct target descendants covered by matching rows.

Create and import paths must call the shared collection insert helper so the
closure table receives the self row and inherited ancestor rows in the same
transaction as the collection row. Move operations update `parent_collection_id`
and rebuild only the closure rows that connect the moved subtree to ancestors
outside that subtree.

The hierarchy logic intentionally remains in the application database layer
instead of a workspace crate. It depends on Diesel table definitions,
PostgreSQL-specific closure-table SQL, application errors, temporal history,
and permission semantics. A workspace crate would be useful only if another
binary or service needed the same hierarchy algorithm without depending on
Hubuum's schema and authorization model.

Performance-sensitive queries rely on these indexes:

- `permissions_group_collection_idx` for principal/group permission lookups
- `collections_parent_idx` for child listings and delete checks
- `collection_closure_descendant_ancestor_depth_idx` for effective permission
  explanations and target-descendant authorization checks
- `collection_closure_ancestor_depth_idx` for subtree scans and move rewrites
