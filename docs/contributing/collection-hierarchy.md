# Collection hierarchy implementation

For collection creation, moves, and inherited access, see the
[collection guide](../collection_hierarchy.md).

## Persistence and authorization

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

The hierarchy implementation intentionally remains inside the PostgreSQL
storage adapter rather than the root application or backend-neutral contract.
It depends on Diesel table definitions, PostgreSQL-specific closure-table SQL,
temporal history, and permission semantics. Other storage adapters must preserve
the same observable hierarchy contract but may use a different persistence
representation.

Performance-sensitive queries rely on these indexes:

- `permissions_group_collection_idx` for principal/group permission lookups
- `collections_parent_idx` for child listings and delete checks
- `collection_closure_descendant_ancestor_depth_idx` for effective permission
  explanations and target-descendant authorization checks
- `collection_closure_ancestor_depth_idx` for subtree scans and move rewrites
