# Issue #48 — Report query performance: batched relation hydration + streamed text size check

Follow-up from PR #42. Performance debt, not a correctness bug. Two independent fixes; both
must preserve current observable behavior (output bytes **and** error messages) except where a
behavior change is explicitly called out and tested.

## Part 1 — Batch the `ObjectsInClass` relation hydration

### Problem

`build_template_items` (`src/api/v1/handlers/reports.rs`, `ReportScopeKind::ObjectsInClass` branch)
loops `for root in roots` and calls `hydrate_objects_in_class_root` per root. Each root issues
serial, un-batched DB round-trips on one held connection:

- `search_objects_related_to` (bidirectional walk via `get_bidirectionally_related_objects`) — 1/root
- `search_object_relations_between_ids` — 1/root
- `load_class_relations_touching_classes` (inside `seed_alias_buckets_from_class_relations`) — 1/root
- class names — already amortized across roots by the shared `class_names` cache

For N roots that is ~O(3N) serial round-trips. The include path next door
(`apply_report_includes` → `related_objects_for_roots`) is already batched and keyed by
`root_object_id`; the hydration path should adopt the same shape.

### Approach: fetch once, assemble in memory

**New batched DB method** in the search backend trait + `User` trait:

```
bidirectionally_related_objects_for_roots(pool, root_ids, max_depth, per_root_cap)
    -> Vec<RelatedObjectForRootRow>
```

- Recursive CTE that carries `root_object_id` through the walk — structurally the include path's
  `related_objects_for_roots_from_backend_with_admin_status` query, but with **bidirectional**
  edges (both `from→to` and `to→from`, as in `get_bidirectionally_related_objects`) and **no
  target-class filter** (hydration wants all related objects up to depth, any class).
- Dedup `DISTINCT ON (root_object_id, descendant_object_id)`, ordered
  `root_object_id ASC, descendant_object_id ASC, depth ASC, path ASC` — the per-root analogue of
  the single-root function's `deduped_walk` order.
- Per-root cap applied via `row_number() OVER (PARTITION BY root_object_id ...) <= per_root_cap`,
  mirroring the include path's `related_rank` mechanism.
- Namespace permission filtering identical to the existing paths: load namespaces granting
  `ReadObject` + `ReadObjectRelation` once; empty → empty result.
- New row type `RelatedObjectForRootRow` = existing `RelatedObjectGraphRow` fields + `root_object_id`,
  with `to_descendant_object_with_path()` (same as the existing impls).

**Assembly in `build_template_items` (ObjectsInClass branch):**

1. Collect all root ids. Call the batched method once with
   `per_root_cap = max_hydrated_template_objects()` (the global budget ceiling — guarantees each
   root has enough rows to reproduce the over-limit error for any budget state; see below).
   Group rows into `BTreeMap<root_id, Vec<HubuumObjectWithPath>>`.
2. One `search_object_relations_between_ids` over the **union** of all roots + all descendants.
   Slice per-root at neighborhood-build time (relations are global facts; each neighborhood only
   includes edges between objects in its own id set).
3. One `load_class_relations_touching_classes` over the union of all object classes; build the
   shared `class_relations_by_object_class` lookup once. One class-name prime over the union of
   **(a)** `object.hubuum_class_id` for every root + descendant **and (b)** both endpoints
   (`from_hubuum_class_id`, `to_hubuum_class_id`) of every loaded `HubuumClassRelation`. Endpoint
   (b) is required because `relation_alias_for_viewer` can need the adjacent class name even when
   no object of that adjacent class is present in the current neighborhood — today
   `seed_alias_buckets_from_class_relations` ensures this via its `relation_class_ids` prime, and
   the refactor must preserve it.
4. Per root, **in `items` order**, build the neighborhood from in-memory slices (no DB) and run
   `hydrate_object` consuming the **shared, sequential** `HydrationBudget`. Sequential hydration
   keeps budget cap semantics (root-order consumption, identical consumption counts) unchanged.

`build_object_neighborhood`, `seed_alias_buckets_from_class_relations`, and `ensure_class_names`
are refactored to accept **pre-fetched data** instead of issuing queries.

### Refinement 1 — Budget error equivalence (byte-for-byte messages)

Today, per root: `cap = HydrationBudget::remaining_related_capacity()` (= `remaining() - 1`, or the
`>= max` `BadRequest` when `remaining() == 0`); `load_related_objects_for_root` fetches with
`LIMIT cap + 1` and errors `BadRequest("... {n} related objects > {cap} remaining related
capacity")` when `n > cap` (so the displayed `n` is `cap + 1`). The first over-limit root
short-circuits the whole report.

Batched assembly reproduces this exactly, per root in `items` order:

1. `let cap = budget.remaining_related_capacity()?;` — reproduces the `remaining() == 0` →
   `"... ({hydrated} >= {max})"` error unchanged.
2. Take this root's prefetched descendants (already deterministically ordered, see Refinement 2),
   **truncate to `cap + 1`**.
3. If `descendants.len() > cap` → `BadRequest("... {len} related objects > {cap} remaining related
   capacity")` and return immediately. `len` is `cap + 1`, matching the old message.
4. Otherwise build + hydrate; budget consumed by `hydrate_object` exactly as before.

Because `cap <= max - 1`, `cap + 1 <= max`, so prefetching `per_root_cap = max` rows per root always
provides enough rows to trigger the error for any budget state. Under the limit (the only case where
output is produced) the *set* of descendants is identical to today regardless of order, and
neighborhoods re-sort internally, so output is byte-identical.

### Refinement 2 — Deterministic ordering

The single-root function's final `SELECT` has no `ORDER BY`, and `load_related_objects_for_root`
applies `LIMIT` with empty `sort` — so which rows survive truncation is currently undefined when a
root is over its query limit (and that case errors anyway). The batched query pins a total order
(`root_object_id, descendant_object_id, depth, path`), and assembly sorts each root's rows by
`(descendant_object_id, depth, path)` before truncation. This is a deterministic stabilization:
identical to today's *set* when under the limit; the over-limit case still errors with a
deterministic count.

### Refinement 3 — `class_relations_by_pair` determinism

`relation_pair_key` normalizes to `(min, max)`, and `UNIQUE(from_hubuum_class_id,
to_hubuum_class_id)` still permits both directional rows `(A,B)` and `(B,A)`. Both map to one
normalized key, so `class_relations_by_pair.insert(...)` is a last-write-wins collision whose winner
is **currently order-dependent** (DB row order × per-root processing order).

The single global `load_class_relations_touching_classes` sorts results by `id ASC` before building
`class_relations_by_pair` (last-write-wins → highest id wins, deterministically). This is a
deliberate stabilization of previously-undefined behavior, not a regression. Existing tests do not
create both directions for one class pair, so their output is byte-identical.

## Part 2 — Stream the text/html/csv size check

### Problem

`render_template` materializes the full `String`, then `enforce_text_output_limit` checks
`rendered.len()` after the fact — an oversized text/html/csv report is fully built in memory before
the 413. The JSON path already streams via `LimitedJsonWriter` and aborts early.

### Approach

- Add `LimitedStringWriter` (sibling to `LimitedJsonWriter`): accumulates into a `Vec<u8>`, returns
  an `io::Error` and sets `exceeded` the moment `max_output_bytes` is crossed; exposes the buffer as
  a `String` (minijinja output is UTF-8) on success.
- Thread `max_output_bytes` into `render_template` and render via minijinja's `render_to_write`
  into a `LimitedStringWriter`. On `exceeded`, return `ApiError::PayloadTooLarge`; otherwise return
  the accumulated `String`. Distinguish exceeded-vs-real-error exactly as the JSON path does.
- Remove the now-redundant `enforce_text_output_limit` post-check and its call site.

### Behavior change (intentional, tested)

A streamed abort cannot know the final length, so the error message changes from
`"... ({actual} > {max})"` to the JSON-style `"... (> {max})"`. The error **type** (`PayloadTooLarge`
→ 413) is unchanged. This is the one deliberate observable change; tests assert the new message.

## Testing (TDD; `source .env && ./run_tests.sh`)

Integration tests live in `src/tests/api/v1/reports.rs` with helpers `create_report_objects`,
`create_class_relation`, `create_object_relation`, `create_template`, `wait_for_task`.

1. **Multi-root batched keying** — an `ObjectsInClass` + `relation_context` report over multiple
   roots; assert each root's hydrated relations/aliases/paths are correct with no cross-root leakage.
   Existing alias/path tests guard byte-identical single-root output.
2. **Budget drift (the critical one)** — two roots where the **second** root exceeds the remaining
   hydration budget after the first consumes some; assert the exact `BadRequest` message and that it
   short-circuits — this is where batched prefetch could most easily drift from per-root behavior.
3. **Budget exhausted** — `remaining() == 0` path returns the `"... ({hydrated} >= {max})"` message.
4. **Streamed text limit** — an oversized text/html/csv report returns 413 `PayloadTooLarge` with the
   new `(> {max})` message.

## Out of scope

- The `RelatedObjects` scope hydration (single source object; already one batched fetch).
- Concurrency / changing the `HydrationBudget` model.
- Touching the include path (`related_objects_for_roots`), which is already batched.
