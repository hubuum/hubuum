# Issue #48 — Report Query Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the N+1 relation-hydration round-trips in the `ObjectsInClass` report path by fetching all per-root data in batched queries, and make the text/html/csv output-size check stream-bounded like the JSON path.

**Architecture:** Part A (independent, do first) streams the template render into a size-bounded writer. Part B replaces the per-root hydration loop with three batched fetches (related objects keyed by root, relations across the union, class metadata once) plus pure in-memory per-root assembly that keeps the existing sequential `HydrationBudget` semantics byte-for-byte.

**Tech Stack:** Rust, Diesel + raw recursive SQL (Postgres), minijinja 2 (`render_to_write`), actix-web integration tests via `rstest`.

**Spec:** `docs/superpowers/specs/2026-06-19-issue-48-report-perf-design.md`

**Test command:** `source .env && ./run_tests.sh` (runs `cargo test`). To target one test: `source .env && ./run_tests.sh <test_name>`.

---

## File Structure

- `src/utilities/reporting.rs` — `render_template` gains a `max_output_bytes` arg and renders via `render_to_write`.
- `src/api/v1/handlers/reports.rs` — `LimitedStringWriter` (new), `build_text_report_artifact` (modified), `enforce_text_output_limit` (deleted); the hydration assembly (`build_template_items` ObjectsInClass branch rewritten; `hydrate_objects_in_class_root` + `load_related_objects_for_root` deleted; `hydrate_related_root`, `build_object_neighborhood` modified; new `take_related_within_budget`, `load_hydration_class_metadata`, `seed_alias_buckets`, `HydrationClassMetadata`).
- `src/models/relation.rs` — new `RelatedObjectForRootRow` row type + `to_descendant_object_with_path`.
- `src/db/traits/user/search.rs` — new batched `bidirectionally_related_objects_for_roots_from_backend[_with_admin_status]`.
- `src/models/traits/user.rs` — new convenience wrapper `bidirectionally_related_objects_for_roots`.
- `src/tests/api/v1/reports.rs` — new integration tests (multi-root batched keying; streamed text 413).
- `src/api/v1/handlers/reports.rs` `#[cfg(test)] mod tests` — new unit tests for `take_related_within_budget`.

---

# Part A — Stream the text/html/csv size check

## Task A1: `LimitedStringWriter` with unit tests

**Files:**
- Modify: `src/api/v1/handlers/reports.rs` (add struct near `LimitedJsonWriter`, ~line 2312)
- Test: `src/api/v1/handlers/reports.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing unit tests**

Add to the `mod tests` block in `src/api/v1/handlers/reports.rs`. Add `LimitedStringWriter` to the `use super::{...}` import list at the top of `mod tests`.

```rust
#[test]
fn limited_string_writer_accumulates_under_limit() {
    use std::io::Write;
    let mut writer = super::LimitedStringWriter::new(16);
    writer.write_all(b"hello ").unwrap();
    writer.write_all(b"world").unwrap();
    assert!(!writer.exceeded());
    assert_eq!(writer.into_string().unwrap(), "hello world");
}

#[test]
fn limited_string_writer_aborts_over_limit() {
    use std::io::Write;
    let mut writer = super::LimitedStringWriter::new(4);
    let err = writer.write_all(b"toolong").unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
    assert!(writer.exceeded());
}
```

- [ ] **Step 2: Run tests, verify they fail to compile**

Run: `source .env && ./run_tests.sh limited_string_writer`
Expected: FAIL — `LimitedStringWriter` not found.

- [ ] **Step 3: Implement `LimitedStringWriter`**

Insert directly after the `impl Write for LimitedJsonWriter { ... }` block (after ~line 2346) in `src/api/v1/handlers/reports.rs`:

```rust
// Mirror of LimitedJsonWriter for template output: minijinja render_to_write streams into
// this sink so an oversized text/html/csv report aborts at the byte budget instead of being
// fully materialized before the 413.
struct LimitedStringWriter {
    max_bytes: usize,
    buffer: Vec<u8>,
    exceeded: bool,
}

impl LimitedStringWriter {
    fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            buffer: Vec::new(),
            exceeded: false,
        }
    }

    fn exceeded(&self) -> bool {
        self.exceeded
    }

    fn into_string(self) -> Result<String, ApiError> {
        String::from_utf8(self.buffer).map_err(|error| {
            ApiError::InternalServerError(format!("Rendered report was not valid UTF-8: {error}"))
        })
    }
}

impl Write for LimitedStringWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.len().saturating_add(buf.len()) > self.max_bytes {
            self.exceeded = true;
            return Err(io::Error::other("template output limit exceeded"));
        }

        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `source .env && ./run_tests.sh limited_string_writer`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/api/v1/handlers/reports.rs
git commit -m "Add LimitedStringWriter for streamed report output bounding

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task A2: Stream the render through the limit; delete the post-hoc check

**Files:**
- Modify: `src/utilities/reporting.rs:79-139` (`render_template`)
- Modify: `src/api/v1/handlers/reports.rs:805-833` (`build_text_report_artifact`)
- Modify: `src/api/v1/handlers/reports.rs:2348-2364` (delete `enforce_text_output_limit`)

- [ ] **Step 1: Change `render_template` to render into a bounded writer**

In `src/utilities/reporting.rs`, change the signature and the render block. Replace the signature (line 79-85) to add `max_output_bytes`:

```rust
pub fn render_template(
    template: &ReportTemplate,
    namespace_templates: &[ReportTemplate],
    context: &serde_json::Value,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    max_output_bytes: usize,
) -> Result<(String, Vec<ReportWarning>), ApiError> {
```

Replace the render block (current lines 129-138, from `begin_template_warning_capture();` through `Ok((rendered?, warnings))`) with:

```rust
    begin_template_warning_capture();
    let mut writer = crate::api::v1::handlers::reports::LimitedStringWriter::new(max_output_bytes);
    let render_result = env
        .env
        .get_template(&env.template_name)
        .and_then(|template| template.render_to_write(context, &mut writer));
    let warnings = finish_template_warning_capture();

    match render_result {
        Ok(()) => Ok((writer.into_string()?, warnings)),
        Err(error) => {
            if writer.exceeded() {
                return Err(ApiError::PayloadTooLarge(format!(
                    "Rendered report exceeded max_output_bytes (> {max_output_bytes})"
                )));
            }
            Err(template_error("Template render failed", error))
        }
    }
```

Note: `LimitedStringWriter` must be reachable. Make it `pub(crate)` — in `src/api/v1/handlers/reports.rs` change `struct LimitedStringWriter {` to `pub(crate) struct LimitedStringWriter {` and its three methods (`new`, `exceeded`, `into_string`) to `pub(crate) fn`. (`exceeded` is used internally; keeping it `pub(crate)` is harmless.)

- [ ] **Step 2: Update the caller `build_text_report_artifact`**

In `src/api/v1/handlers/reports.rs`, replace the body of `build_text_report_artifact` (lines 805-833) so it computes the byte budget and passes it in, and drops the post-hoc check:

```rust
fn build_text_report_artifact(
    runtime: &ReportRuntime,
    execution: ReportExecution,
    timings: ReportExecutionTimings,
) -> Result<ReportArtifact, ApiError> {
    let template = required_template(runtime, runtime.content_type)?;
    let context = report_template_context(&runtime.report, &execution);
    let max_output_bytes = runtime
        .report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or_else(configured_report_max_output_bytes);
    let (rendered, template_warnings) = render_template(
        template,
        &runtime.namespace_templates,
        &context,
        runtime.content_type,
        runtime.missing_data_policy,
        max_output_bytes,
    )?;
    let mut warnings = execution.warnings;
    warnings.extend(template_warnings);

    Ok(ReportArtifact {
        content_type: runtime.content_type,
        json_output: None,
        text_output: Some(rendered),
        meta: execution.meta,
        warnings,
        template_name: Some(template.name.clone()),
        timings,
    })
}
```

- [ ] **Step 3: Delete `enforce_text_output_limit`**

Delete the entire `fn enforce_text_output_limit(...) { ... }` (lines 2348-2364). Confirm there are no other callers:

Run: `grep -rn "enforce_text_output_limit" src/`
Expected: no matches (the only call was in `build_text_report_artifact`, removed in Step 2).

- [ ] **Step 4: Build to verify it compiles**

Run: `source .env && ./run_tests.sh --no-run`
Expected: compiles cleanly. If `render_template` has any other callers, update them to pass `max_output_bytes`. Find them: `grep -rn "render_template(" src/` — the only non-test caller should be `build_text_report_artifact`. If a unit test calls it, pass `usize::MAX`.

- [ ] **Step 5: Commit**

```bash
git add src/utilities/reporting.rs src/api/v1/handlers/reports.rs
git commit -m "Stream text/html/csv report render through size limit

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task A3: Integration test — oversized text report fails with 413 message

**Files:**
- Test: `src/tests/api/v1/reports.rs` (add test in `mod tests`)

- [ ] **Step 1: Add import**

In `src/tests/api/v1/reports.rs`, add `ReportLimits` to the `use crate::models::{...}` list (line 12-18).

- [ ] **Step 2: Write the failing test**

Add this test to `mod tests`:

```rust
#[rstest]
#[actix_web::test]
async fn test_report_text_output_exceeding_max_bytes_fails(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let classes = create_test_classes(&context, "report_text_limit").await;
    let class = classes[0].clone();
    let _ = create_report_objects(&context.pool, &class).await;
    let template_id = create_template(
        &context.pool,
        class.namespace_id,
        "oversized-template",
        ReportContentType::TextPlain,
        "{% for item in items %}{{ item.name }} has a description of {{ item.description }} and lives forever\n{% endfor %}",
    )
    .await;

    let body = ReportRequest {
        scope: ReportScope {
            kind: ReportScopeKind::ObjectsInClass,
            class_id: Some(class.id),
            object_id: None,
        },
        query: None,
        output: Some(crate::models::ReportOutputRequest {
            template_id: Some(template_id),
        }),
        missing_data_policy: None,
        limits: Some(ReportLimits {
            max_items: None,
            max_output_bytes: Some(8),
        }),
        include: None,
        relation_context: None,
    };

    let resp = post_request_with_headers(
        &context.pool,
        &context.admin_token,
        REPORTS_ENDPOINT,
        &body,
        vec![],
    )
    .await;
    let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
    let task: TaskResponse = test::read_body_json(resp).await;
    let task = wait_for_task(&context, task.id, &[TaskStatus::Failed]).await;

    let summary = task.summary.unwrap_or_default();
    assert!(
        summary.contains("Payload too large") && summary.contains("(> 8)"),
        "unexpected summary: {summary}"
    );

    cleanup(&context, "report_text_limit").await;
}
```

Note: confirm the cleanup convention used by other tests in this file. Some use `namespace.cleanup()`, others `cleanup(&context, prefix)`. `create_test_classes` is paired with `cleanup(&context, prefix)` per the import on line 19 — match the pattern used by other `create_test_classes` tests (e.g. `test_report_submission_returns_task_and_json_output_is_refetchable`).

- [ ] **Step 3: Run the test**

Run: `source .env && ./run_tests.sh test_report_text_output_exceeding_max_bytes_fails`
Expected: PASS. (8 bytes is smaller than the first rendered line, so `render_to_write` aborts early and the task fails with the streamed message.)

- [ ] **Step 4: Commit**

```bash
git add src/tests/api/v1/reports.rs
git commit -m "Test streamed text report output size limit

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

# Part B — Batch the ObjectsInClass relation hydration

## Task B1: `RelatedObjectForRootRow` model

**Files:**
- Modify: `src/models/relation.rs` (add after `RelatedObjectIncludeRow`, ~line 230)
- Modify: `src/models/traits/object_relation.rs` (add `to_descendant_object_with_path` impl after the existing impls, ~line 104)

- [ ] **Step 1: Add the row type**

In `src/models/relation.rs`, after the `RelatedObjectIncludeRow` struct, add (the existing file already imports `Integer, Array, Text, Jsonb, Timestamp` for the sibling rows):

```rust
#[derive(Debug, QueryableByName, Serialize, Deserialize, Clone)]
pub struct RelatedObjectForRootRow {
    #[diesel(sql_type = Integer)]
    pub root_object_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_object_id: i32,
    #[diesel(sql_type = Integer)]
    pub depth: i32,
    #[diesel(sql_type = Array<Integer>)]
    pub path: Vec<i32>,
    #[diesel(sql_type = Text)]
    pub descendant_name: String,
    #[diesel(sql_type = Integer)]
    pub descendant_namespace_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_class_id: i32,
    #[diesel(sql_type = Text)]
    pub descendant_description: String,
    #[diesel(sql_type = Jsonb)]
    pub descendant_data: serde_json::Value,
    #[diesel(sql_type = Timestamp)]
    pub descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub descendant_updated_at: chrono::NaiveDateTime,
}
```

- [ ] **Step 2: Add the conversion**

In `src/models/traits/object_relation.rs`, after the `impl RelatedObjectIncludeRow { ... }` block (~line 104), add:

```rust
impl RelatedObjectForRootRow {
    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }
}
```

Add `RelatedObjectForRootRow` to the `use` imports at the top of `src/models/traits/object_relation.rs` if the existing rows are explicitly imported there (check the file head; `HubuumObjectWithPath` and the other row types must resolve).

- [ ] **Step 3: Build**

Run: `source .env && ./run_tests.sh --no-run`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add src/models/relation.rs src/models/traits/object_relation.rs
git commit -m "Add RelatedObjectForRootRow for batched hydration

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task B2: Batched bidirectional related-objects query

**Files:**
- Modify: `src/db/traits/user/search.rs` (add methods inside `impl UserSearchBackend`, after `related_objects_for_roots_from_backend_with_admin_status`, ~line 1796)
- Modify: `src/models/traits/user.rs` (add wrapper after `related_objects_for_roots`, ~line 240)

- [ ] **Step 1: Add the backend methods**

In `src/db/traits/user/search.rs`, after the closing `}` of `related_objects_for_roots_from_backend_with_admin_status` (line ~1796, still inside the `impl` block), add:

```rust
    async fn bidirectionally_related_objects_for_roots_from_backend(
        &self,
        pool: &DbPool,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.bidirectionally_related_objects_for_roots_from_backend_with_admin_status(
            pool,
            root_object_ids,
            max_depth,
            per_root_cap,
            is_admin,
        )
        .await
    }

    async fn bidirectionally_related_objects_for_roots_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
        is_admin: bool,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError> {
        if root_object_ids.is_empty() {
            return Ok(Vec::new());
        }

        let permissions =
            PermissionsList::new([Permissions::ReadObject, Permissions::ReadObjectRelation]);
        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions, is_admin)
            .await?
            .into_iter()
            .map(|namespace| namespace.id)
            .collect();

        if namespace_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut bind_variables = Vec::<SQLValue>::new();
        let root_array_sql = sql_integer_array(root_object_ids, &mut bind_variables);
        let namespace_array_sql = sql_integer_array(&namespace_ids, &mut bind_variables);
        bind_variables.push(SQLValue::Integer(max_depth));
        bind_variables.push(SQLValue::Integer(max_depth));
        bind_variables.push(SQLValue::Integer(per_root_cap));

        let spec = RawSqlQuerySpec {
            sql: format!(
                r#"
WITH RECURSIVE
root_objects AS (
    SELECT unnest({root_array_sql}) AS root_object_id
),
valid_namespaces AS (
    SELECT unnest({namespace_array_sql}) AS namespace_id
),
object_edges AS (
    SELECT from_hubuum_object_id AS source_object_id, to_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation

    UNION ALL

    SELECT to_hubuum_object_id AS source_object_id, from_hubuum_object_id AS target_object_id
    FROM hubuumobject_relation
),
graph_walk AS (
    SELECT
        root_objects.root_object_id,
        object_edges.target_object_id AS descendant_object_id,
        1 AS depth,
        ARRAY[root_objects.root_object_id, object_edges.target_object_id] AS path
    FROM root_objects
    JOIN object_edges
      ON object_edges.source_object_id = root_objects.root_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE ? >= 1
      AND target_object.namespace_id IN (SELECT namespace_id FROM valid_namespaces)

    UNION ALL

    SELECT
        graph_walk.root_object_id,
        object_edges.target_object_id AS descendant_object_id,
        graph_walk.depth + 1,
        graph_walk.path || object_edges.target_object_id
    FROM graph_walk
    JOIN object_edges
      ON object_edges.source_object_id = graph_walk.descendant_object_id
    JOIN hubuumobject target_object
      ON target_object.id = object_edges.target_object_id
    WHERE NOT (object_edges.target_object_id = ANY(graph_walk.path))
      AND graph_walk.depth < ?
      AND target_object.namespace_id IN (SELECT namespace_id FROM valid_namespaces)
),
deduped_walk AS (
    SELECT DISTINCT ON (root_object_id, descendant_object_id)
        root_object_id,
        descendant_object_id,
        depth,
        path
    FROM graph_walk
    ORDER BY root_object_id ASC, descendant_object_id ASC, depth ASC, path ASC
),
ranked_walk AS (
    SELECT
        deduped_walk.*,
        row_number() OVER (
            PARTITION BY root_object_id
            ORDER BY descendant_object_id ASC, depth ASC, path ASC
        ) AS related_rank
    FROM deduped_walk
)
SELECT
    ranked_walk.root_object_id,
    target_object.id AS descendant_object_id,
    ranked_walk.depth,
    ranked_walk.path,
    target_object.name AS descendant_name,
    target_object.namespace_id AS descendant_namespace_id,
    target_object.hubuum_class_id AS descendant_class_id,
    target_object.description AS descendant_description,
    target_object.data AS descendant_data,
    target_object.created_at AS descendant_created_at,
    target_object.updated_at AS descendant_updated_at
FROM ranked_walk
JOIN hubuumobject target_object
  ON target_object.id = ranked_walk.descendant_object_id
WHERE ranked_walk.related_rank <= ?
  AND target_object.namespace_id IN (SELECT namespace_id FROM valid_namespaces)
ORDER BY ranked_walk.root_object_id ASC, ranked_walk.related_rank ASC
"#
            ),
            bind_variables,
        };

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching batched bidirectionally related objects",
            root_object_count = root_object_ids.len(),
            max_depth = max_depth,
            per_root_cap = per_root_cap,
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching batched bidirectionally related objects");

        with_connection(pool, |conn| {
            query.get_results::<RelatedObjectForRootRow>(conn)
        })
    }
```

Note on bind order — the `?` placeholders are consumed left-to-right across the whole SQL string: (1) root ids from `root_array_sql`, (2) namespace ids from `namespace_array_sql`, (3) `max_depth` for base `WHERE ? >= 1`, (4) `max_depth` for recursive `depth < ?`, (5) `per_root_cap` for `related_rank <= ?`. `valid_namespaces` is referenced by sub-select everywhere so the namespace binds appear exactly once. This matches the order the binds are pushed above.

`RelatedObjectForRootRow` is in scope via `use super::*;` at the top of the file (it re-exports `crate::models::relation::*`). If the compiler reports it unresolved, add `use crate::models::RelatedObjectForRootRow;`.

- [ ] **Step 2: Add the `User`-trait wrapper**

In `src/models/traits/user.rs`, after `related_objects_for_roots` (line ~240), add:

```rust
    async fn bidirectionally_related_objects_for_roots<C>(
        &self,
        backend: &C,
        root_object_ids: &[i32],
        max_depth: i32,
        per_root_cap: i32,
    ) -> Result<Vec<RelatedObjectForRootRow>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        self.bidirectionally_related_objects_for_roots_from_backend(
            backend.db_pool(),
            root_object_ids,
            max_depth,
            per_root_cap,
        )
        .await
    }
```

Add `RelatedObjectForRootRow` to the `use crate::models::{...}` import block (line 4-7) in `src/models/traits/user.rs`.

- [ ] **Step 3: Build**

Run: `source .env && ./run_tests.sh --no-run`
Expected: compiles. (Behavior is exercised by the Task B6 integration test.)

- [ ] **Step 4: Commit**

```bash
git add src/db/traits/user/search.rs src/models/traits/user.rs
git commit -m "Add batched bidirectional related-objects query keyed by root

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task B3: `take_related_within_budget` helper + unit tests

**Files:**
- Modify: `src/api/v1/handlers/reports.rs` (add fn near `HydrationBudget`, ~line 170)
- Test: `src/api/v1/handlers/reports.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing unit tests**

Add to `mod tests` in `src/api/v1/handlers/reports.rs`. Add `take_related_within_budget` to the `use super::{...}` list and bring in `HubuumObjectWithPath`.

```rust
fn test_object_with_path(id: i32) -> crate::models::HubuumObjectWithPath {
    crate::models::HubuumObjectWithPath {
        id,
        name: format!("object-{id}"),
        namespace_id: 1,
        hubuum_class_id: 1,
        data: serde_json::json!({}),
        description: String::new(),
        created_at: test_timestamp(),
        updated_at: test_timestamp(),
        path: vec![id],
    }
}

#[test]
fn take_related_within_budget_allows_within_capacity() {
    let mut budget = HydrationBudget::new(5);
    budget.count_object().unwrap(); // hydrated=1, remaining=4, cap=3
    let kept = take_related_within_budget(
        &budget,
        vec![test_object_with_path(10), test_object_with_path(11)],
    )
    .unwrap();
    assert_eq!(kept.len(), 2);
}

#[test]
fn take_related_within_budget_errors_when_second_root_exceeds_remaining() {
    let mut budget = HydrationBudget::new(5);
    // Simulate the first root consuming three objects.
    budget.count_object().unwrap();
    budget.count_object().unwrap();
    budget.count_object().unwrap(); // hydrated=3, remaining=2, cap=1
    let err = take_related_within_budget(
        &budget,
        vec![
            test_object_with_path(10),
            test_object_with_path(11),
            test_object_with_path(12),
        ],
    )
    .unwrap_err();
    match err {
        ApiError::BadRequest(message) => assert_eq!(
            message,
            "Hydrated template object limit exceeded (2 related objects > 1 remaining related capacity)"
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn take_related_within_budget_errors_when_exhausted() {
    let mut budget = HydrationBudget::new(2);
    budget.count_object().unwrap();
    budget.count_object().unwrap(); // hydrated=2, remaining=0
    let err = take_related_within_budget(&budget, vec![test_object_with_path(10)]).unwrap_err();
    match err {
        ApiError::BadRequest(message) => {
            assert_eq!(message, "Hydrated template object limit exceeded (2 >= 2)")
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
```

(`ApiError` is already imported in `reports.rs`; reference it via `super::` if `mod tests` doesn't re-import it — match how existing tests reference error types. `test_timestamp()` already exists in `mod tests`.)

- [ ] **Step 2: Run, verify fail to compile**

Run: `source .env && ./run_tests.sh take_related_within_budget`
Expected: FAIL — function not found.

- [ ] **Step 3: Implement the helper**

Insert after the `impl HydrationBudget { ... }` block (after ~line 170) in `src/api/v1/handlers/reports.rs`:

```rust
// Reproduces the per-root capacity check the old per-root query path applied:
// `remaining_related_capacity()` reserves one slot for the root, the query fetched
// `cap + 1` rows, and a root over `cap` errored with the fetched count (`cap + 1`).
// Roots are processed in `items` order so the shared budget shrinks exactly as before.
fn take_related_within_budget(
    budget: &HydrationBudget,
    mut related: Vec<HubuumObjectWithPath>,
) -> Result<Vec<HubuumObjectWithPath>, ApiError> {
    let max_related_objects = budget.remaining_related_capacity()?;
    related.truncate(max_related_objects.saturating_add(1));
    if related.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related.len(),
            max_related_objects
        )));
    }
    Ok(related)
}
```

- [ ] **Step 4: Run, verify pass**

Run: `source .env && ./run_tests.sh take_related_within_budget`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/api/v1/handlers/reports.rs
git commit -m "Add per-root hydration budget helper preserving error semantics

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task B4: Shared class metadata prefetch + pure neighborhood building

**Files:**
- Modify: `src/api/v1/handlers/reports.rs` — add `HydrationClassMetadata`, `load_hydration_class_metadata`, `seed_alias_buckets`; change `build_object_neighborhood` to be pure; delete `ensure_class_names` and `seed_alias_buckets_from_class_relations`.

This task is a pure refactor (no behavior change yet); it is validated by the existing suite still passing plus the Task B6 test.

- [ ] **Step 1: Add the metadata type and loader**

Insert near the other hydration helpers (e.g. just before `build_object_neighborhood`, ~line 1272) in `src/api/v1/handlers/reports.rs`:

```rust
struct HydrationClassMetadata {
    class_names: BTreeMap<i32, String>,
    class_relations_by_object_class: BTreeMap<i32, Vec<HubuumClassRelation>>,
}

// One-shot replacement for the per-root ensure_class_names + seed_alias DB work.
// Loads every class relation touching any object class once (sorted by id so the
// normalized-pair last-write-wins is deterministic), and primes class names for both
// object classes AND every relation endpoint class (the adjacent class name is needed
// by relation_alias_for_viewer even when no object of that class is in a neighborhood).
async fn load_hydration_class_metadata(
    pool: &DbPool,
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
) -> Result<HydrationClassMetadata, ApiError> {
    let mut object_class_ids = objects_by_id
        .values()
        .map(|object| object.hubuum_class_id)
        .collect::<Vec<_>>();
    object_class_ids.sort_unstable();
    object_class_ids.dedup();

    let mut class_relations = load_class_relations_touching_classes(pool, &object_class_ids).await?;
    class_relations.sort_by_key(|relation| relation.id);

    let mut class_relations_by_object_class = BTreeMap::<i32, Vec<HubuumClassRelation>>::new();
    let mut name_ids = object_class_ids.clone();
    for relation in &class_relations {
        name_ids.push(relation.from_hubuum_class_id);
        name_ids.push(relation.to_hubuum_class_id);
        if object_class_ids
            .binary_search(&relation.from_hubuum_class_id)
            .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.from_hubuum_class_id)
                .or_default()
                .push(relation.clone());
        }
        if relation.to_hubuum_class_id != relation.from_hubuum_class_id
            && object_class_ids
                .binary_search(&relation.to_hubuum_class_id)
                .is_ok()
        {
            class_relations_by_object_class
                .entry(relation.to_hubuum_class_id)
                .or_default()
                .push(relation.clone());
        }
    }

    let mut class_names = BTreeMap::new();
    ensure_class_name_ids(pool, &name_ids, &mut class_names).await?;

    Ok(HydrationClassMetadata {
        class_names,
        class_relations_by_object_class,
    })
}
```

- [ ] **Step 2: Add the pure `seed_alias_buckets`**

This is the pure half of the old `seed_alias_buckets_from_class_relations` (its DB load + name prime now live in `load_hydration_class_metadata`). Insert after `load_hydration_class_metadata`:

```rust
fn seed_alias_buckets(
    objects_by_id: &BTreeMap<i32, HubuumObjectWithPath>,
    aliases_by_object_id: &mut BTreeMap<i32, BTreeMap<String, Vec<i32>>>,
    alias_owners: &mut BTreeMap<i32, BTreeMap<String, i32>>,
    class_relations_by_pair: &mut BTreeMap<(i32, i32), HubuumClassRelation>,
    class_relations_by_object_class: &BTreeMap<i32, Vec<HubuumClassRelation>>,
    class_names: &BTreeMap<i32, String>,
) -> Result<(), ApiError> {
    for object in objects_by_id.values() {
        let Some(class_relations) = class_relations_by_object_class.get(&object.hubuum_class_id)
        else {
            continue;
        };
        for relation in class_relations {
            class_relations_by_pair.insert(
                relation_pair_key(relation.from_hubuum_class_id, relation.to_hubuum_class_id),
                relation.clone(),
            );
            let adjacent_class_id = if relation.from_hubuum_class_id == object.hubuum_class_id {
                relation.to_hubuum_class_id
            } else {
                relation.from_hubuum_class_id
            };

            let alias = relation_alias_for_viewer(
                relation,
                object.hubuum_class_id,
                adjacent_class_id,
                class_names,
            )?;
            let alias_owner_map = alias_owners.get_mut(&object.id).ok_or_else(|| {
                ApiError::InternalServerError("Missing alias ownership state".to_string())
            })?;
            if let Some(existing_class_id) = alias_owner_map.get(&alias)
                && *existing_class_id != adjacent_class_id
            {
                return Err(ApiError::BadRequest(format!(
                    "Relation alias collision for object '{}' on alias '{}'",
                    object.name, alias
                )));
            }
            alias_owner_map.insert(alias.clone(), adjacent_class_id);
            aliases_by_object_id
                .get_mut(&object.id)
                .ok_or_else(|| {
                    ApiError::InternalServerError("Missing alias grouping state".to_string())
                })?
                .entry(alias)
                .or_default();
        }
    }

    Ok(())
}
```

- [ ] **Step 3: Make `build_object_neighborhood` pure**

Replace the whole `build_object_neighborhood` function (lines 1272-1348) with this version — no `pool`, no `&mut class_names`; it takes the shared metadata and reads names from it:

```rust
fn build_object_neighborhood(
    root: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    relations: Vec<HubuumObjectRelation>,
    class_metadata: &HydrationClassMetadata,
) -> Result<ObjectNeighborhood, ApiError> {
    let mut objects_by_id = BTreeMap::new();
    objects_by_id.insert(root.id, root);
    for object in related_objects {
        objects_by_id.insert(object.id, object);
    }

    let class_names = &class_metadata.class_names;

    let mut aliases_by_object_id = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, Vec<i32>>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut alias_owners = objects_by_id
        .keys()
        .map(|object_id| (*object_id, BTreeMap::<String, i32>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut class_relations_by_pair = BTreeMap::new();

    seed_alias_buckets(
        &objects_by_id,
        &mut aliases_by_object_id,
        &mut alias_owners,
        &mut class_relations_by_pair,
        &class_metadata.class_relations_by_object_class,
        class_names,
    )?;

    for relation in relations {
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            class_names,
            relation.from_hubuum_object_id,
            relation.to_hubuum_object_id,
        )?;
        add_bidirectional_alias_edge(
            &objects_by_id,
            &mut aliases_by_object_id,
            &mut alias_owners,
            &class_relations_by_pair,
            class_names,
            relation.to_hubuum_object_id,
            relation.from_hubuum_object_id,
        )?;
    }

    for alias_map in aliases_by_object_id.values_mut() {
        for ids in alias_map.values_mut() {
            ids.sort_unstable_by(|left, right| {
                let left_object = &objects_by_id[left];
                let right_object = &objects_by_id[right];
                left_object
                    .name
                    .cmp(&right_object.name)
                    .then_with(|| left.cmp(right))
            });
            ids.dedup();
        }
    }

    Ok(ObjectNeighborhood {
        objects_by_id,
        aliases_by_object_id,
        class_relations_by_pair,
        class_names_by_id: class_names.clone(),
    })
}
```

- [ ] **Step 4: Delete the now-unused DB helpers**

Delete `ensure_class_names` (lines 1350-1360) and `seed_alias_buckets_from_class_relations` (lines 1362-1450). Keep `ensure_class_name_ids`, `load_class_names`, and `load_class_relations_touching_classes` (all still used by `load_hydration_class_metadata`).

- [ ] **Step 5: Build (will fail on callers — fixed in Task B5)**

Run: `source .env && ./run_tests.sh --no-run`
Expected: FAIL — `build_object_neighborhood` and `hydrate_related_root`/`hydrate_objects_in_class_root` callers still use old signatures. That is fixed in Task B5; do not commit yet. (If you prefer a clean commit boundary, do Task B5 before building.)

## Task B5: Rewrite the assembly (`build_template_items` + `hydrate_related_root`)

**Files:**
- Modify: `src/api/v1/handlers/reports.rs` — `build_template_items` (ObjectsInClass + RelatedObjects branches), `hydrate_related_root`; delete `hydrate_objects_in_class_root` and `load_related_objects_for_root`.

- [ ] **Step 1: Remove the per-root `class_names` cache**

In `build_template_items`, delete the line `let mut class_names = BTreeMap::new();` (line 1101). Keep `let mut hydration_budget = HydrationBudget::new(max_hydrated_template_objects());`.

- [ ] **Step 2: Rewrite the `ObjectsInClass` branch**

Replace the `ReportScopeKind::ObjectsInClass => { ... }` arm (lines 1105-1127) with:

```rust
        ReportScopeKind::ObjectsInClass => {
            let roots = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObject>)
                .collect::<Result<Vec<_>, _>>()?;
            if roots.is_empty() {
                return Ok((Vec::new(), None));
            }

            let root_ids = roots.iter().map(|root| root.id).collect::<Vec<_>>();
            let per_root_cap =
                i32::try_from(max_hydrated_template_objects()).unwrap_or(i32::MAX);
            let related_rows = user
                .bidirectionally_related_objects_for_roots(
                    pool,
                    &root_ids,
                    relation_hydration.depth_limit,
                    per_root_cap,
                )
                .await?;

            // Descendants grouped per root, preserving the query's per-root ordering.
            let mut related_by_root: BTreeMap<i32, Vec<HubuumObjectWithPath>> =
                root_ids.iter().map(|id| (*id, Vec::new())).collect();
            for row in &related_rows {
                if let Some(list) = related_by_root.get_mut(&row.root_object_id) {
                    list.push(row.to_descendant_object_with_path());
                }
            }

            // One relations fetch over the union of all roots + descendants.
            let mut all_object_ids = root_ids.clone();
            for row in &related_rows {
                all_object_ids.push(row.descendant_object_id);
            }
            all_object_ids.sort_unstable();
            all_object_ids.dedup();
            let all_relations = user
                .search_object_relations_between_ids(pool, &all_object_ids)
                .await?;

            // One class-metadata fetch over every object in the report.
            let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                all_objects.insert(root_with_path.id, root_with_path);
            }
            for row in &related_rows {
                let object = row.to_descendant_object_with_path();
                all_objects.entry(object.id).or_insert(object);
            }
            let class_metadata = load_hydration_class_metadata(pool, &all_objects).await?;

            let mut hydrated_items = Vec::with_capacity(roots.len());
            for root in &roots {
                let root_with_path = object_with_root_path(root);
                let related = related_by_root.remove(&root.id).unwrap_or_default();
                let related = take_related_within_budget(&hydration_budget, related)?;

                let mut neighborhood_ids =
                    related.iter().map(|object| object.id).collect::<std::collections::HashSet<_>>();
                neighborhood_ids.insert(root.id);
                let relations = all_relations
                    .iter()
                    .filter(|relation| {
                        neighborhood_ids.contains(&relation.from_hubuum_object_id)
                            && neighborhood_ids.contains(&relation.to_hubuum_object_id)
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                let neighborhood = build_object_neighborhood(
                    root_with_path.clone(),
                    related,
                    relations,
                    &class_metadata,
                )?;
                let hydrated = hydrate_object(
                    &neighborhood,
                    root_with_path.id,
                    vec![root_with_path.id],
                    relation_hydration.depth_limit,
                    &mut hydration_budget,
                )?;
                hydrated_items.push(serde_json::to_value(hydrated)?);
            }

            Ok((hydrated_items, None))
        }
```

- [ ] **Step 3: Rewrite the `RelatedObjects` branch call + `hydrate_related_root`**

The `RelatedObjects` branch (lines 1128-1150) calls `hydrate_related_root(pool, user, source, related_objects, depth, &mut class_names, &mut hydration_budget)`. Change that call to drop `&mut class_names`:

```rust
        ReportScopeKind::RelatedObjects => {
            let source_object = HubuumObjectID(runtime.report.scope.object_id_required()?)
                .instance(pool)
                .await?;
            let source = object_with_root_path(&source_object);
            let related_objects = items
                .iter()
                .cloned()
                .map(serde_json::from_value::<HubuumObjectWithPath>)
                .collect::<Result<Vec<_>, _>>()?;
            let hydrated = hydrate_related_root(
                pool,
                user,
                source,
                related_objects,
                relation_hydration.depth_limit,
                &mut hydration_budget,
            )
            .await?;
            let source = serde_json::to_value(&hydrated)?;
            Ok((vec![source.clone()], Some(source)))
        }
```

Then replace `hydrate_related_root` (lines 1195-1234) with this version (drops `class_names` param; prefetches metadata; new `build_object_neighborhood` signature):

```rust
async fn hydrate_related_root(
    pool: &DbPool,
    user: &crate::models::User,
    source: HubuumObjectWithPath,
    related_objects: Vec<HubuumObjectWithPath>,
    depth_limit: i32,
    hydration_budget: &mut HydrationBudget,
) -> Result<HydratedTemplateObject, ApiError> {
    let max_related_objects = hydration_budget.remaining_related_capacity()?;
    if related_objects.len() > max_related_objects {
        return Err(ApiError::BadRequest(format!(
            "Hydrated template object limit exceeded ({} related objects > {} remaining related capacity)",
            related_objects.len(),
            max_related_objects
        )));
    }

    let object_ids = std::iter::once(source.id)
        .chain(related_objects.iter().map(|object| object.id))
        .collect::<Vec<_>>();
    let relations = user
        .search_object_relations_between_ids(pool, &object_ids)
        .await?;

    let mut all_objects = BTreeMap::<i32, HubuumObjectWithPath>::new();
    all_objects.insert(source.id, source.clone());
    for object in &related_objects {
        all_objects.entry(object.id).or_insert_with(|| object.clone());
    }
    let class_metadata = load_hydration_class_metadata(pool, &all_objects).await?;

    let neighborhood =
        build_object_neighborhood(source.clone(), related_objects, relations, &class_metadata)?;
    hydrate_object(
        &neighborhood,
        source.id,
        vec![source.id],
        depth_limit,
        hydration_budget,
    )
}
```

- [ ] **Step 4: Delete the dead per-root helpers**

Delete `hydrate_objects_in_class_root` (lines 1155-1193) and `load_related_objects_for_root` (lines 1236-1270). Confirm no remaining references:

Run: `grep -rn "hydrate_objects_in_class_root\|load_related_objects_for_root" src/`
Expected: no matches.

- [ ] **Step 5: Build**

Run: `source .env && ./run_tests.sh --no-run`
Expected: compiles. Fix any unused-import warnings (e.g. if `BTreeMap` was only used in deleted code — it is still used).

- [ ] **Step 6: Run the full report suite (regression guard)**

Run: `source .env && ./run_tests.sh report`
Expected: all existing report tests PASS — this proves the batched path produces byte-identical output for the covered scenarios (`test_report_relation_aliases_and_paths_are_available_in_templates`, `test_report_events_include_running_steps_and_related_output`, etc.).

- [ ] **Step 7: Commit**

```bash
git add src/api/v1/handlers/reports.rs
git commit -m "Batch ObjectsInClass relation hydration into shared prefetches

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task B6: Integration test — multi-root batched keying, no cross-root leakage

**Files:**
- Test: `src/tests/api/v1/reports.rs`

- [ ] **Step 1: Add import**

Add `ReportRelationContext` to the `use crate::models::{...}` list in `src/tests/api/v1/reports.rs`.

- [ ] **Step 2: Write the test**

Add to `mod tests`:

```rust
#[rstest]
#[actix_web::test]
async fn test_report_objects_in_class_hydration_keys_per_root(
    #[future(awt)] test_context: TestContext,
) {
    let context = test_context;
    let namespace = context.namespace_fixture("report_multiroot_hydration").await;
    let host_class = create_named_class(
        &context.pool,
        namespace.namespace.id,
        &context.scoped_name("MultiHost"),
    )
    .await;
    let room_class = create_named_class(
        &context.pool,
        namespace.namespace.id,
        &context.scoped_name("MultiRoom"),
    )
    .await;

    let make_object = |name: &str, class_id: i32| NewHubuumObject {
        name: name.to_string(),
        description: "obj".to_string(),
        namespace_id: namespace.namespace.id,
        hubuum_class_id: class_id,
        data: serde_json::json!({}),
    };

    let host_a = make_object("host-a", host_class.id)
        .save(&context.pool)
        .await
        .unwrap();
    let host_b = make_object("host-b", host_class.id)
        .save(&context.pool)
        .await
        .unwrap();
    let room_a = make_object("room-a", room_class.id)
        .save(&context.pool)
        .await
        .unwrap();
    let room_b = make_object("room-b", room_class.id)
        .save(&context.pool)
        .await
        .unwrap();

    let host_room_relation = NewHubuumClassRelation {
        from_hubuum_class_id: host_class.id,
        to_hubuum_class_id: room_class.id,
        forward_template_alias: Some("rooms".to_string()),
        reverse_template_alias: Some("hosts".to_string()),
    }
    .save(&context.pool)
    .await
    .unwrap();

    let _ = create_object_relation(&context.pool, host_a.id, room_a.id, host_room_relation.id).await;
    let _ = create_object_relation(&context.pool, host_b.id, room_b.id, host_room_relation.id).await;

    let template_id = create_template(
        &context.pool,
        namespace.namespace.id,
        "multiroot-template",
        ReportContentType::TextPlain,
        "{% for host in items %}{{ host.name }}:{% for room in host.related.rooms %}{{ room.name }},{% endfor %};{% endfor %}",
    )
    .await;

    let body = ReportRequest {
        scope: ReportScope {
            kind: ReportScopeKind::ObjectsInClass,
            class_id: Some(host_class.id),
            object_id: None,
        },
        query: Some("sort=name".to_string()),
        output: Some(crate::models::ReportOutputRequest {
            template_id: Some(template_id),
        }),
        missing_data_policy: None,
        limits: None,
        include: None,
        relation_context: Some(ReportRelationContext { depth: Some(1) }),
    };

    let resp = post_request_with_headers(
        &context.pool,
        &context.admin_token,
        REPORTS_ENDPOINT,
        &body,
        vec![],
    )
    .await;
    let resp = assert_response_status(resp, StatusCode::ACCEPTED).await;
    let task: TaskResponse = test::read_body_json(resp).await;
    let _ = wait_for_task(&context, task.id, &[TaskStatus::Succeeded]).await;

    let output = get_request(
        &context.pool,
        &context.admin_token,
        &format!("/api/v1/reports/{}/output", task.id),
    )
    .await;
    let output = assert_response_status(output, StatusCode::OK).await;
    let rendered = String::from_utf8(test::read_body(output).await.to_vec()).unwrap();

    // Each host shows only its own room — no cross-root leakage from the batched fetch.
    assert_eq!(rendered, "host-a:room-a,;host-b:room-b,;");

    namespace.cleanup().await.unwrap();
}
```

Note: verify `context.namespace_fixture(...)` and `context.scoped_name(...)` exist (they are used by `test_report_relation_aliases_and_paths_are_available_in_templates`). If the `related.rooms` alias key differs from the template assumption, run the test once and adjust the template/assert to the observed structure — the alias is the class relation's `forward_template_alias` ("rooms"), matching the existing aliases test.

- [ ] **Step 3: Run the test**

Run: `source .env && ./run_tests.sh test_report_objects_in_class_hydration_keys_per_root`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/tests/api/v1/reports.rs
git commit -m "Test multi-root ObjectsInClass hydration keys per root

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

## Task B7: Full suite + lint

- [ ] **Step 1: Run the entire test suite**

Run: `source .env && ./run_tests.sh`
Expected: all PASS.

- [ ] **Step 2: Clippy + fmt (match project convention)**

Run: `cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: clean. Fix anything reported (e.g. unused imports left by deletions).

- [ ] **Step 3: Commit any lint fixes**

```bash
git add -A
git commit -m "Lint and format cleanup for issue #48

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Final verification against the spec

- N+1 removed: ObjectsInClass issues 3 batched queries (related-for-roots, relations-between-ids, class metadata) + 0 per root, instead of ~3N. ✅ (Task B2, B5)
- Budget error messages byte-identical, processed in `items` order. ✅ (Task B3 unit tests)
- Deterministic ordering of per-root descendants and class-relation pair selection. ✅ (Task B2 query order; B4 `sort_by_key(id)`)
- Class-name prime covers object classes AND relation endpoints. ✅ (Task B4)
- Text/html/csv render is stream-bounded; `enforce_text_output_limit` removed; message is the JSON-style `(> N)`. ✅ (Task A1-A3)
- Tests: multi-root keying, budget drift (unit), budget exhausted (unit), streamed text 413. ✅
