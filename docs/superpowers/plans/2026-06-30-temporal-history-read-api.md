# Temporal History Read API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the history captured by Plan 1 through a read API — per-entity version listing (cursor-paginated) and point-in-time (`as_of`) snapshot retrieval — for the core domain resources, with each change attributed to its (pseudonymous) actor.

**Architecture:** For each versioned table, a `Queryable` history struct implements the existing `CursorPaginated` + `CursorSqlMapping` traits (via two shared macros), so the existing pagination machinery (`apply_query_options!`, `finalize_page`, `pagination_headers`) drives the list endpoint. Two endpoints per resource: `GET /<resource>/{id}/history` (list versions, newest-first, cursor-paginated) and `GET /<resource>/{id}/history/as-of?at=<rfc3339>` (snapshot valid at an instant). Both load the current entity and run the same Read permission check as the resource's detail GET; a deleted entity yields 404. Actor ids are resolved to usernames in one batched query.

**Tech Stack:** Rust (edition 2024), Diesel 2 (Postgres, r2d2), actix-web 4.13, chrono, PostgreSQL. Builds on Plan 1 (history tables + trigger + actor capture), already merged on branch `temporal-history`.

## Global Constraints

- This is **Plan 2 of 2** (read side). Plan 1 (capture/actor/anonymization) is already merged.
- **Decisions (locked):** (1) full `CursorPaginated`/`CursorSqlMapping` reuse for pagination — supports `?sort=`; (2) point-in-time is a dedicated `GET /<resource>/{id}/history/as-of?at=<rfc3339>` sub-endpoint (not a query param on the detail GET); (3) history endpoints require the same Read permission as the resource's detail GET, checked via the current instance — a deleted entity returns 404 (auditing deleted-entity history is out of scope).
- History list ordering default: `valid_from DESC`, tie-broken by `history_id DESC` (history_id is unique → total order). Sortable fields: `valid_from`, `history_id`. Arbitrary field filters are NOT supported on history (pagination only).
- History Queryable structs: `valid_from -> chrono::DateTime<chrono::Utc>`, `valid_to -> Option<chrono::DateTime<chrono::Utc>>` (columns are `Timestamptz`); `created_at`/`updated_at -> chrono::NaiveDateTime` (columns are `Timestamp`); `op -> String`; `actor_id -> Option<i32>`; `history_id -> i64`. Field order MUST match `src/schema.rs` exactly.
- **Scope:** 5 resources — `hubuumclass`, `hubuumobject`, `namespaces`, `report_templates`, `remote_targets`. The 2 relation tables (`hubuumclass_relation`, `hubuumobject_relation`) are deferred to a short follow-up (Plan 2b) because their permission derives from related entities across possibly multiple namespaces.
- Env: clean local Postgres is up (docker `hubuum-temporal-testdb`, localhost:55432); before every `cargo`/`diesel` command run `set -a; source .env; set +a && <command>` (or set `DATABASE_URL`/`HUBUUM_DATABASE_URL` to the local DB). If a class/namespace test hits a spurious `Unique constraint not met` from leaked shared-DB rows, clear with `docker exec hubuum-temporal-testdb psql -U hubuum -d hubuum_rust -c "DELETE FROM hubuumclass WHERE name LIKE '%test%';"`. For a definitive green run use `diesel database reset` then `cargo test --lib`.
- Final gate: `cargo clippy --all-targets -- -D warnings` clean and `cargo test --lib` green on a clean DB. TDD, frequent commits.

---

### Task 1: Shared history-read infrastructure

**Files:**
- Modify: `src/models/search.rs` (add `ValidFrom`, `HistoryId` to the `filter_fields!` macro)
- Create: `src/api/v1/handlers/history.rs` (shared helpers + macros + response wrapper)
- Modify: `src/api/v1/handlers/mod.rs` (declare `pub mod history;`)
- Test: in `src/api/v1/handlers/history.rs` (`#[cfg(test)]`)

**Interfaces:**
- Produces:
  - `FilterField::ValidFrom` ("valid_from"), `FilterField::HistoryId` ("history_id").
  - `HistoryResponse<T: Serialize>` (serde-flattens `entry: T` and adds `actor_username: Option<String>`).
  - `pub async fn resolve_actor_usernames(pool: &DbPool, actor_ids: Vec<i32>) -> Result<std::collections::HashMap<i32, String>, ApiError>`.
  - `pub fn parse_as_of(query_string: &str) -> Result<chrono::DateTime<chrono::Utc>, ApiError>` (reads required `at=<rfc3339>`).
  - `macro_rules! impl_history_pagination` and `macro_rules! history_db_fns` (used by Tasks 2-5).

- [ ] **Step 1: Add the FilterField variants**

In `src/models/search.rs`, inside the `filter_fields!(...)` invocation (the list ending with `(Path, "path"),` around line 1845), add two entries:

```rust
    (ValidFrom, "valid_from"),
    (HistoryId, "history_id"),
```

- [ ] **Step 2: Run the build to confirm the enum compiles**

Run: `set -a; source .env; set +a && cargo build 2>&1 | tail -5`
Expected: compiles (the `filter_fields!` macro generates the new variants + their `Display`/parse arms).

- [ ] **Step 3: Write the failing tests for the helpers**

Create `src/api/v1/handlers/history.rs` with the test module first:

```rust
//! Shared building blocks for the per-resource history read API:
//! a response wrapper that adds the actor's username, actor-id resolution,
//! `as_of` query parsing, and macros that implement cursor pagination and the
//! DB fetch functions for each history table.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::Serialize;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;

/// A serialized history row plus the resolved username of its actor (if any).
#[derive(Serialize)]
pub struct HistoryResponse<T: Serialize> {
    #[serde(flatten)]
    pub entry: T,
    pub actor_username: Option<String>,
}

/// Parse the required `at=<rfc3339>` query parameter for the as-of endpoint.
pub fn parse_as_of(query_string: &str) -> Result<DateTime<Utc>, ApiError> {
    let at = url::form_urlencoded::parse(query_string.as_bytes())
        .find(|(k, _)| k == "at")
        .map(|(_, v)| v.into_owned())
        .ok_or_else(|| ApiError::BadRequest("missing required 'at' parameter".into()))?;
    DateTime::parse_from_rfc3339(&at)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| ApiError::BadRequest(format!("invalid rfc3339 timestamp: {at}")))
}

/// Batch-resolve a set of actor ids to usernames (anonymized users keep their
/// tombstoned username; ids with no matching user are simply absent).
pub async fn resolve_actor_usernames(
    pool: &DbPool,
    mut actor_ids: Vec<i32>,
) -> Result<HashMap<i32, String>, ApiError> {
    use crate::schema::users::dsl::{id, username, users};
    actor_ids.sort_unstable();
    actor_ids.dedup();
    if actor_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(i32, String)> = with_connection(pool, |conn| {
        users
            .filter(id.eq_any(&actor_ids))
            .select((id, username))
            .load(conn)
    })?;
    Ok(rows.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_as_of_reads_rfc3339() {
        let dt = parse_as_of("at=2026-01-02T03:04:05Z").unwrap();
        assert_eq!(dt, DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z").unwrap());
    }

    #[test]
    fn parse_as_of_requires_param() {
        assert!(matches!(parse_as_of("foo=bar"), Err(ApiError::BadRequest(_))));
    }

    #[test]
    fn parse_as_of_rejects_garbage() {
        assert!(matches!(parse_as_of("at=not-a-date"), Err(ApiError::BadRequest(_))));
    }
}
```

> Note: this uses the `url` crate's `form_urlencoded` for query parsing. If `url` is not already a dependency, instead parse with the project's existing query helper — check `src/models/search.rs` for a public splitter; if none, add `url = "2"` to `Cargo.toml` (it is commonly already present transitively — verify with `cargo tree -i url`). Keep the parse behavior identical to the tests.

- [ ] **Step 4: Declare the module**

In `src/api/v1/handlers/mod.rs`, add:

```rust
pub mod history;
```

- [ ] **Step 5: Run the helper tests (expect compile/pass)**

Run: `set -a; source .env; set +a && cargo test --lib api::v1::handlers::history::tests -- --nocapture`
Expected: 3 tests PASS.

- [ ] **Step 6: Add the two macros**

Append to `src/api/v1/handlers/history.rs` (above the `#[cfg(test)]` module):

```rust
/// Implement `CursorPaginated` + `CursorSqlMapping` for a history Queryable
/// type. `$table` is the history table name as it appears in SQL (used to
/// build fully-qualified column references for the keyset/ORDER BY clauses).
#[macro_export]
macro_rules! impl_history_pagination {
    ($ty:ty, $table:literal) => {
        impl $crate::traits::CursorPaginated for $ty {
            fn supports_sort(field: &$crate::models::search::FilterField) -> bool {
                matches!(
                    field,
                    $crate::models::search::FilterField::ValidFrom
                        | $crate::models::search::FilterField::HistoryId
                )
            }

            fn cursor_value(
                &self,
                field: &$crate::models::search::FilterField,
            ) -> Result<$crate::traits::CursorValue, $crate::errors::ApiError> {
                Ok(match field {
                    $crate::models::search::FilterField::ValidFrom => {
                        $crate::traits::CursorValue::DateTime(self.valid_from.naive_utc())
                    }
                    $crate::models::search::FilterField::HistoryId => {
                        $crate::traits::CursorValue::Integer(self.history_id)
                    }
                    other => {
                        return Err($crate::errors::ApiError::BadRequest(format!(
                            "Field '{}' is not orderable for history",
                            other
                        )))
                    }
                })
            }

            fn default_sort() -> Vec<$crate::models::search::SortParam> {
                vec![$crate::models::search::SortParam {
                    field: $crate::models::search::FilterField::ValidFrom,
                    descending: true,
                }]
            }

            fn tie_breaker_sort() -> Vec<$crate::models::search::SortParam> {
                vec![$crate::models::search::SortParam {
                    field: $crate::models::search::FilterField::HistoryId,
                    descending: true,
                }]
            }
        }

        impl $crate::traits::CursorSqlMapping for $ty {
            fn sql_field(
                field: &$crate::models::search::FilterField,
            ) -> Result<$crate::traits::CursorSqlField, $crate::errors::ApiError> {
                Ok(match field {
                    $crate::models::search::FilterField::ValidFrom => {
                        $crate::traits::CursorSqlField {
                            column: concat!($table, ".valid_from"),
                            sql_type: $crate::traits::CursorSqlType::DateTime,
                            nullable: false,
                        }
                    }
                    $crate::models::search::FilterField::HistoryId => {
                        $crate::traits::CursorSqlField {
                            column: concat!($table, ".history_id"),
                            sql_type: $crate::traits::CursorSqlType::Integer,
                            nullable: false,
                        }
                    }
                    other => {
                        return Err($crate::errors::ApiError::BadRequest(format!(
                            "Field '{}' is not orderable for history",
                            other
                        )))
                    }
                })
            }
        }
    };
}

/// Generate the two DB fetch functions for a history table:
/// - `$paginate_fn(entity_id, pool, &QueryOptions) -> (Vec<$ty>, i64)` — a page
///   of versions for one entity plus the total version count.
/// - `$as_of_fn(entity_id, at, pool) -> Option<$ty>` — the version valid at `at`.
/// `$schema` is the diesel schema module path, e.g. `crate::schema::hubuumclass_history`.
#[macro_export]
macro_rules! history_db_fns {
    ($paginate_fn:ident, $as_of_fn:ident, $schema:path, $ty:ty) => {
        pub async fn $paginate_fn(
            entity_id: i32,
            pool: &$crate::db::DbPool,
            query_options: &$crate::models::search::QueryOptions,
        ) -> Result<(Vec<$ty>, i64), $crate::errors::ApiError> {
            use $schema::dsl::*;
            let total = $crate::db::with_connection(pool, |conn| {
                $schema::table
                    .filter(id.eq(entity_id))
                    .count()
                    .get_result::<i64>(conn)
            })?;
            let mut query = $schema::table.into_boxed().filter(id.eq(entity_id));
            $crate::apply_query_options!(query, query_options, $ty);
            let items =
                $crate::db::with_connection(pool, |conn| query.load::<$ty>(conn))?;
            Ok((items, total))
        }

        pub async fn $as_of_fn(
            entity_id: i32,
            at: chrono::DateTime<chrono::Utc>,
            pool: &$crate::db::DbPool,
        ) -> Result<Option<$ty>, $crate::errors::ApiError> {
            use $schema::dsl::*;
            $crate::db::with_connection(pool, |conn| {
                $schema::table
                    .into_boxed()
                    .filter(id.eq(entity_id))
                    .filter(valid_from.le(at))
                    .filter(valid_to.is_null().or(valid_to.gt(at)))
                    .order(history_id.desc())
                    .first::<$ty>(conn)
                    .optional()
            })
        }
    };
}
```

> The traits/types referenced (`CursorPaginated`, `CursorValue`, `CursorSqlField`, `CursorSqlType`, `CursorSqlMapping`) are re-exported from `crate::traits` (see `src/models/token.rs` imports). `apply_query_options!` is `#[macro_export]` at crate root (`crate::apply_query_options!`). `FilterField`/`SortParam`/`QueryOptions` live in `crate::models::search`.

- [ ] **Step 7: Build to confirm the macros parse**

Run: `set -a; source .env; set +a && cargo build 2>&1 | tail -5`
Expected: compiles (macros are not yet invoked, so only syntax is checked).

- [ ] **Step 8: Commit**

```bash
git add src/models/search.rs src/api/v1/handlers/history.rs src/api/v1/handlers/mod.rs Cargo.toml
git commit -m "feat(history): shared history-read infra (FilterField, helpers, pagination macros)"
```

---

### Task 2: Class history endpoints (reference implementation)

**Files:**
- Modify: `src/models/class.rs` (add `HubuumClassHistory` + macro invocations)
- Modify: `src/api/v1/handlers/classes.rs` (history DB fns + 2 handlers)
- Modify: `src/api/v1/routes/classes.rs` (register routes)
- Test: `src/tests/temporal/mod.rs` (DB-level) and `src/tests/api/v1/classes.rs` (API-level)

**Interfaces:**
- Consumes: `impl_history_pagination!`, `history_db_fns!`, `HistoryResponse`, `resolve_actor_usernames`, `parse_as_of` (Task 1).
- Produces: `HubuumClassHistory` (Queryable over `hubuumclass_history`); `class_history_paginated_with_total_count`, `class_as_of`; routes `GET /api/v1/classes/{class_id}/history` and `GET /api/v1/classes/{class_id}/history/as-of?at=`.

- [ ] **Step 1: Add the history Queryable struct + trait impls**

In `src/models/class.rs`, add (field order matches `schema.rs` `hubuumclass_history`):

```rust
#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::hubuumclass_history)]
pub struct HubuumClassHistory {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(HubuumClassHistory, "hubuumclass_history");
```

- [ ] **Step 2: Build to confirm the struct loads the table**

Run: `set -a; source .env; set +a && cargo build 2>&1 | tail -8`
Expected: compiles. If diesel complains about `valid_from` type, the column is `Timestamptz` → the field must be `chrono::DateTime<chrono::Utc>` (already specified); a mismatch error means the type was changed — restore `DateTime<Utc>`.

- [ ] **Step 3: Generate the DB fetch functions**

In `src/api/v1/handlers/classes.rs`, near the top-level (module scope, not inside a fn), add:

```rust
crate::history_db_fns!(
    class_history_paginated_with_total_count,
    class_as_of,
    crate::schema::hubuumclass_history,
    crate::models::HubuumClassHistory
);
```

Ensure `HubuumClassHistory` is exported from `crate::models` (add it to the re-export list in `src/models/mod.rs` next to `HubuumClass`).

- [ ] **Step 4: Write the failing API tests**

In `src/tests/api/v1/classes.rs` (inside the `tests` module) add:

```rust
    #[rstest]
    #[actix_web::test]
    async fn test_api_class_history_list_and_as_of(#[future(awt)] test_context: TestContext) {
        use crate::models::UpdateHubuumClass;
        use crate::traits::{CanSave, CanUpdate};

        let context = test_context;
        let ns = context.namespace_fixture("class_history_api").await;

        // Create then update so there are two versions.
        let created = NewHubuumClass {
            name: "class_history_api".to_string(),
            description: "v1".to_string(),
            namespace_id: ns.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
        }
        .save(&context.pool)
        .await
        .unwrap();
        UpdateHubuumClass {
            name: None,
            namespace_id: None,
            json_schema: None,
            validate_schema: None,
            description: Some("v2".to_string()),
        }
        .update(&context.pool, created.id)
        .await
        .unwrap();

        // List history newest-first.
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/history", CLASSES_ENDPOINT, created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: Vec<serde_json::Value> = test::read_body_json(resp).await;
        assert_eq!(body.len(), 2, "expected two versions");
        assert_eq!(body[0]["op"], "U");
        assert_eq!(body[0]["description"], "v2");
        assert_eq!(body[1]["op"], "I");
        assert!(body[0].get("actor_username").is_some(), "actor_username key present");

        // as-of just after the insert (before the update) -> v1.
        let v1_from = body[1]["valid_from"].as_str().unwrap().to_string();
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/{}/history/as-of?at={}", CLASSES_ENDPOINT, created.id, urlencoding::encode(&v1_from)),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let snap: serde_json::Value = test::read_body_json(resp).await;
        assert_eq!(snap["description"], "v1");

        ns.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_web::test]
    async fn test_api_class_history_404_for_missing(#[future(awt)] test_context: TestContext) {
        let context = test_context;
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{}/2147483647/history", CLASSES_ENDPOINT),
        )
        .await;
        assert_response_status(resp, StatusCode::NOT_FOUND).await;
    }
```

> If `urlencoding` is not a dev-dependency, encode inline instead: replace `urlencoding::encode(&v1_from)` with `v1_from.replace('+', "%2B").replace(':', "%3A")` — or simpler, since rfc3339 with `Z` has no `+`, pass `&v1_from` directly (the `:` characters are valid in a query value). Prefer passing `&v1_from` directly if the test reads it back correctly.

- [ ] **Step 5: Run the tests to verify they fail**

Run: `set -a; source .env; set +a && cargo test --lib tests::api::v1::classes::tests::test_api_class_history -- --nocapture`
Expected: FAIL (routes not registered / handlers missing).

- [ ] **Step 6: Add the two handlers**

In `src/api/v1/handlers/classes.rs`, add (ensure imports: `HttpRequest`, `parse_query_parameter`, `prepare_db_pagination`, `paginated_json_mapped_response`, `json_response`, `parse_as_of`, `resolve_actor_usernames`, `HistoryResponse` from `crate::api::v1::handlers::history`):

```rust
#[get("/{class_id}/history")]
async fn get_class_history(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{resolve_actor_usernames, HistoryResponse};

    let user = requestor.user;
    let instance = class_id.into_inner().instance(&pool).await?; // 404 if deleted
    can!(&pool, user, [Permissions::ReadClass], instance);

    let params = parse_query_parameter(req.query_string())?;
    let search_params = prepare_db_pagination::<crate::models::HubuumClassHistory>(&params)?;
    let (rows, total_count) =
        class_history_paginated_with_total_count(instance.id, &pool, &search_params).await?;

    let actor_ids = rows.iter().filter_map(|r| r.actor_id).collect();
    let actor_map = resolve_actor_usernames(&pool, actor_ids).await?;

    paginated_json_mapped_response(rows, total_count, StatusCode::OK, &params, move |rows| {
        rows.into_iter()
            .map(|row| {
                let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
                HistoryResponse { entry: row, actor_username }
            })
            .collect()
    })
}

#[get("/{class_id}/history/as-of")]
async fn get_class_as_of(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    class_id: web::Path<HubuumClassID>,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    use crate::api::v1::handlers::history::{parse_as_of, resolve_actor_usernames, HistoryResponse};

    let user = requestor.user;
    let instance = class_id.into_inner().instance(&pool).await?;
    can!(&pool, user, [Permissions::ReadClass], instance);

    let at = parse_as_of(req.query_string())?;
    let row = class_as_of(instance.id, at, &pool)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no version of class {} at {at}", instance.id)))?;

    let actor_map = resolve_actor_usernames(&pool, row.actor_id.into_iter().collect()).await?;
    let actor_username = row.actor_id.and_then(|aid| actor_map.get(&aid).cloned());
    Ok(json_response(HistoryResponse { entry: row, actor_username }, StatusCode::OK))
}
```

> `prepare_db_pagination`, `parse_query_parameter`, `paginated_json_mapped_response` are already imported in `classes.rs` (used by `get_classes`); add `HttpRequest` if the file doesn't already import it (it does, used by `get_classes`). `.instance(&pool)` comes from the `SelfAccessors`/instance trait already used by `get_class`.

- [ ] **Step 7: Register the routes**

In `src/api/v1/routes/classes.rs`, add to the `config` chain (before the generic `get_class` is fine; actix distinguishes by path depth, but register them explicitly):

```rust
        .service(classes::get_class_history)
        .service(classes::get_class_as_of)
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `set -a; source .env; set +a && cargo test --lib tests::api::v1::classes::tests::test_api_class_history -- --nocapture`
Expected: both PASS. If the as-of `at` equals the update timestamp and returns v2 instead of v1, the test uses `body[1]["valid_from"]` (the insert's timestamp) which is `<` the update; with half-open `[valid_from, valid_to)` the insert row is valid at its own `valid_from`, so v1 is correct.

- [ ] **Step 9: Clippy + commit**

Run: `set -a; source .env; set +a && cargo clippy --all-targets -- -D warnings` (expect clean)

```bash
git add src/models/class.rs src/models/mod.rs src/api/v1/handlers/classes.rs src/api/v1/routes/classes.rs src/tests/api/v1/classes.rs
git commit -m "feat(history): class history list + as-of endpoints"
```

---

### Task 3: Namespace, report-template, and remote-target history endpoints

**Files:**
- Modify: `src/models/namespace.rs`, `src/models/report_template.rs` (or wherever `ReportTemplate` lives), `src/models/remote_target.rs` (add history structs + macros)
- Modify: `src/models/mod.rs` (re-export the three history structs)
- Modify: `src/api/v1/handlers/namespaces.rs`, `.../templates.rs`, `.../remote_targets.rs` (db fns + handlers)
- Modify: `src/api/v1/routes/namespaces.rs`, `.../templates.rs`, `.../remote_targets.rs` (routes)
- Test: `src/tests/api/v1/` for each

**Interfaces:**
- Consumes: Task 1 macros/helpers; the Task 2 pattern.
- Produces: `NamespaceHistory`, `ReportTemplateHistory`, `RemoteTargetHistory`; per-resource `*_history_paginated_with_total_count` / `*_as_of`; routes `GET /api/v1/{namespaces|templates|remote-targets}/{id}/history` and `/history/as-of`.

> This task repeats the Task 2 pattern for three more resources. For EACH resource below, the work is identical in shape: (a) add the history Queryable struct with fields matching `schema.rs` (full lists given), (b) `crate::impl_history_pagination!(Type, "table")`, (c) re-export the type from `src/models/mod.rs`, (d) `crate::history_db_fns!(...)` in the handler module, (e) two handlers mirroring `get_class_history`/`get_class_as_of` but using this resource's ID newtype, `instance()`, and `Permissions::Read*` variant, (f) register the two routes, (g) tests mirroring Task 2's.

- [ ] **Step 1: Confirm each resource's ID newtype, instance type, and Read permission variant**

Run: `set -a; source .env; set +a && grep -nE "ReadNamespace|ReadReportTemplate|ReadRemoteTarget|NamespaceID|ReportTemplateID|RemoteTargetID" src/models/permissions.rs src/models/*.rs | head -40`
Record the exact `Permissions::Read*` variant and `*ID` newtype for each resource. Use these verbatim in the handlers below. (Mirror each resource's existing detail-GET handler — `get_namespace`, the report-template detail GET in `templates.rs`, and the remote-target detail GET in `remote_targets.rs` — for the exact `instance()` + `can!` form.)

- [ ] **Step 2: namespaces — struct + macros**

In `src/models/namespace.rs`:

```rust
#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::namespaces_history)]
pub struct NamespaceHistory {
    pub id: i32,
    pub name: String,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(NamespaceHistory, "namespaces_history");
```

Re-export `NamespaceHistory` from `src/models/mod.rs`.

- [ ] **Step 3: namespaces — db fns + handlers + routes**

In `src/api/v1/handlers/namespaces.rs`:

```rust
crate::history_db_fns!(
    namespace_history_paginated_with_total_count,
    namespace_as_of,
    crate::schema::namespaces_history,
    crate::models::NamespaceHistory
);
```

Add `get_namespace_history` and `get_namespace_as_of` handlers identical to Task 2 Step 6 but with: path `"/{namespace_id}/history"` and `"/{namespace_id}/history/as-of"`, `web::Path<NamespaceID>`, `prepare_db_pagination::<crate::models::NamespaceHistory>`, the `namespace_*` db fns, and the namespace Read permission check copied from the existing `get_namespace` handler in this file. Register both in `src/api/v1/routes/namespaces.rs`.

- [ ] **Step 4: report_templates — struct + macros**

In the report-template model file:

```rust
#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::report_templates_history)]
pub struct ReportTemplateHistory {
    pub id: i32,
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub content_type: String,
    pub template: String,
    pub kind: String,
    pub scope_kind: Option<String>,
    pub class_id: Option<i32>,
    pub default_query: Option<String>,
    pub include: Option<serde_json::Value>,
    pub relation_context: Option<serde_json::Value>,
    pub default_missing_data_policy: Option<String>,
    pub default_limits: Option<serde_json::Value>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(ReportTemplateHistory, "report_templates_history");
```

Re-export from `src/models/mod.rs`.

- [ ] **Step 5: report_templates — db fns + handlers + routes**

In `src/api/v1/handlers/templates.rs`:

```rust
crate::history_db_fns!(
    report_template_history_paginated_with_total_count,
    report_template_as_of,
    crate::schema::report_templates_history,
    crate::models::ReportTemplateHistory
);
```

Add the two handlers (paths under the templates scope: `"/{template_id}/history"` and `"/{template_id}/history/as-of"`) mirroring Task 2, using the template ID newtype, `instance()`, and the Read permission from the existing template detail GET. Register in `src/api/v1/routes/templates.rs`.

- [ ] **Step 6: remote_targets — struct + macros**

In `src/models/remote_target.rs`:

```rust
#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::remote_targets_history)]
pub struct RemoteTargetHistory {
    pub id: i32,
    pub namespace_id: i32,
    pub class_id: Option<i32>,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub allowed_subject_types: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(RemoteTargetHistory, "remote_targets_history");
```

Re-export from `src/models/mod.rs`.

- [ ] **Step 7: remote_targets — db fns + handlers + routes**

In `src/api/v1/handlers/remote_targets.rs`:

```rust
crate::history_db_fns!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    crate::schema::remote_targets_history,
    crate::models::RemoteTargetHistory
);
```

Add the two handlers (`"/{remote_target_id}/history"`, `"/{remote_target_id}/history/as-of"`) mirroring Task 2, using the remote-target ID newtype, `instance()`, and the `Permissions::ReadRemoteTarget` check from the existing remote-target detail GET. Register in `src/api/v1/routes/remote_targets.rs`.

- [ ] **Step 8: Tests — one list+as-of test per resource**

For each of the three resources, add an API test in its `src/tests/api/v1/<resource>.rs` modeled exactly on Task 2's `test_api_class_history_list_and_as_of` (create → update → list asserts 2 versions newest-first with correct `op`; as-of at the insert timestamp returns v1) and a `404` test for a missing id. Use each resource's existing fixture/create helpers (read the neighboring tests in that file to match fixture usage).

- [ ] **Step 9: Run the new tests**

Run: `set -a; source .env; set +a && cargo test --lib tests::api::v1::namespaces:: tests::api::v1::templates:: tests::api::v1::remote_targets:: 2>&1 | tail -15`
Expected: the new history tests PASS. (Clear leaked rows / `diesel database reset` if pre-existing fixed-name flakes appear.)

- [ ] **Step 10: Clippy + commit**

Run: `set -a; source .env; set +a && cargo clippy --all-targets -- -D warnings` (expect clean)

```bash
git add src/models/ src/api/v1/handlers/namespaces.rs src/api/v1/handlers/templates.rs src/api/v1/handlers/remote_targets.rs src/api/v1/routes/ src/tests/api/v1/
git commit -m "feat(history): namespace, report-template, remote-target history endpoints"
```

---

### Task 4: Object history endpoints (nested under class)

**Files:**
- Modify: `src/models/object.rs` (add `HubuumObjectHistory` + macros)
- Modify: `src/models/mod.rs` (re-export)
- Modify: `src/api/v1/handlers/classes.rs` (objects are served under the class scope — db fns + 2 handlers)
- Modify: `src/api/v1/routes/classes.rs` (routes)
- Test: `src/tests/api/v1/` (objects test file or classes file, wherever object tests live)

**Interfaces:**
- Consumes: Task 1 macros/helpers; Task 2 pattern.
- Produces: `HubuumObjectHistory`; `object_history_paginated_with_total_count`, `object_as_of`; routes under the class scope for object history.

> Objects have no top-level route scope — they are served under `/classes/{class_id}/...` (see `get_object_in_class` in `classes.rs`). The history endpoints therefore live under the class scope too.

- [ ] **Step 1: History struct + macros**

In `src/models/object.rs`:

```rust
#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::hubuumobject_history)]
pub struct HubuumObjectHistory {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(HubuumObjectHistory, "hubuumobject_history");
```

Re-export `HubuumObjectHistory` from `src/models/mod.rs`.

- [ ] **Step 2: DB fns**

In `src/api/v1/handlers/classes.rs`:

```rust
crate::history_db_fns!(
    object_history_paginated_with_total_count,
    object_as_of,
    crate::schema::hubuumobject_history,
    crate::models::HubuumObjectHistory
);
```

- [ ] **Step 3: Determine the existing object-detail path + permission**

Run: `set -a; source .env; set +a && grep -nE "get_object_in_class|ReadObject|HubuumObjectID|#\\[get\\(" src/api/v1/handlers/classes.rs | head -30`
Use the exact path pattern of `get_object_in_class` (e.g. `"/{class_id}/{object_id}"` or similar) and its permission check. The history endpoints append `/history` and `/history/as-of` to that object path.

- [ ] **Step 4: Failing test**

In the object API test file, add a `test_api_object_history_list_and_as_of` modeled on Task 2's class test: create an object (via the existing object create helper/endpoint), update it, then GET the object-history path asserting 2 versions newest-first and an as-of returning v1; plus a 404 test. Use the object path discovered in Step 3.

- [ ] **Step 5: Run to verify it fails**

Run: `set -a; source .env; set +a && cargo test --lib tests::api::v1::<objects-test-module>::...history -- --nocapture`
Expected: FAIL (routes/handlers missing).

- [ ] **Step 6: Handlers + routes**

Add `get_object_history` and `get_object_as_of` in `classes.rs`, mirroring Task 2 Step 6 but: path mirrors the object detail path + `/history` (resp. `/history/as-of`); resolve the object instance via the same path params and permission check as `get_object_in_class` (`Permissions::ReadObject`); use `prepare_db_pagination::<crate::models::HubuumObjectHistory>` and the `object_*` db fns. Register both in `src/api/v1/routes/classes.rs`.

- [ ] **Step 7: Run to verify it passes**

Run: `set -a; source .env; set +a && cargo test --lib tests::api::v1::<objects-test-module>::...history -- --nocapture`
Expected: PASS.

- [ ] **Step 8: Clippy + commit**

Run: `set -a; source .env; set +a && cargo clippy --all-targets -- -D warnings` (expect clean)

```bash
git add src/models/object.rs src/models/mod.rs src/api/v1/handlers/classes.rs src/api/v1/routes/classes.rs src/tests/
git commit -m "feat(history): object history endpoints (nested under class)"
```

---

### Task 5: Documentation + full-suite verification

**Files:**
- Modify: `docs/temporal_history.md`

- [ ] **Step 1: Document the read API**

Update `docs/temporal_history.md`:
- Replace the "read access is not yet implemented" note with a "History read API" section.
- For each covered resource (class, object, namespace, report-template, remote-target), document: `GET /<resource-path>/{id}/history` (cursor-paginated, newest-first; `X-Total-Count` / `X-Next-Cursor` headers; `?sort=valid_from|history_id`, `?limit=`, `?cursor=`; response is the snapshot fields plus `op`, `valid_from`, `valid_to`, `actor_id`, `actor_username`, `history_id`) and `GET /<resource-path>/{id}/history/as-of?at=<rfc3339>` (snapshot valid at the instant, 404 if none).
- State the access rule: same Read permission as the resource's detail GET; **404 if the entity no longer exists** (deleted-entity history auditing is not exposed).
- Note the still-deferred pieces: **relation history** (`hubuumclass_relation`, `hubuumobject_relation`) — Plan 2b; and that **background/task writes still record `actor_id = NULL`** (carried over from Plan 1).

- [ ] **Step 2: Full clean-DB suite + clippy**

Run:
```bash
set -a; source .env; set +a
diesel database reset
cargo test --lib 2>&1 | tail -5
cargo clippy --all-targets -- -D warnings 2>&1 | tail -3
```
Expected: all tests pass; clippy clean.

- [ ] **Step 3: Commit**

```bash
git add docs/temporal_history.md
git commit -m "docs(history): document the history read API (list + as-of)"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-25-temporal-history-design.md` §4 Read API):**
- `as_of` point-in-time → dedicated `/history/as-of` endpoint, Tasks 2-4. ✓ (deviation from spec's "query param on detail GET" — decided with the user: dedicated sub-endpoint for shape consistency.)
- list-versions, cursor-paginated, newest-first, with `op/valid_from/valid_to/actor_id/actor_username` → Tasks 2-4 via full `CursorPaginated` reuse. ✓
- `actor_username` via lookup, null when unavailable → `resolve_actor_usernames` + `HistoryResponse`. ✓
- Per-resource `Queryable` history structs + `list_history`/`get_as_of` db fns → Tasks 2-4 via macros. ✓
- Permission: same Read permission as detail GET; 404 if deleted → Tasks 2-4. ✓ (decided with the user.)
- **Deferred:** relation history (2 tables) → Plan 2b, documented in Task 5. Flagged, not silently dropped.

**Placeholder scan:** The "mirror the existing detail GET / use the resource's Read variant" instructions in Tasks 3-4 point at concrete, existing handlers the implementer edits in the same files (with a grep step to capture exact names) — not TBDs. The class reference (Task 2) is fully spelled out. The `url`/`urlencoding` notes give concrete fallbacks.

**Type consistency:** `HistoryResponse<T>`, `resolve_actor_usernames(pool, Vec<i32>) -> HashMap<i32,String>`, `parse_as_of(&str) -> DateTime<Utc>`, the two macros' signatures, and the `*_history_paginated_with_total_count(entity_id, pool, &QueryOptions) -> (Vec<T>, i64)` / `*_as_of(entity_id, DateTime<Utc>, pool) -> Option<T>` shapes are used consistently across all tasks. History struct field order matches `schema.rs` per the verbatim column lists.

**Known risk (documented):** `history_id` is `i64` (BigInt) but `CursorSqlType` has only `Integer`; the keyset/ORDER BY are emitted as raw SQL so this is cosmetic at runtime, and the cursor value round-trips as `i64` (`CursorValue::Integer`). If diesel rejects the `sql::<Integer>` annotation on a BigInt column at runtime, fall back to sorting by `valid_from` only (drop `HistoryId` from `supports_sort`/`default`/`tie_breaker`), using `valid_from` + a secondary unique guarantee — but `valid_from` alone is not unique within a transaction, so prefer keeping `history_id` and verifying the cursor round-trips in the Task 2 test (the list test's two-version assertion exercises ordering).
