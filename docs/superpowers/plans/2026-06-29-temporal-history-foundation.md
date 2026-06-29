# Temporal History Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture full row-level history (insert/update/delete) for hubuum's core domain tables via PostgreSQL triggers, attributing each change to a pseudonymous actor, plus a GDPR anonymization operation — all without touching the existing CRUD code paths.

**Architecture:** Per-table `<table>_history` twin tables populated by one generic PL/pgSQL trigger. The acting user's id is carried into the trigger through a transaction-local Postgres GUC (`hubuum.actor_id`), set from a `tokio::task_local!` that an actix middleware establishes per request (mirroring the existing `AMBIENT_STATEMENT_TIMEOUT` machinery). The middleware also resolves the bearer token once and shares it with the auth extractors. Anonymization tombstones a user's PII in the (non-versioned) `users` table, leaving history's integer `actor_id` as a meaningless pseudonym.

**Tech Stack:** Rust (edition 2024), Diesel 2 (Postgres, r2d2, sync), actix-web 4.13, chrono, PostgreSQL.

**Scope note:** This is **Plan 1 of 2**. It delivers history capture + actor attribution + GDPR anonymization (a complete, testable deliverable). The read API (`as_of` + list-versions across all 7 models, cursor-paginated) is **Plan 2**, written after this plan executes. This plan adds the history tables and the data flowing into them; it intentionally adds no history *read* endpoints.

## Global Constraints

- Rust edition 2024; Diesel 2 features `postgres, serde_json, r2d2, chrono` (sync, r2d2 pool).
- **No new workspace crate** — this feature is coupled to the diesel schema and connection layer.
- History `actor_id` is a **plain `int`, never a foreign key** — erasing a user must never cascade into history.
- Validity columns (`valid_from`/`valid_to`) are `timestamptz` (→ `chrono::DateTime<Utc>` in Rust); snapshot columns keep their base types.
- `op` is `varchar` (not `char`) with `CHECK (op IN ('I','U','D'))`.
- In-scope tables (exactly these 7): `hubuumclass`, `hubuumobject`, `namespaces`, `hubuumclass_relation`, `hubuumobject_relation`, `report_templates`, `remote_targets`. Each has a single-column integer `id` PK (verified).
- `src/schema.rs` is Diesel-generated; `diesel.toml` has `[print_schema] file = "src/schema.rs"`, so `diesel migration run` regenerates it automatically. Ensure `DATABASE_URL` is set (matches `config.database_url`).
- Maintenance contract: any future migration that `ALTER`s a versioned base table MUST mirror the change in its `_history` table.
- TDD, frequent commits. Final gate is `cargo clippy --all-targets -- -D warnings` and `cargo test`; remove dead code as you go (the extractor refactor deletes now-unused helpers).
- The test harness shares one DB pool with no rollback isolation — scope all history assertions to specific entity ids, never global counts.

---

### Task 1: History tables + generic trigger (migration)

**Files:**
- Create: `migrations/2026-06-29-000001_temporal_history/up.sql`
- Create: `migrations/2026-06-29-000001_temporal_history/down.sql`
- Modify (regenerated): `src/schema.rs`
- Create: `src/tests/temporal/mod.rs`
- Modify: `src/tests/mod.rs` (register the new test module)

**Interfaces:**
- Produces: history tables `<t>_history` for each in-scope `t`, with columns = base columns (via `LIKE`) + `op varchar`, `valid_from timestamptz`, `valid_to timestamptz`, `actor_id int`, `history_id bigint` (PK). A generic trigger function `hubuum_record_history()` reading the GUC `hubuum.actor_id`. These appear in `src/schema.rs` as `<t>_history` `table!` blocks consumed by later tasks.

- [ ] **Step 1: Write the migration up.sql**

Create `migrations/2026-06-29-000001_temporal_history/up.sql`:

```sql
-- Generic history trigger: writes a full-row snapshot into <table>_history on
-- every INSERT/UPDATE/DELETE. The acting user id is read from the transaction
-- local GUC `hubuum.actor_id` (NULL when unset = system/migration/background).
CREATE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist  text        := quote_ident(TG_TABLE_NAME || '_history');
  seq   text        := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts    timestamptz := transaction_timestamp();
  actor int         := nullif(current_setting('hubuum.actor_id', true), '')::int;
BEGIN
  IF TG_OP = 'INSERT' THEN
    EXECUTE format('INSERT INTO %s SELECT ($1).*, %L, $2, NULL, $3, nextval(%s)', hist, 'I', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s SELECT ($1).*, %L, $2, NULL, $3, nextval(%s)', hist, 'U', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSE  -- DELETE
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s SELECT ($1).*, %L, $2, $2, $3, nextval(%s)', hist, 'D', seq)
      USING OLD, ts, actor;
    RETURN OLD;
  END IF;
END; $$;

-- Create one history twin + sequence + indexes + trigger per in-scope table.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hubuumclass','hubuumobject','namespaces','hubuumclass_relation',
    'hubuumobject_relation','report_templates','remote_targets'
  ]
  LOOP
    EXECUTE format(
      'CREATE TABLE %1$I_history (
         LIKE %1$I,
         op varchar NOT NULL CHECK (op IN (''I'',''U'',''D'')),
         valid_from timestamptz NOT NULL,
         valid_to timestamptz,
         actor_id int,
         history_id bigint NOT NULL
       )', t);
    EXECUTE format('CREATE SEQUENCE %1$I_history_seq OWNED BY %1$I_history.history_id', t);
    EXECUTE format('ALTER TABLE %1$I_history ADD PRIMARY KEY (history_id)', t);
    EXECUTE format('CREATE INDEX %1$I_history_id_from_idx ON %1$I_history (id, valid_from)', t);
    EXECUTE format('CREATE INDEX %1$I_history_actor_idx ON %1$I_history (actor_id)', t);
    EXECUTE format(
      'CREATE TRIGGER %1$I_history_trg AFTER INSERT OR UPDATE OR DELETE ON %1$I
       FOR EACH ROW EXECUTE FUNCTION hubuum_record_history()', t);
  END LOOP;
END $$;
```

- [ ] **Step 2: Write the migration down.sql**

Create `migrations/2026-06-29-000001_temporal_history/down.sql`:

```sql
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hubuumclass','hubuumobject','namespaces','hubuumclass_relation',
    'hubuumobject_relation','report_templates','remote_targets'
  ]
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %1$I_history_trg ON %1$I', t);
    EXECUTE format('DROP TABLE IF EXISTS %1$I_history', t);  -- drops OWNED sequence too
  END LOOP;
END $$;
DROP FUNCTION IF EXISTS hubuum_record_history();
```

- [ ] **Step 3: Run the migration (and regenerate schema.rs)**

Run: `diesel migration run`
Expected: completes without error; `git status` shows `src/schema.rs` modified with new `hubuumclass_history` … `remote_targets_history` `table!` blocks. Verify down/up round-trips: `diesel migration redo` (expect success).

- [ ] **Step 4: Register the test module**

In `src/tests/mod.rs`, add alongside the other `mod` declarations:

```rust
mod temporal;
```

- [ ] **Step 5: Write the failing DB-level trigger test**

Create `src/tests/temporal/mod.rs`:

```rust
use crate::db::with_connection;
use crate::tests::TestScope;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::sql_types::{Integer, Text};

/// Driving INSERT/UPDATE/DELETE on a base table through raw SQL (with the
/// actor GUC set) must produce I/U/D history rows carrying that actor.
#[actix_rt::test]
async fn trigger_records_ops_and_actor() {
    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("trigger_actor").await;
    let ns_id = ns.namespace.id;
    let cname = format!("trigger_actor_class_{}", scope.scope_id);

    // All three DML statements in one transaction with the actor GUC set.
    with_connection(&pool, |conn| {
        conn.transaction::<(), diesel::result::Error, _>(|conn| {
            diesel::sql_query("SELECT set_config('hubuum.actor_id', '4242', true)").execute(conn)?;
            diesel::sql_query(
                "INSERT INTO hubuumclass (name, namespace_id, validate_schema, description)
                 VALUES ($1, $2, false, 'd')",
            )
            .bind::<Text, _>(&cname)
            .bind::<Integer, _>(ns_id)
            .execute(conn)?;

            let cid: i32 = {
                use crate::schema::hubuumclass::dsl as c;
                c::hubuumclass.filter(c::name.eq(&cname)).select(c::id).first(conn)?
            };
            diesel::sql_query("UPDATE hubuumclass SET description='d2' WHERE id=$1")
                .bind::<Integer, _>(cid)
                .execute(conn)?;
            diesel::sql_query("DELETE FROM hubuumclass WHERE id=$1")
                .bind::<Integer, _>(cid)
                .execute(conn)?;
            Ok(())
        })
    })
    .unwrap();

    // Read back the history for that class, oldest first.
    let rows: Vec<(String, Option<DateTime<Utc>>, Option<i32>)> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass::dsl as c;
        use crate::schema::hubuumclass_history::dsl as h;
        let cid: i32 = c::hubuumclass
            .filter(c::name.eq(&cname))
            .select(c::id)
            .first(conn)
            .optional()?
            .unwrap_or(-1);
        // The class itself is deleted; find history by the name snapshot instead.
        let _ = cid;
        h::hubuumclass_history
            .filter(h::name.eq(&cname))
            .order(h::history_id.asc())
            .select((h::op, h::valid_to, h::actor_id))
            .load(conn)
    })
    .unwrap();

    let ops: Vec<&str> = rows.iter().map(|(op, _, _)| op.as_str()).collect();
    assert_eq!(ops, vec!["I", "U", "D"], "expected insert/update/delete history");
    assert!(rows.iter().all(|(_, _, actor)| *actor == Some(4242)), "actor must be 4242 on every row");

    ns.cleanup().await.unwrap();
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test --lib tests::temporal::trigger_records_ops_and_actor -- --nocapture`
Expected: PASS. (If `hubuumclass_history` is unknown, Step 3 didn't regenerate `src/schema.rs` — re-run `diesel migration run`.)

- [ ] **Step 7: Add a cascade-delete history test**

Append to `src/tests/temporal/mod.rs`:

```rust
/// Deleting a namespace cascades to its classes; the AFTER trigger must still
/// record a 'D' history row for each cascaded class.
#[actix_rt::test]
async fn cascade_delete_records_history() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("cascade_hist").await;
    let cname = format!("cascade_hist_class_{}", scope.scope_id);

    let class = NewHubuumClass {
        name: cname.clone(),
        namespace_id: ns.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool)
    .await
    .unwrap();

    ns.cleanup().await.unwrap(); // cascade-deletes the class

    let ops: Vec<String> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        h::hubuumclass_history
            .filter(h::id.eq(class.id))
            .order(h::history_id.asc())
            .select(h::op)
            .load(conn)
    })
    .unwrap();

    assert!(ops.contains(&"I".to_string()), "insert should be recorded");
    assert!(ops.contains(&"D".to_string()), "cascade delete should be recorded");
}
```

- [ ] **Step 8: Run both tests**

Run: `cargo test --lib tests::temporal:: -- --nocapture`
Expected: both PASS.

- [ ] **Step 9: Commit**

```bash
git add migrations/2026-06-29-000001_temporal_history src/schema.rs src/tests/temporal/mod.rs src/tests/mod.rs
git commit -m "feat(history): add system-versioned history tables and generic trigger"
```

---

### Task 2: Ambient actor plumbing (`src/db/mod.rs`)

**Files:**
- Modify: `src/db/mod.rs` (add task-local, scope helper, actor GUC; extend `with_connection_timeout` and `with_transaction`)
- Modify: `src/tests/temporal/mod.rs` (add Rust-path actor tests)

**Interfaces:**
- Consumes: history tables from Task 1.
- Produces: `pub async fn with_actor_scope<F, R>(actor: Option<i32>, future: F) -> R where F: Future<Output = R>`. After this task, any `with_connection`/`with_transaction` write executed inside a `with_actor_scope(Some(id), …)` records `actor_id = id`; outside any scope it records `NULL`.

- [ ] **Step 1: Write the failing actor-scope test**

Append to `src/tests/temporal/mod.rs`:

```rust
use crate::db::with_actor_scope;

#[actix_rt::test]
async fn actor_scope_sets_actor_and_default_is_null() {
    use crate::models::NewHubuumClass;
    use crate::traits::CanSave;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("actor_scope").await;
    let ns_id = ns.namespace.id;

    // Inside a scope -> actor recorded.
    let in_name = format!("actor_in_{}", scope.scope_id);
    let in_class = with_actor_scope(Some(4242), async {
        NewHubuumClass {
            name: in_name.clone(),
            namespace_id: ns_id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool)
        .await
    })
    .await
    .unwrap();

    // Outside any scope -> actor NULL.
    let out_name = format!("actor_out_{}", scope.scope_id);
    let out_class = NewHubuumClass {
        name: out_name.clone(),
        namespace_id: ns_id,
        json_schema: None,
        validate_schema: Some(false),
        description: "d".into(),
    }
    .save(&pool)
    .await
    .unwrap();

    let read_actor = |id: i32| {
        with_connection(&pool, move |conn| {
            use crate::schema::hubuumclass_history::dsl as h;
            h::hubuumclass_history
                .filter(h::id.eq(id))
                .order(h::history_id.desc())
                .select(h::actor_id)
                .first::<Option<i32>>(conn)
        })
        .unwrap()
    };

    assert_eq!(read_actor(in_class.id), Some(4242));
    assert_eq!(read_actor(out_class.id), None);

    ns.cleanup().await.unwrap();
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib tests::temporal::actor_scope_sets_actor_and_default_is_null`
Expected: FAIL — `with_actor_scope` not found (and, before the impl, the in-scope actor would be NULL).

- [ ] **Step 3: Add the task-local and helpers**

In `src/db/mod.rs`, after the existing `AMBIENT_STATEMENT_TIMEOUT` `task_local!` block (around line 31), add:

```rust
tokio::task_local! {
    /// The acting user id for the current async task, if any. Set via
    /// [`with_actor_scope`] and applied as a transaction-local
    /// `SET LOCAL hubuum.actor_id` by [`with_connection_timeout`] /
    /// [`with_transaction`], so the history trigger can attribute writes to a
    /// user without threading the actor through every caller. Outside any scope
    /// the lookup yields `None`, recorded as a NULL actor.
    static AMBIENT_ACTOR: Option<i32>;
}

/// Run `future` with an ambient actor id in effect (see [`AMBIENT_ACTOR`]).
pub async fn with_actor_scope<F, R>(actor: Option<i32>, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_ACTOR.scope(actor, future).await
}

/// The ambient actor id for the current task, or `None` outside any scope.
fn ambient_actor() -> Option<i32> {
    AMBIENT_ACTOR.try_with(|actor| *actor).unwrap_or(None)
}

/// Apply a transaction-local `SET LOCAL hubuum.actor_id`. Bound, not formatted,
/// mirroring [`set_local_statement_timeout`]. Reverts at COMMIT/ROLLBACK.
fn set_local_actor(conn: &mut PgConnection, actor: i32) -> Result<(), diesel::result::Error> {
    use diesel::RunQueryDsl;
    diesel::sql_query("SELECT set_config('hubuum.actor_id', $1, true)")
        .bind::<diesel::sql_types::Text, _>(actor.to_string())
        .execute(conn)?;
    Ok(())
}
```

- [ ] **Step 4: Apply the actor inside `with_connection_timeout`**

Replace the body of `with_connection_timeout` (currently lines ~134-143) with:

```rust
    let actor = ambient_actor();
    let mut conn = acquire_connection(pool)?;
    if statement_timeout.is_none() && actor.is_none() {
        f(&mut conn).map_err(ApiError::from)
    } else {
        conn.transaction::<R, ApiError, _>(|conn| {
            if let Some(statement_timeout) = statement_timeout {
                set_local_statement_timeout(conn, statement_timeout)?;
            }
            if let Some(actor) = actor {
                set_local_actor(conn, actor)?;
            }
            f(conn).map_err(ApiError::from)
        })
    }
```

- [ ] **Step 5: Apply the actor inside `with_transaction`**

In `with_transaction` (currently lines ~169-177), add the actor alongside the timeout. Replace its body with:

```rust
    let statement_timeout = ambient_statement_timeout();
    let actor = ambient_actor();
    let mut conn = acquire_connection(pool)?;
    conn.transaction::<R, ApiError, _>(|conn| {
        if let Some(statement_timeout) = statement_timeout {
            set_local_statement_timeout(conn, statement_timeout)?;
        }
        if let Some(actor) = actor {
            set_local_actor(conn, actor)?;
        }
        f(conn).map_err(ApiError::from)
    })
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cargo test --lib tests::temporal::actor_scope_sets_actor_and_default_is_null`
Expected: PASS.

- [ ] **Step 7: Run the existing db tests for no regression**

Run: `cargo test --lib db::tests::`
Expected: existing timeout/transaction tests still PASS (the timeout path is unchanged when no actor is set).

- [ ] **Step 8: Commit**

```bash
git add src/db/mod.rs src/tests/temporal/mod.rs
git commit -m "feat(history): thread ambient actor into history via transaction-local GUC"
```

---

### Task 3: Auth/actor middleware + extractor refactor

**Files:**
- Create: `src/middlewares/actor_context.rs`
- Modify: `src/middlewares/mod.rs` (declare + re-export)
- Modify: `src/extractors/mod.rs` (read `ResolvedAuth` from request extensions; drop `extract_token`/`extract_user_from_token`)
- Modify: `src/main.rs` (wrap the app with the middleware)
- Modify: `src/tests/api_operations.rs` (wrap each test app builder with the middleware)
- Modify: `src/tests/api/v1/classes.rs` (add the actor-attribution integration test)

**Interfaces:**
- Consumes: `with_actor_scope` (Task 2); `Token`, `User`, `Token::is_valid` (`crate::db::traits::Status`), `get_user_by_id`.
- Produces: `pub enum ResolvedAuth { Authenticated { token: Token, user: User }, Missing, Invalid }` (derives `Clone`) and `pub async fn actor_context(req: ServiceRequest, next: Next<impl MessageBody + 'static>) -> Result<ServiceResponse<impl MessageBody>, Error>`. Extractors `UserAccess`/`AdminAccess`/`AdminOrSelfAccess` now source their user from `ResolvedAuth` in request extensions.

- [ ] **Step 1: Write the middleware module**

Create `src/middlewares/actor_context.rs`:

```rust
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, HttpMessage};

use crate::db::traits::Status;
use crate::db::{with_actor_scope, DbPool};
use crate::models::token::Token;
use crate::models::user::User;
use crate::utilities::iam::get_user_by_id;

/// Outcome of resolving the bearer token once per request. Stored in request
/// extensions and consumed by the auth extractors so they never re-query.
#[derive(Clone)]
pub enum ResolvedAuth {
    Authenticated { token: Token, user: User },
    Missing,
    Invalid,
}

fn bearer_token(req: &ServiceRequest) -> Option<Token> {
    let header = req.headers().get("Authorization")?.to_str().ok()?;
    header.strip_prefix("Bearer ").map(|s| Token(s.to_string()))
}

async fn resolve_auth(req: &ServiceRequest) -> ResolvedAuth {
    let token = match bearer_token(req) {
        Some(token) => token,
        None => return ResolvedAuth::Missing,
    };
    let pool = match req.app_data::<Data<DbPool>>() {
        Some(pool) => pool.clone(),
        None => return ResolvedAuth::Invalid,
    };
    match token.is_valid(&pool).await {
        Ok(user_token) => match get_user_by_id(&pool, user_token.user_id) {
            Ok(user) => ResolvedAuth::Authenticated { token, user },
            Err(_) => ResolvedAuth::Invalid,
        },
        Err(_) => ResolvedAuth::Invalid,
    }
}

/// Resolve the requesting user once, stash the result in request extensions for
/// the extractors, and run the rest of the request inside a `with_actor_scope`
/// so every DB write attributes its history rows to that user.
pub async fn actor_context(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<impl MessageBody>, Error> {
    let resolved = resolve_auth(&req).await;
    let actor = match &resolved {
        ResolvedAuth::Authenticated { user, .. } => Some(user.id),
        _ => None,
    };
    req.extensions_mut().insert(resolved);
    with_actor_scope(actor, next.call(req)).await
}
```

- [ ] **Step 2: Declare and re-export the module**

In `src/middlewares/mod.rs`, add:

```rust
pub mod actor_context;
```

and to the re-export block:

```rust
pub use actor_context::{actor_context, ResolvedAuth};
```

- [ ] **Step 3: Refactor the extractors to read `ResolvedAuth`**

Replace the contents of `src/extractors/mod.rs` with:

```rust
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors_path::get_user_and_path;
use crate::middlewares::ResolvedAuth;
use crate::models::token::Token;
use crate::models::user::User;

use actix_web::{dev::Payload, web::Data, FromRequest, HttpMessage, HttpRequest};
use futures_util::future::FutureExt;
use std::future::{ready, Ready};
use std::pin::Pin;
use tracing::debug;

pub struct AdminAccess {
    pub token: Token,
    pub user: User,
}

#[allow(dead_code)]
pub struct AdminOrSelfAccess {
    pub token: Token,
    pub user: User,
}

/// A user with a valid token (not necessarily an admin).
pub struct UserAccess {
    pub token: Token,
    pub user: User,
}

fn resolved(req: &HttpRequest) -> Result<(Token, User), ApiError> {
    match req.extensions().get::<ResolvedAuth>() {
        Some(ResolvedAuth::Authenticated { token, user }) => Ok((token.clone(), user.clone())),
        Some(ResolvedAuth::Invalid) => Err(ApiError::Unauthorized("Invalid token".to_string())),
        _ => Err(ApiError::Unauthorized("No token provided".to_string())),
    }
}

impl FromRequest for UserAccess {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(resolved(req).map(|(token, user)| UserAccess { token, user }))
    }
}

impl FromRequest for AdminAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = req.app_data::<Data<DbPool>>().cloned();
        let resolved = resolved(req);
        async move {
            let (token, user) = resolved?;
            let pool = pool.ok_or_else(|| {
                ApiError::InternalServerError("Pool not found".to_string())
            })?;
            if user.is_admin(&pool).await? {
                Ok(AdminAccess { token, user })
            } else {
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}

impl FromRequest for AdminOrSelfAccess {
    type Error = ApiError;
    type Future = Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        let pool = req.app_data::<Data<DbPool>>().cloned();
        let resolved = resolved(req);
        let path_info = req.match_info().clone();
        async move {
            let (token, user) = resolved?;
            let pool = pool.ok_or_else(|| {
                ApiError::InternalServerError("Pool not found".to_string())
            })?;
            let (user_from_path, path) = get_user_and_path(&path_info, &pool).await?;
            if user.is_admin(&pool).await? || user.id == user_from_path.id {
                Ok(AdminOrSelfAccess { token, user })
            } else {
                debug! {
                    message = "User attempted to access an admin-only resource.",
                    user_id = user.id,
                    path = path,
                };
                Err(ApiError::Forbidden("Permission denied".to_string()))
            }
        }
        .boxed_local()
    }
}
```

> Note: `get_user_and_path` is moved out of `extractors/mod.rs` into a small sibling module so it can be imported as shown. Do Step 4 before compiling.

- [ ] **Step 4: Move `get_user_and_path` into a sibling module**

Create `src/extractors_path.rs` with the function lifted verbatim from the old `extractors/mod.rs`:

```rust
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::user::User;
use crate::utilities::iam::get_user_by_id;

pub async fn get_user_and_path(
    path: &actix_web::dev::Path<actix_web::dev::Url>,
    pool: &DbPool,
) -> Result<(User, String), ApiError> {
    let user_id = match path.query("user_id").parse::<i32>() {
        Ok(id) => id,
        Err(_) => {
            return Err(ApiError::InternalServerError(
                "Failed to parse user_id".into(),
            ));
        }
    };
    let path = path.as_str().to_string();
    let user = get_user_by_id(pool, user_id)?;
    Ok((user, path))
}
```

Then declare it in `src/main.rs` (or wherever modules are declared — search for `mod extractors;`) by adding next to it:

```rust
mod extractors_path;
```

- [ ] **Step 5: Wrap the production app with the middleware**

In `src/main.rs`, add the import near the other actix imports:

```rust
use actix_web::middleware::from_fn;
```

and in the `App::new()` builder chain (after `.wrap(Logger::default())`), add:

```rust
            .wrap(from_fn(middlewares::actor_context))
```

- [ ] **Step 6: Wrap every test app builder with the middleware**

In `src/tests/api_operations.rs`, in **each** `test::init_service(App::new() … )` builder, add after the `.wrap(TracingMiddleware::new())` line:

```rust
            .wrap(actix_web::middleware::from_fn(crate::middlewares::actor_context))
```

(There are several builders: `get_request_with_correlation`, `post_request_with_headers`, and the patch/delete equivalents — update all of them.)

- [ ] **Step 7: Verify the build and existing API tests still pass**

Run: `cargo test --lib tests::api::v1::classes::`
Expected: existing class API tests PASS (extractor behavior unchanged; auth still required).

- [ ] **Step 8: Write the actor-attribution integration test**

In `src/tests/api/v1/classes.rs` (inside the `tests` module), add:

```rust
    #[rstest]
    #[actix_web::test]
    async fn test_api_create_records_actor(#[future(awt)] test_context: TestContext) {
        use crate::db::with_connection;
        use diesel::prelude::*;

        let context = test_context;
        let ns = context.namespace_fixture("actor_history").await;

        let new_class = NewHubuumClass {
            name: "actor_history_class".to_string(),
            description: "d".to_string(),
            namespace_id: ns.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
        };

        let resp = post_request(&context.pool, &context.admin_token, CLASSES_ENDPOINT, &new_class).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: HubuumClassExpanded = test::read_body_json(resp).await;

        // The user behind admin_token, resolved straight from the tokens table.
        let expected_actor: i32 = with_connection(&context.pool, |conn| {
            use crate::schema::tokens::dsl as t;
            t::tokens
                .filter(t::token.eq(&context.admin_token))
                .select(t::user_id)
                .first::<i32>(conn)
        })
        .unwrap();

        let actor: Option<i32> = with_connection(&context.pool, |conn| {
            use crate::schema::hubuumclass_history::dsl as h;
            h::hubuumclass_history
                .filter(h::id.eq(created.id))
                .order(h::history_id.desc())
                .select(h::actor_id)
                .first::<Option<i32>>(conn)
        })
        .unwrap();

        assert_eq!(actor, Some(expected_actor), "history must attribute the create to the requestor");
        ns.cleanup().await.unwrap();
    }
```

- [ ] **Step 9: Run the new test**

Run: `cargo test --lib tests::api::v1::classes::tests::test_api_create_records_actor -- --nocapture`
Expected: PASS.

- [ ] **Step 10: Clippy + commit**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean (the old `extract_token`/`extract_user_from_token` are gone, so no dead-code warnings).

```bash
git add src/middlewares/actor_context.rs src/middlewares/mod.rs src/extractors/mod.rs src/extractors_path.rs src/main.rs src/tests/api_operations.rs src/tests/api/v1/classes.rs
git commit -m "feat(history): resolve requestor once in middleware and attribute writes to it"
```

---

### Task 4: GDPR anonymization

**Files:**
- Create: `migrations/2026-06-29-000002_user_anonymized_at/up.sql`
- Create: `migrations/2026-06-29-000002_user_anonymized_at/down.sql`
- Modify (regenerated): `src/schema.rs`
- Modify: `src/models/user.rs` (add `anonymized_at` field to `User`)
- Modify: `src/utilities/iam.rs` (add `anonymize_user`)
- Modify: `src/api/v1/handlers/users.rs` (add `anonymize_user` handler)
- Modify: `src/api/v1/routes/users.rs` (register the route)
- Modify: `src/tests/temporal/mod.rs` (anonymization tests)

**Interfaces:**
- Consumes: history tables (Task 1), `with_actor_scope`/`with_transaction` (Task 2), `AdminAccess` (Task 3).
- Produces: `pub async fn anonymize_user(pool: &DbPool, target_id: i32) -> Result<(), ApiError>`; HTTP `POST /api/v1/iam/users/{user_id}/anonymize` (admin-only, 204).

- [ ] **Step 1: Write the migration**

Create `migrations/2026-06-29-000002_user_anonymized_at/up.sql`:

```sql
ALTER TABLE users ADD COLUMN anonymized_at TIMESTAMP NULL;
```

Create `migrations/2026-06-29-000002_user_anonymized_at/down.sql`:

```sql
ALTER TABLE users DROP COLUMN IF EXISTS anonymized_at;
```

- [ ] **Step 2: Run the migration**

Run: `diesel migration run`
Expected: `src/schema.rs` `users` block now has `anonymized_at -> Nullable<Timestamp>`.

- [ ] **Step 3: Add the field to the `User` struct**

In `src/models/user.rs`, add to `struct User` (after `updated_at`):

```rust
    pub anonymized_at: Option<chrono::NaiveDateTime>,
```

- [ ] **Step 4: Build to surface any `User` construction sites**

Run: `cargo build`
Expected: compiles. If any literal `User { … }` construction errors on the missing field, add `anonymized_at: None`. (`NewUser` is a separate struct and is unaffected.)

- [ ] **Step 5: Write the failing anonymization test**

Append to `src/tests/temporal/mod.rs`:

```rust
#[actix_rt::test]
async fn anonymize_scrubs_pii_but_keeps_history_actor() {
    use crate::db::{with_actor_scope, with_connection};
    use crate::models::{NewHubuumClass, NewUser};
    use crate::traits::CanSave;
    use crate::utilities::iam::anonymize_user;
    use diesel::prelude::*;

    let scope = TestScope::new();
    let pool = scope.pool.clone();
    let ns = scope.namespace_fixture("anon").await;

    // A user who will make a change and then be anonymized.
    let uname = format!("anon_user_{}", scope.scope_id);
    let user = NewUser {
        username: uname.clone(),
        password: "secret".into(),
        email: Some("a@example.com".into()),
    }
    .save(&pool)
    .await
    .unwrap();
    let token = user.create_token(&pool).await.unwrap();
    let _ = token;

    let cname = format!("anon_class_{}", scope.scope_id);
    let class = with_actor_scope(Some(user.id), async {
        NewHubuumClass {
            name: cname.clone(),
            namespace_id: ns.namespace.id,
            json_schema: None,
            validate_schema: Some(false),
            description: "d".into(),
        }
        .save(&pool)
        .await
    })
    .await
    .unwrap();

    anonymize_user(&pool, user.id).await.unwrap();

    // PII scrubbed on the (non-versioned) users row.
    let (username, email, anonymized_at): (String, Option<String>, Option<chrono::NaiveDateTime>) =
        with_connection(&pool, |conn| {
            use crate::schema::users::dsl as u;
            u::users
                .filter(u::id.eq(user.id))
                .select((u::username, u::email, u::anonymized_at))
                .first(conn)
        })
        .unwrap();
    assert_eq!(username, format!("anonymized-{}", user.id));
    assert_eq!(email, None);
    assert!(anonymized_at.is_some());

    // Tokens revoked.
    let token_count: i64 = with_connection(&pool, |conn| {
        use crate::schema::tokens::dsl as t;
        t::tokens.filter(t::user_id.eq(user.id)).count().get_result(conn)
    })
    .unwrap();
    assert_eq!(token_count, 0);

    // History still attributes the change to the (now pseudonymous) id.
    let actor: Option<i32> = with_connection(&pool, |conn| {
        use crate::schema::hubuumclass_history::dsl as h;
        h::hubuumclass_history
            .filter(h::id.eq(class.id))
            .order(h::history_id.desc())
            .select(h::actor_id)
            .first::<Option<i32>>(conn)
    })
    .unwrap();
    assert_eq!(actor, Some(user.id));

    ns.cleanup().await.unwrap();
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cargo test --lib tests::temporal::anonymize_scrubs_pii_but_keeps_history_actor`
Expected: FAIL — `anonymize_user` not found.

- [ ] **Step 7: Implement `anonymize_user`**

In `src/utilities/iam.rs`, add:

```rust
use crate::db::with_transaction;

/// GDPR erasure: tombstone a user's PII (username/email/password), stamp
/// `anonymized_at`, and revoke their tokens, in one transaction. History rows
/// are untouched — they only ever held the integer actor id, now a pseudonym.
pub async fn anonymize_user(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::tokens::dsl as t;
    use crate::schema::users::dsl as u;
    use diesel::prelude::*;

    with_transaction(pool, |conn| -> Result<(), diesel::result::Error> {
        diesel::update(u::users.filter(u::id.eq(target_id)))
            .set((
                u::username.eq(format!("anonymized-{target_id}")),
                u::email.eq::<Option<String>>(None),
                u::password.eq(""),
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)?;
        diesel::delete(t::tokens.filter(t::user_id.eq(target_id))).execute(conn)?;
        Ok(())
    })
}
```

(Ensure `use crate::db::DbPool;` and `use crate::errors::ApiError;` are present at the top of the file — `get_user_by_id` already uses both.)

- [ ] **Step 8: Run the test to verify it passes**

Run: `cargo test --lib tests::temporal::anonymize_scrubs_pii_but_keeps_history_actor`
Expected: PASS.

- [ ] **Step 9: Add the HTTP handler**

In `src/api/v1/handlers/users.rs`, add (mirroring `delete_user`'s shape):

```rust
#[utoipa::path(
    post,
    path = "/api/v1/iam/users/{user_id}/anonymize",
    tag = "users",
    security(("bearer_auth" = [])),
    params(("user_id" = i32, Path, description = "User ID")),
    responses(
        (status = 204, description = "User anonymized"),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 403, description = "Forbidden", body = ApiErrorResponse),
        (status = 404, description = "User not found", body = ApiErrorResponse)
    )
)]
#[post("/{user_id}/anonymize")]
pub async fn anonymize_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let target_id = user_id.id();
    debug!(
        message = "User anonymize requested",
        target = target_id,
        requestor = requestor.user.id
    );
    crate::utilities::iam::anonymize_user(&pool, target_id).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}
```

Ensure `AdminAccess`, `json!`, `json_response`, `StatusCode`, `web`, `Responder`, `debug` are already imported in this file (they are, used by `delete_user`/`update_user`). Add `use actix_web::post;` to the macro imports if `post` is not yet imported there.

- [ ] **Step 10: Register the route**

In `src/api/v1/routes/users.rs`, add to the `config` chain:

```rust
        .service(users::anonymize_user)
```

- [ ] **Step 11: Write the API-level anonymize test**

In `src/tests/api/v1/` add to the users test file (e.g. `src/tests/api/v1/users.rs`; create the test in the existing module there) — model setup on existing user tests:

```rust
    #[rstest]
    #[actix_web::test]
    async fn test_api_anonymize_user(#[future(awt)] test_context: TestContext) {
        use crate::db::with_connection;
        use diesel::prelude::*;

        let context = test_context;

        // Create a throwaway user to anonymize.
        let uname = format!("api_anon_{}", context.scope_id());
        let new_user = crate::models::NewUser {
            username: uname.clone(),
            password: "secret".into(),
            email: Some("x@example.com".into()),
        }
        .save(&context.pool)
        .await
        .unwrap();

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/iam/users/{}/anonymize", new_user.id),
            &serde_json::json!({}),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;

        let username: String = with_connection(&context.pool, |conn| {
            use crate::schema::users::dsl as u;
            u::users.filter(u::id.eq(new_user.id)).select(u::username).first(conn)
        })
        .unwrap();
        assert_eq!(username, format!("anonymized-{}", new_user.id));
    }
```

> If `TestContext` has no `scope_id()` accessor, substitute any unique suffix already used by neighboring tests in that file (read the file to match its convention). The assertion on the tombstoned username is the deliverable.

- [ ] **Step 12: Run the anonymize tests**

Run: `cargo test --lib anonymize`
Expected: both the unit and API anonymize tests PASS.

- [ ] **Step 13: Clippy + commit**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: clean.

```bash
git add migrations/2026-06-29-000002_user_anonymized_at src/schema.rs src/models/user.rs src/utilities/iam.rs src/api/v1/handlers/users.rs src/api/v1/routes/users.rs src/tests/temporal/mod.rs src/tests/api/v1/users.rs
git commit -m "feat(history): add GDPR user anonymization preserving pseudonymous history"
```

---

### Task 5: Documentation

**Files:**
- Create: `docs/temporal_history.md`
- Modify: `docs/` index if one exists (e.g. a `README`/`SUMMARY` listing docs — grep for `deployment.md` references and add a sibling link).

**Interfaces:** none (docs only).

- [ ] **Step 1: Write the documentation**

Create `docs/temporal_history.md` covering, in prose with SQL/Rust snippets drawn from the implementation:

1. **Model** — per-table `<t>_history` twins; full-snapshot rows; `op`/`valid_from`/`valid_to`/`actor_id`/`history_id`; open version has `valid_to IS NULL`; `D` rows are zero-width tombstones.
2. **Generic trigger** — `hubuum_record_history()`, attached to the 7 in-scope tables; reads `hubuum.actor_id`; fires on cascade deletes.
3. **Actor capture** — `AMBIENT_ACTOR` task-local → `SET LOCAL hubuum.actor_id` in `with_connection_timeout`/`with_transaction`; the `actor_context` middleware sets it per request; writes outside a request scope record `NULL` (system).
4. **Maintenance contract** — altering a versioned base table requires mirroring the column change in its `_history` table in the same migration; adding a new versioned table = add it to the `FOREACH` arrays in a new migration.
5. **GDPR / anonymization** — `actor_id` is a pseudonym (plain int, no FK); `POST /api/v1/iam/users/{id}/anonymize` tombstones PII and revokes tokens; `users` is intentionally not versioned so old PII leaves no trace; this is pseudonymization under GDPR Art. 4(5).
6. **Security** — run the app under a non-owning, unprivileged DB role; do not grant `UPDATE`/`DELETE` on `_history` tables in production; trigger auditing can be bypassed by superusers/table owners (accepted limitation).
7. **Note** — read access to history (`as_of` / list-versions) is delivered in Plan 2.

- [ ] **Step 2: Commit**

```bash
git add docs/temporal_history.md
git commit -m "docs(history): document temporal history, actor capture, and GDPR anonymization"
```

---

## Self-Review

**Spec coverage (against `docs/superpowers/specs/2026-06-25-temporal-history-design.md`):**
- §1 Data model → Task 1 (history tables, surrogate `history_id` PK, no-FK `actor_id`, untouched base structs). ✓
- §2 Trigger + actor capture (PG) → Task 1 (generic trigger, cascade-safe). ✓
- §3 Actor threading + single auth lookup → Task 2 (db plumbing) + Task 3 (middleware + extractor refactor). ✓
- §4 Read API → **deferred to Plan 2** (explicitly out of scope here). Flagged, not silently dropped.
- §5 Anonymization → Task 4. ✓
- §6 Error handling/security/testing/docs → tests in Tasks 1-4; security + docs in Task 5. ✓

**Placeholder scan:** No `TODO`/`TBD`. The two soft references ("match neighboring test convention" for a unique suffix; "add docs index link if one exists") are explicit fallbacks, not missing code — the deliverable assertion/content is given.

**Type consistency:** `with_actor_scope(Option<i32>, impl Future)`, `ResolvedAuth` variants, `actor_id: Option<i32>`, `op: String`, `valid_from/valid_to: DateTime<Utc>` used consistently across tasks. `anonymize_user(&DbPool, i32) -> Result<(), ApiError>` matches its handler call site.

**Known trade-off (documented):** when an ambient actor is present, `with_connection` reads also run inside a transaction with one extra `set_config` round-trip. Acceptable for this workload; a future optimization could scope the GUC to write paths only.
