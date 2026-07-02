<!-- markdownlint-disable MD031 MD032 -->

# Temporal History, Actor Capture, and GDPR Anonymization

This document describes Hubuum's row-history mechanism, which records every data modification to seven core tables via a generic PostgreSQL trigger, attributes changes to a per-request actor (user), and enables GDPR-compliant user anonymization.

## Data Model

### History Tables

Every versioned table has a companion `<table>_history` table that records all mutations. For example:
- `hubuumclass` → `hubuumclass_history`
- `hubuumobject` → `hubuumobject_history`
- `namespaces` → `namespaces_history`
- `hubuumclass_relation` → `hubuumclass_relation_history`
- `hubuumobject_relation` → `hubuumobject_relation_history`
- `report_templates` → `report_templates_history`
- `remote_targets` → `remote_targets_history`

### Row Structure

Each history row is a **full-row snapshot** with the following columns:
- **All columns from the base table** (e.g., `id`, `name`, `created_at`, etc.)
- **`op` (varchar)**: The operation performed. Valid values are:
  - `'I'` - INSERT
  - `'U'` - UPDATE
  - `'D'` - DELETE
- **`valid_from` (timestamptz)**: When this version became active. Uses `clock_timestamp()` so long transactions record the trigger execution time.
- **`valid_to` (timestamptz, nullable)**: When this version expired. NULL indicates the row is the current open version.
- **`actor_id` (int, nullable)**: The user who performed the mutation. NULL when recorded outside a request scope (e.g., migrations, background work).
- **`history_id` (bigint, PK)**: Surrogate primary key for the history row itself, auto-incremented per table.

### Open vs. Closed Versions

- An **open version** has `valid_to IS NULL` and represents the active row.
- A **closed version** has `valid_to` set to the timestamp when the row was superseded.
- Querying the current state of a row requires finding the row in the `_history` table with `valid_to IS NULL`.

### Delete Tombstones

When a row is deleted (D operation), a **zero-width tombstone** is created:
- The tombstone holds all columns from the deleted row.
- It has `op = 'D'`.
- Both `valid_from` and `valid_to` are set to the deletion timestamp (matching the format seen in line 24 of `up.sql`).
- This tombstone allows auditing what was deleted and when, while being logically distinct from an UPDATE closure.

## Generic History Trigger

### The `hubuum_record_history()` Function

A single PL/pgSQL trigger function handles all three DML operations (INSERT, UPDATE, DELETE) for all versioned tables:

```sql
CREATE FUNCTION hubuum_record_history() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  hist  text        := quote_ident(TG_TABLE_NAME || '_history');
  seq   text        := quote_literal(TG_TABLE_NAME || '_history_seq');
  ts    timestamptz := clock_timestamp();
  actor int         := nullif(current_setting('hubuum.actor_id', true), '')::int;
BEGIN
  IF TG_OP = 'INSERT' THEN
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id) SELECT <base values>, %L, $2, NULL, $3, nextval(%s)', hist, 'I', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id) SELECT <base values>, %L, $2, NULL, $3, nextval(%s)', hist, 'U', seq)
      USING NEW, ts, actor;
    RETURN NEW;
  ELSE  -- DELETE
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id) SELECT <base values>, %L, $2, $2, $3, nextval(%s)', hist, 'D', seq)
      USING OLD, ts, actor;
    RETURN OLD;
  END IF;
END; $$;
```

### Trigger Attachment

The function is attached to seven in-scope tables via `AFTER` triggers on `INSERT OR UPDATE OR DELETE`:

```sql
CREATE TRIGGER hubuumclass_history_trg AFTER INSERT OR UPDATE OR DELETE ON hubuumclass
  FOR EACH ROW EXECUTE FUNCTION hubuum_record_history();
-- (and similarly for the other 6 tables)
```

### Key Behaviors

- **Cascade-safe**: The trigger fires after the base operation, so constraint cascades are honored.
- **Transaction-local actor**: The trigger reads `hubuum.actor_id` from a PostgreSQL GUC (session configuration), which is set to transaction-local scope so it reverts at commit/rollback.
- **NULL actor = system OR background task**: When `actor_id` is not set, it defaults to `NULL` in the history row. This covers writes outside any request context (migrations, schema changes), **and also async background workers** (notably imports and other `src/tasks` work) that currently run WITHOUT `with_actor_scope`, even when the task was user-initiated. **Planned future enhancement (Plan 2)**: threading the originating user through task execution so background work can be attributed correctly.
- **Dynamic table name**: Using `TG_TABLE_NAME`, the function adapts to whichever table it's attached to, avoiding trigger duplication.

### No-Op Updates

For temporal domain tables (`hubuumclass`, `hubuumobject`, `namespaces`,
`report_templates`, and `remote_targets`), an `UPDATE` whose domain data is
identical to the existing row is suppressed by a `BEFORE UPDATE` trigger.
`updated_at` is intentionally excluded from the comparison.

This means `updated_at` records when the persisted data last changed. A repeated
PATCH/import/update with the same values does not bump `updated_at`, does not
create a `U` history row, and does not create a new temporal version boundary.
If callers need to know when someone attempted an unchanged write, that belongs
in an audit/event stream rather than in the row's temporal state.

## Actor Capture

### Ambient Actor Task-Local

In `src/db/mod.rs`, an async task-local (`tokio::task_local!`) variable stores the current actor ID:

```rust
tokio::task_local! {
    /// The acting user id for the current async task, if any. Set via
    /// [`with_actor_scope`] and applied as a transaction-local
    /// `SET LOCAL hubuum.actor_id` by [`with_connection_timeout`] /
    /// [`with_transaction`], so the history trigger can attribute writes to a
    /// user without threading the actor through every caller.
    static AMBIENT_ACTOR: Option<i32>;
}
```

### Setting the Actor Scope

The `with_actor_scope()` helper establishes the ambient actor for the duration of a future:

```rust
pub async fn with_actor_scope<F, R>(actor: Option<i32>, future: F) -> R
where
    F: std::future::Future<Output = R>,
{
    AMBIENT_ACTOR.scope(actor, future).await
}
```

### Applying to Database Connections

Both `with_connection_timeout()` and `with_transaction()` read the ambient actor and apply it as a transaction-local `SET LOCAL`:

```rust
fn set_local_actor(conn: &mut PgConnection, actor: i32) -> Result<(), diesel::result::Error> {
    use diesel::RunQueryDsl;
    diesel::sql_query("SELECT set_config('hubuum.actor_id', $1, true)")
        .bind::<diesel::sql_types::Text, _>(actor.to_string())
        .execute(conn)?;
    Ok(())
}
```

The second parameter `true` to `set_config()` means the configuration is local to the transaction and reverts automatically at COMMIT/ROLLBACK, avoiding any leak back to the connection pool.

### Middleware Integration

The `actor_context` middleware in `src/middlewares/actor_context.rs` resolves the bearer token once per request and establishes the actor scope for the entire request handler:

```rust
pub async fn actor_context(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let resolved = resolve_auth(&req).await;
    let actor = match &resolved {
        ResolvedAuth::Authenticated { token_meta, .. } => Some(token_meta.principal_id),
        _ => None,
    };
    req.extensions_mut().insert(resolved);
    let res = with_actor_scope(actor, next.call(req)).await?;
    Ok(res.map_into_boxed_body())
}
```

### Execution Flow

1. **Request arrives** → middleware resolves the bearer token to a principal token.
2. **Actor scope established** → `with_actor_scope(Some(token_meta.principal_id), ...)` wraps the request handler.
3. **Handler executes** → any `with_connection()` or `with_transaction()` call inside reads the ambient actor.
4. **SET LOCAL applied** → at the start of the database operation, `SET LOCAL hubuum.actor_id` is executed.
5. **Trigger fires** → the history trigger reads `current_setting('hubuum.actor_id')` and records it.
6. **Transaction completes** → the `SET LOCAL` scope reverts.
7. **Outside any scope** → writes outside a request context (migrations, background jobs) record `actor_id = NULL`.

## Maintenance Contract

### Altering a Versioned Base Table

When you add, remove, or modify a column in a versioned base table (e.g., `hubuumclass`), you **must mirror the change in the corresponding history table in the same migration**. The trigger uses `LIKE <table>` to copy all columns, so the history table's schema must match the base table's.

**Example**: If you add a column `color` to `hubuumclass`:
```sql
ALTER TABLE hubuumclass ADD COLUMN color varchar;
ALTER TABLE hubuumclass_history ADD COLUMN color varchar;
```

### History Closure and ID Reuse

The trigger's `UPDATE` logic (lines 64, 70) that closes old history versions relies on a critical assumption:

```sql
UPDATE <table>_history SET valid_to = <ts> WHERE id = <id> AND valid_to IS NULL
```

This assumes **at most ONE open history version exists per base-table `id`** at any given time. This holds for the normal lifecycle of tables with serial primary keys where `id` values are never recycled.

**Future onboarding**: If a new versioned table reuses or recycles `id` values (for example, a table with a composite key or a non-monotonic primary key), this trigger will incorrectly close ALL open versions with that `id`, breaking the history integrity. Any such table would require a modified trigger that includes additional discriminating columns in the `WHERE` clause.

### Adding a New Versioned Table

To add history tracking to a new table:
1. Create the base table.
2. Add the table name to the `FOREACH` array in migration `up.sql`.
3. Re-run the dynamic SQL block in the migration to create the `_history` table, sequence, indexes, and trigger.

```sql
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'hubuumclass','hubuumobject','namespaces','hubuumclass_relation',
    'hubuumobject_relation','report_templates','remote_targets',
    'new_table'  -- Add here
  ]
  LOOP
    -- ... (same CREATE TABLE IF NOT EXISTS logic)
  END LOOP;
END $$;
```

## GDPR / Anonymization

### The Anonymization Contract

User PII (personally identifiable information) is **intentionally not versioned**. The `users` table does not have a history twin. This means:
- Old principal names, proper names, emails, and password hashes are **never recorded** in a history table.
- When a user is anonymized, their old PII leaves no persistent trace.

This achieves **pseudonymization** under GDPR Article 4(5): once a user is anonymized, history rows still reference them by a numeric actor ID, which is now divorced from any personal identity.

### The `anonymize_user()` Function

Located in `src/utilities/iam.rs`:

```rust
pub async fn anonymize_user(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::principals::dsl as p;
    use crate::schema::tokens::dsl as t;
    use crate::schema::users::dsl as u;

    with_transaction(pool, |conn| -> Result<(), ApiError> {
        diesel::update(u::users.filter(u::id.eq(target_id)))
            .set((
                u::proper_name.eq::<Option<String>>(None),
                u::email.eq::<Option<String>>(None),
                u::password.eq(ANONYMIZED_PASSWORD),  // "!anonymized-no-login"
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)?;
        diesel::update(p::principals.filter(p::id.eq(target_id)))
            .set(p::name.eq(format!("anonymized-{target_id}")))
            .execute(conn)?;
        diesel::update(
            t::tokens
                .filter(t::principal_id.eq(target_id))
                .filter(t::revoked_at.is_null()),
        )
        .set(t::revoked_at.eq(diesel::dsl::now))
        .execute(conn)?;
        Ok(())
    })
}
```

This function:
1. **Tombstones PII**: Clears `proper_name` and `email`, renames the principal to `anonymized-{id}`, and sets `password` to a sentinel string that fails all authentication checks.
2. **Stamps anonymization time**: Sets `anonymized_at` to the current timestamp.
3. **Revokes tokens**: Soft-revokes bearer tokens by setting `revoked_at`, forcing the user to log out while retaining token audit rows.
4. **Executes atomically**: All three updates happen in a single transaction.

### The Anonymization Endpoint

Exposed as `POST /api/v1/iam/users/{user_id}/anonymize` (admin-only):

```rust
#[post("/{user_id}/anonymize")]
pub async fn anonymize_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    let target_id = user_id.id();
    crate::utilities::iam::anonymize_user(&pool, target_id).await?;
    Ok(json_response(json!({}), StatusCode::NO_CONTENT))
}
```

### Why `users` is Not Versioned

The `users` table intentionally **lacks a history twin** because:
- History rows are **immutable audit records** created by triggers.
- PII in history would persist indefinitely, defeating GDPR compliance.
- Anonymization atomically updates the base table; there is no history to retroactively redact.
- Once anonymized, the `actor_id` references in other tables become pseudonymous—no way to link them back to a person.

### The `anonymized_at` Column

Added in migration `2026-06-29-000002_user_anonymized_at/up.sql`:

```sql
ALTER TABLE users ADD COLUMN anonymized_at TIMESTAMP NULL;
```

This column is NULL until anonymization occurs. It serves as:
- A **flag** indicating the user has been anonymized.
- An **audit timestamp** for when the anonymization was performed.
- A **query filter** for compliance reporting (e.g., "show all anonymized users in the past 30 days").

## Security Considerations

### Database Role Privilege Model

Run the Hubuum application under a **non-owning, unprivileged PostgreSQL role**:

```sql
CREATE ROLE hubuum_app NOINHERIT;
GRANT CONNECT ON DATABASE hubuum TO hubuum_app;
GRANT USAGE ON SCHEMA public TO hubuum_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO hubuum_app;
-- Do NOT grant UPDATE/DELETE on *_history tables
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO hubuum_app;
```

### History Table Protections

**DEPLOYMENT CHECKLIST REQUIREMENT**: History integrity depends on database-level enforcement. In production:

- **MUST NOT grant** `UPDATE` or `DELETE` on `_history` tables to the application role.
- Triggers insert into history tables; the application role should only have **SELECT** grants.
- This is the **only** defense against accidental or malicious modification of audit records by the application layer.

Verify in production:
```sql
SELECT grantee, privilege_type
FROM information_schema.role_table_grants
WHERE table_schema = 'public' AND table_name LIKE '%_history'
  AND grantee = 'hubuum_app';
-- Should show ONLY SELECT grants, NOT UPDATE or DELETE.
```

### Trigger Auditing Limitations

- Superusers and table owners can **bypass triggers** by disabling them or directly modifying history tables.
- This is an accepted limitation; secure access control at the PostgreSQL role level is the primary defense.
- For environments where superuser compromise is a real threat, consider:
  - Running the database under a separate, less-privileged superuser account.
  - Enabling PostgreSQL's event log to record DDL changes.
  - Archiving history tables to immutable storage (e.g., object storage, write-once tape).

### Actor Pseudonymization

- `actor_id` is a **plain integer**, not a foreign key to the `principals` or `users` table.
- No PostgreSQL constraint links history rows to the users table.
- Once a user is anonymized, their `actor_id` in history becomes a meaningless pseudonymous number.
- This is **not** the same as deleting the user row; the user record persists (for token validation, session management) but is pseudonymous.

## History Read API

The temporal history system now exposes read-only access to historical versions through two endpoints per resource:

### Endpoints

For each of the five versioned resources (classes, objects, namespaces, report templates, remote targets), two history endpoints are available:

#### 1. List History Versions (Cursor-Paginated)

Returns all historical versions for a specific entity, ordered newest-first by default.

**Endpoints:**
- `GET /api/v1/classes/{class_id}/history`
- `GET /api/v1/classes/{class_id}/{object_id}/history`
- `GET /api/v1/namespaces/{namespace_id}/history`
- `GET /api/v1/templates/{template_id}/history`
- `GET /api/v1/remote-targets/{remote_target_id}/history`

**Query Parameters:**
- `?sort=history_id` - Sort order (default: `history_id` descending for newest-first; ordering is chronological via the monotonic history_id)
- `?limit=N` - Number of results per page (default: 50, max: 500)
- `?cursor=<opaque>` - Pagination cursor from `X-Next-Cursor` header

**Response Headers:**
- `X-Total-Count` - Total number of history rows for this entity
- `X-Next-Cursor` - Opaque cursor for the next page (omitted on last page)

**Response Body:**
Each history row is wrapped in a `HistoryResponse` containing:
- All columns from the resource's base table (e.g., `id`, `name`, `created_at`, etc.)
- `op` (string): Operation type - `'I'` (INSERT), `'U'` (UPDATE), or `'D'` (DELETE)
- `valid_from` (timestamptz): When this version became active
- `valid_to` (timestamptz, nullable): When this version expired (NULL for current/open version)
- `actor_id` (int, nullable): User ID who performed the mutation (NULL for system/background writes)
- `actor_username` (string, nullable): Resolved username for `actor_id` (NULL when unavailable or anonymized)
- `history_id` (int64): Surrogate primary key for this history row

**Example:**
```json
[
  {
    "id": 42,
    "name": "updated-name",
    "created_at": "2026-06-30T10:00:00Z",
    "op": "U",
    "valid_from": "2026-06-30T12:00:00Z",
    "valid_to": null,
    "actor_id": 7,
    "actor_username": "alice",
    "history_id": 1234
  },
  {
    "id": 42,
    "name": "original-name",
    "created_at": "2026-06-30T10:00:00Z",
    "op": "I",
    "valid_from": "2026-06-30T10:00:00Z",
    "valid_to": "2026-06-30T12:00:00Z",
    "actor_id": 7,
    "actor_username": "alice",
    "history_id": 1200
  }
]
```

#### 2. Point-in-Time Snapshot (As-Of Query)

Returns the historical version of an entity that was valid at a specific instant.

**Endpoints:**
- `GET /api/v1/classes/{class_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/classes/{class_id}/{object_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/namespaces/{namespace_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/templates/{template_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/remote-targets/{remote_target_id}/history/as-of?at=<rfc3339>`

**Query Parameters:**
- `at=<rfc3339>` (required) - RFC 3339 timestamp (e.g., `2026-06-30T12:00:00Z`)

**Response:**
Returns a single `HistoryResponse` object with the same structure as the list endpoint. Returns 404 if no version existed at the specified timestamp.

**Example:**
```bash
GET /api/v1/classes/42/history/as-of?at=2026-06-30T11:00:00Z
```

### Access Control

History read access mirrors the base resource's Read permission:

- **Classes**: Requires `Permissions::ReadClass` on the class entity
- **Objects**: Requires `Permissions::ReadObject` on the object entity
- **Namespaces**: Requires `Permissions::ReadCollection` on the namespace entity
- **Report Templates**: Requires `Permissions::ReadTemplate` on the template's parent namespace
- **Remote Targets**: Requires `Permissions::ReadRemoteTarget` on the remote target's parent namespace

**Deleted Entity Handling:**
If an entity has been deleted from the base table, normal callers receive **404 Not Found** because there is no live row to authorize against. Unscoped admins may still read the deleted entity's history and delete tombstone through the same history endpoints for compliance/audit purposes.

**Known Limitation — Cross-Namespace History:**
Because permission is checked against the entity's CURRENT namespace, and an entity's `namespace_id` can change over time, the returned history may include versions (and the `namespace_id`) from when the entity lived in a different namespace. This means the history is visible to anyone who can read the entity's current namespace, even if those historical versions reflect a time when the entity was in a different namespace. This is an accepted limitation of the current permission model.

### Limitations and Future Work

#### Relation History (Deferred to Plan 2b)

The following relation tables are tracked in history but do NOT yet have read endpoints:
- `hubuumclass_relation_history`
- `hubuumobject_relation_history`

Exposing relation history is planned for a future release (Plan 2b).

#### Background Task Attribution

Writes performed by background tasks (e.g., imports, async jobs in `src/tasks`) currently record `actor_id = NULL`, even when the task was initiated by a user. This is a known limitation carried over from Plan 1. Future work (Plan 2c) will thread the originating user through task execution for proper attribution.

## References

- **Migrations**: `migrations/2026-06-29-000001_temporal_history/up.sql`, `migrations/2026-06-29-000002_user_anonymized_at/up.sql`
- **Database actor plumbing**: `src/db/mod.rs`
- **Request-scoped actor context**: `src/middlewares/actor_context.rs`
- **Anonymization logic**: `src/utilities/iam.rs`
- **Anonymization endpoint**: `src/api/v1/handlers/users.rs`
