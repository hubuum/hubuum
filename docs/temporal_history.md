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
- **`valid_from` (timestamptz)**: When this version became active. Uses `transaction_timestamp()` for precision.
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
- **NULL actor = system**: When `actor_id` is not set, it defaults to `NULL` in the history row, indicating a system or background operation.
- **Dynamic table name**: Using `TG_TABLE_NAME`, the function adapts to whichever table it's attached to, avoiding trigger duplication.

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
        ResolvedAuth::Authenticated { user, .. } => Some(user.id),
        _ => None,
    };
    req.extensions_mut().insert(resolved);
    let res = with_actor_scope(actor, next.call(req)).await?;
    Ok(res.map_into_boxed_body())
}
```

### Execution Flow

1. **Request arrives** → middleware resolves the bearer token to a `User`.
2. **Actor scope established** → `with_actor_scope(Some(user.id), ...)` wraps the request handler.
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
- Old usernames, emails, and password hashes are **never recorded** in a history table.
- When a user is anonymized, their old PII leaves no persistent trace.

This achieves **pseudonymization** under GDPR Article 4(5): once a user is anonymized, history rows still reference them by a numeric actor ID, which is now divorced from any personal identity.

### The `anonymize_user()` Function

Located in `src/utilities/iam.rs`:

```rust
pub async fn anonymize_user(pool: &DbPool, target_id: i32) -> Result<(), ApiError> {
    use crate::schema::tokens::dsl as t;
    use crate::schema::users::dsl as u;

    with_transaction(pool, |conn| -> Result<(), diesel::result::Error> {
        diesel::update(u::users.filter(u::id.eq(target_id)))
            .set((
                u::username.eq(format!("anonymized-{target_id}")),
                u::email.eq::<Option<String>>(None),
                u::password.eq(ANONYMIZED_PASSWORD),  // "!anonymized-no-login"
                u::anonymized_at.eq(diesel::dsl::now),
            ))
            .execute(conn)?;
        diesel::delete(t::tokens.filter(t::user_id.eq(target_id))).execute(conn)?;
        Ok(())
    })
}
```

This function:
1. **Tombstones PII**: Sets `username` to a placeholder, clears `email`, and sets `password` to a sentinel string that fails all authentication checks.
2. **Stamps anonymization time**: Sets `anonymized_at` to the current timestamp.
3. **Revokes tokens**: Deletes all bearer tokens for the user, forcing them to log out.
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

- **Do not grant** `UPDATE` or `DELETE` on `_history` tables to the application role in production.
- Triggers insert into history tables; the application should only **read** them.
- This prevents accidental or malicious modification of audit records by the application.

### Trigger Auditing Limitations

- Superusers and table owners can **bypass triggers** by disabling them or directly modifying history tables.
- This is an accepted limitation; secure access control at the PostgreSQL role level is the primary defense.
- For environments where superuser compromise is a real threat, consider:
  - Running the database under a separate, less-privileged superuser account.
  - Enabling PostgreSQL's event log to record DDL changes.
  - Archiving history tables to immutable storage (e.g., object storage, write-once tape).

### Actor Pseudonymization

- `actor_id` is a **plain integer**, not a foreign key to the `users` table.
- No PostgreSQL constraint links history rows to the users table.
- Once a user is anonymized, their `actor_id` in history becomes a meaningless number.
- This is **not** the same as deleting the user row; the user record persists (for token validation, session management) but is pseudonymous.

## Future Work

### Plan 2: Read Access to History

Time-travel queries and version listing are **deferred to a future release**. The foundation is in place:
- All tables are versioned with `valid_from/valid_to` for temporal queries.
- The `actor_id` is recorded for change attribution.
- The infrastructure to filter by timestamp or list versions across time is not yet implemented.

Future endpoints will likely include:
- `GET /api/v1/<resources>?as_of=<timestamp>` - reconstruct the state at a point in time.
- `GET /api/v1/<resource>/{id}/versions` - list all versions of a row with their operation, actor, and validity window.
- `GET /api/v1/<resource>/{id}/history/{history_id}` - fetch a specific historical version.

These will be added without modifying the data model, as the design already supports them.

## References

- **Migrations**: `migrations/2026-06-29-000001_temporal_history/up.sql`, `migrations/2026-06-29-000002_user_anonymized_at/up.sql`
- **Database actor plumbing**: `src/db/mod.rs`
- **Request-scoped actor context**: `src/middlewares/actor_context.rs`
- **Anonymization logic**: `src/utilities/iam.rs`
- **Anonymization endpoint**: `src/api/v1/handlers/users.rs`
