<!-- markdownlint-disable MD031 MD032 -->

# Temporal History, Actor Capture, and GDPR Anonymization

This document describes Hubuum's row-history mechanism, which records every
data modification to seven core tables through a generic PostgreSQL trigger,
preserves direct-user and asynchronous task provenance, and enables
GDPR-compliant user anonymization.

## Data Model

### History Tables

Every versioned table has a companion `<table>_history` table that records all mutations. For example:
- `hubuumclass` → `hubuumclass_history`
- `hubuumobject` → `hubuumobject_history`
- `collections` → `collections_history`
- `hubuumclass_relation` → `hubuumclass_relation_history`
- `hubuumobject_relation` → `hubuumobject_relation_history`
- `export_templates` → `export_templates_history`
- `remote_targets` → `remote_targets_history`

### Row Structure

Each history row is a **full-row snapshot** with the following columns:
- **All columns from the base table** (e.g., `id`, `name`, `created_at`, etc.)
- **`revision` (bigint)**: Positive authoritative revision represented by the
  snapshot. Delete tombstones retain the final live revision.
- **`op` (varchar)**: The operation performed. Valid values are:
  - `'I'` - INSERT
  - `'U'` - UPDATE
  - `'D'` - DELETE
- **`valid_from` (timestamptz)**: When this version became active. Uses `clock_timestamp()` so long transactions record the trigger execution time.
- **`valid_to` (timestamptz, nullable)**: When this version expired. NULL indicates the row is the current open version.
- **`actor_id` (int, nullable)**: The principal that performed the mutation
  when the actor is a user. Worker and system actors normally have no principal.
- **`history_id` (bigint, PK)**: Surrogate primary key for the history row itself, auto-incremented per table.
- **`actor_kind` (text, nullable)**: Immediate actor class: `user`, `worker`,
  or `system`. Unknown legacy rows remain `NULL`.
- **`initiator_user_id` (int, nullable)**: Durable, non-FK ID of the principal
  that submitted the root task.
- **`task_id` (int, nullable)**: Durable, non-FK root task ID.

The provenance IDs deliberately do not have foreign keys. Deleting a principal
or purging a task cannot erase attribution from immutable history.

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

## Historical computed fields

[Historical recomputation](computed_fields.md#historical-recomputation) uses the
object snapshot and the separately versioned complete shared definition set.
`computed_field_history` retains those sets over half-open intervals, including
observed empty sets. Definition history survives deletion and contains no
personal fields or owner/actor identities. It is not an object materialization
history. Definitions predating migration, omitted by backup or removed by a
future retention policy remain explicitly unavailable.

## Lightweight Timeline Navigation

Class and object history can be browsed without fetching every full snapshot:

| Operation | Class route | Object route |
| --- | --- | --- |
| Timeline | `/api/v1/classes/{class_id}/history/timeline` | `/api/v1/classes/{class_id}/{object_id}/history/timeline` |
| Exact entry | `/api/v1/classes/{class_id}/history/entries/{history_id}` | `/api/v1/classes/{class_id}/{object_id}/history/entries/{history_id}` |

Timeline responses are arrays containing resource identity, historical name and
collection/class identity, `history_id`, `revision`, `op`, `valid_from`, `valid_to`,
and actor/initiator/task provenance. They omit `data` and `json_schema`. Exact
entry responses have the same full snapshot shape as the existing history APIs.
Successful responses use `Cache-Control: private, no-store`.

### Selection and Pagination

| Parameter | Meaning |
| --- | --- |
| `direction` | `older` (default) orders descending; `newer` orders ascending. Both order by `(valid_from, history_id)`. |
| `at` | Exact RFC3339 instant. `older` includes entries whose `valid_from <= at`; `newer` includes entries whose `valid_from > at`. |
| `anchor` | Authorized history entry in this resource. Return entries strictly before/after its `(valid_from, history_id)` position. Mutually exclusive with `at`. |
| `from`, `until` | Optional creation-time window: `from <= valid_from < until`. When both are supplied, `from` must precede `until`. |
| `limit` | Positive page size, using the configured default and maximum. |
| `cursor` | Opaque `X-Next-Cursor` value from the previous page. Preserve the resource, direction, window and anchor when continuing. |
| `include_total` | Defaults to `false`. `true` returns `X-Total-Count` for authorized entries matching the window and selection, independent of the cursor. |

The effective limit is returned in `X-Page-Limit`. `X-Next-Cursor` is omitted
when no more authorized entries remain in that direction. Generic `sort` and
filter parameters are not supported by timeline routes.

Timestamp ties are ordered by history identity. Keep server timestamps as exact
strings; do not round them through a JavaScript `Date` before sending them back.
An anchor selects neighbors without timestamp arithmetic. Inserts newer than a
descending cursor do not duplicate or shift earlier entries; refresh the first
page to discover newly committed versions. Pagination is not a frozen snapshot
across requests, and authorization is evaluated on every request.

### Frontend Integration

1. Load a timeline page for markers and snapshot covers.
2. Fetch `entries/{history_id}` only for the selected snapshot and comparison
   baseline. Prefetch a bounded number of neighbors if useful.
3. On a date jump, use the existing `history/as-of?at=...` route for the active
   snapshot. Load surrounding markers with `timeline?at=...&direction=older`
   and `timeline?at=...&direction=newer`.
4. For exact bookmarks or pinned comparisons, use resource identity plus
   `history_id`. Fetch that entry directly, then use `anchor={history_id}` for
   its older and newer neighbors.

For the first/last available authorized marker, request `limit=1` with
`direction=newer`/`direction=older` and no selection or window. This avoids
requiring an exact count or loading all versions just to establish bounds.
Full-document comparisons and change summaries remain opt-in client work;
listing markers never computes document diffs. Existing clients can continue
using the original full-row history list and as-of endpoints.

### Identity, Visibility, and Availability

A history ID is table-specific. Exact lookup always constrains it by resource
kind and resource ID; object routes also constrain the stored class ID. A
revision alone cannot identify a deletion marker because the marker retains
the final live revision. Exact lookup can return a tombstone even though as-of
lookup cannot return a zero-width active interval.

Current resource access is required before navigation. Historical versions must
also pass checks using their own stored collection, class, and name. Hidden
entries do not contribute markers, totals, cursors, or availability bounds.
Only unscoped administrators may browse deleted-resource history, matching the
existing policy. Inaccessible, missing, wrong-resource, and unavailable exact
entries or anchors return the same `404` response after the live-resource gate.

Empty windows return an empty page for a known authorized resource. Bounds
describe available authorized data; they do not prove an uninterrupted history
or establish a retention cutoff. Until retention provenance is implemented in
[#251](https://github.com/hubuum/hubuum/issues/251), a missing version is not
labelled expired and no `410` response is inferred from the oldest visible row.

### Storage and Performance

Both PostgreSQL and memory implement the navigation contract. PostgreSQL selects
only marker and authorization fields and uses indexes beginning with the resource
identity, followed by `valid_from` and `history_id`. Apply the navigation-index
migration through the normal one-shot administrator migration process during
maintenance: its transactional index builds briefly block history writes. Lock
acquisition is limited to five seconds and each statement to sixty seconds;
failure rolls back both indexes. Test the migration against a representative
database before upgrading a deployment with very large history tables.

The regression suite exercises deep index seeks on 12,000 entries with 64 KiB
JSON payloads, in both directions and with tied timestamps. It checks bounded
query-plan work and compares full-row and marker payload sizes. HTTP tests also
check that schemas and object data never appear in marker responses. These
checks establish payload/query bounds; they are not a production latency claim.

A PostgreSQL 18 measurement using these installed indexes and 12,000 versions
with 64 KiB JSON snapshots produced the following uncompressed `row_to_json`
sizes for 25 entries:

| Resource | Full rows (bytes) | Marker projection (bytes) | Reduction |
| --- | --- | --- | --- |
| Class | 1,647,098 | 5,898 | 99.64% |
| Object | 1,646,848 | 5,898 | 99.64% |

Deep 25-entry seeks in both directions used index scans, visited 25 rows, and
touched 5–6 local buffer blocks, without a sort or sequential scan. These
synthetic database measurements exclude HTTP provenance enrichment and transport
compression; the API projection/count tests separately verify the real request
path does not load JSON snapshots or run an unrequested history count.

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
  actor_kind_value text := nullif(current_setting('hubuum.actor_kind', true), '');
  initiator int := nullif(current_setting('hubuum.initiator_user_id', true), '')::int;
  provenance_task_id int := nullif(current_setting('hubuum.task_id', true), '')::int;
BEGIN
  IF TG_OP = 'INSERT' THEN
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id, actor_kind, initiator_user_id, task_id) SELECT <base values>, %L, $2, NULL, $3, nextval(%s), $4, $5, $6', hist, 'I', seq)
      USING NEW, ts, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSIF TG_OP = 'UPDATE' THEN
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id, actor_kind, initiator_user_id, task_id) SELECT <base values>, %L, $2, NULL, $3, nextval(%s), $4, $5, $6', hist, 'U', seq)
      USING NEW, ts, actor, actor_kind_value, initiator, provenance_task_id;
    RETURN NEW;
  ELSE  -- DELETE
    EXECUTE format('UPDATE %s SET valid_to=$1 WHERE id=$2 AND valid_to IS NULL', hist)
      USING ts, OLD.id;
    EXECUTE format('INSERT INTO %s (<base columns>, op, valid_from, valid_to, actor_id, history_id, actor_kind, initiator_user_id, task_id) SELECT <base values>, %L, $2, $2, $3, nextval(%s), $4, $5, $6', hist, 'D', seq)
      USING OLD, ts, actor, actor_kind_value, initiator, provenance_task_id;
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
- **Transaction-local provenance**: The trigger reads actor kind, actor
  principal, initiator principal, and task ID from PostgreSQL GUCs that revert
  at commit or rollback.
- **No guessing**: Writes outside a typed provenance scope retain `NULL`
  attribution. Legacy rows with no actor or task evidence remain unknown.
- **Worker attribution**: Task execution runs in a worker provenance scope.
  Import-created versions therefore record `actor_kind = 'worker'`, the
  submitting principal as initiator, and the task ID.
- **Dynamic table name**: Using `TG_TABLE_NAME`, the function adapts to whichever table it's attached to, avoiding trigger duplication.

### No-Op Updates

For temporal domain tables (`hubuumclass`, `hubuumobject`, `collections`,
`export_templates`, and `remote_targets`), an `UPDATE` whose domain data is
identical to the existing row is suppressed by a `BEFORE UPDATE` trigger.
`updated_at` is intentionally excluded from the comparison.

This means `updated_at` records when the persisted data last changed. A repeated
PATCH/import/update with the same values does not bump `updated_at`, does not
create a `U` history row, and does not create a new temporal version boundary.
If callers need to know when someone attempted an unchanged write, that belongs
in an audit/event stream rather than in the row's temporal state.

## Actor Capture

### Ambient Mutation-Provenance Task-Local

In `crates/hubuum-storage-postgres/src/runtime.rs`, an async task-local
(`tokio::task_local!`) variable stores typed mutation provenance:

```rust
tokio::task_local! {
    static AMBIENT_MUTATION_PROVENANCE: Option<MutationProvenance>;
}
```

`MutationProvenance` distinguishes:

- the immediate `actor_kind`
- the optional actor principal
- the optional root-task initiator
- the optional root task ID

### Setting the Provenance Scope

The `with_mutation_provenance()` helper establishes provenance for the
duration of a future:

```rust
pub async fn with_mutation_provenance<F>(
    provenance: Option<MutationProvenance>,
    future: F,
) -> F::Output
where
    F: Future,
{
    AMBIENT_MUTATION_PROVENANCE.scope(provenance, future).await
}
```

### Applying to Database Connections

Both `PostgresRuntime::with_connection()` and
`PostgresRuntime::with_transaction()` apply all four values as
transaction-local settings with bound parameters. The equivalent database
operation is:

```sql
SELECT
  set_config('hubuum.actor_kind', $1, true),
  set_config('hubuum.actor_id', $2, true),
  set_config('hubuum.initiator_user_id', $3, true),
  set_config('hubuum.task_id', $4, true);
```

The second parameter `true` to `set_config()` means the configuration is local to the transaction and reverts automatically at COMMIT/ROLLBACK, avoiding any leak back to the connection pool.

### Request and Worker Integration

The `actor_context` middleware resolves the bearer token once and wraps the
request in `MutationProvenance::user(principal_id)`. Task workers wrap
execution and failure finalization in
`task.worker_provenance()`.

### Execution Flow

1. **Request arrives** → middleware resolves the bearer token to a principal token.
2. **Provenance scope established** → a user or worker scope wraps the mutation.
3. **Handler or worker executes** → database helpers read the ambient provenance.
4. **Transaction-local settings applied** → all available provenance values
   are set at the start of the database operation.
5. **Trigger fires** → the history trigger records those durable values.
6. **Transaction completes** → the `SET LOCAL` scope reverts.
7. **Outside any scope** → writes retain unknown (`NULL`) provenance.

History API responses preserve `actor_id`, `actor_kind`, and `actor_username`
for compatibility and add the shared `provenance` object used by task and
event responses. Actor and initiator IDs are unioned and name-resolved with one
query per page.

## Maintenance Contract

### Altering a Versioned Base Table

When you add, remove, or modify a column in a versioned base table (e.g., `hubuumclass`), you **must mirror the change in the corresponding history table in the same migration**. The trigger uses `LIKE <table>` to copy all columns, so the history table's schema must match the base table's.

**Example**: If you add a column `color` to `hubuumclass`:
```sql
ALTER TABLE hubuumclass ADD COLUMN color varchar;
ALTER TABLE hubuumclass_history ADD COLUMN color varchar;
```

### History Closure and ID Reuse

The trigger's `UPDATE` logic that closes superseded history versions relies on a critical assumption:

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
    'hubuumclass','hubuumobject','collections','hubuumclass_relation',
    'hubuumobject_relation','export_templates','remote_targets',
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
pub async fn anonymize_user(pool: &PostgresPool, target_id: i32) -> Result<(), ApiError> {
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
    pool: web::Data<PostgresPool>,
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

The greenfield schema defines `users.anonymized_at`:

```sql
ALTER TABLE users ADD COLUMN anonymized_at TIMESTAMP NULL;
```

This column is NULL until anonymization occurs. It serves as:
- A **flag** indicating the user has been anonymized.
- An **audit timestamp** for when the anonymization was performed.
- A **query filter** for compliance exporting (e.g., "show all anonymized users in the past 30 days").

## Security Considerations

### Database Role Privilege Model

For database-enforced separation, opt into
`HUBUUM_DATABASE_ROLE_MODE=split` and run the application under a
**non-owning, unprivileged PostgreSQL role**. Hubuum's generated privilege
manifest supplies the exact grants; this simplified example illustrates the
boundary:

```sql
CREATE ROLE hubuum_app NOINHERIT;
GRANT CONNECT ON DATABASE hubuum TO hubuum_app;
GRANT USAGE ON SCHEMA public TO hubuum_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO hubuum_app;
-- Do NOT grant UPDATE/DELETE on *_history tables
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO hubuum_app;
```

The default `single` topology intentionally uses one login for runtime and
schema management. It remains supported, but that login can alter or own
database objects, so it does not provide this defense against direct database
credential compromise. See [PostgreSQL Database Roles](database_roles.md).

### History Table Protections

In split-role production deployments, history integrity depends on
database-level enforcement:

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

For each of the five versioned resources (classes, objects, collections, export templates, remote targets), two history endpoints are available:

#### 1. List History Versions (Cursor-Paginated)

Returns the historical versions the caller may read for a specific entity,
ordered newest-first by default.

**Endpoints:**
- `GET /api/v1/classes/{class_id}/history`
- `GET /api/v1/classes/{class_id}/{object_id}/history`
- `GET /api/v1/collections/{collection_id}/history`
- `GET /api/v1/export-templates/{template_id}/history`
- `GET /api/v1/remote-targets/{remote_target_id}/history`

**Query Parameters:**
- `?sort=history_id` - Sort order (default: `history_id` descending for newest-first; ordering is chronological via the monotonic history_id)
- `?limit=N` - Number of results per page (default: 50, max: 500)
- `?cursor=<opaque>` - Pagination cursor from `X-Next-Cursor` header
- `?include_total=false` - Skip the exact count query and omit `X-Total-Count`

**Response Headers:**
- `X-Total-Count` - Total number of visible history rows for this entity (omitted when `include_total=false`)
- `X-Next-Cursor` - Opaque cursor for the next page (omitted on last page)

**Response Body:**
Each history row is wrapped in a `HistoryResponse` containing:
- All columns from the resource's base table (e.g., `id`, `name`, `created_at`, etc.)
- `revision` (int64): Positive authoritative revision for the snapshot
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

#### Exact-Name Object History Across Identities

```http
GET /api/v1/classes/42/objects/history?name=server01&limit=50
```

This read-only endpoint selects full retained object snapshots by **stored class
ID and exact stored name**, across deletion and recreation with different object
IDs. It does not resolve a live object first. Names are case-sensitive; numeric
names such as `123` remain names. Supply `name` exactly once, URL-encoded as a
query value (including commas, slashes, spaces, and Unicode). An empty name is
also an exact value. Class-name reuse across different class IDs is not followed.

If object 123 named `server01` is deleted and object 456 is created with the same
name, an authorized query includes both identities' matching versions and 123's
delete tombstone. If 456 is renamed to `server02`, its new version is excluded;
renaming back adds matching versions again. The existing ID history endpoint
continues to follow one identity through all its names.

The response uses the existing full `HistoryResponse<HubuumObjectHistory>` shape:
`id` is the object ID, `history_id` identifies the exact returned snapshot,
`hubuum_class_id` and `collection_id` are historical coordinates, and `name`,
`revision`, `op`, `valid_from`, `valid_to`, `data`, and provenance describe that
version. No follow-up snapshot fetch is required. Delete tombstones retain the
final live revision, so revision alone is not a unique history-entry identifier.

Clients use the standard `X-Next-Cursor`, `X-Page-Limit`, and optional
`X-Total-Count` headers. Default ordering is descending `history_id`; `sort`
accepts `history_id` or `revision` with an optional `-` prefix. `history_id` breaks
revision ties across identities, independently of timestamp ties. Revision
filters use the existing history syntax. `include_total=false` skips the exact
total and stops authorization after a page plus look-ahead. Cursors are specific
to the class, exact name, revision filters, normalized ordering, and authorization
token and its permission/resource scopes; changing any of these requires
starting without a cursor. Routine token credential migration preserves cursors.

Each live identity must pass its current `ReadObject` check and every returned
version must pass the historical snapshot check, including the token's resource
boundary and external policies. A readable replacement never authorizes its
predecessor. **Deleted identities remain visible only to unscoped administrators**,
as with ID history. An object moved to another class remains a live identity: its
actual current placement and the retained snapshot must both be readable, including
for external-policy administrators. Scoped administrators cannot read deleted predecessors. Hidden versions
contribute no rows, totals, or cursor boundaries. The endpoint returns `200` with
an empty array when no authorized retained versions match, including unknown
class IDs or names with no live object. A token excluding `ReadObject` gets `403`.

This is historical state lookup. Audit search separately selects actions under
`ReadAudit` and can have different retention and snapshot redaction rules.
Rust clients and CLI implementations should offer explicit ID and name selectors,
preserve the returned object IDs, reuse the full snapshot type, and treat cursor
values as opaque. The CLI syntax in issue #471 remains client follow-up work.

#### 2. Point-in-Time Snapshot (As-Of Query)

Returns the historical version of an entity that was valid at a specific instant.

**Endpoints:**
- `GET /api/v1/classes/{class_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/classes/{class_id}/{object_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/collections/{collection_id}/history/as-of?at=<rfc3339>`
- `GET /api/v1/export-templates/{template_id}/history/as-of?at=<rfc3339>`
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

History read access mirrors the base resource's Read permission. Live resources
must pass the normal current-resource check. Each returned historical version
must also pass authorization using the collection, name, and class attributes
stored in that version:

- **Classes**: Requires `Permissions::ReadClass` on the class entity
- **Objects**: Requires `Permissions::ReadObject` on the object entity
- **Collections**: Requires `Permissions::ReadCollection` on the collection entity
- **Export Templates**: Requires `Permissions::ReadTemplate` on the template's parent collection
- **Remote Targets**: Requires `Permissions::ReadRemoteTarget` on the remote target's parent collection

**Deleted Entity Handling:**
If an entity has been deleted from the base table, normal callers receive **404 Not Found** because there is no live row to authorize against. Unscoped admins may still read the deleted entity's history and delete tombstone through the same history endpoints for compliance/audit purposes.

**Cross-Collection History:**
When an entity moves between collections, list endpoints omit versions from
collections the caller cannot read. Counts and cursors are computed after this
visibility filter. As-of endpoints authorize only the version selected by the
requested timestamp and return **403 Forbidden** when that historical version
is not readable.

### Direct Relationship History

Direct relationship history is available independently of live adjacency:

- `GET /api/v1/classes/{class_id}/relations/history`
- `GET /api/v1/classes/{class_id}/relations/history/as-of?at=<RFC3339>`
- `GET /api/v1/classes/{class_id}/{object_id}/relations/history`
- `GET /api/v1/classes/{class_id}/{object_id}/relations/history/as-of?at=<RFC3339>`

The change endpoints include retained inserts, updates, and deletion markers.
As-of endpoints require exactly one `at` and select active non-deleted versions
with `valid_from <= at < valid_to` (null means open). Nanosecond request precision
is preserved when comparing PostgreSQL's microsecond boundaries. Recreating an
edge produces a different relationship ID; deletion markers do not create active
adjacency. These endpoints return direct edges only.

All four accept `limit`, `cursor`, `include_total` (default true), and
`sort=history_id` or `sort=-history_id` (default). Other filters and sorts are
rejected. Continuations are bound to the root, selected instant or change stream,
ordering, principal, token, and token scope. `include_total=false` omits the total
header and stops scanning once an authorized page plus look-ahead is found.

The body contains `at` (null for a change stream), `relations`, and
`coverage: "retained_history"`. Each entry has `kind: "class"` or `"object"`, the
stored `relation`, revision, history ID, operation, validity, durable provenance,
and `context_at`. Relationship changes resolve endpoint context at their
`valid_from`; as-of reads resolve it at the requested `at`. Class relationship
aliases and limits come from that relationship version. Object relationship
`class_relation_id` remains a historical reference without expanded class-edge
labels. Collection and class IDs are historical references; collection names,
object payloads, computed values, and schemas are not expanded.

The `from` and `to` contexts have `status: "available"` with a lightweight
`snapshot` containing the historical name, identity, class (for objects),
collection, history ID, revision, and validity. An authorized endpoint with no
retained effective version has `status: "unavailable"` and
`reason: "history_unavailable"`. Current rows never fill a historical gap.
An empty retained page does not prove that no relationship existed.

Current and historical relationship and endpoint permissions are checked before
rows, counts, or cursors are exposed. Class reads require `ReadClassRelation` and
`ReadClass`; object reads require `ReadObjectRelation` and `ReadObject`. Token
resource scopes must cover both endpoints. A live endpoint moved to an unreadable
collection hides the edge, including its older versions. A deleted edge or
endpoint requires an unscoped administrator, following the existing deleted
history policy; surviving endpoints still pass their current and historical
checks. Missing historical endpoint context is also restricted to an unscoped
administrator. External policy selection is preserved. Object routes use the
current class for a live object; retained context can describe an older class.
A deleted root must have retained object history in the requested class; an
unrelated class or nonexistent root returns `404`, including for administrators.

Both adapters bound candidate pages and endpoint hydration. PostgreSQL reads each
page and its endpoint batch in one read-only snapshot, using two disjoint indexed
endpoint branches and one batched historical endpoint query. Exact authorized
counts may scan all candidates in bounded pages. Deploy the relationship-history
index migration during maintenance: transactional index builds use a five-second
lock timeout and a sixty-second statement timeout and temporarily block writes.
Existing live relationship endpoints and their response shapes are unchanged.

#### Background Task Attribution

Writes performed by background tasks (e.g., imports, async jobs in `src/tasks`) currently record `actor_id = NULL`, even when the task was initiated by a user. This is a known limitation carried over from Plan 1. Future work (Plan 2c) will thread the originating user through task execution for proper attribution.

## References

- **Schema**: `crates/hubuum-storage-postgres/migrations/2023-12-27-011440_initial/up.sql`
- **Database actor plumbing**: `crates/hubuum-storage-postgres/src/runtime.rs`
- **Request-scoped actor context**: `src/middlewares/actor_context.rs`
- **Anonymization logic**: `src/utilities/iam.rs`
- **Anonymization endpoint**: `src/api/v1/handlers/users.rs`

## Historical class object populations

`GET /api/v1/classes/{class_id}/objects/history/as-of?at=<RFC3339>` lists
objects that belonged to the class at the requested instant. It reads each
object's retained `hubuum_class_id` and half-open validity interval
(`valid_from <= at < valid_to`, with an open upper bound for current versions).
Deletion markers do not represent active objects. Recreating a name creates a
separate identity; current names, membership, data and computed values are never
substituted for historical fields.

```http
GET /api/v1/classes/12/objects/history/as-of?at=2026-10-18T10:00:00Z&limit=25&include_total=false
Authorization: Bearer <token>
```

The response contains `at`, `class`, `objects` and
`coverage: "retained_history"`. `class` is the class history response, including
its stored JSON schema, effective at the same instant. `objects` contains normal
object history responses, with stable object/history IDs, revision, validity,
provenance and stored fields. Collection IDs are historical references;
expanded collection names and metadata are not included. Clients needing them
must use the collection as-of API at the same instant and treat unavailable
context explicitly. Computed values are unavailable on this endpoint.

The caller needs current and historical class read access and current and
historical object read access. External policy attributes and token resource
scopes apply to both shapes. Moving an object does not preserve access through
its old collection. Only unscoped administrators can include subsequently deleted
objects or query a deleted class. Hidden objects contribute neither rows nor
counts nor cursor boundaries.

Pagination uses the standard headers and configured `limit` bounds (defaults:
100 rows, maximum 250),
`cursor` and `include_total` (default true). Totals count authorized retained
objects across all pages; `include_total=false` omits the total header and avoids
a full count. Supported sorting is `history_id` (descending by default) and
`revision`, with a unique history-ID tie-breaker; prefix a sort field with `-`
for descending. Revision filters use the standard numeric operators, applied
only to the effective historical version. Other filters, including live JSON or
computed filters, are rejected. Cursors bind the class, full-precision instant,
filters, ordering, principal and token scope. Changing any of these requires a
new first page; changing the page size does not.

A missing class snapshot returns `404` with no claim about whether it never
existed or its history was removed. Successful results describe retained
versions only. An empty page or zero total does **not** establish that a class
was historically empty; retained-history completeness and expiry are not yet
tracked (see retention issue #251). Authorization is evaluated again on each
page, so changing access can change subsequent results.

The PostgreSQL adapter uses class/history-ID and validity-range
indexes, applies count and page selection in one read snapshot, and fetches only
the requested bounded page. The memory adapter retains only bounded candidate
keys and the selected snapshots. External authorization fetches candidates in
bounded batches and never hydrates a whole class. Optional exact counts can
still require visiting all matching candidates. Apply the new migration with
`hubuum-admin --migrate` during maintenance before starting the upgraded server.
Both index builds share a transaction and temporarily block writes. Lock
acquisition is limited to 5 seconds and each statement to 60 seconds; failure
rolls back both builds. Check build duration on representative history tables
before upgrading.
