# Temporal History with GDPR-aware Actor Tracking — Design

- **Date:** 2026-06-25
- **Status:** Approved (pending implementation plan)
- **Scope:** Add system-versioned ("temporal") history to hubuum's core domain
  tables, capturing *what* changed, *when*, and *who* (actor) — in a way that
  survives GDPR erasure of the actor.

## Goal

Provide django-simple-history-style capabilities for hubuum:

- A full version history per row of each core domain entity.
- Point-in-time reconstruction (`as_of`) and a per-entity changelog
  (list-versions).
- Attribution of each change to the acting user, stored **pseudonymously** so
  that erasing a user's PII does not require rewriting history.

Non-goals (explicitly out of scope for this work):

- Versioning auth/bookkeeping tables (users, tokens, groups, permissions,
  tasks, task_events, import/report bookkeeping).
- Auditing `SELECT`/read access (not possible with triggers).
- A history UI.

## Decisions (settled during brainstorming)

1. **Actor storage:** pseudonymous integer `user_id`, stored as a plain column,
   **not** a foreign key. PII (username/email) lives only in `users`.
2. **Implementation strategy:** hand-rolled PostgreSQL triggers writing to
   per-table full-snapshot history tables. **No** `temporal_tables` extension
   (avoids a deployment dependency).
3. **Trigger structure:** a single generic PL/pgSQL trigger function, attached
   per table, keyed on `TG_TABLE_NAME` with a `<table>_history` naming
   convention.
4. **Read API:** both `as_of` (point-in-time) and list-versions per resource.
5. **Erasure:** include a GDPR anonymization mechanism now (not just an
   erasure-safe schema).
6. **No new workspace crate.** The feature is intrinsically coupled to hubuum's
   diesel schema and connection layer; there is no dependency-light,
   primitive-in/primitive-out kernel to extract (unlike `hubuum-outbound-http`
   / `hubuum-templates`). Revisit only if a portable piece emerges.

## Table scope (initial)

Core domain models receive history:

- `hubuumclass`
- `hubuumobject`
- `namespaces`
- `hubuumclass_relation`
- `hubuumobject_relation`
- `report_templates`
- `remote_targets`

More tables can be added later by creating a `<table>_history` twin + sequence
+ trigger (one migration each).

> **Implementation note:** the generic trigger assumes a single-column integer
> `id` primary key on every versioned table. All seven are expected to have it;
> verify the two relation tables (`hubuumclass_relation`,
> `hubuumobject_relation`) during implementation.

## Section 1 — Data model (history tables)

For each in-scope table `T`, create a twin `T_history`:

```sql
CREATE TABLE <T>_history (
  LIKE <T>,                       -- all base columns, same names/types/NOT NULL, NO defaults/PK
  op          char(1) NOT NULL CHECK (op IN ('I','U','D')),
  valid_from  timestamptz NOT NULL,
  valid_to    timestamptz,        -- NULL = current open version
  actor_id    int,                -- pseudonymous; NOT a foreign key
  history_id  bigint NOT NULL     -- surrogate PK, filled via sequence in the trigger
);
CREATE SEQUENCE <T>_history_seq OWNED BY <T>_history.history_id;
ALTER TABLE <T>_history ADD PRIMARY KEY (history_id);
CREATE INDEX <T>_history_id_from_idx ON <T>_history (id, valid_from);
CREATE INDEX <T>_history_actor_idx  ON <T>_history (actor_id);
```

Semantics (full-snapshot model):

- **INSERT** → one open row, `op='I'`, `valid_from=ts`, `valid_to=NULL`.
- **UPDATE** → close the prior open row (`valid_to=ts`), insert a new open row
  `op='U'`.
- **DELETE** → close the prior open row (`valid_to=ts`), insert a zero-width
  tombstone row `op='D'` with `valid_from=valid_to=ts` recording the deleter.
- Current open version always has `valid_to IS NULL`.

Rationale for key choices:

- **Surrogate `history_id` PK** (instead of `(id, valid_from)`) avoids
  same-transaction primary-key collisions when a row is created-then-updated or
  updated twice within one transaction (these share `transaction_timestamp()`).
- **`actor_id` is a plain int, not an FK** — anonymizing or deleting a user
  never cascades into or corrupts history.
- **Base tables and their diesel `Queryable`/`Insertable`/`AsChangeset`
  structs are untouched.** History is purely additive in separate tables.

**Maintenance contract:** because history mirrors base columns positionally
(via `LIKE` and `($row).*` in the trigger), any future migration that `ALTER`s
a versioned base table MUST mirror the column change in its `_history` table in
the same migration. Documented in `docs/temporal_history.md`.

## Section 2 — Generic trigger + actor capture (PostgreSQL)

One generic function, attached to each versioned table:

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

CREATE TRIGGER <T>_history_trg AFTER INSERT OR UPDATE OR DELETE ON <T>
  FOR EACH ROW EXECUTE FUNCTION hubuum_record_history();
```

- Reads the actor from the `hubuum.actor_id` session GUC; absent/empty → `NULL`
  (= system / migration / background change).
- `AFTER` triggers fire on **cascade deletes** too, so child rows
  (e.g. classes/objects under a deleted namespace) get history rows carrying the
  same actor.
- `($row).*` expands the base columns positionally; the history table's leading
  columns must therefore match the base table's column order (guaranteed by
  `LIKE` + the maintenance contract).

## Section 3 — Actor threading + single auth lookup (Rust)

### `src/db/mod.rs` — ambient actor

Mirror the existing `AMBIENT_STATEMENT_TIMEOUT` machinery:

- Add `tokio::task_local!` `AMBIENT_ACTOR: Option<i32>` and
  `pub async fn with_actor_scope(actor: Option<i32>, fut) -> R`.
- In `with_connection_timeout` / `with_transaction` (which already open a
  transaction to scope `SET LOCAL`), additionally emit
  `SELECT set_config('hubuum.actor_id', $1, true)` when an ambient actor is
  present, bound (not formatted) like the timeout value.
- Reading uses `AMBIENT_ACTOR.try_with(...)`, matching
  `ambient_statement_timeout()`.

Propagation is sound because `with_connection`/`with_transaction` are
synchronous calls invoked inline on the same task as the handler future; no
`spawn_blocking`/`web::block` is used for DB work (same property the existing
timeout scope relies on).

### Auth middleware (single lookup, shared with extractors)

Implemented via `actix_web::middleware::from_fn`, running once per request:

- Extract the bearer token. If present, resolve it **once** through the existing
  `is_valid` → `get_user_by_id` path.
- Store the outcome in request extensions as an enum (`ResolvedAuth`) so the
  current error semantics are preserved:
  - `Authenticated { token: Token, user: User }`
  - `Missing` (no `Authorization` header) → `Unauthorized("No token provided")`
  - `Invalid` (present but bad/expired) → `Unauthorized("Invalid token")`
- When `Authenticated`, wrap the downstream handler future in
  `with_actor_scope(Some(user.id))`; otherwise run with no actor (`NULL`).

### Extractors refactored

`UserAccess`, `AdminAccess`, `AdminOrSelfAccess` (`src/extractors/mod.rs`):

- Stop calling `extract_user_from_token`; read `ResolvedAuth` from request
  extensions and map `Missing`/`Invalid` to the existing errors.
- `AdminAccess` still performs `is_admin`; `AdminOrSelfAccess` still performs
  its path-user check (that path-user lookup remains — it is a *different*
  user). Both reuse the already-resolved acting `user`.
- Requires `User` and `Token` to be `Clone` (they are).

**Net:** exactly one token→user resolution per request, shared by actor capture
and authorization. The actor middleware adds zero extra DB hits and removes the
previous per-extractor lookup.

## Section 4 — Read API (`as_of` + list-versions)

Per versioned resource:

- `GET /api/v1/<resource>/{id}/history` → versions newest-first, each
  `{op, valid_from, valid_to, actor_id, actor_username?, ...snapshot}`,
  cursor-paginated (reuse existing pagination). `actor_username` comes from a
  `LEFT JOIN users` and is `null` when the user was anonymized or deleted.
- `GET /api/v1/<resource>/{id}?as_of=<rfc3339>` → the snapshot valid at that
  instant (`valid_from <= T AND (valid_to IS NULL OR valid_to > T)`), or `404`
  if the entity did not exist then. Zero-width `D` tombstones never match
  `as_of`.

Diesel layer:

- One `Queryable` history struct per table (base columns + `op`, `valid_from`,
  `valid_to`, `actor_id`).
- `list_history(id)` and `get_as_of(id, ts)` in the relevant `src/db/traits/*`.
- Regenerate `src/schema.rs` after the migration (the history tables and the
  new `users.anonymized_at` column appear there).

## Section 5 — Anonymization (GDPR erasure)

- Add `anonymized_at timestamptz NULL` to `users` (lets the API render an actor
  as "anonymized"). `users` is **not** versioned, so the old username/email
  leaves no historical trace.
- Admin-only `POST /api/v1/users/{id}/anonymize`, in one transaction:
  - `username = 'anonymized-' || id` (preserves NOT NULL + uniqueness),
  - `email = NULL`,
  - blank/disable `password` (prevents login),
  - `anonymized_at = now()`,
  - delete the user's `tokens` (kills active sessions).
- History tables are untouched — they only ever held the integer `actor_id`,
  which now resolves to a tombstoned user (or a `null` username). This is
  pseudonymization under GDPR Art. 4(5).

## Section 6 — Error handling, edge cases, security, testing

### Edge cases

- Writes outside a request scope (migrations, background tasks, CLI) →
  `actor_id = NULL` (system change). Background tasks may opt in later by
  establishing `with_actor_scope`.
- Transaction rollback discards `AFTER`-trigger history rows with the
  transaction → history stays consistent with committed state.
- Multi-row statements and same-transaction multiple changes are handled by
  `FOR EACH ROW` + the surrogate `history_id` PK.
- Cascade deletes are captured (AFTER triggers fire per cascaded row).

### Security

- Production app should run as a non-owning, unprivileged DB role; do not grant
  `UPDATE`/`DELETE` on `_history` tables in production (no rewriting history).
  Documented, not enforced in code.
- Trigger-based auditing can be bypassed by superusers/table owners — accepted
  limitation, noted in docs.

### Testing

- DB-level: insert/update/delete via diesel inside `with_actor_scope` → assert
  ops, `valid_from`/`valid_to` chaining, and `actor_id`.
- Cascade delete records child rows with the actor.
- `as_of` correctness at two distinct instants.
- Anonymize → PII removed from `users`, history still queryable, `actor_id`
  unchanged, login disabled.
- Middleware integration: POST as user X ⇒ history `actor_id = X`;
  unauthenticated write ⇒ `actor_id = NULL`.
- Same-transaction double-change ⇒ no PK violation.

### Docs

- New `docs/temporal_history.md`: the versioning model, the alter-base-table
  maintenance contract, the read API, and the GDPR/anonymization story.

## Affected files (anticipated)

- `migrations/<date>_temporal_history/{up,down}.sql` — history tables,
  sequences, indexes, generic trigger function, per-table triggers,
  `users.anonymized_at`.
- `src/db/mod.rs` — `AMBIENT_ACTOR`, `with_actor_scope`, `set_config` for actor.
- `src/middleware/` (new) — auth/actor middleware + `ResolvedAuth`.
- `src/extractors/mod.rs` — read `ResolvedAuth` from extensions.
- `src/schema.rs` — regenerated.
- `src/models/*` — history `Queryable` structs; `users` gains `anonymized_at`.
- `src/db/traits/*` — `list_history`, `get_as_of`; anonymize op.
- `src/api/v1/handlers/*` + `routes/*` — history endpoints, `as_of` param,
  anonymize endpoint.
- `src/tests/*` — coverage per above.
- `docs/temporal_history.md` — new.
```
