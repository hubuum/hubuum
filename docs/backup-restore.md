# Backup and restore

Hubuum can create versioned full-system logical backups and perform a staged,
destructive system restore. Backup generation uses one PostgreSQL
`REPEATABLE READ, READ ONLY` transaction, so every section in a document is
read from the same database snapshot.

Backup/restore is the disaster-recovery path: it preserves identifiers and
replaces the whole system. Export/import is the portable merge path for moving
selected collections or hosts while retaining the destination's existing data
and history.

## Backup API

Submit a full backup as an unscoped administrator:

```http
POST /api/v1/backups
Authorization: Bearer <admin-token>
Content-Type: application/json

{
  "include_history": true
}
```

`include_history` defaults to `true`. Set it to `false` only when the eventual
restore is intended to reset audit, task, delivery, and temporal history.

The response is a background task. Poll `GET /api/v1/backups/{task_id}` and,
after it succeeds, download `GET /api/v1/backups/{task_id}/output`. The output
response includes `Digest` and `X-Hubuum-Backup-SHA256` headers. Stored outputs
are served as attachments with `Cache-Control: no-store` and expire according
to `HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS`.

Full backups contain integration configuration but exclude password hashes,
authentication tokens, and token scopes. Passwords and tokens must be reset or
reissued after a restore. Environment-backed secret values are also outside the
database backup.

Backup version `5` preserves authoritative resource revisions, collection
authorization-set revisions, temporal-history revisions, and event before/after
revisions. It identifies sections by Hubuum resources rather than database
tables. State sections include identity scopes, groups, principals, users,
service accounts, memberships, collections, authorization state, hierarchy,
permission grants, classes, computed-field definitions, relations, objects,
export templates, remote targets, event sinks, and event subscriptions. History
sections describe resource history, terminal tasks and results, audit events,
and terminal event deliveries.

Rows use the versioned logical vocabulary: `class_id`, `from_class_id`,
`from_object_id`, principal identifiers, permission-name arrays,
`history_entry_id`, and the temporal operations `create`, `update`, and
`delete`. Timestamp values are RFC 3339 UTC instants. The PostgreSQL adapter
privately maps these sections and fields to its tables, columns, and trigger
operation codes; another adapter must implement the same logical projection
without reproducing PostgreSQL names.

Restore rejects version 4 and older backups, unknown or incomplete sections,
malformed logical rows, invalid timestamps, and invalid, maximum, or
inconsistent revisions. Create a new backup after upgrading and before relying
on restore. Class computation state and object materializations remain excluded
as rebuildable caches; restore validates definitions and queues class rebuild
tasks. The manifest does not carry partial-selection counts, import-planning
warnings, a collection scope, or an embedded import request.

Backups cannot be scoped and backup documents are not import requests. Use the
export/import workflow (with an import-compatible export template or adapter)
to build a portable `ImportRequest`, then submit it to `POST /api/v1/imports`
for an administrator-controlled merge. Import follows normal collision and
atomicity rules and does not erase the database or reset its history.

## Restore API

Full restore is deliberately separate from merge import. It replaces all
Hubuum application data.

First, stage and validate the exact bytes downloaded from the backup endpoint:

```http
POST /api/v1/restores
Authorization: Bearer <admin-token>
Content-Type: application/json

<complete BackupDocument JSON>
```

Staging returns the document SHA-256 and a restore capability. Hubuum stores
only a hash of that capability. Keep it available because the restore replaces
the administrator token used to stage the operation. Restore responses use
`Cache-Control: no-store` so clients and intermediaries do not retain the
capability or restore metadata.

Staging and validation do not enter maintenance mode or lock application data.

Confirm the validated stage with the administrator token, one-time capability,
exact SHA-256, and destructive phrase:

```http
POST /api/v1/restores/{restore_id}/confirm
Authorization: Bearer <admin-token>
Content-Type: application/json

{
  "restore_capability": "<capability>",
  "sha256": "<sha256>",
  "confirmation": "REPLACE ALL HUBUUM DATA"
}
```

The endpoint commits draining maintenance and returns `202 Accepted` with
status `confirmed`. It does not perform privileged SQL. A separately deployed
`hubuum-admin --restore-executor` process, holding the database URL selected
for its single- or split-role topology, re-loads and revalidates the staged
bytes, waits for API and worker instances to drain, and applies the replacement
transaction.

Inspect validation, draining, or failure status by sending the capability in a
header:

```http
GET /api/v1/restores/{restore_id}/status
X-Hubuum-Restore-Capability: <capability>
```

Do not put the capability in a query string, where access logs could retain it.
Continue polling after confirmation. The status changes from `confirmed` to
`succeeded` or `failed`; successful and failed terminal rows contain no backup
document. A successful restore keeps its document-free receipt and capability
hash so the client whose administrator token was replaced can observe the
result. A later successful restore removes older receipts.

Deploy exactly one executor replica in ordinary operation. It exposes no HTTP
listener and accepts no request-provided SQL, identifier, or file path. Multiple
replicas are protected by the same database advisory lock, but a single replica
avoids redundant conflict logs. See
[PostgreSQL Database Roles](database_roles.md) for deployment isolation, threat
model, and role-topology choices.

## Admin CLI

Create a full backup:

```text
hubuum-admin --database-url "$HUBUUM_DATABASE_URL" --backup backup.json
```

The CLI writes an owner-only temporary file, synchronizes it, and atomically
replaces the destination. On Unix, it also synchronizes the destination
directory before reporting success.

History is included by default. Add `--backup-without-history` only to create a
backup whose eventual restore resets terminal task, audit, delivery, and
temporal history.

Verify the artifact without a database connection before moving or archiving
it:

```text
hubuum-admin --verify-backup backup.json --json
```

This bounded, non-destructive check verifies the document version, SHA-256,
manifest counts and exclusions, required sections and seed rows, timestamp and
revision invariants, logical references, computed-field definitions, and JSON
Schema. Its versioned report contains only artifact metadata, counts, timings,
and results; it never includes backup rows, database URLs, tokens, or
credentials. A format-only success proves that the bytes are internally valid,
not that PostgreSQL can restore them.

For a real recovery rehearsal, create a new empty disposable PostgreSQL
database and let the candidate binary migrate, restore, and check it:

```text
createdb --maintenance-db="$POSTGRES_ADMIN_URL" hubuum_restore_drill
hubuum-admin \
  --verify-backup backup.json \
  --restore-test-database-url \
    'postgres://hubuum_restore_drill:.../hubuum_restore_drill' \
  --json
dropdb --maintenance-db="$POSTGRES_ADMIN_URL" hubuum_restore_drill
```

The restore-test login must be able to migrate and replace data in that one
database. The command refuses PostgreSQL maintenance databases, a database
containing any user object, and a target matching either configured Hubuum
database URL even when the usernames differ. It uses the production restore
validation and transaction path, checks storage readiness, and takes a second
logical snapshot. Authoritative state and retained history must match the
source, apart from the one documented `restore.succeeded` provenance event.
The JSON report includes the canonical state digest and comparison results. By
default the command resets the disposable database's `public` schema after
both successful and failed verification. The database itself remains the
caller's responsibility and should be dropped afterward.

Add `--keep-restore-test-database` when the restored API and worker must be
started for application-level smoke tests. That option deliberately leaves the
restored schema intact; delete the database after inspection. Password hashes,
tokens, and token scopes are excluded from backups, so reset an administrator
password and issue a new token before exercising authenticated endpoints.

## Existing deployment upgrade path

Treat a restorable backup as an upgrade prerequisite, especially when adopting
the split database roles described in
[PostgreSQL Database Roles](database_roles.md). Use this sequence:

1. While the existing release is healthy, stop new backup, restore, and import
   operations and create a version 5 backup with history. Keep the old release,
   its database credential, and this artifact until the upgrade is accepted.
2. Run the candidate `hubuum-admin --verify-backup` against those exact bytes.
   If the installed release is v0.0.9 or older, first upgrade normally to
   v0.0.10 or v0.0.11, create a version 5 backup there, and only then continue;
   converting a version 4-or-older document in place is not supported.
3. Restore the artifact into a newly created empty disposable database using
   `--restore-test-database-url` and
   `--keep-restore-test-database`. Start the candidate API and worker against
   only that database, reset an administrator password, and verify login,
   representative reads, background work, audit history, and computed-field
   rebuilding. Then delete the disposable database.
4. Confirm production maintenance is `normal` and no confirmed restore is in
   flight. Run the one-shot schema migration, start the isolated restore
   executor, and roll runtime-only API and worker processes. If opting into
   split roles, complete the documented adoption before those workloads start.
5. Keep restore confirmation blocked until the executor is healthy. In split
   mode, also require the runtime privilege report to pass. Retain the
   pre-upgrade deployment and backup through the observation window. After a
   split-role adoption, application rollback uses the compatibility runtime
   login; web restore stays blocked and the candidate one-shot admin restore
   remains the recovery path.

This is an application migration path, not an automatic database downgrade.
Older application releases are only certified against the adjacent migrated
schema as documented in [Releasing Hubuum](releasing.md).

Restore always requires the explicit destructive phrase. Split mode also uses
the separate migration credential:

```text
hubuum-admin \
  --migration-database-url "$HUBUUM_MIGRATION_DATABASE_URL" \
  --restore backup.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
```

In the default single-role mode, omit `--migration-database-url`; the command
uses `HUBUUM_DATABASE_URL`. Split-role deployments use the privileged URL shown
above.

The CLI stages and confirms the document, then runs one executor iteration in
the same process. It appends the `restore.succeeded` provenance event on
success and rolls back to the old application data if validation, insertion,
or constraint checks fail. See
[PostgreSQL Database Roles](database_roles.md) for credential handling and
workload isolation.

## Configuration ownership

The startup configuration layer resolves environment values once and translates
them into consumer-owned `BackupSettings` and `RestoreSettings`. Runtime config
reports only the safe projections.

| Variable | Default | Purpose |
| -------- | ------- | ------- |
| `HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS` | `24` | Hours a successful backup remains downloadable |
| `HUBUUM_BACKUP_MAX_ACTIVE_TASKS_PER_USER` | `1` | Maximum active backup tasks per administrator |
| `HUBUUM_BACKUP_MAX_OUTPUT_BYTES` | `268435456` | Maximum stored backup document size in bytes |
| `HUBUUM_EXPORT_OUTPUT_CLEANUP_INTERVAL_SECONDS` | `300` | Shared cleanup cadence for expired export and backup artifacts; the legacy name is retained for compatibility |
| `HUBUUM_RESTORE_STAGE_RETENTION_MINUTES` | `60` | Minutes a validated restore stage remains confirmable |
| `HUBUUM_RESTORE_MAX_UPLOAD_BYTES` | `268435456` | Maximum restore document upload size in bytes |

## SQL safety

Backup and restore values are always Diesel bind parameters, including uploaded
JSON bound as `jsonb`. The few statements that must format table or column
identifiers accept only identifiers from closed, compile-time lists; arbitrary
predicates and request-provided identifiers are not accepted.

## Benchmark dataset seeding

The extended import graph can seed deterministic users, groups, memberships,
permissions, collections, classes, objects, relations, templates, remote
targets, event sinks, and subscriptions. That makes import a suitable setup
mechanism for a separate performance harness that varies dataset size,
principal count, and permission topology. Benchmark measurement should remain
separate from backup and restore code so setup cost is not mixed into query
latency.
