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

Full backups contain password hashes and integration configuration. Protect
backup files as credentials. Authentication tokens and token scopes are never
included and must be reissued after a restore. Environment-backed secret values
are also outside the database backup.

The version 3 manifest reports only counts for included sections and the fixed
list of exclusions. It includes personal and shared computed-field definitions
as authoritative state. Class computation state and object materializations are
excluded as rebuildable caches; restore validates the definitions and queues
class rebuild tasks. The manifest does not carry partial-selection counts,
import-planning warnings, a collection scope, or an embedded import request.

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

Confirm with the exact SHA-256, capability, and destructive phrase:

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

During confirmation, Hubuum first commits a global `draining` maintenance
state. All instances reject ordinary API work, background workers stop starting
work, and readiness returns `503`; liveness and capability-authenticated restore
status remain available. Every runtime role, including API-only replicas,
registers a heartbeat and participates in this drain barrier. After every live
instance reports drained, the restore takes PostgreSQL `ACCESS EXCLUSIVE` locks
on every replaced table and performs truncation plus all inserts in one database
transaction. If draining, an insert, or a constraint check fails, Hubuum rolls
back the replacement and returns to normal mode with the old application data
intact.

The restore coordinator waits for a 60-second confirmation grace period before
treating a confirmed drain as interrupted. This keeps a live API or CLI
confirmation authoritative through the bounded drain window while still
allowing another replica to resume a restore after the confirming process exits.

On success, Hubuum deletes all restore staging records and server-heartbeat
records. The restored backup is the sole source of application data, with one
intentional exception: Hubuum appends a `restore.succeeded` system audit event
in the same transaction. Its metadata records the backup SHA-256 and the
initiating administrator's immutable identity snapshot. This event is the
logical-restore provenance marker; an administrator performing a physical
database restore can, by definition, replace it as well.

Inspect validation, draining, or failure status by sending the capability in a
header:

```http
GET /api/v1/restores/{restore_id}/status
X-Hubuum-Restore-Capability: <capability>
```

Do not put the capability in a query string, where access logs could retain it.
Stored stages can report `validated`, `confirmed`, `failed`, or `expired`.
The successful confirmation response reports `succeeded` directly, but that
status is never persisted. Subsequent status lookups return `404` because a
successful restore removes all staging records.

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

Restore requires the same explicit destructive phrase as the API:

```text
hubuum-admin \
  --database-url "$HUBUUM_DATABASE_URL" \
  --restore backup.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
```

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
