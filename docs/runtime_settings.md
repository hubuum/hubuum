# Runtime settings

Most Hubuum configuration is read once when a process starts. Runtime settings
are the exception: a small set of shared policy values is stored in the primary
storage backend. Administrators change them through the versioned API, and every
replica applies the change without a restart.

Changes go through one authenticated API and service layer, so the CLI and web
UI share the same authorization, validation, revision checks, and audit trail.

## Ownership

Each setting is owned either by deployment configuration or by the runtime
settings store. The initial runtime-managed settings are listed below.

| Group | Setting | Takes effect | Deployment ceiling |
| --- | --- | --- | --- |
| `pagination` | `default_page_limit` | When the next API request starts | `HUBUUM_MAX_PAGE_LIMIT` |
| `query_observations` | `enabled` | On the next eligible query | Sampling and capacity limits |

Everything else remains deployment configuration. That includes:

- storage backend selection, database credentials, and pool sizes;
- bind address, TLS, runtime role, and worker counts;
- the secret provider, signing keys, and hash keys;
- authentication and authorization backend selection;
- machine resource ceilings and filesystem paths; and
- `HUBUUM_MAX_PAGE_LIMIT` and the query observation sampling and capacity
  limits, which act as ceilings for the runtime values above.

Other candidates, such as retention, task admission, token lifetimes, and worker
concurrency, will move only after their effect on running work and their failure
behavior have been defined. Secrets never move into runtime settings.

### Groups and ownership states

Settings are updated in groups. A group's values change together and share a
revision. A group is always in one of two states:

- **Deployment**: each process applies its own startup configuration. This is
  the state on fresh installations and after an upgrade.
- **Runtime**: the persisted value applies to every replica that uses the same
  storage, whatever each replica's startup configuration says.

Process startup never writes runtime settings. A replica's environment can
change what it applies only while the group is in the deployment state.

## Adopting existing configuration

Upgrading changes no behavior on its own. Pagination stays in the deployment
state, and replicas whose startup values differ keep applying their own values.
`GET /api/v1/admin/runtime-settings` shows each responding replica's deployment
values. Compare replicas before adopting a shared value.

To adopt a value, an administrator writes it explicitly, for example the
current deployment default. From then on the persisted value is authoritative
on every replica:

```sh
curl --fail-with-body --request PUT \
  "$HUBUUM_URL/api/v1/admin/runtime-settings/pagination" \
  --header "Authorization: Bearer $HUBUUM_TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{"expected_revision":1,"default_page_limit":100}'
```

To give a group back to deployment configuration, delete its runtime value. The
release is recorded as a new revision:

```sh
curl --fail-with-body --request DELETE \
  "$HUBUUM_URL/api/v1/admin/runtime-settings/pagination?expected_revision=2" \
  --header "Authorization: Bearer $HUBUUM_TOKEN"
```

The query observation override from earlier releases is imported once, by the
migration that introduces runtime settings. If an override was set, the
`query_observations` group starts at revision 2. Its audit record names the
`system` actor.

## Administrator API

Every endpoint requires an unscoped human administrator token.

| Method and path | Purpose |
| --- | --- |
| `GET /api/v1/admin/runtime-settings` | Report every group as applied by the responding replica |
| `PUT /api/v1/admin/runtime-settings/pagination` | Set the shared default page size |
| `PUT /api/v1/admin/runtime-settings/query_observations` | Set shared query observation enablement |
| `DELETE /api/v1/admin/runtime-settings/{pagination,query_observations}` | Return the group to deployment configuration |
| `GET /api/v1/admin/runtime-settings/history/{group}` | List durable audit records, newest first |
| `GET /api/v1/admin/config` | Effective process configuration, including `runtime_settings` |

### Revisions and concurrent edits

Every group has a revision, starting at 1. Each write must send the
`expected_revision` it read. If another change committed first, the request is
rejected with `412 Precondition Failed`, and the body's `current_revision` holds
the latest revision. Re-read the group, then decide whether to apply your change
again. Writing the value that is already current returns `"changed": false`; no
revision or audit record is created.

Reverting a value is a new revision, not an undo. Rolling back a policy cannot
restore effects it already had.

### Validation

Requests are validated before anything is stored. Unknown fields, wrong types,
non-positive values, and inconsistent combinations are rejected with `400 Bad
Request`. `default_page_limit` must be between 1 and the responding replica's
`HUBUUM_MAX_PAGE_LIMIT`. If another replica has a lower ceiling, it clamps the
shared value to that ceiling and reports `constrained_by_ceiling: true`.

### Audit

The value change and its audit record commit in the same transaction. A record
holds the group, the new revision, the previous and new ownership and value, the
commit time, the actor kind and principal, and any request or correlation
identifier. Audit records are append-only: PostgreSQL rejects updates, deletes,
and truncation of the audit table.

## Replica consistency

Storage is the authority. Each process keeps the last snapshot it loaded
successfully and applies it without a storage read on request paths.

- Processes load runtime settings before they serve requests or start
  background work. If the load fails, startup fails.
- A committed change sends a storage notification. With PostgreSQL, this uses
  `LISTEN`/`NOTIFY` on `hubuum_runtime_settings`. A notification only wakes the
  replica, which then reloads the authoritative state. Listeners register
  `LISTEN` before reloading, including after reconnecting, so changes made while
  disconnected are not missed.
- Every process also reconciles every 30 seconds, to recover from missed
  notifications.
- A successful `PUT` or `DELETE` means the change is committed, not that every
  replica has applied it. The response reports `committed_revision` and whether
  the responding replica already applies it
  (`applied_by_responding_replica`). Other replicas apply it on notification, or
  within the reconcile interval.

### Effect timing

- **Pagination**: an API request fixes its page limits when it starts. A
  request in progress keeps them even if a change arrives. Exports capture the
  effective default when they are submitted.
- **Query observations**: sampling decisions read the applied value for each
  eligible query. Requests that are already being observed may finish
  recording after collection is disabled.

### Failure behavior

If a refresh fails, the replica keeps its last valid snapshot and reports the
`stale` status, with the time of the failure. Pagination continues with the
retained snapshot. Query observation collection pauses until the next successful
refresh (`collection_paused: true`) rather than keep an enablement that may be
stale. Ordinary queries never wait on, or fail because of, a runtime settings
read.

A replica is also reported as stale when it has not completed a successful
refresh for more than two reconcile intervals.

## Reporting

`GET /api/v1/admin/runtime-settings` and the `runtime_settings` member of
`GET /api/v1/admin/config` report, for the responding replica:

- `replica.status`: `current`, `stale`, or `not_loaded`, together with refresh
  timestamps and the reconcile interval;
- for each group:
  - `source`: `deployment` or `runtime`;
  - `revision`: the revision this replica applies;
  - `changed_at` and `changed_by`;
  - `mutable`, `restart_required`, and `takes_effect`;
  - the `effective`, `runtime`, and `deployment` values; and
  - any deployment ceiling that applies.

Both reads reconcile the replica before they report. Reports contain no secret
values.

`GET /api/v1/config` returns the effective default page size to unauthenticated
clients. The committed OpenAPI document describes the default from deployment
configuration.

## Backups, restores, and storage backends

Runtime settings and their audit records are deployment configuration, not
domain data. They are excluded from portable backups and survive logical
restores, as the query observation override did before. Restoring a backup does
not roll back a runtime setting. Reapply settings explicitly after moving to a
new deployment.

PostgreSQL shares runtime settings across every replica that uses the same
database. The memory backend shares them only among handles of one in-process
store, has no notifications, and loses them on restart. Independent memory
processes do not form a shared deployment.

## Upgrading

Apply the runtime settings migration, then upgrade every API and worker replica
together. Upgraded replicas no longer read or write the legacy
`query_observation_control` table, so previous-release replicas that keep
running would not follow query observation changes. The migration imports the
legacy override once. A later release drops the unused table.

The earlier `GET` and `PUT /api/v1/admin/query-observations` endpoints have been
removed. Use `GET /api/v1/admin/runtime-settings` and
`PUT /api/v1/admin/runtime-settings/query_observations` instead. The
`query_observations.runtime_override` field of `GET /api/v1/admin/config` has
been replaced by `runtime_settings.query_observations`.
