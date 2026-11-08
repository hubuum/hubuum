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
| `authentication` | LDAP provider activation and runtime-owned definitions | On the next authentication request | 64 providers |
| `authorization` | Permission backend selection | On the next API request | Available backend and deployment connection settings |
| `pagination` | `default_page_limit` | When the next API request starts | `HUBUUM_MAX_PAGE_LIMIT` |
| `query_observations` | `enabled`, `sample_every`, `max_patterns`, `max_patterns_per_class`, `retention_seconds`, `max_predicates_per_query` | On the next eligible query | Query-observation ceiling variables |

Everything else remains deployment configuration. That includes:

- storage backend selection, database credentials, and pool sizes;
- bind address, TLS, runtime role, and worker counts;
- the secret provider, signing keys, hash keys, and runtime-settings encryption
  key reference;
- LDAP secret-source selection and authorization backend connection settings;
- machine resource ceilings and filesystem paths;
- `HUBUUM_MAX_PAGE_LIMIT`, which caps `pagination.default_page_limit`; and
- query-observation sampling and capacity ceiling variables, which cap the
  runtime values above.

Other candidates, such as retention, task admission, token lifetimes, and worker
concurrency, will move only after their effect on running work and their failure
behavior have been defined. Raw secrets never move into runtime settings. An
LDAP bind password supplied through the runtime API is encrypted before it is
stored.

### Groups and ownership states

Settings are updated in groups. A group's values change together and share a
revision. A group is always in one of two states:

- **Deployment**: each process applies its own startup configuration. This is
  the state on fresh installations and after an upgrade.
- **Runtime**: the persisted value applies to every replica that uses the same
  storage, whatever each replica's startup configuration says.

Process startup never writes runtime settings. A replica's environment can
change the active selection only while the group is in the deployment state.
Deployment-owned provider definitions and authorization connection settings still
come from deployment configuration.

### Authentication provider ownership

The authentication group stores the complete external-provider selection. Local
password authentication is always enabled and cannot be replaced, disabled, or
included in that selection. An empty external selection therefore means local
login only.

Each provider entry has a stable `name` matching its LDAP identity scope,
`kind: "ldap"`, an explicit `enabled` flag, and one of two definition sources:

- `source: "deployment"` references the definition in `HUBUUM_AUTH_CONFIG_PATH`.
  The API can change activation but rejects a `configuration` field. Connection
  settings and secret references stay deployment-owned.
- `source: "runtime"` supplies `configuration`. Taking over an existing
  deployment definition requires `adopt_deployment: true` on the first write.
  Subsequent edits use the stored definition, including after restart. To return
  that provider to deployment ownership, explicitly replace its entry with a
  deployment reference.

For example, this complete selection disables the deployment-defined corporate
provider while retaining local login:

```json
{
  "expected_revision": 1,
  "providers": [
    {"source": "deployment", "name": "corporate", "kind": "ldap", "enabled": false}
  ]
}
```

Send it to `POST /api/v1/admin/runtime-settings/authentication/preflight`.
Include every external provider that should remain enabled. Omitted deployment
providers stay disabled while the group is runtime-owned; newly added deployment
providers also need explicit activation. Omitted runtime definitions are removed.
Restart never resets these activation decisions. Releasing the whole group with
`DELETE` explicitly restores the complete deployment selection, after preflight.
A disabled reference can be retained when its deployment definition is missing;
enabling it requires the definition to exist.

Fully runtime-owned definitions remain usable if the old deployment file is
removed. An enabled deployment reference missing on a replica is unavailable;
that replica never substitutes another provider or an older definition.
If releasing the group encounters an invalid deployment configuration on another
replica, that replica stops authentication and fails readiness until repaired;
it does not keep the previous runtime providers active.

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

Every endpoint requires an unscoped human administrator token. Authentication
and authorization changes, including releases, additionally require a local
human administrator with password login configured. External administrators can
inspect settings but must establish a local recovery administrator before
changing security settings.

| Method and path | Purpose |
| --- | --- |
| `GET /api/v1/admin/runtime-settings` | Report every group as applied by the responding replica |
| `POST /api/v1/admin/runtime-settings/authentication/preflight` | Validate and test LDAP providers; persist only after successful preflight |
| `PUT /api/v1/admin/runtime-settings/authorization` | Select `local` or `treetop` authorization |
| `PUT /api/v1/admin/runtime-settings/pagination` | Set the shared default page size |
| `PUT /api/v1/admin/runtime-settings/query_observations` | Set shared query observation enablement, sampling, and capacity limits |
| `DELETE /api/v1/admin/runtime-settings/{authentication,authorization,pagination,query_observations}` | Return the group to deployment configuration |
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
`HUBUUM_MAX_PAGE_LIMIT`. Query-observation values must fit the responding
replica's ceilings: `sample_every` must be at least
`HUBUUM_QUERY_OBSERVATIONS_MIN_SAMPLE_EVERY`, and each capacity/retention value
must not exceed its corresponding `HUBUUM_QUERY_OBSERVATIONS_*_CEILING`.
Omitted observation fields use the responding replica's deployment defaults.
If another replica has a lower ceiling, it clamps the shared values and
reports `constrained_by_ceiling: true`.

Authentication updates use the preflight endpoint. It validates ownership and
identity-scope bindings, tests enabled providers' configuration, connectivity,
and service bind, and commits the new revision only after all checks succeed.
Disabled providers do not require connectivity or decryption, so an unavailable
provider or key does not prevent disabling it. Their configuration is checked
before they can be enabled. A stale revision discovered after preflight
still prevents the write. Omitting `configuration.bind_password` for an
existing provider of the same name preserves its encrypted password; setting it
to `null` clears it. A supplied password is encrypted using AES-256-GCM with a
random data key wrapped by the secret resolved from
`HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF`. The key must resolve to exactly
32 bytes. The envelope pins the secret reference used for encryption. Treat
each reference as an immutable key version: use a new reference when rotating
the key, and keep old references and their original key material resolvable
until all dependent ciphertext is replaced. This API does not provide key
rewrapping. API reports and audit responses never return encrypted envelopes,
password values, or secret references.

Passwords stored by earlier unreleased builds used process-local key versions.
Re-enter those passwords through the preflight API after upgrading; preserving
an existing envelope does not convert it to the durable reference format.

Authorization updates select a backend that is already supported by the
running binary. Backend-specific connection options, such as the Treetop URL
and TLS settings, remain deployment configuration. Before committing an
authorization update or release, the responding replica initializes and checks
the selected backend and verifies that the local administrator making the change
is also an administrator under the replacement. A backend that is reachable but
would remove that access is rejected without a revision or audit write. Exactly
one backend is selected; there is no disabled authorization state or automatic
fallback to local policy. Other replicas must have compatible builds and
deployment configuration.

Local login remains available independently of the external-provider selection.
Consequently, concurrent authentication and authorization updates cannot remove
the validated local recovery path by disabling its authentication provider.
Revision checks serialize competing edits of each group. This protects settings
changes; operators must still preserve the recovery account, its password, and
its grants when editing users, groups, or external policy directly. If a replica
cannot initialize the selected authorization backend, permission checks fail
closed and that replica's readiness probe returns `503` until reinitialization
succeeds.

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
- **Authentication and authorization**: new requests use the newly applied
  providers or permission backend. A request already in progress keeps the
  authorization backend it started with. An invalid authentication provider is
  reported unavailable without disabling other providers; reconciliation
  retries unavailable providers even when the revision has not changed.
  Disabled providers are excluded from public discovery. After a replica applies
  a disable, new logins, bearer-token requests, and session refreshes for that
  scope are rejected even if cached identity data is fresh. Requests already in
  progress may finish. Disablement suspends access; it does not permanently revoke
  stored credentials, and re-enabling may restore unexpired credentials. Use
  credential revocation when permanent invalidation is required.
- **Query observations**: sampling and predicate budgets read the applied value
  for each eligible query. Capacity and retention changes apply as the next
  observation is recorded or reported; existing entries over a newly lowered
  limit are evicted. Requests already being observed may finish recording
  after collection is disabled.

### Failure behavior

If a refresh fails, the replica keeps its last valid snapshot and reports the
`stale` status, with the time of the failure. New API requests and readiness
return `503` until reconciliation succeeds, including requests using local
credentials. This prevents an isolated replica from indefinitely accepting a
provider or authorization policy that was disabled elsewhere. Existing work
retains its captured settings. Query observation collection also pauses until
the next successful refresh (`collection_paused: true`). Request paths check
cached freshness; they do not wait for a settings read.

A replica also becomes stale when its last successful refresh is older than two
reconcile intervals plus the five-second refresh timeout (65 seconds). Missed
notifications therefore have a bounded security effect even if reconciliation
stalls. Inspect logs and readiness during an outage; the administrator API is
available again after reconciliation succeeds.

## Reporting

`GET /api/v1/admin/runtime-settings` and the `runtime_settings` member of
`GET /api/v1/admin/config` report, for the responding replica:

- `replica.status`: `current`, `stale`, or `not_loaded`, together with refresh
  timestamps and the reconcile interval;
- authentication provider names, definition `source`, `enabled`,
  `definition_mutable`, and `available`, including disabled deployment providers;
- the effective, runtime, and deployment authorization backend and its readiness;
- for each group:
  - `source`: `deployment` or `runtime`;
  - `revision`: the latest revision loaded by this replica; authentication
    providers can be individually unavailable while that revision is retried;
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

Earlier unreleased builds accepted authentication provider entries without
`source` and `enabled`. Update clients to supply both fields explicitly. Existing
stored runtime definitions retain runtime ownership and default to enabled;
existing runtime selections continue to suppress omitted deployment providers.
Security-setting edits now require a local administrator, so provision and test
that account before upgrading installations administered solely through LDAP.
