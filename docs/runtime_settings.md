# Runtime settings

Most Hubuum configuration is read once when a process starts. Runtime settings
are shared policies stored in the primary
storage backend. Administrators change them through the versioned API, and every
replica applies the change without a restart.

Changes go through one authenticated API and service layer, so the CLI and web
UI share the same authorization, validation, revision checks, and audit trail.

## Ownership

Each setting is owned either by deployment configuration or by the runtime
settings store. The runtime-managed groups are listed below.

| Group | Setting | Takes effect | Deployment ceiling |
| --- | --- | --- | --- |
| `authentication` | LDAP provider activation and runtime-owned definitions | On the next authentication request | 64 providers |
| `authorization` | Permission backend selection | On the next API request | Available backend and deployment connection settings |
| `pagination` | `default_page_limit`, `max_page_limit` | When the next API request starts | `HUBUUM_MAX_PAGE_LIMIT` |
| `query_observations` | `enabled`, `sample_every`, `max_patterns`, `max_patterns_per_class`, `retention_seconds`, `max_predicates_per_query` | On the next eligible query | Query-observation ceiling variables |
| `observability` | Log filter, metrics enablement and scrape path | On reconciliation | Literal non-conflicting scrape path |
| `network` | Forwarded-header trust, proxy networks/hops and client allowlist | Next request | Updating local administrator must retain network access |
| `query_limits` | Maximum transitive depth | Next query | 1–512 |
| `tasks` | Polling, lease/heartbeat, recovery, reindex batch | Next claim or batch | Heartbeat below lease; reindex batch at most 1,000; worker counts stay fixed |
| `remote_calls` | Timeout, response bytes and private destinations | Next dispatch | Deployment prohibition on private targets remains authoritative |
| `tokens` | Default issuance lifetime, expired-token retention and purge policy | Next issuance or purge batch | Deployment maximum token lifetime; legacy cutoffs stay fixed |
| `login_rate_limit` | Enablement, identity/account/IP/subnet attempts, window and backoff | Next login attempt | Backoff base no greater than maximum |
| `events` | Fanout/delivery batches and polling, delivery attempts/timeout, retention and archive enablement | Next batch | Transport timeout below deployment lease; archive requires usable deployment path |
| `exports` | Output retention/cleanup/size, stage/query budgets and template limits | Next export or cleanup pass | Existing expiry timestamps are preserved |
| `backups` | Output/row limits and retention | Next execution | Existing expiry timestamps are preserved |
| `restores` | Upload limit and staging retention | Next upload | An upload retains its limit; existing expiry timestamps are preserved |

Everything else remains deployment configuration. That includes:

- storage backend selection, database credentials, and pool sizes;
- bind address, TLS, runtime role, and worker counts;
- the secret provider, signing keys, hash keys, and runtime-settings encryption
  key reference;
- LDAP secret-source selection and authorization backend connection settings;
- machine resource ceilings and filesystem paths;
- `HUBUUM_MAX_PAGE_LIMIT`, which caps both runtime pagination limits; and
- query-observation sampling and capacity ceiling variables, which cap the
  runtime values above.

Pool size/acquire timeout, database statement timeout, TLS/certificate reload,
administrator-group selection, worker counts, token hash-key rotation, and
Treetop connection settings remain deployment configuration. Raw secrets never
move into runtime settings. An
LDAP bind password supplied through the runtime API is encrypted before it is
stored.

Task admission is governed by the shared policy at
`/api/v1/admin/task-capacity`. The legacy `tasks.imports_per_user`,
`tasks.exports_per_user`, `tasks.remote_calls_per_user` and
`backups.max_active_per_user` runtime fields are read-only deployment seeds
retained for persisted-document compatibility. Discovery marks them read-only;
edits must use the task-capacity endpoint and its own policy revision. Existing
runtime assignments cannot replace shared admission or reset its usage.

### Operational policy updates

Use the catalogue to discover field names, types, bounds and deployment defaults.
The additional groups accept revision-checked `PATCH`, `GET`, history reads and
`DELETE` to release ownership, through the same endpoints as pagination. Their
state is also included under `policies` in the complete runtime-settings report.
No raw secrets, filesystem locations or derived "configured" flags are editable.
Network lists are comma-separated IP/CIDR strings; `client_allowlist` also accepts
`*`. Counts in the administrator configuration remain derived displays.

For example, send this to `PATCH /api/v1/admin/runtime-settings/tasks`:

```json
{
  "expected_revision": 1,
  "changes": {"lease_seconds": 120, "heartbeat_seconds": 30}
}
```

Requests and task executions capture a configuration snapshot. Existing claims
retain their lease and heartbeat pair; changes do not shorten an existing lease.
Batch workers read a new policy between batches, and settings notifications wake
sleeping workers. Retention loops remain present while disabled, then pause until
enabled. API-only processes never start background workers. Per-user admission
limits apply to new submissions and do not cancel accepted work. Delivery attempt
changes retain attempt counts and do not revive terminal failures. Reindexing
reads its validated batch size from shared storage before each batch.

New output/stage retention applies when an artifact is created; stored expiry
values do not change. Export stage timeouts remain post-completion checks, while
storage query budgets are enforced during reads. Zero disables the storage query
budget. Archive enablement requires a writable deployment archive directory;
archive failure still prevents event deletion. Destructive retention pauses on
stale replicas until reconciliation succeeds, including replicas missing a
required archive-path configuration. New task claims and delivery batches also
wait for current security policy. Webhook deliveries capture the
remote-call timeout, response-size and private-destination policy for their
batch while other transport connections remain in place.

`network`, `tokens`, `login_rate_limit`, and `remote_calls` changes require a local
password-capable human administrator, including release. Network edits and release
also resolve the current connection through the candidate proxy policy and reject
changes that would exclude it. All request consumers use the same network snapshot
for allowlisting, tracing and login IP attribution. Deployment access is the
recovery path if topology changes later.

Token lifetime changes apply to new issuance, without altering explicit expiry
or the deployment-derived legacy token cutoff. Login attempts retain the policy
used to reserve capacity until they finish. Disabling the limiter does not reset
its store. Existing lockout deadlines persist; retained failures are evaluated
using the next attempt's window. Expired/pruned observations cannot be recovered
by increasing that window. Zero disables the account, IP or subnet dimension.

Metrics enablement and path changes use the `observability` group. The previous
path stops serving metrics after reconciliation; update scraper configuration
alongside the change. Disabled metrics retain their registry and counters.
Worker-only processes keep their HTTP listener bound even while metrics are
disabled, allowing later enablement. See [metrics](metrics.md) for deployment
requirements. Log filters reload independently of distributed tracing sampling.

### Upgrade and rollback

**Breaking deployment change:** stop older API and worker binaries before applying
migration `2026-11-10-000001_runtime_policies`, then deploy the upgraded binaries
for every replica. Older readers cannot parse the new group rows, even before a
policy is edited. Back up deployment configuration and the database before the
upgrade. Existing policy ownership and audit history are retained.

Worker-only processes now reserve `HUBUUM_BIND_IP:HUBUUM_PORT` even when metrics
are disabled. Assign separate ports or network namespaces before upgrading
workers that previously shared a host and port with another process.

The new migration can be rolled back while its groups have no audit entries.
After an edit, its foreign key deliberately prevents rollback from deleting the
append-only audit trail; release to deployment ownership does not erase that
history. Use an upgraded binary or a database backup for rollback after use.

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

New groups start in the deployment state. Existing ownership is preserved, and
replicas whose deployment-owned values differ keep applying their own values.
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
  --data '{"expected_revision":1,"default_page_limit":100,"max_page_limit":200}'
```

The default applies when a client omits `limit`; the maximum also clamps explicit
client limits. Both values are committed in one revision and must satisfy
`1 <= default_page_limit <= max_page_limit <= HUBUUM_MAX_PAGE_LIMIT`.
For example, with the settings above, `?limit=500` returns at most 200 items.

Existing default-only policies continue to inherit each replica's deployment
maximum. Omitting `max_page_limit` (or setting it to `null`) on a replacement
`PUT` restores that inheritance. Use `PATCH` to preserve an unchanged field:

```sh
curl --fail-with-body --request PATCH \
  "$HUBUUM_URL/api/v1/admin/runtime-settings/pagination" \
  --header "Authorization: Bearer $HUBUUM_TOKEN" \
  --header 'Content-Type: application/json' \
  --data '{"expected_revision":2,"changes":{"max_page_limit":150}}'
```

The merged values are validated together. Lowering the maximum below the current
default requires changing both in the same request. `PATCH` rejects `null`.

To give a group back to deployment configuration, delete its runtime value. The
release is recorded as a new revision:

```sh
curl --fail-with-body --request DELETE \
  "$HUBUUM_URL/api/v1/admin/runtime-settings/pagination?expected_revision=3" \
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
| `PUT /api/v1/admin/runtime-settings/pagination` | Set shared default and maximum page sizes |
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
Request`. Deployment and runtime configuration share the same LDAP definition,
identity-scope-name, and pagination validators. `default_page_limit` must be
between 1 and 4,294,967,295 and cannot exceed the effective maximum.
`max_page_limit` must be positive and cannot exceed the responding replica's
`HUBUUM_MAX_PAGE_LIMIT`. With no runtime maximum, the deployment maximum applies.
Query-observation values must fit the responding
replica's ceilings: `sample_every` must be at least
`HUBUUM_QUERY_OBSERVATIONS_MIN_SAMPLE_EVERY`, and each capacity/retention value
must not exceed its corresponding `HUBUUM_QUERY_OBSERVATIONS_*_CEILING`.
Omitted observation fields use the responding replica's deployment defaults.
If another replica has a lower ceiling, it clamps the shared values and
reports `constrained_by_ceiling: true`.
For pagination this clamps the maximum first, then the default, preserving
`default_page_limit <= max_page_limit`.

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

`GET /api/v1/config` returns the effective default and maximum page sizes to
unauthenticated clients. Administrator pagination reports include both limits
under `effective`, `runtime`, and `deployment`; legacy runtime values omit the
inherited maximum. The top-level `pagination.max_page_limit` in the runtime
settings report remains the deployment ceiling for compatibility. The committed
OpenAPI document describes the default from deployment configuration.

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

Upgrade every API and worker replica before writing a runtime `max_page_limit`.
Older binaries reject persisted pagination documents containing that field.
Existing default-only documents and requests remain supported without a data
migration.

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

## Discovery for generic clients

`GET /api/v1/admin/runtime-settings/schema` returns an administrator-only
catalogue with `schema_version: 1` and a `groups` array. Each descriptor has a
stable `key`, title, description, JSON Schema, `patch_supported`,
`requires_local_admin`, application timing, and an optional read-only reason.
The catalogue describes the responding binary and deployment, not every replica
in the cluster. Upgrade all participating replicas before enabling new settings;
keep deployment ceilings consistent when identical effective values are needed.

The value schemas use JSON Schema 2020-12. Simple editors can support boolean,
integer, number, string, and enum fields, including minimum/maximum bounds,
descriptions, defaults, and the optional `x-unit` annotation. Ignore unfamiliar
annotations. Display unsupported field shapes read-only and never send them back
as part of an edit. An unknown catalogue major version must disable edits.
`default` describes the responding deployment's value; it is not the current
runtime value and does not authorize resetting a field.

Every advertised field includes a plain-text `title` and `description`. Keep
descriptions alongside the request type so OpenAPI and discovery share the field
help; presentation titles live in the catalogue.
Describe what the setting controls, its units and special values, interactions
with other settings, and the operational effect of changing it. Catalogue tests
require nonempty help for every group and field, including read-only fields.

String enums also include `x-enum-descriptions`: an object mapping each advertised
wire value to its plain-text explanation. Only supported choices are included.
Clients should show these explanations alongside the selector and associate
field help with its input for assistive technologies. Render help as text, not
HTML. Clients may ignore this additive annotation and continue displaying the
raw enum values; the discovery version remains 1. For example:

```json
{
  "type": "string",
  "title": "Authorization backend",
  "description": "Backend used to evaluate permissions on subsequent requests.",
  "enum": ["local"],
  "x-enum-descriptions": {
    "local": "Evaluate permissions using Hubuum's built-in authorization backend."
  }
}
```

`GET /api/v1/admin/runtime-settings/{group}` returns a uniform view with ownership,
revision, replica application status, `effective`, `runtime`, `deployment`, and
`constrained_by_ceiling`. Prefer requested `runtime` values when preparing an edit;
`effective` can be lower because of a deployment ceiling. Authentication exposes
only redacted provider status, with runtime/deployment definitions omitted.

For groups with `patch_supported: true`, send only the edited top-level fields:

```json
{
  "expected_revision": 3,
  "changes": {
    "default_page_limit": 75
  }
}
```

Send that document to `PATCH /api/v1/admin/runtime-settings/pagination`. The server
loads the authoritative group, preserves omitted fields, validates the complete
candidate using the same typed constructors as PUT, and commits it with an audit
record and revision check. Empty changes, unknown fields, null values, incorrect
types, invalid combinations, and deployment-ceiling violations are rejected.
A stale revision returns `412`; refetch and let the administrator review the
conflict rather than silently retrying the mutation. DELETE remains the explicit
operation for returning an entire group to deployment ownership.

The initial generic editors cover pagination, query observations, and authorization
backend selection. Authorization retains the local recovery-administrator check.
Authentication is advertised read-only to generic editors because its existing
preflight workflow validates ownership transfer, secrets, and connectivity before
committing. Discovery never relaxes server authorization or makes deployment-only
settings mutable. None of these additive endpoints changes persistence or backup
semantics.

Clients must discover group and field names at runtime. An older generic client
can then edit a newly added scalar field without recompilation, while partial
updates preserve fields that client does not understand. A `404` from discovery
means an older server does not implement this capability; retain read-only
configuration inspection and do not fall back to unrevisioned writes.
