# PostgreSQL Database Roles

Hubuum uses three distinct PostgreSQL roles. Long-lived API and worker
processes must use only the runtime role. Schema changes and the isolated
restore executor use a separate migrator credential, while a non-login owner
holds application objects.

| Role | Login | Purpose | Must not have |
| --- | --- | --- | --- |
| Owner | No | Owns the application schema, tables, sequences, and functions | A password or workload identity |
| Migrator | Yes | Assumes the owner role for embedded migrations, grant reconciliation, and the isolated restore executor | Superuser, `CREATEROLE`, or `BYPASSRLS` |
| Runtime | Yes | Serves API requests and runs background workers | Ownership, DDL, migration writes, direct history mutation, or broad audit-event mutation |

The default names are `hubuum_owner`, `hubuum_migrator`, and
`hubuum_runtime`. Override all workloads consistently with
`HUBUUM_DATABASE_OWNER_ROLE`, `HUBUUM_DATABASE_MIGRATOR_ROLE`, and
`HUBUUM_DATABASE_RUNTIME_ROLE`.

## Privilege Manifest

The version-controlled
`crates/hubuum-storage-postgres/database-privileges.json` manifest is the
machine-readable source for generated grants and diagnostics. After every
embedded migration, Hubuum transfers application-object ownership to the
owner and reconciles runtime privileges for every table, sequence, and
function in the application schema. This catalog-driven step prevents a new
object from silently being omitted from a handwritten grant list.

Runtime access is intentionally narrower for integrity-bearing objects:

- migration and `*_history` tables are read-only;
- audit events allow `SELECT`, `INSERT`, and updates only to the delivery claim
  columns listed in the manifest;
- application security-invoker functions are executable because PostgreSQL
  still checks the caller's underlying privileges;
- security-definer functions are denied unless explicitly allowlisted; and
- destructive snapshot replacement is available only to the isolated
  migrator-backed restore executor.

The hardened security-definer functions use a fixed `pg_catalog` search path,
qualified application objects, closed identifier inputs, and non-login owners.
Setting Hubuum's transaction-local restore flags from a runtime session does
not grant restore authority or suppress temporal history.

## Initial Provisioning

Run the full setup SQL through an identity that may create roles and change
object ownership. The output is idempotent and contains no credentials:

```bash
hubuum-admin --database-role-setup-sql > hubuum-database-roles.sql
psql "$POSTGRES_BOOTSTRAP_URL" --set ON_ERROR_STOP=1 \
  --file hubuum-database-roles.sql
```

Set the migrator and runtime passwords, certificates, or workload-identity
mappings through the database provider's secret-management interface. Do not
put credentials into the generated SQL file. Then migrate with the migrator
URL:

```bash
export HUBUUM_MIGRATION_DATABASE_URL='postgres://hubuum_migrator:.../hubuum'
hubuum-admin --migrate
```

The migrator must be a member of the owner role. It connects under its own
login and Hubuum explicitly runs `SET ROLE hubuum_owner` before applying
migrations. The runtime login must not be a member of either privileged role.

### Restricted Managed PostgreSQL

Some managed control planes create identities outside SQL or do not allow the
application operator to run `CREATE ROLE` and `ALTER ROLE`. Create the three
identities and the owner-to-migrator membership through that control plane,
then generate only the database-local ownership and grants:

```bash
hubuum-admin --database-role-grants-sql > hubuum-database-grants.sql
psql "$MANAGED_DATABASE_ADMIN_URL" --set ON_ERROR_STOP=1 \
  --file hubuum-database-grants.sql
```

The provider-created identities must still match the role matrix above. Use a
dedicated Hubuum database: reconciliation intentionally owns and protects all
application objects in its `public` schema.

### Adopting An Existing Single-Role Database

Take a verified backup first. For a no-downtime first adoption, use the
existing application login as the configured runtime role and introduce new
owner and migrator roles around it. Applying the generated setup SQL transfers
ownership and grants runtime DML in one transaction, so existing API and worker
connections keep the access they need after the commit while losing DDL and
direct integrity-table mutation authority:

```bash
hubuum-admin \
  --database-owner-role hubuum_owner \
  --database-migrator-role hubuum_migrator \
  --database-runtime-role EXISTING_APPLICATION_ROLE \
  --database-role-setup-sql > hubuum-database-role-adoption.sql
psql "$EXISTING_OWNER_DATABASE_URL" --set ON_ERROR_STOP=1 \
  --file hubuum-database-role-adoption.sql
```

Set `HUBUUM_DATABASE_RUNTIME_ROLE=EXISTING_APPLICATION_ROLE` on the old and new
server versions during that rollout. Then use this order:

1. Verify the backup and inspect the restore control plane:

   ```sql
   SELECT state, restore_job_id,
          EXISTS (SELECT 1 FROM restore_jobs WHERE status = 'confirmed')
            AS has_confirmed_restore
     FROM system_maintenance
    WHERE id = 1;
   ```

   Continue only when the state is `normal`, `restore_job_id` is null, and
   `has_confirmed_restore` is false. Finish or explicitly recover any confirmed
   restore first. If a pre-upgrade confirmation was interrupted, keep or restart
   the previous release with its existing credential and let its reconciliation
   loop return maintenance to `normal` before changing roles. Do not run the new
   migration or revoke the old authority first. Validated, unconfirmed stages
   may remain in the database.
2. Pause `POST /api/v1/restores/*/confirm` at the ingress for the bounded
   migration window. If the ingress cannot block one route and method, briefly
   stop all old API replicas; workers do not need the migration credential.
   Ordinary API traffic may otherwise continue while the existing login keeps
   compatibility runtime grants.
3. Create the separate migrator credential and run `hubuum-admin --migrate`.
   The database-role migration refuses to run while maintenance is draining or
   a confirmed restore exists; it serializes this check with restore
   confirmation and preserves validated stages.
4. Start `hubuum-admin --restore-executor` with only
   `HUBUUM_MIGRATION_DATABASE_URL`, then roll API and worker processes with only
   the runtime URL. Verify the executor stays running before unpausing the
   confirmation route. Existing validated stages remain confirmable through
   the new API and are claimed by the executor.
5. Run the runtime privilege report, enable strict mode, and perform a staged
   web restore drill against a disposable backup if operational policy permits.
6. In a later credential-rotation rollout, replace the compatibility login
   with `hubuum_runtime`. Keep both runtime grants only for that bounded overlap
   and revoke or disable the old login afterward.

The managed single-host updater automates this transition. It retains the
existing volume's bootstrap password, creates and reconciles the three new
roles, updates their passwords through the local PostgreSQL container, runs the
one-shot migration, and then replaces long-lived containers with runtime-only
credentials. The bootstrap secret remains infrastructure-only and is never
injected into an API or worker container.

Grant reconciliation is safe to repeat after a partial rollout. It moves any
objects owned by the former migration/application role to the non-login owner,
removes pre-existing direct runtime grants before rebuilding the manifest, and
does not fall back to making the runtime role an owner if migration fails. A
failed restore preflight leaves the prior roles and data in place; recover the
active restore and repeat the same reconciliation and migration commands.

Keep the existing login configured as the compatibility runtime identity until
the new API and executor have both passed their health checks. If application
rollback is required after the role migration, the old binary can continue
ordinary service with that compatibility runtime login, but its synchronous
web-restore implementation no longer has destructive database authority. Keep
confirmation blocked during that rollback and use the new one-shot
`hubuum-admin --restore` path for disaster recovery. Database migration rollback
removes any `succeeded` polling receipt before restoring the older status
constraint; it does not re-grant broad ownership to the runtime login.

## Runtime Diagnostics

Audit the runtime connection and current catalog grants before rollout:

```bash
hubuum-admin \
  --database-url "$HUBUUM_DATABASE_URL" \
  --check-database-privileges \
  --role runtime
```

Add `--json` for machine-readable output. The audit checks the role named in
the manifest and also requires PostgreSQL `current_user` to be that role; an
overprivileged connection cannot pass by auditing a different safe identity.

Server startup repeats this audit. `HUBUUM_DATABASE_PRIVILEGE_MODE=warn` is the
compatibility default and emits findings without stopping the process. New
production deployments should use `strict`, which fails startup when the role
is dangerous, incomplete, different from the connected identity, or cannot be
inspected. Runtime configuration reports the mode and role names but never a
database URL or credential.

## Container And Single-Host Deployments

The server image entrypoint checks schema readiness only. It never runs
migrations. For the repository Compose example, initialize the database and
run the one-shot service before starting the application:

```bash
docker compose --profile administration run --rm hubuum-migrate --migrate
docker compose up -d hubuum-restore-executor hubuum
```

Set unique `POSTGRES_PASSWORD`, `POSTGRES_MIGRATOR_PASSWORD`, and
`POSTGRES_RUNTIME_PASSWORD` values in `.env`. The long-lived `hubuum` service
receives only the runtime URL. The isolated
`hubuum-restore-executor` receives only the migration URL, exposes no network
port, and runs read-only with all Linux capabilities dropped. The transient
`hubuum-migrate` service uses the same privileged URL for schema changes.

The single-host installer implements the same boundary automatically. For an
external PostgreSQL server, both URLs are required:

```bash
sudo ./scripts/install-single-host.sh \
  --api hubuum-api.example.com \
  --email admin@example.com \
  --database-url 'postgres://hubuum_runtime:.../hubuum?sslmode=require' \
  --migration-database-url 'postgres://hubuum_migrator:.../hubuum?sslmode=require'
```

The migration URL is stored in the owner-only deployment environment file and
injected only into `hubuum-migrate` and the isolated restore executor. It is not
present in primary or standby API or worker container environments. Managed
updates run migrations, recreate the executor, and only then replace API
replicas.

## Distributed Orchestration

Store runtime and migration URLs in different secret objects and reference the
migration secret only from the one-shot job and isolated restore executor. The
application Deployment should also enable strict diagnostics:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: hubuum-runtime-database
stringData:
  database-url: postgres://hubuum_runtime:REPLACE@postgres/hubuum
---
apiVersion: v1
kind: Secret
metadata:
  name: hubuum-migration-database
stringData:
  database-url: postgres://hubuum_migrator:REPLACE@postgres/hubuum
---
apiVersion: batch/v1
kind: Job
metadata:
  name: hubuum-migrate
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: migrate
          image: ghcr.io/hubuum/hubuum-server:VERSION
          command: ["/usr/local/bin/hubuum-admin", "--migrate"]
          env:
            - name: HUBUUM_MIGRATION_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: hubuum-migration-database
                  key: database-url
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hubuum-api
spec:
  template:
    spec:
      containers:
        - name: hubuum-api
          image: ghcr.io/hubuum/hubuum-server:VERSION
          args: ["--runtime-role", "api"]
          env:
            - name: HUBUUM_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: hubuum-runtime-database
                  key: database-url
            - name: HUBUUM_DATABASE_PRIVILEGE_MODE
              value: strict
```

Apply and await the Job before updating API or worker Deployments. Never mount
the migration Secret into those Deployments, including as an unused projected
volume or broad `envFrom` source. Add a separate one-replica Deployment for the
executor:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: hubuum-restore-executor
spec:
  replicas: 1
  selector:
    matchLabels:
      app: hubuum-restore-executor
  template:
    metadata:
      labels:
        app: hubuum-restore-executor
    spec:
      containers:
        - name: restore-executor
          image: ghcr.io/hubuum/hubuum-server:VERSION
          command: ["/usr/local/bin/hubuum-admin", "--restore-executor"]
          env:
            - name: HUBUUM_MIGRATION_DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: hubuum-migration-database
                  key: database-url
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
```

The executor needs database connectivity but no Service or inbound network
path. Secret rotation should update the applicable workload only: restart API
and workers for a runtime-credential rotation, and recreate both the migration
Job and restore executor for a migrator-credential rotation.

## Destructive Restore

Web restore remains available without putting a migration credential in an API
process. The administrator stages and confirms the exact document through the
API. Confirmation enters draining maintenance and returns `202 Accepted`; the
isolated `hubuum-admin --restore-executor` process re-loads and validates the
stored document, waits for runtime instances to drain, and performs the
replacement with `HUBUUM_MIGRATION_DATABASE_URL`. The capability-authenticated
status endpoint reports `confirmed`, then `succeeded` or `failed`.

The executor accepts no SQL, identifiers, or document path from the network.
It reads only the typed staged artifact referenced by the committed maintenance
row; the adapter rechecks lifecycle ownership, uses closed table and column
lists, and applies replacement and provenance in one transaction. A compromised
administrator session can intentionally request destructive data replacement,
which is inherent in the restore feature.

This boundary protects the migration credential and schema-owner authority; it
does not claim that a compromised runtime database identity cannot damage
application data. The runtime role owns the restore control-plane DML required
to stage and confirm through the backend contract, so direct database compromise
can fabricate a validated request for the executor and replace data, including
restored history. It still cannot obtain schema ownership, execute DDL, mutate
history tables directly, or set trusted restore flags in its own session.

Direct disaster-recovery restore remains available as a one-shot command:

```bash
hubuum-admin \
  --migration-database-url "$HUBUUM_MIGRATION_DATABASE_URL" \
  --restore backup.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
```

Treat this URL as a disaster-recovery credential. Supply it only to the
migration job, isolated executor, or a tightly controlled one-shot restore;
audit every use.
