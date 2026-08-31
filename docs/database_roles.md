# PostgreSQL Database Roles

Hubuum uses three distinct PostgreSQL roles. Long-lived API and worker
processes must use only the runtime role. Schema changes and destructive
restore use a separate one-shot migrator credential, while a non-login owner
holds application objects.

| Role | Login | Purpose | Must not have |
| --- | --- | --- | --- |
| Owner | No | Owns the application schema, tables, sequences, and functions | A password or workload identity |
| Migrator | Yes | Assumes the owner role for embedded migrations, grant reconciliation, and destructive restore | Superuser, `CREATEROLE`, or `BYPASSRLS` |
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
- direct destructive restore is migrator-only.

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
server versions during that rollout. Create the separate migrator credential,
run `hubuum-admin --migrate`, verify the runtime report, and only then enable
strict mode. A later credential-rotation rollout may replace the compatibility
runtime login with `hubuum_runtime`; keep both runtime grants only for that
bounded overlap and revoke the old login afterward.

The managed single-host updater automates this transition. It retains the
existing volume's bootstrap password, creates and reconciles the three new
roles, updates their passwords through the local PostgreSQL container, runs the
one-shot migration, and then replaces long-lived containers with runtime-only
credentials. The bootstrap secret remains infrastructure-only and is never
injected into an API or worker container.

Grant reconciliation is safe to repeat after a partial rollout. It moves any
objects owned by the former migration/application role to the non-login owner,
removes pre-existing direct runtime grants before rebuilding the manifest, and
does not fall back to making the runtime role an owner if migration fails.

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
docker compose up -d hubuum
```

Set unique `POSTGRES_PASSWORD`, `POSTGRES_MIGRATOR_PASSWORD`, and
`POSTGRES_RUNTIME_PASSWORD` values in `.env`. The long-lived `hubuum` service
receives only the runtime URL. The `hubuum-migrate` service receives only the
migration URL.

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
injected only into the transient `hubuum-migrate` service. It is not present in
the primary or standby API container environment.

## Distributed Orchestration

Store runtime and migration URLs in different secret objects and reference the
migration secret only from the one-shot job. The application Deployment should
also enable strict diagnostics:

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
volume or broad `envFrom` source. Secret rotation should update the applicable
workload only: restart API and workers for a runtime-credential rotation, and
create a new migration Job for a migrator-credential rotation.

## Destructive Restore

Destructive restore requires `HUBUUM_MIGRATION_DATABASE_URL` and is available
only through `hubuum-admin`. API staging and status remain available for backup
validation, but API confirmation returns `501 Not Implemented`; a bearer-token
request running under the runtime database role cannot cross the privilege
boundary.

```bash
hubuum-admin \
  --migration-database-url "$HUBUUM_MIGRATION_DATABASE_URL" \
  --restore backup.json \
  --restore-confirmation "REPLACE ALL HUBUUM DATA"
```

Treat this URL as a disaster-recovery credential. Supply it only to a tightly
controlled administrative workload, audit its use, and remove that workload
after completion.
