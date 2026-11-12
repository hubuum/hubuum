# Security configuration candidates

Authentication and authorization use the shared runtime-settings revisions,
compare-and-swap writes, audit history and replica reconciler. A candidate is
short-lived verification state; it does not change the live configuration.
Local password login remains enabled.

## Create, verify and activate

Use an unscoped local human administrator with a working password and local
administrator grants. The actor must also be an administrator under the current
policy and any proposed authorization policy.

1. Read `GET /api/v1/admin/runtime-settings/schema` and the current group revision.
   The `security` capabilities report supported provider kinds, deployment locks,
   trust profiles and limits. Generic PATCH/DELETE controls cannot edit security
   groups.
2. Create a candidate at
   `POST /api/v1/admin/runtime-settings/security-candidates`. Include the complete
   provider selection for authentication, or the desired authorization backend.
3. For every changed, enabled LDAP provider, submit a real test sign-in to
   `POST /api/v1/admin/runtime-settings/security-candidates/{id}/verify-ldap` with
   `provider_id`, `username` and `password`. The response contains `candidate`
   and a bounded `verification` preview of the subject and complete mapped groups.
   Testing creates no principal, membership, token or session. Passwords are
   never retained. Existing login throttling applies.
4. After every required check passes, keep the returned `verification_receipt`.
   Request a fresh approval from `POST /api/v1/iam/credential-approvals` using the
   local administrator's password and the exact operation below.
5. POST `expected_revision` and `verification_receipt` to
   `/api/v1/admin/runtime-settings/security-candidates/{id}/activate`, with the
   approval in `X-Hubuum-Credential-Approval`.

For example, a candidate selecting local authorization is:

```json
{
  "group": "authorization",
  "expected_revision": 1,
  "backend": "local",
  "source": "runtime"
}
```

The corresponding approval operation is:

```json
{
  "kind": "activate_security_candidate",
  "candidate_id": "c64ef56b-58b2-4831-868f-d140238ae281",
  "group": "authorization",
  "expected_revision": 1
}
```

Candidates last at most five minutes, with at most eight per actor and 256 per
installation. Approval validity is at most 120 seconds and cannot outlive the
originating credential. Both are single-use and bound to the actor, originating
token, candidate ID, complete definition, base revision, secret values/versions
and deployment safeguards. Activation rechecks local recovery grants under the
same transaction that consumes authority and commits settings and audit records.
A failed transaction consumes neither authority. Concurrent verification updates
cannot overwrite another completed check.

GET candidate status is owner-only, returns sanitized reason codes and never
rediscloses a receipt. All candidate responses use `Cache-Control: no-store`.
An expired, failed or stale candidate must be recreated. After an ambiguous
activation response, inspect its status and the current group revision; do not
assume that a failed response means the commit failed. A committed change remains
committed if the responding replica's subsequent reconciliation fails.

## Providers, credentials and connections

Provider UUIDs are server-issued. Reuse the returned ID for subsequent edits;
legacy LDAP definitions without an ID acquire one on their first activation.
The scope, kind and OIDC issuer/OAuth subject namespace cannot be reassigned,
even after removing the definition. Use a new scope for a new issuer or subject
namespace. Client-ID and other configuration changes require verification again.
Disabled providers can be removed or kept without contacting their endpoint or
resolving their existing credentials.

LDAP keeps its deployment-reference and explicit adoption rules. Runtime LDAP
connection and operation timeouts cannot exceed 30 seconds; each verification
has an overall 30-second deadline. DNS is screened on every connection and the
screened socket is pinned while LDAP verifies TLS against the original hostname.

OIDC and OAuth definitions are validated and may be stored disabled. Their live
activation remains blocked until the later provider/interaction implementation
can produce a real browser sign-in proof. Connectivity alone cannot enable them.
The accepted browser contract and fixtures remain in [authentication](authentication/contract.md).
Hosted local/LDAP interactions are available through the shared
[interaction API](authentication_interactions.md), including candidate verification
when the selected LDAP provider has the final pending identity check. For several
enabled LDAP providers, verify the other providers through `verify-ldap` first.

Browser credentials use `keep`, `reference`, `replace` or `clear` actions.
References resolve in consumer-specific namespaces:

| Consumer | Environment prefix | Mounted-file directory |
| --- | --- | --- |
| LDAP | `HUBUUM_LDAP_SECRET_` | `ldap/` |
| OIDC | `HUBUUM_OIDC_SECRET_` | `oidc/` |
| OAuth | `HUBUUM_OAUTH_SECRET_` | `oauth/` |

Replacement credentials require the deployment-owned runtime-settings encryption
key and are encrypted with provider-specific associated data. Reports, discovery
and ordinary audit summaries never return aliases, envelopes, passwords or client
secrets. Candidate approval and activation refresh relevant secret caches so a
source rotation invalidates an earlier receipt before normal cache expiry.

Runtime Treetop configuration requires HTTPS, verified certificates, bounded
connect/request timeouts, no proxies or redirects, and a registered trust profile.
`system` uses system roots; `deployment` additionally uses the configured Treetop
CA bundle. The deployment's Treetop timeouts remain ceilings. Readiness and local
administrator access are checked before activation. Failure never selects local
policy automatically. Deployment references retain their deployment connection
configuration.

`HUBUUM_AUTH_RUNTIME_ALLOW_PRIVATE_TARGETS=true` explicitly permits private
LDAP and runtime browser/Treetop targets. The default denies loopback, link-local,
private and metadata addresses. Set this deployment option before using private
identity services. `HUBUUM_AUTH_RUNTIME_LOCKED_PROVIDERS` is a comma-separated
list of provider names whose definitions and activation cannot be changed.

## Changes, release and replicas

Omitted providers are disabled while authentication is runtime-owned. Removing,
disabling or changing a provider revokes sessions, bearer credentials and pending
approvals owned by its external principals; manual grants remain. Unchanged,
enabled definitions retain their credentials. First adoption and release
conservatively revoke external credentials. A login started against an earlier
configuration revision cannot issue credentials after the change commits.
Rollback is another verified, approved revision and never revives revoked tokens.

To release authentication ownership, create an authentication candidate with
`release_ownership: true` and no provider overrides. It verifies the entire
current deployment selection, including real LDAP test sign-ins. To release
authorization, set `release_ownership: true`, `source: "deployment"`, the actual
deployment backend and no connection override. Activation returns ownership to
deployment while retaining audit history and provider identity bindings.

Notifications accelerate application; periodic reconciliation handles missed
notifications and restart. Requested revisions, responding-replica application,
provider availability and the non-secret runtime authorization connection are
reported by the existing administrator APIs. Stale security state fails closed.
Memory implements the logical contract within one shared in-memory store;
PostgreSQL provides cross-process durability and locking.

## Upgrade and recovery

**Breaking change:** stop every older API and worker before migration
`2026-11-12-000001_security_candidates`. Back up the database and deployment
configuration, apply migrations and role grants, then run matching binaries on
all replicas. Upgrade administrator clients to create, verify, approve and
activate candidates. The old authentication preflight write, direct authorization
PUT and security-group PATCH/DELETE now reject writes. Regenerate OpenAPI clients;
complete Rust storage adapters must implement the added candidate/recovery methods
and handle the additional configuration variants.

For private LDAP, set the explicit private-target deployment allowance before
restarting. Do not downgrade to older binaries after storing new provider/profile
values or activation approval evidence; restore the pre-upgrade database backup
and matching deployment configuration instead.

If remote authorization is unavailable, stop privileged writes on **every**
replica. Use the privileged database credentials on a trusted administrative host:

```sh
hubuum-admin --recover-security --recovery-administrator-id 1 \
  --recovery-admin-group admin --writes-quiesced
```

The ID must identify an existing password-capable local administrator with local
grants. The command commits local authorization and local-only authentication
atomically, records explicit recovery audit evidence, discards candidates and
unconsumed approvals, and revokes external credentials. It never calls the remote
policy backend. Restart/reconcile every replica and verify local administrator
login before resuming writes. `--writes-quiesced` is an operator assertion; the
command cannot stop other processes for you.

Logical restore retains installation-owned configuration, audit and immutable
provider bindings while discarding candidate/approval authority. Candidate secrets
are excluded from portable backups. For a whole-database snapshot restore, keep
the restored installation isolated, run offline recovery, then verify and activate
fresh provider selections before exposing it to clients.
