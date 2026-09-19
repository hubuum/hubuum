# Fresh authentication for credential management

Credential creation and renewal require a single-use approval obtained by
rechecking the acting human's password. A valid bearer token alone cannot
perform these operations. Local user creation and password updates use the same
approval flow. Credential-bearing imports and restore confirmation are also
protected. Ordinary API access, token listing, and revocation do not require
an approval.

This is a breaking change for the frontend, CLI, SDKs, and scripts that manage
credentials. Existing bearer tokens remain valid for their existing permissions
and lifetime. This change does not introduce access/refresh token families.

## Client flow

1. Collect the intended operation, including the target principal, permissions,
   resource scope, and expiry. Display these choices before confirmation.
2. Ask for the acting human's current password. For service-account credentials,
   authenticate the human administrator or owner-group member managing the
   account. Service accounts do not password-login.
3. Send `POST /api/v1/iam/credential-approvals` with the existing bearer token,
   password, and operation. The server selects the human's existing identity
   scope; clients cannot substitute another account or provider.
4. Keep the returned approval in transient protected memory. For token creation
   and renewal, copy the returned `token_expires_at` into the final request,
   including when the original expiry was omitted.
5. Send the normal mutation request with the same bearer token and
   `X-Hubuum-Credential-Approval` header. Preserve the approved request exactly.
6. Discard the approval after success. Display or save the newly issued bearer
   credential using the client's normal secure credential-handling flow.

Approvals last at most 120 seconds, shortened by the originating bearer token's
expiry. They authorize one operation. Changing the target, operation, scope,
expiry, name, description, or other approved request fields requires a new
approval. Switching to another bearer token, even for the same human, also
requires a new approval. The approval is never a normal API bearer credential.

### Create a token

The following bodies use illustrative IDs and passwords. Never put a real
password or approval in shell history, process arguments, logs, or examples.

```http
POST /api/v1/iam/credential-approvals
Authorization: Bearer <current-human-token>
Content-Type: application/json

{
  "password": "<current-human-password>",
  "operation": {
    "kind": "create_token",
    "principal_id": 42,
    "token": {
      "name": "inventory-reader",
      "description": "Read-only inventory integration",
      "expires_at": null,
      "scope": { "permissions": ["ReadObject"] }
    }
  }
}
```

A successful `201` response contains:

```json
{
  "approval": "hca1.<64 lowercase hexadecimal characters>",
  "record": {
    "id": 123,
    "actor_id": 7,
    "token_id": 18,
    "operation": "create_token",
    "target_id": 42,
    "restore_job_id": null,
    "invalidated_at": null,
    "authenticated_at": "2026-09-19T12:00:00Z",
    "expires_at": "2026-09-19T12:02:00Z",
    "consumed_at": null
  },
  "token_expires_at": "2026-09-20T12:00:00"
}
```

`record.token_id` identifies the human's originating bearer token, not the token
being created. Approval timestamps use UTC with a `Z` suffix. Token expiry uses
the existing API's UTC timestamp representation without an offset. Clients
should preserve the returned expiry precision rather than rounding it.

```http
POST /api/v1/iam/principals/42/tokens
Authorization: Bearer <same-current-human-token>
X-Hubuum-Credential-Approval: <approval-from-previous-response>
Content-Type: application/json

{
  "name": "inventory-reader",
  "description": "Read-only inventory integration",
  "expires_at": "2026-09-20T12:00:00",
  "scope": { "permissions": ["ReadObject"] }
}
```

The success response remains the existing token issuance response. Both approval
and token issuance responses use `Cache-Control: no-store`.

### Other operation shapes

| Approval operation | Final request |
| --- | --- |
| `{"kind":"renew_token","principal_id":42,"token_id":99,"token":{"expires_at":null}}` | `POST /api/v1/iam/principals/42/tokens/99/renew`; replace the null expiry with `token_expires_at` |
| `{"kind":"create_user","user":{...}}` | `POST /api/v1/iam/users` with the same `NewUser` body |
| `{"kind":"update_user","user_id":42,"user":{...}}` | `PATCH /api/v1/iam/users/42` with the same `UpdateUser` body and normal revision precondition |
| `{"kind":"import_credentials","import":{...}}` | `POST /api/v1/imports` with the identical `ImportRequest` body |
| `{"kind":"confirm_restore","restore_id":42,"confirmation":{...}}` | `POST /api/v1/restores/42/confirm` with the identical `RestoreConfirmRequest` body |

A user patch requires approval when it supplies a password. Profile-only patches
retain their existing authorization behavior. The password inside `user` is the
new account password; the top-level `password` authenticates the acting human.
Neither password is stored in the approval record or audit event.

### Imports and restores

An import needs approval if any human principal carries `password` or
`password_hash`, including dry runs. Approval requires an unscoped human
administrator. Include the complete import under `operation.import`; preserve
array order and all values for submission. Imports without credentials retain
their existing flow. The approval endpoint accepts up to 2 MiB plus 64 KiB for
the wrapper; the normal import request limit still applies.

Send a client-generated `Idempotency-Key` for credential-bearing imports.
Consumption commits with queue admission; the consumption event records the
admitted task ID. An accepted task can execute after the
approval expires; its worker uses the existing task authorization and scope
checks. Repeating a previously accepted import's matching `Idempotency-Key`
returns that task without consuming another approval. The approval header is
still required at the HTTP boundary. Use task status to resolve ambiguous
admission responses. A changed payload needs a new approval and idempotency key.

Stage and validate a restore using the existing upload flow. Immediately before
confirmation, obtain `confirm_restore` approval with the stage ID and the full
confirmation body: `restore_capability`, `sha256`, and the exact confirmation
phrase. The approval binds all four values. The restore capability remains
required; approval does not replace it. Consumption commits with the transition
to draining maintenance, before the separate executor replaces data. Executor
failure does not make the approval reusable. Continue capability-authenticated
status polling through the existing restore workflow.

A completed restore invalidates every unused approval and retains local approval
evidence outside the replaced identity data. Logical backups do not transfer
approval authority to another installation. Restore completion provenance links
the consumed approval; ordinary event history is replaced by the backup's history.

## Errors and retry behavior

| Response | Client behavior |
| --- | --- |
| `403`, `reason: "reauthentication_required"` | Obtain a new approval; do not repeat the request without one |
| `401` during approval creation | Password authentication or the bearer credential failed; do not retry automatically |
| Other `403` or `404` | Preserve existing permission and visibility behavior; fresh authentication cannot grant missing authority |
| `429` | Respect authentication throttling; avoid repeated password prompts or automated credential guessing |
| Provider unavailable | Fail closed; cached LDAP membership information cannot prove fresh password authentication |
| Mutation validation or transaction failure | No credential change or approval consumption commits; correct invalid input through a new approval |

The same generic approval rejection covers missing, malformed, expired,
previously consumed, mismatched, and invalidated approvals. Clients must inspect
the stable `reason`, rather than parsing human-readable messages.

If the final response is lost, do not assume the mutation failed. Read
`GET /api/v1/iam/credential-approvals/{approval_id}` to inspect `consumed_at`.
The record is visible to its actor and administrators and contains no approval
secret or digests. A consumed approval cannot return the new bearer secret
again. Inspect retained credential metadata, revoke any unwanted credential,
and obtain a new approval before creating a replacement. Concurrent submissions
with one approval can commit at most once.

Password resets and originating-token revocation invalidate unused approvals.
Permission checks run again for the final request. An approval cannot retain
administrator or owner-group authority that the caller no longer has.

## Frontend integration

Keep the bearer token and approval in the BFF's server-side session or request
state. Browser JavaScript must not receive either value. Collect the password in
a confirmation form, forward it transiently, and discard it immediately. Keep
CSRF protections on approval creation and final mutation. Do not cache approval
responses, record form bodies in telemetry, or expose approval headers in proxy
logs.

Keep pending operations separate within a session: an approval for one tab's
operation must not be reused for another. Show the exact target, permissions,
and expiry the user is confirming. If those choices change, restart approval.
Do not turn successful reauthentication into a session-wide elevated flag.

## CLI and SDK integration

Use a hidden interactive password prompt. Do not add password or approval command
line flags whose contents would enter process listings or shell history. Keep
both values out of debug output and HTTP recording. SDK approval values should
have redacted string/debug representations and should not be persisted as
ordinary credentials.

An unattended script holding only a human bearer token can no longer create or
renew tokens. Provision automation credentials through an interactive approved
operation and inject them into the workload's secret store. Existing automation
credentials retain their normal resource access; possession of one does not
provide a credential-management approval. A dedicated unattended rotation
protocol is outside this change.

## Audit and retained evidence

Successful password verification creates a retained approval record and a
`credential_approval.created` audit event in one transaction. Successful use
records consumption and `credential_approval.succeeded` in the same transaction
as the credential change and its existing token/user event, import admission,
or restore confirmation. An event or mutation
failure rolls back the whole operation.

Records retain the actor, originating token ID, operation, target, authentication
time, expiry, consumption/invalidation time, restore stage ID where applicable,
and secret/request digests. Raw approval secrets,
passwords, password hashes, and approved request bodies never appear in these
records or approval audit events. Request fingerprints use a keyed digest so
retained evidence cannot be used to guess a new account password offline.

Structured security logs record successful issuance, committed use, throttling,
authentication failure, and approval rejection without secret values. Audit
events retain request/correlation/trace context through the ordinary event
pipeline and can be selected with `entity_type=credential_approval`.

Approval records remain retained after use or expiry; no automatic purge policy
is introduced here. Ordinary event retention still applies to audit events.
Logical backups include retained audit events but exclude approval records and
digests. A restore preserves local records and invalidates outstanding approvals.

## Deployment transition

Apply migration `2026-09-19-000001_credential_approvals` before starting the new
server. Update frontend, CLI, and SDK credential-management flows before directing
users to upgraded instances. No compatibility switch permits bearer-only minting.

Quiesce protected mutations during the rollout. Upgrade every API replica, task
worker, and event worker before resuming them. Old API instances still permit
bearer-only credential management, and older event readers/workers may reject the
new `credential_approval` entity. Routing writes to new API instances alone does
not solve worker or event-reader compatibility. Update strict event decoders to
accept `credential_approval.created` and `credential_approval.succeeded`.

Application rollback restores the older credential policy and may also require
handling retained events unknown to the old event catalog. Keep the additive
approval table if evidence must be retained: its down migration deletes approval
records. Treat rollback as a coordinated application/data compatibility decision,
not a routine reversal of the security policy.

External storage adapters must implement approval persistence and metadata reads,
accept approval-bearing task and restore requests,
consume attached credential claims atomically with mutations, retain consumed
evidence, enforce origin-token validity and single use, and append both audit
events. The development memory backend implements the same logical contract;
production PostgreSQL supplies durable coordination across replicas.
