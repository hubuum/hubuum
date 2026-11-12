# Durable authentication interactions

Status: #575 implementation in progress, draft PR #591 on `project/auth-support`.
The domain model, complete storage adapters, atomic session issuance and v1
password-login transport are implemented. Local and enabled LDAP providers use
public discovery, begin, credentials, protected status, complete, cancel and
issued-session revocation. Existing password-login endpoints keep their wire
behavior. Hosted local/LDAP sign-in, explicit approval, registered BFF handoff,
credential-approval completion and LDAP candidate verification use the same
durable lifecycle. Real OIDC/OAuth execution remains in #577/#578.

## Password interaction API

`GET /api/v1/auth/methods` returns protocol version 1 and only public method
metadata. Begin a `login` with `delivery: "poll"`, the selected provider ID and a
stable client ID using `POST /api/v1/auth/interactions`. Continue with the same
client ID and `Authorization: Hubuum-Interaction <redemption_secret>` on the
interaction's `credentials`, `status`, `complete`, `cancel` and
`issued-session/revoke` operations. Submit only `name` and `password` alongside
the client ID for credentials; the server selects the bound provider scope.
Completion returns the existing session response and preserves that client ID
through refresh.

The secret-bearing JSON operations are for CLI and BFF server-to-server requests
and reject browser Origin/Fetch Metadata headers and query parameters. Public
method discovery is also available to browser callers. All auth responses
use `Cache-Control: no-store`. JSON and redirects use
`Referrer-Policy: no-referrer`; hosted HTML uses `same-origin` so native POST
forms retain the Origin header while suppressing cross-origin referrers. Redemption
secrets contain 256 random bits, are stored only as domain-separated digests,
and cannot authenticate ordinary API routes. Existing password throttling also
applies to credential continuation. Failed credentials allow explicit retries
within the interaction's three-attempt limit.

Early polls return `429` with reason `slow_down` and increasing `Retry-After`.
Terminal and revision errors use stable machine-readable reasons in `ApiError`.
Status after consumption exposes only issuance metadata, and repeated completion
returns `interaction_consumed`. Cancellation cannot undo an issued session;
use the dedicated secret-protected revocation operation after response loss.

## Hosted browser flow

Browser transport is opt-in. Configure the public origin and optional exact BFF
return registrations in deployment settings:

```bash
export HUBUUM_AUTH_BROWSER='{"origin":"https://auth.example.com","return_targets":[{"id":"web","client_id":"hubuum-web","uri":"https://app.example.com/auth/callback"}]}'
```

The origin has no path, query, fragment or user information. Return URIs must use
HTTPS, have no query, fragment or user information, and match a registered client.
For local development only, `allow_loopback_http: true` permits loopback HTTP
origins and targets. Production uses host-only `__Host-` cookies with `Secure`,
`HttpOnly`, `SameSite=Lax` and a ten-minute maximum age. A reverse proxy must
preserve the configured public Host; forwarded headers cannot select the origin.
All replicas must use the same browser configuration.

A CLI or BFF sends `interaction_mode: "browser"` when beginning an interaction.
The default remains `credentials` for existing password clients. The browser next
step returns `verification_uri` and a short `user_code`. Open that URI and enter
the code; never pass the redemption secret to the browser. The hosted forms show
the requesting client identifier and provider, authenticate the user, and show
an explicit approve/deny page with the identity, purpose and safe request details.
Client identifiers are correlation labels, not verified application identities.
The browser never receives session tokens, approval secrets or candidate receipts.

Code attachment consumes the code's ability to attach another browser. The
cookie and hidden CSRF challenge bind continuation to that browser; authentication
rotates the challenge before approval. Every form mutation requires the exact
configured Origin and, when supplied, same-origin Fetch Metadata. HTML is rendered
by MiniJinja with automatic escaping and a restrictive CSP; no JavaScript is used.
Reloading a pending sign-in or approval page preserves the bound interaction.
A failed browser sign-in or expired authentication lease terminates that attempt;
begin a new interaction to retry. Cancellation, denial and expiry never issue a
result. Existing credentials-mode interactions retain their three-attempt limit.

For `delivery: "bff"`, supply a registered `return_target_id` and a random
32–128-character `return_nonce` containing only ASCII letters, digits or `-_.~`.
BFF delivery always selects browser mode. After either decision the browser gets
a 303 to the registered URI with **only** `interaction_id` and `nonce`. The BFF
must bind these to its initiating server-side session, verify the nonce, and
complete using the redemption capability it retained server-side. It must never
treat the redirect itself as authentication. Targets are rechecked before both
handoff and redemption. Removed or changed registrations fail closed. No arbitrary
redirect URI, browser-supplied provider or return location is accepted.

## Privileged purposes

Beginning `credential_approval` requires an existing unscoped human bearer and an
`operation`. The server authorizes and normalizes that operation, binds its exact
content and initiating principal/token. The operation is encrypted using the
existing `ring` AES-256-GCM secret-envelope implementation and an HKDF-SHA-256
key derived from the private redemption capability; only its safe summary reaches the browser. After fresh
authentication as that same principal and any browser approval, `complete` takes
only the client ID, as for login. Completion returns the
existing credential-approval response, never a session. Use that response with
the existing operation endpoint and approval header. Passwords and restore
capabilities in operations remain on the server-to-server path and are excluded
from the browser prompt.

Beginning `provider_verification` requires a local recovery administrator's
unscoped bearer, a `candidate_id`, and the candidate's enabled LDAP provider UUID.
The candidate must be current and have passed every check except this provider's
identity check. Authentication uses the existing LDAP adapter against the exact
candidate configuration; it does not provision identities, synchronize groups,
activate settings or create a session. After explicit browser approval, completion
atomically verifies the candidate and returns the existing LDAP verification
response: a `candidate` report with verification receipt and a `verification`
preview of subject, admission and complete group membership. These reach only
the initiating client. The existing activation endpoint still requires
its separate operation-bound administrator approval. A failed, changed, expired,
revoked or unsupported candidate fails closed. OIDC/OAuth execution and complete
project configure/test/activate qualification remain separate #577/#578/#581 work.

## Authority and completion

An interaction binds its identifier, selected provider UUID (or local password
provider), authentication configuration revision, client and purpose at creation.
The redemption digest is separate from the visible user-code digest. A user code
can attach one browser, but cannot poll, cancel or redeem a session. Browser
continuation requires its cookie and CSRF challenge; successful authentication
rotates the challenge and requires an explicit approval decision.

Only a leased authentication attempt can submit proof. Failed credentials permit
at most three attempts; an expired or ambiguous claim is terminal. Rejected early
polls commit increasing backoff. Every adapter rechecks current authentication
revision and, for privileged purposes, the initiating unscoped human credential.
Provider verification also rechecks local recovery-administrator membership.

Completion locks the interaction and commits consumption with exactly one result:

| Purpose | Result | Additional binding |
| --- | --- | --- |
| Login | Existing Hubuum session family and audit receipts | Current eligible identity and unchanged credential fingerprint |
| Credential approval | Existing approval record and audit receipt | Original actor/token and canonical operation-request digest |
| Provider verification | Verified candidate, without session or identity changes | Candidate ID/content, selected provider namespace, configuration revision, initiating local administrator and passed identity check |

The session service generates access/refresh credentials, invokes atomic storage
completion and returns the existing session response only after commit. A failed
transaction rolls back both issuance and consumption. The caller supplies a
canonical operation digest that includes operation, target and all relevant
request fields; it must not accept an arbitrary client-provided digest.

Persisted documents are private trusted-storage data, never transport DTOs.
Decoding revalidates phase, mode, claim, proof and purpose correlations. Human
proofs retain a credential fingerprint and event-time subject, with current
identity eligibility checked again at completion. Consumed rows retain only
non-secret issuance metadata and correlation hashes, not reusable proof or raw
access/refresh credentials.

## Response loss and bounded retention

Repeating completion cannot issue another result or recover plaintext credentials.
A protected status poll returns consumed metadata. The original client can use
its redemption capability to revoke exactly the issued session family, including
rotated descendants. Cleanup is repeatable and leaves unrelated families intact.
Configuration changes do not remove this cleanup capability.

Interactions live at most 600 seconds (default 300). Retention is limited to
65,536 rows, with at most 4,096 active globally, 256 per client and 32 per
privileged actor. Creation opportunistically removes up to 128 expired rows;
explicit cleanup accepts batches of at most 4,096. The existing 30-second settings
reconciler removes up to 128 expired records per refresh, including while idle.
Consumed metadata survives until one hour after the original expiry for
response-loss cleanup. Other rows
can be removed at expiry. Browser attachment and claim state are bounded within
each record; persistence documents are limited to 256 KiB.

HTTP admission uses `governor`'s GCRA limiter for each IP, allowing bursts of
30 begins and 600 total requests. Capacity replenishes continuously at one begin
every two seconds and one total request every 100 milliseconds. Rejected begins
still consume the total request budget. `Retry-After: 60` is conservative.

Each replica retains at most 10,000 client-IP buckets. Requests without an
attributable IP share one bucket. Active buckets are never evicted to admit a
new address; idle buckets can be reclaimed after a minute, when both quotas
have fully replenished. These per-replica budgets complement the shared
global/client/actor storage limits; they do not claim a cluster-wide IP quota.

PostgreSQL serializes creation capacity checks and locks settings before
interaction, principal and credential rows. Terminal rows cannot be reopened.
The memory adapter stages the whole mutation and commits only after success.
Both adapters retain transition rejections that intentionally mutate state.

## Security building blocks

The interaction implementation delegates reusable security mechanisms to crates:

| Mechanism | Implementation |
| --- | --- |
| Capability generation | `rand` cryptographic thread RNG, 256 bits per secret |
| Independent encryption keys and verification receipts | `ring::hkdf`, RFC 5869 HKDF-SHA-256 with protocol salt and separate purpose labels |
| Stored capability fingerprints | `sha2` SHA-256 with distinct protocol domains |
| Password-bearing operation binding | `hmac` HMAC-SHA-256 keyed by the private redemption capability |
| Operation confidentiality and authentication | Existing `ring` AES-256-GCM envelopes, authenticated with the interaction ID |
| CSRF, interaction-digest and verification-receipt comparisons | `subtle` constant-time byte comparison; public lengths may differ |
| Per-IP admission rate calculation | `governor` GCRA; application-owned bounded bucket retention |
| HTTP, form and cookie parsing | Actix Web and its cookie implementation |
| URL parsing and HTML escaping | `url` and MiniJinja autoescaping |

Hubuum still owns the protocol and its policy: CSRF rotation tied to durable
browser state, exact Host/Origin/Fetch Metadata checks, client-bound return
registrations, authorization, provider freshness, and atomic single-use issuance.
A generic cookie-session or CSRF middleware would not enforce those correlated
database transitions. The crate substitutions do not make this a standard
OAuth/OIDC flow or eliminate the need to review that composition.

## Upgrade and recovery

This is an offline migration. Stop all API and worker replicas and take a
verified backup. Apply migrations and role grants, then start matching binaries
everywhere. Do not run older recovery or restore tools: they do not know to clear
interaction authority. Restore the pre-upgrade snapshot before downgrading.

**Existing #591 preview deployments:** HKDF replaces the earlier draft's custom
hash-based derivation. Pending approval operations and verification receipts
from that draft are incompatible. Stop every old replica and let pending
interactions expire (at most ten minutes) before starting the updated binaries;
clients must begin fresh interactions. No legacy derivation fallback is retained.

Logical backups exclude interactions. Logical restore and offline security
recovery discard pending, ready and consumed interaction records. External Rust
adapters must upgrade the complete SDK graph to `0.9.0`; see the
[SDK migration guide](storage_adapter_sdk.md#upgrading-from-08-to-09).
