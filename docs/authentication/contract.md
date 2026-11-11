# External authentication contract

Status: reviewed design under [#573](https://github.com/hubuum/hubuum/issues/573),
on Project 6's `project/auth-support` branch. This document specifies delivery;
it does not claim that these new endpoints or providers are implemented.
The released [runtime settings](../runtime_settings.md),
[sessions](../auth_model.md), [LDAP](../external_auth.md), and
[credential approvals](../credential_approvals.md) remain the compatibility baseline.

## Decisions and crate ownership

Extend the existing authentication crates. Do not make OIDC pretend to implement
the username/password-only `ExternalIdentityProvider::authenticate` interface.

| Owner | Responsibility |
| --- | --- |
| `hubuum-auth-core` | Provider capabilities, validated identity/subject, membership completeness, interaction purpose and authenticated/fresh-authentication proof types; redacted secret wrappers and adapter errors |
| `hubuum-auth-ldap` | Existing password bind and directory refresh, normalized into the common proofs; preserve its public password flow |
| Proposed `hubuum-auth-oidc` | Discovery, authorization-code exchange, ID-token/JWKS validation, subject-matched UserInfo, Keycloak profile and upstream lifecycle |
| Proposed `hubuum-auth-oauth` | Code exchange, bounded authenticated-user JSON profiles and GitHub profile, with shared core transaction bindings |
| Application models/services | HTTP request conversion, runtime configuration authority, candidate verification/activation, persisted interaction transitions, admission, membership synchronization, approval and existing session issuance |
| Backend-neutral storage / adapters | Complete interaction/configuration contract, transactional single-use consumption, fencing, audit, encrypted upstream credentials and durable PostgreSQL coordination |
| SDKs, BFF, CLI | Generic interaction handling; no upstream token exchange, provider SDK, identity parsing or authorization policy |

Proofs have private fields, fallible validating constructors and explicit accessors.
Adapters return a verified provider identity bound to provider instance, scope,
configuration revision and interaction purpose. Services accept that proof instead
of raw profile JSON. A separate fresh-authentication proof includes the actor,
origin credential, operation digest and verified authentication time; ordinary
login cannot be cast into it. Raw DTOs are not proof types.
Keep OIDC-library types inside its adapter. Keep Actix, Diesel, global config,
`ApiError`, task persistence and session issuance out of auth crates. Convert
adapter errors once at the application boundary. Add crates only in their owning
implementation increments, including workspace/Docker manifest parity then.

## Runtime setup, verification and activation

An administrator must be able to set up and test remote authentication without
restarting the server. Reuse #568's single configuration authority, revisions,
ownership, audit, reconciliation, readiness and effective-value reporting.
Keep draft verification separate from activation: saving or testing a candidate
must not change live login, provision an account or grant permissions.

1. Read the catalogue and current `authentication` or `authorization` revision.
   Edit an existing definition or create a disabled runtime provider.
2. Validate the complete proposed selection against deployment locks and ceilings.
   Resolve aliases through the existing secret source, validate TLS and endpoints,
   and test provider discovery/connectivity. Return bounded, redacted per-check
   results, with `not_run` distinct from `passed`.
   Authentication candidate reports include every candidate provider and its ID,
   including server-allocated IDs for new definitions. Use those IDs to begin
   verification before activation; public discovery does not list draft providers.
   Authorization candidate reports return an empty provider list.
3. For LDAP, test a password bind and search with an administrator-supplied test
   identity. For OIDC/OAuth, run a real browser sign-in against the candidate,
   through the same callback/PKCE/identity rules as live login. The test result
   previews the stable identity, admission decision, groups and completeness.
   Passwords exist only for the test request; browser verification is bound to
   the initiating administrator's management session. Neither test issues a
   Hubuum session, links/provisions an identity or updates memberships.
4. Activate the exact verified candidate with `expected_revision`, a single-use
   verification receipt and fresh local-administrator approval. Bind both to the
   entire selection digest, actor, originating token, secret version identifiers,
   deployment-policy fingerprint and base revision. Receipt lifetime is at most
   five minutes. Recheck authority, secrets, policy and revision at commit; a
   changed input requires verification again. Audit activation atomically.
5. Show committed and locally applied revisions and sanitized verification time.
   New requests use the new immutable configuration snapshot; replicas reconcile
   by notification, startup reload and bounded polling. Restart is not required.

Verification is a point-in-time check, not a promise of future availability.
Browser login is required before *enabling* a new/changed browser definition;
connectivity alone must never be labelled sign-in verified. Every enabled changed
provider must pass its applicable checks. Unchanged providers reuse their existing
accepted definition. Disabling an unavailable provider requires approval and a
revision, but must not require contacting it or decrypting its credentials.
Candidate material and verification authority expire; retain only redacted audit
evidence. Concurrent activate/delete/rollback can commit against one revision only.

### Configuration ownership

The existing baseline already permits runtime-owned LDAP definitions/activation,
encrypted LDAP bind passwords, and local/Treetop backend selection. Extend that
baseline deliberately; do not describe all authentication as deployment-only.

| Setting | Proposed ownership and safeguards |
| --- | --- |
| Provider ID, kind, scope binding | Server-issued immutable provider UUID; scope and provider identity namespace cannot be repointed once bound; migrate through a new scope |
| LDAP definition/activation | Existing deployment-reference or explicit runtime adoption semantics, with non-activating tests added |
| OIDC issuer/client ID, scopes, supported signing algorithms, membership/admission policy | Runtime definition with validated typed fields, restricted by deployment allowlists/locks; issuer/client changes invalidate outstanding interactions and provider sessions |
| OAuth profile/endpoints, client ID, scopes, bounded JSON field mappings | Runtime administrator metadata under the same outbound restrictions; profile identity semantics cannot be changed in place |
| Provider secret alias or write-only credential | Runtime reference, or write-only encrypted credential input using the existing runtime-settings envelope; never ordinary plaintext configuration, discovery, logs or audit |
| Remote authorization connection profile | Runtime Treetop URL and connect/request timeouts within deployment bounds, plus a deployment-registered TLS trust-profile ID; preflight must prove readiness and that the acting local recovery administrator remains an administrator |
| Secret-source selection/root, encryption/signing/hash keys, TLS trust material, egress policy, process/storage backend, resource ceilings, public origin and registered BFF destinations | Deployment-owned; editing these safeguards still requires deployment action |
| Local password login/recovery administrator | Always available independently of the external selection; no runtime option can disable it |

Moving the non-secret Treetop connection profile to runtime is an intentional
extension requested for this project. Current released connections are still
deployment-owned. Selecting a pre-registered trust profile is allowed; uploading
arbitrary CA files, disabling certificate validation or changing the secret root
through this API is not. Treetop policies remain managed by Treetop, not this UI.
Do not confuse this with downstream remote-target authentication, whose existing
per-target secret-reference APIs remain separate.

Deployment may lock a definition or individual fields. The catalogue reports
`editable`, safe lock reasons, ceilings and supported kinds; runtime requests
cannot bypass them. Preserve deployment references and require explicit adoption.
Omitted deployment providers stay disabled while the selection is runtime-owned.
Report requested/effective/applied revisions without aliases or encrypted envelopes;
credential edits use keep/replace/clear actions instead of echoing stored secrets.
Aliases use existing syntax and consumer-specific namespaces introduced by #574.
Secret rotation invalidates candidate receipts; live clients resolve a new version
within the existing bounded cache policy, with no fallback to an expired secret.

Use HTTPS with verified certificates (LDAP retains verified LDAPS/StartTLS).
Validate configured destinations and every resolved address on every connection;
deny loopback, link-local and metadata destinations unless explicitly permitted by
deployment. Private IdPs/Treetop require an explicit deployment egress allowance.
Do not follow cross-origin redirects with credentials. Reject userinfo/fragments in
endpoint URLs, arbitrary headers, runtime script expressions and user-chosen
destinations. Discovery/JWKS/profile requests have deployment-bounded time, size,
cache, redirect and concurrency limits. PKCE or TLS cannot be disabled per login.

### Rollback and failure

Rollback is a new revision, subjected to the same verification and local-recovery
checks; it does not resurrect revoked sessions, approvals or discarded identities.
Releasing runtime ownership preflights the full deployment selection. Startup must
never overwrite a runtime choice. A stale or invalid security snapshot fails closed
for new requests and readiness according to #568, retaining the last valid snapshot
for recovery diagnostics rather than serving it beyond its security stale bound.
An unready Treetop profile never silently falls back to local authorization.

Keep a tested local human administrator, its password and grants in both current
and proposed authorization policies. Security edits require that local actor and
a fresh operation-bound approval. A new remote provider cannot approve its own
activation. If all remote policy is unavailable, use a documented, offline
`hubuum-admin` recovery operation to commit an audited revision selecting local
authorization and local-only authentication, after quiescing privileged writes.
This is explicit break-glass administration, never an automatic HTTP bypass.
Reconcile all replicas before resuming writes. Implement and test the recovery
operation in #574; its existence is not claimed for the released baseline.

Runtime configuration/audit retain their existing installation ownership across
logical restore. Drafts, receipts, browser state, upstream credentials and approval
authority are not portable backup contents. Restore invalidates outstanding
interactions/receipts and destroys retained upstream credentials, requiring new
provider login. Document whole-database recovery separately. Memory implements
the logical contract only; restart/shared-replica durability requires PostgreSQL.

## Hubuum interaction protocol version 1

Choose a **custom Hubuum protocol**, inspired by RFC 8628, not a compliant OAuth
device authorization/token endpoint. Clients discover `interaction_version: 1`.
An upstream device grant is unnecessary: Hubuum hosts the browser flow. Human
automation still uses provisioned service-account/PAT credentials.

The separate [proposed OpenAPI](openapi.proposed.json) describes additive paths
and typed bodies; [fixtures](fixtures.json) are shared server/client vectors.
The production `docs/openapi.json` remains generated from implemented handlers.
Do not generate clients against proposed paths as if a released server supports them.

| Method and path (all API paths under `/api/v1`) | Meaning |
| --- | --- |
| `GET /auth/methods` | Safe typed discovery; enabled public provider ID/name/kind, password/browser mode and capability flags; no issuer, endpoint, credential alias or internal diagnostic |
| `POST /auth/interactions` | Begin `login`, actor-bound `credential_approval`, or administrator-only `provider_verification`, with `client_id`, provider ID and delivery mode |
| `POST /auth/interactions/{id}/credentials` | Password continuation for local/LDAP, protected by interaction secret; existing login throttling applies |
| `POST /auth/interactions/{id}/status` | Protected polling/status; secret is in an Authorization header, never the URL |
| `POST /auth/interactions/{id}/complete` | Atomic single-use redemption by the initiating BFF/CLI, returning the existing session, approval or verification result according to purpose |
| `POST /auth/interactions/{id}/cancel` | Idempotent cancellation by the initiating secret; no effect after consumption |
| `POST /auth/interactions/{id}/issued-session/revoke` | Secret-protected orphan-session cleanup after an ambiguous completion response; never returns credentials |
| `GET /auth/callbacks/{provider_id}` | Provider-specific query callback to Hubuum; validate state, browser binding, issuer and pinned provider/revision before exchange |
| `GET /auth/interactions/browser` | Hubuum HTML entry accepting a user code, then CSRF-protected continue/approve/deny forms; no credential JSON |
| `POST /admin/runtime-settings/security-candidates` | Create/validate non-live candidate for authentication or authorization, at the current group revision |
| `GET /admin/runtime-settings/security-candidates/{id}` | Sanitized checks and browser-test outcome, visible only to its initiating local administrator |
| `POST /admin/runtime-settings/security-candidates/{id}/activate` | Approved atomic revisioned activation of exact verified candidate |

The local provider has a reserved stable ID `local`.
LDAP/OIDC/OAuth providers use server-issued UUIDs in discovery, interactions,
callbacks and candidate reports; names are display metadata, never provider IDs.
Shared fixtures use deterministic valid UUIDs for those external providers.
Existing `GET /auth/providers`, `POST /auth/login` and session refresh/logout retain their
wire behavior. Old clients see the existing password-provider list, not names
they cannot authenticate by password. New clients use typed discovery; a genuine
404/unsupported-version falls back to existing password login, while a timeout,
503 or malformed success does not trigger a downgrade. Ordinary session responses
reuse the exact `SessionResponse`; approval completion reuses its existing response.
New provider administration requires new server/client versions, not changes to
old password bodies. No upstream password grant, implicit flow or client token login.

### Secret and browser binding

Begin returns an opaque interaction ID, 256-bit redemption secret, expiry,
poll interval and a tagged `credentials` or `browser` next step. The browser step
contains a Hubuum verification URI and a distinct human-readable user code.
User codes (at least 40 bits, short-lived, attempt-limited) only locate a pending
request; neither they nor public `client_id` values can redeem it. Store redemption
digests, encrypt recoverable PKCE/upstream secrets and redact all representations.
Use `Authorization: Hubuum-Interaction <secret>` only on interaction endpoints;
it is not a normal API bearer token. Beginning a privileged purpose additionally
requires the existing valid human bearer credential and exact operation/candidate.

For browser flows, bind the first visit to a server-generated Secure, HttpOnly,
SameSite=Lax host-only browser cookie and a one-time CSRF value before redirecting.
State, nonce and S256 verifier are independent and transaction-specific. Upstream
redirects end at the registered Hubuum provider-specific callback, never the CLI
or arbitrary BFF URL. Verify both state and the same browser binding on return;
unsolicited/wrong-browser callbacks cannot complete the interaction.
Local development HTTP is explicitly deployment-scoped, not a production option.

The browser confirms provider, client display name and authenticated identity
before approving a CLI session, especially on another device. User-code entry
alone does not approve. Warn against approving an unsolicited terminal request.
For BFF mode, begin runs server-side and the BFF stores the redemption secret plus
a per-login nonce in its server session. A registered BFF return URI receives only
an opaque interaction ID and that nonce; both are verified against the initiating
HttpOnly session. The BFF redeems server-to-server and rotates `hubuum.sid`.
Keep the same `client_id` from begin through every interaction request and
subsequent session refresh: `hubuum-frontend` for the BFF and `hubuum-cli` for the
CLI. Completion must install the session with that original client binding.
No bearer, refresh, approval, redemption secret or upstream credential enters
browser JavaScript, URLs, logs or local storage. The return nonce is not a bearer
capability. Use no-store and no-referrer on HTML/redirect/token responses; exclude
callback query strings and sensitive bodies/headers from proxy/tracing logs.
Browser mutation forms and BFF routes require same-origin/CSRF validation.

Apple form POST is not part of the initial query-callback implementation. #580
must add a separately reviewed correlation mechanism that works without a Lax
cookie on cross-site POST, plus explicit PKCE/freshness capability decisions.
It must not weaken the generic callback checks.

### State, retries and concurrency

States are `pending`, `ready`, `denied`, `expired`, `cancelled`, `consumed`, `failed`.
Pending may wait for credentials, provider callback or explicit browser approval.
Only validated identity plus approval produces `ready`; only `complete` consumes
it and issues the purpose-specific result in one storage transaction. Lock/fence
the interaction, check expiry, initiating credential, provider revision and browser
proof, then commit session/approval/audit plus consumption atomically. Concurrent
completions can issue at most one result. Upstream code exchange uses a leased
claim; a crash after an ambiguous exchange fails the interaction instead of
blindly replaying the code. Restart never releases consumed/terminal state.

| Situation | HTTP / reason | Client action |
| --- | --- | --- |
| Waiting | `200`, `pending` | Poll at or after the advertised interval |
| Ready | `200`, `ready` | Complete once using the same client and secret |
| Wrong secret, unknown ID or wrong client | `401`, `interaction_invalid` | Stop; do not reveal whether the ID exists |
| Denied/cancelled | `403`, `access_denied` / `interaction_cancelled` | Stop and let the human explicitly restart |
| Expired | `410`, `interaction_expired` | Discard secrets and begin again |
| Previously consumed | `409`, `interaction_consumed` | Never replay issuance; see response-loss policy |
| Changed security revision | `409`, `configuration_changed` | Begin again against current discovery |
| Early/abusive polls | `429`, `slow_down`, `Retry-After` | Increase interval by at least five seconds; jitter and exponential backoff for transport failures |
| Unsupported evidence/feature | `422`, `capability_unsupported` | Explain limitation; never fall back to weaker proof |
| Provider temporarily unavailable | `503`, `provider_unavailable` | Honor Retry-After within expiry; no cached login proof |

Default interaction TTL is five minutes; deployment ceiling ten minutes. Poll
interval starts at five seconds. Apply bounded per-IP/client/actor creation,
user-code lookup and poll budgets, bounded active rows and expired-row cleanup.
Do not let invalid input create unlimited state or arbitrary metric labels.
Passwords are not retried automatically. Terminal reasons are stable machine
fields in the ordinary `ApiError` surface; messages remain sanitized.

A lost begin response requires a new begin (the orphan expires). A lost completion
response must not mint another session or redisclose a secret: authenticated
status reveals only consumed metadata/session ID. An initiating BFF/CLI that did
not receive credentials can cancel/revoke that orphan session using the same
interaction secret and a dedicated consumed-session cleanup operation, then begin
again. Cleanup revokes only that issued session, cannot recover tokens, and is
idempotent. #575 must implement cleanup before claiming safe response-loss handling.
For approval loss, inspect existing approval metadata and obtain a new approval;
never reuse a consumed grant. Cancelling before consumption wins atomically or
reports consumed; it cannot silently undo a successfully delivered session.

## Provider identity, admission and groups

OIDC identity is the exact validated `(issuer, sub)` within an immutable provider
instance/scope binding. OAuth identity is `(provider UUID, stable subject)` from
the configured authenticated-user endpoint using the server-exchanged token.
GitHub numeric IDs are parsed losslessly and normalized as decimal strings, never
through a floating-point value. Profile name/email may change or be absent.
Reject an absent, empty, malformed, ambiguous or changed subject. No automatic
linking by username/email and no client-supplied profile/token authentication.
Database unique constraints serialize first provisioning; name collisions fail
without overwriting another principal. A scope cannot silently change identity
provider/kind/issuer. Client-ID change requires new login, not subject reassignment.

Default admission is `existing_only`. An administrator may explicitly choose
`provision_without_grants` or a bounded stable-subject allowlist; a newly
provisioned identity has no permissions from admission itself. Apply policy to
every new login, including a previously materialized identity. Existing sessions
also recheck eligibility on the configured schedule. Interactive account linking
is deferred: reject linking requests explicitly. A future reviewed migration may
link identities only after proving both sides and defining grant transfer.

Local manual membership is the default for consumer providers. Optional
provider-managed membership requires a configured claim source, JSON Pointer,
string-array type, complete-set semantics, bounded mapping rules and stable keys.
ID-token claims are signature-validated; UserInfo must match the ID-token `sub`.
Keycloak group paths use full paths as keys; a path rename is remove/add unless
the deployment supplies immutable group IDs. Realm/client roles are separate
fields and require explicit mappings into namespaced provider groups. `admin`,
`site_admin`, email domain, GitHub orgs/teams and Google hosted domain never grant
local administrative authority implicitly.

| Membership result | Required behavior |
| --- | --- |
| Local-only mode, claim absent | Preserve manual memberships; do not invent provider grants |
| Complete validated empty list | Remove only this provider's grants |
| Complete nonempty list | Reconcile only this provider's source; preserve manual/other sources |
| Required claim absent, wrong type, truncated or incomplete | Reject fresh authentication/refresh, invalidate cached provider authority and require successful new login; never reinterpret as empty or a successful unchanged snapshot |
| Temporary upstream outage | Previously validated grants only within the configured max-stale bound; never new login/approval |

Direct claim mappings into existing local groups are **deferred** for the initial
milestone. Reject that configuration instead of silently ignoring it. Existing
manual memberships in local groups remain supported, survive provider group
removal, and can intentionally outlive it. Admission/deprovisioning invalidates the
principal's externally authenticated sessions even when manual grants remain.

## Upstream freshness, credentials and privileged actions

Record last successful eligibility/membership refresh separately from Hubuum
session rotation. Default refresh interval is 300 seconds and max-stale 900 seconds,
subject to stricter deployment ceilings and `max_stale >= refresh_interval`.
At a due API request/session refresh, prove the same subject and current eligibility
through the adapter's documented refresh/UserInfo/authenticated-user mechanism.
If no usable mechanism remains, require interactive login; do not let Hubuum
refresh perpetually extend upstream authority. Provider disable, subject mismatch,
definitive account rejection or invalid grant revokes provider-derived sessions
and invalidates approvals. Temporary failure may use the last verified snapshot
only to max-stale; afterward deny requests with `503`. The outage bound is the
configured max-stale, not instant revocation. Local disable/revocation remains
immediate. Apply identity freshness on external-human PAT use too, preserving
existing LDAP behavior; service-account credentials have no upstream identity.

Retain only upstream credentials required for that lifecycle, encrypted at rest
using versioned secret-source keys and associated provider/principal/session
bindings. Never store them in portable exports or normal config/audit records.
Discard authorization codes/PKCE after terminal completion. Serialize upstream
refresh-token rotation; invalid grants do not fall back to older credentials.
Deletion/disable/logout destroys local upstream credentials and attempts supported
revocation without delaying local revocation; record safe failure diagnostics.
Local logout does not promise global IdP logout. RP logout/revocation capabilities
are separate and optional; never redirect users to arbitrary logout destinations.

Fresh approval starts with the existing unscoped human actor/token and the exact
existing operation shape, including normalized expiry. Bind the proof to that
actor's immutable upstream identity, provider/revision, origin token, operation
digest and one interaction. Recheck token validity/authority at completion and
at the final mutation. Issue the existing single-use approval (at most 120 seconds,
also bounded by origin-token expiry). No session-wide elevated flag.
Extend the existing credential-operation enum with `activate_security_candidate`,
binding candidate ID, group, base revision and its server-held complete definition
digest. The approval must not merely authorize a provider name or a generic
"change settings" action. Existing password-approval requests accept this additive
operation when #574 implements it; their established response remains unchanged.

Local/LDAP use a real password verification. OIDC requires `max_age=0` and a
validated signed `auth_time` no earlier than interaction start, allowing at most
60 seconds of deployment-bounded clock skew; check required configured `acr`/`amr`
values only where the provider profile actually guarantees them. A different
authenticated subject is rejected, not linked. Consent, account picker, redirect,
new ID-token `iat` or Hubuum refresh is not fresh evidence. GitHub/generic OAuth
without independently verified authentication-time evidence returns
`capability_unsupported`. Do not ask those users for an unrelated local password
or bypass approval; an existing local administrator can perform authorized
administrative operations through its own separately approved session.

## Capability and qualification matrix

These are contract requirements or documented candidates, **not completed provider
qualification**. Public discovery advertises only capabilities implemented and
verified for the selected profile. Runtime preflight refuses required unsupported
capabilities. Token endpoint authentication is server-owned in every case.

| Profile | PKCE / token authentication / callback | Identity and groups | Refresh / logout / fresh approval | Gate |
| --- | --- | --- | --- | --- |
| Local | Password, no upstream callback | Local principal/manual groups | Existing Hubuum session lifecycle; actual password recheck | Preserve compatibility |
| LDAP | Verified TLS password bind | Stable configured subject, complete directory group mapping | Existing bounded directory refresh; local logout; password rebind | Preserve compatibility and runtime test |
| Keycloak OIDC | S256 required; `client_secret_basic` initially; query callback | Exact realm issuer/sub, JWKS, optional subject-matched UserInfo, explicit full-path group mapper | Server-retained refresh where enabled, current eligibility check; supported revocation/RP logout; validated `auth_time` for approval | Required #577 real pinned fixture |
| Generic OIDC | S256 required; supported advertised method intersected with adapter allowlist | Signed issuer/sub, bounded claims; never assume a groups claim | Explicit profile capability, otherwise interactive reauth; verified fresh evidence required | No universal compatibility claim |
| GitHub.com OAuth App | Documented S256; `client_secret_post`; query callback | Authenticated `/user`, lossless numeric `id`; optional email, local groups | Retain access token for bounded `/user` eligibility checks; no assumed refresh; local logout and supported revocation; fresh approval unsupported | Required #578 authorized live qualification; Enterprise separate |
| Generic OAuth JSON profile | S256 required; `client_secret_basic` or `client_secret_post`; query | One trusted bearer-authenticated GET JSON object with bounded field pointers and exact string/integer subject type | Explicit refresh/revocation profile or reauth; no inferred freshness | #578 second synthetic provider; complex profiles need adapter |
| Google OIDC | Candidate code/PKCE and server credentials; query | Issuer/sub; no Workspace group lookup in ordinary login | Provider-specific refresh/revocation and `auth_time` guarantees must be qualified | Follow-up #579, not initial milestone |
| Apple OIDC | Signed client-secret JWT; form POST; PKCE capability unresolved | Issuer/sub, private-relay email and first-authorization profile handling | Rotation/revocation/freshness need explicit provider-specific design | Follow-up #580; disabled until reviewed qualification |

Protocol sources checked 2026-11-11: [OAuth security BCP](https://www.rfc-editor.org/rfc/rfc9700.html)
for code binding, exact redirects and mix-up protections;
[OIDC Core](https://openid.net/specs/openid-connect-core-1_0.html)
for issuer/subject/nonce/audience and signed authentication-time validation;
[RFC 8628](https://www.rfc-editor.org/rfc/rfc8628.html) for the device-style
separation and polling model (Hubuum's protocol is custom).
[Keycloak endpoints](https://www.keycloak.org/securing-apps/oidc-layers),
[GitHub authorization](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps),
[GitHub authenticated user](https://docs.github.com/en/rest/users/users#get-the-authenticated-user),
[Google OIDC](https://developers.google.com/identity/openid-connect/openid-connect),
and [Apple credential errors](https://developer.apple.com/documentation/technotes/tn3107-resolving-sign-in-with-apple-response-errors)
inform profile differences; they do not substitute for the qualification gates.

## Cross-client review and implementation handoff

[Review record](review.md) maps the proposed contract to pinned current server,
BFF, Rust async/blocking, Python sync/async and CLI surfaces. It records author
compatibility review separately from independent source-review acceptance. Do not close #573 or
mark #574 Ready until the contract PR is reviewed, accepted and merged into
`project/auth-support`; record its immutable commit and fixtures digest then.

Shared fixture validation is a repository-tool check, not proof of runtime
authentication security. The implementing Rust services/adapters own behavior,
authorization, transaction/race and production-adapter tests. Each client consumes
the same fixtures for decoding/state handling in its own implementation issue.
The corpus declares its logical UTC `clock`. Freeze a test clock there, or rebase
all fixture timestamps by one common offset in the consumer harness, preserving
durations and explicit expired-state preconditions. Never decode success examples
against wall-clock expiry or submit these public deterministic credentials to a
real server.
Each fixture's `request` contains its JSON body or callback query parameters;
path parameters and authentication setup follow its `requires` preconditions.
`invalid_request_fields` lists JSON pointers to deliberately malformed or
unsupported fields in negative cases. Removing only those fields must leave a
structurally valid request; all other fixture requests conform without repair.
Provider live qualification remains #577/#578 and ecosystem acceptance #581.
