# External authentication contract review

This record separates author compatibility review from independent source-review
acceptance for [#573](https://github.com/hubuum/hubuum/issues/573). The [contract](contract.md),
[OpenAPI proposal](openapi.proposed.json) and [47 shared vectors](fixtures.json)
form one review unit.

## Identity proof implementation in progress (#576)

Implementation starts from accepted integration commit
`04937a948c6b29055c4bcaf858b5ef1b5ada6fb1` on `project/auth-support`.
The sole native prerequisite, #575, is closed as completed with its accepted
interaction candidate and contract handoff. #576 remains In progress; its
acceptance criteria are not completed by this initial proof increment.

`hubuum-domain` now defines (and `hubuum-auth-core` re-exports) private-fielded,
non-deserializable external
identity, membership, admission and fresh-authentication proofs. A context pins
the validated provider definition and complete interaction binding. OIDC identity
uses the exact issuer/subject within the provider instance and scope. OAuth
subjects use the configured JSON pointer and exact string/integer type, preserving
integers above the floating-point precision and machine-integer limits. Optional
profile metadata cannot link identities. Admission returns a proof only for login;
verification and credential approval cannot use it to provision a principal.

Membership evidence distinguishes local-only preservation, authoritative empty
sets, missing/malformed claims and incomplete observations. Only the configured
source and pointer contribute groups. Full-path keys make renames remove/add;
immutable keys ignore display metadata. These groups confer no implicit local
administrative authority. The pure freshness policy defaults to 300/900 seconds
and accepts stricter bounds; it takes upstream verification time, never session
rotation time. LDAP's existing public password API and freshness defaults remain
unchanged.

Fresh approval requires an OIDC approval request bound to the existing immutable
identity, actor, token, operation and complete interaction. It checks signed
authentication time against the request start with at most 60 seconds of skew
and reapplies admission. OAuth returns unsupported capability. These constructors
do not verify signatures or perform network authentication: the owning adapters
must validate responses, request `max_age=0` and check any supported required
`acr`/`amr` evidence before calling them.

The service/storage integration adds `AuthenticateExternal` to the existing
interaction operation. Both adapters preflight the claimed interaction, provider
binding and purpose, check admission under the stable-identity lock, and commit
identity/membership writes with interaction progress. PostgreSQL serializes
absent-row provisioning across replicas before taking actor/principal locks.
Older observations and callbacks predating a definitive rejection cannot restore
authority. The memory adapter now rolls back failed synchronization and retains
independent membership provenance through removal and backup.

Fresh approval consumption uses the existing purpose-bound completion path and
checks the proof's start against the persisted interaction start. Ordinary
password evidence is limited to local/LDAP. The SDK therefore advances to 0.10;
external adapters must implement the new action and upgrade all ten crates.

Existing browser authority now uses its upstream verification timestamp for both
bearer use and session refresh. Without a usable renewal mechanism, an identity
due for refresh must sign in again. A recorded temporary upstream failure permits
existing authority only inside max-stale and returns 503 afterward. Neither
session rotation nor profile persistence substitutes for upstream verification.
LDAP retains its existing refresh path and avoids an extra binding lookup.

Validation of this integration is in progress. Retained upstream credentials,
protocol-specific renewal/revocation and real-provider qualification still need
their owning adapter implementation and handoff review; no browser provider is
enabled by this increment. #576 remains In progress, #577/#578 remain Blocked,
and PR #592 remains draft against `project/auth-support`. No HTTP schema changes
are introduced by these service/storage boundaries.

## Reviewed baseline

The accepted #568/#572 foundation is server integration commit
`4d8c73e0b1fd4e682739a3c6adb3aadb0433a2db`, including main
`dd27a4c0c401dff873351d7cf80bf4bbf33537f1`. Both native blockers are closed
as completed, with their acceptance ledgers inspected. Companion revisions are
the accepted #572 sources with released session/runtime-settings interfaces.

| Surface and pinned source | Author compatibility findings | Owning implementation |
| --- | --- | --- |
| [Server](https://github.com/hubuum/hubuum/tree/4d8c73e0b1fd4e682739a3c6adb3aadb0433a2db) | Reuse `SessionResponse`, session issuance and operation-bound approvals. Extend password-oriented auth-core with separate browser/proof capabilities. Runtime LDAP/authz selection already exists; non-live verification and connection profiles are additive. | #574–#578 |
| [Rust async](https://github.com/hubuum/hubuum-client-rust/blob/93d3aa460d8cf70f34096d694c727afa8a443d29/src/client/async/authentication.rs) | Preserve session typestate and `SessionClientId`; add generic interaction/candidate types. Existing session decoding/storage is reusable after completion. Redact interaction secrets and receipts. | [SDK #147](https://github.com/hubuum/hubuum-client-rust/issues/147) |
| [Rust blocking](https://github.com/hubuum/hubuum-client-rust/blob/93d3aa460d8cf70f34096d694c727afa8a443d29/src/client/sync/authentication.rs) | Match async shapes/errors. Expose a single poll attempt and timing metadata so callers control cancellation and waiting. No provider implementation in the SDK. | SDK #147 |
| [Python sync](https://github.com/hubuum/hubuum-client-python/blob/144c5d41eda8028e21935c1aa520fed813cd0739/src/hubuum_client/services.py) | Reuse typed `AuthService` and managed `SessionResponse`. Add redacted secret types and constrained interaction transport: ordinary bearer handling must not overwrite the dedicated interaction Authorization scheme. | [SDK #92](https://github.com/hubuum/hubuum-client-python/issues/92) |
| [Python async](https://github.com/hubuum/hubuum-client-python/blob/144c5d41eda8028e21935c1aa520fed813cd0739/src/hubuum_client/async_services.py) | Match sync methods, tagged next steps and state/reason decoding. Cancellation must resolve ambiguous completion. Share fixture decoding without duplicating policy logic. | SDK #92 |
| [BFF](https://github.com/hubuum/hubuum-frontend/blob/55e7f445356bb603d463fe5397bd71cfa0f14702/src/lib/auth/session.ts) | Retain Valkey credentials and HttpOnly `hubuum.sid`. Store pending login nonce/secret server-side, bind backend origin/revision, validate registered return and CSRF, redeem server-to-server and rotate session ID. Generate production models only from implemented OpenAPI. | [#196](https://github.com/hubuum/hubuum-frontend/issues/196), [#197](https://github.com/hubuum/hubuum-frontend/issues/197) |
| [CLI](https://github.com/hubuum/hubuum-cli/blob/d2da4ac52232b00edb434d49cfda85f30d460cb4/src/app.rs) | Keep `hubuum-cli` client ID, hidden password prompt and protected store. Browser and SSH login share protected polling; no loopback listener or upstream OAuth code. Clean up lost completion before restarting. | [#159](https://github.com/hubuum/hubuum-cli/issues/159) |
| Runtime administration and ecosystem docs | Expose candidate checks and exact activation inputs. Distinguish connectivity from successful test sign-in, preview identity/groups without writes, preserve live login until activation, report replica application. Distinguish deployed behavior from the proposal. | [CLI #160](https://github.com/hubuum/hubuum-cli/issues/160), [frontend #198](https://github.com/hubuum/hubuum-frontend/issues/198), [site #7](https://github.com/hubuum/hubuum.github.io/issues/7) |

## Follow-up source review

The 2026-11-11 follow-up compared the proposal and fixtures with the pinned
implementations above, including both Rust and Python transports, session
validation/storage, credential-approval handling, the BFF login/mutation routes
and CLI persistence. No further contract blockers were found after correcting
the BFF fixture's client binding. This is source compatibility review, not
execution of the proposed flows in clients that have not implemented them.

- **Server:** completion can return the existing session and approval response
  shapes. Preserve the origin-token and exact-operation binding when extending
  `CredentialActor` and the fresh-authentication proof; a rotated or expired
  origin credential requires restarting approval, not silently rebinding it.
- **Rust async/blocking:** the existing raw-request builder rejects Authorization
  overrides. Add dedicated, origin-bound interaction transport; do not weaken
  that general guard. Reuse session decoding and store installation with the
  original `SessionClientId`, and keep completion/password submission single-attempt.
- **Python sync/async:** authenticated transport overwrites Authorization with its
  bearer. Interaction methods must bypass that injection and automatic session
  refresh. Extend diagnostic redaction to the bare `Hubuum-Interaction` secret;
  the current scheme-aware stripping recognizes only Basic/Bearer. Preserve
  normalized approval expiry when sending the bound mutation.
- **BFF:** [session storage](https://github.com/hubuum/hubuum-frontend/blob/55e7f445356bb603d463fe5397bd71cfa0f14702/src/lib/auth/session-credentials.ts)
  fixes the client ID to `hubuum-frontend`. The former BFF completion vector used
  `hubuum-cli`, which would create a session that cannot be refreshed using that
  store. The corrected `bff_begin` → `bff_server_completion` pair includes the
  registered return target, nonce and initiating-session precondition. A regression
  now checks that binding against the pinned BFF convention.
- **CLI:** the existing store scopes credentials and serializes writes. Keep
  `hubuum-cli`, install completed credentials through the SDK's managed session
  path, respect poll timing/cancellation, and perform orphan-session cleanup after
  an ambiguous completion before restarting.

These implementation requirements remain with the owning issues in the table;
the author follow-up does not qualify a provider or establish independent acceptance.

## Independent source-review acceptance

A user-authorized separate Codex reviewer, with fresh context and no editing role,
reviewed commit `6aa144b55070715eb932d5e7be3e4bd36e74213e` across all seven
surfaces. It found substantive coverage of all eight #573 criteria and requested
one correction before acceptance: external provider IDs in the schema and
positive fixtures must obey the prose's immutable UUID invariant. The correction
uses shared local-or-UUID/external-UUID schemas, deterministic provider UUIDs and
stdlib format checks. The throttled-poll vector also now advertises `Retry-After`
to exercise the documented backoff.

On 2026-11-11, the separate reviewer accepted corrected commit
`f2edaf93e15886440236cbb4bcbfa3b9c71f4e19` with no remaining findings for the
server, Rust async, Rust blocking, Python sync, Python async, BFF and CLI.
All eight #573 design criteria were covered and accepted. The reviewer inspected
the complete correction delta, ran all 12 contract checks, independently checked
the pattern-only provider-ID alternatives and all 11 fixture provider-ID
occurrences, and verified these immutable inputs:

- Proposed OpenAPI SHA-256:
  `03c5796e481302429467c461f4300f0c8dfbe7681f310f8885d52cd2bc532eba`.
- Shared fixtures SHA-256:
  `4f0bf9837ba705e32891f220140e5009e103350efd7ca81e86cee1828e8bde17`.

This is user-authorized independent AI source-review evidence, not human
maintainer approval, generated-client execution or real-provider qualification.
The remaining final CI and project merge gates still apply.

## Accepted design decisions

- Runtime configure → non-activating verification → approved activation is required;
  Treetop connection metadata moves to runtime within deployment egress/trust bounds.
- Extend auth crates; application/storage still own configuration, persistence,
  approvals and sessions.
- Use the custom v1 protocol, separate user/redemption secrets, explicit browser
  approval and atomic response-loss cleanup.
- Default to existing-only provisioning. Reject/defer automatic linking and direct
  claim mappings into existing local groups.
- OAuth without verified fresh evidence cannot approve privileged operations.
  Keycloak must prove signed authentication time for the existing actor.
- Review OpenAPI/fixtures together, including candidate revisions, secret rotation,
  deployment locks and manual-membership survival.

After merge into `project/auth-support`, pin the merge commit and JSON
SHA-256 digests in #573 and Project 6, complete the eight criteria and only then
make #574 Ready. Later dependencies retain their native blockers. No stable
release or main promotion is authorized by contract acceptance.

## Hosted password implementation clarification (PR #591)

This implementation follow-up adds an optional password `interaction_mode` and
public `browser_available` flag. It also reuses #574's existing LDAP verification
response envelope (`candidate` plus `verification`) so the initiating client
receives the verification receipt required for activation alongside the identity
preview. The corresponding proposal and shared vector are updated together.
Completion retains its client-only request; approval operations stay encrypted
server-side and are bound to the initiating redemption capability.

These changes require implementation review in #591. They do not retroactively
extend the independent acceptance recorded above, qualify OIDC/OAuth providers,
or establish #575 acceptance.

Native HTML forms use `Referrer-Policy: same-origin`: browsers otherwise suppress
the POST Origin header under `no-referrer`. Redirects and token responses retain
`no-referrer`; cross-origin referrers remain suppressed and form submissions still
require an exact Origin plus cookie/CSRF proof. This browser-tested clarification
also requires review with #591.

## Expiry and cleanup clarification (PR #591)

The bounded cleanup policy does not retain permanent expiry tombstones. Valid
initiating credentials receive `410 interaction_expired` while the expired record
exists; after it is removed they receive `401 interaction_invalid`. Wrong
client/secret credentials remain `401` in either state. Both terminal responses
require a fresh interaction. The shared fixtures and HTTP regressions cover both
retained and reclaimed records. This clarification preserves the implemented
retention bound and makes the v1 contract's cleanup behavior explicit.

## Interaction implementation review and HTTPS qualification (PR #591)

Implementation review on 2026-11-12 accepts the optional password browser mode,
LDAP verification envelope, native-form referrer policy and bounded expiry
clarifications above. This is an implementation review by the coding agent under
the maintainer's close-out request; it is not an additional independent review.
The reviewed boundary retains application-specific correlation, purpose and
transaction rules while delegating cryptography, constant-time comparison,
rate limiting, cookies, URL parsing and HTML escaping to established libraries.

Fresh-database qualification found and corrected two defects: an unregistered
deployment LDAP provider hid local interaction discovery, and configuration
activation manually advanced token revisions during revocation. Discovery now
advertises only providers with usable interaction identities, keeping local login
available; deployment LDAP joins after candidate activation. Revocation relies on
the database revision trigger. The PostgreSQL fixture now installs that production
trigger, which `CREATE TABLE ... LIKE ... INCLUDING ALL` does not copy.

The review-corrected runtime at `282ce376f3d6ab92ae97b89f0e55031915730e99`
was exercised through a loopback TLS reverse proxy with normal
certificate validation in Chromium 157 and Safari 27.2. Authentication and BFF
used different registrable domains. `allow_loopback_http` was disabled. A
short-lived, hostname-constrained test CA was local environment setup only;
certificates, keys and trust installation are not repository inputs. Chromium
rejected the untrusted certificate before the positive run. Both browsers then
accepted the trusted endpoint without an interstitial bypass or validation flag.
After qualification, the temporary certificate was removed from the login
keychain, no user trust settings remained, and operating-system certificate
validation again rejected the leaf certificate. Temporary keys were deleted.

| Browser check | Chromium | Safari |
| --- | --- | --- |
| Public discovery, legacy discovery and protected JSON transport | 13/13 checks passed | 13/13 checks passed |
| Local login, explicit approval and server-only session completion | Passed | Passed |
| Verified LDAP login and cross-site HTTPS BFF 303 handoff | Passed | Passed |
| Candidate verification without a session or provisioning | Passed | Passed |
| Operation-bound credential approval and candidate activation | Passed | Passed |
| Explicit denial and failed-password termination | Passed | Passed |
| Client cancellation prevents browser continuation and issuance | Passed | Passed |
| Real elapsed expiry followed by bounded cleanup rejects continuation | Passed | Passed |
| Wrong-browser access to a live bound interaction | Rejected | Rejected |
| Pending/approval reload preserves the request | Passed | Passed |
| Secure cookies sent on HTTPS and withheld on plaintext | Passed | Passed |
| HttpOnly cookies absent from JavaScript | Passed | Passed |

The BFF fixture binds the callback nonce to its own Secure/HttpOnly host cookie
and consumes that session once. Chromium callback replay was rejected. Repeating
server completion returned `409 interaction_consumed`; exact-family cleanup
returned `204` twice. Denial returned `403 access_denied`, cancellation
`403 interaction_cancelled`, and failed authentication `409 interaction_failed`.
Expired and reclaimed records returned `401 interaction_invalid`; focused HTTP
regressions also cover retained expiry (`410`). The run produced exactly four
browser login sessions and two principals. Verification/approval added neither
sessions nor principals. Three browser sessions were revoked by repeatable
exact-family cleanup. All 143 proxied responses passed the
redemption-capability leakage check; response headers and cookie attributes were
recorded without cookie values. Both browsers rejected BFF callback replay.
Fresh expiry cases were created after the final provider activation, attached
to their respective browsers and left for their actual five-minute lifetime
without further configuration changes. Server completion and browser continuation
then rejected the expired, reclaimed records.

Reproduction uses the standard-library fixture described in
[the interaction guide](../authentication_interactions.md#reproducing-browser-qualification).
The fixture accepts certificate paths and has no trust-installation or
certificate-validation bypass. Secrets stay in its disposable private state.
Rust tests own authorization, persistence, concurrency and domain assertions.

| #575 acceptance criterion | Implementation and evidence |
| --- | --- |
| Versioned operations and password compatibility | v1 handlers, generated OpenAPI, shared contract fixtures, `tests/api_identity_suite/auth_interactions.rs` and existing login/session suites |
| Typed durable bindings and atomic completion | Domain lifecycle/persistence types; complete memory/PostgreSQL adapters; shared storage contracts and `crates/hubuum-storage-postgres/tests/auth_interactions` races/rollback/restart coverage |
| Hosted entry, separate secrets, approval and bounded polling | Real-browser matrix above; browser handler and admission tests; HTTP polling/backoff, cancellation, expiry and invalid-authority regressions |
| Callback ownership, exact handoff and origin/CSRF isolation | Browser Host/Origin/Fetch Metadata and cookie/CSRF tests; real cross-site BFF redirect; destination/client registrations; no public-ID redemption |
| Established issuance and no browser token exposure | Validated proof types, existing session service, atomic completion, sanitized browser responses and exact session/principal counts |
| Response loss and bounded cleanup | Consumed metadata, repeated-completion rejection, exact-family revocation, retention limits, scheduled/opportunistic purge and production-adapter cleanup tests |
| Races, replay, mismatch, compatibility and schema | Domain/shared/native adapter tests, browser replay and wrong-browser rejection, HTTP compatibility suite, regenerated OpenAPI and proposed-contract fixtures |

OIDC/OAuth provider callbacks and live upstream protocol behavior remain
issues #577/#578; #575 proves the shared interaction authority and handoff boundary.
Identity/freshness orchestration remains #576, with ecosystem qualification in
issue #581. Those downstream tasks are not claimed complete by this review. Final CI,
immutable candidate acceptance and integration commit identities are recorded in
PR #591, issue #575 and Project 6 when available.

### Final automated-review corrections

The subsequent automated source review identified additional edge cases. Return
URI bounds now apply after URL normalization; removed bound BFF registrations
return `configuration_changed`; OpenAPI declares anonymous login and conditional
human-bearer authentication for privileged begin requests. Interaction transitions
and cleanup use application UTC consistently with creation and proof timestamps,
with PostgreSQL regressions for a database clock 100 ms ahead or behind.
Application replica clocks still require synchronization.

Approval completion locks actor and target principals together before its actor
fence; a reciprocal-administrator concurrency regression exercises a blocked token
lock. The maintenance activity guard now survives the admitted handler, preserving
restore draining. Admission aggregates IPv6 /64 addresses with `ipnet` and reserves
a shared bounded overflow bucket without evicting active quotas. Focused tests
cover normalization round trips, removed registrations, subnet/overflow behavior
and maintenance activity across a handler suspension.

Two existing bounds are retained and documented explicitly. Interaction JSON is
limited to 64 KiB, its sealed operation to 128 KiB and its persisted document to
256 KiB; larger credential imports use the direct credential-approval API. Hosted
candidate verification completes the final pending LDAP identity check; candidates
with several enabled LDAP providers use the direct candidate verification endpoint
for the other providers first. Increasing only a JSON extractor limit or treating
partial verification as a fully verified candidate would violate those bounds.
The browser matrix above was rerun after these corrections. Final automated
regression and CI evidence is pinned in the PR ledger. CI subsequently exposed
Cargo 1.100's changed intermediate build layout: template tests could no longer
find their worker by walking from the test executable. The test runners now pass
Cargo's reported worker path using its standard `CARGO_BIN_EXE_*` interface;
packaged applications retain the adjacent-worker convention.
