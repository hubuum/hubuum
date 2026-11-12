# External authentication contract review

This record separates author compatibility review from independent source-review
acceptance for [#573](https://github.com/hubuum/hubuum/issues/573). The [contract](contract.md),
[OpenAPI proposal](openapi.proposed.json) and [47 shared vectors](fixtures.json)
form one review unit.

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
