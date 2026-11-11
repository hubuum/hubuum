# External authentication contract review

This is the author compatibility review for [#573](https://github.com/hubuum/hubuum/issues/573),
not an independent approval or provider qualification. The [contract](contract.md),
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
the source review does not qualify a provider or establish independent acceptance.

## Acceptance review requested

Independent acceptance is **pending**. Review the PR as one shared contract for
server, BFF, Rust async/blocking, Python sync/async and CLI, recording approval or
requested revisions for each surface; one maintainer may cover several surfaces.
This author review and green tooling do not establish independent acceptance.
No real provider credentials are needed to review the inert fixtures.

Confirm these decisions before acceptance:

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

After review and merge into `project/auth-support`, pin the merge commit and JSON
SHA-256 digests in #573 and Project 6, complete the eight criteria and only then
make #574 Ready. Later dependencies retain their native blockers. No stable
release or main promotion is authorized by contract acceptance.
