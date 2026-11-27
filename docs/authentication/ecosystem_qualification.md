# Cross-native authentication qualification

On 21 November 2026, Project 6 accepted the Keycloak and GitHub qualification
on `project/auth-support`. The automated matrix exercised all four native
products. A separate live run verified GitHub.com sign-in with the production
CLI and frontend. The [acceptance review][accepted] records the six completed
native prerequisites. The [ecosystem guide][guide] lists the tested versions and
upgrade order. Stable delivery requires the [reviewed promotion][promotion].

The [source manifest](ecosystem-source.json) pins accepted server #599 and each
accepted native source/tree, owning driver, consumer executable/script and wire
contract. `tests/python/integration/ecosystem.py` runs the unchanged owning
consumer against that explicit server target. It verifies clean checkouts,
source/tree and input hashes, resolves the selected image to an accepted immutable
identity, and requires the owning consumer's OpenAPI/corpus hashes to match.
Fixture imports come from the verified server checkout even when the shared
Python runner has already imported its own integration package.

This changes fixture selection only. The original client fixture pin remains
recorded alongside the actual server target. The owning native consumer keeps
its configuration, preview, activation, login/renewal, approval and logout
assertions. Rust and CLI executables are frozen in a private temporary directory
for each run. Their snapshots and all fixture credentials are removed afterward.

## Running the explicit matrix

Provide clean accepted checkouts and the immutable inputs in the manifest. Keep
existing qualified production standalone output in the frontend checkout. The
Rust consumer is built from accepted SDK source with Rust 1.100.0:

```sh
cargo +1.100.0 build --locked -p e2e_client --bin authentication-project \
  --features project-authentication-tests
```

Run a native profile from the server checkout, for example:

```sh
python3 -B tests/python/integration/ecosystem.py \
  --client rust --profile id_token \
  --client-source /path/to/accepted-rust-sdk \
  --server-source /path/to/accepted-server-source \
  --image sha256:qualified-server-image \
  --consumer /path/to/authentication-project \
  --evidence /path/to/private-evidence/rust-id-token
```

The example image and paths are placeholders; use the exact manifest identities.
A new build can produce a different executable digest. Record its clean source,
locked command/toolchain and new digest, then qualify that explicit build; do not
silently reuse a shared cache executable or label it as an older binary.

| Client | Profiles | Consumer input |
| --- | --- | --- |
| Rust | `id_token`, `userinfo`, `oauth` | Pinned `authentication-project` executable; async and blocking run together |
| Python | `id_token`, `userinfo`, `oauth` | Pinned `authentication_consumer.py`; sync and async run together |
| CLI | `ldap`, `id_token`, `userinfo`, `oauth` | Pinned release CLI executable |
| Frontend | `local`, `ldap`, `id_token`, `userinfo`, `oauth` | Pinned Node.js runtime; actual administration and login/approval UI |
| Frontend authorization | `treetop` | Same Node.js runtime plus the separately pinned `permissions-treetop` server image |

For Python SDK qualification, invoke the coordinator with the already-qualified
SDK consumer's Python runtime so its child can import that SDK. The coordinator
itself imports only the standard library; no server-tool dependency installation
or new virtual environment is required. Other clients use the ordinary supported
`python3`. Fixture ports are fixed, so run profiles serially.

The coordinator writes `ecosystem-qualification.json` beside the owning consumer
report. It records actual source/image/consumer/controller/manifest identities,
the original fixture source, unchanged wire hashes, Python runtime and successful
check count. A consumer failure records only its exception type in this summary;
credential-bearing exception text is not propagated. Never retain private fixture
inputs, callback query strings, browser profiles or secret-bearing traces.

## Acceptance boundaries

Synthetic OAuth and real disposable Keycloak are automated fixtures. Their
results cover the automated profiles below. The live GitHub run used a disposable
OAuth App with public-data-only consent. Candidate preview issued no session.
A fresh local administrator approval activated that candidate. The CLI completed
the remote-browser handoff and reused its saved session for a second `whoami`.
The frontend returned to the authenticated workspace and retained its session
after reload. The server recorded both sessions for the same external principal.
The disposable app and test credentials were removed after the run.

The [live evidence manifest](native-github-evidence.json) pins the CLI executable,
the server and frontend images, the PostgreSQL and Valkey fixtures, the workflow
runs and artifact digests. Its source and image pins apply to these two sign-ins.
The automated matrix retains the inputs used for its own runs.

The manifest abbreviates the Valkey repository as `valkey`. To reproduce the
live run, pull the same digest from the `valkey/valkey` repository used by the
frontend fixture.

```sh
docker pull docker.io/valkey/valkey:9.1.2-alpine3.24@sha256:48332870af354a799964c0012ae1194a0bf2bf894eb508f945810596dc2d8d11
```

The [security candidate guide](../security_candidates.md) defines deployment
locks, private credentials, exact revision/receipt fences and offline recovery.
Local accounts stay configured. A remote Treetop outage fails privileged access
closed and can also block new-login audit snapshots; use the documented offline
recovery procedure or reconnect the policy service. The existing group read API
does not enumerate independent per-membership source history. Upstream GitHub
token invalidation must be independently probed before such a guarantee is made.

The [qualification review][accepted] records project-candidate acceptance and
provider limits. Each participating repository requires a reviewed integration PR
for promotion. Direct Google and Apple profiles remain follow-up work.

## Recorded automated matrix

The [sanitized result ledger](ecosystem-qualification.json) records **255 passing
checks across 16 client/profile runs**, with zero failures. Qualified coordinator
source is `93aac99c656d29187b6494d3386447a03f246591`; accepted server source is
`8e6e8344c458765af556890572593775ca4fd9a0`. Each record pins the owning source/tree,
consumer, controller, manifest, actual server image and original fixture target.
Fixture overrides preserve the unchanged OpenAPI and 48-case corpus hashes.

| Native product | Passing checks | Modes/profiles |
| --- | --- | --- |
| Rust SDK | 60 | Async/blocking; both Keycloak profiles and synthetic OAuth |
| Python SDK | 66 | Sync/async; both Keycloak profiles and synthetic OAuth |
| Release CLI | 54 | LDAPS, both Keycloak profiles, synthetic OAuth; local/remote terminal behavior |
| Frontend | 75 | Actual local/LDAPS/OAuth/Keycloak administration and login UI; real Treetop operations |

The full required Rust suite on this coordinator source passed **7,030 tests,
zero failures** (41 existing ignores), and the complete standard-library Python
suite passed **344 tests**. Strict all-target Clippy, rustfmt, unchanged regenerated
OpenAPI, CI classification, Markdown and strict documentation rendering pass.
The [coordinator PR](https://github.com/hubuum/hubuum/pull/600) records the final
CI and candidate acceptance for this matrix.

## Security evidence ownership

| Area | Evidence owner and scope |
| --- | --- |
| Source/image and wire compatibility | The source manifest, accepted cross-repository candidates and 255-check native ledger |
| Candidate receipts, revisions, locks and concurrent completion | Rust API/browser and complete storage-contract tests; native preview/activation plus accepted server #599's 20-check LDAP credential-intent matrix |
| Replay/mix-up, CSRF/open redirects, polling and response loss | Rust interaction/session/approval tests, accepted BFF #196 security/loss qualification and release CLI cancellation/terminal checks |
| Group removal, stale claims, deprovisioning and signing-key rotation | Accepted #599 real Keycloak production-image qualification: 19 checks per ID-token/UserInfo profile, plus Rust contracts and native membership checks |
| Deployment, restore and monitoring | Accepted #599's exact-image deployment/backup/restore and alert/lifecycle reports; 28 correctness and 37 benchmark jobs, 25 targets without regressions or overrides |
| Real native GitHub | [Accepted CLI and frontend sign-ins][accepted] on server #608, following the [11-check server fixture](https://github.com/hubuum/hubuum/pull/595) |

Representative Rust sources are `tests/api_identity_suite/auth_interactions.rs`,
`tests/api_identity_suite/sessions.rs`,
`tests/api_identity_suite/credential_approvals.rs`,
`src/api/v1/handlers/auth_interactions/browser/tests.rs`, and the complete storage
contracts in `src/tests/storage_contract/auth_interactions.rs` and
`src/tests/storage_contract/runtime_settings/security_candidates.rs`. Provider
integration belongs to `tests/python/integration/keycloak.py` and `oauth.py`.
These owners retain exhaustive domain/concurrency assertions; the coordinator
selects qualified fixtures and runs the owning native products.

The ledger's acceptance fields describe the automated run at the time it was
recorded. The subsequent [live sign-in review][accepted] completes #581.
The [promotion record][promotion] tracks main integration and stable artifacts.

[accepted]: https://github.com/hubuum/hubuum/issues/581#issuecomment-7063706082
[guide]: https://github.com/hubuum/hubuum.github.io/blob/f8a2fc2133a74629eb10b66619d0d9aa78446a4f/docs/external-authentication.md
[promotion]: https://github.com/hubuum/hubuum/issues/582
