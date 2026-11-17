# Cross-native authentication qualification

Project 6 uses `project/auth-support` in the server, Rust/Python SDK, CLI and
frontend repositories. All six strict native implementation prerequisites are
completed with accepted project artifacts. The final #581 security and real
GitHub matrix remains in progress; #582 and main/stable promotion remain gated.

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
results do not qualify real GitHub. GitHub requires an authorized disposable
OAuth App, protected local client-secret configuration, its exact callback and
actual native sign-ins with explicit Hubuum approval. The former disposable app
was deleted; a new setup/sign-in handoff is pending. No repository or email scope
is required for the public-identity profile.

The [security candidate guide](../security_candidates.md) defines deployment
locks, private credentials, exact revision/receipt fences and offline recovery.
Local accounts stay configured. A remote Treetop outage fails privileged access
closed and can also block new-login audit snapshots; use the documented offline
recovery procedure or reconnect the policy service. The existing group read API
does not enumerate independent per-membership source history. Upstream GitHub
token invalidation must be independently probed before such a guarantee is made.

Source/candidate acceptance is implementing-agent acceptance unless an independent
review is explicitly recorded. Keep full milestone criteria open until all
required native/security/provider evidence is accepted. Current qualification
reports and unresolved limitations belong to #581; this manifest alone does not
establish acceptance or authorize main/stable promotion.
