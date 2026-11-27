# OAuth provider setup and qualification

OAuth login uses a server-owned authorization-code exchange with S256 PKCE and
one configured, bearer-authenticated GET JSON user endpoint. Upgrade all API and
worker replicas before enabling OAuth or adding profile headers. It shares OIDC's
browser cookie/CSRF, interaction claim, revision, admission, session and membership
fences. Neither caller-supplied tokens nor caller-supplied user documents are
accepted as login. Web/BFF and local/remote CLI flows use the same interaction.

## GitHub.com

Register a **GitHub OAuth App** for the Hubuum deployment. GitHub Apps and GitHub
Enterprise Server are separate profiles and are not qualified by this adapter.
Set the app callback to `https://AUTH_ORIGIN/api/v1/auth/callbacks/PROVIDER_UUID`.
When first registering an app, use a temporary callback on that origin, then
replace it with the server-issued provider UUID before candidate test sign-in.
Set the client secret through the write-only runtime `replace` action. Hubuum
encrypts it using the deployment's runtime-settings key; no provider-specific
environment variable or restart is needed. Ordinary login clients never receive
it. A deployment-managed alias remains an optional alternative; see
[secret sources](../secret_sources.md).

Create an enabled runtime authentication candidate with the following provider.
An existing provider requires its immutable `provider_id`; omit that field for
the initial candidate. Preserve the local recovery administrator.

```json
{
  "group": "authentication",
  "expected_revision": 1,
  "providers": [{
    "name": "github",
    "kind": "oauth",
    "source": "runtime",
    "enabled": true,
    "configuration": {
      "profile": "github",
      "authorization_endpoint": "https://github.com/login/oauth/authorize",
      "token_endpoint": "https://github.com/login/oauth/access_token",
      "profile_endpoint": "https://api.github.com/user",
      "client_id": "YOUR_OAUTH_APP_CLIENT_ID",
      "client_authentication": "client_secret_post",
      "scopes": [],
      "subject_pointer": "/id",
      "subject_type": "integer",
      "name_pointer": "/login",
      "membership": {
        "mode": "local_only",
        "refresh_interval_seconds": 300,
        "max_stale_seconds": 900
      },
      "admission": {"mode": "existing_only"},
      "credential": {"action": "replace", "value": "YOUR_OAUTH_APP_CLIENT_SECRET"}
    }
  }]
}
```

No scope is required for the authenticated public `/user` response. Do not request
repository, organization or email permissions merely to sign in. An absent or
private email is accepted and never links accounts. The positive integer `id` is
preserved exactly, including IDs above JavaScript's safe integer range; floats,
exponents, strings and out-of-range numbers are rejected rather than rounded.
Usernames can change without changing identity. `site_admin`, organizations and
teams never grant Hubuum authority. This profile uses only local memberships.

Pinned GitHub endpoints, subject semantics, local memberships and standard request
headers cannot be overridden in the built-in profile. GitHub API requests use
version `2026-03-10`. Enterprise deployments need separate explicit qualification;
changing these pinned endpoints is rejected.

GitHub callbacks must include the exact authorization-server issuer
`iss=https://github.com/login/oauth`. Missing or different issuers are rejected
before code exchange, including alternate spellings, trailing slashes, queries
and fragments. The configured profile and encrypted pending state also bind the
callback and client definition. Generic JSON profiles have no issuer contract
and reject an `iss` parameter; providers requiring issuer support need an explicit
profile. See [GitHub's issuer](https://docs.github.com/en/apps/github-authentication-discovery-endpoints#issuer)
and [RFC 9207](https://www.rfc-editor.org/rfc/rfc9207.html#section-2.4).

The local administrator starts a `provider_verification` interaction against the
candidate and completes a real GitHub sign-in. The preview does not provision a
user or issue a session. Activate only after that preview succeeds, using its
verification receipt and the administrator's fresh operation-bound approval.
For initial provisioning, explicitly choose `provision_without_grants` or a stable
subject allowlist according to the [shared contract](contract.md). Existing-only
admission requires an already provisioned matching external identity.

## Bounded JSON user profiles

The `json_user` profile supports a different authorization endpoint, token endpoint
and profile endpoint; `client_secret_basic` or `client_secret_post`; configured
scope tokens; JSON Pointers for a string or integer subject and optional display
name; and existing local-only or explicit profile-group membership policies.
It requires S256 PKCE and query callbacks. It does not infer identity from OAuth
or follow provider links, pagination, distributed claims or additional endpoints.
Providers requiring those features need a small explicit adapter.

Successful token and user-profile responses require one `application/json`
Content-Type header. Parameters such as `charset=utf-8` are accepted. A token
response containing an `error` member stops the exchange before the profile lookup.
Hubuum rejects a response that contains both an error and a credential on either
a success or an error HTTP status.

Optional `profile_headers` permits at most eight bounded lowercase `accept`,
`user-agent` or `x-*` headers. These headers select the identity namespace. Changing a
header requires a new scope. Authentication, host, cookies, framing and
`x-forwarded-*` headers are rejected. All endpoints require HTTPS without
userinfo, query or fragments. Shared transport screens DNS on every request,
pins resolved addresses, refuses redirects and proxies, validates certificates,
limits responses to 128 KiB and bounds concurrency and deadlines. Only deployment
policy may permit private destinations; administrators cannot disable TLS checks.

## Credentials, freshness and logout

The server encrypts retained access tokens with interaction/provider/revision and
subject-bound associated data. At the configured eligibility interval, it rereads
the pinned user endpoint and requires the same subject and complete configured
membership claims. An upstream rejection invalidates authority; a temporary
failure permits only the existing bounded stale window. Local session rotation
does not extend upstream identity evidence.

GitHub primary and secondary rate limits are temporary failures, including HTTP
403 responses identified by rate-limit metadata or the secondary-limit error
message. For the GitHub user endpoint, an explicit rate-limit header on a 403
is sufficient even when the error body is unreadable. If a generic provider
drops the connection partway through a 403 response, Hubuum records an outage.
After a temporary profile-read failure, Hubuum retains
the encrypted access token and waits 30 seconds before retrying that session. The failed read
does not extend the cached membership lifetime.

This profile does **not** perform refresh-token grants. GitHub OAuth Apps may
issue expiring tokens and refresh tokens; the adapter honors access-token expiry
and retires the expired grant. A profile HTTP 401 during renewal also retires
that bearer as soon as the response headers arrive, even if the body is incomplete.
A due eligibility check tries another retained
grant for the same principal when available. One request checks at most eight
grants with a shared 25-second deadline. If the budget runs out, Hubuum returns
HTTP 503. The bearer becomes eligible for retry as soon as the worker releases
its claim. Exhausting this local budget preserves the verification timestamps
and does not grant access under the cached-membership outage policy. The worker
finishes recording any provider response it already received before releasing
its restore guard. Once all access tokens have expired, the user must sign in
again. Existing Hubuum sessions and personal
access tokens remain subject to the same identity freshness check. The adapter
does not retain a returned refresh token.
Do not request `offline_access` expecting automatic renewal.

Local logout commits before upstream work. For GitHub, a bounded best-effort
`DELETE /applications/{client_id}/token` attempt revokes the retained access token;
it does not delete the entire app authorization. Provider failure cannot restore
the local session or delay its response. Generic JSON profiles have no configured
upstream revocation operation: local logout destroys the retained credential, and
users can revoke the app at their provider. Configuration changes retain the
existing session/credential and in-flight interaction fences.

Discovery reports `fresh_authentication: false` for both profiles. Credential
approval returns HTTP 422 with `capability_unsupported`: consent, account picking
and profile retrieval do not prove when authentication occurred. Use an authorized
local administrator's own approval for privileged administration.

## Qualification

Rust tests own protocol/identity validation and shared storage/API contracts.
The synthetic deployment fixture runs without any personal GitHub credentials:

```bash
docker build --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' --tag hubuum-server:verify .
python3 -I -S tests/python/run.py integration oauth --image hubuum-server:verify --evidence target/oauth-evidence/synthetic
```

It provisions a disposable PostgreSQL instance, HTTPS Hubuum server and second
OAuth provider with nested string subjects, Basic authentication, a custom header
and profile memberships. Docker, OpenSSL and Python 3.11+ are required; no Python
packages or personal secrets are required. Ports default to 48479/48480. Containers,
temporary certificates and secrets are removed at exit. Do not expose fixture
listeners beyond the test host. On Linux the server uses the Docker host gateway;
the synthetic provider must be reachable from that gateway.

Full GitHub compatibility additionally requires an authorized real login:

```bash
python3 -I -S tests/python/run.py integration oauth --image hubuum-server:verify --evidence target/oauth-evidence/github --github-client YOUR_CLIENT_ID --github-secret-file /private/path/to/client-secret
```

Use a disposable OAuth App and a mode-0600 secret file. The harness prints the
exact callback for app registration, then prompts for browser sign-in and explicit
Hubuum approval for candidate verification and poll/BFF sessions. Its temporary
localhost certificate may need a browser exception confined to this disposable
test. Delete the app and secret file after qualification. Do not add personal
credentials to CI or commit them. Reports retain image/source identities and
sanitized case results without tokens, callback URLs or user profile data.

The 21 November 2026 qualification verified real GitHub.com sign-ins through the
CLI and frontend. The CLI reused its saved session and the frontend retained its
session after reload. The [qualification record](ecosystem_qualification.md)
links the accepted artifacts, tested provider capabilities and promotion status.

Protocol references: [GitHub OAuth authorization and PKCE](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps),
[authenticated user](https://docs.github.com/en/rest/users/users#get-the-authenticated-user),
and [token revocation](https://docs.github.com/en/rest/apps/oauth-applications#delete-an-app-token).
