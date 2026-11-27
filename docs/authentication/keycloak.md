# Keycloak OIDC authentication

Hubuum uses a confidential OIDC client with authorization code, S256 PKCE and
query callbacks. Browser login, local CLI polling and remote-browser CLI polling
share the server-owned interaction protocol. Upstream tokens never reach a CLI,
BFF, JavaScript, URL or portable backup. The initial qualification target is
Keycloak; Google and Apple remain separate compatibility work.

## Realm and client setup

Use a dedicated Keycloak realm and an exact public HTTPS realm issuer, such as
`https://identity.example/realms/company`. Configure a confidential client with
client authentication enabled, standard authorization-code flow enabled, and
S256 required. Disable implicit flow and direct-access grants. Hubuum initially
supports `client_secret_basic`; accept RS256 or ES256 explicitly in its provider
definition and configure a corresponding realm signing key.

Register exactly one callback for the server-issued provider UUID:
`https://hubuum.example/api/v1/auth/callbacks/PROVIDER_UUID`. Do not use wildcard
redirects. Set `HUBUUM_AUTH_BROWSER` with the matching public HTTPS origin and
any exact BFF return registrations. Those BFF callbacks are separate from the
OIDC provider callback. Register the Keycloak callback after creating the
candidate and before its test sign-in.

Supply the client secret at runtime with
`"credential": {"action": "replace", "value": "CLIENT_SECRET"}` in the candidate's
provider configuration. It is encrypted before persistence; no provider-specific
environment variable or restart is needed. A credential `reference` resolving
`HUBUUM_OIDC_SECRET_COMPANY` or the `oidc/` mounted-secret namespace remains an
optional deployment-managed alternative. Retained protocol state and runtime
replacement credentials require the deployment-owned
`HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF` and its versioned secret source.
Never place secrets in provider URLs or commit real values to examples.

For a private identity service, explicitly enable
`HUBUUM_AUTH_RUNTIME_ALLOW_PRIVATE_TARGETS=true`. This permits private addresses;
TLS certificates and hostnames are still verified. Discovery, JWKS, token,
UserInfo and revocation requests use the hardened transport without redirects,
proxy environment variables or automatic retries. Discovery validates every
advertised destination. Each response is limited to 128 KiB, JWKS to 32 keys,
requests to five seconds, discovery to fifteen seconds and an exchange to
25 seconds. At most 32 transport requests run concurrently per process. Keys
are fetched for every operation so rotations require no cache invalidation.

## Identity and groups

The immutable identity is the provider instance, scope and signed `sub` under
the exact realm issuer. Changing a username or email never changes that identity;
a matching local username or email never links accounts or grants administrator
status. Keep the client audience exclusive to this client. Hubuum validates the
signature, configured algorithm, exact issuer/audience, optional `azp`, expiry,
issue time, nonce and access-token hash when supplied.

Configure a **Group Membership** protocol mapper with token claim name `groups`,
**Full group path** enabled, and **Add to ID token** enabled. Request `openid`
and optionally `profile` and `email`. Realm roles and client roles are separate
claims and do not substitute for group paths. Example non-secret definition:

```json
{
  "issuer": "https://identity.example/realms/company",
  "client_id": "hubuum",
  "client_authentication": "client_secret_basic",
  "scopes": ["openid", "profile", "email"],
  "signing_algorithms": ["RS256"],
  "retain_refresh_tokens": true,
  "membership": {
    "mode": "provider_groups",
    "refresh_interval_seconds": 300,
    "max_stale_seconds": 900,
    "claim_source": "id_token",
    "claim_pointer": "/groups",
    "key_mode": "full_path"
  },
  "admission": {"mode": "provision_without_grants"}
}
```

For `claim_source: "userinfo"`, enable the mapper's UserInfo output as well.
Hubuum requests UserInfo only for that profile and requires its `sub` to match
the validated ID token. UserInfo must return one `application/json` Content-Type
header. Hubuum rejects duplicate fields in discovery documents and signing-key
sets before using the endpoints or keys. Missing, malformed, partial or
distributed group claims fail closed. An explicit empty group array removes provider-owned memberships;
manual local assignments remain. Choose an admission policy deliberately;
`provision_without_grants` creates the identity without granting local access.

Keycloak 26.8.2's stock mapper omits `groups` when the user has no groups; it does
not emit an empty array. Hubuum rejects that missing claim. For users who should
remain eligible after their last access group is removed, assign a baseline
realm group such as `/authenticated` with **no Hubuum permissions**, or deploy a
reviewed mapper that emits an explicit empty array. The qualification fixture
checks both access-group removal while the baseline remains and rejection after
the final group disappears. See Keycloak's
[claim mapping implementation](https://github.com/keycloak/keycloak/blob/26.8.2/services/src/main/java/org/keycloak/protocol/oidc/mappers/OIDCAttributeMapperHelper.java).

## Configure, test and activate

Use the [security candidate workflow](../security_candidates.md). Candidate
creation checks discovery and destinations. Start a `provider_verification`
interaction with the candidate ID and selected provider ID, complete its real
Keycloak sign-in and confirm the identity shown in the hosted browser. Complete
the interaction in the initiating client to review the admission decision and
mapped groups. Verification creates no principal, membership or session.
Activation still requires the existing local
recovery administrator and a fresh operation-bound approval.

Live sign-in is selected through method discovery and the
[interaction API](../authentication_interactions.md). The initiating client keeps
its redemption secret, and the browser attaches using the displayed user code.
An independent cookie and CSRF token bind the hosted page. The callback claims
its encrypted state once before code exchange; wrong state, issuer, provider,
cookie, configuration revision or a replay cannot complete it. Successful IdP
sign-in still requires explicit Hubuum approval of the requesting client.

## Freshness, refresh and logout

`retain_refresh_tokens` is opt-in. Without it, interactive authentication is
required when the external authority snapshot becomes due. With it, a due API
request can renew the same immutable subject and synchronize complete current
memberships. This also applies to external-human PATs; rotating a Hubuum token
alone never extends upstream authority. Default refresh and maximum-stale
intervals are 300 and 900 seconds.

Refresh tokens and callback secrets are encrypted at rest with the versioned
runtime secret key and associated provider, revision, originating interaction
and immutable subject. Each authenticated interaction issues at most one
session, and the retained record is attached atomically to that principal and
session. Concurrent callers claim one token; the old encrypted credential is
removed before exchange and a validated replacement commits with membership
updates. If the verified refresh response omits a replacement, Hubuum retains
the original refresh token for the next renewal. Ambiguous failures never retry
the old token. Temporary failures may
use the last successful snapshot only within its configured maximum-stale
bound. Invalid grants, subject changes and definitive rejection invalidate
provider-derived authority. Disabled providers and changed configuration fence
in-flight work immediately.

Local logout commits first and destroys the retained credential. Supported
OIDC revocation is attempted afterward with a bounded deadline and cannot delay
or undo local revocation. Local logout does not promise global Keycloak SSO
logout. RP-initiated browser logout is not exposed by this profile. The current
logout API performs local logout and best-effort token revocation.

An unsuccessful or superseded renewal discards any returned grant locally.
Keycloak can share one upstream client session across Hubuum logins, including
logins under different provider scopes. Revoking a discarded refresh token
could end that session after a new login has completed. Only an explicit
revocation operation sends credentials it captured to the provider. An invalid
ID token or a contradictory grant response denies renewal and invalidates cached
authority. A valid grant requires a single `application/json` Content-Type.
Missing, ambiguous, or incorrect media types follow the temporary outage policy.
Hubuum counts time spent acknowledging the database claim against the renewal
deadline. It checks the deadline and task cancellation again after DNS
screening, immediately before sending the grant. An exchange that already
submitted its grant finishes under its existing timeout. Restore maintenance waits for active renewal workers even
if their callers disconnect.

Credential approval uses `max_age=0` and a signed `auth_time` at or after the
interaction start, with one second of clock skew. Existing SSO, a new token
`iat`, consent and Hubuum refresh do not establish freshness. When retention is
enabled, a successful fresh authentication replaces the credential on an
existing eligible session of the same immutable provider identity. This happens
before the separate approval decision: denying the operation does not discard
the new grant and strand the ordinary session on an older authentication time.
The replacement fences an in-flight renewal of the old grant, creates no new
Hubuum session, and preserves strict issuer, subject, nonce and authentication
time checks on subsequent refreshes. Configure MFA in
Keycloak's browser authentication flow. This initial profile does not infer
assurance from unspecified `acr` or `amr` values.

## Upgrade and recovery

This increment is an offline upgrade. Stop all API and worker replicas, back up
the database, apply migrations through
`2026-11-23-000001_verification_group_budget` and role grants,
then start matching binaries everywhere and begin new interactions. Upgrade
external storage adapters and all ten SDK dependencies to `0.11.0`. Regenerate
OpenAPI clients for the new browser routes and OIDC option. Older binaries and
recovery tools cannot enforce this credential lifecycle; restore the pre-upgrade
database snapshot and matching configuration before downgrading.

Logical exports omit upstream credentials and protocol state. Logical restore
and security recovery destroy them. Retain access to versioned encryption keys
for active sessions, using a new immutable alias when rotating the wrapping key;
unavailable keys require fresh authentication after the bounded existing snapshot
expires. Removing a provider, deleting its user or
revoking its session makes retained credentials unusable.

## Reproducing provider qualification

The standard-library driver `tests/python/integration/keycloak.py` uses Keycloak
26.8.2 pinned to image digest
`sha256:c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0`.
It provisions a disposable realm, confidential S256 client, full-path group mapper,
password and TOTP users, verified TLS and isolated PostgreSQL. Published fixture
ports bind only host loopback. It cleans up its containers, volumes, network and
temporary secrets when finished; it requires Docker, OpenSSL and Python 3.11+.

Build the production image with both TLS implementations, then run both claim
source profiles from the repository root:

```sh
docker build --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' --tag hubuum-server:verify .
python3 -I -S tests/python/integration/keycloak.py --image hubuum-server:verify --group-source id_token --evidence target/keycloak-evidence/id-token
python3 -I -S tests/python/integration/keycloak.py --image hubuum-server:verify --group-source userinfo --evidence target/keycloak-evidence/userinfo
```

The report records the exact server image ID, pinned provider/database images,
source commit and dirty state, fixture SHA-256, claim source and individual
assertions. CI retains these files as the `keycloak-qualification` artifact.
Only a successful report for the exact tested server/source establishes
qualification; fixture availability alone does not. Passwords, cookies, tokens,
callback URLs and provider response bodies never enter these evidence files.

## Qualification fixture

The pinned fixture uses `quay.io/keycloak/keycloak:26.8.2` at manifest digest
`sha256:c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0`.
It uses disposable identities and certificates. Qualification results and the
immutable tested server image are recorded with the implementing pull request;
this setup document alone is not evidence that a run passed.
