# Secret Sources

Hubuum resolves credential material through one process-wide secret source. API
records and authentication configuration contain validated aliases only; they
cannot select an environment variable, provider, or filesystem path.

## Environment Source

The default source is `environment`. Existing deployments remain compatible:

| Consumer | Environment mapping |
| --- | --- |
| PostgreSQL | `HUBUUM_DATABASE_URL` |
| Token hashing (compatible single key) | `HUBUUM_TOKEN_HASH_KEY` |
| Token key-ring ID `primary` | `HUBUUM_TOKEN_HASH_KEY_PRIMARY` |
| Event sink alias `NAME` | `HUBUUM_EVENT_SINK_SECRET_NAME` |
| Remote-target alias `NAME` | `HUBUUM_REMOTE_SECRET_NAME` |
| LDAP alias `NAME` | `HUBUUM_LDAP_SECRET_NAME` |

Alias letters are uppercased and hyphens become underscores for environment
lookup. Missing `HUBUUM_SECRET_SOURCE` is equivalent to:

```text
HUBUUM_SECRET_SOURCE=environment
```

Environment values are limited to 1 MiB and preserve raw bytes on Unix.
Missing and empty values are distinct; an empty value is rejected as invalid.

## Mounted File Source

Set both variables to use a mounted secret volume:

```text
HUBUUM_SECRET_SOURCE=file
HUBUUM_SECRET_FILE_ROOT=/run/secrets/hubuum
```

The root has a fixed, application-owned layout:

```text
/run/secrets/hubuum/
├── database/
│   └── url
├── event-sink/
│   └── <alias>
├── ldap/
│   └── <alias>
├── remote/
│   └── <alias>
└── token/
    ├── key
    ├── primary
    └── previous
```

The file provider accepts binary values up to 1 MiB, opens ordinary files
only, performs a descriptor-bounded read, detects changes during the read, and
rejects traversal outside the configured root. Consumer protocols that require
text reject values that are not UTF-8.

Kubernetes projected-secret symlinks are supported explicitly. Every resolved
target and opened descriptor must remain below `HUBUUM_SECRET_FILE_ROOT`;
symlinks escaping that root are rejected. Other users of the internal file
provider default to rejecting symlinks unless they opt into the same confined
projected-volume behavior.

## Reload And Rotation

Secret resolution is single-flight and cached separately for each bounded
consumer class. Each cache holds at most 128 aliases and 128 MiB for five
minutes. Failed resolutions are not cached. The application fails closed on
provider errors and does not return expired values; the internal resolver
exposes an explicit opt-in stale policy for consumers that define a different
availability contract.

LDAP service binds, event deliveries, and remote-target calls observe a rotated
value after the cache entry expires or is explicitly invalidated. AMQP, SMTP,
and Valkey sink connection pools key clients by the resolved URI, so a rotated
credential creates a new client and old idle clients leave through the existing
bounded LRU policy.

The PostgreSQL URL and token-hash keys are startup secrets. Updating their
source does not change an established process; restart replicas according to
the database or token key-ring procedure. The ring makes rolling process
restarts safe, but does not hot-reload key material inside a process.

The login rate-limit Valkey URL, Treetop URL, TLS private-key passphrase, and
other certificate paths continue to use their existing configuration adapters
and require restart after changes. They are intentionally listed here so those
consumers are not mistaken for live-rotating integrations.

## Diagnostics

Administrator configuration reports the selected provider, whether a file root
is configured, the effective cache bounds, fail-closed stale policy, and
projected-symlink confinement without returning aliases, paths, versions, or
values. Prometheus exports `hubuum_secret_source_info`,
`hubuum_secret_resolutions_total`, and
`hubuum_secret_resolution_duration_seconds` with bounded provider, consumer,
and outcome labels. Secret values and alias names are never labels.

## Token Key-Ring Rotation

Hubuum accepts one active issuance key and at most seven previous verification
keys. Key IDs contain 1-32 lowercase ASCII letters, numbers, or interior
hyphens. Every key must contain at least 32 bytes. Startup rejects missing keys,
duplicate IDs or material, malformed IDs, short or empty material, and rings
larger than the bound. Error messages and logs never include key material.

The compatible configuration remains:

```text
HUBUUM_TOKEN_HASH_KEY=<stable-secret>
```

It is represented internally as the stable key ID `legacy`. Set
`HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true` in production to make a missing
stable key a startup error instead of creating an ephemeral process-local key.

For an environment-backed ring, key ID `old` maps to
`HUBUUM_TOKEN_HASH_KEY_OLD`; for a file-backed ring it maps to `token/old`
below `HUBUUM_SECRET_FILE_ROOT`. IDs are non-secret configuration:

```text
HUBUUM_TOKEN_HASH_ACTIVE_KEY_ID=old
HUBUUM_TOKEN_HASH_PREVIOUS_KEY_IDS=new
HUBUUM_TOKEN_HASH_KEY_OLD=<old-secret>
HUBUUM_TOKEN_HASH_KEY_NEW=<new-secret>
HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY=true
```

Use this staged multi-replica procedure:

1. Upgrade every replica while retaining the compatible
   `HUBUUM_TOKEN_HASH_KEY=<old-secret>` setting. During a rolling software
   upgrade, keep that setting even if ring variables are also staged, because
   a pre-key-ring binary only reads the compatible setting.
2. Generate the new independent key. Deploy `old` as active and `new` as a
   previous key to every replica. Do not advance until every replica's running
   configuration reports the same ring identity. The compatible old-key
   variable may be removed after no pre-key-ring binaries remain.
3. Deploy `new` as active and `old` as previous. During this configuration
   rollout, replicas still using the old-active ring can verify new tokens
   because step 2 taught them `new`, and new-active replicas can verify old
   tokens through `old`.
4. Wait for the previous-key active count to reach zero. Check
   `hubuum-admin --token-key-status` for active, revoked, and expired counts,
   latest validation, and expiry bounds. The runtime configuration exposes
   active and previous IDs plus a deterministic redacted ring identity.
   Prometheus exposes the active ID and redacted identity through
   `hubuum_token_hash_key_info`, plus `hubuum_token_hash_stored` with bounded
   key-state and lifecycle labels.
5. Remove `old` from the previous list and remove its secret, then restart all
   replicas. Confirm the final ring identity is consistent.

New bearer values have the opaque form `hbt1.<key-id>.<secret>`. Verification
uses only the embedded key ID; an unknown or malformed versioned token never
falls back across the ring. Unversioned legacy tokens are checked against the
bounded ring in one storage operation and, after a valid active authentication,
their unidentified stored digest is migrated atomically to the active key.
Revoked and expired tokens are never migrated. A versioned token issued under
a previous key keeps that key ID and ages out through expiry or revocation;
changing its stored digest would contradict the no-fallback format contract.

To roll back step 3, redeploy `old` as active with `new` previous while both
secrets are still retained. Tokens issued during the attempted rotation remain
valid. Never replace the material behind an existing ID in place: that creates
the same mixed-replica failure as the former single-key configuration.
