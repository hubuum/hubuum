# Secret Sources

Hubuum resolves credential material through one process-wide secret source. API
records and authentication configuration contain validated aliases only; they
cannot select an environment variable, provider, or filesystem path.

## Environment Source

The default source is `environment`. Existing deployments remain compatible:

| Consumer | Environment mapping |
| --- | --- |
| PostgreSQL | `HUBUUM_DATABASE_URL` |
| Token hashing | `HUBUUM_TOKEN_HASH_KEY` |
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
    └── key
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

The PostgreSQL URL and token-hash key are startup secrets. Rotate them by
updating every replica's source and restarting the replicas according to the
database or token-key migration procedure. Hubuum does not claim live rotation
for an established database pool or for token hashing. Token key-ring rotation
is tracked separately from this source abstraction.

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
