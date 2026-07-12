# Structured Logging

Hubuum writes newline-delimited JSON logs only. JSON is the stable operational interface for containers, collectors, and command-line tooling; there is no text formatter toggle. Configure verbosity with `HUBUUM_LOG_LEVEL`.

## Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `HUBUUM_LOG_LEVEL` | `info` | Minimum log level: `trace`, `debug`, `info`, `warn`, or `error` |

## Common Fields

Every log record includes:

| Field | Description |
| ----- | ----------- |
| `time` | UTC timestamp in RFC 3339 format with millisecond precision |
| `severity` | Log level as `TRACE`, `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `message` | Stable event message |

Request-scoped records also include `request_id` and, when accepted from the client, `correlation_id`. Authenticated requests record `principal_id` on the request span after bearer token resolution.

The server emits one `server startup` record at `INFO` after binding succeeds. It includes the package version, build Git SHA, bind address, TLS state, worker counts, database and authorization backends, log format and level, and the number of enabled event sinks. Release and container builds populate `git_sha`; local builds report `unknown` unless `HUBUUM_BUILD_GIT_SHA` is set while compiling.

## Request Logs

Request completion is the canonical HTTP request log event. Actix's default text request logger is disabled to avoid duplicate unstructured logs.

Completion records include:

| Field | Description |
| ----- | ----------- |
| `message` | `request complete` |
| `method` | HTTP method |
| `path` | Request path without query string |
| `status` | HTTP response status |
| `client_ip` | Resolved client IP when available |
| `elapsed_ms` | Request duration in whole milliseconds |
| `error` | Present when a downstream service error ended the request |

Severity is derived from the outcome:

| Outcome | Severity |
| ------- | -------- |
| `2xx` and `3xx` | `INFO` |
| `4xx` | `WARN` |
| `5xx` | `ERROR` |

Hubuum applies the status-to-severity mapping to downstream service errors as well as normal responses. Early middleware rejections, including client-allowlist denials, are returned as responses so they receive the same completion event and `x-request-id` header as handler responses.

Clients may send `X-Correlation-ID`. Accepted values are 1 to 128 visible ASCII bytes without whitespace. Hubuum echoes accepted values as `x-correlation-id`; invalid values are ignored without logging or echoing the supplied value. Hubuum always returns `x-request-id`.

## Operation And Authorization Logs

Domain mutation logs are queued by the audit event writer and emitted at `INFO` only after the surrounding database transaction commits. Failed and rolled-back transactions discard their queued mutation logs. These logs use the audit catalog labels:

| Field | Description |
| ----- | ----------- |
| `operation` | `mutation_committed` |
| `mutation_phase` | `committed` |
| `entity_type` | Catalog entity label, such as `collection` |
| `action` | Catalog action label, such as `created` |
| `entity_id` | Entity identifier when available |
| `actor_principal_id` | Acting principal when available |

Service list/get paths log at `DEBUG`. The audit-event query path additionally uses the standardized `operation=read` helper with optional catalog entity, action, and entity ID filters.

Authorization decision logs use:

| Decision | Severity |
| -------- | -------- |
| Grant | `DEBUG` |
| Denial | `WARN` |

Authorization records include `event_type=authorization`, `decision`, `principal_id`, requested `permissions` as a JSON array, derived `action` and `entity_type` when the requested permissions share them, nullable `collection_id` and `collection_count`, and a short `reason`.

## Sensitive Data Rules

Do not log secrets or high-volume payloads. In particular, logs must not include bearer tokens or token fragments, token hashes, password hashes, sink secrets, raw `Authorization` headers, request bodies, response bodies, audit `before` or `after` snapshots, or remote target secret material.

Prefer stable identifiers, catalog labels, counts, and short reason strings over raw payload data.

## jq Recipes

Pretty-print logs:

```bash
jq . hubuum.log
```

Show failed requests:

```bash
jq 'select(.message == "request complete" and (.status >= 500 or .severity == "ERROR"))' hubuum.log
```

Trace one request:

```bash
jq 'select(.request_id == "REPLACE-WITH-REQUEST-ID")' hubuum.log
```

Trace a client-supplied correlation ID:

```bash
jq 'select(.correlation_id == "REPLACE-WITH-CORRELATION-ID")' hubuum.log
```

List authorization denials:

```bash
jq 'select(.event_type == "authorization" and .decision == "deny")' hubuum.log
```
