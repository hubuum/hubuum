# Structured Logging

Hubuum writes newline-delimited JSON logs only. There is no text formatter toggle; configure verbosity with `HUBUUM_LOG_LEVEL`.

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

Request-scoped records also include `request_id` and, when supplied by the client, `correlation_id`. Authenticated requests record `principal` on the request span after bearer token resolution.

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

Severity is derived from the outcome:

| Outcome | Severity |
| ------- | -------- |
| `2xx` and `3xx` | `INFO` |
| `4xx` | `WARN` |
| `5xx` and service errors | `ERROR` |

Clients may send `X-Correlation-ID`; Hubuum echoes it as `x-correlation-id`. Hubuum always returns `x-request-id`.

## Operation And Authorization Logs

Domain mutation-recorded logs are emitted from the audit event writer at `INFO`. They mean Hubuum inserted an audit event row in the current database transaction; if a later step rolls the transaction back, the audit row and the domain mutation are rolled back together even though the log line has already been written. These logs use the audit catalog labels:

| Field | Description |
| ----- | ----------- |
| `operation` | `mutation_recorded` |
| `mutation_phase` | `recorded` |
| `entity_type` | Catalog entity label, such as `collection` |
| `action` | Catalog action label, such as `created` |
| `entity_id` | Entity identifier when available |
| `actor_principal_id` | Acting principal when available |

Read helpers log at `DEBUG` for code paths that opt in to operation read logging.

Authorization decision logs use:

| Decision | Severity |
| -------- | -------- |
| Grant | `DEBUG` |
| Denial | `WARN` |

Authorization records include `event_type=authorization`, `decision`, `principal_id`, requested `permissions` as a JSON array, nullable `collection_count`, and a short `reason`.

## Sensitive Data Rules

Do not log secrets or high-volume payloads. In particular, logs must not include bearer tokens, token hashes, password hashes, sink secrets, raw `Authorization` headers, request bodies, response bodies, audit `before` or `after` snapshots, or remote target secret material.

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
