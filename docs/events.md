# Event Delivery

Hubuum stores one canonical event stream in `events`. Audit reads query that
stream directly, while external delivery fans matching events out through
`event_subscriptions` and `event_deliveries`.

## Webhook Sinks

Webhook delivery is the reference concrete sink. A webhook subscription posts
the event envelope as JSON to the URL in the subscription `routing` object:

```json
{
  "routing": {
    "url": "https://example.com/hubuum/events"
  }
}
```

The request method is always `POST`. Hubuum sends the event UUID in both
`Idempotency-Key` and `X-Hubuum-Event-Id`, and the JSON body includes the same
`event_id` field. Consumers should deduplicate by `event_id`.

Webhook sink `config` may include static string headers and optional local
delivery limits:

```json
{
  "config": {
    "headers": {
      "X-Integration": "inventory-sync"
    },
    "timeout_ms": 5000,
    "max_response_bytes": 65536
  },
  "secret_ref": "inventory_webhook"
}
```

When `secret_ref` is set, Hubuum reads
`HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>` with the reference uppercased and sends
it as a bearer token. For the example above, the environment variable is
`HUBUUM_EVENT_SINK_SECRET_INVENTORY_WEBHOOK`.

Webhook HTTP execution uses the shared hardened outbound HTTP layer: HTTPS-only
URLs, embedded credential rejection, DNS resolution and address screening, IP
pinning, redirect refusal, timeout caps, response-size caps, and sensitive
response-header redaction.

## AMQP Sinks

AMQP delivery is available when Hubuum is built with the `amqp` feature. An
AMQP sink publishes the event envelope as JSON to the exchange in sink
`config`:

```json
{
  "config": {
    "uri": "amqps://publisher:{secret}@rabbitmq.example/%2f",
    "exchange": "hubuum.events",
    "exchange_type": "topic",
    "declare_exchange": true,
    "durable": true,
    "mandatory": true
  },
  "secret_ref": "rabbitmq_password"
}
```

When `secret_ref` is set, the AMQP URI must contain `{secret}`. Hubuum reads
`HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for URI
userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_RABBITMQ_PASSWORD`.

The routing key is always `{entity_type}.{action}`, such as
`namespace.created`. Hubuum sets the AMQP `message_id` property to the event
UUID and enables publisher confirms for each delivery attempt. Consumers should
deduplicate by `event_id` or `message_id`.

## Valkey Stream Sinks

Valkey stream delivery is available when Hubuum is built with the `valkey`
feature. Hubuum uses the mature Redis protocol client for this transport, so
the connection URL uses the standard `redis://` form accepted by Redis and
Valkey deployments.

The subscription `routing` object selects the stream key:

```json
{
  "routing": {
    "stream": "hubuum:events"
  }
}
```

The sink `config` holds the Valkey connection URL and optional stream trim
settings:

```json
{
  "config": {
    "uri": "redis://default:{secret}@valkey.example/0",
    "max_len": 100000,
    "approximate_trim": true
  },
  "secret_ref": "valkey_password"
}
```

When `secret_ref` is set, the URI must contain `{secret}`. Hubuum reads
`HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for URI
userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_VALKEY_PASSWORD`.

Each `XADD` entry includes discrete fields for `event_id`, `entity_type`,
`entity_name`, `action`, and `actor_kind`, plus the full JSON envelope in the
`payload` field. Consumers should deduplicate by `event_id`.

## Email / SMTP Sinks

Email delivery is available when Hubuum is built with the `email` feature. This
transport is intended for human-facing notifications, while the canonical event
stream remains the source of truth for audit and retry state.

The subscription `routing` object holds the message recipients:

```json
{
  "routing": {
    "recipients": ["Ops <ops@example.com>"],
    "cc": ["audit@example.com"],
    "bcc": ["archive@example.com"]
  }
}
```

`to` is accepted as an alias for `recipients`.

The sink `config` holds the SMTP connection URL, sender, optional reply-to
address, and MiniJinja templates for the subject and text body:

```json
{
  "config": {
    "uri": "smtps://hubuum:{secret}@smtp.example.com",
    "from": "Hubuum <hubuum@example.com>",
    "reply_to": "noreply@example.com",
    "subject_template": "Hubuum {{ entity_type }} {{ action }}: {{ entity_name | default_if_empty(summary) }}",
    "body_template": "{{ summary }}\n\nEvent: {{ event_id }}\nEntity: {{ entity_type }}\nAction: {{ action }}\n"
  },
  "secret_ref": "smtp_password"
}
```

SMTP URLs use the normal `smtp://` and `smtps://` schemes supported by SMTP
deployments. When `secret_ref` is set, the URI must contain `{secret}`. Hubuum
reads `HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for
URI userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_SMTP_PASSWORD`.

Template context exposes the event envelope fields at the top level, including
`event_id`, `entity_type`, `entity_name`, `action`, `summary`, and
`occurred_at`. The full envelope is also available as `event`. Subjects must
render to a single non-empty line, and bodies must render to non-empty text.

## Delivery Semantics

Delivery is at least once. A successful `2xx` webhook response marks the
delivery `succeeded`; transport errors or non-success status codes are retried
with backoff until the configured attempt limit, then marked `dead`.

Hubuum does not guarantee ordering across events. Consumers that need ordering
should reconcile with `occurred_at` and the internal monotonic `id`, while still
deduplicating by `event_id`.

Operators can inspect, retry, or dead-letter delivery rows through the
`/api/v1/event-deliveries` admin endpoints.

## Operational Health

The admin endpoint `GET /api/v1/event-deliveries/health` returns a delivery
pipeline snapshot for operators and dashboards. It includes:

- Fan-out backlog: undispatched events, in-flight fan-out claims, stale fan-out
  claims, oldest pending age, worker settings, and notification-versus-poll
  wakeup counters.
- Delivery backlog: status counts, retryable failed rows, stale delivery
  claims, oldest due age, worker settings, and notification-versus-poll wakeup
  counters.
- Per-sink and per-subscription delivery counts, stale claims, retryable rows,
  enabled flags, and oldest due age.

Use the fields together to distinguish common failure modes:

- `fanout.pending_events > 0` with rising `oldest_pending_age_seconds` means
  events are being written but fan-out is not keeping up.
- Fan-out `stale_claims > 0` means a worker claimed events but did not clear
  them before the lock expired.
- `delivery.counts.pending` or `delivery.counts.retryable` growing while
  fan-out is clear means delivery workers are not keeping up or are disabled.
- `delivery.stale_claims > 0` means a delivery worker claimed rows and did not
  finish before the lock expired.
- Per-sink `failed`, `dead`, or `retryable` growth with other sinks healthy
  usually points to a sink configuration, credential, endpoint, or broker
  problem.
- Wakeup `notifications_sent` increasing while `notification_wakeups` stays
  flat and backlog only drains on `poll_wakeups` points to worker wakeup
  trouble or missing workers.

Alert thresholds are deployment-specific, but a practical baseline is to page
when fan-out or delivery oldest due age exceeds the poll interval by several
minutes, when stale claims remain non-zero across multiple lock-timeout
windows, or when dead-letter counts grow for a production sink. First confirm
worker counts and lock/poll settings in the health response, then inspect the
affected delivery rows, fix the sink or worker condition, and use the retry
admin action for rows that should be released from `failed` or `dead`.

## Retention And Archival

Event retention purge is available as an operational worker, but is disabled by
default because it deletes audit rows. Enable it only after choosing retention
windows that match the deployment's audit requirements:

```text
HUBUUM_EVENT_RETENTION_PURGE_ENABLED=true
HUBUUM_EVENT_RETENTION_DAYS=365
HUBUUM_EVENT_DELIVERY_RETENTION_DAYS=30
HUBUUM_EVENT_RETENTION_PURGE_INTERVAL_SECONDS=3600
HUBUUM_EVENT_RETENTION_PURGE_BATCH_SIZE=1000
```

The purge path is the only application path allowed to bypass the append-only
`events` trigger. It sets the transaction-local `events.allow_purge` guard
before deleting eligible event rows. Normal `DELETE` statements against
`events` continue to fail.

Events become purge-eligible after `HUBUUM_EVENT_RETENTION_DAYS`, but the purge
will not delete an event while it has active `pending`, `failed`, or
`in_flight` deliveries. Deleting an eligible event cascades to its remaining
delivery rows. Terminal `succeeded` and `dead` delivery rows are also purged
independently after `HUBUUM_EVENT_DELIVERY_RETENTION_DAYS`, using their
`updated_at` timestamp so retention starts when the delivery reaches its
terminal state.

Optional archival writes selected event rows as JSON Lines before deletion:

```text
HUBUUM_EVENT_RETENTION_ARCHIVE_PATH=/var/lib/hubuum/event-archive.jsonl
```

Each archive line contains `archived_at` and the full event row. If archive
writing fails, the worker does not delete that batch.
