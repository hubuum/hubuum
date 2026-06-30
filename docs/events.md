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

## Delivery Semantics

Delivery is at least once. A successful `2xx` webhook response marks the
delivery `succeeded`; transport errors or non-success status codes are retried
with backoff until the configured attempt limit, then marked `dead`.

Hubuum does not guarantee ordering across events. Consumers that need ordering
should reconcile with `occurred_at` and the internal monotonic `id`, while still
deduplicating by `event_id`.

Operators can inspect, retry, or dead-letter delivery rows through the
`/api/v1/event-deliveries` admin endpoints.
