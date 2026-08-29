# Event And Audit

Hubuum stores one canonical event stream in `events`. Audit reads query that
stream directly, while external delivery fans matching events out through
`event_subscriptions` and `event_deliveries`.

The event stream is append-only during normal application operation. Domain
changes emit events in the same database transaction as the state change, so an
event exists only if the change commits.

At the storage boundary, ordinary audited mutations require an explicit actor
context and return a committed receipt identifying that durable event. A
receipt is non-sensitive proof of persistence, not an audit projection; event
snapshots and provenance remain permission-scoped reads. The normative adapter
guarantees and their reusable certification are documented in the
[storage contract](storage_boundary/contract.md).

Audited mutation paths compare the requested state with the stored state before
writing. Requests that would leave domain state unchanged are no-ops: they do
not advance `updated_at` or append lifecycle events. This includes identical
entity updates, moves to the current parent, repeated permission grants or
revocations, repeated service-account disable requests, and membership changes
that do not change effective membership.

## Audit Log

Audit readers query the canonical stream with `GET /api/v1/events`. The
endpoint is cursor-paginated and supports the normal pagination headers:

```http
GET /api/v1/events?entity_type=collection&action=created&limit=50&sort=-occurred_at
Authorization: Bearer <token>
```

Supported audit filters are:

| Filter | Meaning |
| ------ | ------- |
| `entity_type` | Event entity type, such as `collection`, `class`, `object`, or `task` |
| `entity_id` | Integer id of the affected entity |
| `action` | Event action for the entity type, such as `created`, `updated`, or `deleted` |
| `actor_kind` | Immediate actor class: `user`, `worker`, or `system` |
| `actor_user_id` | Principal id for user actors, including service accounts |
| `initiator_user_id` | Root task initiator principal id; legacy task events derive this from their queued event |
| `collection_id` | Collection directly attached to the event |
| `occurred_after` | Lower `occurred_at` bound; accepts RFC 3339 or `YYYY-MM-DD` |
| `occurred_before` | Upper `occurred_at` bound; accepts RFC 3339 or `YYYY-MM-DD` |
| `before_revision` | Positive revision from the pre-mutation entity snapshot |
| `after_revision` | Positive revision from the post-mutation entity snapshot |

Supported sorts are `id` and `occurred_at`, with `-` for descending order.
For example, `sort=-occurred_at` returns the newest visible events first.

Every audit response retains the compatibility fields `actor_user_id` and
`actor_kind` and also includes shared mutation provenance:

```json
{
  "actor_user_id": null,
  "actor_kind": "worker",
  "provenance": {
    "actor": {
      "kind": "worker",
      "principal": null
    },
    "initiator": {
      "principal_id": 1,
      "name": "admin"
    },
    "task_id": 12
  }
}
```

The actor is the principal or server component that performed the immediate
mutation. The initiator is the principal that submitted the root task. Direct
request mutations normally have a user actor and no separate initiator.
Principal names are resolved when a page is read; durable IDs remain present
when a principal has been deleted, while an unavailable name is `null`.
Historical task events that predate stored initiator provenance derive it from
the task's queued event with one bounded query per response page.

Audit visibility is collection-scoped:

- A caller sees collection events only for collections where the caller has
  `ReadAudit`.
- Events that reference related collections in event metadata are visible to a
  caller with `ReadAudit` on one of those related collections.
  Related-collection-only visibility returns the event identity, actor, summary,
  metadata, and schema version, but redacts `before` and `after` snapshots.
  A caller sees snapshots only when they also have direct `ReadAudit` on the
  event's own collection.
- Collection-less events are visible only to unscoped admins.
- Scoped tokens are constrained by both their token scope and the caller's
  underlying permissions.

Task lifecycle history is also stored in `events`. User-facing task reads
should usually use `GET /api/v1/tasks/{task_id}/events`, which applies the task
authorization model and returns task-focused history.

Convenience audit routes are available for common resources. These routes are
thin wrappers around `GET /api/v1/events`, apply the same `ReadAudit` scoping,
and accept the same pagination, actor, initiator, action, collection, and time
filters:

```http
GET /api/v1/collections/12/events
GET /api/v1/classes/34/events
GET /api/v1/classes/34/56/events
GET /api/v1/iam/users/78/events
GET /api/v1/iam/groups/90/events
GET /api/v1/export-templates/11/events
GET /api/v1/remote-targets/22/events
```

The object convenience route verifies that the object belongs to the class in
the path. A mismatched class and object pair returns `404 Not Found` rather than
querying the object through an unrelated class URL.

Use the generic endpoint for relation, permission, token, sink, and
subscription events where the useful identity is often in event metadata rather
than a single resource path.

## Sinks And Subscriptions

External delivery is configured in two layers:

- Event sinks are global transport definitions. Admins manage them through
  `/api/v1/event-sinks`.
- Event subscriptions are collection-scoped routing rules. Callers need
  `ManageEventSubscription` on the collection and manage them through
  `/api/v1/collections/{collection_id}/event-subscriptions`.

A sink describes how to deliver. A subscription describes which events should
be delivered to a sink. The primary subscription filters are `entity_types` and
`actions`; Hubuum validates these against the event catalog and rejects
impossible entity/action pairs at write time. Subscriptions may also include a
`filter` object that narrows matching by stable event-envelope fields before
delivery rows are created.

Supported `filter` fields are:

| Field | Meaning |
| ----- | ------- |
| `collection_ids` | Match events directly attached to one of these collections |
| `related_collection_ids` | Match events whose metadata references one of these related collections |
| `entity_ids` | Match affected entity ids |
| `entity_names` | Match affected entity names exactly |
| `actor_kinds` | Match actor kinds: `user`, `system`, or `worker` |
| `actor_user_ids` | Match actor principal ids |
| `initiator_user_ids` | Match root task initiator principal ids |
| `request_ids` | Match request UUIDs |
| `correlation_ids` | Match correlation ids exactly |

Each field is optional. Empty and omitted fields match all events for that
dimension. Multiple populated fields are combined with AND; values inside one
field are combined with OR. The filter can only narrow the subscription's
collection-scoped visibility. It cannot deliver unrelated collection events to a
subscription.

Example sink:

```json
{
  "name": "inventory-webhook",
  "kind": "webhook",
  "config": {
    "headers": {
      "X-Integration": "inventory"
    }
  },
  "secret_ref": "inventory_webhook",
  "enabled": true
}
```

Example collection subscription:

```json
{
  "sink_id": 1,
  "name": "collection-lifecycle-to-inventory",
  "description": "Send collection lifecycle events to inventory",
  "entity_types": ["collection"],
  "actions": ["created", "updated", "deleted"],
  "filter": {
    "actor_kinds": ["user"]
  },
  "routing": {
    "url": "https://inventory.example/hubuum/events"
  },
  "enabled": true
}
```

For email sinks, create narrow subscriptions rather than sending every audit
event to human recipients. For example, this subscription sends only failed
task lifecycle events from a collection to the configured mailbox:

```json
{
  "sink_id": 2,
  "name": "task-failures-to-ops",
  "description": "Email ops when collection tasks fail",
  "entity_types": ["task"],
  "actions": ["failed"],
  "filter": {
    "actor_kinds": ["worker"]
  },
  "routing": {
    "recipients": ["Ops <ops@example.com>"]
  },
  "enabled": true
}
```

Both the sink and the subscription must be enabled for matching events to fan
out to delivery rows. Disabling either one stops new matching deliveries
without deleting historical events or existing delivery rows.

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

Webhook, AMQP, Valkey, and email deliveries all receive the same `provenance`
object shown above. Actor and initiator names are resolved once for each
claimed delivery batch. A deleted principal keeps its durable ID and has a
`null` name when no current or tombstoned principal record can be resolved.

Webhook sink `config` may include static string headers and optional local
delivery limits:

```json
{
  "config": {
    "headers": {
      "X-Integration": "inventory-sync"
    },
    "timeout_ms": 5000,
    "max_response_bytes": 65536,
    "max_request_bytes": 1000000
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
`config`. AMQP event delivery requires TLS, so configure `amqps://` URLs:

```json
{
  "config": {
    "uri": "amqps://publisher:{secret}@rabbitmq.example/%2f",
    "exchange": "hubuum.events",
    "exchange_type": "topic",
    "declare_exchange": true,
    "durable": true,
    "mandatory": true,
    "max_payload_bytes": 1000000
  },
  "secret_ref": "rabbitmq_password"
}
```

When `secret_ref` is set, the AMQP URI must contain `{secret}`. Hubuum reads
`HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for URI
userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_RABBITMQ_PASSWORD`.
Literal credentials in sink URIs are rejected; use `{secret}` plus `secret_ref`
instead.

The routing key is always `{entity_type}.{action}`, such as
`collection.created`. Hubuum sets the AMQP `message_id` property to the event
UUID and enables publisher confirms for each delivery attempt. Consumers should
deduplicate by `event_id` or `message_id`.

## Valkey Stream Sinks

Valkey stream delivery is available when Hubuum is built with the `valkey`
feature. Hubuum uses the mature Redis protocol client for this transport, so
the connection URL uses the standard Redis protocol URL form accepted by Redis
and Valkey deployments. Event delivery requires TLS, so configure `rediss://`
URLs.

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
    "uri": "rediss://default:{secret}@valkey.example/0",
    "max_len": 100000,
    "approximate_trim": true,
    "max_payload_bytes": 1000000,
    "io_timeout_ms": 25000
  },
  "secret_ref": "valkey_password"
}
```

When `secret_ref` is set, the URI must contain `{secret}`. Hubuum reads
`HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for URI
userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_VALKEY_PASSWORD`.
Literal credentials in sink URIs are rejected; use `{secret}` plus `secret_ref`
instead. `io_timeout_ms` bounds the Redis protocol connection and socket I/O
for the blocking driver call and defaults to 25,000 ms.

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
address, and MiniJinja export templates for the subject and text body:

```json
{
  "config": {
    "uri": "smtps://hubuum:{secret}@smtp.example.com",
    "from": "Hubuum <hubuum@example.com>",
    "reply_to": "noreply@example.com",
    "max_payload_bytes": 1000000,
    "subject_template": "Hubuum {{ entity_type }} {{ action }}: {{ entity_name | default_if_empty(summary) }}",
    "body_template": "{{ summary }}\n\nEvent: {{ event_id }}\nEntity: {{ entity_type }}\nAction: {{ action }}\n"
  },
  "secret_ref": "smtp_password"
}
```

SMTP URLs must use the TLS `smtps://` scheme. When `secret_ref` is set, the URI
must contain `{secret}`. Hubuum
reads `HUBUUM_EVENT_SINK_SECRET_<SECRET_REF>`, percent-encodes the value for
URI userinfo use, and substitutes it into the URI. For the example above, the
environment variable is `HUBUUM_EVENT_SINK_SECRET_SMTP_PASSWORD`.
Literal credentials in sink URIs are rejected; use `{secret}` plus `secret_ref`
instead.

Template context exposes the event envelope fields at the top level, including
`event_id`, `entity_type`, `entity_name`, `action`, `summary`, and
`occurred_at`, and `provenance`. The full envelope is also available as
`event`. Subjects must render to a single non-empty line, and bodies must
render to non-empty text. Webhook `max_request_bytes` and the transport
`max_payload_bytes` settings default to 1,000,000 bytes.

## Delivery Semantics

Delivery is at least once. A successful transport-specific acknowledgement
marks the delivery `succeeded`; transport errors or failed acknowledgements are
retried with backoff until the configured attempt limit, then marked `dead`.
For webhooks, any `2xx` response is successful and non-`2xx` responses are
retried.

Hubuum does not guarantee ordering across events. Consumers that need ordering
should reconcile with `occurred_at` and the internal monotonic `id`, while still
deduplicating by `event_id`.

Operators can inspect delivery rows through `GET /api/v1/event-deliveries` and
`GET /api/v1/event-deliveries/{delivery_id}`. Admins can release a failed or
dead delivery with `POST /api/v1/event-deliveries/{delivery_id}/retry`, or move
a row to the dead-letter state with
`POST /api/v1/event-deliveries/{delivery_id}/dead`.

Delivery workers are configurable and default-disabled:

```text
HUBUUM_EVENT_FANOUT_WORKERS=1
HUBUUM_EVENT_FANOUT_BATCH_SIZE=100
HUBUUM_EVENT_FANOUT_POLL_INTERVAL_MS=5000
HUBUUM_EVENT_FANOUT_LOCK_TIMEOUT_MS=30000
HUBUUM_EVENT_DELIVERY_WORKERS=0
HUBUUM_EVENT_DELIVERY_BATCH_SIZE=100
HUBUUM_EVENT_DELIVERY_POLL_INTERVAL_MS=5000
HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS=30000
HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS=25000
HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_BASE_MS=1000
HUBUUM_EVENT_DELIVERY_RETRY_BACKOFF_MAX_MS=300000
HUBUUM_EVENT_DELIVERY_MAX_ATTEMPTS=10
```

Keep delivery workers at `0` when the deployment uses the audit log only or
when sink credentials are not ready. Set `HUBUUM_EVENT_DELIVERY_WORKERS` above
zero once operators are ready for external transport delivery.
`HUBUUM_EVENT_DELIVERY_TRANSPORT_TIMEOUT_MS` must be less than
`HUBUUM_EVENT_DELIVERY_LOCK_TIMEOUT_MS`; this prevents a delivery attempt from
running past its claim window and racing with a retry worker.

Workers use PostgreSQL `LISTEN`/`NOTIFY` for low-latency wakeups across
processes and fall back to the configured poll intervals for eventual progress.
Event writes notify the fan-out channel only after commit, and fan-out notifies
delivery workers when it creates delivery rows. An idle delivery worker also
wakes at the earliest scheduled retry or expired-claim deadline, so retry
backoffs shorter than the safety poll remain effective.

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

Worker replicas coordinate claims with a PostgreSQL advisory lock. A claim
transaction persists an immutable batch ID, the exact selected event IDs and
documents, and the bounded terminal-delivery cutoff; it does not delete the
events. Archive I/O then runs outside the database transaction. A completion
transaction locks the same claim, deletes exactly its event IDs, performs the
bounded terminal-delivery cleanup, and records the final counts. A partial
index on terminal delivery age keeps that lookup bounded to its eligible queue.

If archival fails or the process exits, the pending claim and source events
remain durable. The next worker receives the same batch ID and documents.
Archives must therefore be idempotent by batch ID, and completion is also
idempotent. A purge-count mismatch, malformed claimed document, maintenance
transition, or busy coordinator is an error rather than a successful empty
batch.

Events become purge-eligible after `HUBUUM_EVENT_RETENTION_DAYS`, but the purge
will not delete an event while it has active `pending`, `failed`, or
`in_flight` deliveries. Deleting an eligible event cascades to its remaining
delivery rows. Terminal `succeeded` and `dead` delivery rows are also purged
independently after `HUBUUM_EVENT_DELIVERY_RETENTION_DAYS`, using their
`updated_at` timestamp so retention starts when the delivery reaches its
terminal state.

Local file archival is opt-in. Most deployments should export or consume audit
events through the database or event sinks instead of writing container-local
files. Enable local JSON Lines archival only when the configured directory is
durable and access-controlled:

```text
HUBUUM_EVENT_RETENTION_FILE_ARCHIVE_ENABLED=true
HUBUUM_EVENT_RETENTION_ARCHIVE_PATH=/var/lib/hubuum/event-archive
```

Each claim is written atomically to `<batch-uuid>.jsonl`. Every line contains
`retention_batch_id`, `archived_at`, and the full event row, including durable
`initiator_user_id` and `task_id` values. Event `schema_version` is `2` for
revision-aware mutation events; older stored events remain readable as version
`1` with null revision fields. If the final file already exists, the worker
accepts it only after validating its batch ID, line count, and exact event
documents against the claim; any mismatch is fatal.

On Unix, the archive directory is a real non-symlink directory restricted to
mode `0700`. Temporary and final batch files are regular non-symlink files with
mode `0600`. The worker flushes and synchronizes file contents before atomic
rename, then synchronizes the directory entry before acknowledging archival.
