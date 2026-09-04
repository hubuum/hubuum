# Distributed Tracing

Hubuum can export a deliberately small OpenTelemetry trace model through
OTLP/HTTP protobuf. Tracing is disabled by default and does not change request,
task, event, or delivery behavior when the collector is unavailable.

## Configuration

At minimum, enable tracing and provide an HTTPS collector base endpoint:

```env
HUBUUM_TRACING_ENABLED=true
HUBUUM_TRACING_OTLP_ENDPOINT=https://otel-collector.example.com:4318
```

The exporter appends `/v1/traces` according to the OTLP/HTTP protocol. The
endpoint must use HTTPS, include no embedded credentials, query, or fragment,
and pass ordinary certificate and hostname verification.

Optional settings are:

- `HUBUUM_TRACING_OTLP_HEADERS`: comma-separated `name=value` collector
  metadata, such as an authorization header. The complete value is limited to
  8 KiB and 16 headers. It is secret configuration and is never reported.
- `HUBUUM_TRACING_OTLP_CA_CERT`: bounded PEM CA bundle for a private collector
  trust root. When configured, this bundle is the complete collector trust
  store; include every required root and intermediate.
- `HUBUUM_TRACING_OTLP_CLIENT_CERT` and
  `HUBUUM_TRACING_OTLP_CLIENT_KEY`: bounded PEM client identity files for
  mutual TLS. Configure both or neither.
- `HUBUUM_TRACING_CONNECT_TIMEOUT_MS`: collector connection timeout; defaults
  to `2000`.
- `HUBUUM_TRACING_EXPORT_TIMEOUT_MS`: one OTLP request timeout; defaults to
  `5000`.
- `HUBUUM_TRACING_FLUSH_TIMEOUT_MS`: graceful-shutdown flush bound; defaults to
  `5000`.
- `HUBUUM_TRACING_QUEUE_CAPACITY`: bounded span queue; defaults to `2048` and
  accepts `1` through `65536`.
- `HUBUUM_TRACING_BATCH_SIZE`: maximum export batch; defaults to `512`, accepts
  `1` through `8192`, and cannot exceed the queue capacity.
- `HUBUUM_TRACING_SAMPLING_MODE`: `off`, `always-on`, or
  `parent-based-ratio`; defaults to `parent-based-ratio`.
- `HUBUUM_TRACING_SAMPLE_RATIO`: finite root sampling ratio from `0` through
  `1`; defaults to `0.1`.
- `HUBUUM_TRACING_SERVICE_NAME`, `HUBUUM_TRACING_SERVICE_NAMESPACE`, and
  `HUBUUM_TRACING_DEPLOYMENT_ENVIRONMENT`: bounded resource identity. Defaults
  are `hubuum`, `hubuum`, and `production`.
- `HUBUUM_TRACING_TRUST_INCOMING_SAMPLING`: honor an upstream sampled flag;
  defaults to `false`. Keep this disabled at an untrusted public edge.
- `HUBUUM_TRACING_PROPAGATE_OUTBOUND`: inject the current W3C context into
  supported outbound HTTP integrations; defaults to `true`.

The authenticated running-configuration endpoint reports effective non-secret
values and whether endpoint, header, and TLS files are configured. It never
returns their values or paths.

### Why the exporter uses blocking reqwest

OpenTelemetry Rust 0.32's default `BatchSpanProcessor` executes exporter
futures on its own dedicated, non-Tokio thread. An asynchronous reqwest client
expects a Tokio reactor on the thread polling it, so the supported custom
transport here is reqwest's blocking client. The client and provider are
constructed and destroyed on blocking workers because reqwest also rejects
blocking-client lifecycle work inside an async runtime. This does not block
request or worker tasks: they enqueue completed spans into the bounded batch
processor, and only the exporter thread performs the HTTPS request. The
dependency and implementation declarations carry the same note so this
feature is not removed as apparently unnecessary.

## Propagation And Crate Boundaries

Live work uses the standard ambient OpenTelemetry `Context`. The application
composition root configures the exporter and the W3C propagator. Outbound HTTP
code reads the current context and receives only a narrow propagation-enabled
toggle; it does not receive `AppConfig` or exporter credentials.

Asynchronous work cannot depend on an ambient request span surviving a queue or
process boundary. `hubuum-events-core` therefore owns two small validated
values:

- `CorrelationId` is a printable, non-whitespace client value from 1 through
  128 bytes.
- `TraceLink` contains only a W3C trace ID, span ID, flags, and version.

Task and event storage contracts persist `TraceLink` as nullable typed fields.
Workers reconstruct an OpenTelemetry link from that value when executing a
task, fanning out an event, or attempting a delivery. Storage crates never
depend on OpenTelemetry, HTTP headers, the application configuration, baggage,
or a PostgreSQL-specific context type.

This separation is intentional: SDK context is used for immediate causal
parentage, while a minimal durable link is used for delayed or fan-out work.
Retries reuse the original event link rather than creating new persisted
provenance.

## W3C Header Policy

Inbound HTTP accepts one bounded `traceparent` and one bounded `tracestate`.
Each header is limited to 512 bytes, `tracestate` is limited to 32 members, and
invalid or repeated headers are ignored after emitting only a fixed reason.
Raw invalid values are never logged. Baggage is neither parsed nor propagated.

Supported outbound HTTP calls inject `traceparent` and `tracestate` from the
current span. Integration configuration cannot override `traceparent`,
`tracestate`, or `baggage`.

## Exported Span Catalog

Only these fixed span names can cross the OTLP boundary:

- `http.server.request`
- `auth.token_validation`
- `auth.provider`
- `auth.identity_refresh`
- `authz.permission_backend`
- `authz.scope_intersection`
- `db.connection`
- `db.operation`
- `storage_operation`
- `task.admission`
- `task.execute`
- `event.fanout`
- `event.delivery`
- `http.client.request`

`storage_operation` spans cover logical persistence calls for every selected
backend, including memory storage. PostgreSQL connection and transaction
diagnostics remain available as `db.connection` and `db.operation` spans.

Each category has a closed attribute allowlist. Route templates, status codes,
bounded operation categories, result categories, counts, attempts, and coarse
network or principal categories are allowed. SQL, bind values, raw URL paths,
hosts, IP addresses, usernames, principal IDs, object names, request or response
bodies, authorization values, tokens, event snapshots, and error messages are
not exported. Arbitrary tracing events and their fields are stripped at the
final exporter boundary. Baggage is excluded completely.

JSON logs remain the detailed local operational record. When a sampled or
recording span is active they include fixed-width lowercase `trace_id` and
`span_id` fields, allowing a trace lookup without broadening the exported
attribute policy.

## Failure And Shutdown Behavior

Collector DNS, connection, TLS, authorization, timeout, or response failures
happen on the batch processor thread. They do not fail the request or queued
work that produced the span. Queue capacity, batch size, and request timeouts
are bounded; the SDK drops spans when the queue cannot accept more work.

On graceful HTTP or worker shutdown, Hubuum asks the tracer provider to flush
and shut down within `HUBUUM_TRACING_FLUSH_TIMEOUT_MS` on a blocking worker so
the blocking client's internal runtime is never dropped from an async executor
context. A timeout or exporter failure is logged and counted, but cannot hold
process shutdown indefinitely.

Use the tracing metrics documented in [Runtime Metrics](metrics.md) to monitor
configuration, export outcomes, dropped spans, batch volume, queue utilization,
and flush outcomes. Check application logs for fixed initialization or shutdown
errors. Collector endpoints, header values, certificate paths, private keys,
and invalid incoming trace headers are never included in diagnostics.
