# Schema profiling

Uses the published [`json-schema-profiler` 0.0.1](https://crates.io/crates/json-schema-profiler/0.0.1) library.

Queue a schema suggestion for a class with
`POST /api/v1/classes/{class_id}/schema/profiles`. The request has two optional
settings:

```http
POST /api/v1/classes/42/schema/profiles
Authorization: Bearer <token>
Content-Type: application/json
Idempotency-Key: hosts-schema-review

{"preset":"balanced","sample_limit":100}
```

| Option | Default | Values |
| --- | --- | --- |
| `preset` | `balanced` | `strict`, `balanced`, `expansive` |
| `sample_limit` | `100` | Integer from 1 through 250 |

Unknown fields, duplicate fields, invalid presets, and invalid sample sizes return
400 before a task is submitted. `{}` uses the defaults.

| Preset | Mixed types | Required properties | Additional properties |
| --- | --- | --- | --- |
| `strict` | Reject unrelated types | Present in every applicable object | Deny |
| `balanced` | Preserve observed unions | Present in every applicable object | Allow |
| `expansive` | Preserve observed unions | All optional | Allow |

All presets preserve observed nulls and distinguish missing properties from null
values. Integer and fractional observations widen to `number`. Numeric evidence
preserves arbitrary precision. This initial API offers no subtree selection,
per-path policies, string enums, or computed-field profiling.

## Task and result

Submission returns `202 Accepted`, a `schema_profiling` task, and a `Location`
header pointing to `/api/v1/tasks/{task_id}`. Poll that URL for completion. The
`details.schema_profiling.output_url` points to
`GET /api/v1/classes/{class_id}/schema/profiles/{task_id}`. Task listing supports
`kind=schema_profiling`, `class_id`, and `output_state` filters. Existing
idempotency semantics and `POST /api/v1/tasks/{task_id}/cancel` apply.

The result contains:

- `sampled: true`, `sampled_objects`, `effective_sample_limit`, and `truncation`.
- The normalized request `options` and `status`: `candidate`, `blocked`, or
  `insufficient`.
- `schema`, containing a Draft 2020-12 candidate when inference succeeds.
- `profile`, containing structural counts and distinct-document evidence from
  `json-schema-profiler` 0.0.1, and `findings` explaining inference conflicts or
  limited evidence. Scalar values are not collected.
- `replay`, containing validation counts and bounded diagnostics for exactly the
  in-memory sample used for inference, and `sample_valid`, indicating complete,
  nonempty acceptance of that sample. These are null when no candidate exists.

An empty authorized sample yields `insufficient`. A strict-policy conflict yields
`blocked`. Both are successful report-producing tasks. Resource-limit errors or
execution failures fail the task and publish no report. Cancellation and
execution deadlines likewise publish no partial report.

The endpoint never changes the class schema. Review a candidate and use the
existing [schema evolution](schema_evolution.md) workflow to stage it, analyze
whole-class impact, and activate it separately. A passing sampled replay does not
prove whole-class compatibility or predict future data.

## Authorization and sampling

Submission and execution require `ReadClass`. Sampling intersects current
`ReadCollection` and `ReadObject` permissions with the submitting token's retained
scope boundary. External policy decisions are applied before profiling. The
worker resolves the submitting principal again at execution time.

Objects are chosen by ascending ID from the authorized raw `data` documents.
This is a deterministic convenience sample, not a random or statistically
representative sample. It requests no exact class-wide count. `sample_limit`
truncation means an additional authorized object existed; `candidate_limit`
means the external-policy scan reached 1,024 candidates without proving
exhaustion. Otherwise `truncation` is null. Concurrent writes can change the
objects visible between reads; replay uses the captured sample without rereading.

Download requires task visibility, current class access, and current read access
to every sampled object. A scoped token must cover the submitting token's scope;
it cannot download a report submitted without scopes. Deleted sampled objects
make the report unavailable. Reports use `Cache-Control: private, no-store`.
Generic task events and projections contain no report data or sampled object IDs.

## Limits and retention

Each principal may have two active profiling tasks. CPU work runs on a blocking
worker with cooperative cancellation checks between documents and phases. The
execution deadline defaults to 300 seconds and is configurable with
`HUBUUM_TASK_SCHEMA_PROFILING_EXECUTION_TIMEOUT_SECONDS`; queue time is excluded.

Server bounds include 32 MiB of compact JSON across the sample, 100,000 nodes per
document, depth 32, 1,024 profile paths, 256 KiB of path payload, a 1 MiB profile,
a 512 KiB schema, 4,096 findings, and a 4 MiB stored artifact. Limits reject
incomplete profiling instead of presenting a partial candidate. Object loading
uses the existing bounded catalog contract; these are logical workload and output
bounds, not an exact heap-memory limit.

Reports expire after 24 hours. Reads return 410 after expiry, including after
cleanup; reports not yet produced return 404, with status available through the
task endpoint. Reports are transient artifacts excluded from backup documents.
Restoring a backup retains terminal task metadata but requires resubmitting
profiling to obtain a report. Apply the new migration before running this server
version, and upgrade all API and worker processes before accepting profiling
requests. Rolling back the migration deletes profiling tasks and their reports.
