# Expensive request admission

This is the **partial occupancy and fencing slice** of
[#521](https://github.com/hubuum/hubuum/issues/521). The issue remains open.
The [measured request inventory](performance/api_admission.md) selects the first
paths; [lifecycle H5](performance/lifecycle_budgets.md) remains the resource
acceptance gate. This slice does **not** establish H5 candidate/scanned-input
limits or a hard process-heap bound.

## Policy and coverage

Authenticated aggregate, history and audit GET requests, and POST submissions to
`/api/v1/imports` and `/api/v1/exports`, reserve shared capacity before handler
extraction and preparation. Existing authorization remains in each handler.
Health/readiness, ordinary resource reads, task status/cancel/event streams and
artifact downloads are outside this request lane. Artifact producers and durable
task admission retain their own accounting.

The authoritative PostgreSQL control policy initially permits 16 concurrent
expensive requests deployment-wide and two per captured principal UUID. Each
request receives 15,000 milliseconds, a 16 MiB ingress ceiling and a 16 MiB
response-publication ceiling. These are provisional finite safety settings, not
measured production sizing recommendations. The duration accepts 1–60,000 ms;
count/byte limits are positive signed-64-bit values.

An unscoped human administrator reads `GET /api/v1/admin/api-request-capacity`
and replaces the policy with `PUT` to the same route:

```json
{
  "expected_revision": 1,
  "policy": {
    "deployment": 16,
    "principal": 2,
    "duration": 15000,
    "input_bytes": 16777216,
    "output_bytes": 16777216
  }
}
```

The response contains `revision` and `policy`. Stale updates receive 409.
Replicas use the committed policy; process restarts cannot overwrite it. A
reduction blocks new reservations while existing requests drain under their
original byte and duration allowances.

Production PostgreSQL composition limits expensive productive transactions to
`max(pool_size - 1, 1)` per process. With at least two pool connections, expensive
work leaves a connection available for ordinary/operational work. A one-connection
pool uses shared capacity and has weaker control-path progress guarantees.
Cancellation uses the existing isolated operational pool. Settlement uses a
primary-pool control transaction so waiting for an owner cannot occupy the
connection needed to cancel that owner's productive work.

## Ownership, cancellation and restore

One random request UUID owns each physical attempt; it is not a fabricated task.
Admission serializes on the shared policy row, captures the stable principal UUID
and current application incarnation, and charges the complete configured time
allowance. This counter is admitted time, not a measurement of actual CPU time.
Repeated preparation for a task duplicate is another physical attempt.
Task receipt lookup and durable task reservation semantics remain independent.

Every productive database transaction takes a shared lock on the exact owner
tuple, verifies its deadline/incarnation and retains the lock until completion.
Settlement deletes that tuple and waits for issued transactions. Repeated cleanup
cannot retire a newer owner. Expiry never releases occupancy by itself: elapsed
time, a lost socket or an unlocked database tuple cannot prove HTTP-side work has
stopped. The supervisor settles the exact owner after work ends.
Late work cannot resume through the retired tuple, including an older
repeatable-read snapshot.

A supervisor owns admission itself as well as handler/body execution, retaining
cleanup ownership when the HTTP wait is dropped. Its local
deadline starts before admission, so policy waits and response/network latency
and task scheduling cannot refresh the absolute allowance. It signals
cooperative cancellation, carries the original deadline into PostgreSQL query
cancellation, and settles occupancy after the handler/body boundary. Timeout does
not prove an import/export task was never committed. Clients should retain their
idempotency key and inspect/retry the same submission under its existing receipt
contract after an ambiguous connection failure. Deadlines cannot preempt a
synchronous Rust computation; occupancy remains charged until it actually ends.

An abrupt replica crash can leave owner rows charged indefinitely. This reduces
available capacity and can eventually reject all expensive requests. Automatic
crash reconciliation is **not delivered** by this slice. To recover abandoned
owners, first stop/isolate all API replicas so their HTTP producers cannot resume.
Then, using migration authority, settle the remaining owners:

```sql
SELECT public.hubuum_api_request_settle(owner)
FROM hubuum_control.api_request_owners;
```

Each call still waits for outstanding database locks. Restart API replicas only
after settlement completes. Never infer safe recovery from row age or lock
availability while HTTP producers may still run.

The control policy and monotonic attempt/time charges survive logical restore.
Owner incarnation checks fence pre-restore requests. Old occupancy is reclaimed
only when its database work has settled; principal integers reused by restored
application data do not transfer reservations. No signed checkpoint function is
replaced and no new independently protected policy authority is introduced.

## Error and observability contract

All admission errors use `Cache-Control: no-store`. The standard outer tracing
middleware supplies `X-Request-ID` and echoes a valid `X-Correlation-ID`. No
other principal's identity, counts or workload appears in the body.

| HTTP status | Bounded reason | Retryable |
| --- | --- | --- |
| 429 | `api_capacity_deployment`, `api_capacity_principal` | Yes, after capacity drains |
| 503 | `api_capacity_identity_unavailable`, `api_capacity_reconciliation_required` | Yes, after recovery |
| 413 | `api_capacity_input_bytes`, `api_capacity_output_bytes` | No; reduce the request |
| 422 | `api_capacity_deadline_exceeded` | No; reduce the request |

```http
HTTP/1.1 429 Too Many Requests
Cache-Control: no-store
Content-Type: application/json
X-Request-ID: 2e66394a-3ec6-44e6-bf79-8a0e2a84b395
X-Correlation-ID: client-import-17

{"error":"API Request Admission","reason":"api_capacity_principal","retryable":true,"message":"Expensive request admission is temporarily unavailable; retry after capacity or control state recovers"}
```

There is no `Retry-After`: request occupancy has no reliable release-time
prediction, and reconciliation may need operator action. Consumers should use
bounded backoff for retryable outcomes and preserve idempotency keys. They should
handle unknown future reason strings using status/retryability rather than prose.

Existing `hubuum_api_errors_total` records the finite reason vocabulary.
Backend-neutral storage observations use capability `api_request_admission` and
the fixed policy/admit/settle operation names. They include cleanup success or
failure without principal, owner, path or resource IDs in metric labels.

## Remaining H5 resource acceptance

The ingress wrapper counts HTTP payload chunks before forwarding them to JSON
extraction. The JSON extractor also retains its existing decompression limits.
The response wrapper rejects over-limit publication. Neither check implies that
all producer allocations happened after a byte check. Time occupancy charges are
configured execution allowances; they are not measurements of scanned bytes.

| Path | Current estimate/charging | Allocation before checks | Required remaining evidence/enforcement |
| --- | --- | --- | --- |
| Aggregates | Shared occupancy/time; existing 500-candidate, 8 MiB intermediate batches | PostgreSQL aggregate state, one decoded batch and serialized values | Bound/charge request-wide actual candidates and source bytes; reject unknown-cost shapes; qualify local and delegated paths against H5 |
| History lists, population and navigation | Shared occupancy/time; returned page count | Diesel can retain a complete page, including large JSON values, before publication checking | Bound retained source production and cumulative candidates/bytes before accepting results; qualify exact totals and as-of selection |
| Audit | Shared occupancy/time; authorization discovery has its existing candidate bound | Complete stored-event page and enrichment precede publication checking | Bound actual audit-source production and visibility work; qualify exact totals, source bytes and large event documents |
| Import submission | Actual ingress chunks and full configured time allowance | A decoded request exists before validation/hash/planning completes | Measure and bound preparation candidates/intermediates; demonstrate retry work charges and ambiguity-safe receipt behavior |
| Export submission | Actual ingress chunks and full configured time allowance | Decoded request and template/runtime preparation precede durable task admission | Measure and bound preparation/source loads independently of artifact production; qualify duplicate preparation |

H5 remains 10,000 candidates, 64 MiB captured/scanned input and 16 MiB results.
SQL statement counts, response sizes and planner estimates cannot substitute for
actual source accounting. Unsupported or unknown-cost shapes must fail or use an
explicit finite conservative bound in the remaining implementation. Physical
retries remain chargeable. Partial work must never become a complete empty result.

Live replica/disconnect/crash tests, large-row and exact-boundary resource tests,
logical-restore charge tests, consumer contract qualification and the coordinated
full suite/container checks remain required before closing #521.

## Upgrade

Run migration `2026-11-03-000002_api_request_admission` with migration authority
and refresh runtime function grants before deploying this slice. The migration
creates only control tables/functions; it does not rewrite retained application
data. Rollback requires draining request owners and stopping producers first.
The required storage capability is an incompatible storage-SDK 0.18 change;
update all ten exact SDK dependencies and implement the
[adapter upgrade contract](storage_adapter_sdk.md#upgrading-from-017-to-018).
The integer REST resource IDs and task idempotency contract remain unchanged.
