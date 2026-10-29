# Expensive API admission measurements

This is the measurement and acceptance slice of
[#521](https://github.com/hubuum/hubuum/issues/521), following the merged identity
and task-admission foundations in #505/#506. It does not implement shared
reservations for synchronous requests or establish their production limits.
The [lifecycle budgets](lifecycle_budgets.md) and #503 remain the design authority.

## Selected operations and current bounds

Measure aggregates, retained history and audit reads first. These operations can
return a small page after doing substantial database or authorization work.
Import/export submission remains in #521's scope; measure its preparation work
separately from worker execution and coordinate its integration with #508.

| Operation | Existing execution and cost | Existing bound and measurement gap |
| --- | --- | --- |
| Class object aggregates | `src/services/object_aggregates.rs` delegates to the PostgreSQL aggregate operation. Locally authorized plain values use SQL aggregation; computed values and delegated authorization use candidate batches. Grouping and exact totals can process more source rows than the returned page. | At most three grouping dimensions and four measures. Production candidate pages target 500 rows, with one-row lookahead and an 8 MiB encoded batch bound; delegated intermediate aggregates have an 8 MiB bound. These are shape/batch bounds, not a request-wide scanned-row or byte allowance. |
| Retained resource history | `src/services/history.rs` and `history/navigation.rs` authorize history reads. PostgreSQL list operations optionally count matching retained versions before fetching a cursor page; handlers enrich captured principal identities. | Configured pagination bounds returned rows. Count queries are separate from the page limit. Measure deep histories, selective filters and `include_total` both ways. |
| Class population as of an instant | `src/api/v1/handlers/classes/history_population.rs` resolves the historical class, loads retained object versions and enriches names. Its cursor binds the instant, filters and authorization context. | Pagination limits the returned population, not the work needed to select effective versions or compute an exact total. An empty retained result does not establish historical emptiness. |
| Audit events | `src/api/v1/handlers/events.rs` resolves visible collections before `list_audit_events`; PostgreSQL counts, pages and enriches events within a read snapshot. | Pagination bounds the event page. Delegated complete-collection discovery has a 10,000-candidate ceiling; it is not an event-scan ceiling. Include visibility resolution and enrichment when measuring the complete request. |
| Import submission | `src/api/v1/handlers/imports.rs` validates the decoded request, handles credential approval when applicable, serializes/hashes it and captures scope before task submission. | Existing task admission applies when creating the durable task. It does not account for all preparation already performed by the HTTP request. Measure input size and preparation separately. |
| Export submission | `src/services/exports/submission.rs` serializes/hashes the request, prepares the runtime and validates it before finding or creating a task. Template preparation can load collection templates. | Task idempotency can avoid a second task reservation, but request preparation has already done work. Do not treat duplicate submission as zero-cost HTTP execution. |

The default page size is 100 and the default maximum is 250, configured through
`HUBUUM_DEFAULT_PAGE_LIMIT` and `HUBUUM_MAX_PAGE_LIMIT`. Cursor pagination may fetch
one additional row to detect continuation. A page size is not a scan budget.
Plain SQL aggregate queries still group their eligible source rows when exact
group totals are disabled. Computed/delegated batching bounds each batch, while
its loop can continue through the eligible source population.

These observations describe the checked-in implementation. Small test-only
aggregate batch limits are not production defaults. See the source bounds in
`crates/hubuum-storage-postgres/src/operations/object_aggregate.rs`,
`object_aggregate/bounded_json.rs`, `event_audit.rs` and `history.rs`.

## Measurement contract

Use reproducible, isolated fixtures and preserve correctness checks alongside
cost evidence. Vary source population, retained versions, payload size, filter
selectivity, group cardinality and exact-total selection. Keep local and delegated
authorization results separate. Record warm-up and every measured sample; report
medians and tail observations without describing small samples as reliable
production percentiles.

| Evidence | Meaning and limits |
| --- | --- |
| Fixture recipe and counts | Expected source population, retained versions, payload shape and visibility. Fixture rows are not measured physical scans. |
| Elapsed time and SQL statement count | Application/storage cost for the declared boundary. Fixed round trips do not establish fixed scan, CPU or memory cost. |
| Returned rows and encoded result bytes | Size of the measured serialization only. State whether it includes HTTP enrichment, headers and framing. |
| PostgreSQL plan evidence | Record actual rows/loops, filtered rows, buffers and temporary I/O when collected. Do not sum parent/child plan rows as distinct source candidates. Buffer accesses are not unique encoded input bytes. |
| Memory and concurrency | Measure process RSS, active reservations and database work separately. A sequential request timing is not multi-replica admission evidence. |
| Environment | Record source revision, build mode, database version/settings, fixture size, authorization mode and concurrency. Unmeasured values remain unknown. |

Current query-budget regressions in `src/tests/storage_performance.rs` provide
instrumentation and correctness checks. They do not establish a safe concurrent
request limit. Measurements in this slice must identify any HTTP, authorization,
serialization or physical-I/O work outside their execution boundary.

### Reproducing the diagnostic run

The ignored `api_admission_measurement` test in `tests/api_platform_suite/`
exercises authenticated requests through the Actix test application and reads
each response body. Its initial cases are class aggregates grouped by name,
collection history and collection-filtered audit events. Each uses an ordinary
locally authorized principal, a page size of 25, 1,000 or 10,000 source rows,
and `include_total=false` or `true`. Each case has five fresh fixture rounds,
with two warm-up requests and ten measured requests per round. The emitted
evidence records those settings under recipe `api-admission-v1`, format version 1.

Seeding, `ANALYZE` and removal of retained fixture rows use the runner's migration
authority; timed requests use the runtime database role and ordinary user token.
History versions come from real updates with the installed triggers. History and
audit samples assert that actor names are resolved. Payloads contain 256 bytes of
deterministic fixture text. Rounds create new scoped fixtures in the same database;
they do not reset its physical allocation or caches. Inventory reads and warm-ups
precede timing, and SQL capture overhead is included.

Build before timing, then run through the repository runner so migrations and
test data live in its disposable databases. Configure `.env` with the test
PostgreSQL settings described in [testing](../testing.md).

```bash
source .env
cargo test --locked --features integration-test-support,embedded-migrations \
  --test api_platform --no-run
source .env && ./run_tests.sh api_admission_measurement \
  -- --ignored --nocapture --test-threads=1
```

Preserve the `API_ADMISSION_EVIDENCE` JSON lines with the source revision and
environment. Reports include elapsed samples through response-body reads,
SQL/control/domain query counts, connection checkouts, returned rows and response
body bytes. Fixture counts and `row_to_json` encoded source bytes are separate
from physical scan measurements, which remain unknown. Do not label fixture
size as scanned work or use a small response body to claim H5 compliance.

This test executes the real application request path in-process; it does not
exercise a TCP connection, client disconnects or multiple server replicas. Its
sequential local timings do not establish constrained-profile performance or
reservation correctness. No PostgreSQL plan or physical-I/O evidence is implied
by its statement counts. Delegated authorization, class population as-of and
import/export submission remain additional measurement cases.

### Recorded baseline

The [2026-10-29 baseline](api_admission_baseline.json) retains all 600 latency
samples from 60 fixture rounds, their query counts and byte measurements, the
probe checksum and environment. The run used Rust 1.98.0 with an unoptimized test
build and debug information disabled, and PostgreSQL 18.6 on the same host. The
host exposes 32 logical CPUs and about 62 GiB RAM; the database container had no
CPU or memory quota. Storage medium and database RTT were not independently
measured. This is warmed local diagnostic evidence, not a constrained acceptance
run or a basis for production concurrency defaults.

Each latency below is the median of five round medians, with ten measured
requests after two warm-ups per round. SQL counts include control statements
and were constant across both source sizes for each row. Every response contained
25 rows. The complete suite and both Clippy passes finished before these samples
were collected; exclusive use of the shared host was not established.

| Operation | Exact total | 1,000 source rows | 10,000 source rows | SQL statements per request |
| --- | --- | --- | --- | --- |
| Object aggregates grouped by name | No | 12.67 ms | 42.88 ms | 11 |
| Object aggregates grouped by name | Yes | 18.29 ms | 77.77 ms | 12 |
| Collection history | No | 17.57 ms | 18.44 ms | 17 |
| Collection history | Yes | 17.39 ms | 23.02 ms | 18 |
| Collection-filtered audit events | No | 15.06 ms | 15.05 ms | 12 |
| Collection-filtered audit events | Yes | 15.57 ms | 17.60 ms | 13 |

Aggregate response bodies remained exactly 2,251 bytes while source population
grew tenfold. With exact totals, the median increased about 4.3 times despite
the same twelve SQL statements. Counted 10,000-object aggregate round medians
ranged from 76.23 to 153.73 ms; the slower round is retained in the report.
Small differences between nearby history/audit timings should not be interpreted
as improvements or production tail guarantees.

These observations make aggregate source work the strongest first admission
candidate among the measured shapes. History and audit exact totals also add
work beyond their returned page. A fixed page size, response-byte limit or
statement-count allowance alone cannot bound these operations. Actual scanned
work, memory, delegated authorization, larger payloads and concurrent replicas
still need evidence before selecting shared ceilings. The acceptance matrix
below remains open.

### H5 and physical work

Lifecycle budget H5 proposes ceilings of 10,000 candidates, 64 MiB of
captured/scanned input and 16 MiB of results per task. These are downstream
engineering targets, not limits currently enforced on every synchronous endpoint.
The request-admission implementation must define which selected operations use
these ceilings and how each unit is observed before claiming compliance.

Keep request occupancy, resident bytes and cumulative work distinct. Release
occupancy when execution is settled; release resident storage when its actual
owner is cleaned up. Neither action refunds cumulative work already performed.
Count repeated physical work on retries. Returned rows, task request JSON bytes,
an SQL statement count and planner estimates cannot stand in for actual scanned
input. An unknown estimate must reject or use a documented conservative bound;
it must not silently admit unbounded work. Budget exhaustion must remain an
explicit failure or incomplete/planning outcome, never a complete empty result.

## Acceptance matrix for shared request reservations

The following cases are required of the future implementation. They are not
claims established by the measurement harness.

| Case | Required result | Evidence owner |
| --- | --- | --- |
| Concurrent replicas and one principal | Atomic admission cannot exceed the configured shared ceiling, including simultaneous requests to different replicas. Captured principal UUIDs prevent integer reuse from transferring ownership. | Rust PostgreSQL concurrency and shared storage-contract tests; live replica exercise. |
| Different principals and operation classes | Per-principal and deployment ceilings compose; a saturated expensive operation cannot consume reserved operational/task-control capacity. | Rust admission contracts and live overload exercise. |
| Cheap endpoints | Health/readiness and ordinary cheap reads avoid blanket expensive-request charging. Saturated expensive work leaves the reserved operational path usable. | API regressions and live overload exercise. |
| Cancellation or client disconnect | Stop/fence ongoing work and settle the reservation; disconnect alone is not proof that database or spawned work stopped. Retried cleanup cannot release a newer owner. | Rust cancellation/fencing tests; real socket disconnect integration. |
| Replica crash and abandoned work | Recovery reclaims occupancy only after fencing or establishing that the old execution cannot continue. Late completion cannot publish under a replacement reservation. | PostgreSQL recovery tests and process-kill integration. |
| Logical restore and reused IDs | Restore reconciliation preserves the authority of captured identities and cumulative accounting; old occupancy cannot revive or attach to replacement principals. | Rust isolated restore contracts. |
| Retry or duplicate submission | Reuse recognized logical ownership where appropriate, without erasing cumulative charges. Repeated preparation, scans or transfers add their physical work. | Rust admission and endpoint regressions. |
| Unknown or exceeded cost | Reject or use a documented finite conservative bound. Exact-limit and one-unit-overflow cases stop before an invalid publication; partial work is never reported as a complete empty result. | Rust domain, storage and API tests with measured units. |
| Policy changes during execution | Reductions prevent new admissions while existing occupancy drains under documented rules; replicas observe one authoritative revision. | PostgreSQL concurrency and shared storage-contract tests. |
| Rejections and diagnostics | Public reasons are stable and bounded, without other principals' identities or usage. Metrics use bounded operation/reason labels and record recovery and releases. | API and observability regressions. |

Reuse shared validated count/byte values and captured identity proof types.
Choose request reservation ownership and recovery semantics with a real consumer;
do not represent synchronous requests as a fabricated task kind. A process-local
semaphore cannot establish a deployment-wide ceiling. Keep authorization checks
and existing query semantics intact while adding admission.

## Remaining delivery

This slice provides operation inventory and repeatable measurement support.
Shared request-reservation persistence, runtime integration, cancellation/recovery
and the acceptance cases above remain implementation work under #521. Select
initial operation classes and limits using measured cost; keep import/export
preparation independent from the artifact producer's execution allowance.

Any implementation that changes the storage SDK, endpoint errors, configuration
or persisted control state must complete the repository's corresponding API,
migration, container, compatibility and changelog checks. The measurement slice
alone does not enable admission behavior or require an operator upgrade.
