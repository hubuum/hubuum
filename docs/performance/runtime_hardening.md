# Runtime hardening measurements

These are reproducible workload samples from 2026-09-05, recorded in
[the machine-readable results](runtime_hardening.json). The host was an Apple M2
Pro with 12 logical CPUs and 32 GiB RAM, running macOS 26.6.2 and Rust 1.98.0.
PostgreSQL 17 ran in a disposable local Docker container. This was a shared
developer machine with other builds and tests running, so these samples establish
behavior and approximate costs, rather than a production capacity estimate or a
before/after speedup. No timing threshold is used as a correctness assertion.

## Authenticated requests, graphs, and external policy

These measurements use the unoptimized test profile and a real PostgreSQL
database. Request measurements cover Actix middleware, bearer authentication,
authorization, handler execution, response serialization, and response-body
consumption. They exclude socket transport and TLS. The first request is a
warm-up, including the token's throttled activity update.

| Workload | Observed result | Enforced behavior |
| --- | --- | --- |
| 20 warm authenticated collection GETs | Median 7.36 ms; p95 12.28 ms; 100 SQL queries | Five queries per request, no audit transaction control queries; separate invalid-token test proves one lookup |
| Export 3 permitted objects among 300 candidates, each with 8 KiB data | 31.99 ms; 15 SQL queries; policy batches 128, 128, 44 | Bounded candidate pages and early termination after the visible page is filled |
| Dense depth-3 graph, 7 vertices / 10 edges | 16.32 ms; one returned row | Fits a test work budget of 64 path rows |
| Dense depth-3 graph, 13 vertices / 36 edges | Rejected in 6.97 ms | Exceeds the same path-work budget despite an output limit of one |
| Dense depth-3 graph, 25 vertices / 136 edges | Rejected in 6.46 ms | Rejects further density growth before final sorting and deduplication |

The external-policy export uses the configured external authorization path with
an in-process deterministic policy backend. It measures real candidate fetching,
payload handling and authorization batching; it does not measure a remote
Treetop service's network latency. A separate all-denied scan test stops after
10,000 authorization candidates, allowing one extra fetched row to detect
exhaustion. The last permitted candidate is still eligible for the output page.

Graph work is measured in generated path rows. It does not bound every index
probe or filtered edge examination, nor total PostgreSQL memory. Statement
timeouts remain necessary. Larger deployment-scale tests should vary graph
density, depth and payload size independently and collect database CPU, resident
memory and temporary I/O in addition to these checks.

## Concurrent template and schema validation

The release-profile benchmark validates a 256-item JSON document with 1 KiB
payloads per item and renders it through a macro in a fresh worker process.
Every iteration includes input serialization, admission, process creation,
template compilation, rendering and response transport. Schema compilation is
warmed before concurrency comparisons. A separate cold-start observation was
493 ms; one cold sample cannot establish a startup latency distribution.

| Concurrent callers | Completed renders | Total time | Median operation | p95 operation | Peak live Rust heap per worker |
| --- | --- | --- | --- | --- | --- |
| 1 | 10 | 87 ms | 8.00 ms | 8.08 ms | 1,960,075 bytes |
| 4 | 40 | 89 ms | 8.04 ms | 8.45 ms | 1,960,075 bytes |
| 8 | 80 | 173 ms | 16.16 ms | 24.20 ms | 1,960,075 bytes |

At eight callers, the four-worker admission limit increases waiting time. The
heap counter includes worker compilation and rendering allocations, but excludes
allocator bookkeeping, stacks, executable pages and the parent's protocol
buffers. Separate tests exercise oversized output, retained intermediate strings
and large macro captures, proving that budget exhaustion terminates the worker
and leaves the parent able to render again. These tests cover limits that normal
workload measurements alone cannot establish.

## Reproduction

Use a disposable PostgreSQL database with the test runner's configured database
administrator. The runner creates and removes its own database and roles:

```sh
source .env
RUST_TEST_NOCAPTURE=1 ./run_tests.sh --no-fail-fast
```

The request, export and graph samples emit `PERFORMANCE_EVIDENCE` JSON lines.
Run the independent template/schema harness with:

```sh
cargo build --locked --release -p hubuum-templates --bin hubuum-template-worker
cargo bench --locked --features runtime-limit-bench --bench template_schema_concurrency
```

The benchmark workflow retains its JSON-lines output as the
`template-schema-evidence` artifact. The conventional nested Cargo benchmark
entrypoint keeps this custom harness separate from Criterion/Callgrind discovery.

For the enforced limits and deployment changes, see
[runtime boundaries](../runtime_hardening.md).
