# Template worker measurements

These workload samples were recorded on 2026-09-05 and extracted from the
runtime-hardening change at `22743cca1bf5de204fd52c761afac614244f36a4`.
See [the machine-readable results](template_worker.json). The host was an Apple
M2 Pro with 12 logical CPUs and 32 GiB RAM, running macOS 26.6.2 and Rust 1.98.0.
This was a shared developer machine with other builds and tests running, so
these samples establish behavior and approximate costs, not production capacity
or a controlled before/after speedup. No timing threshold is used as a
correctness assertion.

## Concurrent template and schema validation

The release-profile benchmark validates a 256-item JSON document with 1 KiB
payloads per item and renders it through a macro in a fresh worker process.
Every iteration includes input serialization, admission, process creation,
template compilation, rendering and response transport. Schema compilation is
warmed before concurrency comparisons. A separate cold-start observation was
283 ms; one cold sample cannot establish a startup latency distribution.

| Concurrent callers | Completed renders | Total time | Median operation | p95 operation | Peak live Rust heap per worker |
| --- | --- | --- | --- | --- | --- |
| 1 | 10 | 82 ms | 8.25 ms | 9.37 ms | 1,960,075 bytes |
| 4 | 40 | 88 ms | 8.78 ms | 9.53 ms | 1,960,075 bytes |
| 8 | 80 | 166 ms | 16.05 ms | 18.05 ms | 1,960,075 bytes |

These samples use asynchronous callers and a dedicated process supervisor.
The earlier synchronous samples remain in the JSON for provenance; the shared
machine and separate runs do not establish a controlled speed comparison.
At eight callers, the four-worker admission limit increases waiting time. The
heap counter includes worker compilation and rendering allocations, but excludes
allocator bookkeeping, stacks, executable pages and the parent's protocol
buffers. Separate tests exercise oversized output, retained intermediate strings
and large macro captures, proving that budget exhaustion terminates the worker
and leaves the parent able to render again. Further regression tests saturate
admission, cancel queued and executing work, stall pipes, enforce deadlines,
verify OS child reaping, shut down the caller runtime and supervisor, and check
lifecycle logging without sensitive content. An authenticated request regression
runs long template operations on the same single-thread runtime and requires an
unrelated request to complete before the template deadline. These tests cover limits that normal
workload measurements alone cannot establish.

## Reproduction

Build the worker, then run the template/schema harness:

```sh
cargo build --locked --release -p hubuum-templates --bin hubuum-template-worker
cargo bench --locked --features runtime-limit-bench --bench template_schema_concurrency
```

The benchmark workflow retains its JSON-lines output as the
`template-schema-evidence` artifact. The conventional nested Cargo benchmark
entrypoint keeps this custom harness separate from Criterion/Callgrind discovery.

For the enforced limits and deployment changes, see
[template worker limits](../template_worker.md).

## Batching comparison

The harness also compares individual worker calls with one batched call for two
small email templates and a remote request with ten headers (twelve templates).
The email individual-call baseline includes the two former syntax validations.
Each pair uses identical sources and a shared 4 KiB context payload, checks the
rendered content, alternates execution order, discards its first warm-up pair,
and reports twenty samples per mode. These scenarios measure template processing,
including serialization and child startup; they exclude SMTP, HTTP and database
work. Both modes use the same worker binary and protocol version, so they measure
the effect of batching rather than a complete comparison against the old PR.
The historical measurements above predate batching.

A release-profile run on 2026-09-07 produced the following paired samples.
See the [machine-readable batching results](template_worker_batching.json) for
host, compiler, source hashes and the full harness output. Other verification
builds were running on this shared developer host; these small-template samples
do not establish production capacity or overall delivery/request latency.

| Template workload | Individual median | Batch median | Individual p95 | Batch p95 | Worker starts |
| --- | --- | --- | --- | --- | --- |
| Email subject and body, including former syntax checks | 3.693 ms | 0.956 ms | 3.753 ms | 1.011 ms | 4 → 1 |
| Remote URL, ten headers and body | 11.336 ms | 1.107 ms | 11.568 ms | 1.125 ms | 12 → 1 |
