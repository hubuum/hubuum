# Computed object enrichment

The `computed_enrichment_criterion` target measures a bounded server-side page
through the PostgreSQL storage capability. It does not measure HTTP latency,
authorization, or client deserialization. Source object loading, fixture setup,
cache invalidation, and cloning the input page happen outside the timed region.
The measured region includes the enrichment snapshot and, for stale pages, the
synchronous repair transactions. Serialization is reported separately using the
production API response mapping.

## Reproduction

```sh
cargo bench --bench computed_enrichment_criterion \
  --features postgres-bench,query-capture -- --noplot
```

The fixture starts a disposable PostgreSQL container pinned to
`postgres:18.6-alpine3.24@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`.
Cases sample one or 20 objects from a 20-object fixture with eight `first_non_null` definitions, a short selected
string, and either 1,024 or 262,144 bytes of unrelated string padding per object.
Cases cover fresh shared materializations, deliberately removed shared
materializations, personal-only definitions for one owner, and disabled shared
definitions. Stale preparation runs before **each** measured iteration. Fresh
cases are warmed before measurement. The target prints query counts, connection
checkouts, exact serialized response bytes, and separate serialization time.
It also samples 100 individual enrichment calls for p50/p95 latency. On Linux,
`/proc/self/stat` and `getconf CLK_TCK` provide approximate process CPU time,
excluding fixture preparation and database-server CPU. CPU tick resolution is
printed; short operations require interpreting the aggregate, not single calls.

The optimization preserves the source hash contract, evaluation limits,
authorization boundary, cached result-type/error validation, and snapshot
revision/staleness semantics. Definitions and validation metadata are prepared
lazily once per class and owner scope. Cache JSON is consumed; only repeated
object identities need a copy. Read repair reloads authoritative data using typed
identities, with the same class-before-object transaction lock order.

## Interpreting the measurements

Read-query counts are independent of page size within one class. Repair reads
scale with the configured batch count; each repaired object still needs its own
materialization upsert. Regression tests cover fresh pages of one and 20 objects,
repeated identities, and bounded repair batches.

Use the [computed stage spans](../tracing.md) to separate hashing and evaluation
from pool waits, queries, locks, repair, and response serialization in a deployed
server. Spans export no object contents. Wall times include database waits; the separate
process CPU samples do not include PostgreSQL CPU. No allocator profiler is used. Removed allocations
are established by ownership in the implementation; their byte counts are not
presented as measured heap savings. Smaller operation time is not automatically
an equal improvement in end-to-end HTTP latency.
