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

## Measured results

Measured on 2026-10-25 on the same shared Linux host with Rust 1.98.0 and the
pinned PostgreSQL image above. Baseline application code is
`61182b17866e14b5705c760316496518aa70b907`; optimized application code and benchmark
are `2742327aef454004d8aef33bbf52bde48cc78692`. The baseline checkout used the
identical benchmark harness and response-mapping helper, with no production
optimization. The table reports 100 individually sampled calls per case.
Times and CPU are microseconds per page. CPU excludes PostgreSQL and has 10 ms
tick resolution before aggregation. Other builds/tests shared the host, so these
are reproducible fixture observations, not isolated capacity or HTTP claims.

| Mode | Objects | Padding/object | p50 before → after | p95 before → after | Process CPU before → after |
| --- | --- | --- | --- | --- | --- |
| fresh | 1 | 1,024 B | 1,182 → 941 | 1,461 → 1,082 | 300 → 400 |
| fresh | 20 | 1,024 B | 2,002 → 1,220 | 2,206 → 1,245 | 1,100 → 600 |
| fresh | 1 | 262,144 B | 4,764 → 2,647 | 5,274 → 2,786 | 3,500 → 2,100 |
| fresh | 20 | 262,144 B | 38,112 → 36,789 | 39,896 → 37,135 | 37,700 → 36,700 |
| stale | 1 | 1,024 B | 3,886 → 2,844 | 4,917 → 3,753 | 1,400 → 700 |
| stale | 20 | 1,024 B | 15,283 → 12,601 | 19,866 → 21,282 | 5,200 → 3,800 |
| stale | 1 | 262,144 B | 12,067 → 6,304 | 16,082 → 7,471 | 7,800 → 3,500 |
| stale | 20 | 262,144 B | 147,587 → 80,376 | 497,110 → 198,158 | 110,300 → 60,500 |
| personal | 1 | 1,024 B | 997 → 939 | 1,111 → 1,096 | 300 → 500 |
| personal | 20 | 1,024 B | 1,823 → 1,033 | 2,308 → 1,078 | 1,000 → 500 |
| personal | 1 | 262,144 B | 3,478 → 1,483 | 4,991 → 1,620 | 2,800 → 900 |
| personal | 20 | 262,144 B | 43,423 → 7,752 | 48,652 → 12,025 | 43,700 → 7,400 |
| disabled | 1 | 1,024 B | 1,060 → 883 | 1,259 → 943 | 500 → 300 |
| disabled | 20 | 1,024 B | 1,752 → 994 | 1,931 → 1,062 | 1,000 → 400 |
| disabled | 1 | 262,144 B | 2,888 → 934 | 3,274 → 1,001 | 2,300 → 400 |
| disabled | 20 | 262,144 B | 36,724 → 916 | 38,015 → 1,061 | 35,900 → 400 |

For the 20-object, 256 KiB disabled-definition page, p50 fell from 36,724 to
916 µs and process CPU from 35,900 to 400 µs. This specifically measures avoiding
unnecessary source hashing. Shared fresh pages still hash source data; streaming
that hash is a separate change. Stale cases still include synchronous repair.
Criterion's batched stale results were noisier than the individual samples;
no universal latency percentage is inferred from those distributions.

The query counts and checkouts were identical before and after for every case:

| Mode | Objects | Queries | Checkouts | Response bytes, 1 KiB padding | Response bytes, 256 KiB padding |
| --- | --- | --- | --- | --- | --- |
| fresh | 1 | 7 | 1 | 1,584 | 262,705 |
| fresh | 20 | 7 | 1 | 31,683 | 5,254,093 |
| stale | 1 | 16 | 2 | 1,584 | 262,705 |
| stale | 20 | 35 | 2 | 31,672 | 5,254,092 |
| personal | 1 | 7 | 1 | 1,622 | 262,743 |
| personal | 20 | 7 | 1 | 32,432 | 5,254,852 |
| disabled | 1 | 7 | 1 | 1,346 | 262,467 |
| disabled | 20 | 7 | 1 | 26,912 | 5,249,332 |

Response byte sizes above are from the optimized run; independently allocated
fixture IDs cause small byte differences between runs. The response shape did
not change. Serialization remains proportional to source bytes: the optimized
20-object, 256 KiB cases took roughly 4 ms to serialize, independently of the
enrichment optimization. No transport, compression, or client decode is included.
