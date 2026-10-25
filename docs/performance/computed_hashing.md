# Computed source hashing

Computed shared-cache freshness still requires matching class, evaluation
revision, and the canonical SHA-256 of the read's source document. Cached result
types, enabled keys, and error/null consistency remain validated. The optimization
streams canonical JSON tokens into SHA-256 and formats the final 64-character
hexadecimal digest in one allocation. It removes the full canonical-document
buffer; hashing still takes time proportional to document size.

An 8 KiB `std::io::BufWriter` batches the small fragments emitted by JSON escaping
and delimiters before updating the hasher. Its storage is bounded independently
of document size; sorted-key scratch space still grows with object width.

Canonicalization preserves sorted object keys, array order, compact delimiters,
JSON string escaping, and the existing numeric tokens. It deliberately does not
introduce numeric normalization. PostgreSQL JSONB may normalize input number
representations during persistence; parity tests compare Rust and
`hubuum_computed_source_sha256` against the **same persisted JSONB document**.
Other regressions compare the old and new Rust digests and retain computed-sort
fresh-cache and stale-fallback behavior.

## Reproducing the hash measurement

```sh
cargo bench --bench computed_hash_criterion --features postgres-bench -- --noplot
```

The benchmark compares the production function with the exact pre-change
implementation from `61182b17866e14b5705c760316496518aa70b907` in one process.
Fixtures contain ordinary strings, heavily escaped strings, or many small nested
values, at three input-size settings. Output includes actual serialized size.
Fixture construction, digest-equivalence assertions, and allocation samples are
outside Criterion's timed region.

A wrapper around the system allocator counts allocation/reallocation requests
for a separate synchronous call. `allocated_bytes` is cumulative requested bytes,
including reallocations, not peak live heap or resident memory. Input storage is
excluded; the returned digest is included. Timing uses the same allocator wrapper
with counting disabled for both implementations. These hash-only timings exclude
object loading, SQL, locks, authorization, evaluation, and response serialization.
The computed-enrichment benchmark measures the larger server operation separately.

## Constant-time freshness assessment

A timestamp is not a replacement for content evidence. The existing object
resource revision is also not currently a persisted cache freshness contract:
materialization stores the source hash, and its input carries the object ID, class
ID, and data rather than a proof that a particular data generation was evaluated.
Resource revisions cover metadata changes as well as data changes, and privileged
restore sessions deliberately restore revision values from artifacts.

| Mutation path | Current integrity boundary | Requirement for a future marker |
| --- | --- | --- |
| Creation and ordinary updates | Materialization and object mutation share a transaction and class lock. | Assign a data generation and store that exact generation with the evaluated snapshot. |
| JSON Patch | Patch validation and resulting data persistence use the same locked object transaction. | Advance the marker for the final authoritative data, including multiple operations. |
| Imports | Imported writes use the object operation and preserve imported timestamp policy. | Preserve the generation invariant independently of imported timestamps and overwrite modes. |
| Class moves | Class and object locks protect reclassification; cache freshness checks class identity. | Keep class identity and evaluation revision checks even if the data generation is unchanged. |
| Definition changes | Evaluation revision changes and rebuild work invalidate the prior definition set. | Keep the separate evaluation revision and cached-value validation. |
| Restore | Privileged restore settings replay resource revisions; derived computations are rebuilt. | Discard old markers, initialize a new generation domain, and rebuild before accepting legacy cache rows. |
| Supported SQL data writes | Revision triggers guard ordinary object mutations; content hashing detects changed source data without relying on a writer updating a cache marker. | Use a database-enforced data marker, with documented treatment of owner/migrator operations and trigger bypass. |
| Existing cache rows | Stored hashes remain readable immediately after upgrade. | Backfill or mark every legacy materialization stale; coordinate sorting SQL, backups, and rollback. |

A data-only generation maintained by a database trigger could support a future
constant-time check, but it needs an explicit migration, typed generation-bearing
materialization input, atomic cache association, restore policy, and mixed-version
upgrade behavior. Simply comparing the existing revision or timestamp would skip
that work. This change retains content hashing and adds no stored state, migration,
backfill, SQL write, or new public API. Existing writers use the streamed hash
where they already computed a digest; hash timing therefore also describes that
part of write cost. PostgreSQL's canonical hashing function is unchanged.

## Pinned hash measurements

Measured on 2026-10-25 at `d53d206e` (the streaming implementation is
`ea287f89`), using the repository's optimized release profile on a shared Linux
host. Legacy code is pinned to `61182b17866e14b5705c760316496518aa70b907`.
Each latency is Criterion's point estimate from 30 samples after a one-second
warmup, with a three-second measurement window. These are hash-function costs,
not HTTP latency. The isolated allocation call excludes input storage.

Process CPU is measured separately from `/proc/self/stat` over 10,000 calls for
the 1 KiB setting and 100 calls for larger settings. The clock has 10 ms ticks;
these aggregate samples are coarse estimates, not per-call CPU timers. Arrows
show legacy followed by buffered streaming.

| Shape | Input setting | Serialized bytes | Latency (µs) | Process CPU/call (µs) | Allocations | Requested bytes |
| --- | --- | --- | --- | --- | --- | --- |
| string | 1,024 | 1,066 | 10.96 → 8.94 | 10 → 9 | 46 → 3 | 7,122 → 8,288 |
| escaped | 1,024 | 1,659 | 17.52 → 17.43 | 17 → 22 | 48 → 3 | 9,711 → 8,288 |
| wide | 1,024 | 887 | 15.12 → 9.92 | 18 → 13 | 172 → 3 | 19,048 → 8,512 |
| string | 262,144 | 262,186 | 1,964.30 → 1,858.40 | 1,900 → 1,800 | 46 → 3 | 1,573,842 → 8,288 |
| escaped | 262,144 | 419,451 | 3,739.90 → 3,996.20 | 3,700 → 3,900 | 56 → 3 | 2,307,567 → 8,288 |
| wide | 262,144 | 244,651 | 3,651.30 → 3,073.70 | 3,800 → 3,100 | 32,820 → 3 | 4,784,488 → 73,792 |
| string | 1,048,576 | 1,048,618 | 7,735.30 → 7,388.40 | 7,800 → 7,400 | 46 → 3 | 6,292,434 → 8,288 |
| escaped | 1,048,576 | 1,677,747 | 13,884.00 → 15,804.00 | 13,900 → 15,900 | 58 → 3 | 9,228,183 → 8,288 |
| wide | 1,048,576 | 1,004,699 | 15,996.00 → 14,270.00 | 16,100 → 14,000 | 131,126 → 3 | 19,136,872 → 270,400 |

Streaming removes the document-sized temporary buffer and most allocation calls.
The fixed 8 KiB buffer slightly increases requested bytes for the smallest plain
string fixture. Wide objects still allocate sorted-key scratch space. Large
escaped strings show a latency/CPU tradeoff: the 1 MiB input setting took about
14% longer despite reducing cumulative requested bytes from 9.23 MB to 8.29 KB.
Ordinary large strings and wide objects improved in this run. This is not a
universal CPU speedup. No SQL write or additional persisted state is introduced;
writers that already hash data share these same shape-dependent costs.
