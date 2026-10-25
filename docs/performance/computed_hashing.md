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
