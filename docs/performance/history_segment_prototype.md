# Offline history segment prototype

The first implementation slice of [#514](https://github.com/hubuum/hubuum/issues/514)
exercises a retained segment format and bounded exact/as-of record lookups. It
lives under `benchmark_support::history_segments`, outside the storage SDK and
HTTP API. It does not complete #514 or enable source retirement.

## Format and limits

Version 1 is uncompressed. `HUBH` plus a one-byte version precedes a
length-prefixed JSON scope: one history family and stable collection UUID.
Length-prefixed JSON records preserve stable resource/version UUIDs, closed
half-open validity intervals, dependency version UUIDs, and payloads. Mixing
scopes, duplicate version UUIDs, and overlapping intervals of one resource fail
staging.

A JSON offset index follows the records. Its length and `HUBI` plus a one-byte
version terminate the stream. The existing artifact manifest binds every byte,
including the header, framing, index, and footer. The producer uses the existing
bounded channel and independent full read-back verification; it does not
publish the artifact. Catalog owners must fence publication separately.

Readers reconstruct the index from verified blocks, reject unknown versions,
validate contiguous frame coverage, and bind each decoded record to its indexed
identity, interval, and segment scope. They verify every fetched block before
decoding. The caller supplies cumulative physical-read accounting: a repeated
read is charged again, including bytes outside the selected record in a fetched
block. A lookup miss describes only this segment, not complete query coverage.

| Input | Prototype ceiling |
| --- | --- |
| Complete segment, including index and framing | 64 MiB |
| Encoded record, including its four-byte length | 4 MiB |
| Artifact block | 512 KiB for new writes; retained reader ceiling 1 MiB |
| Records per segment | 10,000 |
| Encoded index | 4 MiB |
| Scope header | 512 bytes |
| Dependency version references per record | 128 distinct, non-self references |

These finite format-reader ceilings are prototype choices. They do not replace
the [lifecycle budgets](lifecycle_budgets.md), including H4's smaller capture
batches, task-level H5 limits, or production admission/accounting. The producer
receives one record at a time; the caller still owns input capture and payload
allocation bounds.

## Reproduction

Run the offline diagnostic with 1–10,000 records:

```bash
cargo run --locked --example history_segments -- 10000
```

The fixture streams 4 KiB JSON payloads into the existing in-memory artifact
store. It reports encoded segment/index/manifest sizes, an exact result size,
physical block bytes for exact/as-of reads with a loaded index. UUIDs vary
between runs; their encoded widths are constant. This is not a throughput or
latency benchmark or an acceptance result.

One unoptimized Rust 1.99.1 diagnostic with 512 KiB write blocks on
2026-10-31 produced:

| Measurement | Bytes |
| --- | --- |
| Complete 10,000-record segment | 46,267,575 |
| Embedded offset index | 2,157,478 |
| Artifact manifest | 3,321 |
| Sample exact/as-of encoded record | 4,407 |
| Exact physical block read, index already loaded | 524,288 |
| As-of physical block read, index already loaded | 524,288 |

The segment includes the index. These counts exclude database/catalog
allocation and index-open traffic; they cannot establish P1/P2 acceptance for
the eventual complete query pipeline.

New writes use 512 KiB blocks. Two full blocks then cost 1 MiB, so a small
record crossing a block boundary fits P2's `8 × result bytes + 1 MiB`
exact-read allowance. Larger contiguous frames add less than two blocks of
rounding to their framed size, including the four-byte length prefix. The
`cross_block_reads_fit_p2_budget` regressions check exact/as-of reads where the
prefix or payload crosses a boundary, including block-sized and maximum-sized
frames. Each read receives the smaller of P2's absolute and proportional limits;
full physical blocks are still charged.

The reader retains version 1's independent 1 MiB block ceiling and reads the
original 1 MiB-block segments. The segment framing and embedded index are
unchanged; block size is bound by the artifact manifest. Existing segments can
still exhaust the smaller exact-read allowance at a boundary. They remain
readable with a sufficient explicit budget; changing the write policy does not
waive physical-read accounting or turn exhaustion into a complete empty result.

These checks cover selected-frame traffic with the index already loaded. Cold
index loading, dependency reads, and the complete production query pipeline
remain unqualified. The smaller write blocks increase the manifest entry count
from 45 to 89 at this scale; the table includes that cost.

## Remaining acceptance

Production integration still needs persisted catalog locations and scope
evidence, bounded hot-input capture, generation-checked pins, task/result
lifetime, live authorization and erasure fencing, dependency resolution, and
the typed query-coverage contract in #545. A segment-local miss must never be
promoted to a complete empty query without that planning evidence.

The diagnostic does not qualify worker RSS, constrained deployments, 100,000
retained versions, complete online catalog/index allocation, cold index load
costs, subset rewrites, or all H1–H5/P1/P2 targets. The in-memory fixture retains
artifact bytes; it cannot establish bounded production storage or durability.
Retained-reader compatibility beyond this initial version remains an explicit
gate before any format is deployed or primary data is retired.

This slice has no user-facing behavior or compatibility change and needs no
changelog entry. It is independently reviewable against `main`; it does not
depend on the recovery, audit-contract, receipt, or scheduler PR stacks.
