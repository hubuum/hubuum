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
| Artifact block | 1 MiB |
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

One unoptimized Rust 1.99.1 diagnostic on 2026-10-30 produced:

| Measurement | Bytes |
| --- | --- |
| Complete 10,000-record segment | 46,267,575 |
| Embedded offset index | 2,157,478 |
| Artifact manifest | 1,737 |
| Sample exact/as-of encoded record | 4,407 |
| Exact physical block read, index already loaded | 1,048,576 |
| As-of physical block read, index already loaded | 1,048,576 |

The segment includes the index. These counts exclude database/catalog
allocation and index-open traffic; they cannot establish P1/P2 acceptance for
the eventual complete query pipeline.

The `small_cross_block_record_exposes_p2_budget_gap` regression places a small
record across two full 1 MiB blocks. Reading both exceeds P2's
`8 × result bytes + 1 MiB` allowance, so the reader returns budget exhaustion.
The format therefore still needs a measured packing or block-size decision
before P2 can pass across all record positions. The favorable sample above is
not evidence that this boundary case meets the target.

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
