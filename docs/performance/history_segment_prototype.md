# Offline history segment prototype

The first implementation slice of [#514](https://github.com/hubuum/hubuum/issues/514)
exercises a retained segment format and bounded exact/as-of record lookups. It
lives under `services::history::segments`, with the existing
`benchmark_support::history_segments` import retained for diagnostics. It remains
outside the storage SDK and HTTP API. It does not complete #514 or enable source retirement.

## Format and limits

Both versions are uncompressed. `HUBH` plus a one-byte version precedes a
length-prefixed JSON scope: one history family and stable collection UUID.
Length-prefixed JSON records preserve stable resource/version UUIDs, closed
half-open validity intervals, dependency version UUIDs, and payloads. Mixing
scopes, duplicate version UUIDs, and overlapping intervals of one resource fail
staging.

An offset index follows the records. Its four-byte little-endian length and
`HUBI` plus the matching header version terminate the stream. Version 1 stores
JSON entries. Version 2 stores one 64-byte entry per record in physical order.

| Bytes in a v2 entry | Value |
| --- | --- |
| 0–15, 16–31 | Resource and version UUIDs in RFC byte order |
| 32–43, 44–55 | Start and end, each signed 64-bit Unix seconds and unsigned 32-bit subsecond nanoseconds, little-endian |
| 56–59, 60–63 | Unsigned 32-bit frame offset and length including prefix, little-endian |

The reader reconstructs the full index on open. The count comes from the
encoded length, which must be a multiple of 64 in v2. Both versions validate
UUIDs, intervals, unique versions and contiguous record boundaries.
The existing artifact manifest binds every byte,
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
| Artifact block | 64 KiB for new writes, retained reader ceiling 1 MiB |
| Records per segment | 10,000 |
| Encoded index | 4 MiB for v1, 640,000 bytes for v2 |
| Scope header | 512 bytes |
| Dependency version references per record | 128 distinct, non-self references |

These finite format-reader ceilings are prototype choices. They do not replace
the [lifecycle budgets](lifecycle_budgets.md), including H4's smaller capture
batches, task-level H5 limits, or production admission/accounting. The producer
receives one record at a time; the caller still owns input capture and payload
allocation bounds.

## Published-reference reader foundation

`RetainedHistorySegment::open` accepts the existing trusted catalog
`ArtifactReference`, resolves durable publication, checks its exact immutable
location and manifest digest, and reconstructs the index from verified blocks.
It does not need the producer's in-memory verification proof. Serialized reference
roundtrips exercise exact and half-open as-of reads, including the original
1 MiB and 512 KiB v1 blocks.
Unpublished attempts, changed locations and wrong manifest digests fail closed.

Before external I/O, the reader requires the artifact contract's full 4 MiB
retained manifest envelope to fit the remaining budget. Successful fetches charge
actual manifest bytes; failed fetches charge the full envelope. Every fetched
index/data block also charges the same cumulative budget. History decoding still requires a manifest
within 256 KiB and a segment within 64 MiB. This conservative charge is explicit;
it does not establish P2 acceptance for the complete query pipeline.

The segment reader establishes inclusion within one published segment. The
[gated collection-history service](../history_archive_reads.md) owns catalog
selection, capture, authorization, result lifetime and coverage.

## Reproduction

Run the offline diagnostic with 1–10,000 records:

```bash
cargo run --locked --example history_segments -- 10000
```

The fixture streams 4 KiB JSON payloads into the in-memory artifact store. It
measures exact and as-of reads with the index already loaded, then publishes
the segment in that store and repeats each query from a fresh reader. It
reports encoded sizes and physical bytes fetched. UUIDs vary between runs and
have constant encoded widths.

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

Later version-1 writes used 512 KiB blocks. Two full blocks then cost 1 MiB, so a small
record crossing a block boundary fits P2's `8 × result bytes + 1 MiB`
exact-read allowance. Larger contiguous frames add less than two blocks of
rounding to their framed size, including the four-byte length prefix. The
`cross_block_reads_fit_p2_budget` regressions check exact/as-of reads where the
prefix or payload crosses a boundary, including block-sized and maximum-sized
frames. Each read receives the smaller of P2's absolute and proportional limits;
full physical blocks are still charged.

The reader retains version 1's independent 1 MiB block ceiling and reads the
original 1 MiB-block segments. The artifact manifest binds the block size. Existing segments can
still exhaust the smaller exact-read allowance at a boundary. They remain
readable with a sufficient explicit budget; changing the write policy does not
waive physical-read accounting or turn exhaustion into a complete empty result.

These checks cover selected-frame traffic with the index already loaded. The
smaller write blocks increase the manifest entry count from 45 to 89 at this
scale. The table includes that cost.

## Cold index and record reads

Opening a published segment reads its manifest, scope header, footer and index.
The header and footer each have a nine-byte framing marker. The reader keeps
the verified blocks containing those markers until it has assembled the index.
This avoids fetching a block again when it also contains scope or index bytes,
including when the footer crosses a block boundary. A fresh open starts a new
set of reads. Each record lookup independently verifies and charges its blocks.

On 2026-11-27 the 10,000-record fixture above produced these physical byte
counts for the same 4,407-byte result and 512 KiB write blocks.

| Operation | Before block reuse | With block reuse |
| --- | ---: | ---: |
| Open the published index, including its manifest | 3,409,511 | 2,754,992 |
| Open and read the exact record | 3,933,799 | 3,279,280 |
| Read the exact record with the index loaded | 524,288 | 524,288 |

A separate cold as-of lookup also fetched 3,279,280 bytes. The applicable P2
limits for this result are 1,083,832 bytes for exact and 2,167,664 bytes for
as-of. Both cold reads exceed those limits. The embedded index alone occupies
2,157,478 bytes. These measurements include the fetched manifest, framing,
index and selected data blocks. Database catalog traffic and dependency reads
remain outside this fixture.

## Compact index measurements

On the same 10,000-record fixture, a fresh version-2 reader returned the
same 4,407-byte record for exact and as-of lookups. Each lookup used a new
manifest and index. Both versions verified the full index.

| Bytes | v1, 512 KiB blocks | v2, 64 KiB blocks |
| --- | ---: | ---: |
| Complete segment | 46,267,575 | 44,750,097 |
| Embedded index | 2,157,478 | 640,000 |
| Manifest | 3,321 | 24,705 |
| Cold open including manifest | 2,754,992 | 734,610 |
| Cold exact lookup | 3,279,280 | 800,146 |
| Cold as-of lookup | 3,279,280 | 800,146 |
| Record lookup with loaded index | 524,288 | 65,536 |

The smaller blocks increased manifest traffic by 21,384 bytes. The complete
cold lookup saved 2,479,134 bytes (75.6%). This leaves 283,686 bytes of the
exact-read allowance and 1,367,518 bytes of the as-of allowance for other
work on this fixture. Storage round trips, database catalog traffic,
dependency resolution and worker memory need separate measurement.

Upgrade archive readers and recovery binaries before publishing version-2
segments. Catalog producers can obtain the verified format from
`HistorySegment::format_version`. Version-1 segments can stay in place.

## Remaining acceptance

Project #3 tracks the remaining #514 work on `project/workload-lifecycle`.
Production qualification still needs worker RSS on constrained deployments,
complete online catalog/index allocation at 10,000 and 100,000 versions,
end-to-end cold reads within P2, and subset-rewrite measurements. See the
[lifecycle budgets](lifecycle_budgets.md) for the profiles and pass rules.
The fixture holds artifact bytes in memory. Durable storage and reader
compatibility across future format versions require separate evidence before
production deployment or primary-copy retirement.
