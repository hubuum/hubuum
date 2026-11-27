# Offline history segment prototype

The first implementation slice of [#514](https://github.com/hubuum/hubuum/issues/514)
exercises a retained segment format and bounded exact/as-of record lookups. It
lives under `services::history::segments`, with the existing
`benchmark_support::history_segments` import retained for diagnostics. It remains
outside the storage SDK and HTTP API. It does not complete #514 or enable source retirement.

## Format and limits

All versions store uncompressed record frames. `HUBH` plus a one-byte version precedes a
length-prefixed JSON scope: one history family and stable collection UUID.
Length-prefixed JSON records preserve stable resource/version UUIDs, closed
half-open validity intervals, dependency version UUIDs, and payloads. Mixing
scopes, duplicate version UUIDs, and overlapping intervals of one resource fail
staging.

An offset index follows the records. Its four-byte little-endian length and
`HUBI` plus the matching header version terminate the stream. Version 1 stores
JSON entries. Version 2 stores one 64-byte entry per record in physical order.
Version 3 begins its index with the version key width, followed by one
dictionary-free Zstandard frame. The writer takes a prefix of each version
UUID of length 1, 2, 4, 8 or 16 bytes, choosing the smallest width
that distinguishes every version in this segment. All other fields use their
v2 field encodings. Each byte position across the entries forms one column, in
physical record order. Each column starts with its first byte and then stores
the difference from the preceding byte modulo 256, including across wraparound.
The frame contains the columns consecutively.
A shortened key locates one candidate. Exact lookups verify the full
UUID in the decoded record before returning it, including when an absent UUID
shares a key. Catalogs, query pins, and the records themselves retain full
identities. The reader checks the single-segment flag, frame boundary and
declared size before allocating space for the entries. The window and decoded
index are both bounded by that declared size.
[Zstandard frame header](https://www.rfc-editor.org/rfc/rfc8878#section-3.1.1.1)

| Bytes in a v2 entry | Value |
| --- | --- |
| 0–15, 16–31 | Resource and version UUIDs in RFC byte order |
| 32–43, 44–55 | Start and end, each signed 64-bit Unix seconds and unsigned 32-bit subsecond nanoseconds, little-endian |
| 56–59, 60–63 | Unsigned 32-bit frame offset and length including prefix, little-endian |

The reader reconstructs the full index on open. The count comes from the
decoded length divided by the entry width, 64 bytes in v2 and 48 bytes plus
the key width in v3. Readers reject partial entries, duplicate version keys, invalid
intervals and discontinuous record boundaries.
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
| Encoded index | 4 MiB for v1, 640,000 bytes for v2, 1 MiB stored and 640,000 bytes decoded for v3 |
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

Catalog producers obtain the verified format from `HistorySegment::format_version`.
Upgrade archive readers and recovery binaries before publishing version-3
segments. Both earlier formats can stay in place.

## PostgreSQL catalog, index and manifest size

The catalog, embedded index and artifact manifests exceeded the P1 allowance at
both sizes in this collection-history fixture. The calculation pairs each
PostgreSQL descriptor with the exact version-2 segment it references. On
2026-11-27, five fresh paired runs after one warm-up produced identical byte
counts on PostgreSQL 18.7 with default TOAST compression. Each table entry is
the value observed in all five samples, including the median and p95.

| Physical or encoded bytes | 10,000 versions | 100,000 versions |
| --- | ---: | ---: |
| Framed records | 15,715,400 | 157,350,300 |
| PostgreSQL catalog table including TOAST | 1,712,128 | 16,711,680 |
| PostgreSQL catalog indexes | 16,384 | 57,344 |
| Embedded segment indexes | 640,000 | 6,400,000 |
| Artifact manifests | 22,500 | 225,000 |
| Metadata lower bound | 2,391,012 | 23,394,024 |
| P1 allowance (5% of framed record bytes plus 1 MiB) | 1,834,346 | 8,916,091 |

Even counting the entire segment in the denominator gives allowances of only
1,866,851 and 9,241,141 bytes. At 100,000 versions the native PostgreSQL table
and indexes alone exceed either allowance. The embedded index also repeats the
version and interval already in the catalog.

Each resource has 100 closed versions and one independently verified, published
segment. Each small collection description contains 256 bytes. Every hundredth
contains 64 KiB. Resource and version UUIDs are freshly generated. The
diagnostic copies the migrated catalog definition and indexes into a temporary
PostgreSQL table, inserts the production JSONB representation, and runs
`ANALYZE` before measuring the table, TOAST, and indexes. Segment and manifest
counts come from the same publications in the memory artifact store.

Run against a disposable, migrated PostgreSQL database with
`HUBUUM_DATABASE_URL` set. Split-role deployments also need the migration
connection and owner settings used by the repository test runner.

```bash
cargo test --locked -p hubuum --features integration-test-support,embedded-migrations \
  --lib postgres_archive_catalog_allocation -- --ignored --nocapture --test-threads=1
```

The original v2 measurements above count encoded manifests and exclude other
provider allocations. The current diagnostic also copies the migrated
PostgreSQL destination, attempt and chunk tables into the temporary schema.
It inserts the published manifests and the verified bytes of each block.
It counts the physical destination and attempt relations, including manifests
and indexes. For the chunk relation it subtracts the physical content column
sizes from the full table, TOAST and index allocation. The difference includes
tuple overhead and unused page space. The embedded index is counted
separately at its encoded size. Each manifest is counted once. The sum of
logical chunk lengths must equal the published segments' lengths.

The diagnostic compares the current JSONB catalog with a candidate binary
encoding in a separate temporary table. It verifies that each candidate
decodes to the complete original catalog, including all version identities
and content digests. These tables model a fresh set of published rows. Live
update bloat, query captures, dependent history families and the class-skewed
`archive-exact-v1` deployment still require measurement.

## Compressed index and packed catalog measurements

At commit `bb069f25`, a fresh v3 open of the 10,000-record single-segment
fixture read 295,444 bytes, including a 24,489-byte manifest. The segment
occupied 44,311,147 bytes including a 201,050-byte index. Both cold exact and
cold as-of reads returned the 4,407-byte record after reading 360,980 bytes
in total. The v2 result was 800,146 bytes. These are segment reads; the
application also reads the catalog and validates the capture.

On PostgreSQL 18.7, five paired runs after one warm-up used new resource,
version and artifact UUIDs in the collection fixture. Each entry below is the
median and nearest-rank p95 in bytes. Total metadata includes the measured
catalog, embedded index and PostgreSQL artifact overhead described above.

| Allocation (median / p95) | 10,000 versions | 100,000 versions |
| --- | ---: | ---: |
| JSONB catalog | 1,728,512 / 1,728,512 | 16,769,024 / 16,769,024 |
| Candidate packed catalog | 679,936 / 679,936 | 6,422,528 / 6,422,528 |
| Embedded v3 index | 37,486 / 38,261 | 367,296 / 367,591 |
| PostgreSQL artifacts including manifests | 409,773 / 410,711 | 2,011,525 / 2,029,084 |
| Total with JSONB catalog | 2,175,771 / 2,176,029 | 19,147,646 / 19,164,412 |
| Total with packed catalog | 1,127,195 / 1,127,453 | 8,801,150 / 8,817,916 |
| P1 allowance | 1,834,346 | 8,916,091 |

| Packed total, bytes | Run 1 | Run 2 | Run 3 | Run 4 | Run 5 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10,000 versions | 1,127,114 | 1,127,195 | 1,127,453 | 1,127,352 | 1,127,066 |
| 100,000 versions | 8,784,622 | 8,817,916 | 8,801,644 | 8,801,150 | 8,792,946 |

At 100,000 versions, 98,175 bytes remained in the largest sample. Additional
online state or a different resource distribution can consume this margin.
All five JSONB totals exceeded the allowance at both sizes. Packed catalog
storage in this diagnostic is experimental.

## Remaining acceptance

Project #3 tracks the remaining #514 work on `project/workload-lifecycle`.
Production qualification still needs worker RSS on constrained deployments,
complete online catalog/index allocation at 10,000 and 100,000 versions,
end-to-end cold reads within P2, and subset-rewrite measurements. See the
[lifecycle budgets](lifecycle_budgets.md) for the profiles and pass rules.
The fixture holds artifact bytes in memory. Durable storage and reader
compatibility across future format versions require separate evidence before
production deployment or primary-copy retirement.

## Local mixed-segment rewrite

Run the full 10,000- and 100,000-version diagnostic with the command below.

```bash
cargo test --lib one_percent_subset_rewrite_traffic --locked -- --ignored --nocapture --test-threads=1
```

The fixture distributes 10,000 objects over 128 classes and 32 collections.
Eight classes own 60% of objects. Each object has one version in the smaller
run and ten in the larger run. Ordinary payloads contain 256 bytes of filler
and every hundredth object contains 64 KiB. Each collection is segmented at
the 10,000-frame limit. The selected offset rotates in each group of 100
versions. Every touched segment produces a separate traffic record.

After a warm-up, five fresh runs generated new UUIDs and independently
verified the same logical workload. All traffic quantities are bytes.

| Versions | Touched segments | Selected | Source open, median | Source scan, median | Destination written, median | Destination verification, median | Largest segment, p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 32 of 32 | 100 | 2,861,033 | 12,712,710 | 12,694,361 | 15,481,230 | 4,623,784 |
| 100,000 | 36 of 36 | 1,000 | 4,342,760 | 126,435,512 | 125,691,484 | 129,739,600 | 45,516,363 |

| Run | 10k source plus destination | 10k maximum segment | 100k source plus destination | 100k maximum segment |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 28,271,256 | 4,622,985 | 256,469,747 | 45,516,363 |
| 2 | 28,266,506 | 4,623,784 | 256,469,750 | 45,516,362 |
| 3 | 28,268,086 | 4,622,985 | 256,469,774 | 45,516,363 |
| 4 | 28,268,634 | 4,623,513 | 256,469,750 | 45,516,363 |
| 5 | 28,268,098 | 4,622,985 | 256,469,781 | 45,516,363 |
| Median | 28,268,098 | 4,622,985 | 256,469,750 | 45,516,363 |
| p95 | 28,271,256 | 4,623,784 | 256,469,781 | 45,516,363 |

With five runs, nearest-rank p95 is the largest observation. Source open
includes the published manifest and verified framing/index blocks. Source scan
charges each intersecting record block at its actual physical length.
Destination writes include the sealed manifest. Verification reads back the
manifest and every destination block and reopens its index. Each segment stayed
below the P8 ceiling of 134,217,728 bytes for source open, source scan and
destination writes combined. Subsequent independent lookups compared every
complete surviving record and checked that each selected UUID was absent.

The fixture uses the memory artifact adapter and excludes database/WAL,
provider retries, process RSS, publication, superseded-copy cleanup and restore.
The named deployment still needs those measurements. A staged replacement alone
supplies no authority to switch catalogs or erase a source.

The rewrite entry point takes an admitted output receipt. Its intent binds the
exact source manifest digest and sorted resource/version pairs to the new
attempt. A caller can retain that intent before writing and reconcile the same
attempt after a restart. The provider checks its owner when staging, sealing
and fencing the replacement. A different selection cannot use the receipt.
If the worker loses a seal or publication reply, it can reopen the provider and
resolve the retained intent. Recovery reads back the sealed manifest and blocks,
checks each surviving interval against the replacement index and reconstructs
the expected stream digest from the source frames and the retained header and
index. This also checks payloads and dependencies when the logical identities
match. The worker rereads the provider phase under the same owner before
returning the verified output and any existing publication reference. A fenced
attempt yields a conflict. Recovery reports the extra destination reads in its
verification byte count. The five measurements above cover fresh writes.
Only the owner responsible for settlement can fence an abandoned attempt,
so a failed writer cannot invalidate a concurrent successful write.
Fencing prevents subsequent publication. Physical cleanup requires separate evidence.
