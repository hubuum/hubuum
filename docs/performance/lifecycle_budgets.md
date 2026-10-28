# Lifecycle inventory and initial budgets

Budget revision **1**, dated **2026-10-27**, for
[#504](https://github.com/hubuum/hubuum/issues/504). These are initial engineering
ceilings and acceptance targets, not production defaults or a claim that future
archive paths already meet them. The [delivery plan](../lifecycle_delivery_plan.md)
tracks their consumers.

## Storage inventory and restore dependencies

The inventory enumerates every ordinary table in PostgreSQL's `public` schema,
including empty tables. It reports rows, logical encoded bytes, largest encoded
row, table allocation and index allocation separately. This discovers new tables
without a manually maintained SQL allowlist. Names appear only in offline
reports, never metric labels. Payloads and resource/principal identifiers never
leave the aggregate query.

| Family | Current storage and dependencies |
| --- | --- |
| Tasks/outcomes | `tasks`, `import_task_results`, `remote_call_results`, `schema_profile_outputs`, `schema_validation_work`, `schema_impact_findings`, `schema_repair_reports`; preserve provenance, deduplication and authorization semantics. |
| Artifacts | `export_task_outputs` stores JSON/text and metadata; `backup_task_outputs` stores bytes. Whole-output compatibility paths are #508's baseline. |
| Resource history | `collections_history`, `hubuumclass_history`, `hubuumobject_history`, `hubuumclass_relation_history`, `hubuumobject_relation_history`; preserve intervals, memberships and endpoints. |
| Definition/schema history | `computed_field_history`, `export_templates_history`, `remote_targets_history`, `class_schema_history`; retain historical definitions, `class_schema_revisions`, schema state and object evidence. |
| Audit/delivery history | `events`, `event_deliveries`; event identity, actor attribution, sink/subscription dependencies and remote-effect recovery remain significant. |
| Restore/staging | `restore_artifact_chunks`, `restore_jobs`, `restore_success_receipts`, `system_maintenance`, authorization generations; physical stage ownership has a different lifetime from tasks. |
| Live restore dependencies | Principals/users/service accounts, groups/memberships, identity scopes, permissions/hierarchy, resources, definitions, templates, targets, subscriptions, sinks and grants. Existing backup rules still determine inclusion/exclusion. |
| Derived structures | Computed indexes, relation closure, schema indexes and query usage declarations; measure them even when restore rebuilds them. |
| Local spools | Backup replay and verification/restore files lie outside PostgreSQL. Measure peak temporary-directory allocation and process RSS separately. Database inventory does not imply zero spool use. |
| Future structures | Catalogs, block manifests, provider caches and independent deletion authority are not measured yet; consumers must add their measurements. |

Logical bytes mean `octet_length(to_jsonb(row)::text)` over complete rows in one
repeatable-read snapshot, including metadata and field names; `bytea` is hex
encoded. This is an inventory encoding, **not** the future archive/backup stream.
Measure those final stream sizes separately. `pg_table_size` includes TOAST and
auxiliary forks; `pg_indexes_size` supplies the relation's indexes. Their sum is
allocation, not reclaimable space. Physical sizes are not MVCC snapshots; use
an otherwise idle disposable database. WAL is cluster-wide and is contaminated
when other writers run concurrently.

## Named environments and fixtures

Inputs are `scale-benchmarks/lifecycle/dataset-v1.toml` and `workload-v1.toml`.
The dataset reuses the scale generator's `large` name with a custom specification;
identify it by the full specification and manifest digest, never `large` alone.

It contains 32 collections, 128 classes, 10k objects, 20k object relations, 1,000
available principals, definition/schema history and 10k tasks. Eight classes
contain 60% of objects. The overlay adds 256 payload bytes per ordinary object
and 64 KiB every hundredth object, retaining prior versions through real triggers.
Payloads are deterministic MD5-derived hexadecimal fixture text, not production
data or integrity evidence. Every hundredth task receives a 1 MiB text output:
100 MiB in total.

Shape and payload recipes are deterministic. Generated timestamps, credential
salts and PostgreSQL allocation can vary between loads; compare row counts and
the manifest before interpreting small byte differences. The fixture validates
matching corpus/task counts and caps both added object payload and task outputs
at 1 GiB before the adapter starts loading.

Task recipes use 32 submitting principals; one supplies 80% of arrivals. Every
tenth task takes 30 seconds and others 100 ms. The storage fixture records
terminal timestamps, **not measured scheduler performance**. #507 must execute
the recipe with real workers, also at 1k and 100k backlog sizes, all-overdue
arrivals and continuously replenished work. Count starts and occupancy separately.

| Profile | Database and worker resources | Topology/storage | Workload |
| --- | --- | --- | --- |
| `lifecycle-local-v1` | PostgreSQL 18.6, 2 vCPU/2 GiB DB; worker 2 vCPU/512 MiB RSS; pool 8 | One API/worker replica, one slot; local SSD, same-host DB | Checked-in dataset/recipe; storage baseline and single-worker fallback. |
| `lifecycle-shared-v1` | PostgreSQL 18.6, 4 vCPU/4 GiB DB; each worker 2 vCPU/512 MiB RSS; pool 8 per process | Two API/two worker replicas, two slots each; SSD, DB RTT at most 2 ms | Same dataset; 1k/10k/100k backlogs, 80% principal skew, 30 s/100 ms mix, overdue saturation. |
| `artifact-slow-v1` | Same as shared; four concurrent producers | Directly readable destination at 1 MiB/s per stream, then interrupted; same bound for read-back | 1 GiB output, 4 MiB single encoded row, 64 KiB large rows, empty output, cancellation, spool loss. |
| `archive-exact-v1` | Same as shared | Initially uncompressed 64 MiB segments, 1 MiB independently verified blocks | 10k/100k retained versions, skewed classes, exact/as-of reads, 1% subset rewrite. |

Record versions/features, CPU/memory limits, filesystem/storage, worker settings,
DB settings and RTT with every acceptance run. Other environments are diagnostic
until equivalence is justified. Single-worker operation and nonpreemptible long
tasks cannot promise prompt start latency.

## Hard resource ceilings

Exceeding a ceiling must reject or stop cleanly before publication, regardless of
throughput. RSS includes process baseline; a two-slot worker still has one
512 MiB ceiling. These are profile contracts, implemented by their consumers.

| ID | Profiles/issues | Ceiling | Measurement and pass rule |
| --- | --- | --- | --- |
| H1 | All; #508/#514/#515/#519 | 512 MiB worker RSS | Process-only RSS at 20 ms plus allocator/high-water instrumentation; both below the ceiling with concurrency, slow consumers and largest rows. Report DB memory separately. |
| H2 | Artifacts/archives; #508/#514/#516 | 2 GiB resident spool/staging per worker; 8 GiB deployment-wide | Count live files, reserved chunks and pending cleanup obligations at every transition. Release accounting only after physical cleanup. |
| H3 | Artifacts/archives; #508/#514 | 1 GiB artifact, 4 MiB encoded row, 1 MiB block, 1,024 blocks, 256 KiB manifest | Exact-limit and one-unit-overflow tests include framing overhead. Rows may span verified blocks with bounded decode state. No whole artifact/hash-array allocation. |
| H4 | Archives; #513/#514/#515/#518 | 1,000 rows and 16 MiB input per claimed batch, whichever comes first | Enforce before capture/transfer; explicit oversized-item outcome. Resume only a fixed claimed manifest. |
| H5 | Archives/API; #514/#519/#521 | 10k candidates, 64 MiB captured/scanned input, 16 MiB result per task | Charge physical retries. Exhaustion is an explicit incomplete/planning outcome, never a complete empty result. |

Resident space and cumulative transferred bytes have different lifetimes.
Artifacts larger than H4 stream across bounded batches without memory aggregation.

## Performance targets

Use five fresh runs after declared warm-up; report all samples and median/p95
where applicable. Failure blocks dependent acceptance until fixed or explicitly
revised with evidence, and cannot waive H1–H5.

| ID | Profiles/issues | Target | Method and pass rule |
| --- | --- | --- | --- |
| P1 | Archive exact; #514/#519 | Catalog at most 5% of retained encoded bytes plus 1 MiB fixed overhead | Include every online catalog/index/manifest dependency at 10k/100k versions after identical maintenance. |
| P2 | Archive exact; #514/#519 | Exact read at most max(2 MiB, result bytes + 2 MiB) and 8x result bytes + 1 MiB; as-of at most max(4 MiB, result bytes + 4 MiB) and 16x result bytes + 2 MiB | Satisfy both bounds, counting verification and dependency reads. The absolute allowance accommodates H3's 4 MiB row. |
| P3 | Local/shared; #507 | Claim p95 at most 25 ms, p99 100 ms; coordination-lock p95 10 ms | Real claims with releasing capacity; at most 12 round trips and 1,024 examined candidates including retries. Report empty/failed polls separately. |
| P4 | Shared; #507 | Ordinary-lane shares within 5 percentage points of weights over 10k selections with all compared classes continuously eligible; promotion at most one in eight successes | Also measure skew, unequal durations, caps, reservations, overdue saturation and shrinking pools, reporting cap-blocked time and per-principal opportunities separately. At least 85% utilization of eligible non-reserved slots while runnable work exists. Starts are not CPU shares. |
| P5 | Archive exact; #513/#515/#518 | At most 64 MiB WAL per H4 batch; sustained movement at least 4 MiB/s | Isolated cluster LSN deltas include publication/retirement/cleanup. Separate logical throughput from verification traffic. |
| P6 | Local/shared; #512/#513/#515 | Eligible expiry p95 lag at most 60 s; orphan cleanup within 300 s after recovery | Steady 10k workload with releasing capacity; separately report outages, holds, recovery drain and oldest outstanding age. |
| P7 | Artifact slow; #508 | At least 0.8 MiB/s upload throughput per 1 MiB/s stream | Report producer/upload/verification/publication separately, count upload and full read-back, preserve H1/H2. Publication still requires completed verification. |
| P8 | Archive exact; #518 | At most 128 MiB source-plus-destination bytes per touched 64 MiB segment, plus reported verification traffic | Erase 1% of records; report all touched segments instead of extrapolating one segment's cost to the dataset. |

P4 proposes an eight-claim promotion interval for evaluation, not shipped defaults.
Catalog/segment limits reflect bounded block reads and an initial 5% online
metadata envelope. Memory/spool ceilings support four concurrent streams without
retaining four 1 GiB artifacts. WAL limits allow write amplification up to four
times bounded input; latency/throughput targets remain hypotheses to validate.

## Reproduction and current evidence

Use Python 3.11+ for tooling. Build the existing binaries and migrate a fresh
local disposable database:

```bash
cargo build --locked --release -p hubuum-scale-benchmark
cargo build --locked --release --features embedded-migrations --bin hubuum-admin
createdb hubuum_lifecycle_504
target/release/hubuum-admin --migrate \
  --database-url postgres://localhost/hubuum_lifecycle_504
/usr/bin/time -v target/release/hubuum-scale-benchmark lifecycle-inventory \
  --database-url postgres://localhost/hubuum_lifecycle_504 \
  --profile-spec scale-benchmarks/lifecycle/dataset-v1.toml \
  --output target/lifecycle-504/inventory.json
dropdb hubuum_lifecycle_504
```

The loader rejects populated domain databases; every reproduction needs a fresh
database and permission to set `session_replication_role` for the existing scale
loader. Reports include full specifications, base manifest, DB identity/settings,
before/after relation sizes and load-plus-inventory WAL. The manifest identifies
the base corpus; the workload describes the overlay. No API or worker runs in
this offline inventory. Preserve `/usr/bin/time` output alongside the JSON.

The [recorded baseline](lifecycle_baseline.json) uses
`lifecycle-unconstrained-local-v1`: PostgreSQL 18.6 in a separate rootless Podman
instance, pool 8, 256 MiB shared buffers, local loopback and an unoptimized Rust
1.98 build. No API/worker processes run. The host exposes 32 logical CPUs and
about 62 GiB RAM; it cannot enforce rootless CPU cgroups. Storage medium and RTT
were not independently measured. It is diagnostic storage evidence, not a run
of the constrained acceptance profiles. Concurrent compilation makes elapsed
time unsuitable for latency comparisons.

| Measurement | Base corpus | After lifecycle overlay |
| --- | --- | --- |
| Inventoried relations | 71 | 71 |
| Rows | 89,998 | 100,098 |
| Encoded logical bytes | 37,965,571 | 168,506,283 |
| Table/TOAST/auxiliary allocation bytes | 16,932,864 | 155,713,536 |
| Relation index allocation bytes | 30,973,952 | 46,981,120 |

The complete load and two inventories took 48,366 ms and generated 276,133,408
WAL bytes on that isolated instance. This is whole-fixture setup work, not P5's
bounded lifecycle batch. Inventory-client peak RSS was 36,687,872 bytes, not
worker RSS. Object history increased from 10,884 to 20,884 rows; 100 MiB of task
output was added. Allocated sizes include update churn and retained history;
the report does not promise that deleting equivalent logical bytes reclaims the
same disk space. Empty result/staging tables remain unmeasured workload costs.

Reproduce the diagnostic run with the exact image and build mode below. The
known password is solely for this disposable fixture. The database and container
names must be unused before starting:

```bash
cargo build --locked -p hubuum-scale-benchmark
docker run --detach --rm --name hubuum-lifecycle-baseline \
  --env POSTGRES_PASSWORD=lifecycle-disposable --publish 127.0.0.1::5432 \
  docker.io/library/postgres@sha256:0377e72c5289ed2f98cf61b1a9c2db9eb9d300317fe14244492fbc94343b3d04 \
  postgres -c shared_buffers=256MB -c max_connections=200
export PGHOST=127.0.0.1 PGUSER=postgres PGPASSWORD=lifecycle-disposable
export PGPORT="$(docker port hubuum-lifecycle-baseline 5432/tcp | cut -d: -f2)"
# Wait for pg_isready to succeed before creating the database.
pg_isready
createdb hubuum_lifecycle_504
export LIFECYCLE_DATABASE_URL="postgres://postgres:lifecycle-disposable@127.0.0.1:$PGPORT/hubuum_lifecycle_504"
diesel migration run --locked-schema \
  --migration-dir crates/hubuum-storage-postgres/migrations \
  --database-url "$LIFECYCLE_DATABASE_URL"
/usr/bin/time -v bash -c \
  'exec target/debug/hubuum-scale-benchmark lifecycle-inventory --database-url "$LIFECYCLE_DATABASE_URL" --output target/lifecycle-504/inventory.json'
dropdb hubuum_lifecycle_504
docker rm --force hubuum-lifecycle-baseline
```

The JSON summary records input hashes, manifest identity, environment, aggregate
sizes, selected families and the complete relation coverage list. Raw reports
belong with benchmark artifacts; they are not committed. Existing
[backup memory evidence](backup_memory.json) remains a separate baseline for the
250k-object/1m-relation fixture on its recorded environment. It measures
capture/restore, not #508 verified task-output streaming. No archive/catalog path
has yet been measured.

## Revision ledger and downstream gates

| Revision | Previous target | New target | Evidence and rationale |
| --- | --- | --- | --- |
| 1, 2026-10-27 | None | H1–H5, P1–P8 above | Initial one/four-slot profiles and bounded-block contracts. Latency/throughput targets are hypotheses, not measurements of unimplemented archives. |

Never overwrite a target to make a failing measurement pass. Append old/new
values, fixture, evidence, rationale and approval context, retaining this revision.
Issue #507 must cite P3/P4 and real concurrent-worker runs. #508 must cite H1–H3/P7
through real export hydration/rendering, backup capture/serialization, verification
and download. #514 adds H1–H5/P1/P2; movement/erasure/cleanup add P5/P6/P8.
Legacy-reader exceptions require finite limits and separate measurements. Empty
tables and synthetic timestamps cannot satisfy those implementation gates.
