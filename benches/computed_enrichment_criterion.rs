//! Bounded server-side enrichment, excluding HTTP and fixture preparation.
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use diesel::QueryableByName;
use diesel::sql_types::{Integer, Jsonb, Text};
use hubuum_domain::{ObjectId, PrincipalId};
use hubuum_storage_core::{
    ComputedObjectStorage, ObjectStorage, StorageComputedObjectEnrichmentQuery,
};
use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
use hubuum_storage_postgres::{
    PostgresStorage, PostgresStorageError, capture_queries, with_connection,
};
use serde_json::json;

mod support;
use support::postgres::{PostgresBenchmarkDatabase, benchmark_pool, runtime, unique_name};

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

// Linux process CPU is sampled independently of wall time. Tick resolution is
// intentionally reported rather than implying allocation or database CPU data.
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

fn cpu_ticks_per_second() -> Option<u64> {
    let output = std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()?;
    output.status.success().then_some(())?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}

fn benchmark_computed_enrichment(c: &mut Criterion) {
    let cpu_hz = cpu_ticks_per_second().filter(|_| process_cpu_ticks().is_some());
    let database = PostgresBenchmarkDatabase::start();
    let runtime = runtime();
    let pool = {
        let _guard = runtime.enter();
        benchmark_pool(database.url())
    };
    let storage = PostgresStorage::unobserved(pool.clone());
    let (collection_id, owner_id) = runtime.block_on(with_connection(&pool, async |conn| {
        let collection = diesel::sql_query("INSERT INTO collections (name, description, parent_collection_id) SELECT 'computed-benchmark', '', id FROM collections WHERE parent_collection_id IS NULL RETURNING id")
            .get_result::<IdRow>(conn).await?.id;
        let owner = diesel::sql_query("INSERT INTO principals (kind, name) VALUES ('human', 'computed-benchmark') RETURNING id")
            .get_result::<IdRow>(conn).await?.id;
        diesel::sql_query("INSERT INTO users (id) VALUES ($1)").bind::<Integer,_>(owner).execute(conn).await?;
        Ok::<_, PostgresStorageError>((collection, owner))
    })).unwrap();
    let mut group = c.benchmark_group("computed_enrichment");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    for bytes in [1024, 262_144] {
        for mode in ["fresh", "stale", "personal", "disabled"] {
            let data = json!({"value": "benchmark.example", "padding": "x".repeat(bytes)});
            let (class_id, object_ids) = runtime.block_on(with_connection(&pool, async |conn| {
                let class = diesel::sql_query("INSERT INTO hubuumclass (name, collection_id, description, validate_schema) VALUES ($1, $2, '', false) RETURNING id")
                    .bind::<Text,_>(unique_name("computed")).bind::<Integer,_>(collection_id)
                    .get_result::<IdRow>(conn).await?.id;
                diesel::sql_query("INSERT INTO class_computation_state (class_id) VALUES ($1)")
                    .bind::<Integer,_>(class).execute(conn).await?;
                for slot in 0..8 {
                    diesel::sql_query("INSERT INTO computed_field_definitions (class_id, visibility, owner_user_id, key, label, description, operation, result_type, enabled) VALUES ($1, $2, $3, $4, 'Benchmark field', '', $5, 'string', $6)")
                        .bind::<Integer,_>(class).bind::<Text,_>(if mode == "personal" {"personal"} else {"shared"})
                        .bind::<diesel::sql_types::Nullable<Integer>,_>((mode == "personal").then_some(owner_id))
                        .bind::<Text,_>(format!("field_{slot}"))
                        .bind::<Jsonb,_>(json!({"type":"first_non_null", "paths":["/value"]}))
                        .bind::<diesel::sql_types::Bool,_>(mode != "disabled").execute(conn).await?;
                }
                let ids = diesel::sql_query("INSERT INTO hubuumobject (name, collection_id, hubuum_class_id, description, data) SELECT 'sample-' || n, $1, $2, '', $3 FROM generate_series(1, 20) n RETURNING id")
                    .bind::<Integer,_>(collection_id).bind::<Integer,_>(class).bind::<Jsonb,_>(&data)
                    .load::<IdRow>(conn).await?;
                Ok::<_, PostgresStorageError>((class, ids))
            })).unwrap();
            let objects = runtime.block_on(async {
                let mut objects = Vec::new();
                for id in object_ids {
                    objects.push(
                        storage
                            .get_object(ObjectId::new(id.id).unwrap())
                            .await
                            .unwrap()
                            .object()
                            .clone(),
                    );
                }
                objects
            });
            for page_size in [1, 20] {
                let owner = (mode == "personal").then(|| PrincipalId::new(owner_id).unwrap());
                let prepare = || {
                    if mode == "stale" {
                        runtime
                            .block_on(with_connection(&pool, async |conn| {
                                diesel::sql_query(
                                    "DELETE FROM object_computed_data WHERE class_id = $1",
                                )
                                .bind::<Integer, _>(class_id)
                                .execute(conn)
                                .await
                            }))
                            .unwrap();
                    }
                    StorageComputedObjectEnrichmentQuery::new(objects[..page_size].to_vec(), owner)
                };
                // Warm up materializations and then report a captured query budget.
                runtime
                    .block_on(storage.enrich_objects_with_computed(prepare()))
                    .unwrap();
                let (result, queries) = runtime.block_on(capture_queries(
                    storage.enrich_objects_with_computed(prepare()),
                ));
                let response =
                    hubuum::benchmark_support::computed_response(result.unwrap()).unwrap();
                let response_bytes = serde_json::to_vec(&response).unwrap().len();
                eprintln!(
                    "fixture mode={mode} padding_bytes={bytes} objects={page_size} definitions=8 queries={} checkouts={} response_bytes={response_bytes}",
                    queries.total_queries(),
                    queries.connection_checkouts()
                );
                group.bench_with_input(
                    BenchmarkId::new(format!("{mode}/{page_size}"), bytes),
                    &(),
                    |b, _| {
                        b.iter_batched(
                            prepare,
                            |query| {
                                black_box(
                                    runtime
                                        .block_on(storage.enrich_objects_with_computed(query))
                                        .unwrap(),
                                );
                            },
                            BatchSize::PerIteration,
                        );
                    },
                );
                let mut latencies = Vec::with_capacity(100);
                let mut cpu_ticks = 0;
                for _ in 0..100 {
                    let query = prepare();
                    let cpu_before = process_cpu_ticks();
                    let started = Instant::now();
                    black_box(
                        runtime
                            .block_on(storage.enrich_objects_with_computed(query))
                            .unwrap(),
                    );
                    latencies.push(started.elapsed().as_micros());
                    if let (Some(before), Some(after)) = (cpu_before, process_cpu_ticks()) {
                        cpu_ticks += after.saturating_sub(before);
                    }
                }
                latencies.sort_unstable();
                eprintln!(
                    "server_samples mode={mode} objects={page_size} padding_bytes={bytes} n=100 p50_us={} p95_us={} process_cpu_us_per_page={:?} cpu_ticks_per_second={cpu_hz:?}",
                    latencies[49],
                    latencies[94],
                    cpu_hz
                        .filter(|hz| *hz > 0)
                        .map(|hz| cpu_ticks * 1_000_000 / hz / 100)
                );
                let started = Instant::now();
                for _ in 0..100 {
                    black_box(serde_json::to_vec(&response).unwrap());
                }
                eprintln!(
                    "serialization mode={mode} objects={page_size} padding_bytes={bytes} mean_us={}",
                    started.elapsed().as_micros() / 100
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_computed_enrichment);
criterion_main!(benches);
