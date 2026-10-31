//! Bounded server-side enrichment, excluding HTTP and fixture preparation.
use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use diesel::QueryableByName;
use diesel::sql_types::{Integer, Jsonb, Text};
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalId};
use hubuum_storage_core::{
    ComputedFieldStorage, ComputedObjectStorage, ObjectStorage, StorageComputedFieldRebuildRequest,
    StorageComputedObjectEnrichmentQuery, StorageTaskLeaseDuration, TaskExecutionStorage,
};
use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
use hubuum_storage_postgres::{
    PostgresPool, PostgresStorage, PostgresStorageError, capture_queries, with_connection,
};
use serde_json::json;

mod support;
use support::postgres::{PostgresBenchmarkDatabase, benchmark_pool, runtime, unique_name};

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

// Worker service time is measured separately from the response operation.
async fn drain_repairs(storage: &PostgresStorage, pool: &PostgresPool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(claim) = storage
            .claim_next_task(StorageTaskLeaseDuration::from_milliseconds(60_000).unwrap())
            .await
            .unwrap()
        {
            storage
                .execute_computed_field_rebuild(claim.lease().clone())
                .await
                .unwrap();
            continue;
        }
        // Claims skip locked rows. An admission can still finish after its
        // acknowledgement deadline, so an empty claim is not an empty queue.
        let remaining = with_connection(pool, async |conn| {
            diesel::sql_query("SELECT count(*)::integer AS id FROM tasks WHERE status IN ('queued', 'validating', 'running') AND request_payload->>'type' = 'computed_read_repairs'")
                .get_result::<IdRow>(conn).await
        }).await.unwrap().id;
        if remaining == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "repair queue did not drain: {remaining} active tasks"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
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
            // Prepare fresh data outside timing without relying on best-effort
            // read-repair admission.
            if mode == "fresh" {
                runtime
                    .block_on(storage.request_computed_field_rebuild(
                        StorageComputedFieldRebuildRequest::new(
                            ClassId::new(class_id).unwrap(),
                            CollectionId::new(collection_id).unwrap(),
                            None,
                        ),
                    ))
                    .unwrap();
                runtime.block_on(drain_repairs(&storage, &pool));
            }
            for page_size in [1, 20] {
                let owner = (mode == "personal").then(|| PrincipalId::new(owner_id).unwrap());
                let prepare = || {
                    if mode == "stale" {
                        runtime.block_on(drain_repairs(&storage, &pool));
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
                // Warm the response path before reporting its captured query budget.
                runtime
                    .block_on(storage.enrich_objects_with_computed(prepare()))
                    .unwrap();
                runtime.block_on(drain_repairs(&storage, &pool));
                let (result, queries) = runtime.block_on(capture_queries(
                    storage.enrich_objects_with_computed(prepare()),
                ));
                let response =
                    hubuum::benchmark_support::computed_response(result.unwrap()).unwrap();
                if mode == "fresh" {
                    let body = serde_json::to_value(&response).unwrap();
                    assert!(body.as_array().unwrap().iter().all(
                        |object| object["computed"]["shared"]["materialization_stale"] == false
                    ));
                }
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
                let mut repairs = Vec::new();
                let mut max_backlog = 0;
                let mut admission_misses = 0;
                let mut partial_admissions = 0;
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
                    if mode == "stale" {
                        let queued = runtime.block_on(with_connection(&pool, async |conn| {
                            diesel::sql_query("SELECT (identity->>'object_id')::integer AS id FROM tasks CROSS JOIN LATERAL jsonb_array_elements(request_payload->'objects') AS identity WHERE status IN ('queued', 'validating', 'running') AND request_payload->>'type' = 'computed_read_repairs'")
                                .load::<IdRow>(conn).await
                        })).unwrap();
                        max_backlog = max_backlog.max(queued.len());
                        let admitted = objects[..page_size]
                            .iter()
                            .filter(|object| queued.iter().any(|row| row.id == object.id().id()))
                            .cloned()
                            .collect::<Vec<_>>();
                        admission_misses += usize::from(admitted.is_empty());
                        partial_admissions +=
                            usize::from(!admitted.is_empty() && admitted.len() < page_size);
                        let started = Instant::now();
                        runtime.block_on(drain_repairs(&storage, &pool));
                        if !admitted.is_empty() {
                            repairs.push(started.elapsed().as_micros());
                            let fresh = runtime
                                .block_on(storage.enrich_objects_with_computed(
                                    StorageComputedObjectEnrichmentQuery::new(admitted, owner),
                                ))
                                .unwrap();
                            let body = serde_json::to_value(
                                hubuum::benchmark_support::computed_response(fresh).unwrap(),
                            )
                            .unwrap();
                            assert!(
                                body.as_array().unwrap().iter().all(
                                    |object| object["computed"]["shared"]["materialization_stale"]
                                        == false
                                ),
                                "queued={}, page={page_size}, states={:?}",
                                queued.len(),
                                body.as_array()
                                    .unwrap()
                                    .iter()
                                    .map(|object| (
                                        object["id"].clone(),
                                        object["computed"]["shared"]["materialization_stale"]
                                            .clone()
                                    ))
                                    .collect::<Vec<_>>()
                            );
                        }
                    }
                }
                if !repairs.is_empty() {
                    repairs.sort_unstable();
                    eprintln!(
                        "repair_samples objects={page_size} padding_bytes={bytes} admitted_samples={} p50_us={} p95_us={} max_observed_queued_identities={max_backlog} remaining_after_drain=0 observed_admission_misses={admission_misses} partial_admissions={partial_admissions}",
                        repairs.len(),
                        repairs[(repairs.len() * 50).div_ceil(100) - 1],
                        repairs[(repairs.len() * 95).div_ceil(100) - 1]
                    );
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

// CLI profiles override these standalone defaults equally on both revisions.
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = benchmark_computed_enrichment
}
criterion_main!(benches);
