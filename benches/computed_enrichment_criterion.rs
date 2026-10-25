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

fn benchmark_computed_enrichment(c: &mut Criterion) {
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
                StorageComputedObjectEnrichmentQuery::new(objects.clone(), owner)
            };
            // Warm up materializations and then report a captured query budget.
            runtime
                .block_on(storage.enrich_objects_with_computed(prepare()))
                .unwrap();
            let (result, queries) = runtime.block_on(capture_queries(
                storage.enrich_objects_with_computed(prepare()),
            ));
            let response = hubuum::benchmark_support::computed_response(result.unwrap()).unwrap();
            let response_bytes = serde_json::to_vec(&response).unwrap().len();
            eprintln!(
                "fixture mode={mode} padding_bytes={bytes} objects=20 definitions=8 queries={} checkouts={} response_bytes={response_bytes}",
                queries.total_queries(),
                queries.connection_checkouts()
            );
            group.bench_with_input(BenchmarkId::new(mode, bytes), &(), |b, _| {
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
            });
            let started = Instant::now();
            for _ in 0..100 {
                black_box(serde_json::to_vec(&response).unwrap());
            }
            eprintln!(
                "serialization mode={mode} padding_bytes={bytes} mean_us={}",
                started.elapsed().as_micros() / 100
            );
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark_computed_enrichment);
criterion_main!(benches);
