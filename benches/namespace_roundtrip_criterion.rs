use criterion::{criterion_group, criterion_main, Criterion};
use hubuum::db::init_pool;
use hubuum::models::group::GroupID;
use hubuum::models::namespace::NewNamespaceWithAssignee;
use hubuum::tests::ensure_admin_group;
use hubuum::traits::{CanDelete, CanSave};
use std::env;
use std::hint::black_box;
use std::time::Duration;
use tokio::runtime::Runtime;

fn benchmark_namespace_roundtrip(c: &mut Criterion) {
    let database_url = env::var("HUBUUM_BENCH_DATABASE_URL")
        .or_else(|_| env::var("HUBUUM_DATABASE_URL"))
        .ok();

    let Some(database_url) = database_url else {
        eprintln!(
            "Skipping namespace_roundtrip_criterion benchmark: set HUBUUM_BENCH_DATABASE_URL or HUBUUM_DATABASE_URL"
        );
        return;
    };

    let runtime = Runtime::new().expect("tokio runtime");
    let pool = init_pool(&database_url, 4);
    let admin_group_id = runtime.block_on(async { GroupID(ensure_admin_group(&pool).await.id) });
    let mut sequence = 0_u64;

    let mut group = c.benchmark_group("db_namespace");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("create_delete_namespace", |b| {
        b.iter(|| {
            sequence += 1;
            let namespace_name = format!(
                "criterion-bench-{}-{}",
                std::process::id(),
                black_box(sequence)
            );

            runtime.block_on(async {
                let namespace = NewNamespaceWithAssignee {
                    name: namespace_name,
                    description: "Criterion database benchmark".to_string(),
                    group_id: admin_group_id.0,
                }
                .save(&pool)
                .await
                .expect("namespace should be created");

                namespace
                    .delete(&pool)
                    .await
                    .expect("namespace should be deleted");
            });
        })
    });

    group.finish();
}

criterion_group!(benches, benchmark_namespace_roundtrip);
criterion_main!(benches);
