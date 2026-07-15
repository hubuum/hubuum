use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use hubuum::db::{DbPool, ensure_database_schema_ready, init_pool_with_statement_timeout};
use hubuum::events::EventContext;
use hubuum::models::{
    Collection, CollectionID, Group, NewCollectionWithAssignee, NewGroup, collection_ancestors,
};
use hubuum::traits::{CanDelete, CanSave, CollectionAccessors};
use tokio::runtime::{Builder, Runtime};

static NEXT_NAME_ID: AtomicU64 = AtomicU64::new(1);

fn unique_name(prefix: &str) -> String {
    let id = NEXT_NAME_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}

fn benchmark_database_url() -> Option<String> {
    std::env::var("HUBUUM_BENCH_DATABASE_URL").ok()
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime should build")
}

struct StorageFixture {
    pool: DbPool,
    owner_group: Group,
    collections: Vec<Collection>,
}

impl StorageFixture {
    fn new(runtime: &Runtime, database_url: &str) -> Self {
        let pool = {
            let _runtime_guard = runtime.enter();
            init_pool_with_statement_timeout(database_url, 4, 0)
        };
        runtime
            .block_on(ensure_database_schema_ready(&pool))
            .expect("benchmark database should be migrated");

        let owner_group = runtime
            .block_on(
                NewGroup {
                    identity_scope: None,
                    groupname: unique_name("storage-bench-group"),
                    description: Some("PostgreSQL storage benchmark owner".to_string()),
                }
                .save_without_events(&pool),
            )
            .expect("benchmark owner group should save");

        let first = runtime
            .block_on(
                NewCollectionWithAssignee {
                    name: unique_name("storage-bench-collection"),
                    description: "PostgreSQL storage point-read benchmark".to_string(),
                    group_id: owner_group.id,
                    parent_collection_id: None,
                }
                .save_without_events(&pool),
            )
            .expect("benchmark collection should save");
        let mut collections = vec![first];

        for depth in 1..=16 {
            let parent_id = collections.last().expect("parent collection").id;
            let collection = runtime
                .block_on(
                    NewCollectionWithAssignee {
                        name: unique_name(&format!("storage-bench-depth-{depth}")),
                        description: format!("PostgreSQL storage ancestor level {depth}"),
                        group_id: owner_group.id,
                        parent_collection_id: Some(
                            CollectionID::new(parent_id).expect("valid parent id"),
                        ),
                    }
                    .save_without_events(&pool),
                )
                .expect("nested benchmark collection should save");
            collections.push(collection);
        }

        Self {
            pool,
            owner_group,
            collections,
        }
    }

    fn point_read_id(&self) -> CollectionID {
        CollectionID::new(self.collections[0].id).expect("valid point-read id")
    }

    fn leaf_id(&self) -> CollectionID {
        CollectionID::new(self.collections.last().expect("leaf collection").id)
            .expect("valid leaf id")
    }

    fn cleanup_created_collection(&self, runtime: &Runtime, collection: &Collection) {
        runtime
            .block_on(collection.delete_without_events(&self.pool))
            .expect("created benchmark collection should delete");
    }

    fn cleanup(self, runtime: &Runtime) {
        for collection in self.collections.iter().rev() {
            runtime
                .block_on(collection.delete_without_events(&self.pool))
                .expect("benchmark collection should delete");
        }
        runtime
            .block_on(self.owner_group.delete_without_events(&self.pool))
            .expect("benchmark owner group should delete");
    }
}

fn benchmark_postgres_storage(c: &mut Criterion) {
    let Some(database_url) = benchmark_database_url() else {
        eprintln!(
            "Skipping storage_postgres_criterion: set HUBUUM_BENCH_DATABASE_URL to a migrated, \
             disposable benchmark database"
        );
        return;
    };

    let runtime = runtime();
    let fixture = StorageFixture::new(&runtime, &database_url);
    let point_read_id = fixture.point_read_id();
    let leaf_id = fixture.leaf_id();

    runtime
        .block_on(point_read_id.collection(&fixture.pool))
        .expect("point-read warmup should succeed");
    runtime
        .block_on(collection_ancestors(&fixture.pool, leaf_id))
        .expect("ancestor warmup should succeed");

    let mut group = c.benchmark_group("storage_postgres");
    group.bench_function("collection_point_read", |b| {
        b.iter(|| {
            let collection = runtime
                .block_on(black_box(point_read_id).collection(black_box(&fixture.pool)))
                .expect("point read should succeed");
            black_box(collection);
        });
    });
    group.bench_function("collection_ancestors_depth_16", |b| {
        b.iter(|| {
            let ancestors = runtime
                .block_on(collection_ancestors(
                    black_box(&fixture.pool),
                    black_box(leaf_id),
                ))
                .expect("ancestor read should succeed");
            black_box(ancestors);
        });
    });
    group.bench_function("collection_create_with_event", |b| {
        b.iter_custom(|iterations| {
            let mut measured = Duration::ZERO;
            for _ in 0..iterations {
                let command = NewCollectionWithAssignee {
                    name: unique_name("storage-bench-create"),
                    description: "PostgreSQL storage create benchmark".to_string(),
                    group_id: fixture.owner_group.id,
                    parent_collection_id: Some(point_read_id),
                };
                let started = Instant::now();
                let collection = runtime
                    .block_on(command.save(&fixture.pool, &EventContext::system()))
                    .expect("timed collection create should succeed");
                measured += started.elapsed();

                fixture.cleanup_created_collection(&runtime, &collection);
            }
            measured
        });
    });
    group.finish();

    fixture.cleanup(&runtime);
}

criterion_group!(benches, benchmark_postgres_storage);
criterion_main!(benches);
