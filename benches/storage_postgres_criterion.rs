use std::hint::black_box;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use diesel::{Connection, PgConnection};
use hubuum::events::EventContext;
use hubuum::models::{
    Collection, CollectionID, Group, GroupID, NewCollectionWithAssignee, NewGroup,
};
use hubuum::services::Services;
use hubuum::storage::{BenchmarkStorageContext, TransactionStorage};
use hubuum::traits::{CanDelete, CanSave};
use hubuum_storage_core::StorageCollectionCreate;
use hubuum_storage_postgres::{PostgresPool, PostgresPoolSettings, build_postgres_pool};
use tokio::runtime::{Builder, Runtime};

static NEXT_NAME_ID: AtomicU64 = AtomicU64::new(1);

const POSTGRES_DATABASE: &str = "hubuum_bench";
const POSTGRES_IMAGE: &str = "docker.io/library/postgres:18.4-alpine3.24@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15";

fn benchmark_pool(database_url: &str) -> PostgresPool {
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(4)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(30_000)
        .build()
        .expect("benchmark pool settings must be valid");
    build_postgres_pool(&settings).expect("benchmark pool must be constructible")
}

fn unique_name(prefix: &str) -> String {
    let id = NEXT_NAME_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{}-{id}", std::process::id())
}

fn command_diagnostics(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

struct PostgresBenchmarkDatabase {
    container_name: String,
    database_url: String,
}

impl PostgresBenchmarkDatabase {
    fn start() -> Self {
        let container_name = unique_name("hubuum-storage-benchmark");
        let output = Command::new("docker")
            .args(["run", "--detach", "--rm", "--name"])
            .arg(&container_name)
            .args([
                "--env",
                "POSTGRES_PASSWORD=postgres",
                "--env",
                "POSTGRES_DB=hubuum_bench",
                "--publish",
                "127.0.0.1::5432",
                POSTGRES_IMAGE,
                "postgres",
                "-c",
                "autovacuum=off",
                "-c",
                "checkpoint_timeout=30min",
            ])
            .output()
            .expect("Docker must be installed to run the PostgreSQL benchmark");
        assert!(
            output.status.success(),
            "PostgreSQL benchmark container should start:\n{}",
            command_diagnostics(&output),
        );

        let mut database = Self {
            container_name,
            database_url: String::new(),
        };
        let port = database.wait_for_port();
        database.database_url =
            format!("postgres://postgres:postgres@127.0.0.1:{port}/{POSTGRES_DATABASE}");
        database.wait_until_ready();
        hubuum_storage_postgres::run_embedded_migrations(&database.database_url)
            .expect("benchmark database migrations should succeed");
        database
    }

    fn wait_for_port(&self) -> u16 {
        for _ in 0..80 {
            let output = Command::new("docker")
                .args(["port", &self.container_name, "5432/tcp"])
                .output()
                .expect("Docker should inspect the PostgreSQL benchmark port");
            if output.status.success()
                && let Some(port) = String::from_utf8_lossy(&output.stdout)
                    .lines()
                    .find_map(|line| line.rsplit_once(':')?.1.trim().parse().ok())
            {
                return port;
            }
            thread::sleep(Duration::from_millis(250));
        }
        panic!("Docker did not publish the PostgreSQL benchmark port");
    }

    fn wait_until_container_ready(&self) {
        for _ in 0..120 {
            let status = Command::new("docker")
                .args([
                    "exec",
                    &self.container_name,
                    "pg_isready",
                    "--username",
                    "postgres",
                    "--dbname",
                    POSTGRES_DATABASE,
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("Docker should check PostgreSQL benchmark readiness");
            if status.success() {
                return;
            }
            thread::sleep(Duration::from_millis(250));
        }

        let logs = Command::new("docker")
            .args(["logs", &self.container_name])
            .output()
            .expect("Docker should read PostgreSQL benchmark logs");
        panic!(
            "PostgreSQL benchmark container did not become ready:\n{}",
            command_diagnostics(&logs),
        );
    }

    fn wait_until_ready(&self) {
        self.wait_until_container_ready();

        let mut last_error = None;
        for _ in 0..120 {
            match PgConnection::establish(&self.database_url) {
                Ok(_) => return,
                Err(error) => last_error = Some(error),
            }
            thread::sleep(Duration::from_millis(250));
        }

        let logs = Command::new("docker")
            .args(["logs", &self.container_name])
            .output()
            .expect("Docker should read PostgreSQL benchmark logs");
        panic!(
            "PostgreSQL benchmark container was not reachable from the host: {}\n{}",
            last_error.expect("at least one host connection attempt should fail"),
            command_diagnostics(&logs),
        );
    }

    fn url(&self) -> &str {
        &self.database_url
    }
}

impl Drop for PostgresBenchmarkDatabase {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.container_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("benchmark runtime should build")
}

struct StorageFixture {
    storage: BenchmarkStorageContext,
    services: Services,
    owner_group: Group,
    collections: Vec<Collection>,
}

impl StorageFixture {
    fn new(runtime: &Runtime, database_url: &str) -> Self {
        let pool = {
            let _runtime_guard = runtime.enter();
            benchmark_pool(database_url)
        };
        let schema_ready = runtime
            .block_on(hubuum_storage_postgres::schema_is_ready(&pool))
            .expect("benchmark database readiness should be queryable");
        assert!(schema_ready, "benchmark database should be migrated");
        let storage = hubuum::benchmark_support::storage_for_postgres(pool);
        let services = hubuum::benchmark_support::services_for_storage(&storage);

        let owner_group = runtime
            .block_on(
                NewGroup {
                    identity_scope: None,
                    groupname: unique_name("storage-bench-group"),
                    description: Some("PostgreSQL storage benchmark owner".to_string()),
                }
                .save_without_events(&storage),
            )
            .expect("benchmark owner group should save");

        let first = runtime
            .block_on(
                NewCollectionWithAssignee {
                    name: unique_name("storage-bench-collection"),
                    description: "PostgreSQL storage point-read benchmark".to_string(),
                    group_id: GroupID::new(owner_group.id)
                        .expect("persisted owner group id should be positive"),
                    parent_collection_id: None,
                }
                .save_without_events(&storage),
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
                        group_id: GroupID::new(owner_group.id)
                            .expect("persisted owner group id should be positive"),
                        parent_collection_id: Some(
                            CollectionID::new(parent_id).expect("valid parent id"),
                        ),
                    }
                    .save_without_events(&storage),
                )
                .expect("nested benchmark collection should save");
            collections.push(collection);
        }

        Self {
            storage,
            services,
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
            .block_on(collection.delete_without_events(&self.storage))
            .expect("created benchmark collection should delete");
    }

    fn cleanup(self, runtime: &Runtime) {
        for collection in self.collections.iter().rev() {
            runtime
                .block_on(collection.delete_without_events(&self.storage))
                .expect("benchmark collection should delete");
        }
        runtime
            .block_on(self.owner_group.delete_without_events(&self.storage))
            .expect("benchmark owner group should delete");
    }
}

fn benchmark_postgres_storage(c: &mut Criterion) {
    let database = PostgresBenchmarkDatabase::start();
    let runtime = runtime();
    let fixture = StorageFixture::new(&runtime, database.url());
    let collections = fixture.services.collections();
    let point_read_id = fixture.point_read_id();
    let leaf_id = fixture.leaf_id();

    runtime
        .block_on(collections.get(point_read_id))
        .expect("point-read warmup should succeed");
    runtime
        .block_on(collections.ancestors(leaf_id))
        .expect("ancestor warmup should succeed");

    let mut group = c.benchmark_group("storage_postgres");
    group.bench_function("collection_point_read", |b| {
        b.iter(|| {
            let collection = runtime
                .block_on(collections.get(black_box(point_read_id)))
                .expect("point read should succeed");
            black_box(collection);
        });
    });
    group.bench_function("collection_ancestors_depth_16", |b| {
        b.iter(|| {
            let ancestors = runtime
                .block_on(collections.ancestors(black_box(leaf_id)))
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
                    group_id: GroupID::new(fixture.owner_group.id)
                        .expect("persisted owner group id should be positive"),
                    parent_collection_id: Some(point_read_id),
                };
                let started = Instant::now();
                let collection = runtime
                    .block_on(collections.create(command, &EventContext::system()))
                    .expect("timed collection create should succeed");
                measured += started.elapsed();

                fixture.cleanup_created_collection(&runtime, &collection);
            }
            measured
        });
    });
    group.bench_function("collection_create_with_event_in_unit_of_work", |b| {
        b.iter_custom(|iterations| {
            let mut measured = Duration::ZERO;
            for _ in 0..iterations {
                let command = StorageCollectionCreate::new(
                    unique_name("storage-bench-transaction-create"),
                    "PostgreSQL storage transaction create benchmark",
                    GroupID::new(fixture.owner_group.id)
                        .expect("persisted owner group id should be positive"),
                    Some(point_read_id),
                );
                let started = Instant::now();
                let collection = runtime
                    .block_on(fixture.storage.transaction(
                        EventContext::system(),
                        move |transaction| {
                            Box::pin(async move { transaction.collections().create(command).await })
                        },
                    ))
                    .expect("timed transaction collection create should succeed");
                measured += started.elapsed();

                let collection = runtime
                    .block_on(
                        collections.get(
                            CollectionID::new(collection.into_value().id().id())
                                .expect("transaction-created collection id should be positive"),
                        ),
                    )
                    .expect("transaction-created collection should resolve");
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
