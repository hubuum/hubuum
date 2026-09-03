use std::hint::black_box;
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use diesel::sql_types::{Integer, Text};
use diesel::{Connection, PgConnection, QueryableByName};
use hubuum::events::EventContext;
use hubuum::models::search::MAX_RELATED_FILTER_DEPTH;
use hubuum::models::{
    Collection, CollectionID, Group, GroupID, HubuumClassID, NewCollectionWithAssignee, NewGroup,
    StructuredSearchOperator,
};
use hubuum::services::Services;
use hubuum::storage::{BenchmarkStorageContext, TransactionStorage};
use hubuum::traits::{CanDelete, CanSave};
use hubuum_storage_core::StorageCollectionCreate;
use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
use hubuum_storage_postgres::{
    PostgresPool, PostgresPoolSettings, build_postgres_pool, with_connection,
};
use tokio::runtime::{Builder, Runtime};

static NEXT_NAME_ID: AtomicU64 = AtomicU64::new(1);

const POSTGRES_DATABASE: &str = "hubuum_bench";
const POSTGRES_IMAGE: &str = "docker.io/library/postgres:18.4-alpine3.24@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15";
const STRUCTURED_SEARCH_CHAINS: i32 = 128;
const UNRELATED_HYDRATION_RELATIONS: i32 = 100_000;

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
    structured_search: StructuredSearchFixture,
}

struct StructuredSearchFixture {
    source_class_id: HubuumClassID,
    target_class_id: HubuumClassID,
    hydration_target_class_ids: Vec<HubuumClassID>,
    source_object_ids: Vec<i32>,
    selective_target_name: String,
    target_name_fragment: String,
}

#[derive(QueryableByName)]
struct ClassIdRow {
    #[diesel(sql_type = Integer)]
    id: i32,
}

impl StructuredSearchFixture {
    fn new(runtime: &Runtime, pool: &PostgresPool, collection_id: i32) -> Self {
        let prefix = unique_name("structured-depth-ten");
        let source_class_name = format!("{prefix}-class-00");
        let target_class_name = format!("{prefix}-class-{MAX_RELATED_FILTER_DEPTH:02}");
        let target_name_fragment = format!("{prefix}-target");
        let selective_target_name = format!("{target_name_fragment}-000");

        let setup = runtime
            .block_on(with_connection(pool, async |connection| {
                diesel::sql_query(
                    "INSERT INTO hubuumclass \
                        (name, collection_id, description, validate_schema) \
                     SELECT $1 || '-class-' || lpad(level::text, 2, '0'), \
                            $2, \
                            'structured search depth benchmark class', \
                            false \
                     FROM generate_series(0, $3) AS level \
                     ORDER BY level",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(collection_id)
                .bind::<Integer, _>(i32::from(MAX_RELATED_FILTER_DEPTH))
                .execute(connection)
                .await?;

                diesel::sql_query(
                    "INSERT INTO hubuumclass_relation \
                        (from_hubuum_class_id, to_hubuum_class_id) \
                     SELECT lower_class.id, upper_class.id \
                     FROM generate_series(0, $2 - 1) AS level \
                     JOIN hubuumclass lower_class \
                       ON lower_class.name = $1 || '-class-' || lpad(level::text, 2, '0') \
                     JOIN hubuumclass upper_class \
                       ON upper_class.name = $1 || '-class-' || lpad((level + 1)::text, 2, '0') \
                     ORDER BY level",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(i32::from(MAX_RELATED_FILTER_DEPTH))
                .execute(connection)
                .await?;

                diesel::sql_query(
                    "INSERT INTO hubuumobject \
                        (name, collection_id, hubuum_class_id, data, description) \
                     SELECT CASE \
                                WHEN level = $3 THEN \
                                    $1 || '-target-' || lpad(chain::text, 3, '0') \
                                ELSE \
                                    $1 || '-node-' || lpad(level::text, 2, '0') || '-' || \
                                    lpad(chain::text, 3, '0') \
                            END, \
                            $2, \
                            class.id, \
                            jsonb_build_object('chain', chain, 'level', level), \
                            'structured search depth benchmark object' \
                     FROM generate_series(0, $3) AS level \
                     CROSS JOIN generate_series(0, $4 - 1) AS chain \
                     JOIN hubuumclass class \
                       ON class.name = $1 || '-class-' || lpad(level::text, 2, '0') \
                     ORDER BY level, chain",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(collection_id)
                .bind::<Integer, _>(i32::from(MAX_RELATED_FILTER_DEPTH))
                .bind::<Integer, _>(STRUCTURED_SEARCH_CHAINS)
                .execute(connection)
                .await?;

                diesel::sql_query(
                    "INSERT INTO hubuumobject_relation \
                        (from_hubuum_object_id, to_hubuum_object_id, class_relation_id) \
                     SELECT LEAST(lower_object.id, upper_object.id), \
                            GREATEST(lower_object.id, upper_object.id), \
                            class_relation.id \
                     FROM generate_series(0, $2 - 1) AS level \
                     CROSS JOIN generate_series(0, $3 - 1) AS chain \
                     JOIN hubuumclass lower_class \
                       ON lower_class.name = $1 || '-class-' || lpad(level::text, 2, '0') \
                     JOIN hubuumclass upper_class \
                       ON upper_class.name = $1 || '-class-' || lpad((level + 1)::text, 2, '0') \
                     JOIN hubuumclass_relation class_relation \
                       ON class_relation.from_hubuum_class_id = lower_class.id \
                      AND class_relation.to_hubuum_class_id = upper_class.id \
                     JOIN hubuumobject lower_object \
                       ON lower_object.hubuum_class_id = lower_class.id \
                      AND (lower_object.data ->> 'chain')::integer = chain \
                     JOIN hubuumobject upper_object \
                       ON upper_object.hubuum_class_id = upper_class.id \
                      AND (upper_object.data ->> 'chain')::integer = chain \
                     ORDER BY level, chain",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(i32::from(MAX_RELATED_FILTER_DEPTH))
                .bind::<Integer, _>(STRUCTURED_SEARCH_CHAINS)
                .execute(connection)
                .await?;

                for table in [
                    "hubuumclass",
                    "hubuumclass_relation",
                    "hubuumobject",
                    "hubuumobject_relation",
                ] {
                    diesel::sql_query(format!("ANALYZE {table}"))
                        .execute(connection)
                        .await?;
                }

                let source = diesel::sql_query("SELECT id FROM hubuumclass WHERE name = $1")
                    .bind::<Text, _>(&source_class_name)
                    .get_result::<ClassIdRow>(connection)
                    .await?;
                let target = diesel::sql_query("SELECT id FROM hubuumclass WHERE name = $1")
                    .bind::<Text, _>(&target_class_name)
                    .get_result::<ClassIdRow>(connection)
                    .await?;
                let hydration_targets = diesel::sql_query(
                    "SELECT class.id \
                     FROM generate_series(1, 3) AS level \
                     JOIN hubuumclass class \
                       ON class.name = $1 || '-class-' || lpad(level::text, 2, '0') \
                     ORDER BY level",
                )
                .bind::<Text, _>(&prefix)
                .load::<ClassIdRow>(connection)
                .await?;
                let source_objects = diesel::sql_query(
                    "SELECT id FROM hubuumobject WHERE hubuum_class_id = $1 ORDER BY id",
                )
                .bind::<Integer, _>(source.id)
                .load::<ClassIdRow>(connection)
                .await?;
                Ok::<_, hubuum_storage_postgres::PostgresStorageError>((
                    source.id,
                    target.id,
                    hydration_targets
                        .into_iter()
                        .map(|row| row.id)
                        .collect::<Vec<_>>(),
                    source_objects
                        .into_iter()
                        .map(|row| row.id)
                        .collect::<Vec<_>>(),
                ))
            }))
            .expect("structured search benchmark graph should save");
        let (source_class_id, target_class_id, hydration_target_class_ids, source_object_ids) =
            setup;

        Self {
            source_class_id: HubuumClassID::new(source_class_id)
                .expect("source class id should be positive"),
            target_class_id: HubuumClassID::new(target_class_id)
                .expect("target class id should be positive"),
            hydration_target_class_ids: hydration_target_class_ids
                .into_iter()
                .map(|id| {
                    HubuumClassID::new(id).expect("hydration target class id should be positive")
                })
                .collect(),
            source_object_ids,
            selective_target_name,
            target_name_fragment,
        }
    }

    fn add_unrelated_hydration_corpus(
        &self,
        runtime: &Runtime,
        pool: &PostgresPool,
        collection_id: i32,
    ) {
        let prefix = unique_name("hydration-unrelated");
        runtime
            .block_on(with_connection(pool, async |connection| {
                diesel::sql_query(
                    "INSERT INTO hubuumobject \
                        (name, collection_id, hubuum_class_id, data, description) \
                     SELECT $1 || '-source-' || slot::text, $2, $3, \
                            jsonb_build_object('hydration_corpus_slot', slot), \
                            'unrelated template hydration benchmark source' \
                     FROM generate_series(1, $4) AS slot",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(collection_id)
                .bind::<Integer, _>(self.source_class_id.id())
                .bind::<Integer, _>(UNRELATED_HYDRATION_RELATIONS)
                .execute(connection)
                .await?;

                diesel::sql_query(
                    "INSERT INTO hubuumobject \
                        (name, collection_id, hubuum_class_id, data, description) \
                     SELECT $1 || '-target-' || slot::text, $2, $3, \
                            jsonb_build_object('hydration_corpus_slot', slot), \
                            'unrelated template hydration benchmark target' \
                     FROM generate_series(1, $4) AS slot",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(collection_id)
                .bind::<Integer, _>(
                    self.hydration_target_class_ids
                        .as_slice()
                        .first()
                        .expect("depth-one hydration target class")
                        .id(),
                )
                .bind::<Integer, _>(UNRELATED_HYDRATION_RELATIONS)
                .execute(connection)
                .await?;

                diesel::sql_query(
                    "INSERT INTO hubuumobject_relation \
                        (from_hubuum_object_id, to_hubuum_object_id, class_relation_id) \
                     SELECT source_object.id, target_object.id, class_relation.id \
                     FROM generate_series(1, $2) AS slot \
                     JOIN hubuumobject source_object \
                       ON source_object.name = $1 || '-source-' || slot::text \
                     JOIN hubuumobject target_object \
                       ON target_object.name = $1 || '-target-' || slot::text \
                     JOIN hubuumclass_relation class_relation \
                       ON class_relation.from_hubuum_class_id = source_object.hubuum_class_id \
                      AND class_relation.to_hubuum_class_id = target_object.hubuum_class_id",
                )
                .bind::<Text, _>(&prefix)
                .bind::<Integer, _>(UNRELATED_HYDRATION_RELATIONS)
                .execute(connection)
                .await?;

                diesel::sql_query("ANALYZE hubuumobject")
                    .execute(connection)
                    .await?;
                diesel::sql_query("ANALYZE hubuumobject_relation")
                    .execute(connection)
                    .await?;
                Ok::<_, hubuum_storage_postgres::PostgresStorageError>(())
            }))
            .expect("unrelated template hydration corpus should save");
    }
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

        let fixture_pool = {
            let _runtime_guard = runtime.enter();
            benchmark_pool(database_url)
        };
        let structured_search =
            StructuredSearchFixture::new(runtime, &fixture_pool, collections[0].id);

        Self {
            storage,
            services,
            owner_group,
            collections,
            structured_search,
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
    let selective_rows = runtime
        .block_on(hubuum::benchmark_support::structured_related_object_search(
            &fixture.storage,
            fixture.structured_search.source_class_id,
            fixture.structured_search.target_class_id,
            StructuredSearchOperator::Equals,
            &fixture.structured_search.selective_target_name,
            MAX_RELATED_FILTER_DEPTH,
        ))
        .expect("selective structured-search warmup should succeed");
    assert_eq!(selective_rows.len(), 1);
    let non_selective_rows = runtime
        .block_on(hubuum::benchmark_support::structured_related_object_search(
            &fixture.storage,
            fixture.structured_search.source_class_id,
            fixture.structured_search.target_class_id,
            StructuredSearchOperator::Icontains,
            &fixture.structured_search.target_name_fragment,
            MAX_RELATED_FILTER_DEPTH,
        ))
        .expect("non-selective structured-search warmup should succeed");
    assert_eq!(non_selective_rows.len(), STRUCTURED_SEARCH_CHAINS as usize);

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
    group.bench_function("structured_related_depth_10_selective", |b| {
        b.iter(|| {
            let rows = runtime
                .block_on(hubuum::benchmark_support::structured_related_object_search(
                    &fixture.storage,
                    black_box(fixture.structured_search.source_class_id),
                    black_box(fixture.structured_search.target_class_id),
                    StructuredSearchOperator::Equals,
                    black_box(&fixture.structured_search.selective_target_name),
                    MAX_RELATED_FILTER_DEPTH,
                ))
                .expect("selective structured search should succeed");
            black_box(rows);
        });
    });
    group.bench_function("structured_related_depth_10_non_selective", |b| {
        b.iter(|| {
            let rows = runtime
                .block_on(hubuum::benchmark_support::structured_related_object_search(
                    &fixture.storage,
                    black_box(fixture.structured_search.source_class_id),
                    black_box(fixture.structured_search.target_class_id),
                    StructuredSearchOperator::Icontains,
                    black_box(&fixture.structured_search.target_name_fragment),
                    MAX_RELATED_FILTER_DEPTH,
                ))
                .expect("non-selective structured search should succeed");
            black_box(rows);
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
                    .block_on(fixture.storage.with_transaction(
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

    {
        let hydration_roots = &fixture.structured_search.source_object_ids;
        assert_eq!(hydration_roots.len(), STRUCTURED_SEARCH_CHAINS as usize);
        let mut benchmark_hydration = |corpus_label: &str| {
            for depth in 1..=3 {
                let name = format!(
                    "template_multi_root_bidirectional_{}_roots_depth_{depth}_{corpus_label}",
                    hydration_roots.len()
                );
                group.bench_function(&name, |b| {
                    b.iter(|| {
                        let rows = runtime
                        .block_on(
                            hubuum::benchmark_support::template_multi_root_bidirectional_objects(
                                &fixture.storage,
                                black_box(hydration_roots),
                                depth,
                                10,
                            ),
                        )
                        .expect("bidirectional template hydration should succeed");
                        debug_assert_eq!(rows.len(), hydration_roots.len() * depth as usize);
                        black_box(rows);
                    });
                });

                let name = format!(
                    "template_related_include_{}_roots_depth_{depth}_{corpus_label}",
                    hydration_roots.len()
                );
                let target_class_id =
                    fixture.structured_search.hydration_target_class_ids[(depth - 1) as usize];
                group.bench_function(&name, |b| {
                    b.iter(|| {
                        let rows = runtime
                            .block_on(hubuum::benchmark_support::template_related_include_objects(
                                &fixture.storage,
                                black_box(hydration_roots),
                                target_class_id,
                                depth,
                                1,
                            ))
                            .expect("related-object template include should succeed");
                        debug_assert_eq!(rows.len(), hydration_roots.len());
                        black_box(rows);
                    });
                });
            }
        };

        benchmark_hydration("base_corpus");

        let scale_setup_pool = {
            let _runtime_guard = runtime.enter();
            benchmark_pool(database.url())
        };
        fixture.structured_search.add_unrelated_hydration_corpus(
            &runtime,
            &scale_setup_pool,
            fixture.collections[0].id,
        );

        benchmark_hydration(&format!(
            "plus_{UNRELATED_HYDRATION_RELATIONS}_unrelated_relations"
        ));
    }
    group.finish();
    // The benchmark owns the whole disposable database container. Dropping it
    // is both faster and more representative than timing an unrelated cascade
    // delete of the synthetic scale corpus.
}

criterion_group!(benches, benchmark_postgres_storage);
criterion_main!(benches);
