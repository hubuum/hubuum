//! Opt-in diagnostic baseline for #521. Request timing includes authentication,
//! authorization and body serialization, but not network transport. Fixture size
//! is deliberately separate from unknown database scan work.

use std::time::Instant;

use actix_web::{App, http::StatusCode, middleware::from_fn, test};
use diesel::sql_types::{BigInt, Integer, Text};
use hubuum_storage_postgres::{
    PostgresConnection, PostgresPool, PostgresStorageError, capture_queries,
    diesel_async_prelude::*,
    test_support::{
        database_role_tests_enabled, integration_test_database_roles,
        integration_test_migration_pool,
    },
    with_connection, with_transaction,
};
use rstest::rstest;
use serde_json::{Value, json};

use crate::models::NewHubuumClass;
use crate::tests::{CollectionFixture, TestContext};
use crate::traits::CanSave;

const ROUNDS: usize = 5;
const WARMUPS: usize = 2;
const SAMPLES: usize = 10;
const PAGE_SIZE: usize = 25;

#[derive(Clone, Copy, Debug)]
enum Operation {
    Aggregate,
    History,
    Audit,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Aggregate => "object_aggregate_groups",
            Self::History => "collection_history",
            Self::Audit => "collection_audit",
        }
    }
}

struct Fixture {
    context: TestContext,
    fixture_pool: PostgresPool,
    collection: CollectionFixture,
    operation: Operation,
    resource_id: i32,
}

#[derive(QueryableByName)]
struct SourceSize {
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    row_json_bytes: i64,
}

#[derive(QueryableByName)]
struct DatabaseVersion {
    #[diesel(sql_type = Text)]
    server_version: String,
}

async fn assume_fixture_owner(
    connection: &mut PostgresConnection,
) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        let roles = integration_test_database_roles();
        diesel::sql_query("SELECT set_config('role', $1, true)")
            .bind::<Text, _>(roles.owner().as_str())
            .execute(connection)
            .await?;
    }
    Ok(())
}

impl Fixture {
    async fn new(operation: Operation, rows: i32) -> Self {
        let context = TestContext::new().await;
        let fixture_pool = integration_test_migration_pool(1);
        let collection = context
            .collection_fixture("api_admission_measurement")
            .await;
        collection
            .owner_group
            .add_member_without_events(&context.pool, &context.normal_user)
            .await
            .unwrap();
        let collection_id = collection.collection.id;
        let resource_id = match operation {
            Operation::Aggregate => {
                let class = NewHubuumClass {
                    name: context.scoped_name("admission_class"),
                    collection_id,
                    description: "admission measurement".into(),
                    json_schema: None,
                    validate_schema: Some(false),
                }
                .save_without_events(&context.pool)
                .await
                .unwrap();
                with_transaction(&fixture_pool, async |connection| {
                    assume_fixture_owner(connection).await?;
                    diesel::sql_query(
                        "INSERT INTO hubuumobject \
                         (name, description, collection_id, hubuum_class_id, data) \
                         SELECT 'object-' || lpad(n::text, 5, '0'), 'measurement', $1, $2, \
                         jsonb_build_object('blob', repeat(md5(n::text), 8)) \
                         FROM generate_series(1, $3) n",
                    )
                    .bind::<Integer, _>(collection_id)
                    .bind::<Integer, _>(class.id)
                    .bind::<Integer, _>(rows)
                    .execute(connection)
                    .await?;
                    Ok::<_, PostgresStorageError>(())
                })
                .await
                .unwrap();
                class.id
            }
            Operation::History => {
                // Real updates exercise the installed history/identity triggers.
                // Only fixture-generated positive integers enter this SQL.
                with_transaction(&fixture_pool, async |connection| {
                    assume_fixture_owner(connection).await?;
                    diesel::sql_query(
                        "SELECT set_config('hubuum.actor_id', $1, true), \
                         set_config('hubuum.actor_kind', 'user', true)",
                    )
                    .bind::<Text, _>(context.normal_user.id.to_string())
                    .execute(connection)
                    .await?;
                    diesel::sql_query(format!(
                        "DO $fixture$ BEGIN FOR n IN 1..{} LOOP \
                         UPDATE collections SET description = repeat(md5(n::text), 8) \
                         WHERE id = {collection_id}; END LOOP; END $fixture$",
                        rows - 1,
                    ))
                    .execute(connection)
                    .await?;
                    Ok::<_, PostgresStorageError>(())
                })
                .await
                .unwrap();
                collection_id
            }
            Operation::Audit => {
                // Synthetic read fixture: no delivery worker runs. Identity and
                // audit constraints remain enabled; no timings include seeding.
                with_transaction(&fixture_pool, async |connection| {
                    assume_fixture_owner(connection).await?;
                    diesel::sql_query("SET LOCAL events.allow_purge = 'on'")
                        .execute(connection).await?;
                    diesel::sql_query("DELETE FROM events WHERE collection_id = $1")
                        .bind::<Integer, _>(collection_id).execute(connection).await?;
                    diesel::sql_query(
                        "INSERT INTO events \
                         (event_id, entity_type, entity_id, collection_id, action, actor_kind, summary, metadata, actor_user_id) \
                         SELECT gen_random_uuid(), 'collection', $1, $1, 'updated', 'user', \
                         'measurement', jsonb_build_object('blob', repeat(md5(n::text), 8)), $3 \
                         FROM generate_series(1, $2) n",
                    )
                    .bind::<Integer, _>(collection_id)
                    .bind::<Integer, _>(rows)
                    .bind::<Integer, _>(context.normal_user.id)
                    .execute(connection)
                    .await?;
                    Ok::<_, PostgresStorageError>(())
                })
                .await
                .unwrap();
                collection_id
            }
        };
        Self {
            context,
            fixture_pool,
            collection,
            operation,
            resource_id,
        }
    }

    fn endpoint(&self, include_total: bool) -> String {
        let path = match self.operation {
            Operation::Aggregate => format!(
                "/api/v1/classes/{}/object-aggregates?group_by=name&",
                self.resource_id,
            ),
            Operation::History => format!(
                "/api/v1/collections/{}/history?sort=history_id.desc&",
                self.resource_id,
            ),
            Operation::Audit => format!(
                "/api/v1/events?collection_id={}&sort=id.desc&",
                self.resource_id,
            ),
        };
        format!("{path}limit={PAGE_SIZE}&include_total={include_total}")
    }

    fn source_relation(&self) -> (&'static str, &'static str) {
        match self.operation {
            Operation::Aggregate => ("hubuumobject", "hubuum_class_id"),
            Operation::History => ("collections_history", "id"),
            Operation::Audit => ("events", "collection_id"),
        }
    }

    async fn source_size(&self) -> SourceSize {
        let (table, column) = self.source_relation();
        with_connection(&self.context.pool, async |connection| {
            diesel::sql_query(format!(
                "SELECT count(*)::bigint AS rows, \
                 coalesce(sum(octet_length(row_to_json(r)::text)), 0)::bigint AS row_json_bytes \
                 FROM {table} r WHERE {column} = $1",
            ))
            .bind::<Integer, _>(self.resource_id)
            .get_result(connection)
            .await
        })
        .await
        .unwrap()
    }

    async fn cleanup(self) {
        self.collection.cleanup().await.unwrap();
        // Normal domain deletion intentionally retains history/audit. Remove
        // this probe's rows explicitly so later rounds do not inherit them.
        let collection_id = self.collection.collection.id;
        with_transaction(&self.fixture_pool, async |connection| {
            assume_fixture_owner(connection).await?;
            diesel::sql_query("SET LOCAL events.allow_purge = 'on'")
                .execute(connection)
                .await?;
            for sql in [
                "DELETE FROM events WHERE collection_id = $1",
                "DELETE FROM hubuumobject_history WHERE collection_id = $1",
                "DELETE FROM hubuumclass_history WHERE collection_id = $1",
                "DELETE FROM collections_history WHERE id = $1",
            ] {
                diesel::sql_query(sql)
                    .bind::<Integer, _>(collection_id)
                    .execute(connection)
                    .await?;
            }
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        self.context
            .normal_user
            .delete_without_events(&self.context.pool)
            .await
            .unwrap();
        self.context
            .admin_user
            .delete_without_events(&self.context.pool)
            .await
            .unwrap();
    }
}

#[rstest]
#[case::aggregate(Operation::Aggregate)]
#[case::history(Operation::History)]
#[case::audit(Operation::Audit)]
#[actix_web::test]
#[ignore = "diagnostic baseline: run serially on an isolated disposable database; see docs/performance/api_admission.md"]
async fn api_admission_measurement(
    #[case] operation: Operation,
    #[values(1_000, 10_000)] source_rows: i32,
    #[values(false, true)] include_total: bool,
) {
    for round in 0..ROUNDS {
        let fixture = Fixture::new(operation, source_rows).await;
        with_transaction(&fixture.fixture_pool, async |connection| {
            assume_fixture_owner(connection).await?;
            diesel::sql_query(format!("ANALYZE {}", fixture.source_relation().0))
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        let source_size = fixture.source_size().await;
        assert_eq!(source_size.rows, i64::from(source_rows));
        let version = with_connection(&fixture.context.pool, async |connection| {
            diesel::sql_query("SHOW server_version")
                .get_result::<DatabaseVersion>(connection)
                .await
        })
        .await
        .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(crate::tests::app_context(&fixture.context.pool))
                .wrap(from_fn(crate::middlewares::actor_context))
                .configure(crate::api::config),
        )
        .await;
        let endpoint = fixture.endpoint(include_total);
        let mut samples = Vec::with_capacity(SAMPLES);
        for sample in 0..WARMUPS + SAMPLES {
            let request = test::TestRequest::get()
                .uri(&endpoint)
                .insert_header((
                    "Authorization",
                    format!("Bearer {}", fixture.context.normal_token),
                ))
                .to_request();
            let started = Instant::now();
            let ((status, total, body), queries) = capture_queries(async {
                let response = test::call_service(&app, request).await;
                let status = response.status();
                let total = response
                    .headers()
                    .get("x-total-count")
                    .map(|value| value.to_str().unwrap().parse::<i64>().unwrap());
                (status, total, test::read_body(response).await)
            })
            .await;
            let elapsed_us = started.elapsed().as_micros();
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
            let returned: Vec<Value> = serde_json::from_slice(&body).unwrap();
            assert_eq!(returned.len(), PAGE_SIZE);
            assert_eq!(total, include_total.then_some(i64::from(source_rows)));
            if matches!(operation, Operation::History | Operation::Audit) {
                assert!(
                    returned.iter().all(|row| row
                        .pointer("/provenance/actor/principal/name")
                        .and_then(Value::as_str)
                        .is_some()),
                    "history/audit samples must include principal enrichment"
                );
            }
            if sample >= WARMUPS {
                samples.push(json!({
                    "elapsed_us": elapsed_us,
                    "total_queries": queries.total_queries(),
                    "domain_queries": queries.domain_queries(),
                    "control_queries": queries.control_queries(),
                    "connection_checkouts": queries.connection_checkouts(),
                    "response_rows": returned.len(),
                    "response_body_bytes": body.len(),
                }));
            }
        }
        eprintln!(
            "API_ADMISSION_EVIDENCE {}",
            json!({
                "format_version": 1,
                "recipe": "api-admission-v1",
                "operation": operation.name(),
                "round": round + 1,
                "rounds": ROUNDS,
                "warmups": WARMUPS,
                "page_limit": PAGE_SIZE,
                "include_total": include_total,
                "authorization": "local_ordinary_collection_member",
                "postgres_version": version.server_version,
                "source_rows": source_size.rows,
                "source_row_json_bytes": source_size.row_json_bytes,
                "database_scanned_rows": null,
                "database_scanned_bytes": null,
                "samples": samples,
            })
        );
        fixture.cleanup().await;
    }
}
