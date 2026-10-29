#![cfg(feature = "integration-test-support")]

use std::panic::{AssertUnwindSafe, resume_unwind};

use diesel::QueryableByName;
use diesel::sql_types::{Binary, Integer, Uuid as SqlUuid};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use futures_util::FutureExt;
use hubuum_artifacts::{
    ArtifactError, ArtifactIdentity, ArtifactLimits, ArtifactManifest, ArtifactReader,
    ArtifactReference, ArtifactStore, ArtifactWriter, AttemptId, Destination, DestinationId,
    ManifestBuilder, verify, verify_and_publish,
};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_database_url,
    integration_test_migration_pool, integration_test_pool,
};
use hubuum_storage_postgres::{
    PostgresArtifactStore, PostgresPool, PostgresPoolSettings, PostgresStorage,
    PostgresStorageError, build_postgres_pool, with_connection, with_transaction,
};
use rstest::rstest;
use uuid::Uuid;

/// Each test owns a schema and pools; teardown runs even after an assertion
/// panic. Tests cannot erase another test's artifacts or change public triggers.
struct TestContext {
    schema: String,
    admin: PostgresPool,
    pool: PostgresPool,
    store: PostgresArtifactStore,
    destination: Destination,
}

impl TestContext {
    async fn run(test: impl for<'a> AsyncFnOnce(&'a Self)) {
        let context = Self::new().await;
        let result = AssertUnwindSafe(test(&context)).catch_unwind().await;
        context
            .admin_sql(&format!("DROP SCHEMA {} CASCADE", context.schema))
            .await;
        if let Err(panic) = result {
            resume_unwind(panic);
        }
    }

    async fn new() -> Self {
        let schema = format!("artifact_test_{}", Uuid::new_v4().simple());
        let admin = if database_role_tests_enabled() {
            integration_test_migration_pool(2)
        } else {
            integration_test_pool(2)
        };
        with_transaction(&admin, async |connection| -> Result<(), PostgresStorageError> {
            if database_role_tests_enabled() {
                connection.batch_execute(&format!("SET LOCAL ROLE {}", integration_test_database_roles().owner().as_str())).await?;
            }
            connection.batch_execute(&format!("CREATE SCHEMA {schema}; SET LOCAL search_path TO {schema};")).await?;
            connection.batch_execute(include_str!("../migrations/2026-10-27-000002_artifact_staging/up.sql")).await?;
            if database_role_tests_enabled() {
                let runtime = integration_test_database_roles().runtime().as_str().to_owned();
                connection.batch_execute(&format!("GRANT USAGE ON SCHEMA {schema} TO {runtime}; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {runtime}; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {runtime};")).await?;
            }
            Ok(())
        }).await.unwrap();
        let mut url = integration_test_database_url();
        url.push(if url.contains('?') { '&' } else { '?' });
        url.push_str(&format!("options=-csearch_path%3D{schema}%2Cpublic"));
        let settings = PostgresPoolSettings::builder(url)
            .max_size(3)
            .statement_timeout_ms(5000)
            .acquire_timeout_ms(5000)
            .build()
            .unwrap();
        let pool = build_postgres_pool(&settings).unwrap();
        let store = Self::store(&pool);
        let destination = Destination::new(DestinationId::generate(), "test-pg").unwrap();
        store.register_destination(&destination).await.unwrap();
        Self {
            schema,
            admin,
            pool,
            store,
            destination,
        }
    }

    fn store(pool: &PostgresPool) -> PostgresArtifactStore {
        PostgresStorage::unobserved(pool.clone()).artifact_store()
    }

    async fn admin_sql(&self, sql: &str) {
        with_transaction(
            &self.admin,
            async |connection| -> Result<(), PostgresStorageError> {
                if database_role_tests_enabled() {
                    connection
                        .batch_execute(&format!(
                            "SET LOCAL ROLE {}",
                            integration_test_database_roles().owner().as_str()
                        ))
                        .await?;
                }
                connection
                    .batch_execute(&format!("SET LOCAL search_path TO {}, public", self.schema))
                    .await?;
                connection.batch_execute(sql).await?;
                Ok(())
            },
        )
        .await
        .unwrap();
    }

    fn identity(&self) -> ArtifactIdentity {
        ArtifactIdentity::new(self.destination.id())
    }

    async fn seal(&self, bytes: &[u8]) -> ArtifactManifest {
        let id = self.identity();
        self.store.stage(&id, limits()).await.unwrap();
        let mut builder = ManifestBuilder::new(id.clone(), limits());
        for (ordinal, block) in bytes.chunks(4).enumerate() {
            self.store.append(&id, ordinal as u32, block).await.unwrap();
            builder.push(block).unwrap();
        }
        let manifest = builder.finish().unwrap();
        self.store.seal(&manifest).await.unwrap();
        manifest
    }
}

fn limits() -> ArtifactLimits {
    ArtifactLimits::builder()
        .block_bytes(4)
        .artifact_bytes(12)
        .blocks(3)
        .build()
        .unwrap()
}

#[rstest]
#[case(b"".as_slice())]
#[case(b"abcd".as_slice())]
#[case(b"abcdefghij".as_slice())]
#[tokio::test]
async fn published_bytes_survive_a_new_adapter_instance(#[case] bytes: &[u8]) {
    TestContext::run(async |context| {
        let mut writer = ArtifactWriter::new(&context.store, context.identity(), limits())
            .await
            .unwrap();
        for fragment in bytes.chunks(3) {
            writer.write(fragment).await.unwrap();
        }
        let reference = writer.finish().await.unwrap();
        let other = TestContext::store(&context.pool);
        let mut reader = ArtifactReader::open(&other, &reference).await.unwrap();
        let mut actual = Vec::new();
        while let Some(bytes) = reader.next_block().await.unwrap() {
            actual.extend(bytes);
        }
        assert_eq!(actual, bytes);
    })
    .await;
}

#[tokio::test]
async fn sealed_attempt_can_resume_verification_on_another_replica() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcdefghij").await;
        let other = TestContext::store(&context.pool);
        let persisted = other.manifest(manifest.identity()).await.unwrap();
        let recovered =
            ArtifactManifest::decode(&persisted, manifest.identity(), limits()).unwrap();
        let reference = verify_and_publish(&other, &recovered).await.unwrap();
        assert!(
            ArtifactReader::open(&context.store, &reference)
                .await
                .is_ok()
        );
    })
    .await;
}

#[tokio::test]
async fn sealed_attempt_is_unavailable_to_downloads() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        let reference = ArtifactReference::from_catalog(
            manifest.identity().clone(),
            manifest.manifest_digest(),
        );
        assert!(
            ArtifactReader::open(&context.store, &reference)
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn concurrent_identical_retries_store_one_block() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        let (a, b) = tokio::join!(
            context.store.append(&id, 0, b"abcd"),
            context.store.append(&id, 0, b"abcd")
        );
        a.unwrap();
        b.unwrap();
        let mut builder = ManifestBuilder::new(id, limits());
        builder.push(b"abcd").unwrap();
        let manifest = builder.finish().unwrap();
        context.store.seal(&manifest).await.unwrap();
        assert!(verify_and_publish(&context.store, &manifest).await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn conflicting_block_retry_cannot_overwrite_staging() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        let (a, b) = tokio::join!(
            context.store.append(&id, 0, b"abcd"),
            context.store.append(&id, 0, b"efgh")
        );
        assert_ne!(a.is_ok(), b.is_ok());
    })
    .await;
}

#[tokio::test]
async fn new_attempt_cannot_claim_an_existing_artifact() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        let forged = ArtifactIdentity::from_parts(
            id.artifact(),
            AttemptId::generate(),
            id.destination(),
            id.version(),
        );
        assert!(context.store.stage(&forged, limits()).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn named_destination_cannot_be_rebound() {
    TestContext::run(async |context| {
        let replacement =
            Destination::new(DestinationId::generate(), context.destination.name()).unwrap();
        assert!(
            context
                .store
                .register_destination(&replacement)
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn aborting_an_unknown_attempt_does_not_claim_a_durable_fence() {
    TestContext::run(async |context| {
        assert_eq!(
            context.store.abort(&context.identity()).await,
            Err(ArtifactError::NotFound)
        );
    })
    .await;
}

#[tokio::test]
async fn abort_and_publication_have_one_winner() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        let (published, aborted) = tokio::join!(
            verify_and_publish(&context.store, &manifest),
            context.store.abort(manifest.identity())
        );
        assert_ne!(published.is_ok(), aborted.is_ok());
    })
    .await;
}

#[tokio::test]
async fn verified_proof_cannot_bypass_a_later_cancellation() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        let proof = verify(&context.store, &manifest).await.unwrap();
        context.store.abort(manifest.identity()).await.unwrap();
        assert!(context.store.publish(&proof).await.is_err());
    })
    .await;
}

#[rstest]
#[case("UPDATE artifact_chunks SET content='corrupt'")]
#[case("DELETE FROM artifact_chunks")]
#[case("UPDATE artifact_attempts SET manifest='corrupt'")]
#[case("UPDATE artifact_attempts SET state='staging'")]
#[case("UPDATE artifact_attempts SET attempt_id=gen_random_uuid()")]
#[case("DELETE FROM artifact_attempts")]
#[case("DELETE FROM artifact_destinations")]
#[tokio::test]
async fn database_fences_protect_published_content(#[case] sql: &str) {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        verify_and_publish(&context.store, &manifest).await.unwrap();
        let result = with_connection(&context.pool, async |connection| {
            diesel::sql_query(sql).execute(connection).await
        })
        .await;
        assert!(result.is_err());
    })
    .await;
}

#[tokio::test]
async fn corrupted_destination_cannot_publish_echoed_metadata() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        context.admin_sql("ALTER TABLE artifact_chunks DISABLE TRIGGER USER; UPDATE artifact_chunks SET content='efgh'; ALTER TABLE artifact_chunks ENABLE TRIGGER USER;").await;
        assert!(verify_and_publish(&context.store, &manifest).await.is_err());
    }).await;
}

#[tokio::test]
async fn expiry_fences_publication_and_retains_cleanup_obligations() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcd").await;
        context.admin_sql("ALTER TABLE artifact_attempts DISABLE TRIGGER USER; UPDATE artifact_attempts SET expires_at=clock_timestamp() - interval '1 second'; ALTER TABLE artifact_attempts ENABLE TRIGGER USER;").await;
        assert!(verify_and_publish(&context.store, &manifest).await.is_err());
        assert_eq!(context.store.cleanup().await.unwrap(), 1);
    }).await;
}

#[tokio::test]
async fn cancellation_cleanup_is_bounded_and_retryable() {
    TestContext::run(async |context| {
        let id = context.identity();
        let limits = ArtifactLimits::builder()
            .block_bytes(1)
            .artifact_bytes(17)
            .blocks(17)
            .build()
            .unwrap();
        context.store.stage(&id, limits).await.unwrap();
        for ordinal in 0..17 {
            context.store.append(&id, ordinal, b"x").await.unwrap();
        }
        context.store.abort(&id).await.unwrap();
        assert_eq!(context.store.cleanup().await.unwrap(), 16);
        assert_eq!(context.store.cleanup().await.unwrap(), 1);
        assert_eq!(context.store.cleanup().await.unwrap(), 0);
        context.store.abort(&id).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn delayed_stage_retry_cannot_resurrect_a_discarded_attempt() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        context.store.append(&id, 0, b"abcd").await.unwrap();
        context.store.abort(&id).await.unwrap();
        context.store.cleanup().await.unwrap();
        assert!(context.store.stage(&id, limits()).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn partial_final_block_cannot_be_extended() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        context.store.append(&id, 0, b"ab").await.unwrap();
        assert!(context.store.append(&id, 1, b"cd").await.is_err());
    })
    .await;
}

#[tokio::test]
async fn missing_final_block_fails_download() {
    TestContext::run(async |context| {
        let manifest = context.seal(b"abcdefghij").await;
        let reference = verify_and_publish(&context.store, &manifest).await.unwrap();
        context.admin_sql("ALTER TABLE artifact_chunks DISABLE TRIGGER USER; DELETE FROM artifact_chunks WHERE ordinal=2; ALTER TABLE artifact_chunks ENABLE TRIGGER USER;").await;
        let mut reader = ArtifactReader::open(&context.store, &reference).await.unwrap();
        reader.next_block().await.unwrap();
        reader.next_block().await.unwrap();
        assert!(reader.next_block().await.is_err());
    }).await;
}

#[tokio::test]
async fn chunk_size_is_enforced_by_database_constraints() {
    TestContext::run(async |context| {
        let id = context.identity();
        context.store.stage(&id, limits()).await.unwrap();
        let result = with_connection(&context.pool, async |connection| {
            diesel::sql_query(
                "INSERT INTO artifact_chunks(artifact_id, ordinal, content) VALUES ($1,0,$2)",
            )
            .bind::<SqlUuid, _>(Uuid::from_bytes(*id.artifact().as_bytes()))
            .bind::<Binary, _>(b"12345".as_slice())
            .execute(connection)
            .await
        })
        .await;
        assert!(result.is_err());
    })
    .await;
}

#[tokio::test]
async fn migration_can_be_reverted_before_feature_activation() {
    TestContext::run(async |context| {
        context.admin_sql(include_str!("../migrations/2026-10-27-000002_artifact_staging/down.sql")).await;
        #[derive(QueryableByName)]
        struct Count { #[diesel(sql_type = Integer)] count: i32 }
        let count = with_connection(&context.pool, async |connection| {
            diesel::sql_query("SELECT count(*)::int AS count FROM information_schema.tables WHERE table_schema=current_schema() AND table_name LIKE 'artifact_%'").get_result::<Count>(connection).await
        }).await.unwrap();
        assert_eq!(count.count, 0);
    }).await;
}
