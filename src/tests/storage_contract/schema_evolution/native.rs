use super::*;
use diesel::sql_types::{Integer, Text};
use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
use hubuum_storage_postgres::{capture_queries, with_connection, with_transaction};

#[rstest::rstest]
#[case::document("UPDATE class_schema_revisions SET json_schema='false'::jsonb WHERE class_id=$1")]
#[case::delete("DELETE FROM class_schema_revisions WHERE class_id=$1 AND revision=2")]
#[case::status(
    "UPDATE class_schema_revisions SET status='abandoned' WHERE class_id=$1 AND revision=1"
)]
#[case::projection("UPDATE class_schema_state SET active_revision=2 WHERE class_id=$1")]
#[actix_web::test]
async fn postgres_rejects_schema_revision_invariant_violations(#[case] sql: &'static str) {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![]).await;
    fixture.stage(json!({"type":"object"}), true).await;
    let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
        unreachable!()
    };
    let result = with_transaction(pool, async |connection| {
        diesel::sql_query(sql)
            .bind::<Integer, _>(fixture.class_id().id())
            .execute(connection)
            .await
            .map_err(hubuum_storage_postgres::PostgresStorageError::from)
    })
    .await;
    assert!(result.is_err());
    assert_eq!(
        fixture
            .backend
            .get_schema_state(fixture.class_id())
            .await
            .unwrap()
            .active()
            .reference()
            .revision(),
        SchemaRevision::INITIAL
    );
    fixture.cleanup().await;
}

#[actix_web::test]
async fn schema_activation_has_a_constant_query_budget_and_large_batches_bound_json() {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![]).await;
    let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
        unreachable!()
    };
    with_connection(pool,async |connection|{diesel::sql_query("INSERT INTO hubuumobject(name,description,collection_id,hubuum_class_id,data) SELECT 'large_'||n,'schema scale fixture',$1,$2,jsonb_build_object('payload',repeat('x',262144)) FROM generate_series(1,128) n")
        .bind::<Integer,_>(fixture.collection_id().id()).bind::<Integer,_>(fixture.class_id().id()).execute(connection).await.map_err(hubuum_storage_postgres::PostgresStorageError::from)}).await.unwrap();
    let revision = fixture.stage(json!({"required":["missing"]}), true).await;
    let (activation, capture) = capture_queries(fixture.activate(
        &revision,
        SchemaRevision::INITIAL,
        StorageSchemaActivationPolicy::AllowPending,
        None,
    ))
    .await;
    let activation = activation.unwrap();
    assert!(capture.domain_queries() <= 24, "{capture:?}");
    assert_eq!(
        capture.queries_matching("COUNT(*)"),
        0,
        "activation must not scan the object population"
    );
    let lease = fixture.claim(activation.task_id().unwrap(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(64, 1024 * 1024, 512 * 1024).unwrap();
    let (batch, capture) =
        capture_queries(fixture.backend.process_schema_work(lease, limits)).await;
    let batch = batch.unwrap();
    // Four serialized 256 KiB documents exceed the 1 MiB aggregate byte budget.
    assert_eq!(batch.examined(), 3);
    assert!(capture.domain_queries() <= 35, "{capture:?}");
    assert_eq!(capture.connection_checkouts(), 2);
    fixture.cleanup().await;
}

#[actix_web::test]
async fn postgres_worker_cannot_publish_evidence_for_an_object_changed_after_inspection() {
    struct ScanObserver(Notify);
    impl PostgresObserver for ScanObserver {
        fn operation_finished(
            &self,
            _: StorageCallSite,
            operation: &'static str,
            _: Duration,
            error: Option<StorageErrorKind>,
        ) {
            if operation == "transaction" && error.is_none() {
                self.0.notify_one();
            }
        }
    }
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({"n":1})]).await;
    let revision = fixture.stage(json!({"required":["n"]}), true).await;
    let activation = fixture
        .activate(
            &revision,
            SchemaRevision::INITIAL,
            StorageSchemaActivationPolicy::AllowPending,
            None,
        )
        .await
        .unwrap();
    let lease = fixture.claim(activation.task_id().unwrap(), 60_000).await;
    let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
        unreachable!()
    };
    let observer = Arc::new(ScanObserver(Notify::new()));
    let worker = AdapterPostgresStorage::new(pool.clone(), observer.clone());
    let task = with_transaction(pool, async |connection| {
        diesel::sql_query("SELECT id FROM hubuumclass WHERE id=$1 FOR UPDATE")
            .bind::<Integer, _>(fixture.class_id().id())
            .execute(&mut *connection)
            .await?;
        let task = tokio::spawn(async move {
            worker
                .process_schema_work(lease, StorageSchemaBatchLimits::default())
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), observer.0.notified())
            .await
            .expect("worker snapshot transaction completed");
        diesel::sql_query("UPDATE hubuumobject SET data=$2::jsonb WHERE id=$1")
            .bind::<Integer, _>(fixture.resources.objects[0].id().id())
            .bind::<Text, _>("{\"changed\":true}")
            .execute(connection)
            .await?;
        Ok::<_, hubuum_storage_postgres::PostgresStorageError>(task)
    })
    .await
    .unwrap();
    let result = task.await.unwrap().unwrap();
    assert_eq!(result.stale(), 1);
    assert_eq!(
        fixture.compliance().await[0].status(),
        StorageComplianceStatus::Pending
    );
    fixture.cleanup().await;
}

#[actix_web::test]
async fn postgres_rolls_back_schema_checkpoint_when_task_failure_cannot_commit() {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({})]).await;
    let revision = fixture.stage(json!(true), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let failure = StorageTaskFailure::new(
        lease.clone(),
        "worker failure",
        StorageTaskEventInput::new("failed", "worker failure"),
    );
    let error = PostgresFaultController::failing(PostgresFaultPoint::TaskFinalizeAfterEvent)
        .run(fixture.backend.fail_task(failure))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::Backend);
    let persisted = fixture
        .backend
        .get_schema_work(work.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&persisted).unwrap(),
        serde_json::to_value(&work).unwrap()
    );
    let task = fixture
        .backend
        .get_task_access(work.task_id())
        .await
        .unwrap()
        .into_parts()
        .0;
    assert!(task.status().is_active());
    fixture
        .finish_claimed(lease, StorageSchemaBatchLimits::default())
        .await;
    fixture.cleanup().await;
}
