use super::*;
use diesel::sql_types::{BigInt, Integer, Text};
use hubuum_storage_postgres::diesel_async_prelude::RunQueryDsl;
use hubuum_storage_postgres::{capture_queries, with_connection, with_transaction};

impl SchemaFixture {
    async fn set_worker_timezone(&mut self, timezone: &str) {
        let config = crate::tests::integration_test_config().unwrap();
        // A dedicated single-connection pool keeps the zone local to this fixture.
        let pool = crate::tests::postgres_test_pool(&config.database_url, 1);
        with_connection(&pool, async |connection| {
            diesel::sql_query("SELECT set_config('TimeZone', $1, false)")
                .bind::<Text, _>(timezone)
                .execute(connection)
                .await
        })
        .await
        .unwrap();
        self.backend =
            StorageHandle::from_registered_backend(PostgresStorage::unobserved(pool.clone()));
        self.environment = BackendTestEnvironment::Postgres { pool };
    }
}

#[rstest::rstest]
#[case::utc("UTC")]
#[case::positive_offset("Asia/Kolkata")]
#[case::negative_offset("America/New_York")]
#[actix_web::test]
async fn nonterminal_schema_task_timestamps_are_utc(
    #[case] timezone: &str,
    #[values(StorageSchemaWorkKind::Impact, StorageSchemaWorkKind::Revalidation)]
    kind: StorageSchemaWorkKind,
) {
    let mut fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({}); 2]).await;
    let revision = fixture.stage(json!(true), true).await;
    let task_id = match kind {
        StorageSchemaWorkKind::Impact => {
            fixture.request(revision.reference(), kind).await.task_id()
        }
        StorageSchemaWorkKind::Revalidation => fixture
            .activate(
                &revision,
                SchemaRevision::INITIAL,
                StorageSchemaActivationPolicy::AllowPending,
                None,
            )
            .await
            .unwrap()
            .task_id()
            .unwrap(),
    };
    let lease = fixture.claim(task_id, 60_000).await;
    fixture.set_worker_timezone(timezone).await;
    let before = chrono::Utc::now();
    let work = fixture
        .backend
        .process_schema_work(
            lease,
            StorageSchemaBatchLimits::try_new(1, 1024, 1024).unwrap(),
        )
        .await
        .unwrap();
    let after = chrono::Utc::now();
    assert_eq!(work.status(), StorageSchemaWorkStatus::Running);
    assert_eq!(work.examined(), 1);

    // Read the generic task before renewal or completion can repair updated_at.
    let access = fixture.backend.get_task_access(task_id).await;
    fixture.cleanup().await;

    let (task, _) = access
        .expect("nonterminal task must remain readable")
        .into_parts();
    assert!(task.status().is_active());
    assert_eq!(task.progress().processed(), 1);
    assert!(task.updated_at() >= task.started_at().expect("claimed task has started"));
    assert!(
        task.updated_at() >= before && task.updated_at() <= after,
        "{timezone}, {kind:?}: {}",
        task.updated_at()
    );
}

#[rstest::rstest]
#[case::completed_utc("UTC", false)]
#[case::completed_positive_offset("Asia/Kolkata", false)]
#[case::completed_negative_offset("America/New_York", false)]
#[case::deleted_utc("UTC", true)]
#[case::deleted_positive_offset("Asia/Kolkata", true)]
#[case::deleted_negative_offset("America/New_York", true)]
#[actix_web::test]
async fn terminal_schema_task_timestamps_are_utc(
    #[case] timezone: &str,
    #[case] delete_class: bool,
) {
    let mut fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![]).await;
    let revision = fixture.stage(json!(true), true).await;
    let work = fixture
        .request(revision.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let task_id = work.task_id();
    let lease = if delete_class {
        None
    } else {
        Some(fixture.claim(task_id, 60_000).await)
    };
    // Complete or delete an existing task from a worker connection in another zone.
    fixture.set_worker_timezone(timezone).await;
    let backend = fixture.backend.clone();
    let before = chrono::Utc::now();
    if let Some(lease) = lease {
        fixture
            .finish_claimed(lease, StorageSchemaBatchLimits::default())
            .await;
    }
    fixture.cleanup().await;
    let after = chrono::Utc::now();
    // Loading validates the terminal timestamps against created_at and updated_at.
    let (task, _) = backend.get_task_access(task_id).await.unwrap().into_parts();
    assert_eq!(
        task.status(),
        if delete_class {
            StorageTaskStatus::Cancelled
        } else {
            StorageTaskStatus::Succeeded
        }
    );
    for timestamp in [task.finished_at(), task.request_redacted_at()] {
        let timestamp = timestamp.expect("terminal task timestamp");
        assert!(
            timestamp >= before && timestamp <= after,
            "{timezone}: {timestamp}"
        );
    }
}

#[actix_web::test]
async fn impact_report_polling_does_not_recount_or_read_objects() {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({}); 16]).await;
    let candidate = fixture.stage(json!({"type":"object"}), true).await;
    let work = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let (report, capture) = capture_queries(crate::services::schema_evolution::get_work(
        &fixture.backend,
        work.task_id(),
    ))
    .await;
    report.unwrap();
    assert!(capture.domain_queries() <= 2, "{capture:?}");
    assert_eq!(capture.queries_matching("hubuumobject"), 0);
    assert_eq!(capture.queries_matching("count("), 0);
    fixture.cleanup().await;
}

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

#[rstest::rstest]
#[case::impact(StorageSchemaWorkKind::Impact)]
#[case::revalidation(StorageSchemaWorkKind::Revalidation)]
#[actix_web::test]
async fn postgres_worker_cannot_publish_results_for_an_object_changed_after_inspection(
    #[case] kind: StorageSchemaWorkKind,
) {
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
    let revision = fixture.stage(json!({"required":["missing"]}), true).await;
    let task_id = if kind == StorageSchemaWorkKind::Impact {
        fixture.request(revision.reference(), kind).await.task_id()
    } else {
        fixture
            .activate(
                &revision,
                SchemaRevision::INITIAL,
                StorageSchemaActivationPolicy::AllowPending,
                None,
            )
            .await
            .unwrap()
            .task_id()
            .unwrap()
    };
    let lease = fixture.claim(task_id, 60_000).await;
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
    if kind == StorageSchemaWorkKind::Impact {
        let report = crate::services::schema_evolution::get_work(&fixture.backend, task_id)
            .await
            .unwrap();
        assert!(report.impact.unwrap().failures.is_empty());
    } else {
        assert_eq!(
            fixture.compliance().await[0].status(),
            StorageComplianceStatus::Pending
        );
    }
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

#[rstest::rstest]
#[case::four_batches(256)]
#[case::thirty_two_batches(2048)]
#[actix_web::test]
async fn schema_impact_batch_traffic_is_independent_of_prior_findings(#[case] objects: i32) {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![]).await;
    let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
        unreachable!()
    };
    with_connection(pool, async |connection| {
        diesel::sql_query("INSERT INTO hubuumobject(name,description,collection_id,hubuum_class_id,data) SELECT 'mismatch_'||n,'impact traffic fixture',$1,$2,'{}'::jsonb FROM generate_series(1,$3) n")
            .bind::<Integer,_>(fixture.collection_id().id()).bind::<Integer,_>(fixture.class_id().id()).bind::<Integer,_>(objects)
            .execute(connection).await.map_err(hubuum_storage_postgres::PostgresStorageError::from)
    }).await.unwrap();
    let candidate = fixture.stage(json!({"required":["missing"]}), true).await;
    let work = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 300_000).await;
    let mut total_bytes = 0;
    for batch in 1..=objects / 64 {
        let (work, capture) = capture_queries(
            fixture
                .backend
                .process_schema_work(lease.clone(), StorageSchemaBatchLimits::default()),
        )
        .await;
        let work = work.unwrap();
        assert_eq!(work.examined(), (batch * 64) as u64);
        assert_eq!(
            capture.queries_matching("FROM schema_impact_findings"),
            0,
            "workers must not reload prior findings"
        );
        assert_eq!(
            capture.queries_matching("INSERT INTO schema_impact_findings"),
            1
        );
        let checkpoint_bytes = capture.rendered_bytes_matching("UPDATE schema_validation_work");
        let finding_bytes = capture.rendered_bytes_matching("INSERT INTO schema_impact_findings");
        assert!(
            checkpoint_bytes > 0 && checkpoint_bytes < 4096,
            "batch {batch}: checkpoint writes grew to {checkpoint_bytes} bytes"
        );
        // Each fixture finding now includes its revision, time, and one diagnostic.
        // The per-batch ceiling must remain independent of already saved batches.
        assert!(
            finding_bytes > 0 && finding_bytes < 64 * 1024,
            "batch {batch}: finding writes grew to {finding_bytes} bytes"
        );
        total_bytes += checkpoint_bytes + finding_bytes;
    }
    assert!(
        total_bytes < objects as usize * 1088,
        "traffic must grow linearly with inspected objects"
    );
    let (report, capture) = capture_queries(crate::services::schema_evolution::get_work(
        &fixture.backend,
        work.task_id(),
    ))
    .await;
    let report = report.unwrap();
    assert!(capture.domain_queries() <= 3, "{capture:?}");
    assert_eq!(capture.queries_matching("hubuumobject"), 0);
    assert_eq!(
        report.impact.unwrap().failures[0].samples.len(),
        objects as usize
    );
    fixture.cleanup().await;
}

#[actix_web::test]
async fn failed_schema_batch_rolls_back_findings_with_its_checkpoint() {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type=BigInt)]
        count: i64,
    }
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({})]).await;
    let candidate = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let result = PostgresFaultController::failing(PostgresFaultPoint::SchemaImpactAfterFindings)
        .run(
            fixture
                .backend
                .process_schema_work(lease.clone(), StorageSchemaBatchLimits::default()),
        )
        .await;
    assert_eq!(result.unwrap_err().kind(), StorageErrorKind::Backend);
    let checkpoint = fixture
        .backend
        .get_schema_work(work.task_id())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(checkpoint).unwrap(),
        serde_json::to_value(&work).unwrap()
    );
    let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
        unreachable!()
    };
    let rows = with_connection(pool, async |connection| {
        diesel::sql_query("SELECT count(*) AS count FROM schema_impact_findings WHERE task_id=$1")
            .bind::<Integer, _>(work.task_id().id())
            .get_result::<Count>(connection)
            .await
            .map_err(hubuum_storage_postgres::PostgresStorageError::from)
    })
    .await
    .unwrap();
    assert_eq!(rows.count, 0);
    fixture
        .finish_claimed(lease, StorageSchemaBatchLimits::default())
        .await;
    let report = crate::services::schema_evolution::get_work(&fixture.backend, work.task_id())
        .await
        .unwrap();
    assert_eq!(
        report.impact.unwrap().failures[0].samples,
        vec![fixture.resources.objects[0].id().id()]
    );
    fixture.cleanup().await;
}

#[rstest::rstest]
#[case::next_batch(false)]
#[case::work_removed(true)]
#[actix_web::test]
async fn schema_report_reads_findings_from_the_checkpoints_snapshot(#[case] remove_work: bool) {
    let fixture = SchemaFixture::new(StorageBackendKind::Postgres, vec![json!({}); 2]).await;
    let candidate = fixture.stage(json!(false), true).await;
    let work = fixture
        .request(candidate.reference(), StorageSchemaWorkKind::Impact)
        .await;
    let lease = fixture.claim(work.task_id(), 60_000).await;
    let limits = StorageSchemaBatchLimits::try_new(1, 2048, 1024).unwrap();
    fixture
        .backend
        .process_schema_work(lease.clone(), limits)
        .await
        .unwrap();
    let gate = PostgresFaultController::pausing(PostgresFaultPoint::SchemaReportAfterCheckpoint);
    let backend = fixture.backend.clone();
    let reader_gate = gate.clone();
    let task_id = work.task_id();
    let reader = tokio::spawn(async move {
        reader_gate
            .run(backend.get_schema_work_report(task_id))
            .await
    });
    tokio::time::timeout(Duration::from_secs(10), gate.wait_until_reached())
        .await
        .unwrap();
    if remove_work {
        let BackendTestEnvironment::Postgres { pool } = &fixture.environment else {
            unreachable!()
        };
        with_connection(pool, async |connection| {
            diesel::sql_query("DELETE FROM tasks WHERE id=$1")
                .bind::<Integer, _>(task_id.id())
                .execute(connection)
                .await
                .map_err(hubuum_storage_postgres::PostgresStorageError::from)
        })
        .await
        .unwrap();
    } else {
        fixture
            .backend
            .process_schema_work(lease, limits)
            .await
            .unwrap();
    }
    gate.resume();
    let report = reader.await.unwrap().unwrap();
    assert_eq!(report.work().examined(), 1);
    let impact = serde_json::to_value(report.impact().unwrap()).unwrap();
    assert_eq!(
        impact["failures"][0]["samples"],
        json!([fixture.resources.objects[0].id().id()])
    );
    fixture.cleanup().await;
}
