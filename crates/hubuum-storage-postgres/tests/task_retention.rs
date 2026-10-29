#![cfg(feature = "integration-test-support")]

use diesel::sql_types::{BigInt, Bool, Integer, Jsonb, Text};
use diesel::{QueryableByName, sql_query};
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use hubuum_storage_core::{TaskPurgeScope, TaskPurgeTicket};
use hubuum_storage_postgres::test_support::{
    database_role_tests_enabled, integration_test_database_roles, integration_test_migration_pool,
    integration_test_pool,
};
use hubuum_storage_postgres::{PostgresConnection, PostgresStorageError, with_transaction};
use rstest::rstest;
use serde_json::{Value, json};
use uuid::Uuid;

static CONTROL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(QueryableByName)]
struct Material {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}
#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    count: i64,
}
#[derive(QueryableByName)]
struct Flag {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

async fn assume_owner(connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
    if database_role_tests_enabled() {
        sql_query("SELECT set_config('role',$1,true)")
            .bind::<Text, _>(integration_test_database_roles().owner().as_str())
            .execute(connection)
            .await?;
    }
    Ok(())
}

async fn setup(connection: &mut PostgresConnection) -> Result<(), PostgresStorageError> {
    assume_owner(connection).await?;
    connection
        .batch_execute("SAVEPOINT retention_fixture")
        .await?;
    sql_query("SELECT public.hubuum_task_retention_lock()")
        .execute(connection)
        .await?;
    connection.batch_execute("UPDATE hubuum_control.task_retention_control SET activation=jsonb_build_object('state','protected','key_fingerprint',repeat('a',64)); UPDATE hubuum_control.task_retention_policies SET material=jsonb_set(material,'{details_after_terminal_secs}','0') WHERE revision=0;").await?;
    Ok(())
}

async fn task(
    connection: &mut PostgresConnection,
    kind: &str,
) -> Result<i32, PostgresStorageError> {
    #[derive(QueryableByName)]
    struct Id {
        #[diesel(sql_type=Integer)]
        id: i32,
    }
    Ok(sql_query("INSERT INTO tasks(kind,status,finished_at,created_at) VALUES($1,'succeeded',now()-interval '1 hour',now()-interval '2 hours') RETURNING id")
        .bind::<Text,_>(kind)
        .get_result::<Id>(connection).await?.id)
}

async fn prepare(
    connection: &mut PostgresConnection,
    scope: TaskPurgeScope,
) -> Result<(TaskPurgeTicket, Value), PostgresStorageError> {
    let scope = match scope {
        TaskPurgeScope::TaskDetails => "task_details",
        TaskPurgeScope::ExpiredOutputs => "expired_outputs",
    };
    let value = sql_query("SELECT public.hubuum_task_purge_prepare($1,$2,64) AS material")
        .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
        .bind::<Text, _>(scope)
        .get_result::<Material>(connection)
        .await?
        .material;
    let ticket: TaskPurgeTicket =
        serde_json::from_value(value).expect("SQL produces a validated immutable ticket");
    let authority = sql_query("SELECT material FROM hubuum_control.deletion_authority WHERE id=1")
        .get_result::<Material>(connection)
        .await?
        .material;
    let proof = json!({"lineage":authority["lineage"],"epoch":authority["epoch"],"task_revision":ticket.prepared_revision().value(),"checkpoint":"b".repeat(64)});
    Ok((ticket, proof))
}

async fn execute(
    connection: &mut PostgresConnection,
    ticket: &TaskPurgeTicket,
    proof: &Value,
    rows: i32,
    bytes: i64,
) -> Result<Value, PostgresStorageError> {
    Ok(
        sql_query("SELECT public.hubuum_task_purge_execute($1,$2,$3,$4) AS material")
            .bind::<Jsonb, _>(serde_json::to_value(ticket).unwrap())
            .bind::<Jsonb, _>(proof)
            .bind::<Integer, _>(rows)
            .bind::<BigInt, _>(bytes)
            .get_result::<Material>(connection)
            .await?
            .material,
    )
}

#[tokio::test]
async fn import_receipt_allows_cancellation_but_excludes_purge_lock() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(2);
    let id = with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        task(connection, "import").await
    })
    .await
    .unwrap();
    let result = with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("INSERT INTO import_task_results(task_id,entity_kind,action,outcome) VALUES($1,'object','create','success')")
            .bind::<Integer, _>(id)
            .execute(connection)
            .await?;
        // The child remains uncommitted while a separate transaction takes the
        // same parent lock used by cancellation and ordinary lease updates.
        with_transaction(&pool, async |other| {
            assume_owner(other).await?;
            sql_query("SELECT id FROM tasks WHERE id=$1 FOR NO KEY UPDATE NOWAIT")
                .bind::<Integer, _>(id)
                .execute(other)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await?;
        with_transaction(&pool, async |other| {
            assume_owner(other).await?;
            sql_query("SELECT set_config('hubuum.test_task_id',$1,true)")
                .bind::<Text, _>(id.to_string())
                .execute(other)
                .await?;
            other
                .batch_execute(
                    "DO $$ BEGIN
                        PERFORM 1 FROM public.tasks
                            WHERE id=current_setting('hubuum.test_task_id')::integer
                            FOR UPDATE NOWAIT;
                        RAISE EXCEPTION 'Purge parent lock admitted an uncommitted attachment';
                    EXCEPTION WHEN lock_not_available THEN NULL; END $$",
                )
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await;
    with_transaction(&pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("DELETE FROM tasks WHERE id=$1")
            .bind::<Integer, _>(id)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    result.unwrap();
}

#[tokio::test]
async fn parent_removal_never_cascades_past_the_child_row_budget() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool,async |connection| {
        setup(connection).await?;
        let id=task(connection,"import").await?;
        sql_query("INSERT INTO import_task_results(task_id,entity_kind,action,outcome) SELECT $1,'object','create','success' FROM generate_series(1,3)").bind::<Integer,_>(id).execute(connection).await?;
        let (ticket,proof)=prepare(connection,TaskPurgeScope::TaskDetails).await?;
        let progress=execute(connection,&ticket,&proof,2,16_777_216).await?;
        assert_eq!(progress["rows"],2); assert_eq!(progress["state"],"more");
        let remaining=sql_query("SELECT count(*) FROM import_task_results WHERE task_id=$1").bind::<Integer,_>(id).get_result::<Count>(connection).await?.count;
        assert_eq!(remaining,1);
        execute(connection,&ticket,&proof,2,16_777_216).await?;
        let complete=execute(connection,&ticket,&proof,2,16_777_216).await?;
        assert_eq!(complete["state"],"removed");
        let exists=sql_query("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1) AS allowed").bind::<Integer,_>(id).get_result::<Flag>(connection).await?.allowed;
        assert!(!exists);
        connection.batch_execute("ROLLBACK TO SAVEPOINT retention_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

#[rstest]
#[case::equivalent_rfc3339_instant(false)]
#[case::changed_eligibility_instant(true)]
#[tokio::test]
async fn purge_ticket_timestamp_roundtrip_preserves_exact_instant(#[case] changed: bool) {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool, async move |connection| {
        setup(connection).await?;
        task(connection, "import").await?;
        let (ticket, proof) = prepare(connection, TaskPurgeScope::TaskDetails).await?;
        let mut request = serde_json::to_value(&ticket).unwrap();
        let at =
            ticket.manifest().eligible_at() + chrono::Duration::microseconds(i64::from(changed));
        request["manifest"]["eligible_at"] = json!(
            at.with_timezone(&chrono::FixedOffset::east_opt(3600).unwrap())
                .to_rfc3339()
        );
        let result =
            sql_query("SELECT public.hubuum_task_purge_execute($1,$2,100,16777216) AS material")
                .bind::<Jsonb, _>(request)
                .bind::<Jsonb, _>(proof)
                .get_result::<Material>(connection)
                .await;
        if changed {
            assert!(matches!(
                result,
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::SerializationFailure,
                    _
                ))
            ));
        } else {
            assert_eq!(result?.material["state"], "removed");
        }
        connection
            .batch_execute("ROLLBACK TO SAVEPOINT retention_fixture")
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn output_only_purge_preserves_task_and_owned_results_without_a_details_deadline() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool,async |connection| {
        setup(connection).await?;
        let id=task(connection,"schema_profiling").await?;
        sql_query("UPDATE tasks SET detail_expires_at=NULL WHERE id=$1").bind::<Integer,_>(id).execute(connection).await?;
        sql_query("INSERT INTO import_task_results(task_id,entity_kind,action,outcome) VALUES($1,'object','create','success')").bind::<Integer,_>(id).execute(connection).await?;
        sql_query("INSERT INTO schema_profile_outputs(task_id,report,output_expires_at) VALUES($1,'{}',now()-interval '1 minute')").bind::<Integer,_>(id).execute(connection).await?;
        let (ticket,proof)=prepare(connection,TaskPurgeScope::ExpiredOutputs).await?;
        let result=execute(connection,&ticket,&proof,100,16_777_216).await?;
        assert_eq!(result["state"],"removed");
        let visible=sql_query("SELECT public.hubuum_task_details_visible($1) AND NOT public.hubuum_task_outputs_visible($1) AS allowed").bind::<Integer,_>(id).get_result::<Flag>(connection).await?.allowed;
        assert!(visible);
        assert_eq!(sql_query("SELECT count(*) FROM import_task_results WHERE task_id=$1").bind::<Integer,_>(id).get_result::<Count>(connection).await?.count,1);
        connection.batch_execute("ROLLBACK TO SAVEPOINT retention_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

#[rstest]
#[case::details(TaskPurgeScope::TaskDetails)]
#[case::outputs(TaskPurgeScope::ExpiredOutputs)]
#[tokio::test]
async fn late_output_attachment_is_rejected_after_the_scoped_intent(#[case] scope: TaskPurgeScope) {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool,async move |connection| {
        setup(connection).await?;
        let id=task(connection,"schema_profiling").await?;
        sql_query("INSERT INTO schema_profile_outputs(task_id,report,output_expires_at) VALUES($1,'{}',now()-interval '1 minute')").bind::<Integer,_>(id).execute(connection).await?;
        prepare(connection,scope).await?;
        connection.batch_execute("SAVEPOINT attachment").await?;
        let result=sql_query("UPDATE schema_profile_outputs SET report='{\"late\":true}' WHERE task_id=$1").bind::<Integer,_>(id).execute(connection).await;
        assert!(matches!(result,Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::CheckViolation,_))));
        connection.batch_execute("ROLLBACK TO SAVEPOINT retention_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn an_oversized_target_does_not_starve_another_target_in_the_manifest() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool,async |connection| {
        setup(connection).await?;
        let large=task(connection,"import").await?; let small=task(connection,"import").await?;
        sql_query("INSERT INTO import_task_results(task_id,entity_kind,action,outcome,details) VALUES($1,'object','create','success',to_jsonb(repeat('x',50000)))").bind::<Integer,_>(large).execute(connection).await?;
        let (ticket,proof)=prepare(connection,TaskPurgeScope::TaskDetails).await?;
        let result=execute(connection,&ticket,&proof,100,16000).await?;
        assert_eq!(result["state"],"more");
        let removed=sql_query("SELECT NOT EXISTS(SELECT 1 FROM tasks WHERE id=$1) AND EXISTS(SELECT 1 FROM tasks WHERE id=$2) AS allowed").bind::<Integer,_>(small).bind::<Integer,_>(large).get_result::<Flag>(connection).await?.allowed;
        assert!(removed);
        connection.batch_execute("ROLLBACK TO SAVEPOINT retention_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn purge_rejects_an_inherited_repeatable_read_snapshot() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    let mut connection = pool.get().await.unwrap();
    let result = connection
        .transaction::<(), diesel::result::Error, _>(async |connection| {
            connection
                .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await?;
            diesel::select(diesel::dsl::sql::<Integer>("1"))
                .get_result::<i32>(connection)
                .await?;
            sql_query("SELECT public.hubuum_task_purge_prepare($1,'task_details',1)")
                .bind::<diesel::sql_types::Uuid, _>(Uuid::new_v4())
                .execute(connection)
                .await?;
            Ok(())
        })
        .await;
    let error = result.unwrap_err();
    assert!(
        matches!(error,diesel::result::Error::DatabaseError(_,ref info) if info.message()=="Task purge requires READ COMMITTED"),
        "{error:?}"
    );
}

#[tokio::test]
async fn output_only_intent_rejects_a_writer_using_an_older_repeatable_read_snapshot() {
    use diesel::result::{DatabaseErrorKind, Error as DieselError};
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let id = with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        let id = task(connection, "export").await?;
        sql_query("INSERT INTO export_task_outputs(task_id,content_type,json_output,meta_json,warnings_json,output_expires_at) VALUES($1,'application/json','{}','{}','[]',now()-interval '1 minute')")
            .bind::<Integer,_>(id).execute(connection).await?;
        Ok::<_, PostgresStorageError>(id)
    })
    .await
    .unwrap();
    let mut connection = fixture.pool.get().await.unwrap();
    let result = connection
        .transaction::<_, PostgresStorageError, _>(async |connection| {
            connection
                .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await?;
            assume_owner(connection).await?;
            sql_query("SELECT count(*) FROM tasks WHERE id=$1")
                .bind::<Integer, _>(id)
                .get_result::<Count>(connection)
                .await?;
            with_transaction(&fixture.pool, async |other| {
                assume_owner(other).await?;
                prepare(other, TaskPurgeScope::ExpiredOutputs).await?;
                Ok::<_, PostgresStorageError>(())
            })
            .await?;
            connection
                .batch_execute("SAVEPOINT stale_attachment")
                .await?;
            let write =
                sql_query("UPDATE export_task_outputs SET json_output='{}' WHERE task_id=$1")
                    .bind::<Integer, _>(id)
                    .execute(connection)
                    .await;
            connection
                .batch_execute("ROLLBACK TO SAVEPOINT stale_attachment")
                .await?;
            Ok(write)
        })
        .await;
    drop(connection);
    with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("DELETE FROM tasks WHERE id=$1")
            .bind::<Integer, _>(id)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    fixture.finish().await;
    let write = result.unwrap();
    assert!(
        matches!(
            write,
            Err(DieselError::DatabaseError(
                DatabaseErrorKind::SerializationFailure,
                _
            ))
        ),
        "old snapshots must not miss the output-only tombstone: {write:?}"
    );
}

#[tokio::test]
async fn runtime_has_only_operation_shaped_access_to_protected_receipts() {
    if !database_role_tests_enabled() {
        return;
    }
    let pool = integration_test_pool(1);
    let allowed=with_transaction(&pool,async |connection| {
        Ok::<_,PostgresStorageError>(sql_query("SELECT NOT has_table_privilege(current_user,(SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='hubuum_control' AND c.relname='task_receipts'),'SELECT,INSERT,UPDATE,DELETE') AND has_function_privilege(current_user,'public.hubuum_task_receipt_read(uuid,text)','EXECUTE') AND NOT has_function_privilege(current_user,'public.hubuum_task_purge_block(uuid,text,timestamptz)','EXECUTE') AS allowed").get_result::<Flag>(connection).await?.allowed)
    }).await.unwrap();
    assert!(allowed);
}

#[tokio::test]
async fn task_child_graph_is_an_explicit_bounded_deletion_allowlist() {
    let pool = integration_test_migration_pool(1);
    let edges=with_transaction(&pool,async |connection| {
        let rows=sql_query("WITH RECURSIVE relations(oid) AS (SELECT 'public.tasks'::regclass::oid UNION SELECT c.conrelid FROM pg_constraint c JOIN relations p ON p.oid=c.confrelid WHERE c.contype='f') SELECT jsonb_build_object('child',c.conrelid::regclass::text,'parent',c.confrelid::regclass::text,'delete',c.confdeltype::text,'columns',(SELECT jsonb_agg(a.attname ORDER BY k.ordinality) FROM unnest(c.conkey) WITH ORDINALITY k(attnum,ordinality) JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=k.attnum)) AS material FROM pg_constraint c WHERE c.contype='f' AND c.confrelid IN (SELECT oid FROM relations) ORDER BY c.conrelid::regclass::text").load::<Material>(connection).await?;
        Ok::<_,PostgresStorageError>(rows.into_iter().map(|r|r.material).collect::<Vec<_>>())
    }).await.unwrap();
    assert_eq!(
        edges,
        vec![
            json!({"child":"backup_task_outputs","parent":"tasks","delete":"c","columns":["task_id"]}),
            json!({"child":"class_computation_state","parent":"tasks","delete":"n","columns":["active_task_id"]}),
            json!({"child":"export_task_outputs","parent":"tasks","delete":"c","columns":["task_id"]}),
            json!({"child":"import_task_results","parent":"tasks","delete":"c","columns":["task_id"]}),
            json!({"child":"remote_call_results","parent":"tasks","delete":"c","columns":["task_id"]}),
            json!({"child":"schema_impact_findings","parent":"schema_validation_work","delete":"c","columns":["task_id"]}),
            json!({"child":"schema_profile_outputs","parent":"tasks","delete":"c","columns":["task_id"]}),
            json!({"child":"schema_repair_reports","parent":"schema_validation_work","delete":"c","columns":["task_id"]}),
            json!({"child":"schema_validation_work","parent":"tasks","delete":"c","columns":["task_id"]}),
        ]
    );
}

struct AdmissionFixture {
    pool: hubuum_storage_postgres::PostgresPool,
    control: Value,
    application: Value,
    principal: i32,
    owner: hubuum_domain::PrincipalIdentity,
}
impl AdmissionFixture {
    async fn new() -> Self {
        let pool = integration_test_migration_pool(3);
        let (control,application,principal,owner)=with_transaction(&pool,async |connection| {
            assume_owner(connection).await?;
            let control=sql_query("SELECT to_jsonb(c) AS material FROM hubuum_control.task_retention_control c WHERE id=1").get_result::<Material>(connection).await?.material;
            assert_eq!(control["activation"]["state"],"disabled");
            let application=sql_query("SELECT to_jsonb(c) AS material FROM hubuum_control.application_control c WHERE id=1").get_result::<Material>(connection).await?.material;
            let value=sql_query("INSERT INTO principals(kind,name) VALUES('human',$1) RETURNING jsonb_build_object('id',id,'identity',logical_identity #>> '{subject,identity,identity}') AS material")
                .bind::<Text,_>(format!("receipt_fixture_{}",Uuid::new_v4().simple())).get_result::<Material>(connection).await?.material;
            Ok::<_,PostgresStorageError>((control,application,value["id"].as_i64().unwrap() as i32,serde_json::from_value(value["identity"].clone()).unwrap()))
        }).await.unwrap();
        let fixture = Self {
            pool,
            control,
            application,
            principal,
            owner,
        };
        use hubuum_storage_core::TaskRetentionStorage;
        let admin = fixture.admin();
        for activation in [
            hubuum_storage_core::TaskReceiptActivation::Backfilling {
                key_fingerprint: fixture.key(),
            },
            hubuum_storage_core::TaskReceiptActivation::Protected {
                key_fingerprint: fixture.key(),
            },
        ] {
            let current = admin.read_task_control().await.unwrap();
            let next = current.with_activation(activation).unwrap();
            admin
                .commit_task_control(
                    hubuum_storage_core::TaskControlChange::new(current, next).unwrap(),
                )
                .await
                .unwrap();
        }
        fixture
    }
    fn admin(&self) -> hubuum_storage_postgres::PostgresStorage {
        hubuum_storage_postgres::PostgresStorage::unobserved(self.pool.clone())
    }
    fn runtime(&self) -> hubuum_storage_postgres::PostgresStorage {
        hubuum_storage_postgres::PostgresStorage::unobserved(integration_test_pool(3))
    }
    fn key(&self) -> hubuum_storage_core::ControlKeyFingerprint {
        hubuum_storage_core::ControlKeyFingerprint::new("a".repeat(64)).unwrap()
    }
    fn request(&self) -> hubuum_storage_core::StorageTaskCreateRequest {
        use hubuum_storage_core::{
            ProtectedTaskKey, StorageTaskCreateRequest, StorageTaskKind, TaskReceiptBinding,
            TaskRequestFingerprint, TaskSubmissionIdentity, TaskSubmissionLookup,
        };
        let lookup = TaskSubmissionLookup::new(
            TaskSubmissionIdentity::new(self.owner, ProtectedTaskKey::new("c".repeat(64)).unwrap()),
            StorageTaskKind::Import,
            TaskRequestFingerprint::new("d".repeat(64)).unwrap(),
        );
        StorageTaskCreateRequest::builder(
            StorageTaskKind::Import,
            hubuum_domain::PrincipalId::new(self.principal).unwrap(),
            json!({"changes":[]}),
            1,
        )
        .idempotency_key(Some(
            hubuum_task_core::IdempotencyKey::new("native_fixture").unwrap(),
        ))
        .receipt_binding(Some(TaskReceiptBinding::new(lookup, self.key())))
        .try_build()
        .unwrap()
    }
    async fn finish(self) {
        with_transaction(&self.pool,async |connection| {
            assume_owner(connection).await?;
            sql_query("DELETE FROM tasks WHERE submitted_by=$1").bind::<Integer,_>(self.principal).execute(connection).await?;
            sql_query("DELETE FROM principals WHERE id=$1").bind::<Integer,_>(self.principal).execute(connection).await?;
            connection.batch_execute("DELETE FROM hubuum_control.task_purge_targets; DELETE FROM hubuum_control.task_purge_batches; DELETE FROM hubuum_control.task_receipts; DELETE FROM hubuum_control.task_retention_holds;").await?;
            sql_query("UPDATE hubuum_control.task_retention_control SET (revision,hold_release_revision,activation,receipt_expiry_floor,current_policy_revision,receipt_count,receipt_bytes,retention_bytes,reapplied_incarnation,reapplied_revision)=(SELECT r.revision,r.hold_release_revision,r.activation,r.receipt_expiry_floor,r.current_policy_revision,r.receipt_count,r.receipt_bytes,r.retention_bytes,r.reapplied_incarnation,r.reapplied_revision FROM jsonb_populate_record(NULL::hubuum_control.task_retention_control,$1) r) WHERE id=1").bind::<Jsonb,_>(self.control).execute(connection).await?;
            sql_query("UPDATE hubuum_control.application_control SET (incarnation,reapplication_revision,released_revision,checkpoint,pending_restore)=(SELECT r.incarnation,r.reapplication_revision,r.released_revision,r.checkpoint,r.pending_restore FROM jsonb_populate_record(NULL::hubuum_control.application_control,$1) r) WHERE id=1").bind::<Jsonb,_>(self.application).execute(connection).await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }
}

#[tokio::test]
async fn native_adapter_satisfies_shared_protected_admission_contract() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let result = hubuum_storage_conformance::verify_protected_task_admission(
        &fixture.runtime(),
        fixture.request(),
        hubuum_storage_core::DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
    )
    .await;
    fixture.finish().await;
    result.unwrap();
}

#[rstest]
#[case::queued("queued")]
#[case::omitted_from_restore("missing")]
#[case::unproven_terminal("terminal")]
#[tokio::test]
async fn recovered_nonterminal_admission_is_settled_without_replay_before_new_proof(
    #[case] restored: &str,
) {
    use hubuum_storage_core::{
        DeletionCheckpointIdentity, DeletionControlStorage, ProtectedTaskReapplication,
        StorageTaskSubmissionOutcome, TaskControlCheckpoint, TaskPurgeBudget, TaskQueueStorage,
        TaskReapplicationProgress, TaskReceiptOutcome, TaskRetentionStorage,
    };
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let admin = fixture.admin();
    let StorageTaskSubmissionOutcome::PendingProtection(ticket) = fixture
        .runtime()
        .create_task(fixture.request())
        .await
        .unwrap()
    else {
        panic!("pending")
    };
    with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        if restored=="missing" {
            sql_query("DELETE FROM tasks WHERE submitted_by=$1").bind::<Integer,_>(fixture.principal).execute(connection).await?;
        } else if restored=="terminal" {
            sql_query("UPDATE tasks SET status='succeeded',finished_at=timezone('UTC',clock_timestamp()) WHERE submitted_by=$1").bind::<Integer,_>(fixture.principal).execute(connection).await?;
        }
        sql_query("SELECT public.hubuum_rotate_application_for_restore()")
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let state = admin.read_task_control().await.unwrap();
    let before = state.revision();
    let fence = admin.read_application_control().await.unwrap().fence();
    let checkpoint = TaskControlCheckpoint::new(
        state.lineage(),
        state.epoch(),
        state.revision(),
        DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
    )
    .unwrap();
    let result = admin
        .reapply_task_retention(
            ProtectedTaskReapplication::new(state, fence, checkpoint).unwrap(),
            TaskPurgeBudget::default(),
        )
        .await;
    let after = admin.read_task_control().await.unwrap();
    let saved = after
        .receipt(ticket.receipt().submission())
        .unwrap()
        .clone();
    let gate = admin.capture_application_execution(None).await;
    fixture.finish().await;
    let TaskReapplicationProgress::Complete(proof) = result.unwrap() else {
        panic!("complete")
    };
    assert!(matches!(
        saved.outcome(),
        TaskReceiptOutcome::RecoveryUnknown { .. }
    ));
    assert_eq!(
        after.revision().value(),
        before.value() + 2,
        "unknown settlement and completion proof each advance signed authority"
    );
    assert_eq!(proof.task_revision(), after.revision());
    assert!(
        gate.is_err(),
        "completion requires independent protection before release"
    );
}

fn legacy_replay_identity(
    row: Value,
    principal: bool,
    rebackup: bool,
    output: Option<Value>,
) -> (hubuum_storage_core::StorageIdentityEvidence, Option<Value>) {
    use hubuum_storage_core::{
        StorageBackupBudget, StorageBackupHistorySection as History, StorageBackupReplay,
        StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection as State,
        StorageIdentityUpgrade,
    };
    let mut state = State::ALL
        .iter()
        .map(|section| (*section, Vec::new()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut history = History::ALL
        .iter()
        .map(|section| (*section, Vec::new()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let row = StorageBackupRow::try_from_value(row).unwrap();
    if principal {
        state.get_mut(&State::Principals).unwrap().push(row);
    } else {
        history.get_mut(&History::TerminalTasks).unwrap().push(row);
    }
    if let Some(output) = output {
        history
            .get_mut(&History::ExportOutputs)
            .unwrap()
            .push(StorageBackupRow::try_from_value(output).unwrap());
    }
    let budget = StorageBackupBudget::new(1_048_576, 100).unwrap();
    let replay = StorageBackupReplay::from_snapshot(
        StorageBackupSnapshot::try_new(state, Some(history)).unwrap(),
        budget,
    )
    .unwrap()
    .upgrade_logical_identities(StorageIdentityUpgrade::Legacy(
        hubuum_domain::LegacyIdentityCohort::generate(),
    ))
    .unwrap();
    // Re-encoding the upgraded data as a native backup must not turn a derived
    // identity into evidence that it is unrelated to retained decisions.
    let replay = if rebackup {
        StorageBackupReplay::from_snapshot(replay.replay().materialize().unwrap(), budget)
            .unwrap()
            .upgrade_logical_identities(StorageIdentityUpgrade::Native)
            .unwrap()
    } else {
        replay
    };
    let row = if principal {
        replay.replay().state_rows(State::Principals).next()
    } else {
        replay.replay().history_rows(History::TerminalTasks).next()
    }
    .unwrap()
    .unwrap();
    let identity = serde_json::from_value(row.get("logical_identity").unwrap().clone()).unwrap();
    let output = replay
        .replay()
        .history_rows(History::ExportOutputs)
        .next()
        .map(|row| row.unwrap().into_value());
    (identity, output)
}

async fn reapply_fixture(
    fixture: &AdmissionFixture,
) -> hubuum_storage_core::TaskReapplicationProgress {
    use hubuum_storage_core::{
        DeletionCheckpointIdentity, DeletionControlStorage, ProtectedTaskReapplication,
        TaskControlCheckpoint, TaskPurgeBudget, TaskRetentionStorage,
    };
    let admin = fixture.admin();
    let state = admin.read_task_control().await.unwrap();
    let fence = admin.read_application_control().await.unwrap().fence();
    let checkpoint = TaskControlCheckpoint::new(
        state.lineage(),
        state.epoch(),
        state.revision(),
        DeletionCheckpointIdentity::new("b".repeat(64)).unwrap(),
    )
    .unwrap();
    admin
        .reapply_task_retention(
            ProtectedTaskReapplication::new(state, fence, checkpoint).unwrap(),
            TaskPurgeBudget::default(),
        )
        .await
        .unwrap()
}

#[rstest]
#[case::details_legacy(TaskPurgeScope::TaskDetails, false)]
#[case::details_rebacked_up(TaskPurgeScope::TaskDetails, true)]
#[case::outputs_legacy(TaskPurgeScope::ExpiredOutputs, false)]
#[case::outputs_rebacked_up(TaskPurgeScope::ExpiredOutputs, true)]
#[tokio::test]
async fn legacy_backup_replay_cannot_remint_a_task_around_retained_purges(
    #[case] scope: TaskPurgeScope,
    #[case] rebackup: bool,
) {
    use hubuum_storage_core::{
        DeletionControlStorage, TaskPurgeBlock, TaskReapplicationProgress, TaskRetentionStorage,
    };
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let (mut original, output) = with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        let id = task(connection,"export").await?;
        sql_query("UPDATE tasks SET detail_expires_at=timezone('UTC',now()-interval '1 minute') WHERE id=$1")
            .bind::<Integer,_>(id).execute(connection).await?;
        sql_query("INSERT INTO export_task_outputs(task_id,content_type,json_output,meta_json,warnings_json,output_expires_at) VALUES($1,'application/json','{}','{}','[]',now()-interval '1 minute')")
            .bind::<Integer,_>(id).execute(connection).await?;
        let original = sql_query("SELECT to_jsonb(t) AS material FROM tasks t WHERE id=$1")
            .bind::<Integer,_>(id).get_result::<Material>(connection).await?.material;
        let output = sql_query("SELECT to_jsonb(o) AS material FROM export_task_outputs o WHERE task_id=$1")
            .bind::<Integer,_>(id).get_result::<Material>(connection).await?.material;
        let (ticket, proof) = prepare(connection, scope).await?;
        assert!(ticket.manifest().tasks().iter().any(|task| task.to_string()==original["logical_identity"]["subject"]["identity"]["identity"].as_str().unwrap()));
        assert_eq!(execute(connection,&ticket,&proof,100,16_777_216).await?["state"],"removed");
        Ok::<_,PostgresStorageError>((original, output))
    }).await.unwrap();
    let (identity, output) =
        legacy_replay_identity(original.clone(), false, rebackup, Some(output));
    let output = output.expect("upgraded export output");
    assert!(identity.has_unknown_task_restore_source());
    original["logical_identity"] = serde_json::to_value(identity).unwrap();
    let id = original["id"].as_i64().unwrap() as i32;
    let replay_result = with_transaction(&fixture.pool,async |connection| {
        assume_owner(connection).await?;
        connection.batch_execute("SET LOCAL hubuum.restore_identities='on'").await?;
        sql_query("DELETE FROM tasks WHERE id=$1").bind::<Integer,_>(id).execute(connection).await?;
        sql_query("INSERT INTO tasks SELECT (jsonb_populate_record(NULL::tasks,$1)).*")
            .bind::<Jsonb,_>(&original).execute(connection).await?;
        sql_query("INSERT INTO export_task_outputs SELECT (jsonb_populate_record(NULL::export_task_outputs,$1)).*")
            .bind::<Jsonb,_>(&output).execute(connection).await?;
        sql_query("SELECT public.hubuum_rotate_application_for_restore()").execute(connection).await?;
        Ok::<_,PostgresStorageError>(())
    }).await;
    if let Err(error) = replay_result {
        with_transaction(&fixture.pool, async |connection| {
            assume_owner(connection).await?;
            sql_query("DELETE FROM tasks WHERE id=$1")
                .bind::<Integer, _>(id)
                .execute(connection)
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
        fixture.finish().await;
        panic!("Legacy task and output replay failed: {error:?}");
    }
    let before = fixture.admin().read_task_control().await.unwrap();
    let result = reapply_fixture(&fixture).await;
    let after = fixture.admin().read_task_control().await.unwrap();
    let exposure = fixture.admin().capture_application_execution(None).await;
    with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("DELETE FROM tasks WHERE id=$1")
            .bind::<Integer, _>(id)
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    fixture.finish().await;
    assert_eq!(
        result,
        TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)
    );
    assert_eq!(
        after, before,
        "uncertain correspondence must not create a release proof"
    );
    assert!(exposure.is_err());
}

#[rstest]
#[case::legacy(false)]
#[case::rebacked_up(true)]
#[tokio::test]
async fn legacy_owner_replay_cannot_create_a_new_receipt_namespace(#[case] rebackup: bool) {
    use hubuum_storage_core::{
        DeletionControlStorage, ExecutionStorage, ProtectedTaskKey, StorageExecutionScope,
        StorageTaskCreateRequest, StorageTaskKind, StorageTaskSubmissionOutcome, TaskPurgeBlock,
        TaskQueueStorage, TaskReapplicationProgress, TaskReceiptBinding, TaskRequestFingerprint,
        TaskRetentionStorage, TaskSubmissionIdentity, TaskSubmissionLookup,
    };
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let runtime = fixture.runtime();
    let permit = runtime.capture_application_execution(None).await.unwrap();
    assert!(matches!(
        runtime.create_task(fixture.request()).await.unwrap(),
        StorageTaskSubmissionOutcome::PendingProtection(_)
    ));
    let source = with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        Ok::<_, PostgresStorageError>(
            sql_query("SELECT to_jsonb(p) AS material FROM principals p WHERE id=$1")
                .bind::<Integer, _>(fixture.principal)
                .get_result::<Material>(connection)
                .await?
                .material,
        )
    })
    .await
    .unwrap();
    let (identity, _) = legacy_replay_identity(source, true, rebackup, None);
    assert!(identity.has_unknown_principal_restore_source());
    let hubuum_storage_core::StorageIdentitySubject::Principal { identity: owner } =
        identity.subject().unwrap()
    else {
        panic!("principal subject")
    };
    let owner = owner.known_identity().unwrap();
    with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        connection
            .batch_execute("SET LOCAL hubuum.restore_identities='on'")
            .await?;
        sql_query("UPDATE principals SET logical_identity=$2 WHERE id=$1")
            .bind::<Integer, _>(fixture.principal)
            .bind::<Jsonb, _>(serde_json::to_value(&identity).unwrap())
            .execute(connection)
            .await?;
        sql_query("SELECT public.hubuum_rotate_application_for_restore()")
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    let before = fixture.admin().read_task_control().await.unwrap();
    let result = reapply_fixture(&fixture).await;
    let lookup = TaskSubmissionLookup::new(
        TaskSubmissionIdentity::new(owner, ProtectedTaskKey::new("c".repeat(64)).unwrap()),
        StorageTaskKind::Import,
        TaskRequestFingerprint::new("d".repeat(64)).unwrap(),
    );
    let retry = StorageTaskCreateRequest::builder(
        StorageTaskKind::Import,
        hubuum_domain::PrincipalId::new(fixture.principal).unwrap(),
        json!({"changes":[]}),
        1,
    )
    .idempotency_key(Some(
        hubuum_task_core::IdempotencyKey::new("native_fixture").unwrap(),
    ))
    .receipt_binding(Some(TaskReceiptBinding::new(lookup, fixture.key())))
    .try_build()
    .unwrap();
    let retry = runtime
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(permit),
            runtime.create_task(retry),
        )
        .await;
    let after = fixture.admin().read_task_control().await.unwrap();
    let exposure = fixture.admin().capture_application_execution(None).await;
    fixture.finish().await;
    assert_eq!(
        result,
        TaskReapplicationProgress::Blocked(TaskPurgeBlock::UnknownIdentity)
    );
    assert_eq!(
        after, before,
        "reapplication and rejected retry preserve the original receipt"
    );
    assert!(
        retry.is_err(),
        "captured application work cannot admit the reminted owner"
    );
    assert!(exposure.is_err());
}

#[tokio::test]
async fn released_hold_requires_a_checkpoint_covering_its_release_revision() {
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let pool = integration_test_migration_pool(1);
    with_transaction(&pool,async |connection| {
        setup(connection).await?;
        task(connection,"import").await?;
        let (ticket,proof)=prepare(connection,TaskPurgeScope::TaskDetails).await?;
        connection.batch_execute("UPDATE hubuum_control.task_retention_control SET revision=revision+1,hold_release_revision=revision+1 WHERE id=1; SAVEPOINT old_protection").await?;
        let result=sql_query("SELECT public.hubuum_task_purge_execute($1,$2,100,16777216)")
            .bind::<Jsonb,_>(serde_json::to_value(&ticket).unwrap()).bind::<Jsonb,_>(&proof).execute(connection).await;
        assert!(matches!(result,Err(diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::SerializationFailure,_))));
        connection.batch_execute("ROLLBACK TO SAVEPOINT old_protection").await?;
        let mut current=proof;
        current["task_revision"]=json!(ticket.prepared_revision().value()+1);
        assert_eq!(execute(connection,&ticket,&current,100,16_777_216).await?["state"],"removed");
        connection.batch_execute("ROLLBACK TO SAVEPOINT retention_fixture").await?;
        Ok::<_,PostgresStorageError>(())
    }).await.unwrap();
}

#[tokio::test]
async fn task_delete_triggers_and_many_child_indexes_are_reviewed_explicitly() {
    let pool = integration_test_migration_pool(1);
    let (triggers,indexed)=with_transaction(&pool,async |connection| {
        let triggers=sql_query("WITH RECURSIVE relations(oid) AS (SELECT 'public.tasks'::regclass::oid UNION SELECT c.conrelid FROM pg_constraint c JOIN relations p ON p.oid=c.confrelid WHERE c.contype='f') SELECT jsonb_build_object('table',t.tgrelid::regclass::text,'trigger',t.tgname) AS material FROM pg_trigger t WHERE NOT t.tgisinternal AND (t.tgtype&8)=8 AND t.tgrelid IN (SELECT oid FROM relations) ORDER BY t.tgrelid::regclass::text,t.tgname").load::<Material>(connection).await?;
        let indexed=sql_query("SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='import_task_results' AND indexdef LIKE '%(task_id, id)%') AND EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname='public' AND tablename='schema_impact_findings' AND indexdef LIKE '%(task_id, object_id)%') AS allowed").get_result::<Flag>(connection).await?.allowed;
        Ok::<_,PostgresStorageError>((triggers.into_iter().map(|row|row.material).collect::<Vec<_>>(),indexed))
    }).await.unwrap();
    assert!(
        indexed,
        "Many-child purge needs task-leading bounded lookup indexes"
    );
    assert_eq!(
        triggers,
        vec![
            json!({"table":"backup_task_outputs","trigger":"task_output_capacity"}),
            json!({"table":"export_task_outputs","trigger":"task_output_capacity"}),
            json!({"table":"remote_call_results","trigger":"task_output_capacity"}),
            json!({"table":"schema_profile_outputs","trigger":"task_output_capacity"}),
            json!({"table":"schema_repair_reports","trigger":"task_output_capacity"}),
            json!({"table":"schema_validation_work","trigger":"schema_work_deleted"}),
            json!({"table":"tasks","trigger":"task_capacity_accounting"}),
            json!({"table":"tasks","trigger":"task_execution_claim_delete"}),
        ],
        "New DELETE-trigger side effects require an explicit boundedness review"
    );
}

#[tokio::test]
async fn internal_supersession_receipt_catches_up_to_its_original_terminal_clock() {
    use hubuum_domain::{ClassId, CollectionId, PrincipalId};
    use hubuum_storage_core::{
        ComputedFieldStorage, ProtectedTaskKey, StorageComputedFieldRebuildRequest,
        StorageTaskCreateRequest, StorageTaskKind, StorageTaskSubmissionOutcome,
        TaskExecutionStorage, TaskQueueStorage, TaskReceiptBinding, TaskReceiptOutcome,
        TaskRequestFingerprint, TaskRetentionStorage, TaskSubmissionIdentity, TaskSubmissionLookup,
        TaskSubmissionResolution,
    };
    let _guard = CONTROL_TEST_LOCK.lock().await;
    let fixture = AdmissionFixture::new().await;
    let class=with_transaction(&fixture.pool,async |connection| {
        assume_owner(connection).await?;
        let row=sql_query("INSERT INTO hubuumclass(name,description,collection_id) SELECT $1,'',id FROM collections WHERE parent_collection_id IS NULL RETURNING jsonb_build_object('id',id,'collection',collection_id) AS material")
            .bind::<Text,_>(format!("retention_supersession_{}",Uuid::new_v4().simple())).get_result::<Material>(connection).await?;
        Ok::<_,PostgresStorageError>(row.material)
    }).await.unwrap();
    let class_id = ClassId::new(class["id"].as_i64().unwrap() as i32).unwrap();
    let lookup = TaskSubmissionLookup::new(
        TaskSubmissionIdentity::new(
            fixture.owner,
            ProtectedTaskKey::new("c".repeat(64)).unwrap(),
        ),
        StorageTaskKind::Reindex,
        TaskRequestFingerprint::new("d".repeat(64)).unwrap(),
    );
    let create=StorageTaskCreateRequest::builder(StorageTaskKind::Reindex,PrincipalId::new(fixture.principal).unwrap(),json!({"type":"computed_fields","class_id":class_id.id(),"target_revision":0,"upper_bound":0}),0)
        .idempotency_key(Some(hubuum_task_core::IdempotencyKey::new("superseded_rebuild").unwrap()))
        .receipt_binding(Some(TaskReceiptBinding::new(lookup.clone(),fixture.key()))).try_build().unwrap();
    let runtime = fixture.runtime();
    let StorageTaskSubmissionOutcome::PendingProtection(ticket) =
        runtime.create_task(create).await.unwrap()
    else {
        panic!("pending")
    };
    runtime
        .request_computed_field_rebuild(StorageComputedFieldRebuildRequest::new(
            class_id,
            CollectionId::new(class["collection"].as_i64().unwrap() as i32).unwrap(),
            Some(PrincipalId::new(fixture.principal).unwrap()),
        ))
        .await
        .unwrap();
    let original=with_transaction(&fixture.pool,async |connection| {
        Ok::<_,PostgresStorageError>(sql_query("SELECT jsonb_build_object('finished_at',finished_at AT TIME ZONE 'UTC','body_redacted',request_payload IS NULL) AS material FROM tasks WHERE receipt_generation=$1")
            .bind::<diesel::sql_types::Uuid,_>(ticket.receipt().generation().value()).get_result::<Material>(connection).await?.material)
    }).await.unwrap();
    let before = runtime.read_task_control().await.unwrap();
    assert!(matches!(
        before
            .receipt(ticket.receipt().submission())
            .unwrap()
            .outcome(),
        TaskReceiptOutcome::MayExecute
    ));
    assert!(
        matches!(
            runtime.lookup_task_submission(lookup).await.unwrap(),
            TaskSubmissionResolution::DetailsExpired(_)
        ),
        "lag never treats the retained key as fresh"
    );
    runtime.recover_expired_task_leases(10).await.unwrap();
    let after = runtime.read_task_control().await.unwrap();
    let settled = after
        .receipt(ticket.receipt().submission())
        .unwrap()
        .clone();
    with_transaction(&fixture.pool, async |connection| {
        assume_owner(connection).await?;
        sql_query("DELETE FROM hubuumclass WHERE id=$1")
            .bind::<Integer, _>(class_id.id())
            .execute(connection)
            .await?;
        Ok::<_, PostgresStorageError>(())
    })
    .await
    .unwrap();
    fixture.finish().await;
    let TaskReceiptOutcome::Terminal {
        status,
        finished_at,
        ..
    } = settled.outcome()
    else {
        panic!("settled")
    };
    assert_eq!(
        *status,
        hubuum_storage_core::StorageTaskTerminalStatus::Cancelled
    );
    let original_clock: chrono::DateTime<chrono::Utc> =
        serde_json::from_value(original["finished_at"].clone()).unwrap();
    assert_eq!(*finished_at, original_clock);
    assert_eq!(original["body_redacted"], true);
}
