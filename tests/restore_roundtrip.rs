use diesel::ExpressionMethods;
use diesel_async::RunQueryDsl;
use hubuum::backups::create_backup_document;
use hubuum::config::DEFAULT_DB_STATEMENT_TIMEOUT_MS;
use hubuum::events::{
    Action, ActorKind, EntityType, EventEntityId, MutationProvenance, NewEvent, PrincipalId, TaskId,
};
use hubuum::models::{
    BackupRequest, NewHubuumClass, NewHubuumClassRelation, ObjectRelationLimit,
    RESTORE_CONFIRMATION_PHRASE, RestoreConfirmRequest, RestoreInitiator, RestoreJobID,
    RestoreJobStatus, RestoreStageRequest, TaskKind, TaskStatus,
};
use hubuum::restores::{
    RestoreSettings, confirm_restore, maintenance_state, reconcile_interrupted_restore,
    stage_restore,
};
use hubuum::schema::{
    collections, events, hubuumclass_history, hubuumclass_reachability, hubuumclass_relation,
    restore_jobs, system_maintenance, tasks,
};
use hubuum::storage::postgres::operations::task_rows::NewTaskRow as NewTaskRecord;
use hubuum::storage::postgres::prelude::*;
use hubuum::storage::postgres::{
    init_postgres_pool_with_statement_timeout, with_connection, with_transaction,
};
use hubuum::storage::with_mutation_provenance;
use hubuum::test_support::create_audit_event;
use hubuum::traits::CanSave;

fn database_url() -> String {
    std::env::var("HUBUUM_DATABASE_URL")
        .expect("HUBUUM_DATABASE_URL must point to the isolated migrated test database")
}

#[tokio::test]
async fn interrupted_restore_is_reconciled_after_the_drain_transition() {
    let pool = init_postgres_pool_with_statement_timeout(
        &database_url(),
        2,
        DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    );
    let root_collection_id = with_connection(&pool, async |conn| {
        collections::table
            .filter(collections::parent_collection_id.is_null())
            .select(collections::id)
            .first::<i32>(conn)
            .await
    })
    .await
    .expect("root collection");
    let provenance_initiator_id = 12_345;
    let provenance_task_id = 54_321;
    let first_class = with_mutation_provenance(
        &pool,
        Some(MutationProvenance::worker(
            Some(PrincipalId::new(provenance_initiator_id).unwrap()),
            TaskId::new(provenance_task_id).unwrap(),
        )),
        NewHubuumClass {
            name: "restore_roundtrip_first".to_string(),
            description: "restore round-trip fixture".to_string(),
            collection_id: root_collection_id,
            json_schema: None,
            validate_schema: Some(false),
        }
        .save_without_events(&pool),
    )
    .await
    .expect("first class");
    let second_class = NewHubuumClass {
        name: "restore_roundtrip_second".to_string(),
        description: "restore round-trip fixture".to_string(),
        collection_id: root_collection_id,
        json_schema: None,
        validate_schema: Some(false),
    }
    .save_without_events(&pool)
    .await
    .expect("second class");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: first_class.id,
        to_hubuum_class_id: second_class.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: Some(ObjectRelationLimit::new(1).unwrap()),
        to_max_relations: None,
    }
    .save_without_events(&pool)
    .await
    .expect("class relation");
    let historical_task_id = with_connection(&pool, async |conn| {
        diesel::insert_into(tasks::table)
            .values((
                NewTaskRecord {
                    kind: TaskKind::Reindex.as_str().to_string(),
                    status: TaskStatus::Succeeded.as_str().to_string(),
                    submitted_by: None,
                    idempotency_key: Some("pre-backup-history".to_string()),
                    request_hash: None,
                    request_payload: None,
                    summary: Some("completed before backup".to_string()),
                    total_items: 1,
                    processed_items: 1,
                    success_items: 1,
                    failed_items: 0,
                    submitted_token_id: None,
                    submitted_token_scoped: false,
                    submitted_token_scopes: serde_json::json!([]),
                    request_redacted_at: None,
                    started_at: Some(chrono::Utc::now().naive_utc()),
                    finished_at: Some(chrono::Utc::now().naive_utc()),
                },
                tasks::initiator_user_id.eq(Some(provenance_initiator_id)),
            ))
            .returning(tasks::id)
            .get_result::<i32>(conn)
            .await
    })
    .await
    .expect("historical task");
    let historical_task_event = NewEvent::new(
        EntityType::Task,
        Action::Succeeded,
        ActorKind::Worker,
        "Historical task completed",
    )
    .unwrap()
    .with_entity_id(EventEntityId::new(historical_task_id).unwrap())
    .with_mutation_provenance(&MutationProvenance::worker(
        Some(PrincipalId::new(provenance_initiator_id).unwrap()),
        TaskId::new(historical_task_id).unwrap(),
    ));
    let historical_task_event_id = historical_task_event.event_id();
    create_audit_event(&pool, &historical_task_event)
        .await
        .expect("historical task event");
    let document = create_backup_document(
        &pool,
        &BackupRequest {
            include_history: true,
        },
    )
    .await
    .expect("full backup document");
    let document = serde_json::to_vec(&document).expect("serialize backup document");
    let settings = RestoreSettings::new(60, document.len() + 1).expect("restore settings");
    let initiator =
        RestoreInitiator::new(None, "test", "restore-roundtrip").expect("restore initiator");
    let request = RestoreStageRequest::new(initiator, document.clone()).expect("restore request");
    let staged = stage_restore(&pool, &settings, request)
        .await
        .expect("stage restore");
    let marker_task_id = with_connection(&pool, async |conn| {
        diesel::insert_into(tasks::table)
            .values(NewTaskRecord {
                kind: TaskKind::Reindex.as_str().to_string(),
                status: TaskStatus::Queued.as_str().to_string(),
                submitted_by: None,
                idempotency_key: None,
                request_hash: None,
                request_payload: None,
                summary: Some("created after backup".to_string()),
                total_items: 0,
                processed_items: 0,
                success_items: 0,
                failed_items: 0,
                submitted_token_id: None,
                submitted_token_scoped: false,
                submitted_token_scopes: serde_json::json!([]),
                request_redacted_at: None,
                started_at: None,
                finished_at: None,
            })
            .returning(tasks::id)
            .get_result::<i32>(conn)
            .await
    })
    .await
    .expect("post-backup marker task");

    with_transaction(&pool, async |conn| {
        let interrupted_at = chrono::Utc::now().naive_utc() - chrono::Duration::seconds(61);
        diesel::update(restore_jobs::table.filter(restore_jobs::id.eq(staged.id)))
            .set((
                restore_jobs::status.eq(RestoreJobStatus::Confirmed.as_str()),
                restore_jobs::confirmed_at.eq(Some(interrupted_at)),
            ))
            .execute(conn)
            .await?;
        diesel::update(system_maintenance::table.filter(system_maintenance::id.eq(1_i16)))
            .set((
                system_maintenance::state.eq("draining"),
                system_maintenance::restore_job_id.eq(Some(staged.id)),
                system_maintenance::generation.eq(system_maintenance::generation + 1_i64),
            ))
            .execute(conn)
            .await?;
        Ok::<_, diesel::result::Error>(())
    })
    .await
    .expect("simulate interrupted restore after drain transition");

    reconcile_interrupted_restore(&pool)
        .await
        .expect("reconcile interrupted restore");

    let (
        restore_job_exists,
        marker_exists,
        historical_task,
        historical_task_event,
        class_history_provenance,
        restore_event,
    ) = with_connection(&pool, async |conn| {
        let restore_job = restore_jobs::table
            .filter(restore_jobs::id.eq(staged.id))
            .select(restore_jobs::id)
            .first::<i64>(conn)
            .await
            .optional()?;
        let marker = tasks::table
            .filter(tasks::id.eq(marker_task_id))
            .select(tasks::id)
            .first::<i32>(conn)
            .await
            .optional()?;
        let history = tasks::table
            .filter(tasks::id.eq(historical_task_id))
            .select((tasks::id, tasks::idempotency_key, tasks::initiator_user_id))
            .first::<(i32, Option<String>, Option<i32>)>(conn)
            .await
            .optional()?;
        let historical_task_event = events::table
            .filter(events::event_id.eq(historical_task_event_id.as_uuid()))
            .select((events::initiator_user_id, events::task_id))
            .first::<(Option<i32>, Option<i32>)>(conn)
            .await
            .optional()?;
        let class_history_provenance = hubuumclass_history::table
            .filter(hubuumclass_history::id.eq(first_class.id))
            .order(hubuumclass_history::history_id.asc())
            .select((
                hubuumclass_history::actor_kind,
                hubuumclass_history::initiator_user_id,
                hubuumclass_history::task_id,
            ))
            .first::<(Option<String>, Option<i32>, Option<i32>)>(conn)
            .await
            .optional()?;
        let restore_event = events::table
            .filter(events::entity_type.eq("restore"))
            .filter(events::action.eq("succeeded"))
            .order(events::id.desc())
            .select((events::actor_kind, events::metadata))
            .first::<(String, serde_json::Value)>(conn)
            .await
            .optional()?;
        Ok::<_, diesel::result::Error>((
            restore_job,
            marker,
            history,
            historical_task_event,
            class_history_provenance,
            restore_event,
        ))
    })
    .await
    .expect("restored data lookup");
    assert_eq!(
        (
            restore_job_exists,
            marker_exists,
            historical_task,
            restore_event.as_ref().map(|event| event.0.as_str()),
            restore_event
                .as_ref()
                .and_then(|event| event.1.get("backup_sha256"))
                .and_then(serde_json::Value::as_str),
        ),
        (
            None,
            None,
            Some((historical_task_id, None, Some(provenance_initiator_id))),
            Some("system"),
            Some(staged.sha256.as_str()),
        )
    );
    assert_eq!(
        historical_task_event,
        Some((Some(provenance_initiator_id), Some(historical_task_id)))
    );
    assert_eq!(
        class_history_provenance,
        Some((
            Some("worker".to_string()),
            Some(provenance_initiator_id),
            Some(provenance_task_id),
        ))
    );
    let (relation_exists, reachability_exists) = with_connection(&pool, async |conn| {
        let relation_exists = hubuumclass_relation::table
            .filter(hubuumclass_relation::id.eq(class_relation.id))
            .select((
                hubuumclass_relation::id,
                hubuumclass_relation::from_max_relations,
                hubuumclass_relation::to_max_relations,
            ))
            .first::<(i32, Option<i32>, Option<i32>)>(conn)
            .await
            .optional()?;
        let reachability_exists = hubuumclass_reachability::table
            .filter(hubuumclass_reachability::ancestor_class_id.eq(first_class.id))
            .filter(hubuumclass_reachability::descendant_class_id.eq(second_class.id))
            .select(hubuumclass_reachability::depth)
            .first::<i32>(conn)
            .await
            .optional()?;
        Ok::<_, diesel::result::Error>((relation_exists, reachability_exists))
    })
    .await
    .expect("restored class graph");
    assert_eq!(
        (
            relation_exists,
            reachability_exists,
            maintenance_state(&pool).await.unwrap(),
        ),
        (
            Some((class_relation.id, Some(1), None)),
            Some(1),
            "normal".to_string(),
        )
    );

    let initiator = RestoreInitiator::new(None, "test", "restore-confirmation")
        .expect("restore confirmation initiator");
    let request = RestoreStageRequest::new(initiator, document).expect("restore request");
    let confirmed_stage = stage_restore(&pool, &settings, request)
        .await
        .expect("stage confirmation-path restore");
    let completed = confirm_restore(
        &pool,
        RestoreJobID::new(confirmed_stage.id).expect("positive staged restore id"),
        &RestoreConfirmRequest {
            restore_capability: confirmed_stage
                .restore_capability
                .clone()
                .expect("restore capability"),
            sha256: confirmed_stage.sha256,
            confirmation: RESTORE_CONFIRMATION_PHRASE.to_string(),
        },
    )
    .await
    .expect("confirm restore");
    let remaining_restore_jobs = with_connection(&pool, async |conn| {
        restore_jobs::table.count().get_result::<i64>(conn).await
    })
    .await
    .expect("remaining restore jobs");
    assert_eq!(
        (completed.status, remaining_restore_jobs),
        (RestoreJobStatus::Succeeded, 0)
    );
}
