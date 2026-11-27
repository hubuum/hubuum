use super::*;

#[rstest::rstest]
#[case::bytes(1, 1000)]
#[case::rows(1024 * 1024, 1)]
#[actix_web::test]
async fn every_available_storage_backend_enforces_backup_budgets(
    #[case] bytes: usize,
    #[case] rows: usize,
) {
    use hubuum_storage_conformance::verify_backup_budget_rejected;

    let _permit = postgres_permit().await;
    for backend in available_backends() {
        verify_backup_budget_rejected(&backend, StorageBackupBudget::new(bytes, rows).unwrap())
            .await
            .unwrap();
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_backup_snapshots() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let fixture = create_backend_object_fixture(
            &backend,
            &prefix("backup_snapshot"),
            vec![serde_json::json!({"backed_up": true})],
        )
        .await;
        let (state, history) = backend
            .capture_backup_replay(
                false,
                StorageBackupBudget::new(256 * 1024 * 1024, 1_000_000).unwrap(),
            )
            .await
            .expect("certified backend should supply a state-only backup snapshot")
            .materialize()
            .expect("replay rows")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::storage::StorageBackupStateSection::ALL.len()
        );
        for section in crate::storage::StorageBackupStateSection::ALL {
            assert!(state.contains_key(section));
        }
        for (section, expected_id) in [
            (
                crate::storage::StorageBackupStateSection::Collections,
                fixture.collection.id().id(),
            ),
            (
                crate::storage::StorageBackupStateSection::Classes,
                fixture.class.id().id(),
            ),
            (
                crate::storage::StorageBackupStateSection::Objects,
                fixture.objects[0].id().id(),
            ),
        ] {
            assert!(
                state[&section]
                    .iter()
                    .any(|row| row.get("id").and_then(serde_json::Value::as_i64)
                        == Some(i64::from(expected_id))),
                "{section} must project the backend's stored fixture"
            );
        }
        assert!(history.is_none());

        let (state, history) = backend
            .capture_backup_replay(
                true,
                StorageBackupBudget::new(256 * 1024 * 1024, 1_000_000).unwrap(),
            )
            .await
            .expect("certified backend should supply a history-inclusive backup snapshot")
            .materialize()
            .expect("replay should preserve the snapshot")
            .into_parts();
        assert_eq!(
            state.len(),
            crate::storage::StorageBackupStateSection::ALL.len()
        );
        for section in crate::storage::StorageBackupStateSection::ALL {
            assert!(state.contains_key(section));
        }
        let history = history.expect("history was requested");
        assert_eq!(
            history.len(),
            crate::storage::StorageBackupHistorySection::ALL.len()
        );
        for section in crate::storage::StorageBackupHistorySection::ALL {
            assert!(history.contains_key(section));
        }
        delete_backend_object_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_export_template_lifecycle() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let owner = create_backend_user(&backend, &prefix("export_template_owner")).await;
        let storage_collection_id = self::collection_id(1);
        let event_context = EventContext::user(owner.principal_id, None, None);
        let name = prefix("export_template");
        let definition = StorageExportTemplateDefinition::new(
            "compatibility fragment",
            "text/plain",
            "Hello {{ object.name }}",
            "fragment",
        );
        let created = backend
            .create_export_template(StorageExportTemplateCreate::new(
                storage_collection_id,
                name.clone(),
                definition,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create an export template")
            .into_value();
        let (metadata, created_collection_id, created_name, _) = created.into_parts();
        let template_id = hubuum_domain::ExportTemplateId::from(metadata.id());
        assert_eq!(created_collection_id, storage_collection_id);
        assert_eq!(created_name, name);

        let loaded = backend
            .get_export_template(template_id)
            .await
            .expect("certified backend should load an export template");
        assert_eq!(
            loaded.into_parts().0.id(),
            ResourceId::new(template_id.id()).unwrap()
        );

        let (templates, total) = backend
            .list_export_templates(StorageExportTemplateListQuery::within_collections(
                vec![storage_collection_id],
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list export templates")
            .into_parts();
        assert_eq!(total, Some(1));
        assert_eq!(templates.len(), 1);

        let siblings = backend
            .list_export_templates_in_collection(storage_collection_id, Some(template_id))
            .await
            .expect("certified backend should list collection template siblings");
        assert!(siblings.is_empty());

        let replacement_name = format!("{name}_updated");
        let replaced = backend
            .replace_export_template(StorageExportTemplateReplace::new(
                template_id,
                storage_collection_id,
                replacement_name.clone(),
                StorageExportTemplateDefinition::new(
                    "updated compatibility fragment",
                    "text/plain",
                    "Updated {{ object.name }}",
                    "fragment",
                ),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should replace an export template")
            .into_value();
        assert_eq!(replaced.into_parts().2, replacement_name);

        backend
            .delete_export_template(StorageExportTemplateDelete::new(
                template_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete an export template")
            .into_value();
        assert!(backend.get_export_template(template_id).await.is_err());
        delete_backend_user(&backend, owner).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_remote_target_lifecycle() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let owner = create_backend_user(&backend, &prefix("remote_target_owner")).await;
        let storage_collection_id = self::collection_id(1);
        let event_context = EventContext::user(owner.principal_id, None, None);
        let name = prefix("remote_target");
        let created = backend
            .create_remote_target(StorageRemoteTargetCreate::new(
                storage_collection_id,
                name.clone(),
                StorageRemoteTargetDefinition::new(
                    "Compatibility remote target",
                    StorageRemoteTargetTransport::try_new(
                        crate::storage::StorageRemoteTargetHttpMethod::Get,
                        "https://compatibility.invalid/collections/{{ collection.id }}",
                        serde_json::json!({}),
                        None,
                        serde_json::json!({"type": "none"}),
                        1_000,
                    )
                    .expect("valid compatibility remote-target transport"),
                    StorageRemoteTargetPolicy::try_new(
                        None,
                        vec![crate::storage::StorageRemoteTargetSubjectType::Collection],
                        true,
                    )
                    .expect("valid compatibility remote-target policy"),
                ),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should create a remote target")
            .into_value();
        let target_id = hubuum_domain::RemoteTargetId::from(created.metadata().id());
        assert_eq!(created.collection_id(), storage_collection_id);

        let loaded = backend
            .get_remote_target(target_id)
            .await
            .expect("certified backend should load a remote target");
        assert_eq!(
            loaded.metadata().id(),
            ResourceId::new(target_id.id()).unwrap()
        );

        let (targets, total) = backend
            .list_remote_targets(StorageRemoteTargetListQuery::new(
                vec![storage_collection_id],
                QueryOptions::new(Vec::new(), Vec::new(), Some(10), None, true)
                    .expect("contract query must be valid"),
            ))
            .await
            .expect("certified backend should list remote targets")
            .into_parts();
        assert_eq!(total, Some(1));
        assert!(
            targets
                .iter()
                .any(|target| target.metadata().id().id() == target_id.id())
        );

        let updated = backend
            .update_remote_target(StorageRemoteTargetUpdate::new(
                target_id,
                loaded.metadata().revision(),
                StorageRemoteTargetPatch::new()
                    .with_name(Some(format!("{name}_updated")))
                    .with_enabled(Some(false)),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should update a remote target")
            .into_value();
        let (metadata, _, updated_name, definition) = updated.into_parts();
        let (_, _, policy) = definition.into_parts();
        let (_, allowed_subject_types, enabled) = policy.into_parts();
        assert_eq!(metadata.id().id(), target_id.id());
        assert_eq!(updated_name, format!("{name}_updated"));
        assert_eq!(
            allowed_subject_types,
            [crate::storage::StorageRemoteTargetSubjectType::Collection]
        );
        assert!(!enabled);

        backend
            .record_remote_target_invocation(StorageRemoteTargetInvocation::new(
                target_id,
                hubuum_domain::TaskId::new(12345).unwrap(),
                crate::storage::StorageRemoteTargetSubjectType::Collection,
                ResourceId::new(storage_collection_id.id()).unwrap(),
                event_context.clone(),
            ))
            .await
            .expect("certified backend should record remote-target invocation provenance")
            .into_value();
        backend
            .delete_remote_target(StorageRemoteTargetDelete::new(
                target_id,
                event_context.clone(),
            ))
            .await
            .expect("certified backend should delete a remote target")
            .into_value();
        assert!(backend.get_remote_target(target_id).await.is_err());
        delete_backend_user(&backend, owner).await;
    }
}

#[actix_web::test]
async fn concurrent_restore_stages_share_the_principals_retained_artifact_budget() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let backend = environment.storage();
        let principal =
            StorageRestoreInitiator::try_new(None, "contract", prefix("stage_capacity")).unwrap();
        let stage = StorageRestoreStageArtifact::try_new(
            principal,
            hubuum_storage_core::StorageRestoreArtifact::from_reader(b"{}".as_slice(), 2).unwrap(),
            "b".repeat(64),
            serde_json::json!({}),
            chrono::Utc::now() + chrono::Duration::hours(1),
        )
        .unwrap();
        let (first, second, third) = tokio::join!(
            backend.stage_restore_artifact(stage.clone()),
            backend.stage_restore_artifact(stage.clone()),
            backend.stage_restore_artifact(stage.clone())
        );
        let mut ids = Vec::new();
        let mut limited = 0;
        for result in [first, second, third] {
            match result {
                Ok(status) => ids.push(status.summary().id()),
                Err(error) if error.kind() == crate::storage::StorageErrorKind::RateLimited => {
                    limited += 1
                }
                Err(error) => panic!("unexpected admission error: {error}"),
            }
        }
        assert_eq!((ids.len(), limited), (2, 1));
        assert!(backend.expire_restore_stage(ids[0]).await.unwrap());
        ids.push(
            backend
                .stage_restore_artifact(stage)
                .await
                .unwrap()
                .summary()
                .id(),
        );
        if let BackendTestEnvironment::Postgres { .. } = environment {
            hubuum_storage_postgres::test_support::delete_restore_jobs(pool().get_ref(), &ids)
                .await
                .unwrap();
        }
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_restore_lifecycle_and_coordination() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let now = chrono::Utc::now();
    let instance_id = uuid::Uuid::new_v4();
    let mut staged_ids = Vec::new();

    for environment in available_backend_environments() {
        let backend = environment.storage();
        let label = prefix("restore");
        let job = backend
            .stage_restore_artifact(
                StorageRestoreStageArtifact::try_new(
                    StorageRestoreInitiator::try_new(None, "compatibility", label.clone())
                        .expect("valid restore initiator"),
                    hubuum_storage_core::StorageRestoreArtifact::from_reader(b"{}".as_slice(), 2)
                        .unwrap(),
                    "b".repeat(64),
                    serde_json::json!({"compatible": true}),
                    now + chrono::Duration::try_hours(1).expect("valid duration"),
                )
                .expect("valid restore staging request"),
            )
            .await
            .expect("certified backend should stage a restore artifact");
        let job_id = job.summary().id();
        if matches!(environment, BackendTestEnvironment::Postgres { .. }) {
            staged_ids.push(job_id);
        }
        assert_eq!(job.summary().status(), StorageRestoreJobStatus::Validated);

        let loaded = backend
            .get_restore_artifact(job_id)
            .await
            .expect("certified backend should load staged restore bytes");
        let (loaded_summary, document, capability_hash) = loaded.into_parts();
        assert_eq!(loaded_summary.id(), job_id);
        assert_eq!(
            serde_json::from_reader::<_, serde_json::Value>(document.unwrap().reader()).unwrap(),
            serde_json::json!({})
        );
        assert_eq!(capability_hash, "b".repeat(64));

        let status = backend
            .get_restore_status(job_id)
            .await
            .expect("certified backend should load document-free restore status");
        let (status_summary, status_capability_hash, validation) = status.into_parts();
        assert_eq!(status_summary.status(), StorageRestoreJobStatus::Validated);
        assert_eq!(status_capability_hash, "b".repeat(64));
        assert_eq!(validation, serde_json::json!({"compatible": true}));

        let snapshot = backend
            .get_restore_coordinator_snapshot()
            .await
            .expect("certified backend should read restore coordination state");
        assert!(snapshot.maintenance_state().is_normal());
        assert_eq!(snapshot.restore_job_id(), None);

        let local_idle = || true;
        let tick = backend
            .tick_restore_coordinator(instance_id, &local_idle, false)
            .await
            .expect("certified backend should publish a coordinator heartbeat");
        assert!(tick.maintenance_state().is_normal());
        let (generation, instances) = backend
            .get_restore_drain_state(
                tick.backend_now() - chrono::Duration::try_minutes(1).expect("valid duration"),
            )
            .await
            .expect("certified backend should report live restore coordinators")
            .into_parts();
        let instance = instances
            .into_iter()
            .find(|instance| instance.instance_id() == instance_id)
            .expect("compatibility coordinator should be visible");
        assert_eq!(instance.maintenance_generation(), generation);
        assert!(!instance.is_drained());
        backend
            .remove_restore_instance(instance_id)
            .await
            .expect("certified backend should remove coordinator membership");

        backend
            .fail_restore_and_resume(StorageRestoreFailure::new(job_id, "compatibility failure"))
            .await
            .expect("certified backend should atomically fail a restore");
        let failed = backend
            .get_restore_artifact(job_id)
            .await
            .expect("failed restore should remain queryable");
        let (failed_summary, failed_document, _) = failed.into_parts();
        assert_eq!(failed_summary.status(), StorageRestoreJobStatus::Failed);
        assert!(failed_document.is_none());

        backend
            .resume_maintenance_without_restore()
            .await
            .expect("orphaned-maintenance recovery should be idempotent");
        backend
            .resume_terminal_restore(job_id)
            .await
            .expect("terminal-restore recovery should be idempotent");

        let expired_label = prefix("expired_restore");
        let expired = backend
            .stage_restore_artifact(
                StorageRestoreStageArtifact::try_new(
                    StorageRestoreInitiator::try_new(None, "compatibility", expired_label.clone())
                        .expect("valid restore initiator"),
                    hubuum_storage_core::StorageRestoreArtifact::from_reader(b"{}".as_slice(), 2)
                        .unwrap(),
                    "d".repeat(64),
                    serde_json::json!({"compatible": true}),
                    now - chrono::Duration::try_minutes(1).expect("valid duration"),
                )
                .expect("valid restore staging request"),
            )
            .await
            .expect("certified backend should stage an expiring restore artifact");
        let expired_id = expired.summary().id();
        if matches!(environment, BackendTestEnvironment::Postgres { .. }) {
            staged_ids.push(expired_id);
        }
        assert!(
            backend
                .expire_restore_stage(expired_id)
                .await
                .expect("certified backend should expire a validated restore")
        );
        let expired_status = backend
            .get_restore_status(expired_id)
            .await
            .expect("expired restore should remain queryable");
        let (expired_summary, _, _) = expired_status.into_parts();
        assert_eq!(expired_summary.status(), StorageRestoreJobStatus::Expired);
    }

    staged_ids.sort_unstable();
    staged_ids.dedup();
    hubuum_storage_postgres::test_support::delete_restore_jobs(pool.get_ref(), &staged_ids)
        .await
        .expect("restore compatibility fixtures should be removed");
}
