use super::task_control::Fixture;
use super::*;
use chrono::Utc;
use hubuum_storage_core::artifacts::*;
use hubuum_storage_core::{StorageExportTaskArtifactContent, TaskCapacityStorage};
use rstest::rstest;

async fn stage(
    fixture: &Fixture,
    lease: &StorageTaskLease,
    size: usize,
) -> (ArtifactIdentity, ArtifactLimits) {
    let store = fixture.backend.artifact_store();
    let destination =
        Destination::new(DestinationId::generate(), prefix("artifact_contract")).unwrap();
    store.register_destination(&destination).await.unwrap();
    let identity = ArtifactIdentity::new(destination.id());
    let limits = ArtifactLimits::builder()
        .block_bytes(1024)
        .artifact_bytes(size as u64)
        .blocks((size as u32).div_ceil(1024))
        .build()
        .unwrap();
    store.stage(&identity, limits).await.unwrap();
    store
        .bind_task(
            &identity,
            lease,
            ArtifactReservation::new(
                *uuid::Uuid::new_v4().as_bytes(),
                size as u64,
                2 * size as u64 + u64::from(limits.manifest_bytes()),
            )
            .unwrap(),
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .unwrap();
    (identity, limits)
}

fn completion(lease: StorageTaskLease, proof: VerifiedArtifact) -> StorageTaskCompletion {
    StorageTaskCompletion::new(
        StorageTaskTerminalUpdate::new(
            lease,
            StorageTaskTerminalStatus::Succeeded,
            StorageTaskResultCounts::try_new(1, 1, 0).unwrap(),
        ),
        StorageTaskEventInput::new("succeeded", "Verified output"),
        StorageTaskCompletionPayload::Export(
            StorageExportTaskArtifact::builder(
                "text/plain",
                StorageExportTaskArtifactContent::Verified(proof),
                serde_json::json!({}),
                serde_json::json!([]),
                Utc::now() + chrono::Duration::hours(1),
            )
            .try_build()
            .unwrap(),
        ),
    )
}

async fn reconcile(store: &dyn TaskArtifactStore) {
    let deadline = tokio::time::Instant::now() + LOCAL_SPOOL_CLEANUP_GRACE + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        assert!(store.cleanup().await.unwrap() <= 16);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn drain(store: &dyn TaskArtifactStore, reference: &ArtifactReference) -> Vec<u8> {
    let mut reader = ArtifactReader::open(store, reference).await.unwrap();
    let mut result = Vec::new();
    while let Some(block) = reader.next_block().await.unwrap() {
        result.extend(block);
    }
    result
}

#[actix_web::test]
async fn an_empty_output_still_retains_manifest_space() {
    let fixture = Fixture::new(
        BackendTestEnvironment::Memory {
            storage: MemoryStorage::new(),
        },
        StorageTaskKind::Export,
    )
    .await;
    let lease = fixture.claim().await;
    let (identity, limits) = stage(&fixture, &lease, 0).await;
    let store = fixture.backend.artifact_store();
    let writer = ArtifactWriter::new(store.as_ref(), identity, limits)
        .await
        .unwrap();
    let manifest = writer.seal().await.unwrap();
    let proof = verify(store.as_ref(), &manifest).await.unwrap();
    fixture
        .backend
        .complete_task(completion(lease, proof))
        .await
        .unwrap();
    assert_eq!(
        fixture
            .backend
            .task_capacity_snapshot()
            .await
            .unwrap()
            .ordinary()
            .resident_bytes(),
        117 + 4 // Empty manifest plus retained {} metadata and [] warnings.
    );
    fixture.cleanup().await;
}

#[actix_web::test]
async fn worker_admission_reserves_manifest_space_before_upload() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let store = fixture.backend.artifact_store();
        let destination =
            Destination::new(DestinationId::generate(), prefix("manifest_capacity")).unwrap();
        store.register_destination(&destination).await.unwrap();
        let identity = ArtifactIdentity::new(destination.id());
        let limits = ArtifactLimits::builder()
            .artifact_bytes(4096)
            .build()
            .unwrap();
        store.stage(&identity, limits).await.unwrap();
        assert!(
            store
                .bind_task(
                    &identity,
                    &lease,
                    ArtifactReservation::new(*uuid::Uuid::new_v4().as_bytes(), 4096, 8192).unwrap(),
                    Utc::now() + chrono::Duration::hours(1),
                )
                .await
                .is_err()
        );
        store.abort(&identity).await.unwrap();
        store.cleanup().await.unwrap();
        fixture.cleanup().await;
    }
}

#[rstest]
#[case::empty(0)]
#[case::partial(123)]
#[case::multiple_blocks(3500)]
#[case::past_legacy_block_limit(1025 * 1024)]
#[actix_web::test]
async fn replica_can_verify_and_atomically_publish_without_the_producer_spool(#[case] size: usize) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        fixture
            .backend
            .renew_task_lease(
                lease.clone(),
                StorageTaskLeaseDuration::from_milliseconds(600_000).unwrap(),
            )
            .await
            .unwrap();
        let (identity, limits) = stage(&fixture, &lease, size).await;
        let store = fixture.backend.artifact_store();
        let content = vec![b'x'; size];
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        writer.write(&content).await.unwrap();
        let manifest = writer.seal().await.unwrap();
        let reference = ArtifactReference::from_catalog(identity, manifest.manifest_digest());
        assert!(
            ArtifactReader::open(store.as_ref(), &reference)
                .await
                .is_err()
        );
        let replica = fixture.environment.storage();
        let remote = replica.artifact_store();
        let durable = ArtifactManifest::decode(
            &remote.manifest(reference.identity()).await.unwrap(),
            reference.identity(),
            ArtifactLimits::format_ceiling(),
        )
        .unwrap();
        let proof = verify(remote.as_ref(), &durable).await.unwrap();
        assert!(
            remote.publish(&proof).await.is_err(),
            "task output needs its task transaction"
        );
        replica
            .complete_task(completion(lease, proof))
            .await
            .unwrap();
        assert_eq!(drain(remote.as_ref(), &reference).await, content);
        let StorageTaskOutputLookup::Available(output) =
            replica.get_export_output(fixture.task.id()).await.unwrap()
        else {
            panic!("completed output missing")
        };
        assert_eq!(output.artifact_reference(), Some(&reference));
        fixture.cleanup().await;
        while store.cleanup_batch().await.unwrap() != ArtifactCleanupBatch::Idle {}
    }
}

#[rstest]
#[case::staging(0)]
#[case::uploaded(1)]
#[case::sealed(2)]
#[case::verified(3)]
#[actix_web::test]
async fn cancellation_fences_each_publication_boundary(#[case] boundary: u8) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, limits) = stage(&fixture, &lease, 20 * 1024).await;
        let store = fixture.backend.artifact_store();
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        if boundary > 0 {
            writer.write(&vec![b'c'; 20 * 1024]).await.unwrap();
        }
        let manifest = if boundary > 1 {
            Some(writer.seal().await.unwrap())
        } else {
            drop(writer);
            None
        };
        let proof = if boundary > 2 {
            Some(
                verify(store.as_ref(), manifest.as_ref().unwrap())
                    .await
                    .unwrap(),
            )
        } else {
            None
        };
        fixture
            .backend
            .request_task_cancellation(fixture.request())
            .await
            .unwrap();
        // The worker still owns local resources until it acknowledges the stop.
        store.cleanup().await.unwrap();
        if let Some(manifest) = &manifest {
            assert_eq!(store.manifest(&identity).await.unwrap(), manifest.encode());
        }
        fixture
            .backend
            .acknowledge_task_stop(lease.clone())
            .await
            .unwrap();
        let before_cleanup = fixture.backend.task_capacity_snapshot().await.unwrap();
        store.cleanup().await.unwrap();
        if fixture.environment.kind() == StorageBackendKind::Memory {
            // This fixture has a private store, so other tests cannot change
            // its capacity between snapshots. Fencing must not free local bytes.
            assert_eq!(
                fixture
                    .backend
                    .task_capacity_snapshot()
                    .await
                    .unwrap()
                    .ordinary()
                    .resident_bytes(),
                before_cleanup.ordinary().resident_bytes()
            );
        }
        if let Some(proof) = proof {
            assert!(
                fixture
                    .backend
                    .complete_task(completion(lease, proof))
                    .await
                    .is_err()
            );
        }
        // Each batch is bounded, and reservations survive renderer quiescence.
        reconcile(store.as_ref()).await;
        if fixture.environment.kind() == StorageBackendKind::Memory {
            assert_eq!(
                fixture
                    .backend
                    .task_capacity_snapshot()
                    .await
                    .unwrap()
                    .ordinary()
                    .resident_bytes()
                    + 2 * limits.artifact_bytes()
                    + u64::from(limits.manifest_bytes()),
                before_cleanup.ordinary().resident_bytes()
            );
        }
        assert!(store.manifest(&identity).await.is_err());
        assert!(store.stage(&identity, limits).await.is_err());
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn artifact_publication_rolls_back_with_task_finalization() {
    let _permit = postgres_permit().await;
    let fixture = Fixture::new(
        BackendTestEnvironment::Postgres {
            pool: pool().get_ref().clone(),
        },
        StorageTaskKind::Export,
    )
    .await;
    let lease = fixture.claim().await;
    let (identity, limits) = stage(&fixture, &lease, 123).await;
    let store = fixture.backend.artifact_store();
    let mut writer = ArtifactWriter::new(store.as_ref(), identity, limits)
        .await
        .unwrap();
    writer.write(b"transactional output").await.unwrap();
    let manifest = writer.seal().await.unwrap();
    let proof = verify(store.as_ref(), &manifest).await.unwrap();
    let reference = proof.reference();
    let completion = completion(lease, proof);
    PostgresFaultController::failing(PostgresFaultPoint::TaskFinalizeAfterEvent)
        .run(fixture.backend.complete_task(completion.clone()))
        .await
        .unwrap_err();
    assert!(
        ArtifactReader::open(store.as_ref(), &reference)
            .await
            .is_err()
    );
    fixture.backend.complete_task(completion).await.unwrap();
    assert_eq!(
        drain(store.as_ref(), &reference).await,
        b"transactional output"
    );
    fixture.cleanup().await;
    store.cleanup().await.unwrap();
}

#[rstest]
#[case::partial(false)]
#[case::sealed(true)]
#[actix_web::test]
async fn lost_worker_fails_without_replaying_and_reconciles_its_bytes(#[case] sealed: bool) {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, limits) = stage(&fixture, &lease, 20 * 1024).await;
        let store = fixture.backend.artifact_store();
        let mut writer = ArtifactWriter::new(store.as_ref(), identity.clone(), limits)
            .await
            .unwrap();
        writer.write(&vec![b'l'; 20 * 1024]).await.unwrap();
        if sealed {
            writer.seal().await.unwrap();
        } else {
            drop(writer);
        }
        fixture
            .backend
            .renew_task_lease(
                lease,
                StorageTaskLeaseDuration::from_milliseconds(1).unwrap(),
            )
            .await
            .unwrap();
        let replica = fixture.environment.storage();
        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                replica.recover_expired_task_leases(100).await.unwrap();
                let (task, _) = replica
                    .get_task_access(fixture.task.id())
                    .await
                    .unwrap()
                    .into_parts();
                if task.status().is_terminal() {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered.status(), StorageTaskStatus::Failed);
        reconcile(store.as_ref()).await;
        assert!(store.manifest(&identity).await.is_err());
        assert!(matches!(
            replica.get_export_output(fixture.task.id()).await.unwrap(),
            StorageTaskOutputLookup::Missing
        ));
        fixture.cleanup().await;
    }
}

#[actix_web::test]
async fn published_bytes_are_immutable_and_repeatedly_verifiable() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let storage = environment.storage();
        let store = storage.artifact_store();
        let destination =
            Destination::new(DestinationId::generate(), prefix("immutable_artifact")).unwrap();
        store.register_destination(&destination).await.unwrap();
        let identity = ArtifactIdentity::new(destination.id());
        let limits = ArtifactLimits::default();
        store.stage(&identity, limits).await.unwrap();
        store.append(&identity, 0, b"immutable").await.unwrap();
        let mut builder = ManifestBuilder::new(identity.clone(), limits);
        builder.push(b"immutable").unwrap();
        let manifest = builder.finish().unwrap();
        store.seal(&manifest).await.unwrap();
        let proof = verify(store.as_ref(), &manifest).await.unwrap();
        let reference = store.publish(&proof).await.unwrap();
        assert!(store.append(&identity, 0, b"different").await.is_err());
        assert!(store.abort(&identity).await.is_err());
        assert_eq!(
            store.published_manifest(&reference).await.unwrap(),
            manifest.encode()
        );
        store.cleanup().await.unwrap();
        assert_eq!(
            store.block(&identity, 0).await.unwrap().unwrap(),
            b"immutable"
        );
    }
}

#[actix_web::test]
async fn admitted_artifact_cannot_be_rebound_to_another_task() {
    let _permit = postgres_permit().await;
    for environment in available_backend_environments() {
        let fixture = Fixture::new(environment, StorageTaskKind::Export).await;
        let lease = fixture.claim().await;
        let (identity, _) = stage(&fixture, &lease, 1024).await;
        let store = fixture.backend.artifact_store();
        let second = Fixture::new(fixture.environment.clone(), StorageTaskKind::Export).await;
        let other = second.claim().await;
        assert!(
            store
                .bind_task(
                    &identity,
                    &other,
                    ArtifactReservation::new(*uuid::Uuid::new_v4().as_bytes(), 1024, 4096).unwrap(),
                    Utc::now() + chrono::Duration::hours(1)
                )
                .await
                .is_err()
        );
        store.abort(&identity).await.unwrap();
        reconcile(store.as_ref()).await;
        second.cleanup().await;
        fixture.cleanup().await;
    }
}

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
