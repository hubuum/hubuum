use super::*;
use crate::test_support::integration_test_pool;
use tokio::time::{Duration, timeout};

#[derive(QueryableByName)]
struct Allowed {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    allowed: bool,
}

async fn attempted_child_from_callback(
    storage: &PostgresStorage,
    connection: &mut PostgresConnection,
    parent: HistoryReadContext,
) -> Result<bool, PostgresStorageError> {
    let epoch = parent.epoch();
    let runtime = epoch.application().owner().unwrap();
    // Even exempt control/lease callbacks explicitly hold the real original
    // authority before this parent lock; exemption grants no history bypass.
    let app =
        sql_query("SELECT public.hubuum_application_execution_allowed($1,$2,$3,$4) AS allowed")
            .bind::<SqlUuid, _>(runtime.id())
            .bind::<SqlUuid, _>(runtime.ownership_token())
            .bind::<SqlUuid, _>(epoch.history().incarnation())
            .bind::<BigInt, _>(epoch.history().deletion_revision().value() as i64)
            .get_result::<Allowed>(connection)
            .await?
            .allowed;
    let history = sql_query(
        "SELECT public.hubuum_history_read_authority($1,$2,FALSE) IS NOT NULL AS allowed",
    )
    .bind::<SqlUuid, _>(runtime.id())
    .bind::<SqlUuid, _>(runtime.ownership_token())
    .get_result::<Allowed>(connection)
    .await?
    .allowed;
    let parent_locked = sql_query(
        "SELECT public.hubuum_history_publication_execution($1,$2,$3,$4,$5,$6) AS allowed",
    )
    .bind::<SqlUuid, _>(parent.id())
    .bind::<SqlUuid, _>(parent.ownership_token())
    .bind::<SqlUuid, _>(epoch.history().incarnation())
    .bind::<BigInt, _>(epoch.history().deletion_revision().value() as i64)
    .bind::<BigInt, _>(epoch.history().authority_revision().value() as i64)
    .bind::<BigInt, _>(epoch.generation().value() as i64)
    .get_result::<Allowed>(connection)
    .await?
    .allowed;
    let captured = storage.current_execution_scope();
    let marker = captured.storage_callback_active();
    let child_storage = storage.clone();
    let attempted = tokio::spawn(async move {
        // A default prebuilt inner layer cannot clear the captured marker.
        let prebuilt = child_storage.run_in_scope_send(
            StorageExecutionScope::default(),
            child_storage.admit_history_producer(parent),
        );
        child_storage.run_in_scope_send(captured, prebuilt).await
    });
    let refused = timeout(Duration::from_secs(5), attempted)
        .await
        .expect("callback must reject before a self-blocking parent UPDATE")
        .unwrap()
        .is_err();
    Ok(app && history && parent_locked && marker && refused)
}

#[rstest::rstest]
#[case::connection(0)]
#[case::transaction(1)]
#[case::read_only(2)]
#[case::control(3)]
#[case::task_lease(4)]
#[case::snapshot(5)]
#[case::control_snapshot(6)]
#[tokio::test]
async fn archive_callback_guard_survives_each_native_mode_and_spawn(#[case] mode: u8) {
    let storage = PostgresStorage::unobserved(integration_test_pool(3));
    let registration = storage.register_runtime().await.unwrap();
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let scope = StorageExecutionScope::default().with_application_access(application);
    let parent = storage
        .run_in_scope(
            scope.clone(),
            storage.admit_history_publication(HistoryPublicationKind::ArchiveRead),
        )
        .await
        .unwrap();
    let context = parent.context();
    let scope = scope.with_history_read(context.clone());
    let guarded = storage
        .run_in_scope(scope.clone(), async {
            match mode {
                0 => {
                    storage
                        .runtime()
                        .with_connection(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                1 => {
                    storage
                        .runtime()
                        .with_transaction(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                2 => {
                    storage
                        .runtime()
                        .with_read_connection(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                3 => {
                    storage
                        .runtime()
                        .with_control_transaction(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                4 => {
                    storage
                        .runtime()
                        .with_task_lease_connection(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                5 => {
                    storage
                        .runtime()
                        .with_read_only_snapshot(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                6 => {
                    storage
                        .runtime()
                        .with_control_snapshot(async |connection| {
                            attempted_child_from_callback(&storage, connection, context.clone())
                                .await
                        })
                        .await
                }
                _ => unreachable!(),
            }
        })
        .await
        .unwrap();
    let child = storage
        .run_in_scope(scope, storage.admit_history_producer(context))
        .await
        .unwrap();
    let completed = child
        .after_completed(std::future::ready(Ok(())))
        .await
        .unwrap();
    storage.settle_history_producer(&completed).await.unwrap();
    let completed = parent
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    storage.settle_history_publication(completed).await.unwrap();
    storage.unregister_runtime(registration).await.unwrap();
    assert!(guarded);
}

async fn archive_entry_inside_callback(
    storage: &PostgresStorage,
    connection: &mut PostgresConnection,
    permit: &HistoryArchiveExecutionPermit,
    operation: u8,
) -> Result<bool, PostgresStorageError> {
    // Explicitly establish original authority even for API-exempt control/lease
    // callbacks. Neither exemption grants permission to bypass history safety.
    let guarded =
        attempted_child_from_callback(storage, connection, permit.producer().parent().clone())
            .await?;
    let principal = hubuum_domain::PrincipalIdentity::generate();
    let resource = hubuum_domain::ResourceIdentity::generate();
    let outcome = match operation {
        0 => storage
            .history_archive_catalog(resource, permit)
            .await
            .map(|_| ()),
        1 => storage
            .publish_history_archive(HistoryArchiveCatalogGeneration::default(), vec![], permit)
            .await
            .map(|_| ()),
        2 => storage
            .capture_history_archive(
                HistoryArchiveCaptureRequest::new(
                    principal,
                    HistoryArchiveSelection::new(
                        StorageBackupHistorySection::CollectionHistory,
                        resource,
                        HistoryArchivePoint::AsOf { at: Utc::now() },
                    ),
                    HistoryArchiveCatalogGeneration::default(),
                    Utc::now() + chrono::Duration::hours(1),
                ),
                permit,
            )
            .await
            .map(|_| ()),
        3 => storage
            .history_archive_capture(Uuid::new_v4(), principal, permit)
            .await
            .map(|_| ()),
        4 => storage
            .reserve_history_archive_read(Uuid::new_v4(), principal, 1, permit)
            .await
            .map(|_| ()),
        5 => storage
            .claim_history_archive_publication(negative_entry_request(permit), permit)
            .await
            .map(|_| ()),
        _ => unreachable!(),
    };
    Ok(guarded
        && outcome.is_err_and(|error| {
            error.kind() == StorageErrorKind::Unavailable
                && error.to_string() == "Archive cannot enter inside a storage callback"
        }))
}

#[rstest::rstest]
#[case::connection(0)]
#[case::transaction(1)]
#[case::read_only(2)]
#[case::control(3)]
#[case::task_lease(4)]
#[case::snapshot(5)]
#[case::control_snapshot(6)]
#[tokio::test]
async fn archive_callback_five_entries_reject_before_pool_access(
    #[case] mode: u8,
    #[values(0u8, 1, 2, 3, 4)] operation: u8,
) {
    archive_callback_entry_rejects_before_pool_access(mode, operation).await;
}

#[rstest::rstest]
#[case::connection(0)]
#[case::transaction(1)]
#[case::read_only(2)]
#[case::control(3)]
#[case::task_lease(4)]
#[case::snapshot(5)]
#[case::control_snapshot(6)]
#[tokio::test]
async fn archive_callback_publication_claim_rejects_before_pool_access(#[case] mode: u8) {
    archive_callback_entry_rejects_before_pool_access(mode, 5).await;
}

async fn archive_callback_entry_rejects_before_pool_access(mode: u8, operation: u8) {
    let storage = PostgresStorage::unobserved(integration_test_pool(1));
    let registration = storage.register_runtime().await.unwrap();
    let application = storage
        .capture_application_execution(Some(registration.owner()))
        .await
        .unwrap();
    let owner = storage
        .run_in_scope(
            StorageExecutionScope::default().with_application_access(application),
            storage.admit_history_archive_execution(),
        )
        .await
        .unwrap();
    let permit = owner.permit().clone();
    let guarded = storage
        .run_in_scope(permit.execution_scope().clone(), async {
            match mode {
                0 => {
                    storage
                        .runtime()
                        .with_connection(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                1 => {
                    storage
                        .runtime()
                        .with_transaction(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                2 => {
                    storage
                        .runtime()
                        .with_read_connection(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                3 => {
                    storage
                        .runtime()
                        .with_control_transaction(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                4 => {
                    storage
                        .runtime()
                        .with_task_lease_connection(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                5 => {
                    storage
                        .runtime()
                        .with_read_only_snapshot(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                6 => {
                    storage
                        .runtime()
                        .with_control_snapshot(async |connection| {
                            archive_entry_inside_callback(&storage, connection, &permit, operation)
                                .await
                        })
                        .await
                }
                _ => unreachable!(),
            }
        })
        .await
        .unwrap();
    // Callback/pool/transaction lifetimes have all ended before exact cleanup.
    let catalog = storage
        .run_in_scope(
            permit.execution_scope().clone(),
            storage.history_archive_catalog(hubuum_domain::ResourceIdentity::generate(), &permit),
        )
        .await
        .unwrap();
    let completed = owner
        .after_drained(std::future::ready(Ok(())), std::future::ready(Ok(())))
        .await
        .unwrap();
    storage
        .settle_history_producer(completed.producer())
        .await
        .unwrap();
    storage
        .settle_history_publication(completed.into_publication())
        .await
        .unwrap();
    storage.unregister_runtime(registration).await.unwrap();
    assert!(guarded);
    assert!(catalog.versions().is_empty());
}

// Negative before-pool fixture ONLY. The row/capture are deliberately NOT in the
// database: bypassing the callback guard would wait on the occupied one-slot
// pool. This never proves positive capture/claim admission or provider ownership.
fn negative_entry_request(
    permit: &HistoryArchiveExecutionPermit,
) -> HistoryArchivePublicationRequest {
    use hubuum_domain::{IdentityReference, ResourceIdentity, VersionIdentity};
    use hubuum_storage_core::artifacts::{ArtifactIdentity, DestinationId};
    let now = chrono::Utc::now();
    let identity =
        StorageRecordIdentity::new(ResourceIdentity::generate(), VersionIdentity::generate());
    let evidence = StorageIdentityEvidence::try_new(
        Some(StorageIdentitySubject::Resource {
            identity: IdentityReference::Known {
                identity: identity.resource(),
            },
            version: identity.version(),
        }),
        Default::default(),
    )
    .unwrap();
    let row = HistoryArchiveRow::new(identity, now - chrono::Duration::seconds(2), Some(now - chrono::Duration::seconds(1)), serde_json::json!({"id":1,"name":"negative entry only","description":"","created_at":now - chrono::Duration::seconds(3),"updated_at":now - chrono::Duration::seconds(2),"revision":1,"logical_identity":evidence})).unwrap();
    let capture = HistoryArchiveCapture::capture(
        HistoryArchiveCaptureRequest::new(
            hubuum_domain::PrincipalIdentity::generate(),
            HistoryArchiveSelection::new(
                StorageBackupHistorySection::CollectionHistory,
                identity.resource(),
                HistoryArchivePoint::Exact {
                    version: identity.version(),
                },
            ),
            HistoryArchiveCatalogGeneration::default(),
            now + chrono::Duration::hours(1),
        ),
        &HistoryArchiveCatalog::empty(identity.resource()),
        permit.application(),
        HistoryArchiveInput::Available(row),
        now,
    )
    .unwrap()
    .bind_execution(permit)
    .unwrap();
    let manifest = HistoryArchivePublicationManifest::from_capture(
        &capture,
        permit,
        ArtifactIdentity::new(DestinationId::generate()),
        now,
    )
    .unwrap();
    let intent = HistoryArchiveEncodedArtifactIntent::try_new(
        manifest.location().clone(),
        hubuum_storage_core::artifacts::ArtifactLimits::builder()
            .artifact_bytes(HISTORY_ARCHIVE_PUBLICATION_BYTES)
            .build()
            .unwrap(),
        b"negative before-pool metadata only".to_vec(),
        manifest.request_commitment().unwrap(),
    )
    .unwrap();
    HistoryArchivePublicationRequest::try_new(manifest, intent).unwrap()
}
