use super::*;
use chrono::{Duration as TimeDelta, Utc};
use hubuum::benchmark_support::history_segments::{
    HistorySegment, SegmentRecord, SegmentScope, VersionInterval,
};
use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::artifacts::{ArtifactIdentity, ArtifactStore, Destination, DestinationId};

#[rstest]
#[case::catalog(false, false, None)]
#[case::old_input(true, false, None)]
#[case::older_open_hot(false, true, None)]
#[case::at_retained_closure(false, false, Some(0))]
#[case::after_retained_closure(false, false, Some(1))]
#[case::after_newer_retained_closure(false, true, Some(1))]
#[tokio::test]
async fn logical_restore_preserves_catalog_and_invalidates_old_captures(
    #[case] old_input: bool,
    #[case] older_open_hot: bool,
    #[case] closure_gap_micros: Option<i64>,
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    super::task_capacity::with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let class = fixture
            .create_class("archive_restore_marker")
            .await
            .unwrap();
        let collection = class.collection_id();
        let mut identity = storage
            .resolve_logical_identity(StorageIdentityTarget::Resource {
                kind: StorageIdentityResourceKind::Collection,
                id: collection.into(),
            })
            .await
            .unwrap()
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .unwrap();
        let source = fixture.capture(true).await.unwrap();
        let (sections, _) = source.clone().into_parts();
        let evidence: StorageIdentityEvidence = serde_json::from_value(
            sections[&StorageBackupStateSection::Principals][0]
                .get("logical_identity")
                .unwrap()
                .clone(),
        )
        .unwrap();
        let principal: PrincipalIdentity = match evidence.subject().unwrap() {
            StorageIdentitySubject::Principal { identity } => identity.known_identity().unwrap(),
            _ => panic!("principal evidence"),
        };
        let original = source;
        fixture.close_collection_history().await.unwrap();
        if older_open_hot {
            identity = storage
                .resolve_logical_identity(StorageIdentityTarget::Resource {
                    kind: StorageIdentityResourceKind::Collection,
                    id: collection.into(),
                })
                .await
                .unwrap()
                .subject()
                .and_then(StorageIdentitySubject::record_identity)
                .unwrap();
            fixture.close_collection_history().await.unwrap();
        }
        let source = if older_open_hot || closure_gap_micros.is_some() {
            original
        } else {
            fixture.capture(true).await.unwrap()
        };
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let work = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(permit),
                storage.admit_history_archive_execution(),
            )
            .await
            .unwrap();
        let permit = work.permit().clone();
        let setup = storage
            .run_in_scope(permit.execution_scope().clone(), async {
                let selection = HistoryArchiveSelection::new(
                    StorageBackupHistorySection::CollectionHistory,
                    identity.resource(),
                    HistoryArchivePoint::Exact {
                        version: identity.version(),
                    },
                );
                let captured = storage
                    .capture_history_archive(
                        HistoryArchiveCaptureRequest::new(
                            principal,
                            selection,
                            HistoryArchiveCatalogGeneration::default(),
                            Utc::now() + TimeDelta::hours(1),
                        ),
                        &permit,
                    )
                    .await?;
                let row = captured
                    .hot()
                    .ok_or_else(|| StorageError::not_found("Closed fixture version unavailable"))?;
                let destination_id = DestinationId::generate();
                let destination = Destination::new(
                    destination_id,
                    format!("restore-history-{}", String::from(destination_id)),
                )?;
                storage.register_destination(&destination).await?;
                let scope = SegmentScope::new(
                    StorageBackupHistorySection::CollectionHistory,
                    identity.resource(),
                );
                let record = SegmentRecord::new(
                    scope.clone(),
                    VersionInterval::new(
                        identity.resource(),
                        identity.version(),
                        row.valid_from(),
                        row.valid_to().unwrap(),
                    )?,
                    row.payload().clone(),
                );
                let segment = HistorySegment::stage(
                    storage,
                    ArtifactIdentity::new(destination.id()),
                    scope,
                    vec![record],
                )
                .await?;
                let reference = storage.publish(segment.proof()).await?;
                let descriptor = HistoryArchiveDescriptor::new(row, vec![], 1, reference)?;
                let published = storage
                    .publish_history_archive(
                        HistoryArchiveCatalogGeneration::default(),
                        vec![descriptor],
                        &permit,
                    )
                    .await?;
                Ok::<_, Box<dyn std::error::Error>>((captured, published))
            })
            .await;
        // Only joined fixture stage/readback/publish/CAS, no transport output.
        // Failure retains ownership rather than manufacturing completion.
        let (captured, published) = setup.unwrap();
        let completed = work
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
        fixture.restore(source).await.unwrap();
        let new_registration = storage.register_runtime().await.unwrap();
        let new_permit = storage
            .capture_application_execution(Some(new_registration.owner()))
            .await
            .unwrap();
        let new_work = storage
            .run_in_scope(
                StorageExecutionScope::default().with_application_access(new_permit),
                storage.admit_history_archive_execution(),
            )
            .await
            .unwrap();
        let new_permit = new_work.permit().clone();
        storage
            .run_in_scope(new_permit.execution_scope().clone(), async {
                let catalog = storage
                    .history_archive_catalog(identity.resource(), &new_permit)
                    .await;
                let restored_capture = if older_open_hot || closure_gap_micros.is_some() {
                    let selection = HistoryArchiveSelection::new(
                        StorageBackupHistorySection::CollectionHistory,
                        identity.resource(),
                        HistoryArchivePoint::AsOf {
                            at: closure_gap_micros.map_or_else(
                                || captured.hot().unwrap().valid_from(),
                                |micros| {
                                    captured.hot().unwrap().valid_to().unwrap()
                                        + TimeDelta::microseconds(micros)
                                },
                            ),
                        },
                    );
                    Some(
                        storage
                            .capture_history_archive(
                                HistoryArchiveCaptureRequest::new(
                                    principal,
                                    selection,
                                    published.generation(),
                                    Utc::now() + TimeDelta::hours(1),
                                ),
                                &new_permit,
                            )
                            .await,
                    )
                } else if !old_input {
                    Some(
                        storage
                            .capture_history_archive(
                                HistoryArchiveCaptureRequest::new(
                                    principal,
                                    HistoryArchiveSelection::new(
                                        StorageBackupHistorySection::CollectionHistory,
                                        identity.resource(),
                                        HistoryArchivePoint::Exact {
                                            version: identity.version(),
                                        },
                                    ),
                                    published.generation(),
                                    Utc::now() + TimeDelta::hours(1),
                                ),
                                &new_permit,
                            )
                            .await,
                    )
                } else {
                    None
                };
                let old = storage
                    .history_archive_capture(captured.id(), principal, &new_permit)
                    .await;
                if older_open_hot || closure_gap_micros.is_some() {
                    let restored = restored_capture.unwrap().unwrap();
                    assert_eq!(restored.selection().resource(), identity.resource());
                    assert_eq!(restored.generation(), published.generation());
                    assert_eq!(
                        restored.coverage(Utc::now()),
                        HistoryArchiveCoverageState::Unavailable
                    );
                } else if old_input {
                    assert!(old.is_err());
                } else {
                    let compatible = restored_capture.unwrap().unwrap();
                    assert_eq!(
                        compatible.coverage(Utc::now()),
                        HistoryArchiveCoverageState::Complete
                    );
                    assert_eq!(compatible.hot().unwrap().identity(), identity);
                    assert_eq!(compatible.archive(), published.versions().first());
                    assert_eq!(catalog.unwrap(), published);
                }
            })
            .await;
        // Acceptance assertions above finish before exact owned cleanup; no
        // authority reset, stale permit refresh or old-capture rebinding.
        let completed = new_work
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
        storage.unregister_runtime(new_registration).await.unwrap();
    })
    .await;
}
