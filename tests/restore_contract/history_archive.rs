use super::*;
use chrono::{Duration as TimeDelta, Utc};
use hubuum::benchmark_support::history_segments::{
    HistorySegment, SegmentRecord, SegmentScope, VersionInterval,
};
use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::artifacts::{ArtifactIdentity, ArtifactStore, Destination, DestinationId};

#[rstest]
#[case::catalog(false)]
#[case::old_input(true)]
#[tokio::test]
async fn logical_restore_preserves_catalog_and_invalidates_old_captures(
    #[case] old_input: bool,
    #[values(StorageBackendKind::Postgres, StorageBackendKind::Memory)] kind: StorageBackendKind,
) {
    super::task_capacity::with_fixture(kind, async |fixture| {
        let storage = fixture.storage();
        let class = fixture
            .create_class("archive_restore_marker")
            .await
            .unwrap();
        let collection = class.collection_id();
        let identity = storage
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
        fixture.close_collection_history().await.unwrap();
        let source = fixture.capture(true).await.unwrap();
        let registration = storage.register_runtime().await.unwrap();
        let permit = storage
            .capture_application_execution(Some(registration.owner()))
            .await
            .unwrap();
        let setup = async {
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
            let destination = Destination::new(DestinationId::generate(), "restore-history")?;
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
        }
        .await;
        storage.unregister_runtime(registration).await.unwrap();
        let (captured, published) = setup.unwrap();
        fixture.restore(source).await.unwrap();
        let new_registration = storage.register_runtime().await.unwrap();
        let new_permit = storage
            .capture_application_execution(Some(new_registration.owner()))
            .await
            .unwrap();
        let catalog = storage
            .history_archive_catalog(identity.resource(), &new_permit)
            .await;
        let old = storage
            .history_archive_capture(captured.id(), principal, &new_permit)
            .await;
        storage.unregister_runtime(new_registration).await.unwrap();
        if old_input {
            assert!(old.is_err());
        } else {
            assert_eq!(catalog.unwrap(), published);
        }
    })
    .await;
}
