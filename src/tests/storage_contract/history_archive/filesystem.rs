use super::*;
use hubuum_artifacts::{ArtifactStore, FilesystemArtifactStore};
use std::{fs, os::unix::fs::DirBuilderExt, path::PathBuf};

struct ArchiveRoot(PathBuf);

impl ArchiveRoot {
    fn create() -> Self {
        let path = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(prefix("archive-reader"));
        fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}
impl Drop for ArchiveRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn configure(
    ctx: &mut TestContext,
    root: &ArchiveRoot,
    destination: &Destination,
) -> FilesystemArtifactStore {
    let store = FilesystemArtifactStore::open(&root.0).await.unwrap();
    store.register_destination(destination).await.unwrap();
    let config = format!("{}={}", String::from(destination.id()), root.0.display())
        .parse()
        .unwrap();
    ctx.context = ctx
        .context
        .clone()
        .with_history_archive_filesystem(Some(&config))
        .await
        .unwrap();
    store
}

async fn stage(
    store: &dyn ArtifactStore,
    identity: ArtifactIdentity,
    row: &HistoryArchiveRow,
    resource: ResourceIdentity,
) -> HistorySegment {
    let scope = SegmentScope::new(StorageBackupHistorySection::CollectionHistory, resource);
    let interval = VersionInterval::new(
        resource,
        row.identity().version(),
        row.valid_from(),
        row.valid_to().unwrap(),
    )
    .unwrap();
    let record = SegmentRecord::new(scope.clone(), interval, row.payload().clone());
    HistorySegment::stage(store, identity, scope, vec![record])
        .await
        .unwrap()
}

#[rstest]
#[case::exact(false)]
#[case::as_of(true)]
#[actix_web::test]
async fn filesystem_archive_reads_published_history(#[case] as_of: bool) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let root = ArchiveRoot::create();
            let destination = Destination::new(DestinationId::generate(), prefix("warm")).unwrap();
            let store = configure(ctx, &root, &destination).await;
            let segment = stage(
                &store,
                ArtifactIdentity::new(destination.id()),
                &row,
                ctx.resource,
            )
            .await;
            let reference = store.publish(segment.proof()).await.unwrap();
            ctx.publish(
                HistoryArchiveDescriptor::new(&row, vec![], segment.format_version(), reference)
                    .unwrap(),
            )
            .await;
            let point = if as_of {
                HistoryArchivePoint::AsOf {
                    at: row.valid_from(),
                }
            } else {
                HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                }
            };
            let result = ctx
                .query(point)
                .await
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), HistoryArchiveCoverageState::Complete);
            assert_eq!(view.row(), Some(&row));
            assert!(view.charged_read_bytes() > 0);
        })
        .await;
    }
}

async fn sealed_with_primary_twin(
    ctx: &TestContext,
    destination: &Destination,
    store: &FilesystemArtifactStore,
    row: &HistoryArchiveRow,
) -> HistoryArchiveDescriptor {
    let identity = ArtifactIdentity::new(destination.id());
    let sealed = stage(store, identity.clone(), row, ctx.resource).await;
    let reference = ctx
        .with_archive(async {
            let primary = ctx.storage.artifact_store();
            primary.register_destination(destination).await.unwrap();
            let copy = stage(primary.as_ref(), identity, row, ctx.resource).await;
            primary.publish(copy.proof()).await.unwrap()
        })
        .await;
    assert_eq!(
        *reference.manifest_digest(),
        sealed.proof().manifest().manifest_digest()
    );
    HistoryArchiveDescriptor::new(row, vec![], sealed.format_version(), reference).unwrap()
}

#[actix_web::test]
async fn filesystem_archive_sealed_history_is_unavailable_even_with_a_published_primary_copy() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let root = ArchiveRoot::create();
            let destination =
                Destination::new(DestinationId::generate(), prefix("sealed")).unwrap();
            let store = configure(ctx, &root, &destination).await;
            let sealed = sealed_with_primary_twin(ctx, &destination, &store, &row).await;
            ctx.publish(sealed).await;
            let query = ctx
                .query(HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                })
                .await;
            let result = query
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            let view = result
                .access(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(view.coverage(), HistoryArchiveCoverageState::Unavailable);
            assert!(view.row().is_none());
            assert!(view.charged_read_bytes() > 0);
        })
        .await;
    }
}

#[actix_web::test]
async fn filesystem_archive_preserves_reads_from_other_destinations() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend, async |ctx| {
            let row = ctx.closed().await;
            let root = ArchiveRoot::create();
            let destination = Destination::new(DestinationId::generate(), prefix("other")).unwrap();
            configure(ctx, &root, &destination).await;
            let primary = ctx.descriptor(&row, false, vec![]).await;
            ctx.publish(primary).await;
            let result = ctx
                .query(HistoryArchivePoint::Exact {
                    version: row.identity().version(),
                })
                .await
                .execute(&ctx.context, &ctx.subject, None)
                .await
                .unwrap();
            assert_eq!(
                result
                    .access(&ctx.context, &ctx.subject, None)
                    .await
                    .unwrap()
                    .row(),
                Some(&row)
            );
        })
        .await;
    }
}

#[cfg(feature = "integration-test-support")]
#[rstest]
#[case::filesystem_replaces_primary(false)]
#[case::primary_replaces_filesystem(true)]
#[actix_web::test]
async fn erasure_checks_displaced_and_current_copies_on_their_own_stores(#[case] warm_first: bool) {
    with_overflow_backend(async |ctx| {
        let row = ctx.closed().await;
        let root = ArchiveRoot::create();
        let destination = Destination::new(DestinationId::generate(), prefix("rewrite")).unwrap();
        let warm = configure(ctx, &root, &destination).await;
        let segment = stage(
            &warm,
            ArtifactIdentity::new(destination.id()),
            &row,
            ctx.resource,
        )
        .await;
        let reference = warm.publish(segment.proof()).await.unwrap();
        let warm = HistoryArchiveDescriptor::new(&row, vec![], segment.format_version(), reference)
            .unwrap();
        let primary = ctx.descriptor(&row, false, vec![]).await;
        let (first, latest) = if warm_first {
            (warm, primary)
        } else {
            (primary, warm)
        };
        ctx.publish(first.clone()).await;
        let catalog = ctx.publish(latest.clone()).await;

        let readers = ctx.context.history_archive_readers();
        let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
        let plan = SegmentRewriteSelection::new([row.identity()])
            .unwrap()
            .admit_observed_archive_copies(
                &pin_operator(ctx),
                readers,
                ctx.permit.fence(),
                &mut budget,
            )
            .await
            .unwrap();
        assert_eq!(
            plan.catalogs().get(&ctx.resource),
            Some(&catalog.generation())
        );
        assert_eq!(plan.locations().len(), 2);
        for reference in [first.location(), latest.location()] {
            assert!(plan.locations().contains(reference));
            let store = readers.for_reference(reference);
            let source = RetainedHistorySegment::open(store.as_ref(), reference, &mut budget)
                .await
                .unwrap();
            let verified = source
                .verify_fully_selected(
                    store.as_ref(),
                    plan.for_source(&source).unwrap(),
                    &mut budget,
                )
                .await
                .unwrap();
            assert_eq!(verified.source(), reference);
        }
    })
    .await;
}

#[cfg(feature = "integration-test-support")]
#[actix_web::test]
async fn erasure_checks_displaced_sealed_copy_even_when_primary_has_identical_published_bytes() {
    with_overflow_backend(async |ctx| {
        let row = ctx.closed().await;
        let root = ArchiveRoot::create();
        let destination = Destination::new(DestinationId::generate(), prefix("displaced")).unwrap();
        let warm = configure(ctx, &root, &destination).await;
        let sealed = sealed_with_primary_twin(ctx, &destination, &warm, &row).await;
        ctx.publish(sealed).await;
        ctx.publish(ctx.descriptor(&row, false, vec![]).await).await;
        let mut budget = ByteLimit::new(SEGMENT_BYTES).start();
        let plan = SegmentRewriteSelection::new([row.identity()])
            .unwrap()
            .admit_observed_archive_copies(
                &pin_operator(ctx),
                ctx.context.history_archive_readers(),
                ctx.permit.fence(),
                &mut budget,
            )
            .await;
        assert!(matches!(plan, Err(crate::errors::ApiError::Conflict(_))));
    })
    .await;
}
