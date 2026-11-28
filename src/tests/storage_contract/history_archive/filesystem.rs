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
            let identity = ArtifactIdentity::new(destination.id());
            let sealed = stage(&store, identity.clone(), &row, ctx.resource).await;
            let reference = ctx
                .with_archive(async {
                    let primary = ctx.storage.artifact_store();
                    primary.register_destination(&destination).await.unwrap();
                    let copy = stage(primary.as_ref(), identity, &row, ctx.resource).await;
                    primary.publish(copy.proof()).await.unwrap()
                })
                .await;
            assert_eq!(
                *reference.manifest_digest(),
                sealed.proof().manifest().manifest_digest()
            );
            ctx.publish(
                HistoryArchiveDescriptor::new(&row, vec![], sealed.format_version(), reference)
                    .unwrap(),
            )
            .await;
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
