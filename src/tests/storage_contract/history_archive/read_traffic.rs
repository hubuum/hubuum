//! Local P2 diagnostic for the application service with a PostgreSQL artifact store.
use super::{TestContext, with_isolated_archive_backend};
use crate::events::EventContext;
use crate::permissions::{AppContext, LocalPermissionBackend};
use crate::services::history::segments::{
    HistorySegment, SegmentRecord, SegmentScope, VersionInterval,
};
use crate::storage::StorageBackendKind;
use crate::test_support::archive_test_runtime_database_url;
use chrono::Utc;
use hubuum_artifacts::{ArtifactIdentity, Destination, DestinationId};
use hubuum_storage_core::{
    AuthorizationDataStorage, HistoryArchiveCoverageState, HistoryArchiveDescriptor,
    HistoryArchivePoint, HistoryArchiveRow, HistoryArchiveStorage, StorageAuthorizationGrantKey,
    StorageAuthorizationGrantMutation, StorageAuthorizationPermission, StorageBackupHistorySection,
    StorageCollectionUpdate,
};
use reqwest::Url;
use serde_json::json;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

async fn forward(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    bytes: &AtomicU64,
) -> io::Result<()> {
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = from.read(&mut buffer).await?;
        if count == 0 {
            return to.shutdown().await;
        }
        // Increment before forwarding so a completed query includes its final response.
        bytes.fetch_add(count as u64, Ordering::Relaxed);
        to.write_all(&buffer[..count]).await?;
    }
}

#[derive(Default)]
struct WireTraffic {
    requests: AtomicU64,
    replies: AtomicU64,
}

impl WireTraffic {
    fn observed(&self) -> [u64; 2] {
        [
            self.requests.load(Ordering::Relaxed),
            self.replies.load(Ordering::Relaxed),
        ]
    }

    async fn relay(self: Arc<Self>, listener: TcpListener, host: String, port: u16) {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (client, _) = accepted.unwrap();
                    let host = host.clone();
                    let traffic = self.clone();
                    connections.spawn(async move {
                        let server = TcpStream::connect((host.as_str(), port)).await?;
                        client.set_nodelay(true)?;
                        server.set_nodelay(true)?;
                        let (client_read, client_write) = client.into_split();
                        let (server_read, server_write) = server.into_split();
                        tokio::try_join!(
                            forward(client_read, server_write, &traffic.requests),
                            forward(server_read, client_write, &traffic.replies),
                        )
                    });
                }
                Some(completed) = connections.join_next(), if !connections.is_empty() => {
                    completed.unwrap().unwrap();
                }
            }
        }
    }
}

const REVISIONS: usize = 100;

async fn publish_collection_segment(ctx: &TestContext) -> Vec<HistoryArchiveRow> {
    let mut rows = Vec::with_capacity(REVISIONS);
    for revision in 0..=REVISIONS {
        let current = ctx
            .capture(HistoryArchivePoint::AsOf { at: Utc::now() })
            .await;
        let version = current.hot().unwrap().identity().version();
        let size = match revision {
            98 => 64 * 1024,
            99 => 3 * 1024 * 1024,
            _ => 256,
        };
        let description = format!("{revision:04}{}", "x".repeat(size - 4));
        ctx.storage
            .collection_store()
            .update_collection(
                ctx.fixture.collection.id(),
                StorageCollectionUpdate::new(None, Some(description)),
                &EventContext::system(),
            )
            .await
            .unwrap()
            .into_value();
        if revision > 0 {
            rows.push(
                ctx.capture(HistoryArchivePoint::Exact { version })
                    .await
                    .hot()
                    .unwrap()
                    .clone(),
            );
        }
    }
    ctx.with_archive(async {
        let store = ctx.storage.artifact_store();
        let destination =
            Destination::new(DestinationId::generate(), "p2-runtime-traffic").unwrap();
        store.register_destination(&destination).await.unwrap();
        let scope = SegmentScope::new(StorageBackupHistorySection::CollectionHistory, ctx.resource);
        let records: Vec<_> = rows
            .iter()
            .map(|row| {
                SegmentRecord::new(
                    scope.clone(),
                    VersionInterval::new(
                        ctx.resource,
                        row.identity().version(),
                        row.valid_from(),
                        row.valid_to().unwrap(),
                    )
                    .unwrap(),
                    row.payload().clone(),
                )
            })
            .collect();
        let segment = HistorySegment::stage(
            store.as_ref(),
            ArtifactIdentity::new(destination.id()),
            scope,
            records,
        )
        .await
        .unwrap();
        let location = store.publish(segment.proof()).await.unwrap();
        let descriptors = rows
            .iter()
            .map(|row| {
                HistoryArchiveDescriptor::new(
                    row,
                    vec![],
                    segment.format_version(),
                    location.clone(),
                )
                .unwrap()
            })
            .collect();
        let catalog = ctx
            .storage
            .history_archive_catalog(ctx.resource, &ctx.archive_permit)
            .await
            .unwrap();
        ctx.storage
            .publish_history_archive(catalog.generation(), descriptors, &ctx.archive_permit)
            .await
            .unwrap();
    })
    .await;
    rows
}

#[actix_web::test]
#[ignore = "P2 PostgreSQL archive service wire-traffic diagnostic"]
async fn postgres_archive_service_wire_traffic() {
    let traffic = Arc::new(WireTraffic::default());
    let mut runtime_url = Url::parse(&archive_test_runtime_database_url()).unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let local = listener.local_addr().unwrap();
    let relay = tokio::spawn(traffic.clone().relay(
        listener,
        runtime_url.host_str().unwrap().to_owned(),
        runtime_url.port().unwrap_or(5432),
    ));
    runtime_url.set_host(Some("127.0.0.1")).unwrap();
    runtime_url.set_port(Some(local.port())).unwrap();
    with_isolated_archive_backend(
        StorageBackendKind::Postgres,
        runtime_url.as_str(),
        async |ctx| {
            ctx.storage
                .apply_local_collection_grant(StorageAuthorizationGrantMutation::new(
                    StorageAuthorizationGrantKey::new(
                        ctx.fixture.collection.id(),
                        ctx.fixture.owned_group.as_ref().unwrap().id(),
                    ),
                    [StorageAuthorizationPermission::ReadCollection],
                    false,
                    EventContext::system(),
                ))
                .await
                .unwrap()
                .into_value();
            let bound = ctx.context.backend().clone();
            ctx.context = AppContext::new(
                bound.clone(),
                Arc::new(LocalPermissionBackend::new(bound, "admin".into())),
            );
            let rows = publish_collection_segment(ctx).await;
            for (shape, expected) in [
                ("256B", &rows[0]),
                ("64KiB", &rows[98]),
                ("3MiB", &rows[99]),
            ] {
                for (kind, point, factor, slack) in [
                    (
                        "exact",
                        HistoryArchivePoint::Exact {
                            version: expected.identity().version(),
                        },
                        8,
                        1024 * 1024,
                    ),
                    (
                        "as_of",
                        HistoryArchivePoint::AsOf {
                            at: expected.valid_from(),
                        },
                        16,
                        2 * 1024 * 1024,
                    ),
                ] {
                    let start = traffic.observed();
                    let query = ctx.query(point).await;
                    let captured = traffic.observed();
                    let prepared = query
                        .execute(&ctx.context, &ctx.subject, None)
                        .await
                        .unwrap();
                    let executed = traffic.observed();
                    let view = prepared
                        .access(&ctx.context, &ctx.subject, None)
                        .await
                        .unwrap();
                    let accessed = traffic.observed();
                    assert_eq!(view.coverage(), HistoryArchiveCoverageState::Complete);
                    assert_eq!(view.row(), Some(expected));
                    assert!(view.charged_read_bytes() > 0);
                    let result_bytes = serde_json::to_vec(expected.payload()).unwrap().len() as u64;
                    let allowance = (result_bytes * factor + slack).min(result_bytes + 2 * slack);
                    println!(
                        "ARCHIVE_SERVICE_WIRE {}",
                        json!({
                            "revisions": REVISIONS, "shape": shape, "lookup": kind,
                            "payload_bytes": result_bytes, "p2_allowance_bytes": allowance,
                            "provider_read_bytes": view.charged_read_bytes(),
                            "capture_reply_bytes": captured[1] - start[1],
                            "execute_reply_bytes": executed[1] - captured[1],
                            "access_reply_bytes": accessed[1] - executed[1],
                            "all_reply_bytes": accessed[1] - start[1],
                            "all_request_bytes": accessed[0] - start[0],
                        })
                    );
                }
            }
        },
    )
    .await;
    relay.abort();
    assert!(relay.await.unwrap_err().is_cancelled());
}
