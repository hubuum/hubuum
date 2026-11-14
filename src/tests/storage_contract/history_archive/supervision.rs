use super::*;
use crate::models::Permissions;
use async_trait::async_trait;
use chrono::DateTime;
use hubuum_storage_core::artifacts::TaskArtifactCleanup;
use hubuum_storage_core::artifacts::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use tokio::sync::Notify;
use tokio::time::{Duration, timeout};

fn allowed_policy(ctx: &TestContext) -> Arc<MockTreetopBackend> {
    let policy = Arc::new(MockTreetopBackend::new());
    policy.add_rule(MockAllowRule {
        group_id: ctx.fixture.owned_group.as_ref().unwrap().id().id(),
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: None,
        attrs: ResourceFields {
            collection_id: Some(ctx.fixture.collection.id().id()),
            ..Default::default()
        },
    });
    policy
}

#[actix_web::test]
async fn archive_supervised_read_preserves_candidate_and_query_observations() {
    use crate::models::search::QueryOptions;
    use crate::pagination::paginate_in_memory;
    use crate::permissions::types::{PrincipalRef, ResourceRef};
    use crate::permissions::visibility::{
        authorize_cursor_page_from_storage, capture_candidate_fetches,
    };
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend,async |ctx| {
            let row=ctx.closed().await;
            ctx.publish(ctx.descriptor(&row,false,vec![]).await).await;
            let query=ctx.query(HistoryArchivePoint::Exact{version:row.identity().version()}).await;
            let policy=allowed_policy(ctx);
            let inner_policy=allowed_policy(ctx);
            let storage=storage_handle(&ctx.context);
            let collection=ctx.fixture.collection.id();
            let principal=PrincipalRef::new(ctx.user.principal_id.id(),[ctx.fixture.owned_group.as_ref().unwrap().id().id()]);
            policy.set_authorization_hook(move ||async move {
                let options=QueryOptions::new(vec![],vec![],Some(1),None,false).unwrap();
                let page=authorize_cursor_page_from_storage(inner_policy.as_ref(),&principal,None,vec![Permissions::ReadCollection],&options,
                    |options| {
                        let storage=storage.clone();
                        async move {
                            let current=storage.collection_store().get_collection(collection).await?;
                            let current=crate::services::storage_boundary::collection_from_storage(current)?;
                            paginate_in_memory(vec![current],&options)
                        }
                    },|collection|ResourceRef::collection(collection.id)).await.unwrap();
                assert_eq!(page.rows.len(),1);
            });
            let context=AppContext::new(storage_handle(&ctx.context),policy);
            let ((result,fetched),queries)=hubuum_storage_postgres::capture_queries(capture_candidate_fetches(query.execute(&context,&ctx.subject,None))).await;
            let view=result.unwrap().access(&context,&ctx.subject,None).await.unwrap();
            assert_eq!(view.row(),Some(&row));
            assert_eq!(fetched,vec![1],"the real bounded page fetch in the configured policy callback must remain observed inside the task");
            match ctx.storage.descriptor().kind() {
                StorageBackendKind::Postgres=>assert!(queries.total_queries()>0,"all execute SQL is inside the new supervised task"),
                StorageBackendKind::Memory=>assert_eq!(queries.total_queries(),0),
            }
            let (((),empty),queries)=hubuum_storage_postgres::capture_queries(capture_candidate_fetches(async {})).await;
            assert!(empty.is_empty());
            assert_eq!(queries.total_queries(),0,"observers must not leak into a later scope");
        }).await;
    }
}

struct GatedReadStore {
    store: Arc<dyn TaskArtifactStore>,
    receiver: Mutex<Option<Receiver<()>>>,
    started: Arc<Notify>,
    block_read: Notify,
    blocking_joined: AtomicBool,
}
impl GatedReadStore {
    fn new(store: Arc<dyn TaskArtifactStore>) -> (Arc<Self>, SyncSender<()>) {
        let (sender, receiver) = sync_channel(1);
        (
            Arc::new(Self {
                store,
                receiver: Mutex::new(Some(receiver)),
                started: Arc::new(Notify::new()),
                block_read: Notify::new(),
                blocking_joined: AtomicBool::new(false),
            }),
            sender,
        )
    }
}
#[async_trait]
impl ArtifactStore for GatedReadStore {
    async fn register_destination(&self, value: &Destination) -> ArtifactResult<()> {
        self.store.register_destination(value).await
    }
    async fn stage(&self, id: &ArtifactIdentity, limits: ArtifactLimits) -> ArtifactResult<()> {
        self.store.stage(id, limits).await
    }
    async fn append(
        &self,
        id: &ArtifactIdentity,
        ordinal: u32,
        bytes: &[u8],
    ) -> ArtifactResult<()> {
        self.store.append(id, ordinal, bytes).await
    }
    async fn seal(&self, value: &ArtifactManifest) -> ArtifactResult<()> {
        self.store.seal(value).await
    }
    async fn manifest(&self, id: &ArtifactIdentity) -> ArtifactResult<Vec<u8>> {
        self.store.manifest(id).await
    }
    async fn published_manifest(&self, value: &ArtifactReference) -> ArtifactResult<Vec<u8>> {
        let receiver = self.receiver.lock().unwrap().take();
        if let Some(receiver) = receiver {
            let started = self.started.clone();
            tokio::task::spawn_blocking(move || {
                started.notify_one();
                // Sender Drop also releases the worker on a failing assertion.
                let _ = receiver.recv();
            })
            .await
            .map_err(|_| ArtifactError::Backend("Read fixture worker failed".into()))?;
            self.blocking_joined.store(true, Ordering::SeqCst);
        }
        self.store.published_manifest(value).await
    }
    async fn block(&self, id: &ArtifactIdentity, ordinal: u32) -> ArtifactResult<Option<Vec<u8>>> {
        let value = self.store.block(id, ordinal).await?;
        self.block_read.notify_one();
        Ok(value)
    }
    async fn publish(&self, value: &VerifiedArtifact) -> ArtifactResult<ArtifactReference> {
        self.store.publish(value).await
    }
    async fn abort(&self, id: &ArtifactIdentity) -> ArtifactResult<()> {
        self.store.abort(id).await
    }
    async fn cleanup(&self) -> ArtifactResult<u32> {
        self.store.cleanup().await
    }
}
#[async_trait]
impl TaskArtifactStore for GatedReadStore {
    async fn abort_task(&self, cleanup: &TaskArtifactCleanup) -> ArtifactResult<()> {
        self.store.abort_task(cleanup).await
    }

    async fn bind_task(
        &self,
        id: &ArtifactIdentity,
        lease: &StorageTaskLease,
        reservation: ArtifactReservation,
        expires: DateTime<Utc>,
    ) -> ArtifactResult<()> {
        self.store.bind_task(id, lease, reservation, expires).await
    }
}

#[actix_web::test]
async fn archive_supervised_read_keeps_issued_work_after_caller_drop() {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        TestContext::run(backend,async |ctx| {
            let row=ctx.closed().await;
            ctx.publish(ctx.descriptor(&row,false,vec![]).await).await;
            let (store,release)=GatedReadStore::new(ctx.storage.artifact_store());
            let storage=storage_handle(&ctx.context).with_artifact_test_store(store.clone());
            let context=AppContext::new(storage,allowed_policy(ctx));
            let admitted=HistoryArchiveQuery::capture(&context,&ctx.subject,None,HistoryArchiveRequest::collection(ctx.fixture.collection.id(),HistoryArchivePoint::Exact{version:row.identity().version()},Utc::now()+TimeDelta::hours(1))).await.unwrap();
            let HistoryArchiveAdmission::Captured(query)=admitted else { panic!("bounded fixture") };
            let baseline=Arc::strong_count(&store);
            {
                let execute=query.execute(&context,&ctx.subject,None);
                tokio::pin!(execute);
                tokio::select! {
                    result=timeout(Duration::from_secs(5),store.started.notified())=>result.expect("issued read must reach its bounded gate"),
                    result=&mut execute=>panic!("read escaped its issued worker gate: {}",result.is_ok()),
                }
            }
            assert!(!store.blocking_joined.load(Ordering::SeqCst));
            release.send(()).unwrap();
            timeout(Duration::from_secs(5),store.block_read.notified()).await.expect("supervisor must join the issued worker and continue the actual bounded read after caller drop");
            assert!(store.blocking_joined.load(Ordering::SeqCst));
            // Test lifetime only: wait for task-owned context/query references
            // to leave before fixture teardown. This is NOT retirement proof;
            // no joined receipt was delivered and its native owner stays charged.
            timeout(Duration::from_secs(5),async {
                while Arc::strong_count(&store)>=baseline { tokio::task::yield_now().await; }
            }).await.expect("read task must finish its owned awaits before teardown");
        }).await;
    }
}
