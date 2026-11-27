use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::task::JoinHandle;
use tracing::Instrument;

use crate::storage::{StorageHandle, with_storage_call_site_send};

static ACTIVE_MAINTENANCE_WORK: AtomicUsize = AtomicUsize::new(0);

/// Counts local request/worker activity across the maintenance state check and
/// the work it protects. The restore coordinator reports this instance drained
/// only after every such guard has dropped.
/// The activity counter can only be acquired through `begin`.
///
/// ```compile_fail
/// use hubuum::restores::MaintenanceActivityGuard;
/// let guard = MaintenanceActivityGuard;
/// ```
pub struct MaintenanceActivityGuard {
    counter: &'static AtomicUsize,
}

impl MaintenanceActivityGuard {
    pub fn begin() -> Self {
        ACTIVE_MAINTENANCE_WORK.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: &ACTIVE_MAINTENANCE_WORK,
        }
    }

    /// Transfer an admitted operation to a worker before its caller can be cancelled.
    pub(crate) fn spawn<F>(self, storage: StorageHandle, work: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let site = StorageHandle::current_call_site();
        tokio::spawn(
            async move {
                let _activity = self;
                with_storage_call_site_send(&storage, site, work).await
            }
            .in_current_span(),
        )
    }
}

impl Drop for MaintenanceActivityGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) fn active_maintenance_work() -> usize {
    ACTIVE_MAINTENANCE_WORK.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use actix_web::{App, HttpResponse, middleware::from_fn, test, web};

    use crate::permissions::{AppContext, LocalPermissionBackend};
    use crate::storage::StorageHandle;

    #[tokio::test]
    async fn detached_worker_stays_counted_after_its_caller_leaves() {
        use super::{AtomicUsize, MaintenanceActivityGuard, Ordering};
        static COUNT: AtomicUsize = AtomicUsize::new(1);
        let guard = MaintenanceActivityGuard { counter: &COUNT };
        let (release, wait) = tokio::sync::oneshot::channel::<()>();
        let task = guard.spawn(StorageHandle::memory(), async move { wait.await.unwrap() });
        let worker = task.abort_handle();
        drop(task);
        tokio::task::yield_now().await;
        assert_eq!(COUNT.load(Ordering::Acquire), 1);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(COUNT.load(Ordering::Acquire), 0);
    }

    #[rstest::rstest]
    #[case::request(crate::storage::StorageCallSite::HttpRequest)]
    #[case::task_worker(crate::storage::StorageCallSite::TaskWorker)]
    #[tokio::test]
    async fn worker_inherits_its_originating_storage_call_site(
        #[case] site: crate::storage::StorageCallSite,
    ) {
        use crate::storage::{with_mutation_provenance, with_storage_call_site};
        let storage = StorageHandle::memory();
        let observed = with_storage_call_site(
            &storage,
            site,
            with_mutation_provenance(&storage, None, async {
                super::MaintenanceActivityGuard::begin()
                    .spawn(storage.clone(), async {
                        StorageHandle::current_call_site()
                    })
                    .await
                    .unwrap()
            }),
        )
        .await;
        assert_eq!(observed, site);
    }

    #[actix_web::test]
    async fn admitted_http_handler_remains_counted_until_it_returns() {
        let storage = StorageHandle::memory();
        let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppContext::new(storage, permissions)))
                .wrap(from_fn(crate::middlewares::reject_during_maintenance))
                .route(
                    "/api/v1/auth/interactions",
                    web::get().to(|| async {
                        tokio::task::yield_now().await;
                        assert!(
                            super::active_maintenance_work() > 0,
                            "restore draining must still see the admitted handler"
                        );
                        HttpResponse::Ok().finish()
                    }),
                ),
        )
        .await;
        let response = test::TestRequest::get()
            .uri("/api/v1/auth/interactions")
            .send_request(&app)
            .await;
        assert!(response.status().is_success());
    }
}
