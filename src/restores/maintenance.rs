use std::sync::atomic::{AtomicUsize, Ordering};

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
