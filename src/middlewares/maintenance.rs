use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use actix_web::body::{BodySize, BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::Method;
use actix_web::middleware::Next;
use actix_web::web::{Bytes, Data};
use actix_web::{Error, ResponseError};

use crate::config::DEFAULT_METRICS_PATH;
use crate::config::running::RunningConfig;
use crate::errors::ApiError;
use crate::models::MaintenanceState;
use crate::permissions::AppContext;
use crate::restores::{MaintenanceActivityGuard, current_maintenance_state};
use crate::storage::{StorageCallSite, with_storage_call_site};

static HTTP_WORK: LazyLock<Arc<HttpWorkState>> =
    LazyLock::new(|| Arc::new(HttpWorkState::default()));

#[derive(Default)]
struct HttpWorkState {
    active: AtomicUsize,
    abandoned: AtomicBool,
}

impl HttpWorkState {
    fn is_settled(&self) -> bool {
        self.active.load(Ordering::Acquire) == 0 && !self.abandoned.load(Ordering::Acquire)
    }
}

pub(crate) fn http_work_is_settled() -> bool {
    HTTP_WORK.is_settled()
}

/// Cancellation can drop an HTTP future while an issued storage operation is
/// still resolving. A dropped activity count alone cannot prove offline safety.
struct HttpWorkGuard {
    state: Option<Arc<HttpWorkState>>,
}

impl HttpWorkGuard {
    fn begin() -> Self {
        Self::begin_with_state(Arc::clone(&HTTP_WORK))
    }

    fn begin_with_state(state: Arc<HttpWorkState>) -> Self {
        state.active.fetch_add(1, Ordering::AcqRel);
        Self { state: Some(state) }
    }

    fn complete(mut self) {
        if let Some(state) = self.state.take() {
            state.active.fetch_sub(1, Ordering::AcqRel);
        }
    }

    #[cfg(test)]
    fn isolated() -> (Arc<HttpWorkState>, Self) {
        let state = Arc::new(HttpWorkState::default());
        let guard = Self::begin_with_state(Arc::clone(&state));
        (state, guard)
    }
}

impl Drop for HttpWorkGuard {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.abandoned.store(true, Ordering::Release);
            state.active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn allowed_during_maintenance(path: &str, metrics_path: Option<&str>) -> bool {
    matches!(path, "/healthz" | "/readyz")
        || path == DEFAULT_METRICS_PATH
        || metrics_path == Some(path)
        || (path.starts_with("/api/v1/restores/") && path.ends_with("/status"))
}

fn initiates_restore(method: &Method, path: &str) -> bool {
    method == Method::POST
        && path
            .strip_prefix("/api/v1/restores/")
            .and_then(|path| path.strip_suffix("/confirm"))
            .is_some_and(|id| id.parse::<i64>().is_ok_and(|id| id > 0))
}

/// Preserve drain ownership after the handler has returned, until its response
/// body finishes or is dropped by a disconnected client.
struct MaintenanceBody {
    body: BoxBody,
    activity: Option<MaintenanceActivityGuard>,
    http_work: Option<HttpWorkGuard>,
}

impl MaintenanceBody {
    fn new(
        body: BoxBody,
        activity: Option<MaintenanceActivityGuard>,
        http_work: HttpWorkGuard,
    ) -> Self {
        // Empty bodies need not be polled. Fully materialized bytes cannot
        // originate more storage work, even when HEAD suppresses transmission.
        // Keep maintenance activity until network consumption or body drop.
        let (body, materialized) = if matches!(body.size(), BodySize::None | BodySize::Sized(0)) {
            (body, true)
        } else {
            match body.try_into_bytes() {
                Ok(bytes) => (BoxBody::new(bytes), true),
                Err(body) => (body, false),
            }
        };
        let http_work = if materialized {
            http_work.complete();
            None
        } else {
            Some(http_work)
        };
        Self {
            body,
            activity,
            http_work,
        }
    }

    fn complete(&mut self) {
        self.body = BoxBody::new(());
        self.settle();
    }

    fn settle(&mut self) {
        self.activity.take();
        if let Some(work) = self.http_work.take() {
            work.complete();
        }
    }
}

impl MessageBody for MaintenanceBody {
    type Error = <BoxBody as MessageBody>::Error;

    fn size(&self) -> BodySize {
        self.body.size()
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let result = self.body.as_pin_mut().poll_next(context);
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            self.complete();
        }
        result
    }
}

async fn protect_response<B: MessageBody + 'static>(
    activity: Option<MaintenanceActivityGuard>,
    http_work: HttpWorkGuard,
    response: impl Future<Output = Result<ServiceResponse<B>, Error>>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let response = match response.await {
        Ok(response) => response,
        Err(error) => {
            http_work.complete();
            return Err(error);
        }
    };
    Ok(
        response
            .map_body(|_, body| MaintenanceBody::new(body.boxed(), activity, http_work).boxed()),
    )
}

pub async fn reject_during_maintenance(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    reject_with_activity(
        req,
        next,
        MaintenanceActivityGuard::begin,
        HttpWorkGuard::begin(),
    )
    .await
}

async fn reject_with_activity(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
    begin_activity: impl FnOnce() -> MaintenanceActivityGuard,
    http_work: HttpWorkGuard,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let storage = AppContext::from_http_request(req.request())?
        .backend()
        .clone();
    with_storage_call_site(&storage, StorageCallSite::HttpRequest, async move {
        let metrics_path = req
            .app_data::<Data<RunningConfig>>()
            .map(|config| config.server.metrics_path.as_str());
        let allowed = allowed_during_maintenance(req.path(), metrics_path);
        // Operational requests must remain outside the drain count: status
        // polling must not keep the restore operation waiting for itself.
        let activity =
            (!allowed && !initiates_restore(req.method(), req.path())).then(begin_activity);
        protect_response(activity, http_work, async move {
            // Restore status remains available during restore draining, but must
            // not bypass deletion recovery quarantine.
            if !allowed
                || (req.path().starts_with("/api/v1/restores/") && req.path().ends_with("/status"))
            {
                // Activity begins before the maintenance read. Confirmation
                // owns draining and cannot wait on itself, but its HTTP work
                // must still settle before offline cleanup can be authorized.
                let backend = AppContext::from_http_request(req.request())?;
                let state = with_storage_call_site(
                    backend.backend(),
                    StorageCallSite::RequestMaintenance,
                    current_maintenance_state(backend.backend()),
                )
                .await?;
                if !state.is_normal() && !(state == MaintenanceState::Draining && allowed) {
                    let response = ApiError::ServiceUnavailable(format!(
                        "Hubuum is in '{state}' maintenance for restore or deletion recovery"
                    ))
                    .error_response();
                    return Ok(req.into_response(response).map_into_boxed_body());
                }
            }
            next.call(req)
                .await
                .map(ServiceResponse::map_into_boxed_body)
        })
        .await
    })
    .await
}

#[cfg(test)]
mod tests;
