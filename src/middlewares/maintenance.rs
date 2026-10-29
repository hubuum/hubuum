use std::future::Future;
use std::pin::Pin;
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
            self.body = BoxBody::new(());
            self.activity.take();
        }
        result
    }
}

async fn protect_response<B: MessageBody + 'static>(
    activity: Option<MaintenanceActivityGuard>,
    response: impl Future<Output = Result<ServiceResponse<B>, Error>>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let response = response.await?;
    Ok(response.map_body(|_, body| match activity {
        Some(activity) => MaintenanceBody {
            body: body.boxed(),
            activity: Some(activity),
        }
        .boxed(),
        None => body.boxed(),
    }))
}

pub async fn reject_during_maintenance(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    reject_with_activity(req, next, MaintenanceActivityGuard::begin).await
}

async fn reject_with_activity(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
    begin_activity: impl FnOnce() -> MaintenanceActivityGuard,
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
        // Restore status remains available during restore draining, but must not
        // become an exposure bypass during deletion recovery quarantine.
        if !allowed
            || (req.path().starts_with("/api/v1/restores/") && req.path().ends_with("/status"))
        {
            // Begin before reading maintenance state. If draining wins the race,
            // this request is rejected; if the request saw normal first, the
            // coordinator must wait for this guard to drop.
            // The confirmation request owns the drain operation and therefore
            // cannot wait on itself. Its transactional state transition and
            // advisory lock serialize concurrent confirmations.
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
        protect_response(activity, next.call(req)).await
    })
    .await
}

#[cfg(test)]
mod tests;
