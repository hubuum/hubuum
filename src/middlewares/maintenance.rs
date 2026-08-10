use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, ResponseError};

use crate::config::DEFAULT_METRICS_PATH;
use crate::config::running::RunningConfig;
use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::restores::{MaintenanceActivityGuard, current_maintenance_state};
use crate::storage::capabilities::{StorageCallSite, with_storage_call_site};

fn allowed_during_maintenance(path: &str, metrics_path: Option<&str>) -> bool {
    matches!(path, "/healthz" | "/readyz")
        || path == DEFAULT_METRICS_PATH
        || metrics_path == Some(path)
        || (path.starts_with("/api/v1/restores/") && path.ends_with("/status"))
}

fn initiates_restore(path: &str) -> bool {
    path.starts_with("/api/v1/restores/") && path.ends_with("/confirm")
}

pub async fn reject_during_maintenance(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    with_storage_call_site(StorageCallSite::HttpRequest, async move {
        let metrics_path = req
            .app_data::<Data<RunningConfig>>()
            .map(|config| config.server.metrics_path.as_str());
        if !allowed_during_maintenance(req.path(), metrics_path) {
            // Begin before reading maintenance state. If draining wins the race,
            // this request is rejected; if the request saw normal first, the
            // coordinator must wait for this guard to drop.
            // The confirmation request owns the drain operation and therefore
            // cannot wait on itself. Its transactional state transition and
            // advisory lock serialize concurrent confirmations.
            let _activity = (!initiates_restore(req.path())).then(MaintenanceActivityGuard::begin);
            let backend = AppContext::from_http_request(req.request())?;
            let state = with_storage_call_site(
                StorageCallSite::RequestMaintenance,
                current_maintenance_state(backend.backend()),
            )
            .await?;
            if !state.is_normal() {
                let response = ApiError::ServiceUnavailable(format!(
                    "Hubuum is in '{state}' maintenance for a destructive restore"
                ))
                .error_response();
                return Ok(req.into_response(response).map_into_boxed_body());
            }
        }
        Ok(next.call(req).await?.map_into_boxed_body())
    })
    .await
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{allowed_during_maintenance, initiates_restore};

    #[rstest]
    #[case::health("/healthz", None, true)]
    #[case::readiness("/readyz", None, true)]
    #[case::default_metrics("/metrics", None, true)]
    #[case::custom_metrics("/internal/metrics", Some("/internal/metrics"), true)]
    #[case::restore_status("/api/v1/restores/12/status", None, true)]
    #[case::restore_confirmation("/api/v1/restores/12/confirm", None, false)]
    #[case::ordinary_api("/api/v1/classes", None, false)]
    fn maintenance_path_availability(
        #[case] path: &str,
        #[case] metrics_path: Option<&str>,
        #[case] expected: bool,
    ) {
        assert_eq!(allowed_during_maintenance(path, metrics_path), expected);
    }

    #[rstest]
    #[case::restore_confirmation("/api/v1/restores/12/confirm", true)]
    #[case::restore_status("/api/v1/restores/12/status", false)]
    #[case::ordinary_api("/api/v1/classes", false)]
    fn restore_initiation_paths(#[case] path: &str, #[case] expected: bool) {
        assert_eq!(initiates_restore(path), expected);
    }
}
