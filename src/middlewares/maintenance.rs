use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::web::Data;
use actix_web::{Error, ResponseError};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::restores::{MaintenanceActivityGuard, maintenance_state};

fn allowed_during_maintenance(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz")
        || (path.starts_with("/api/v1/restores/") && path.ends_with("/status"))
}

fn initiates_restore(path: &str) -> bool {
    path.starts_with("/api/v1/restores/") && path.ends_with("/confirm")
}

pub async fn reject_during_maintenance(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    if !allowed_during_maintenance(req.path()) {
        // Begin before reading maintenance state. If draining wins the race,
        // this request is rejected; if the request saw normal first, the
        // coordinator must wait for this guard to drop.
        // The confirmation request owns the drain operation and therefore
        // cannot wait on itself. Its transactional state transition and
        // advisory lock serialize concurrent confirmations.
        let _activity = (!initiates_restore(req.path())).then(MaintenanceActivityGuard::begin);
        let pool = req.app_data::<Data<DbPool>>().cloned().ok_or_else(|| {
            ApiError::InternalServerError("Database pool is unavailable".to_string())
        })?;
        let state = maintenance_state(&pool).await?;
        if state != "normal" {
            let response = ApiError::ServiceUnavailable(format!(
                "Hubuum is in '{state}' maintenance for a destructive restore"
            ))
            .error_response();
            return Ok(req.into_response(response).map_into_boxed_body());
        }
    }
    Ok(next.call(req).await?.map_into_boxed_body())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::{allowed_during_maintenance, initiates_restore};

    #[rstest]
    #[case::health("/healthz", true)]
    #[case::readiness("/readyz", true)]
    #[case::restore_status("/api/v1/restores/12/status", true)]
    #[case::restore_confirmation("/api/v1/restores/12/confirm", false)]
    #[case::ordinary_api("/api/v1/classes", false)]
    fn maintenance_path_availability(#[case] path: &str, #[case] expected: bool) {
        assert_eq!(allowed_during_maintenance(path), expected);
    }

    #[rstest]
    #[case::restore_confirmation("/api/v1/restores/12/confirm", true)]
    #[case::restore_status("/api/v1/restores/12/status", false)]
    #[case::ordinary_api("/api/v1/classes", false)]
    fn restore_initiation_paths(#[case] path: &str, #[case] expected: bool) {
        assert_eq!(initiates_restore(path), expected);
    }
}
