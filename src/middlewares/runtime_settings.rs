//! Apply runtime-managed settings at the start of each API request.
use actix_web::body::{BoxBody, MessageBody};
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::middleware::Next;
use actix_web::{Error, web::Data};

use crate::pagination::{deployment_page_limits, with_request_page_limits};
use crate::permissions::AppContext;

/// Fix the request's page limits from the settings this replica applies now.
///
/// Changes committed while the request runs take effect on the next request.
/// Without an application context, deployment configuration applies.
pub async fn apply_runtime_settings(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let Ok(deployment) = deployment_page_limits() else {
        return Ok(next.call(req).await?.map_into_boxed_body());
    };
    let policy = req.app_data::<Data<AppContext>>().and_then(|context| {
        context
            .backend()
            .applied_runtime_settings()
            .snapshot()
            .and_then(|snapshot| snapshot.pagination())
    });
    let limits = deployment.with_runtime_default(policy);
    Ok(with_request_page_limits(limits, next.call(req))
        .await?
        .map_into_boxed_body())
}
