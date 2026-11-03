use std::pin::Pin;
use std::time::{Duration, Instant};

use actix_web::body::{BoxBody, MessageBody, to_bytes_limited};
use actix_web::dev::{Payload, ServiceRequest, ServiceResponse};
use actix_web::error::PayloadError;
use actix_web::http::Method;
use actix_web::middleware::Next;
use actix_web::web::Bytes;
use actix_web::{Error, HttpMessage, ResponseError};
use futures::{Stream, StreamExt};
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{ApiRequestAdmissionStorage, ApiRequestClass, ApiRequestRejection};
use hubuum_task_core::{TaskExecutionContext, TaskStopHandle, TaskStopReason};
use tracing::{Instrument, instrument::WithSubscriber};

use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::storage::ExecutionStorage;

/// Selected expensive routes only. This intentionally excludes task discovery,
/// cancellation, event streams, artifact downloads and ordinary resource reads.
pub(crate) fn operation(method: &Method, path: &str) -> Option<ApiRequestClass> {
    let path = path.trim_end_matches('/');
    if method == Method::POST {
        return match path {
            "/api/v1/imports" => Some(ApiRequestClass::ImportSubmission),
            "/api/v1/exports" => Some(ApiRequestClass::ExportSubmission),
            _ => None,
        };
    }
    if method != Method::GET {
        return None;
    }
    let parts: Vec<_> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.get(..2) != Some(&["api", "v1"]) {
        return None;
    }
    if path == "/api/v1/events" {
        return Some(ApiRequestClass::Audit);
    }
    let family = *parts.get(2)?;
    if !matches!(
        family,
        "classes" | "collections" | "export-templates" | "remote-targets" | "iam"
    ) {
        return None;
    }
    let start = if family == "iam" || parts.get(3) == Some(&"by-name") {
        5
    } else {
        4
    };
    let suffix = parts.get(start..)?;
    if suffix == ["object-aggregates"] && family == "classes" {
        return Some(ApiRequestClass::Aggregate);
    }
    if suffix.contains(&"history") {
        return Some(ApiRequestClass::History);
    }
    if suffix.last() == Some(&"events") {
        return Some(ApiRequestClass::Audit);
    }
    None
}

struct DisconnectStop(TaskStopHandle);
impl Drop for DisconnectStop {
    fn drop(&mut self) {
        self.0.request_stop(TaskStopReason::Cancelled);
    }
}

fn bound_payload(req: &mut ServiceRequest, limit: u64) {
    let payload = req.take_payload();
    let stream = futures::stream::unfold(
        (payload, 0_u64, false),
        move |(mut payload, used, stopped)| async move {
            if stopped {
                return None;
            }
            let item = payload.next().await?;
            match item {
                Ok(bytes) => match used.checked_add(bytes.len() as u64) {
                    Some(total) if total <= limit => Some((Ok(bytes), (payload, total, false))),
                    _ => Some((Err(PayloadError::Overflow), (payload, used, true))),
                },
                Err(error) => Some((Err(error), (payload, used, true))),
            }
        },
    );
    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, PayloadError>>>> = Box::pin(stream);
    req.set_payload(Payload::from(stream));
}

pub(super) async fn run(
    mut req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
    actor: Option<PrincipalId>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let Some((class, principal)) = operation(req.method(), req.path()).zip(actor) else {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_boxed_body);
    };
    let storage = AppContext::from_http_request(req.request())?
        .backend()
        .clone();
    let input_bytes = req
        .headers()
        .get("Content-Length")
        .and_then(|header| header.to_str().ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let admission_started = Instant::now();
    let permit = match storage
        .admit_api_request(principal, class, input_bytes)
        .await
    {
        Ok(permit) => permit,
        Err(error) => {
            return Ok(req
                .into_response(ApiError::from(error).error_response())
                .map_into_boxed_body());
        }
    };
    bound_payload(&mut req, permit.policy().input_bytes().bytes());
    let duration = Duration::from_millis(permit.policy().duration().milliseconds())
        .saturating_sub(admission_started.elapsed());
    let output_limit =
        usize::try_from(permit.policy().output_bytes().bytes()).unwrap_or(usize::MAX);
    let (execution, stop) = TaskExecutionContext::new(duration)
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let completion = execution.clone();
    let scope = storage
        .current_execution_scope()
        .with_api_request(permit.clone())
        .with_task_execution(Some(execution));
    let disconnect = DisconnectStop(stop);
    let http_work = super::maintenance::take_supervised_http_work(&req);
    // Dropping the caller's JoinHandle does not abort this supervisor. Cleanup
    // waits for the database owner lock even if a timeout drops handler work.
    #[cfg(any(test, feature = "integration-test-support", feature = "query-capture"))]
    let query_capture = hubuum_storage_postgres::QueryCaptureContext::current();
    #[cfg(test)]
    let candidate_capture = crate::permissions::visibility::CandidateFetchCapture::current();
    let work = async move {
        let response = storage.run_in_scope(scope, async move {
            let response = next.call(req).await?.map_into_boxed_body();
            if completion.check() == Err(TaskStopReason::DeadlineExceeded) {
                return Err(
                    ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded).into(),
                );
            }
            let (request, response) = response.into_parts();
            let (head, body) = response.into_parts();
            let bytes = to_bytes_limited(body, output_limit)
                .await
                .map_err(|_| ApiError::ApiRequestAdmission(ApiRequestRejection::OutputBytes))?
                .map_err(Error::from)?;
            if completion.check() == Err(TaskStopReason::DeadlineExceeded) {
                return Err(
                    ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded).into(),
                );
            }
            Ok::<_, Error>(ServiceResponse::new(
                request,
                head.set_body(bytes).map_into_boxed_body(),
            ))
        });
        let result = if duration.is_zero() {
            drop(response);
            Err(ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded).into())
        } else {
            match tokio::time::timeout(duration, response).await {
                Ok(result) => result,
                Err(_) => {
                    Err(ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded).into())
                }
            }
        };
        storage.settle_api_request(permit).await.map_err(|error| {
            tracing::warn!(%error, "API request settlement failed");
            ApiError::ApiRequestAdmission(ApiRequestRejection::ReconciliationRequired)
        })?;
        if let Some(http_work) = http_work {
            http_work.complete();
        }
        result
    }
    .in_current_span()
    .with_current_subscriber();
    #[cfg(any(test, feature = "integration-test-support", feature = "query-capture"))]
    let work = query_capture.run(work);
    #[cfg(test)]
    let work = candidate_capture.run(work);
    let work = actix_web::rt::spawn(work);
    let result = work.await.unwrap_or_else(|_| {
        Err(ApiError::ApiRequestAdmission(ApiRequestRejection::ReconciliationRequired).into())
    });
    drop(disconnect);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("/healthz")]
    #[case("/readyz")]
    #[case("/api/v1/classes")]
    #[case("/api/v1/classes/42")]
    #[case("/api/v1/classes/by-name/history")]
    #[case("/api/v1/tasks")]
    #[case("/api/v1/tasks/42/events")]
    #[case("/api/v1/exports/42/output")]
    fn cheap_and_operational_paths_are_exempt(#[case] path: &str) {
        assert_eq!(operation(&Method::GET, path), None);
    }

    #[rstest]
    #[case("/api/v1/classes/42/object-aggregates", ApiRequestClass::Aggregate)]
    #[case(
        "/api/v1/classes/by-name/widgets/object-aggregates/",
        ApiRequestClass::Aggregate
    )]
    #[case("/api/v1/collections/42/history", ApiRequestClass::History)]
    #[case("/api/v1/classes/42/objects/history/as-of", ApiRequestClass::History)]
    #[case(
        "/api/v1/classes/42/43/history/computed/as-of",
        ApiRequestClass::History
    )]
    #[case("/api/v1/events", ApiRequestClass::Audit)]
    #[case("/api/v1/iam/users/42/events", ApiRequestClass::Audit)]
    fn measured_paths_are_selected(#[case] path: &str, #[case] class: ApiRequestClass) {
        assert_eq!(operation(&Method::GET, path), Some(class));
    }
}
