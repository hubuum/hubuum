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
    let Some((class, principal)) = operation(req.method(), req.match_info().as_str()).zip(actor)
    else {
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
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let started = Instant::now();
    let (initial, stop) = TaskExecutionContext::new(Duration::from_millis(
        hubuum_storage_core::ApiRequestDuration::MAX_MILLISECONDS,
    ))
    .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
    let parent_scope = storage
        .current_execution_scope()
        .with_task_execution(Some(initial.clone()));
    let work_stop = stop.clone();
    let disconnect = DisconnectStop(stop);
    let mut http_work = super::maintenance::take_supervised_http_work(&req);
    #[cfg(test)]
    let candidate_capture = crate::permissions::visibility::CandidateFetchCapture::current();
    let work_storage = storage.clone();
    // Supervision owns admission itself as well as handler execution: dropping
    // the HTTP wait must not abandon an admission that is committing.
    let work = async move {
        storage
            .run_in_scope(parent_scope, async move {
                let permit = match work_storage
                    .admit_api_request(principal, class, input_bytes)
                    .await
                {
                    Ok(permit) => permit,
                    Err(error) => {
                        if (error.api_request_rejection().is_some()
                            || matches!(
                                error.kind(),
                                crate::storage::StorageErrorKind::TaskCancelled
                                    | crate::storage::StorageErrorKind::TaskDeadlineExceeded
                            ))
                            && let Some(guard) = http_work.take()
                        {
                            guard.complete();
                        }
                        let error = match error.kind() {
                            crate::storage::StorageErrorKind::TaskDeadlineExceeded => {
                                ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded)
                            }
                            crate::storage::StorageErrorKind::TaskCancelled => {
                                ApiError::ApiRequestAdmission(
                                    ApiRequestRejection::ReconciliationRequired,
                                )
                            }
                            _ => ApiError::from(error),
                        };
                        return Ok(req
                            .into_response(error.error_response())
                            .map_into_boxed_body());
                    }
                };
                bound_payload(&mut req, permit.policy().input_bytes().bytes());
                let deadline =
                    started + Duration::from_millis(permit.policy().duration().milliseconds());
                let execution = initial.with_earlier_deadline(deadline);
                let active = execution.clone();
                let output_limit =
                    usize::try_from(permit.policy().output_bytes().bytes()).unwrap_or(usize::MAX);
                let scope = work_storage
                    .current_execution_scope()
                    .with_api_request(permit.clone())
                    .with_task_execution(Some(execution));
                let response = work_storage.run_in_scope(scope, async move {
                    if active.check() == Err(TaskStopReason::Cancelled) {
                        return Err(ApiError::ApiRequestAdmission(
                            ApiRequestRejection::ReconciliationRequired,
                        )
                        .into());
                    }
                    if Instant::now() >= deadline {
                        return Err(ApiError::ApiRequestAdmission(
                            ApiRequestRejection::DeadlineExceeded,
                        )
                        .into());
                    }
                    let response = next.call(req).await?.map_into_boxed_body();
                    if Instant::now() >= deadline {
                        return Err(ApiError::ApiRequestAdmission(
                            ApiRequestRejection::DeadlineExceeded,
                        )
                        .into());
                    }
                    let (request, response) = response.into_parts();
                    let (head, body) = response.into_parts();
                    let bytes = to_bytes_limited(body, output_limit)
                        .await
                        .map_err(|_| {
                            ApiError::ApiRequestAdmission(ApiRequestRejection::OutputBytes)
                        })?
                        .map_err(Error::from)?;
                    if Instant::now() >= deadline {
                        return Err(ApiError::ApiRequestAdmission(
                            ApiRequestRejection::DeadlineExceeded,
                        )
                        .into());
                    }
                    Ok::<_, Error>(ServiceResponse::new(
                        request,
                        head.set_body(bytes).map_into_boxed_body(),
                    ))
                });
                tokio::pin!(response);
                let result = match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    &mut response,
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        // Dropping a handler can detach template supervision or
                        // blocking checkpoint publication. Stop further work,
                        // but retain its awaits until issued preparation ends.
                        work_stop.request_stop(TaskStopReason::DeadlineExceeded);
                        let _ = response.await;
                        Err(
                            ApiError::ApiRequestAdmission(ApiRequestRejection::DeadlineExceeded)
                                .into(),
                        )
                    }
                };
                work_storage
                    .settle_api_request(permit)
                    .await
                    .map_err(|error| {
                        tracing::warn!(%error,"API request settlement failed");
                        ApiError::ApiRequestAdmission(ApiRequestRejection::ReconciliationRequired)
                    })?;
                if let Some(guard) = http_work {
                    guard.complete();
                }
                result
            })
            .await
    };
    let work = work.in_current_span().with_current_subscriber();
    let work = crate::storage::SpawnObservationContext::current().run(work);
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
