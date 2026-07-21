use base64::Engine;
use hubuum_outbound_http::{
    OutboundHeaders, OutboundHttpError, OutboundMethod, OutboundRequest, validate_outbound_url,
};
use hubuum_templates::prepare_template;
#[cfg(feature = "integration-test-support")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tracing::warn;

use crate::config::{
    DEFAULT_EXPORT_TEMPLATE_FUEL, DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
    DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS, DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
    DEFAULT_REMOTE_CALL_TIMEOUT_MS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::remote_target::insert_remote_call_result;
use crate::db::traits::task::{TaskBackend, TaskStateUpdate};
use crate::errors::ApiError;
use crate::models::{
    NewRemoteCallResult, NewTaskEventRecord, Permissions, RemoteAuthConfig, RemoteHttpMethod,
    RemoteInvocationBodyOverride, RemoteInvocationParameters, RemoteTemplateContext,
    StoredRemoteCallTaskPayload, TaskRecord, TaskStatus, authorize_remote_invocation,
};
use crate::observability::metrics;
use crate::traits::{AuthzSubject, BackendContext};

#[cfg(feature = "integration-test-support")]
static LOCAL_REMOTE_TARGET_TESTS: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "integration-test-support")]
pub(crate) fn enter_local_remote_target_test() {
    LOCAL_REMOTE_TARGET_TESTS.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "integration-test-support")]
pub(crate) fn exit_local_remote_target_test() {
    LOCAL_REMOTE_TARGET_TESTS.fetch_sub(1, Ordering::Relaxed);
}

fn local_remote_targets_enabled_for_tests() -> bool {
    #[cfg(test)]
    return true;

    #[cfg(all(not(test), feature = "integration-test-support"))]
    return LOCAL_REMOTE_TARGET_TESTS.load(Ordering::Relaxed) > 0;

    #[cfg(all(not(test), not(feature = "integration-test-support")))]
    false
}

pub(super) async fn execute_remote_call_task<C>(
    backend: &C,
    task: &TaskRecord,
    user: &impl AuthzSubject,
    scopes: Option<&[Permissions]>,
) -> Result<(), ApiError>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
    let payload = task
        .request_payload
        .clone()
        .ok_or_else(|| ApiError::BadRequest("Remote call task payload is missing".to_string()))?;
    let request: StoredRemoteCallTaskPayload = serde_json::from_value(payload)?;

    task.update_state(
        pool,
        TaskStateUpdate {
            status: TaskStatus::Running,
            summary: None,
            processed_items: 0,
            success_items: 0,
            failed_items: 0,
            started_at: task.started_at,
            finished_at: None,
        },
    )
    .await?;

    let result = execute_remote_call(backend, task.id, user, scopes, &request).await;
    match result {
        Ok(success) => finalize_remote_task(pool, task, success).await,
        Err(error) => {
            let sanitized = crate::tasks::helpers::sanitize_error_for_storage(&error);
            let fallback = NewRemoteCallResult {
                task_id: task.id,
                target_id: Some(request.target_id.id()),
                subject_type: request.subject.subject_type().as_str().to_string(),
                subject_id: request.subject.subject_id(),
                method: "unknown".to_string(),
                rendered_url: "".to_string(),
                response_status: None,
                response_headers: None,
                response_body_preview: None,
                duration_ms: 0,
                success: false,
                error: Some(sanitized.clone()),
            };
            insert_remote_call_result(pool, fallback).await?;
            warn!(
                message = "Remote call task failed before HTTP execution",
                task_id = task.id,
                error = %error
            );
            finalize_remote_task(
                pool,
                task,
                RemoteExecutionOutcome {
                    success: false,
                    summary: sanitized,
                    event_data: None,
                },
            )
            .await
        }
    }
}

struct RemoteExecutionOutcome {
    success: bool,
    summary: String,
    event_data: Option<serde_json::Value>,
}

struct RemoteFailureContext<'a> {
    pool: &'a DbPool,
    task_id: i32,
    target_id: i32,
    subject_type: &'a str,
    subject_id: i32,
    method: &'a str,
}

async fn execute_remote_call<C>(
    backend: &C,
    task_id: i32,
    user: &impl AuthzSubject,
    scopes: Option<&[Permissions]>,
    request: &StoredRemoteCallTaskPayload,
) -> Result<RemoteExecutionOutcome, ApiError>
where
    C: BackendContext + ?Sized,
{
    let pool = backend.db_pool();
    let target = request.target_id.instance(pool).await?;
    let resolved =
        authorize_remote_invocation(backend, user, scopes, &target, &request.subject).await?;

    let context = invocation_context(
        resolved.context,
        request.parameters.clone(),
        request.body_override.clone(),
    )?;

    let rendered_url = render_template("url_template", &target.url_template, &context)?;
    let start = Instant::now();
    let failure_context = RemoteFailureContext {
        pool,
        task_id,
        target_id: target.id,
        subject_type: resolved.subject_type.as_str(),
        subject_id: resolved.subject_id,
        method: target.method.as_str(),
    };
    let normalized_rendered_url = match validate_outbound_url(&rendered_url) {
        Ok(parts) => parts.url().to_string(),
        Err(error) => {
            return record_remote_call_failure(&failure_context, rendered_url, 0, error).await;
        }
    };

    let rendered_headers = render_headers(&target.headers_template, &context)?;
    let rendered_body = target
        .body_template
        .as_deref()
        .map(|template| render_template("body_template", template, &context))
        .transpose()?;

    let mut headers = rendered_headers;
    apply_auth(&mut headers, &target.auth_config)?;

    let timeout_ms = bounded_timeout_ms(target.timeout_ms);
    let preview_limit = get_config()
        .map(|config| config.remote_call_max_response_bytes)
        .unwrap_or(DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES);
    let allow_private_targets = get_config()
        .map(|config| config.remote_call_allow_private_targets)
        .unwrap_or(DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS);

    let local_test_target = local_remote_targets_enabled_for_tests();

    let response_result = OutboundRequest::new(
        outbound_method(target.method),
        normalized_rendered_url.clone(),
        std::time::Duration::from_millis(timeout_ms),
    )
    .headers(headers)
    .body(rendered_body)
    .max_response_bytes(preview_limit)
    .allow_private_targets(allow_private_targets)
    .dangerous_accept_invalid_certs(local_test_target)
    .dangerous_allow_localhost(local_test_target)
    .send()
    .await;

    match response_result {
        Ok(response) => {
            let status = response.status_display();
            let success = response.is_success();
            metrics::remote_call_finished(
                target.method.as_str(),
                status_family(response.status_code()),
                if success { "success" } else { "failure" },
                std::time::Duration::from_millis(
                    u64::try_from(response.duration_ms()).unwrap_or(0),
                ),
            );
            insert_remote_call_result(
                pool,
                NewRemoteCallResult {
                    task_id,
                    target_id: Some(target.id),
                    subject_type: resolved.subject_type.as_str().to_string(),
                    subject_id: resolved.subject_id,
                    method: target.method.as_str().to_string(),
                    rendered_url: response.url().to_string(),
                    response_status: Some(i32::from(response.status_code())),
                    response_headers: Some(response.headers().clone()),
                    response_body_preview: Some(response.body_preview().to_string()),
                    duration_ms: response.duration_ms(),
                    success,
                    error: (!success).then(|| format!("Remote returned HTTP {status}")),
                },
            )
            .await?;

            let summary = if success {
                format!("Remote call succeeded with HTTP {status}")
            } else {
                format!("Remote call failed with HTTP {status}")
            };
            Ok(RemoteExecutionOutcome {
                success,
                summary,
                event_data: Some(serde_json::json!({
                    "status": i32::from(response.status_code()),
                    "duration_ms": response.duration_ms(),
                })),
            })
        }
        Err(error) => {
            let duration_ms = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);
            record_remote_call_failure(
                &failure_context,
                normalized_rendered_url,
                duration_ms,
                error,
            )
            .await
        }
    }
}

async fn record_remote_call_failure(
    context: &RemoteFailureContext<'_>,
    rendered_url: String,
    duration_ms: i32,
    error: OutboundHttpError,
) -> Result<RemoteExecutionOutcome, ApiError> {
    let metric_outcome = remote_error_outcome(&error);
    let api_error = outbound_error_to_api_error(error);
    let message = crate::tasks::helpers::sanitize_error_for_storage(&api_error);
    metrics::remote_call_finished(
        context.method,
        "none",
        metric_outcome,
        std::time::Duration::from_millis(u64::try_from(duration_ms).unwrap_or(0)),
    );
    insert_remote_call_result(
        context.pool,
        NewRemoteCallResult {
            task_id: context.task_id,
            target_id: Some(context.target_id),
            subject_type: context.subject_type.to_string(),
            subject_id: context.subject_id,
            method: context.method.to_string(),
            rendered_url,
            response_status: None,
            response_headers: None,
            response_body_preview: None,
            duration_ms,
            success: false,
            error: Some(message.clone()),
        },
    )
    .await?;
    Ok(RemoteExecutionOutcome {
        success: false,
        summary: message,
        event_data: Some(serde_json::json!({ "duration_ms": duration_ms })),
    })
}

fn status_family(status_code: u16) -> &'static str {
    match status_code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

fn remote_error_outcome(error: &OutboundHttpError) -> &'static str {
    match error {
        OutboundHttpError::Timeout => "timeout",
        OutboundHttpError::DisallowedAddress { .. } => "private_target_rejected",
        OutboundHttpError::InvalidUrl
        | OutboundHttpError::NonHttpsUrl
        | OutboundHttpError::EmbeddedCredentials
        | OutboundHttpError::MissingHost
        | OutboundHttpError::MissingKnownPort
        | OutboundHttpError::InvalidHeaderName { .. }
        | OutboundHttpError::TransportControlledHeader { .. }
        | OutboundHttpError::InvalidHeaderValue { .. } => "validation_rejected",
        OutboundHttpError::DnsResolution { .. }
        | OutboundHttpError::EmptyDnsResolution { .. }
        | OutboundHttpError::ClientBuild(_)
        | OutboundHttpError::ResponseRead(_)
        | OutboundHttpError::Connect
        | OutboundHttpError::Request(_) => "failure",
    }
}

async fn finalize_remote_task(
    pool: &DbPool,
    task: &TaskRecord,
    outcome: RemoteExecutionOutcome,
) -> Result<(), ApiError> {
    let status = if outcome.success {
        TaskStatus::Succeeded
    } else {
        TaskStatus::Failed
    };
    task.finalize_terminal(
        pool,
        TaskStateUpdate {
            status,
            summary: Some(outcome.summary.clone()),
            processed_items: 1,
            success_items: i32::from(outcome.success),
            failed_items: i32::from(!outcome.success),
            started_at: task.started_at,
            finished_at: None,
        },
        NewTaskEventRecord {
            task_id: task.id,
            event_type: status.as_str().to_string(),
            message: outcome.summary,
            data: outcome.event_data,
        },
    )
    .await?;
    Ok(())
}

fn invocation_context(
    mut context: RemoteTemplateContext,
    parameters: RemoteInvocationParameters,
    body_override: RemoteInvocationBodyOverride,
) -> Result<serde_json::Value, ApiError> {
    context.insert("parameters", parameters.into_value())?;
    context.insert("body_override", body_override.into_value())?;
    Ok(context.into_value())
}

fn render_template(
    label: &str,
    template: &str,
    context: &serde_json::Value,
) -> Result<String, ApiError> {
    let (recursion_limit, fuel) = remote_template_limits();
    prepare_template(template)
        .limit_recursion(recursion_limit)
        .limit_fuel(fuel)
        .context(context)
        .render()
        .map_err(|error| ApiError::BadRequest(format!("Failed rendering {label}: {error}")))
}

fn remote_template_limits() -> (usize, u64) {
    get_config()
        .map(|config| {
            (
                config.export_template_recursion_limit,
                config.export_template_fuel,
            )
        })
        .unwrap_or((
            DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
            DEFAULT_EXPORT_TEMPLATE_FUEL,
        ))
}

fn render_headers(
    headers_template: &serde_json::Value,
    context: &serde_json::Value,
) -> Result<OutboundHeaders, ApiError> {
    let mut headers = OutboundHeaders::new();
    let object = headers_template.as_object().ok_or_else(|| {
        ApiError::BadRequest("headers_template must be a JSON object".to_string())
    })?;
    for (name, value) in object {
        let value = value.as_str().ok_or_else(|| {
            ApiError::BadRequest("header template values must be strings".to_string())
        })?;
        let rendered = render_template("header template", value, context)?;
        headers
            .insert(name, &rendered)
            .map_err(outbound_error_to_bad_request)?;
    }
    Ok(headers)
}

fn apply_auth(
    headers: &mut OutboundHeaders,
    auth_config: &RemoteAuthConfig,
) -> Result<(), ApiError> {
    match auth_config {
        RemoteAuthConfig::None => Ok(()),
        RemoteAuthConfig::BearerSecret { secret } => {
            let value = format!("Bearer {}", resolve_secret(secret)?);
            headers.insert("authorization", &value).map_err(|_| {
                ApiError::BadRequest("Resolved bearer secret is not a valid header".to_string())
            })?;
            Ok(())
        }
        RemoteAuthConfig::BasicSecret { username, secret } => {
            let raw = format!("{}:{}", username, resolve_secret(secret)?);
            let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
            headers
                .insert("authorization", &format!("Basic {encoded}"))
                .map_err(|_| {
                    ApiError::BadRequest("Resolved basic secret is not a valid header".to_string())
                })?;
            Ok(())
        }
        RemoteAuthConfig::ApiKeySecret { header, secret } => {
            let value = resolve_secret(secret)?;
            headers
                .insert(header, &value)
                .map_err(|error| match error {
                    OutboundHttpError::InvalidHeaderName { .. } => {
                        ApiError::BadRequest(format!("Invalid API key header name: {header}"))
                    }
                    error @ OutboundHttpError::TransportControlledHeader { .. } => {
                        ApiError::BadRequest(outbound_error_to_api_message(error))
                    }
                    OutboundHttpError::InvalidHeaderValue { .. } => ApiError::BadRequest(
                        "Resolved API key secret is not a valid header".to_string(),
                    ),
                    other => ApiError::BadRequest(outbound_error_to_api_message(other)),
                })?;
            Ok(())
        }
    }
}

fn resolve_secret(secret: &str) -> Result<String, ApiError> {
    let key = format!("HUBUUM_REMOTE_SECRET_{}", secret.to_ascii_uppercase());
    std::env::var(&key).map_err(|_| {
        ApiError::BadRequest(format!(
            "Remote secret reference '{secret}' is not configured"
        ))
    })
}

fn outbound_method(method: RemoteHttpMethod) -> OutboundMethod {
    match method {
        RemoteHttpMethod::Get => OutboundMethod::Get,
        RemoteHttpMethod::Post => OutboundMethod::Post,
        RemoteHttpMethod::Patch => OutboundMethod::Patch,
        RemoteHttpMethod::Delete => OutboundMethod::Delete,
    }
}

fn bounded_timeout_ms(timeout_ms: i32) -> u64 {
    let requested = u64::try_from(timeout_ms).unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS);
    let cap = get_config()
        .map(|config| config.remote_call_timeout_ms)
        .unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS);
    if requested > cap {
        warn!(
            message = "Remote call timeout clamped to configured maximum",
            requested_timeout_ms = requested,
            configured_max_timeout_ms = cap
        );
    }
    requested.min(cap)
}

fn outbound_error_to_api_message(error: OutboundHttpError) -> String {
    match error {
        OutboundHttpError::InvalidUrl => "remote target URL is invalid".to_string(),
        OutboundHttpError::NonHttpsUrl => "remote target URLs must use https".to_string(),
        OutboundHttpError::EmbeddedCredentials => {
            "remote target URLs must not contain embedded credentials".to_string()
        }
        OutboundHttpError::MissingHost => "remote target URL is missing a host".to_string(),
        OutboundHttpError::MissingKnownPort => {
            "remote target URL is missing a known port".to_string()
        }
        OutboundHttpError::DnsResolution { host } => {
            format!("Failed to resolve remote target host '{host}'")
        }
        OutboundHttpError::EmptyDnsResolution { host } => {
            format!("Remote target host '{host}' did not resolve to any address")
        }
        OutboundHttpError::DisallowedAddress { host, address } => {
            format!("Remote target host '{host}' resolves to a disallowed address ({address})")
        }
        OutboundHttpError::ClientBuild(error) => format!("HTTP client error: {error}"),
        OutboundHttpError::ResponseRead(error) => {
            format!("Failed reading remote response: {error}")
        }
        OutboundHttpError::Timeout => "Remote call timed out".to_string(),
        OutboundHttpError::Connect => "Remote connection failed".to_string(),
        OutboundHttpError::Request(error) => format!("Remote call failed: {error}"),
        OutboundHttpError::InvalidHeaderName { name } => format!("Invalid header name: {name}"),
        OutboundHttpError::TransportControlledHeader { name } => {
            format!("Header is controlled by the HTTP transport: {name}")
        }
        OutboundHttpError::InvalidHeaderValue { name } => {
            format!("Invalid header value for {name}")
        }
    }
}

fn outbound_error_to_api_error(error: OutboundHttpError) -> ApiError {
    let internal = matches!(
        &error,
        OutboundHttpError::ClientBuild(_)
            | OutboundHttpError::ResponseRead(_)
            | OutboundHttpError::Request(_)
    );
    let message = outbound_error_to_api_message(error);
    if internal {
        ApiError::InternalServerError(message)
    } else {
        ApiError::BadRequest(message)
    }
}

fn outbound_error_to_bad_request(error: OutboundHttpError) -> ApiError {
    ApiError::BadRequest(outbound_error_to_api_message(error))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(OutboundHttpError::Timeout, "timeout")]
    #[case(
        OutboundHttpError::DisallowedAddress {
            host: "internal.example".to_string(),
            address: IpAddr::V4(Ipv4Addr::LOCALHOST),
        },
        "private_target_rejected"
    )]
    #[case(OutboundHttpError::InvalidUrl, "validation_rejected")]
    #[case(OutboundHttpError::NonHttpsUrl, "validation_rejected")]
    #[case(OutboundHttpError::EmbeddedCredentials, "validation_rejected")]
    #[case(OutboundHttpError::MissingHost, "validation_rejected")]
    #[case(OutboundHttpError::MissingKnownPort, "validation_rejected")]
    #[case(
        OutboundHttpError::InvalidHeaderName {
            name: "bad header".to_string(),
        },
        "validation_rejected"
    )]
    #[case(
        OutboundHttpError::InvalidHeaderValue {
            name: "x-test".to_string(),
        },
        "validation_rejected"
    )]
    #[case(
        OutboundHttpError::TransportControlledHeader {
            name: "content-length".to_string(),
        },
        "validation_rejected"
    )]
    #[case(
        OutboundHttpError::DnsResolution {
            host: "missing.example".to_string(),
        },
        "failure"
    )]
    #[case(
        OutboundHttpError::EmptyDnsResolution {
            host: "empty.example".to_string(),
        },
        "failure"
    )]
    #[case(OutboundHttpError::ClientBuild("client".to_string()), "failure")]
    #[case(OutboundHttpError::ResponseRead("body".to_string()), "failure")]
    #[case(OutboundHttpError::Connect, "failure")]
    #[case(OutboundHttpError::Request("request".to_string()), "failure")]
    fn remote_errors_use_lossless_metric_outcomes(
        #[case] error: OutboundHttpError,
        #[case] expected: &'static str,
    ) {
        assert_eq!(remote_error_outcome(&error), expected);
    }

    #[test]
    fn render_template_supports_curated_filters() {
        // The `tojson` filter is documented for remote target body templates; it must
        // actually render, not just compile, so execution matches the docs.
        let context = serde_json::json!({ "object": { "data": { "host": "h1" } } });
        let rendered =
            render_template("body_template", "{{ object.data | tojson }}", &context).unwrap();
        assert_eq!(rendered, "{\"host\":\"h1\"}");
    }

    #[test]
    fn render_template_is_fuel_bounded() {
        let context = serde_json::json!({});
        let error = render_template(
            "body_template",
            "{% for _ in range(1000000000) %}x{% endfor %}",
            &context,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("fuel")
                || error.to_string().contains("operation")
                || error.to_string().contains("limit")
        );
    }

    #[test]
    fn internal_outbound_errors_are_sanitized_for_storage() {
        let error = outbound_error_to_api_error(OutboundHttpError::ResponseRead(
            "transport failure for https://example.com/secret".to_string(),
        ));

        assert!(matches!(error, ApiError::InternalServerError(_)));
        assert_eq!(
            crate::tasks::helpers::sanitize_error_for_storage(&error),
            "An internal error occurred"
        );
    }

    #[test]
    fn user_actionable_outbound_errors_remain_visible_for_storage() {
        let error = outbound_error_to_api_error(OutboundHttpError::NonHttpsUrl);

        assert!(matches!(error, ApiError::BadRequest(_)));
        assert_eq!(
            crate::tasks::helpers::sanitize_error_for_storage(&error),
            "Invalid input: remote target URLs must use https"
        );
    }
}
