use std::net::SocketAddr;
use std::time::Instant;

use base64::Engine;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tracing::warn;

use crate::can;
use crate::config::{
    DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS, DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES,
    DEFAULT_REMOTE_CALL_TIMEOUT_MS, get_config,
};
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::db::traits::remote_target::insert_remote_call_result;
use crate::db::traits::task::{TaskBackend, TaskStateUpdate};
use crate::errors::ApiError;
use crate::models::{
    NamespaceID, NewRemoteCallResult, NewTaskEventRecord, Permissions, RemoteAuthConfig,
    RemoteHttpMethod, StoredRemoteCallTaskPayload, TaskRecord, TaskStatus, User,
    remote_target_ip_blocked, validate_rendered_remote_url,
};
use crate::traits::{ClassAccessors, NamespaceAccessors, SelfAccessors};

pub(super) async fn execute_remote_call_task(
    pool: &DbPool,
    task: &TaskRecord,
    user: &User,
) -> Result<(), ApiError> {
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

    let result = execute_remote_call(pool, task.id, user, &request).await;
    match result {
        Ok(success) => finalize_remote_task(pool, task, success).await,
        Err(error) => {
            let sanitized = crate::tasks::helpers::sanitize_error_for_storage(&error);
            let fallback = NewRemoteCallResult {
                task_id: task.id,
                target_id: Some(request.target_id.id()),
                object_id: Some(request.object_id.id()),
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

async fn execute_remote_call(
    pool: &DbPool,
    task_id: i32,
    user: &User,
    request: &StoredRemoteCallTaskPayload,
) -> Result<RemoteExecutionOutcome, ApiError> {
    let target = request.target_id.instance(pool).await?;
    if !target.enabled {
        return Err(ApiError::BadRequest(
            "Remote target is disabled".to_string(),
        ));
    }

    let object = request.object_id.instance(pool).await?;
    let class = request.class_id.class(pool).await?;
    if object.hubuum_class_id != class.id {
        return Err(ApiError::NotFound("Object not found in class".to_string()));
    }
    if target.namespace_id != object.namespace_id {
        return Err(ApiError::NotFound(
            "Remote target not found for object namespace".to_string(),
        ));
    }
    let namespace = NamespaceID::new(object.namespace_id)?
        .namespace(pool)
        .await?;

    can!(
        pool,
        user.clone(),
        [Permissions::ReadObject],
        namespace.clone()
    );
    can!(
        pool,
        user.clone(),
        [Permissions::ExecuteRemoteTarget],
        namespace.clone()
    );

    let context = serde_json::json!({
        "object": object,
        "class": class,
        "namespace": namespace,
        "parameters": request.parameters,
        "body_override": request.body_override,
    });

    let rendered_url = render_template("url_template", &target.url_template, &context)?;
    let url_parts = validate_rendered_remote_url(&rendered_url)?;
    let screened_addrs = screen_outbound_host(&url_parts.host, url_parts.port).await?;
    let rendered_headers = render_headers(&target.headers_template, &context)?;
    let rendered_body = target
        .body_template
        .as_deref()
        .map(|template| render_template("body_template", template, &context))
        .transpose()?;

    let mut headers = rendered_headers;
    apply_auth(&mut headers, &target.auth_config)?;

    let timeout_ms = bounded_timeout_ms(target.timeout_ms);
    // Never follow redirects: a 3xx could otherwise bounce the call to a non-https
    // scheme or an internal address that bypassed the initial SSRF screening.
    // Pin DNS to the addresses we already screened so the host cannot rebind to a
    // private address between our check and the connection.
    let client_builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_millis(timeout_ms))
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(&url_parts.host, &screened_addrs);
    #[cfg(test)]
    let client_builder = client_builder.danger_accept_invalid_certs(true);
    let client = client_builder
        .build()
        .map_err(|error| ApiError::InternalServerError(format!("HTTP client error: {error}")))?;

    let mut request_builder = client
        .request(reqwest_method(target.method), rendered_url.clone())
        .headers(headers);
    if let Some(body) = rendered_body {
        request_builder = request_builder.body(body);
    }

    let start = Instant::now();
    let response_result = request_builder.send().await;
    let preview_limit = get_config()
        .map(|config| config.remote_call_max_response_bytes)
        .unwrap_or(DEFAULT_REMOTE_CALL_MAX_RESPONSE_BYTES);

    match response_result {
        Ok(response) => {
            let status = response.status();
            let response_headers = headers_to_json(response.headers());
            let body_preview = read_capped_body(response, preview_limit).await?;
            let duration_ms = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);
            let success = status.is_success();
            insert_remote_call_result(
                pool,
                NewRemoteCallResult {
                    task_id,
                    target_id: Some(target.id),
                    object_id: Some(object.id),
                    method: target.method.as_str().to_string(),
                    rendered_url,
                    response_status: Some(i32::from(status.as_u16())),
                    response_headers: Some(response_headers),
                    response_body_preview: Some(body_preview),
                    duration_ms,
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
                    "status": i32::from(status.as_u16()),
                    "duration_ms": duration_ms,
                })),
            })
        }
        Err(error) => {
            let duration_ms = i32::try_from(start.elapsed().as_millis()).unwrap_or(i32::MAX);
            let message = sanitize_reqwest_error(error);
            insert_remote_call_result(
                pool,
                NewRemoteCallResult {
                    task_id,
                    target_id: Some(target.id),
                    object_id: Some(object.id),
                    method: target.method.as_str().to_string(),
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

fn render_template(
    label: &str,
    template: &str,
    context: &serde_json::Value,
) -> Result<String, ApiError> {
    let mut env = minijinja::Environment::new();
    crate::utilities::reporting::register_curated_helpers(&mut env);
    env.template_from_str(template)
        .and_then(|compiled| compiled.render(context))
        .map_err(|error| ApiError::BadRequest(format!("Failed rendering {label}: {error}")))
}

fn render_headers(
    headers_template: &serde_json::Value,
    context: &serde_json::Value,
) -> Result<HeaderMap, ApiError> {
    let mut headers = HeaderMap::new();
    let object = headers_template.as_object().ok_or_else(|| {
        ApiError::BadRequest("headers_template must be a JSON object".to_string())
    })?;
    for (name, value) in object {
        let value = value.as_str().ok_or_else(|| {
            ApiError::BadRequest("header template values must be strings".to_string())
        })?;
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| ApiError::BadRequest(format!("Invalid header name: {name}")))?;
        let rendered = render_template("header template", value, context)?;
        let value = HeaderValue::from_str(&rendered)
            .map_err(|_| ApiError::BadRequest(format!("Invalid header value for {name}")))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn apply_auth(headers: &mut HeaderMap, auth_config: &RemoteAuthConfig) -> Result<(), ApiError> {
    match auth_config {
        RemoteAuthConfig::None => Ok(()),
        RemoteAuthConfig::BearerSecret { secret } => {
            let value = format!("Bearer {}", resolve_secret(secret)?);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&value).map_err(|_| {
                    ApiError::BadRequest("Resolved bearer secret is not a valid header".to_string())
                })?,
            );
            Ok(())
        }
        RemoteAuthConfig::BasicSecret { username, secret } => {
            let raw = format!("{}:{}", username, resolve_secret(secret)?);
            let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
            headers.insert(
                reqwest::header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Basic {encoded}")).map_err(|_| {
                    ApiError::BadRequest("Resolved basic secret is not a valid header".to_string())
                })?,
            );
            Ok(())
        }
        RemoteAuthConfig::ApiKeySecret { header, secret } => {
            let name = HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
                ApiError::BadRequest(format!("Invalid API key header name: {header}"))
            })?;
            let value = resolve_secret(secret)?;
            headers.insert(
                name,
                HeaderValue::from_str(&value).map_err(|_| {
                    ApiError::BadRequest(
                        "Resolved API key secret is not a valid header".to_string(),
                    )
                })?,
            );
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

fn reqwest_method(method: RemoteHttpMethod) -> reqwest::Method {
    match method {
        RemoteHttpMethod::Get => reqwest::Method::GET,
        RemoteHttpMethod::Post => reqwest::Method::POST,
        RemoteHttpMethod::Patch => reqwest::Method::PATCH,
        RemoteHttpMethod::Delete => reqwest::Method::DELETE,
    }
}

fn bounded_timeout_ms(timeout_ms: i32) -> u64 {
    let requested = u64::try_from(timeout_ms).unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS);
    let cap = get_config()
        .map(|config| config.remote_call_timeout_ms)
        .unwrap_or(DEFAULT_REMOTE_CALL_TIMEOUT_MS);
    requested.min(cap)
}

/// Resolve `host` and reject the call if any resolved address is private/internal,
/// unless the deployment has opted in via `remote_call_allow_private_targets`.
/// Returns the screened socket addresses so the caller can pin reqwest's resolver
/// to exactly these IPs (defeating DNS rebinding).
async fn screen_outbound_host(host: &str, port: u16) -> Result<Vec<SocketAddr>, ApiError> {
    #[cfg(test)]
    if host.eq_ignore_ascii_case("localhost") {
        return Ok(vec![SocketAddr::from(([127, 0, 0, 1], port))]);
    }

    let allow_private = get_config()
        .map(|config| config.remote_call_allow_private_targets)
        .unwrap_or(DEFAULT_REMOTE_CALL_ALLOW_PRIVATE_TARGETS);

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| {
            ApiError::BadRequest(format!("Failed to resolve remote target host '{host}'"))
        })?
        .collect();

    if addrs.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Remote target host '{host}' did not resolve to any address"
        )));
    }

    if !allow_private
        && let Some(blocked) = addrs
            .iter()
            .find(|addr| remote_target_ip_blocked(addr.ip()))
    {
        return Err(ApiError::BadRequest(format!(
            "Remote target host '{host}' resolves to a disallowed address ({})",
            blocked.ip()
        )));
    }

    Ok(addrs)
}

/// Read at most `limit` bytes of the response body, stopping early so an oversized
/// or hostile response cannot exhaust worker memory.
async fn read_capped_body(
    mut response: reqwest::Response,
    limit: usize,
) -> Result<String, ApiError> {
    let mut buffer: Vec<u8> = Vec::new();
    while buffer.len() < limit {
        match response.chunk().await.map_err(|error| {
            ApiError::InternalServerError(format!("Failed reading remote response: {error}"))
        })? {
            Some(chunk) => {
                let remaining = limit - buffer.len();
                let take = remaining.min(chunk.len());
                buffer.extend_from_slice(&chunk[..take]);
            }
            None => break,
        }
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

/// Response headers that may carry secrets the remote echoes back; their values are
/// redacted before storage so they are never persisted in `remote_call_results`.
fn is_sensitive_response_header(name: &str) -> bool {
    const SENSITIVE: [&str; 6] = [
        "set-cookie",
        "set-cookie2",
        "authorization",
        "proxy-authorization",
        "www-authenticate",
        "proxy-authenticate",
    ];
    SENSITIVE.contains(&name.to_ascii_lowercase().as_str())
}

fn headers_to_json(headers: &HeaderMap) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    for (name, value) in headers {
        if is_sensitive_response_header(name.as_str()) {
            object.insert(
                name.to_string(),
                serde_json::Value::String("[redacted]".to_string()),
            );
        } else if let Ok(value) = value.to_str() {
            object.insert(
                name.to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }
    serde_json::Value::Object(object)
}

fn sanitize_reqwest_error(error: reqwest::Error) -> String {
    if error.is_timeout() {
        "Remote call timed out".to_string()
    } else if error.is_connect() {
        "Remote connection failed".to_string()
    } else {
        format!("Remote call failed: {error}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_template_supports_curated_filters() {
        // The `tojson` filter is documented for remote target body templates; it must
        // actually render, not just compile, so execution matches the docs.
        let context = serde_json::json!({ "object": { "data": { "host": "h1" } } });
        let rendered =
            render_template("body_template", "{{ object.data | tojson }}", &context).unwrap();
        assert_eq!(rendered, "{\"host\":\"h1\"}");
    }
}
