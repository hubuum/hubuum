use std::collections::HashMap;
use std::time::Instant;

use actix_web::{
    http::{header, StatusCode},
    post, web, HttpRequest, HttpResponse, Responder,
};
use serde::Serialize;
use serde_json::json;
use tracing::{debug, info, warn};

use crate::api::openapi::ApiErrorResponse;
use crate::can;
use crate::db::traits::UserPermissions;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::models::search::{parse_query_parameter, FilterField, ParsedQueryParam, QueryOptions};
use crate::models::{
    HubuumClassID, HubuumObjectID, NamespaceID, Permissions, ReportContentType, ReportJsonResponse,
    ReportMeta, ReportMissingDataPolicy, ReportRequest, ReportScope, ReportScopeKind,
    ReportTemplate, ReportTemplateID, ReportWarning,
};
use crate::pagination::page_limits_or_defaults;
use crate::traits::{NamespaceAccessors, Search, SelfAccessors};
use crate::utilities::reporting::render_template;
use crate::utilities::response::json_response_with_header;

use super::check_if_object_in_class;

const DEFAULT_MAX_OUTPUT_BYTES: usize = 262_144;
const REPORT_WARNINGS_HEADER: &str = "X-Hubuum-Report-Warnings";
const REPORT_TRUNCATED_HEADER: &str = "X-Hubuum-Report-Truncated";

struct ReportRuntime {
    report: ReportRequest,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    template: Option<ReportTemplate>,
}

struct ReportExecution {
    items: Vec<serde_json::Value>,
    warnings: Vec<ReportWarning>,
    meta: ReportMeta,
}

#[utoipa::path(
    post,
    path = "/api/v1/reports",
    tag = "reports",
    security(("bearer_auth" = [])),
    request_body = ReportRequest,
    responses(
        (
            status = 200,
            description = "Rendered report output. JSON is returned by default. Text outputs require `output.template_id`.",
            content(
                (ReportJsonResponse = "application/json"),
                (String = "text/plain"),
                (String = "text/html"),
                (String = "text/csv")
            )
        ),
        (status = 400, description = "Bad request", body = ApiErrorResponse),
        (status = 401, description = "Unauthorized", body = ApiErrorResponse),
        (status = 406, description = "Not acceptable", body = ApiErrorResponse),
        (status = 413, description = "Rendered report exceeded max_output_bytes", body = ApiErrorResponse)
    )
)]
#[post("")]
pub async fn run_report(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
    report: web::Json<ReportRequest>,
) -> Result<impl Responder, ApiError> {
    let total_start = Instant::now();
    let report = report.into_inner();
    let user = requestor.user;

    debug!(
        message = "Running report",
        user_id = user.id,
        scope = report.scope.kind.as_str(),
        query = report.query
    );

    let runtime = prepare_report_runtime(&pool, &req, &user, report).await?;
    let scope = runtime.report.scope.kind;
    let content_type = runtime.content_type;

    let execution_start = Instant::now();
    let execution = match build_report_execution(&pool, &user, &runtime).await {
        Ok(execution) => execution,
        Err(error) => {
            warn!(
                message = "Report execution failed",
                user_id = user.id,
                scope = scope.as_str(),
                content_type = content_type.as_mime(),
                execution_time = ?execution_start.elapsed(),
                total_time = ?total_start.elapsed(),
                error = error.to_string()
            );
            return Err(error);
        }
    };
    let execution_time = execution_start.elapsed();
    let item_count = execution.items.len();
    let warning_count = execution.warnings.len();
    let truncated = execution.meta.truncated;

    let render_start = Instant::now();
    let response = render_report_response(runtime, execution);
    let render_time = render_start.elapsed();
    let total_time = total_start.elapsed();

    match &response {
        Ok(_) => info!(
            message = "Report completed",
            user_id = user.id,
            scope = scope.as_str(),
            content_type = content_type.as_mime(),
            item_count = item_count,
            warning_count = warning_count,
            truncated = truncated,
            execution_time = ?execution_time,
            render_time = ?render_time,
            total_time = ?total_time
        ),
        Err(error) => warn!(
            message = "Report rendering failed",
            user_id = user.id,
            scope = scope.as_str(),
            content_type = content_type.as_mime(),
            item_count = item_count,
            warning_count = warning_count,
            truncated = truncated,
            execution_time = ?execution_time,
            render_time = ?render_time,
            total_time = ?total_time,
            error = error.to_string()
        ),
    }

    response
}

async fn prepare_report_runtime(
    pool: &DbPool,
    req: &HttpRequest,
    user: &crate::models::User,
    report: ReportRequest,
) -> Result<ReportRuntime, ApiError> {
    report.scope.validate()?;

    let template = resolve_template(pool, user, &report).await?;
    let content_type = resolve_content_type(req, template.as_ref())?;

    if template.is_none() && content_type != ReportContentType::ApplicationJson {
        return Err(ApiError::BadRequest(format!(
            "Output type '{}' requires output.template_id",
            content_type.as_mime()
        )));
    }

    Ok(ReportRuntime {
        content_type,
        missing_data_policy: report
            .missing_data_policy
            .unwrap_or(ReportMissingDataPolicy::Strict),
        template,
        report,
    })
}

async fn resolve_template(
    pool: &DbPool,
    user: &crate::models::User,
    report: &ReportRequest,
) -> Result<Option<ReportTemplate>, ApiError> {
    let Some(template_id) = report.output.as_ref().and_then(|output| output.template_id) else {
        return Ok(None);
    };

    let template = ReportTemplateID(template_id).instance(pool).await?;
    can!(
        pool,
        user.clone(),
        [Permissions::ReadTemplate],
        NamespaceID(template.namespace_id)
    );

    Ok(Some(template))
}

async fn build_report_execution(
    pool: &DbPool,
    user: &crate::models::User,
    runtime: &ReportRuntime,
) -> Result<ReportExecution, ApiError> {
    let query_options = prepare_query_options(&runtime.report)?;
    let (items, mut warnings, truncated) =
        execute_scope(pool, user, &runtime.report.scope, query_options).await?;

    add_truncation_warning(&mut warnings, truncated);

    Ok(ReportExecution {
        meta: ReportMeta {
            count: items.len(),
            truncated,
            scope: runtime.report.scope.clone(),
            content_type: runtime.content_type,
        },
        items,
        warnings,
    })
}

fn add_truncation_warning(warnings: &mut Vec<ReportWarning>, truncated: bool) {
    if truncated {
        warnings.push(ReportWarning {
            code: "truncated".to_string(),
            message: "The report was truncated to the configured max_items limit".to_string(),
            path: None,
        });
    }
}

fn render_report_response(
    runtime: ReportRuntime,
    execution: ReportExecution,
) -> Result<HttpResponse, ApiError> {
    match runtime.content_type {
        ReportContentType::ApplicationJson => render_json_report(runtime.report, execution),
        ReportContentType::TextPlain | ReportContentType::TextHtml | ReportContentType::TextCsv => {
            render_text_report(runtime, execution)
        }
    }
}

fn render_json_report(
    report: ReportRequest,
    execution: ReportExecution,
) -> Result<HttpResponse, ApiError> {
    ensure_json_output_has_no_template_id(&report)?;
    let warning_count = warning_count(&execution);
    let truncated = execution.meta.truncated;

    let response = ReportJsonResponse {
        items: execution.items,
        meta: execution.meta,
        warnings: execution.warnings,
    };

    enforce_json_output_limit(&response, &report)?;

    Ok(json_response_with_header(
        response,
        StatusCode::OK,
        Some(report_headers(warning_count, truncated)),
    ))
}

fn render_text_report(
    runtime: ReportRuntime,
    execution: ReportExecution,
) -> Result<HttpResponse, ApiError> {
    let template = required_template(&runtime, runtime.content_type)?;
    let context = report_template_context(&runtime.report, &execution);
    let (rendered, template_warnings) = render_template(
        template.template.as_str(),
        &context,
        runtime.content_type,
        runtime.missing_data_policy,
    )?;
    let warning_count = warning_count(&execution) + template_warnings.len();

    enforce_text_output_limit(&rendered, &runtime.report)?;

    let mut response = HttpResponse::build(StatusCode::OK);
    response.content_type(runtime.content_type.as_mime());
    for (key, value) in report_headers(warning_count, execution.meta.truncated) {
        response.insert_header((key, value));
    }

    Ok(response.body(rendered))
}

fn ensure_json_output_has_no_template_id(report: &ReportRequest) -> Result<(), ApiError> {
    if report
        .output
        .as_ref()
        .and_then(|output| output.template_id)
        .is_some()
    {
        return Err(ApiError::BadRequest(
            "Template references are only supported for text/plain, text/html, and text/csv output"
                .to_string(),
        ));
    }

    Ok(())
}

fn required_template(
    runtime: &ReportRuntime,
    content_type: ReportContentType,
) -> Result<&ReportTemplate, ApiError> {
    runtime.template.as_ref().ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Output type '{}' requires output.template_id",
            content_type.as_mime()
        ))
    })
}

fn report_template_context(
    report: &ReportRequest,
    execution: &ReportExecution,
) -> serde_json::Value {
    json!({
        "items": &execution.items,
        "meta": &execution.meta,
        "warnings": &execution.warnings,
        "request": report,
    })
}

fn warning_count(execution: &ReportExecution) -> usize {
    execution.warnings.len()
}

fn resolve_content_type(
    req: &HttpRequest,
    template: Option<&ReportTemplate>,
) -> Result<ReportContentType, ApiError> {
    if let Some(template) = template {
        enforce_accept_matches_template(req, template.content_type)?;
        return Ok(template.content_type);
    }

    let accept = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase());

    let Some(accept) = accept else {
        return Ok(ReportContentType::ApplicationJson);
    };

    for content_type in [
        ReportContentType::ApplicationJson,
        ReportContentType::TextPlain,
        ReportContentType::TextHtml,
        ReportContentType::TextCsv,
    ] {
        if accept_allows_content_type(&accept, content_type) {
            return Ok(content_type);
        }
    }

    Err(ApiError::NotAcceptable(
        "No supported report representation matched the Accept header".to_string(),
    ))
}

fn enforce_accept_matches_template(
    req: &HttpRequest,
    template_content_type: ReportContentType,
) -> Result<(), ApiError> {
    let accept = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase());

    if let Some(accept) = accept {
        if !accept_allows_content_type(&accept, template_content_type) {
            return Err(ApiError::NotAcceptable(format!(
                "Accept header does not allow template output type '{}'",
                template_content_type.as_mime()
            )));
        }
    }

    Ok(())
}

fn accept_allows_content_type(accept: &str, content_type: ReportContentType) -> bool {
    if accept.contains("*/*") {
        return true;
    }

    match content_type {
        ReportContentType::ApplicationJson => {
            accept.contains("application/json") || accept.contains("application/*")
        }
        ReportContentType::TextPlain | ReportContentType::TextHtml | ReportContentType::TextCsv => {
            accept.contains(content_type.as_mime()) || accept.contains("text/*")
        }
    }
}

fn prepare_query_options(report: &ReportRequest) -> Result<QueryOptions, ApiError> {
    let mut query_options = parse_query_parameter(report.query.as_deref().unwrap_or_default())?;
    if query_options.cursor.is_some() {
        return Err(ApiError::BadRequest(
            "Reports do not support cursor pagination".to_string(),
        ));
    }

    let (default_page_limit, max_page_limit) = page_limits_or_defaults();
    let configured_limit = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_items)
        .unwrap_or(default_page_limit);
    let requested_limit = query_options.limit.unwrap_or(configured_limit);
    let effective_limit = requested_limit.min(configured_limit).min(max_page_limit);

    query_options.limit = Some(effective_limit.saturating_add(1));
    Ok(query_options)
}

async fn execute_scope(
    pool: &DbPool,
    user: &crate::models::User,
    scope: &ReportScope,
    mut query_options: QueryOptions,
) -> Result<(Vec<serde_json::Value>, Vec<ReportWarning>, bool), ApiError> {
    let item_limit = query_options.limit.unwrap_or(1).saturating_sub(1).max(1);

    let data = match scope.kind {
        ReportScopeKind::Namespaces => {
            to_json_items(user.search_namespaces(pool, query_options).await?)?
        }
        ReportScopeKind::Classes => to_json_items(user.search_classes(pool, query_options).await?)?,
        ReportScopeKind::ObjectsInClass => {
            push_exact_filter(
                &mut query_options,
                FilterField::ClassId,
                scope.class_id_required()?,
            )?;
            to_json_items(user.search_objects(pool, query_options).await?)?
        }
        ReportScopeKind::ClassRelations => {
            to_json_items(user.search_class_relations(pool, query_options).await?)?
        }
        ReportScopeKind::ObjectRelations => {
            to_json_items(user.search_object_relations(pool, query_options).await?)?
        }
        ReportScopeKind::RelatedObjects => {
            let class_id = HubuumClassID(scope.class_id_required()?);
            let object_id = HubuumObjectID(scope.object_id_required()?);
            check_if_object_in_class(pool, &class_id, &object_id).await?;
            let related = user
                .search_objects_related_to(pool, object_id, query_options)
                .await?;
            to_json_items(
                related
                    .into_iter()
                    .map(|relation| relation.to_descendant_object_with_path())
                    .collect::<Vec<_>>(),
            )?
        }
    };

    let (items, truncated) = truncate_items(data, item_limit);
    Ok((items, Vec::new(), truncated))
}

fn push_exact_filter(
    query_options: &mut QueryOptions,
    field: FilterField,
    value: i32,
) -> Result<(), ApiError> {
    if query_options.filters.iter().any(|param| {
        param.field == field
            && matches!(
                param.operator,
                crate::models::search::SearchOperator::Equals { is_negated: false }
            )
            && param.value == value.to_string()
    }) {
        return Ok(());
    }

    query_options.filters.push(ParsedQueryParam::new(
        &field.to_string(),
        None,
        &value.to_string(),
    )?);
    Ok(())
}

fn to_json_items<T: Serialize>(items: Vec<T>) -> Result<Vec<serde_json::Value>, ApiError> {
    items
        .into_iter()
        .map(|item| serde_json::to_value(item).map_err(ApiError::from))
        .collect()
}

fn truncate_items(
    mut items: Vec<serde_json::Value>,
    limit: usize,
) -> (Vec<serde_json::Value>, bool) {
    if items.len() > limit {
        items.truncate(limit);
        (items, true)
    } else {
        (items, false)
    }
}

fn report_headers(warning_count: usize, truncated: bool) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        REPORT_WARNINGS_HEADER.to_string(),
        warning_count.to_string(),
    );
    headers.insert(REPORT_TRUNCATED_HEADER.to_string(), truncated.to_string());
    headers
}

fn enforce_json_output_limit(
    response: &ReportJsonResponse,
    report: &ReportRequest,
) -> Result<(), ApiError> {
    let bytes = serde_json::to_vec(response).map_err(|error| {
        ApiError::InternalServerError(format!("Failed to serialize report: {error}"))
    })?;
    let max_output_bytes = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);

    if bytes.len() > max_output_bytes {
        return Err(ApiError::PayloadTooLarge(format!(
            "Rendered report exceeded max_output_bytes ({} > {})",
            bytes.len(),
            max_output_bytes
        )));
    }

    Ok(())
}

fn enforce_text_output_limit(rendered: &str, report: &ReportRequest) -> Result<(), ApiError> {
    let max_output_bytes = report
        .limits
        .as_ref()
        .and_then(|limits| limits.max_output_bytes)
        .unwrap_or(DEFAULT_MAX_OUTPUT_BYTES);

    if rendered.len() > max_output_bytes {
        return Err(ApiError::PayloadTooLarge(format!(
            "Rendered report exceeded max_output_bytes ({} > {})",
            rendered.len(),
            max_output_bytes
        )));
    }

    Ok(())
}
