use crate::api::handlers::{auth, meta, probes};
use crate::api::v1::handlers::history::HistoryResponse;
use crate::api::v1::handlers::{
    classes, event_deliveries, event_sinks, event_subscriptions, events, groups, imports, me,
    namespaces, principals, relations, remote_targets, reports, search, service_accounts, tasks,
    templates, users,
};
use crate::events::EventResponse;
use crate::models::{
    ClassKey, EventDelivery, EventDeliveryHealthResponse, EventDeliveryQueueHealth,
    EventDeliveryStatus, EventDeliveryStatusCounts, EventDeliveryUpdateResponse, EventFanoutHealth,
    EventSink, EventSinkDeliveryHealth, EventSinkKind, EventSubscription,
    EventSubscriptionDeliveryHealth, EventWorkerHealth, EventWorkerWakeupStats, Group, GroupKey,
    GroupPermission, HubuumClass, HubuumClassExpanded, HubuumClassHistory, HubuumClassRelation,
    HubuumClassWithPath, HubuumObject, HubuumObjectHistory, HubuumObjectRelation,
    HubuumObjectWithPath, ImportAtomicity, ImportClassInput, ImportClassRelationInput,
    ImportCollisionPolicy, ImportGraph, ImportMode, ImportNamespaceInput,
    ImportNamespacePermissionInput, ImportObjectInput, ImportObjectRelationInput,
    ImportPermissionPolicy, ImportRequest, ImportTaskDetails, ImportTaskResultResponse, LoginUser,
    Namespace, NamespaceHistory, NamespaceKey, NewEventSink, NewEventSubscription, NewGroup,
    NewHubuumClass, NewHubuumClassRelation, NewHubuumClassRelationFromClass, NewHubuumObject,
    NewHubuumObjectRelation, NewNamespaceWithAssignee, NewRemoteTarget, NewReportTemplate,
    NewServiceAccount, NewUser, ObjectKey, ObjectsByClass, Permission, Permissions,
    PrincipalMemberResponse, PrincipalToken, PrincipalTokenMetadata, RelatedClassGraph,
    RelatedObjectGraph, RemoteAuthConfig, RemoteCallResult, RemoteHttpMethod,
    RemoteInvocationBodyOverride, RemoteInvocationParameters, RemoteInvocationSubject,
    RemoteTarget, RemoteTargetHistory, RemoteTargetID, RemoteTargetInvokeRequest,
    RemoteTargetSubjectType, ReportContentType, ReportJsonResponse, ReportLimits, ReportMeta,
    ReportMissingDataPolicy, ReportRequest, ReportScope, ReportScopeKind, ReportTaskDetails,
    ReportTemplate, ReportTemplateHistory, ReportTemplateID, ReportTemplateKind,
    ReportTemplateRunRequest, ReportWarning, ServiceAccountResponse, TaskDetails,
    TaskEventResponse, TaskKind, TaskLinks, TaskProgress, TaskResponse, TaskStatus,
    UnifiedSearchBatchResponse, UnifiedSearchDoneEvent, UnifiedSearchErrorEvent, UnifiedSearchKind,
    UnifiedSearchResponse, UnifiedSearchStartedEvent, UpdateEventSink, UpdateEventSubscription,
    UpdateGroup, UpdateHubuumClass, UpdateHubuumObject, UpdateNamespace, UpdateRemoteTarget,
    UpdateReportTemplate, UpdateServiceAccount, UpdateUser, UserResponse,
};
use crate::pagination::{NEXT_CURSOR_HEADER, TOTAL_COUNT_HEADER, page_limits_or_defaults};
use actix_web::{HttpResponse, Responder};
use hubuum_events_core::EventSubscriptionFilter;
use serde::Serialize;
use utoipa::openapi::OpenApi as OpenApiDoc;
use utoipa::openapi::header::Header;
use utoipa::openapi::path::{Operation, Parameter, ParameterBuilder, ParameterIn, PathItem};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::{Object, RefOr, Required, Type};
use utoipa::{Modify, OpenApi, ToSchema};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Hubuum REST API",
        version = env!("CARGO_PKG_VERSION"),
        description = "OpenAPI documentation for the Hubuum REST service."
    ),
    servers((url = "/", description = "Current deployment base URL")),
    paths(
        probes::healthz,
        probes::readyz,
        meta::get_db_state,
        meta::get_object_and_class_count,
        meta::get_task_queue_state,
        meta::get_login_rate_limit_state,
        meta::release_login_rate_limit_entry,
        meta::clear_login_rate_limit,
        auth::login,
        auth::logout,
        auth::logout_all,
        auth::logout_token,
        auth::logout_other,
        auth::validate_token,
        users::get_users,
        users::create_user,
        users::get_user,
        users::update_user,
        users::delete_user,
        users::anonymize_user,
        groups::get_groups,
        groups::create_group,
        groups::get_group,
        groups::update_group,
        groups::delete_group,
        groups::get_group_members,
        groups::add_group_member,
        groups::delete_group_member,
        service_accounts::create_service_account,
        service_accounts::list_service_accounts,
        service_accounts::get_service_account,
        service_accounts::update_service_account,
        service_accounts::disable_service_account,
        service_accounts::delete_service_account,
        principals::create_token,
        principals::list_tokens,
        principals::revoke_token,
        principals::list_principal_groups,
        principals::list_principal_permissions,
        me::get_me,
        me::list_my_tokens,
        me::list_my_groups,
        me::list_my_permissions,
        namespaces::get_namespaces,
        namespaces::create_namespace,
        namespaces::get_namespace,
        namespaces::get_namespace_history,
        namespaces::get_namespace_as_of,
        namespaces::update_namespace,
        namespaces::delete_namespace,
        namespaces::get_namespace_permissions,
        namespaces::get_namespace_group_permissions,
        namespaces::grant_namespace_group_permissions,
        namespaces::replace_namespace_group_permissions,
        namespaces::revoke_namespace_group_permissions,
        namespaces::get_namespace_group_permission,
        namespaces::grant_namespace_group_permission,
        namespaces::revoke_namespace_group_permission,
        namespaces::get_namespace_principal_permissions,
        namespaces::get_namespace_groups_with_permission,
        relations::get_class_relations,
        relations::get_class_relation,
        relations::create_class_relation,
        relations::delete_class_relation,
        relations::get_object_relations,
        relations::get_object_relation,
        relations::create_object_relation,
        relations::delete_object_relation,
        search::get_search,
        search::stream_search,
        reports::run_report,
        reports::get_report,
        reports::get_report_output,
        tasks::get_tasks,
        tasks::get_task,
        tasks::get_task_events,
        events::get_events,
        events::get_namespace_events,
        events::get_class_events,
        events::get_object_events,
        events::get_user_events,
        events::get_group_events,
        events::get_report_template_events,
        events::get_remote_target_events,
        event_sinks::create_event_sink,
        event_sinks::get_event_sinks,
        event_sinks::get_event_sink,
        event_sinks::patch_event_sink,
        event_sinks::delete_event_sink,
        event_deliveries::get_event_deliveries,
        event_deliveries::get_event_delivery_health,
        event_deliveries::get_event_delivery,
        event_deliveries::retry_event_delivery,
        event_deliveries::dead_letter_event_delivery,
        event_subscriptions::create_event_subscription,
        event_subscriptions::get_event_subscriptions,
        event_subscriptions::get_event_subscription,
        event_subscriptions::patch_event_subscription,
        event_subscriptions::delete_event_subscription,
        imports::create_import,
        imports::get_import,
        imports::get_import_results,
        templates::get_templates,
        templates::create_template,
        templates::get_template,
        templates::get_template_history,
        templates::get_template_as_of,
        templates::run_template_report,
        templates::patch_template,
        templates::delete_template,
        remote_targets::get_remote_targets,
        remote_targets::create_remote_target,
        remote_targets::get_remote_target,
        remote_targets::get_remote_target_history,
        remote_targets::get_remote_target_as_of,
        remote_targets::patch_remote_target,
        remote_targets::delete_remote_target,
        remote_targets::invoke_remote_target,
        classes::get_classes,
        classes::create_class,
        classes::get_class,
        classes::get_class_history,
        classes::get_class_as_of,
        classes::update_class,
        classes::delete_class,
        classes::get_class_permissions,
        classes::get_related_classes,
        classes::create_class_relation,
        classes::delete_class_relation,
        classes::get_related_class_relations,
        classes::get_related_class_graph,
        classes::get_objects_in_class,
        classes::create_object_in_class,
        classes::get_object_in_class,
        classes::get_object_history,
        classes::get_object_as_of,
        classes::patch_object_in_class,
        classes::delete_object_in_class,
        classes::get_related_objects,
        classes::get_related_object_relations,
        classes::get_related_object_graph,
        classes::get_object_relation_from_class_and_objects,
        classes::delete_object_relation,
        classes::create_object_relation
    ),
    components(
        schemas(
            ApiErrorResponse,
            MessageResponse,
            probes::ProbeResponse,
            LoginResponse,
            CountsResponse,
            meta::DbStateResponse,
            meta::TaskQueueStateResponse,
            meta::LoginRateLimitConfigResponse,
            meta::LoginRateLimitEntryResponse,
            meta::LoginRateLimitStateResponse,
            meta::ReleaseRateLimitResponse,
            meta::ClearRateLimitResponse,
            ObjectsByClass,
            UserResponse,
            NewUser,
            UpdateUser,
            LoginUser,
            auth::LogoutTokenRequest,
            PrincipalToken,
            PrincipalTokenMetadata,
            PrincipalMemberResponse,
            NewServiceAccount,
            ServiceAccountResponse,
            UpdateServiceAccount,
            principals::NewTokenRequest,
            principals::GroupGrant,
            principals::PrincipalNamespacePermissions,
            me::CurrentTokenMetadata,
            me::MeResponse,
            Group,
            NewGroup,
            UpdateGroup,
            Namespace,
            NewNamespaceWithAssignee,
            UpdateNamespace,
            Permissions,
            Permission,
            GroupPermission,
            HubuumClass,
            HubuumClassHistory,
            HistoryResponse<HubuumClassHistory>,
            HistoryResponse<HubuumObjectHistory>,
            HistoryResponse<NamespaceHistory>,
            HistoryResponse<ReportTemplateHistory>,
            HistoryResponse<RemoteTargetHistory>,
            HubuumClassExpanded,
            HubuumClassWithPath,
            NewHubuumClass,
            UpdateHubuumClass,
            HubuumClassRelation,
            NewHubuumClassRelation,
            NewHubuumClassRelationFromClass,
            HubuumObject,
            HubuumObjectHistory,
            NewHubuumObject,
            UpdateHubuumObject,
            HubuumObjectWithPath,
            HubuumObjectRelation,
            RelatedClassGraph,
            RelatedObjectGraph,
            NewHubuumObjectRelation,
            TaskKind,
            TaskStatus,
            TaskProgress,
            TaskLinks,
            ImportTaskDetails,
            ReportTaskDetails,
            TaskDetails,
            TaskResponse,
            TaskEventResponse,
            EventResponse,
            EventDeliveryStatus,
            EventDelivery,
            EventDeliveryUpdateResponse,
            EventDeliveryStatusCounts,
            EventWorkerWakeupStats,
            EventWorkerHealth,
            EventFanoutHealth,
            EventDeliveryQueueHealth,
            EventSinkDeliveryHealth,
            EventSubscriptionDeliveryHealth,
            EventDeliveryHealthResponse,
            EventSinkKind,
            EventSink,
            NewEventSink,
            UpdateEventSink,
            EventSubscription,
            EventSubscriptionFilter,
            NewEventSubscription,
            UpdateEventSubscription,
            ImportTaskResultResponse,
            ImportAtomicity,
            ImportCollisionPolicy,
            ImportPermissionPolicy,
            ImportMode,
            NamespaceKey,
            NamespaceHistory,
            GroupKey,
            ClassKey,
            ObjectKey,
            ImportNamespaceInput,
            ImportClassInput,
            ImportObjectInput,
            ImportClassRelationInput,
            ImportObjectRelationInput,
            ImportNamespacePermissionInput,
            ImportGraph,
            ImportRequest,
            ReportScopeKind,
            ReportScope,
            ReportContentType,
            ReportMissingDataPolicy,
            ReportLimits,
            ReportRequest,
            ReportWarning,
            ReportMeta,
            ReportJsonResponse,
            UnifiedSearchKind,
            UnifiedSearchResponse,
            UnifiedSearchBatchResponse,
            UnifiedSearchStartedEvent,
            UnifiedSearchDoneEvent,
            UnifiedSearchErrorEvent,
            ReportTemplateID,
            ReportTemplateKind,
            ReportTemplate,
            ReportTemplateHistory,
            ReportTemplateRunRequest,
            NewReportTemplate,
            UpdateReportTemplate,
            RemoteTargetID,
            RemoteHttpMethod,
            RemoteAuthConfig,
            RemoteTarget,
            RemoteTargetHistory,
            NewRemoteTarget,
            UpdateRemoteTarget,
            RemoteTargetSubjectType,
            RemoteInvocationSubject,
            RemoteInvocationParameters,
            RemoteInvocationBodyOverride,
            RemoteTargetInvokeRequest,
            RemoteCallResult
        )
    ),
    modifiers(&SecurityAddon, &OperationDefaults),
    tags(
        (name = "meta", description = "Meta and database state endpoints"),
        (name = "auth", description = "Authentication and token lifecycle"),
        (name = "users", description = "User management endpoints"),
        (name = "service-accounts", description = "Service-account (non-human principal) management endpoints"),
        (name = "principals", description = "Principal-scoped token, group, and permission endpoints (human users and service accounts)"),
        (name = "groups", description = "Group management endpoints"),
        (name = "namespaces", description = "Namespace and permission endpoints"),
        (name = "relations", description = "Class and object relation endpoints"),
        (name = "search", description = "Unified search endpoints"),
        (name = "classes", description = "Class and object-in-class endpoints"),
        (name = "tasks", description = "Generic long-running task endpoints"),
        (name = "imports", description = "Import submission and import-specific result endpoints"),
        (name = "reports", description = "Server-side report execution endpoints"),
        (name = "templates", description = "Stored report template management endpoints"),
        (name = "remote-targets", description = "Namespace-scoped remote target management and invocation endpoints")
    )
)]
pub struct ApiDoc;

pub async fn openapi_json() -> impl Responder {
    HttpResponse::Ok().json(ApiDoc::openapi())
}

#[derive(Serialize, ToSchema)]
#[schema(example = api_error_response_example)]
pub struct ApiErrorResponse {
    pub error: String,
    pub message: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = message_response_example)]
pub struct MessageResponse {
    message: String,
}

impl MessageResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Serialize, ToSchema)]
#[schema(example = login_response_example)]
pub struct LoginResponse {
    token: String,
}

impl LoginResponse {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[derive(Serialize, ToSchema)]
#[schema(example = counts_response_example)]
pub struct CountsResponse {
    pub total_objects: i64,
    pub total_classes: i64,
    pub total_namespaces: i64,
    pub objects_per_class: Vec<ObjectsByClass>,
}

#[allow(dead_code)]
fn api_error_response_example() -> ApiErrorResponse {
    ApiErrorResponse {
        error: "Unauthorized".to_string(),
        message: "Authentication failure".to_string(),
    }
}

#[allow(dead_code)]
fn message_response_example() -> MessageResponse {
    MessageResponse::new("Token is valid.")
}

#[allow(dead_code)]
fn login_response_example() -> LoginResponse {
    LoginResponse::new("eyJhbGciOi...example-token")
}

#[allow(dead_code)]
fn counts_response_example() -> CountsResponse {
    CountsResponse {
        total_objects: 42,
        total_classes: 7,
        total_namespaces: 3,
        objects_per_class: vec![ObjectsByClass {
            hubuum_class_id: 1,
            count: 6,
        }],
    }
}

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "bearer_auth",
            SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
        );
    }
}

struct OperationDefaults;

impl Modify for OperationDefaults {
    fn modify(&self, openapi: &mut OpenApiDoc) {
        for (path, path_item) in &mut openapi.paths.paths {
            for_each_operation_mut(path_item, |method, operation| {
                let operation_id = build_operation_id(method, path);
                operation.operation_id = Some(operation_id.clone());

                if operation
                    .summary
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    operation.summary = Some(title_case(&split_identifier_words(&operation_id)));
                }

                if operation
                    .description
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
                {
                    operation.description = Some(format!(
                        "Auto-generated documentation for {} {}.",
                        method.to_uppercase(),
                        path
                    ));
                }

                if is_cursor_paginated_get(path, method) {
                    add_cursor_pagination_docs(operation);
                }
            });
        }
    }
}

fn is_cursor_paginated_get(path: &str, method: &str) -> bool {
    method.eq_ignore_ascii_case("get")
        && matches!(
            path,
            "/api/v1/iam/users"
                | "/api/v1/iam/service-accounts"
                | "/api/v1/iam/me/tokens"
                | "/api/v1/iam/me/groups"
                | "/api/v1/iam/principals/{principal_id}/tokens"
                | "/api/v1/iam/principals/{principal_id}/groups"
                | "/api/v1/iam/groups"
                | "/api/v1/iam/groups/{group_id}/members"
                | "/api/v1/namespaces"
                | "/api/v1/namespaces/{namespace_id}/history"
                | "/api/v1/namespaces/{namespace_id}/permissions"
                | "/api/v1/namespaces/{namespace_id}/permissions/principal/{principal_id}"
                | "/api/v1/namespaces/{namespace_id}/has_permissions/{permission}"
                | "/api/v1/tasks"
                | "/api/v1/templates"
                | "/api/v1/templates/{template_id}/history"
                | "/api/v1/remote-targets/{remote_target_id}/history"
                | "/api/v1/relations/classes"
                | "/api/v1/relations/objects"
                | "/api/v1/classes"
                | "/api/v1/classes/{class_id}/history"
                | "/api/v1/classes/{class_id}/permissions"
                | "/api/v1/classes/{class_id}/related/classes"
                | "/api/v1/classes/{class_id}/related/relations"
                | "/api/v1/classes/{class_id}/"
                | "/api/v1/classes/{class_id}/{object_id}/history"
                | "/api/v1/classes/{class_id}/objects/{object_id}/related/objects"
                | "/api/v1/classes/{class_id}/objects/{object_id}/related/relations"
        )
}

fn add_cursor_pagination_docs(operation: &mut Operation) {
    let (default_page_limit, max_page_limit) = page_limits_or_defaults();
    let parameters = operation.parameters.get_or_insert_with(Vec::new);
    ensure_query_parameter(
        parameters,
        "limit",
        &format!(
            "Maximum number of items to return. Defaults to {default_page_limit}. Maximum is {max_page_limit}."
        ),
        Type::Integer,
    );
    ensure_query_parameter(
        parameters,
        "sort",
        "Comma-separated sort fields. Cursor pagination uses the requested sort order and appends a stable tie-breaker automatically.",
        Type::String,
    );
    ensure_query_parameter(
        parameters,
        "cursor",
        "Opaque cursor returned in the X-Next-Cursor response header from a previous page. Supply it unchanged to fetch the next page.",
        Type::String,
    );

    if let Some(description) = operation.description.as_mut() {
        let pagination_text = format!(
            " Supports cursor pagination through the `limit`, `sort`, and `cursor` query parameters. The exact total hit count is returned in the `{TOTAL_COUNT_HEADER}` response header, and the next page cursor is returned in the `{NEXT_CURSOR_HEADER}` response header."
        );
        if !description.contains(NEXT_CURSOR_HEADER) {
            description.push_str(&pagination_text);
        }
    }

    if let Some(response) = operation.responses.responses.get_mut("200") {
        add_total_count_header(response);
        add_next_cursor_header(response);
    }
}

fn ensure_query_parameter(
    parameters: &mut Vec<Parameter>,
    name: &str,
    description: &str,
    schema_type: Type,
) {
    if parameters.iter().any(|parameter| {
        parameter.name == name && matches!(parameter.parameter_in, ParameterIn::Query)
    }) {
        return;
    }

    parameters.push(
        ParameterBuilder::new()
            .name(name)
            .parameter_in(ParameterIn::Query)
            .required(Required::False)
            .description(Some(description))
            .schema(Some(Object::with_type(schema_type)))
            .build(),
    );
}

fn add_next_cursor_header(response: &mut RefOr<utoipa::openapi::response::Response>) {
    let RefOr::T(response) = response else {
        return;
    };

    response
        .headers
        .entry(NEXT_CURSOR_HEADER.to_string())
        .or_insert_with(|| {
            let mut header = Header::default();
            header.description = Some(
                "Opaque cursor for the next page. This header is omitted when there are no more results."
                    .to_string(),
            );
            header
        });

    if !response.description.contains(NEXT_CURSOR_HEADER) {
        response.description.push_str(&format!(
            " The response body contains the current page items as a JSON array. Use the `{TOTAL_COUNT_HEADER}` header for the exact number of matching results. Use the `{NEXT_CURSOR_HEADER}` header, when present, to request the next page."
        ));
    }
}

fn add_total_count_header(response: &mut RefOr<utoipa::openapi::response::Response>) {
    let RefOr::T(response) = response else {
        return;
    };

    response
        .headers
        .entry(TOTAL_COUNT_HEADER.to_string())
        .or_insert_with(|| {
            let mut header = Header::default();
            header.description = Some(
                "Exact total number of results matching the current filter set, independent of cursor pagination."
                    .to_string(),
            );
            header
        });
}

fn for_each_operation_mut(
    path_item: &mut PathItem,
    mut callback: impl FnMut(&str, &mut Operation),
) {
    if let Some(operation) = path_item.get.as_mut() {
        callback("get", operation);
    }
    if let Some(operation) = path_item.post.as_mut() {
        callback("post", operation);
    }
    if let Some(operation) = path_item.put.as_mut() {
        callback("put", operation);
    }
    if let Some(operation) = path_item.patch.as_mut() {
        callback("patch", operation);
    }
    if let Some(operation) = path_item.delete.as_mut() {
        callback("delete", operation);
    }
    if let Some(operation) = path_item.options.as_mut() {
        callback("options", operation);
    }
    if let Some(operation) = path_item.head.as_mut() {
        callback("head", operation);
    }
    if let Some(operation) = path_item.trace.as_mut() {
        callback("trace", operation);
    }
}

fn build_operation_id(method: &str, path: &str) -> String {
    let mut parts = vec![method.to_ascii_lowercase()];

    for segment in path.trim_matches('/').split('/') {
        if segment.is_empty() {
            continue;
        }

        if segment.starts_with('{') && segment.ends_with('}') {
            parts.push("by".to_string());
            parts.push(segment[1..segment.len() - 1].to_string());
        } else {
            parts.push(segment.to_string());
        }
    }

    if path.ends_with('/') && path.len() > 1 {
        parts.push("trailing".to_string());
    }

    let mut operation_id = String::new();
    for (index, part) in parts.iter().enumerate() {
        let mut normalized = split_identifier_words(part);
        if index == 0
            && let Some(first) = normalized.first_mut()
        {
            *first = first.to_ascii_lowercase();
        }
        operation_id.push_str(&camel_case(&normalized, index == 0));
    }

    operation_id
}

fn split_identifier_words(input: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();

    for ch in input.chars() {
        if !ch.is_alphanumeric() {
            if !current.is_empty() {
                words.push(current.clone());
                current.clear();
            }
            continue;
        }

        if ch.is_uppercase() && !current.is_empty() {
            words.push(current.clone());
            current.clear();
        }

        current.push(ch.to_ascii_lowercase());
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
}

fn camel_case(words: &[String], keep_first_lower: bool) -> String {
    let mut out = String::new();
    for (idx, word) in words.iter().enumerate() {
        if keep_first_lower && idx == 0 {
            out.push_str(word);
            continue;
        }
        out.push_str(&capitalize(word));
    }
    out
}

fn title_case(words: &[String]) -> String {
    words
        .iter()
        .map(|w| capitalize(w))
        .collect::<Vec<_>>()
        .join(" ")
}

fn capitalize(input: &str) -> String {
    let mut chars = input.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.as_str()),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{
        App,
        http::{Method, StatusCode},
    };
    use serde_json::Value;
    use std::collections::{BTreeSet, HashSet};

    fn openapi_json() -> Value {
        serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI should serialize to JSON")
    }

    fn path_with_sample_params(path: &str) -> String {
        let mut out = String::with_capacity(path.len());
        let mut in_param = false;

        for ch in path.chars() {
            if ch == '{' {
                in_param = true;
                out.push('1');
                continue;
            }

            if ch == '}' {
                in_param = false;
                continue;
            }

            if !in_param {
                out.push(ch);
            }
        }

        out
    }

    #[test]
    fn openapi_paths_match_mounted_routes() {
        let json = openapi_json();
        let actual_paths = json
            .get("paths")
            .and_then(Value::as_object)
            .expect("OpenAPI paths must be an object")
            .keys()
            .map(|p| p.to_string())
            .collect::<BTreeSet<_>>();

        let expected_paths = [
            "/healthz",
            "/readyz",
            "/api/v0/auth/login",
            "/api/v0/auth/logout",
            "/api/v0/auth/logout_all",
            "/api/v0/auth/logout/token",
            "/api/v0/auth/logout/uid/{user_id}",
            "/api/v0/auth/validate",
            "/api/v0/meta/db",
            "/api/v0/meta/counts",
            "/api/v0/meta/tasks",
            "/api/v0/meta/login-rate-limit",
            "/api/v0/meta/login-rate-limit/{id}",
            "/api/v1/iam/users",
            "/api/v1/iam/users/{user_id}",
            "/api/v1/iam/users/{user_id}/events",
            "/api/v1/iam/users/{user_id}/anonymize",
            "/api/v1/iam/groups",
            "/api/v1/iam/groups/{group_id}",
            "/api/v1/iam/groups/{group_id}/events",
            "/api/v1/iam/groups/{group_id}/members",
            "/api/v1/iam/groups/{group_id}/members/{principal_id}",
            "/api/v1/iam/service-accounts",
            "/api/v1/iam/service-accounts/{service_account_id}",
            "/api/v1/iam/service-accounts/{service_account_id}/disable",
            "/api/v1/iam/me",
            "/api/v1/iam/me/tokens",
            "/api/v1/iam/me/groups",
            "/api/v1/iam/me/permissions",
            "/api/v1/iam/principals/{principal_id}/tokens",
            "/api/v1/iam/principals/{principal_id}/tokens/{token_id}/revoke",
            "/api/v1/iam/principals/{principal_id}/groups",
            "/api/v1/iam/principals/{principal_id}/permissions",
            "/api/v1/namespaces",
            "/api/v1/namespaces/{namespace_id}",
            "/api/v1/namespaces/{namespace_id}/events",
            "/api/v1/namespaces/{namespace_id}/history",
            "/api/v1/namespaces/{namespace_id}/history/as-of",
            "/api/v1/namespaces/{namespace_id}/permissions",
            "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
            "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}/{permission}",
            "/api/v1/namespaces/{namespace_id}/permissions/principal/{principal_id}",
            "/api/v1/namespaces/{namespace_id}/has_permissions/{permission}",
            "/api/v1/imports",
            "/api/v1/imports/{task_id}",
            "/api/v1/imports/{task_id}/results",
            "/api/v1/reports",
            "/api/v1/reports/{task_id}",
            "/api/v1/reports/{task_id}/output",
            "/api/v1/search",
            "/api/v1/search/stream",
            "/api/v1/tasks",
            "/api/v1/tasks/{task_id}",
            "/api/v1/tasks/{task_id}/events",
            "/api/v1/events",
            "/api/v1/event-deliveries",
            "/api/v1/event-deliveries/health",
            "/api/v1/event-deliveries/{delivery_id}",
            "/api/v1/event-deliveries/{delivery_id}/retry",
            "/api/v1/event-deliveries/{delivery_id}/dead",
            "/api/v1/event-sinks",
            "/api/v1/event-sinks/{sink_id}",
            "/api/v1/namespaces/{namespace_id}/event-subscriptions",
            "/api/v1/namespaces/{namespace_id}/event-subscriptions/{subscription_id}",
            "/api/v1/templates",
            "/api/v1/templates/{template_id}",
            "/api/v1/templates/{template_id}/events",
            "/api/v1/templates/{template_id}/history",
            "/api/v1/templates/{template_id}/history/as-of",
            "/api/v1/templates/{template_id}/reports",
            "/api/v1/remote-targets",
            "/api/v1/remote-targets/{remote_target_id}/history",
            "/api/v1/remote-targets/{remote_target_id}/history/as-of",
            "/api/v1/remote-targets/{target_id}",
            "/api/v1/remote-targets/{target_id}/events",
            "/api/v1/remote-targets/{target_id}/invoke",
            "/api/v1/relations/classes",
            "/api/v1/relations/classes/{relation_id}",
            "/api/v1/relations/objects",
            "/api/v1/relations/objects/{relation_id}",
            "/api/v1/classes",
            "/api/v1/classes/{class_id}",
            "/api/v1/classes/{class_id}/events",
            "/api/v1/classes/{class_id}/history",
            "/api/v1/classes/{class_id}/history/as-of",
            "/api/v1/classes/{class_id}/permissions",
            "/api/v1/classes/{class_id}/related/classes",
            "/api/v1/classes/{class_id}/related/relations",
            "/api/v1/classes/{class_id}/related/graph",
            "/api/v1/classes/{class_id}/relations",
            "/api/v1/classes/{class_id}/relations/{relation_id}",
            "/api/v1/classes/{class_id}/",
            "/api/v1/classes/{class_id}/{object_id}",
            "/api/v1/classes/{class_id}/{object_id}/events",
            "/api/v1/classes/{class_id}/{object_id}/history",
            "/api/v1/classes/{class_id}/{object_id}/history/as-of",
            "/api/v1/classes/{class_id}/objects/{object_id}/related/objects",
            "/api/v1/classes/{class_id}/objects/{object_id}/related/relations",
            "/api/v1/classes/{class_id}/objects/{object_id}/related/graph",
            "/api/v1/classes/{class_id}/{from_object_id}/relations/{to_class_id}/{to_object_id}",
        ]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<_>>();

        assert_eq!(actual_paths, expected_paths);
    }

    #[test]
    fn openapi_contains_expected_operations_and_security_scheme() {
        let json = openapi_json();

        assert!(json.pointer("/paths/~1api~1v1~1iam~1users/get").is_some());
        assert!(json.pointer("/paths/~1api~1v1~1iam~1users/post").is_some());
        assert!(
            json.pointer("/paths/~1api~1v0~1auth~1login/post/responses/429")
                .is_some()
        );
        assert!(json.pointer("/paths/~1api~1v1~1reports/post").is_some());
        assert!(
            json.pointer("/paths/~1api~1v1~1reports~1{task_id}/get")
                .is_some()
        );
        assert!(
            json.pointer("/paths/~1api~1v1~1reports~1{task_id}~1output/get")
                .is_some()
        );
        assert!(json.pointer("/paths/~1api~1v1~1templates/get").is_some());
        assert!(
            json.pointer("/paths/~1api~1v1~1templates~1{template_id}~1reports/post")
                .is_some()
        );
        assert!(
            json.pointer("/paths/~1api~1v1~1relations~1objects/post")
                .is_some()
        );
        assert!(
            json.pointer("/components/securitySchemes/bearer_auth/type")
                .and_then(Value::as_str)
                == Some("http")
        );
        assert!(
            json.pointer("/components/securitySchemes/bearer_auth/scheme")
                .and_then(Value::as_str)
                == Some("bearer")
        );
    }

    #[test]
    fn openapi_documents_cursor_pagination_for_list_endpoints() {
        let json = openapi_json();

        let parameters = json
            .pointer("/paths/~1api~1v1~1classes/get/parameters")
            .and_then(Value::as_array)
            .expect("classes list parameters must be present");

        let parameter_names = parameters
            .iter()
            .filter_map(|parameter| parameter.get("name").and_then(Value::as_str))
            .collect::<HashSet<_>>();

        assert!(parameter_names.contains("limit"));
        assert!(parameter_names.contains("sort"));
        assert!(parameter_names.contains("cursor"));

        let header_description = json
            .pointer(
                "/paths/~1api~1v1~1classes/get/responses/200/headers/X-Next-Cursor/description",
            )
            .and_then(Value::as_str);
        let total_count_description = json
            .pointer(
                "/paths/~1api~1v1~1classes/get/responses/200/headers/X-Total-Count/description",
            )
            .and_then(Value::as_str);

        assert!(
            header_description.is_some(),
            "X-Next-Cursor header must be documented for paginated list responses"
        );
        assert!(
            total_count_description.is_some(),
            "X-Total-Count header must be documented for paginated list responses"
        );
    }

    #[test]
    fn openapi_operations_have_metadata_unique_operation_ids_and_security() {
        let json = openapi_json();
        let paths = json
            .get("paths")
            .and_then(Value::as_object)
            .expect("OpenAPI paths must be an object");

        let operation_keys = [
            "get", "post", "put", "patch", "delete", "options", "head", "trace",
        ];
        let mut operation_ids = HashSet::new();

        for (path, path_item) in paths {
            let path_item = path_item.as_object().expect("Path item must be an object");

            for method in operation_keys {
                let Some(operation) = path_item.get(method) else {
                    continue;
                };
                let operation = operation.as_object().expect("Operation must be an object");

                let operation_id = operation
                    .get("operationId")
                    .and_then(Value::as_str)
                    .expect("operationId must be present");
                assert!(
                    !operation_id.trim().is_empty(),
                    "operationId is empty for {method} {path}"
                );
                assert!(
                    operation_ids.insert(operation_id.to_string()),
                    "Duplicate operationId found: {operation_id}"
                );

                let summary = operation
                    .get("summary")
                    .and_then(Value::as_str)
                    .expect("summary must be present");
                assert!(
                    !summary.trim().is_empty(),
                    "summary is empty for {method} {path}"
                );

                let description = operation
                    .get("description")
                    .and_then(Value::as_str)
                    .expect("description must be present");
                assert!(
                    !description.trim().is_empty(),
                    "description is empty for {method} {path}"
                );

                let is_public_probe =
                    matches!(path.as_str(), "/healthz" | "/readyz") && method == "get";
                let is_login = path == "/api/v0/auth/login" && method == "post";
                if !is_login && !is_public_probe {
                    let security = operation
                        .get("security")
                        .and_then(Value::as_array)
                        .expect("security must be present for authenticated endpoint");

                    let has_bearer = security.iter().any(|entry| {
                        entry
                            .as_object()
                            .map(|obj| obj.contains_key("bearer_auth"))
                            .unwrap_or(false)
                    });
                    assert!(
                        has_bearer,
                        "missing bearer_auth security for {method} {path}"
                    );
                }
            }
        }
    }

    #[actix_web::test]
    async fn openapi_operations_resolve_to_mounted_routes() {
        let json = openapi_json();
        let paths = json
            .get("paths")
            .and_then(Value::as_object)
            .expect("OpenAPI paths must be an object");
        let operation_keys = [
            "get", "post", "put", "patch", "delete", "options", "head", "trace",
        ];

        let app = actix_web::test::init_service(App::new().configure(crate::api::config)).await;

        for (path, path_item) in paths {
            let path_item = path_item.as_object().expect("Path item must be an object");
            let route_uri = path_with_sample_params(path);

            for method in operation_keys {
                if path_item.get(method).is_none() {
                    continue;
                }

                let http_method = match method {
                    "get" => Method::GET,
                    "post" => Method::POST,
                    "put" => Method::PUT,
                    "patch" => Method::PATCH,
                    "delete" => Method::DELETE,
                    "options" => Method::OPTIONS,
                    "head" => Method::HEAD,
                    "trace" => Method::TRACE,
                    _ => unreachable!("operation key list only contains known HTTP methods"),
                };
                let req = actix_web::test::TestRequest::default()
                    .method(http_method)
                    .uri(&route_uri)
                    .to_request();
                let res = actix_web::test::call_service(&app, req).await;

                assert_ne!(
                    res.status(),
                    StatusCode::NOT_FOUND,
                    "Documented OpenAPI operation is not mounted: {method} {path} (sample: {route_uri})"
                );
            }
        }
    }
}
