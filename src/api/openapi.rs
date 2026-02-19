use crate::api::handlers::{auth, meta};
use crate::api::v1::handlers::{classes, groups, namespaces, relations, users};
use crate::models::{
    Group, GroupPermission, HubuumClass, HubuumClassExpanded, HubuumClassRelation,
    HubuumClassRelationTransitive, HubuumObject, HubuumObjectRelation, HubuumObjectWithPath,
    LoginUser, Namespace, NewGroup, NewHubuumClass, NewHubuumClassRelation,
    NewHubuumClassRelationFromClass, NewHubuumObject, NewHubuumObjectRelation,
    NewNamespaceWithAssignee, NewUser, ObjectsByClass, Permission, Permissions, UpdateGroup,
    UpdateHubuumClass, UpdateHubuumObject, UpdateNamespace, UpdateUser, User, UserToken,
};
use actix_web::{HttpResponse, Responder};
use serde::Serialize;
use utoipa::openapi::path::{Operation, PathItem};
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::openapi::OpenApi as OpenApiDoc;
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
        meta::get_db_state,
        meta::get_object_and_class_count,
        auth::login,
        auth::logout,
        auth::logout_all,
        auth::logout_token,
        auth::logout_other,
        auth::validate_token,
        users::get_users,
        users::create_user,
        users::get_user,
        users::get_user_tokens,
        users::get_user_groups,
        users::update_user,
        users::delete_user,
        groups::get_groups,
        groups::create_group,
        groups::get_group,
        groups::update_group,
        groups::delete_group,
        groups::get_group_members,
        groups::add_group_member,
        groups::delete_group_member,
        namespaces::get_namespaces,
        namespaces::create_namespace,
        namespaces::get_namespace,
        namespaces::update_namespace,
        namespaces::delete_namespace,
        namespaces::get_namespace_permissions,
        namespaces::get_namespace_group_permissions,
        namespaces::grant_namespace_group_permissions,
        namespaces::revoke_namespace_group_permissions,
        namespaces::get_namespace_group_permission,
        namespaces::grant_namespace_group_permission,
        namespaces::revoke_namespace_group_permission,
        namespaces::get_namespace_user_permissions,
        namespaces::get_namespace_groups_with_permission,
        relations::get_class_relations,
        relations::get_class_relation,
        relations::create_class_relation,
        relations::delete_class_relation,
        relations::get_object_relations,
        relations::get_object_relation,
        relations::create_object_relation,
        relations::delete_object_relation,
        classes::get_classes,
        classes::create_class,
        classes::get_class,
        classes::update_class,
        classes::delete_class,
        classes::get_class_permissions,
        classes::get_class_relations,
        classes::create_class_relation,
        classes::delete_class_relation,
        classes::get_class_relations_transitive,
        classes::get_class_relations_transitive_to_class,
        classes::get_objects_in_class,
        classes::create_object_in_class,
        classes::get_object_in_class,
        classes::patch_object_in_class,
        classes::delete_object_in_class,
        classes::list_related_objects,
        classes::get_object_relation_from_class_and_objects,
        classes::delete_object_relation,
        classes::create_object_relation
    ),
    components(
        schemas(
            ApiErrorResponse,
            MessageResponse,
            LoginResponse,
            CountsResponse,
            meta::DbStateResponse,
            ObjectsByClass,
            User,
            NewUser,
            UpdateUser,
            LoginUser,
            UserToken,
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
            HubuumClassExpanded,
            NewHubuumClass,
            UpdateHubuumClass,
            HubuumClassRelation,
            HubuumClassRelationTransitive,
            NewHubuumClassRelation,
            NewHubuumClassRelationFromClass,
            HubuumObject,
            NewHubuumObject,
            UpdateHubuumObject,
            HubuumObjectWithPath,
            HubuumObjectRelation,
            NewHubuumObjectRelation
        )
    ),
    modifiers(&SecurityAddon, &OperationDefaults),
    tags(
        (name = "meta", description = "Meta and database state endpoints"),
        (name = "auth", description = "Authentication and token lifecycle"),
        (name = "users", description = "User management endpoints"),
        (name = "groups", description = "Group management endpoints"),
        (name = "namespaces", description = "Namespace and permission endpoints"),
        (name = "relations", description = "Class and object relation endpoints"),
        (name = "classes", description = "Class and object-in-class endpoints")
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
    pub message: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = login_response_example)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Serialize, ToSchema)]
#[schema(example = counts_response_example)]
pub struct CountsResponse {
    pub total_objects: i64,
    pub total_classes: i64,
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
    MessageResponse {
        message: "Token is valid.".to_string(),
    }
}

#[allow(dead_code)]
fn login_response_example() -> LoginResponse {
    LoginResponse {
        token: "eyJhbGciOi...example-token".to_string(),
    }
}

#[allow(dead_code)]
fn counts_response_example() -> CountsResponse {
    CountsResponse {
        total_objects: 42,
        total_classes: 7,
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
            });
        }
    }
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
        if index == 0 {
            if let Some(first) = normalized.first_mut() {
                *first = first.to_ascii_lowercase();
            }
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
    use serde_json::Value;
    use std::collections::{BTreeSet, HashSet};

    fn openapi_json() -> Value {
        serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI should serialize to JSON")
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
            "/api/v0/auth/login",
            "/api/v0/auth/logout",
            "/api/v0/auth/logout_all",
            "/api/v0/auth/logout/token/{token}",
            "/api/v0/auth/logout/uid/{user_id}",
            "/api/v0/auth/validate",
            "/api/v0/meta/db",
            "/api/v0/meta/counts",
            "/api/v1/iam/users",
            "/api/v1/iam/users/{user_id}",
            "/api/v1/iam/users/{user_id}/tokens",
            "/api/v1/iam/users/{user_id}/groups",
            "/api/v1/iam/groups",
            "/api/v1/iam/groups/{group_id}",
            "/api/v1/iam/groups/{group_id}/members",
            "/api/v1/iam/groups/{group_id}/members/{user_id}",
            "/api/v1/namespaces",
            "/api/v1/namespaces/{namespace_id}",
            "/api/v1/namespaces/{namespace_id}/permissions",
            "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}",
            "/api/v1/namespaces/{namespace_id}/permissions/group/{group_id}/{permission}",
            "/api/v1/namespaces/{namespace_id}/permissions/user/{user_id}",
            "/api/v1/namespaces/{namespace_id}/has_permissions/{permission}",
            "/api/v1/relations/classes",
            "/api/v1/relations/classes/{relation_id}",
            "/api/v1/relations/objects",
            "/api/v1/relations/objects/{relation_id}",
            "/api/v1/classes",
            "/api/v1/classes/{class_id}",
            "/api/v1/classes/{class_id}/permissions",
            "/api/v1/classes/{class_id}/relations",
            "/api/v1/classes/{class_id}/relations/{relation_id}",
            "/api/v1/classes/{class_id}/relations/transitive",
            "/api/v1/classes/{class_id}/relations/transitive/class/{class_id_to}",
            "/api/v1/classes/{class_id}/",
            "/api/v1/classes/{class_id}/{object_id}",
            "/api/v1/classes/{class_id}/{from_object_id}/relations",
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
        assert!(json
            .pointer("/paths/~1api~1v1~1relations~1objects/post")
            .is_some());
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
    fn openapi_operations_have_metadata_unique_operation_ids_and_security() {
        let json = openapi_json();
        let paths = json
            .get("paths")
            .and_then(Value::as_object)
            .expect("OpenAPI paths must be an object");

        let operation_keys = ["get", "post", "put", "patch", "delete", "options", "head", "trace"];
        let mut operation_ids = HashSet::new();

        for (path, path_item) in paths {
            let path_item = path_item
                .as_object()
                .expect("Path item must be an object");

            for method in operation_keys {
                let Some(operation) = path_item.get(method) else {
                    continue;
                };
                let operation = operation
                    .as_object()
                    .expect("Operation must be an object");

                let operation_id = operation
                    .get("operationId")
                    .and_then(Value::as_str)
                    .expect("operationId must be present");
                assert!(!operation_id.trim().is_empty(), "operationId is empty for {method} {path}");
                assert!(
                    operation_ids.insert(operation_id.to_string()),
                    "Duplicate operationId found: {operation_id}"
                );

                let summary = operation
                    .get("summary")
                    .and_then(Value::as_str)
                    .expect("summary must be present");
                assert!(!summary.trim().is_empty(), "summary is empty for {method} {path}");

                let description = operation
                    .get("description")
                    .and_then(Value::as_str)
                    .expect("description must be present");
                assert!(
                    !description.trim().is_empty(),
                    "description is empty for {method} {path}"
                );

                let is_login = path == "/api/v0/auth/login" && method == "post";
                if !is_login {
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
                    assert!(has_bearer, "missing bearer_auth security for {method} {path}");
                }
            }
        }
    }
}
