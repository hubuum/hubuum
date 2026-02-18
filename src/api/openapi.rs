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
use serde::Serialize;
use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
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
    modifiers(&SecurityAddon),
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

#[derive(Serialize, ToSchema)]
pub struct ApiErrorResponse {
    pub error: String,
    pub message: String,
}

#[derive(Serialize, ToSchema)]
pub struct MessageResponse {
    pub message: String,
}

#[derive(Serialize, ToSchema)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Serialize, ToSchema)]
pub struct CountsResponse {
    pub total_objects: i64,
    pub total_classes: i64,
    pub objects_per_class: Vec<ObjectsByClass>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn openapi_json() -> Value {
        serde_json::to_value(ApiDoc::openapi()).expect("OpenAPI should serialize to JSON")
    }

    #[test]
    fn openapi_includes_expected_paths_across_modules() {
        let json = openapi_json();
        let paths = json
            .get("paths")
            .and_then(Value::as_object)
            .expect("OpenAPI paths must be an object");

        let expected = [
            "/api/v0/auth/login",
            "/api/v0/meta/db",
            "/api/v1/iam/users",
            "/api/v1/iam/groups",
            "/api/v1/namespaces",
            "/api/v1/relations/classes",
            "/api/v1/classes",
            "/api/v1/classes/{class_id}/{from_object_id}/relations",
        ];

        for path in expected {
            assert!(paths.contains_key(path), "missing path: {path}");
        }
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
}
