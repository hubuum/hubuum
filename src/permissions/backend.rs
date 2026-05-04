use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::{
    GroupPermission, Namespace, Permission, Permissions, PermissionsList,
};
use crate::models::search::QueryOptions;

use super::types::{
    AuthorizedRequest, PermissionDecision, PermissionRequest, PrincipalRef,
};

#[async_trait]
pub trait PermissionBackend: Send + Sync {
    /// Single point check: does the principal satisfy all `request.permissions`
    /// on `request.resource`?
    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError>;

    /// Batch point checks. Implementations should batch transport-side when
    /// possible (e.g. Treetop's `AuthorizeRequest`). Order of the returned
    /// vector matches the order of `requests`.
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError>;

    /// Filter a candidate set: returns only the requests that resolve to
    /// `Allow`, in input order. Used by list/search visibility paths.
    async fn filter_authorized(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizedRequest>, ApiError>;

    /// All namespaces on which the principal has the given permission.
    /// Used by listing endpoints that want to scope their candidate query
    /// (e.g. `GET /templates`).
    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permission: Permissions,
    ) -> Result<Vec<Namespace>, ApiError>;

    /// (group, permission) pairs visible on a namespace, paginated.
    /// Returns `(rows, total_count)` so handlers can populate `X-Total-Count`.
    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError>;

    /// Single group's permissions on a namespace, or `None` if no row.
    /// In Treetop mode `id` / `created_at` / `updated_at` are synthetic.
    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError>;

    /// Apply (grant or replace) a set of permissions to a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>;

    /// Revoke specific permissions from a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError>;

    /// Revoke all permissions of a group on a namespace.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_all(&self, namespace_id: i32, group_id: i32) -> Result<(), ApiError>;

    /// Whether mutations are supported. Handlers can early-reject before
    /// calling the mutation methods if they want a cleaner error path.
    fn supports_mutation(&self) -> bool;

    /// Backend kind identifier, used in tracing spans and the startup log.
    fn kind(&self) -> &'static str;
}
