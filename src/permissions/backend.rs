use std::num::NonZeroUsize;

use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionID, GroupID, GroupPermission, Permission, Permissions, PermissionsList,
};

use super::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};

pub const MAX_COMPLETE_COLLECTION_CANDIDATES: usize = 10_000;

/// Explicit retention ceiling for operations that require a complete
/// authorized collection list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompleteCollectionCandidateLimit(NonZeroUsize);

impl CompleteCollectionCandidateLimit {
    pub fn try_new(value: usize) -> Result<Self, ApiError> {
        let value = NonZeroUsize::new(value).ok_or_else(|| {
            ApiError::BadRequest(
                "A complete collection candidate limit must be positive".to_string(),
            )
        })?;
        if value.get() > MAX_COMPLETE_COLLECTION_CANDIDATES {
            return Err(ApiError::BadRequest(format!(
                "A complete collection candidate limit must not exceed {MAX_COMPLETE_COLLECTION_CANDIDATES}"
            )));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

#[async_trait]
pub trait PermissionBackend: Send + Sync {
    /// Batch point check: does the principal satisfy each request?
    /// Order of the returned vector matches the order of `requests`.
    ///
    /// This is the only required decision method. The single-request and
    /// filter helpers default to wrapping `authorize_many`; backends that
    /// can batch transport-side (e.g. Treetop's `AuthorizeRequest`) only
    /// need to override this method.
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError>;

    /// Single point check: does the principal satisfy all
    /// `request.permissions` on `request.resource`?
    ///
    /// Default: dispatches to `authorize_many` with a single-element vector
    /// and returns the decision. Backends rarely need to override.
    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError> {
        let mut decisions = self.authorize_many(principal, vec![request]).await?;
        decisions.pop().ok_or_else(|| {
            ApiError::InternalServerError(
                "permission backend returned no decisions for a single request".to_string(),
            )
        })
    }

    /// Decide each request and return decisions paired with their
    /// original requests, in input order. Used by list/search visibility
    /// paths to keep request data alongside its decision so callers don't
    /// have to re-zip parallel vectors.
    ///
    /// **This does not filter** — it returns both Allow and Deny
    /// decisions. Call sites filter on the resulting `decision` field
    /// themselves.
    ///
    /// Default: pairs `authorize_many`'s result with the inputs.
    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        let decisions = self.authorize_many(principal, requests.clone()).await?;
        Ok(requests
            .into_iter()
            .zip(decisions)
            .map(|(request, decision)| AuthorizationResult { request, decision })
            .collect())
    }

    /// Decide whether a principal may read a background task. Local-style
    /// backends retain the existing owner-or-admin semantics; Treetop
    /// overrides this with the schema-level `ReadTask` action.
    async fn authorize_task(
        &self,
        principal: &PrincipalRef,
        task: &ResourceRef,
    ) -> Result<PermissionDecision, ApiError> {
        let allowed =
            self.is_admin(principal).await? || task.attrs.submitted_by == Some(principal.user_id);
        Ok(if allowed {
            PermissionDecision::Allow
        } else {
            PermissionDecision::Deny
        })
    }

    async fn authorize_tasks(
        &self,
        principal: &PrincipalRef,
        tasks: &[ResourceRef],
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        let is_admin = self.is_admin(principal).await?;
        Ok(tasks
            .iter()
            .map(|task| {
                if is_admin || task.attrs.submitted_by == Some(principal.user_id) {
                    PermissionDecision::Allow
                } else {
                    PermissionDecision::Deny
                }
            })
            .collect())
    }

    /// All collections on which the principal has every requested permission.
    /// Used by listing endpoints that want to scope their candidate query
    /// (e.g. `GET /templates`).
    ///
    /// Empty `permissions` means "any permission grants visibility" — that
    /// is, the collection appears if the principal has *any* row on it. The
    /// caller usually passes one or more concrete permissions.
    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
        candidate_limit: CompleteCollectionCandidateLimit,
    ) -> Result<Vec<Collection>, ApiError>;

    /// (group, permission) pairs visible on a collection, paginated.
    ///
    /// `page` is the unprepared public query contract. Implementations apply
    /// the stable sort, cursor, and one-row look-ahead. Returns
    /// `(rows, total_count)` so handlers can populate `X-Total-Count`.
    async fn groups_with_permissions_on(
        &self,
        collection_id: CollectionID,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError>;

    /// Single group's permissions on a collection, or `None` if no row.
    /// In Treetop mode `id` / `created_at` / `updated_at` are synthetic.
    async fn group_permission_on(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<Option<Permission>, ApiError>;

    /// Apply (grant or replace) a set of permissions to a group on a collection.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn apply_permissions(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>;

    /// Revoke specific permissions from a group on a collection.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_permissions(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
    ) -> Result<Permission, ApiError>;

    /// Revoke all permissions of a group on a collection.
    /// Treetop returns `ApiError::NotImplemented`.
    async fn revoke_all(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<(), ApiError>;

    /// Whether mutations are supported. Handlers can early-reject before
    /// calling the mutation methods if they want a cleaner error path.
    fn supports_mutation(&self) -> bool;

    /// Backend kind identifier, used in tracing spans and the startup log.
    fn kind(&self) -> &'static str;

    /// Whether the principal has administrative privileges. Used by the
    /// AdminAccess extractor and by mutation paths that need a global
    /// override. Backends are responsible for whatever "admin" means in
    /// their model — the local backend checks group membership against
    /// the configured admin groupname; Treetop dispatches a Cedar policy
    /// decision against the system resource.
    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError>;

    /// Whether storage reads already enforce this backend's visibility policy.
    /// Backends returning `false` must authorize the candidate resources after
    /// loading them from storage.
    fn supports_storage_visibility_filtering(&self) -> bool;

    /// Whether point authorization and grant queries use Hubuum's local
    /// permission store. Kept separate from visibility filtering so hybrid
    /// backends can independently choose each strategy.
    fn uses_local_permission_store(&self) -> bool;

    /// Whether effective/granting-group provenance is available. A backend
    /// may authorize from the local store without exposing provenance, or
    /// provide provenance through another authoritative store.
    fn supports_permission_provenance(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_collection_candidate_limits_are_positive_and_bounded() {
        assert!(CompleteCollectionCandidateLimit::try_new(0).is_err());
        assert_eq!(
            CompleteCollectionCandidateLimit::try_new(MAX_COMPLETE_COLLECTION_CANDIDATES)
                .unwrap()
                .get(),
            MAX_COMPLETE_COLLECTION_CANDIDATES
        );
        assert!(
            CompleteCollectionCandidateLimit::try_new(MAX_COMPLETE_COLLECTION_CANDIDATES + 1)
                .is_err()
        );
    }
}
