//! Tests for the candidate-then-authorize visibility helper.
//!
//! This forces the slow path on LocalPermissionBackend to prove that the
//! generic helper correctly filters candidates, counts the authorized set
//! (NOT the candidate set), and applies pagination to the authorized rows.

#![cfg(test)]

use std::sync::Arc;

use actix_web::test as actix_test;
use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{Collection, GroupPermission, Permission, Permissions, PermissionsList};
use crate::permissions::backend::PermissionBackend;
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::permissions::visibility::paginate_authorized;
use crate::tests::{
    create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
};
use crate::utilities::auth::generate_random_password;

/// Wrapper that forces the slow-path branch by returning false from
/// `supports_sql_visibility_pushdown`.
struct ForceSlowPath {
    inner: Arc<LocalPermissionBackend>,
}

#[async_trait]
impl PermissionBackend for ForceSlowPath {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        self.inner.authorize_many(principal, requests).await
    }

    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        self.inner.authorize_candidates(principal, requests).await
    }

    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Collection>, ApiError> {
        self.inner
            .collections_user_can(principal, permissions)
            .await
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: i32,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        self.inner
            .groups_with_permissions_on(collection_id, permissions_filter, page)
            .await
    }

    async fn group_permission_on(
        &self,
        collection_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, ApiError> {
        self.inner
            .group_permission_on(collection_id, group_id)
            .await
    }

    async fn apply_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        self.inner
            .apply_permissions(collection_id, group_id, list, replace_existing)
            .await
    }

    async fn revoke_permissions(
        &self,
        collection_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, ApiError> {
        self.inner
            .revoke_permissions(collection_id, group_id, list)
            .await
    }

    async fn revoke_all(&self, collection_id: i32, group_id: i32) -> Result<(), ApiError> {
        self.inner.revoke_all(collection_id, group_id).await
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        self.inner.is_admin(principal).await
    }

    fn supports_mutation(&self) -> bool {
        self.inner.supports_mutation()
    }

    fn kind(&self) -> &'static str {
        "local-forced-slowpath"
    }

    fn supports_sql_visibility_pushdown(&self) -> bool {
        false
    }

    fn uses_sql_permission_store(&self) -> bool {
        true
    }

    fn supports_permission_provenance(&self) -> bool {
        true
    }
}

#[actix_test]
async fn paginate_authorized_filters_pages_correctly_under_slow_path() {
    let (pool, _) = get_pool_and_config().await;
    let local = Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        "admin".to_string(),
    ));
    let backend = ForceSlowPath { inner: local };
    assert!(!backend.supports_sql_visibility_pushdown());
    assert!(backend.uses_sql_permission_store());
    assert!(backend.supports_permission_provenance());

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member_without_events(&pool, &user)
        .await
        .expect("add user to group");

    // Create three collection fixtures; grant ReadCollection on the first
    // and third only. Build a candidate vector with the collections in a
    // known order (sorted by id).
    let unique = generate_random_password(8);
    let ns_a = create_collection_fixture(&pool, &format!("vis_a_{unique}")).await;
    let ns_b = create_collection_fixture(&pool, &format!("vis_b_{unique}")).await;
    let ns_c = create_collection_fixture(&pool, &format!("vis_c_{unique}")).await;

    backend
        .apply_permissions(
            ns_a.collection.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("grant on a");
    backend
        .apply_permissions(
            ns_c.collection.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("grant on c");

    let principal = PrincipalRef::new(user.id, vec![group.id]);

    let candidates = vec![
        ns_a.collection.clone(),
        ns_b.collection.clone(),
        ns_c.collection.clone(),
    ];
    let page = paginate_authorized(
        &backend,
        &principal,
        candidates,
        vec![Permissions::ReadCollection],
        0,
        10,
        |ns: &Collection| ResourceRef::collection(ns.id),
    )
    .await
    .expect("paginate_authorized failed");

    // Authorized: a + c. Total count must be 2 (NOT 3 — the candidate
    // set count would be wrong under Treetop and that's the point of
    // the slow path).
    assert_eq!(
        page.total_count, 2,
        "total_count must be the authorized count"
    );
    assert_eq!(page.rows.len(), 2);
    assert_eq!(page.rows[0].id, ns_a.collection.id);
    assert_eq!(page.rows[1].id, ns_c.collection.id);

    // Pagination of the authorized set: offset=1, limit=10 should return only c.
    let candidates = vec![
        ns_a.collection.clone(),
        ns_b.collection.clone(),
        ns_c.collection.clone(),
    ];
    let page = paginate_authorized(
        &backend,
        &principal,
        candidates,
        vec![Permissions::ReadCollection],
        1,
        10,
        |ns: &Collection| ResourceRef::collection(ns.id),
    )
    .await
    .expect("paginate_authorized offset failed");
    assert_eq!(page.total_count, 2);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].id, ns_c.collection.id);
}
