//! Tests for the candidate-then-authorize visibility helper.
//!
//! This forces the slow path on LocalPermissionBackend to prove that the
//! generic helper correctly filters candidates, counts the authorized set
//! (NOT the candidate set), and applies pagination to the authorized rows.

#![cfg(test)]

use std::sync::Arc;

use crate::models::{Namespace, Permission, Permissions, PermissionsList};
use crate::permissions::backend::PermissionBackend;
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::permissions::visibility::paginate_authorized;

/// Wrapper that forces the slow-path branch by returning false from
/// `supports_sql_visibility_join`.
struct ForceSlowPath {
    inner: Arc<LocalPermissionBackend>,
}

#[async_trait::async_trait]
impl PermissionBackend for ForceSlowPath {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, crate::errors::ApiError> {
        self.inner.authorize_many(principal, requests).await
    }

    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, crate::errors::ApiError> {
        self.inner.authorize_candidates(principal, requests).await
    }

    async fn namespaces_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[Permissions],
    ) -> Result<Vec<Namespace>, crate::errors::ApiError> {
        self.inner.namespaces_user_can(principal, permissions).await
    }

    async fn groups_with_permissions_on(
        &self,
        namespace_id: i32,
        permissions_filter: &[Permissions],
        page: &crate::models::search::QueryOptions,
    ) -> Result<(Vec<crate::models::GroupPermission>, i64), crate::errors::ApiError> {
        self.inner
            .groups_with_permissions_on(namespace_id, permissions_filter, page)
            .await
    }

    async fn group_permission_on(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<Option<Permission>, crate::errors::ApiError> {
        self.inner.group_permission_on(namespace_id, group_id).await
    }

    async fn apply_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
        replace_existing: bool,
    ) -> Result<Permission, crate::errors::ApiError> {
        self.inner
            .apply_permissions(namespace_id, group_id, list, replace_existing)
            .await
    }

    async fn revoke_permissions(
        &self,
        namespace_id: i32,
        group_id: i32,
        list: PermissionsList<Permissions>,
    ) -> Result<Permission, crate::errors::ApiError> {
        self.inner
            .revoke_permissions(namespace_id, group_id, list)
            .await
    }

    async fn revoke_all(
        &self,
        namespace_id: i32,
        group_id: i32,
    ) -> Result<(), crate::errors::ApiError> {
        self.inner.revoke_all(namespace_id, group_id).await
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, crate::errors::ApiError> {
        self.inner.is_admin(principal).await
    }

    fn supports_mutation(&self) -> bool {
        self.inner.supports_mutation()
    }

    fn kind(&self) -> &'static str {
        "local-forced-slowpath"
    }

    fn supports_sql_visibility_join(&self) -> bool {
        false
    }
}

#[actix_web::test]
async fn paginate_authorized_filters_pages_correctly_under_slow_path() {
    let (pool, _) = crate::tests::get_pool_and_config().await;
    let local = Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        "admin".to_string(),
    ));
    let backend = ForceSlowPath { inner: local };

    let user = crate::tests::create_test_user(&pool).await;
    let group = crate::tests::create_test_group(&pool).await;
    group
        .add_member(&pool, &user)
        .await
        .expect("add user to group");

    // Create three namespace fixtures; grant ReadCollection on the first
    // and third only. Build a candidate vector with the namespaces in a
    // known order (sorted by id).
    let unique = crate::utilities::auth::generate_random_password(8);
    let ns_a = crate::tests::create_namespace_fixture(&pool, &format!("vis_a_{unique}")).await;
    let ns_b = crate::tests::create_namespace_fixture(&pool, &format!("vis_b_{unique}")).await;
    let ns_c = crate::tests::create_namespace_fixture(&pool, &format!("vis_c_{unique}")).await;

    backend
        .apply_permissions(
            ns_a.namespace.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("grant on a");
    backend
        .apply_permissions(
            ns_c.namespace.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("grant on c");

    let principal = PrincipalRef::new(user.id, vec![group.id]);

    let candidates = vec![
        ns_a.namespace.clone(),
        ns_b.namespace.clone(),
        ns_c.namespace.clone(),
    ];
    let page = paginate_authorized(
        &backend,
        &principal,
        candidates,
        vec![Permissions::ReadCollection],
        0,
        10,
        |ns: &Namespace| ResourceRef::namespace(ns.id),
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
    assert_eq!(page.rows[0].id, ns_a.namespace.id);
    assert_eq!(page.rows[1].id, ns_c.namespace.id);

    // Pagination of the authorized set: offset=1, limit=10 should return only c.
    let candidates = vec![
        ns_a.namespace.clone(),
        ns_b.namespace.clone(),
        ns_c.namespace.clone(),
    ];
    let page = paginate_authorized(
        &backend,
        &principal,
        candidates,
        vec![Permissions::ReadCollection],
        1,
        10,
        |ns: &Namespace| ResourceRef::namespace(ns.id),
    )
    .await
    .expect("paginate_authorized offset failed");
    assert_eq!(page.total_count, 2);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].id, ns_c.namespace.id);
}
