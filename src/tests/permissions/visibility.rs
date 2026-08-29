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
use crate::models::{
    Collection, CollectionID, GroupID, GroupPermission, Permission, Permissions, PermissionsList,
};
use crate::permissions::backend::PermissionBackend;
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs,
    ResourceKind, ResourceRef,
};
use crate::permissions::visibility::{
    AuthorizationPage, AuthorizedObjectIds, authorize_all_candidates,
    authorize_resource_permissions, paginate_authorized,
};
use crate::tests::{
    create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
};
use crate::utilities::auth::generate_random_password;

#[test]
fn authorized_object_ids_are_sorted_and_deduplicated() {
    let ids = AuthorizedObjectIds::new([3, 1, 3, 2]).unwrap();

    assert_eq!(ids.as_slice(), &[1, 2, 3]);
}

#[test]
fn authorized_object_ids_reject_non_positive_values() {
    let error = AuthorizedObjectIds::new([1, 0]).unwrap_err();

    assert_eq!(
        error,
        ApiError::InternalServerError("Authorized object ids must be positive".to_string())
    );
}

#[test]
fn authorized_object_ids_intersection_preserves_sorted_unique_ids() {
    let left = AuthorizedObjectIds::new([1, 2, 4]).unwrap();
    let right = AuthorizedObjectIds::new([2, 3, 4]).unwrap();

    assert_eq!(left.intersection(&right).as_slice(), &[2, 4]);
}

#[actix_test]
async fn candidate_authorization_bounds_each_expanded_permission_batch() {
    let backend = MockTreetopBackend::new();
    for action in [Permissions::ReadCollection, Permissions::UpdateCollection] {
        backend.add_rule(MockAllowRule {
            group_id: 7,
            action,
            resource_kind: ResourceKind::Collection,
            resource_id: None,
            attrs: ResourceAttrs::default(),
        });
    }
    let principal = PrincipalRef::new(1, [7]);
    let candidates = (1..=300).collect::<Vec<_>>();

    let authorized = authorize_all_candidates(
        &backend,
        &principal,
        candidates.clone(),
        None,
        vec![Permissions::ReadCollection, Permissions::UpdateCollection],
        |collection_id| ResourceRef::collection(*collection_id),
    )
    .await
    .expect("candidate authorization should succeed");

    assert_eq!(authorized, candidates);
    assert_eq!(backend.authorization_batch_sizes(), vec![512, 88]);
}

#[actix_test]
async fn candidate_authorization_normalizes_each_required_permission() {
    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(11),
        attrs: ResourceAttrs::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(5),
        attrs: ResourceAttrs::default(),
    });
    let principal = PrincipalRef::new(1, [7]);

    let authorized = authorize_all_candidates(
        &backend,
        &principal,
        vec![11],
        None,
        vec![Permissions::ReadClass, Permissions::ReadCollection],
        |class_id| ResourceRef {
            kind: ResourceKind::Class,
            id: *class_id,
            attrs: ResourceAttrs {
                collection_id: Some(5),
                ..Default::default()
            },
        },
    )
    .await
    .expect("normalized candidate authorization should succeed");

    assert_eq!(authorized, vec![11]);
    assert_eq!(backend.authorization_batch_sizes(), vec![2]);
}

#[actix_test]
async fn resource_permissions_are_normalized_to_policy_resource_kinds() {
    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadClass,
        resource_kind: ResourceKind::Class,
        resource_id: Some(11),
        attrs: ResourceAttrs::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(5),
        attrs: ResourceAttrs::default(),
    });
    let principal = PrincipalRef::new(1, [7]);
    let class_resource = ResourceRef {
        kind: ResourceKind::Class,
        id: 11,
        attrs: ResourceAttrs {
            collection_id: Some(5),
            ..Default::default()
        },
    };

    let authorized = authorize_resource_permissions(
        &backend,
        &principal,
        &class_resource,
        None,
        &[Permissions::ReadClass, Permissions::ReadCollection],
    )
    .await
    .expect("normalized authorization should succeed");

    assert!(authorized);
    assert_eq!(backend.authorization_batch_sizes(), vec![2]);
}

#[actix_test]
async fn authorized_page_preserves_order_across_batch_boundaries() {
    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: None,
        attrs: ResourceAttrs::default(),
    });
    let principal = PrincipalRef::new(1, [7]);

    let page = paginate_authorized(
        &backend,
        &principal,
        (1..=600).collect(),
        None,
        vec![Permissions::ReadCollection],
        AuthorizationPage::new(510, 4),
        |collection_id| ResourceRef::collection(*collection_id),
    )
    .await
    .expect("authorized pagination should succeed");

    assert_eq!(page.total_count, 600);
    assert_eq!(page.rows, vec![511, 512, 513, 514]);
    assert_eq!(backend.authorization_batch_sizes(), vec![512, 88]);
}

/// Wrapper that forces the slow-path branch by returning false from
/// `supports_storage_visibility_filtering`.
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
        collection_id: CollectionID,
        permissions_filter: &[Permissions],
        page: &QueryOptions,
    ) -> Result<(Vec<GroupPermission>, i64), ApiError> {
        self.inner
            .groups_with_permissions_on(collection_id, permissions_filter, page)
            .await
    }

    async fn group_permission_on(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<Option<Permission>, ApiError> {
        self.inner
            .group_permission_on(collection_id, group_id)
            .await
    }

    async fn apply_permissions(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
        replace_existing: bool,
    ) -> Result<Permission, ApiError> {
        self.inner
            .apply_permissions(collection_id, group_id, list, replace_existing)
            .await
    }

    async fn revoke_permissions(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
        list: PermissionsList,
    ) -> Result<Permission, ApiError> {
        self.inner
            .revoke_permissions(collection_id, group_id, list)
            .await
    }

    async fn revoke_all(
        &self,
        collection_id: CollectionID,
        group_id: GroupID,
    ) -> Result<(), ApiError> {
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

    fn supports_storage_visibility_filtering(&self) -> bool {
        false
    }

    fn uses_local_permission_store(&self) -> bool {
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
        crate::storage::StorageHandle::postgres(pool.clone()),
        "admin".to_string(),
    ));
    let backend = ForceSlowPath { inner: local };
    assert!(!backend.supports_storage_visibility_filtering());
    assert!(backend.uses_local_permission_store());
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
            CollectionID::new(ns_a.collection.id).unwrap(),
            GroupID::new(group.id).unwrap(),
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("grant on a");
    backend
        .apply_permissions(
            CollectionID::new(ns_c.collection.id).unwrap(),
            GroupID::new(group.id).unwrap(),
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
        None,
        vec![Permissions::ReadCollection],
        AuthorizationPage::new(0, 10),
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
        None,
        vec![Permissions::ReadCollection],
        AuthorizationPage::new(1, 10),
        |ns: &Collection| ResourceRef::collection(ns.id),
    )
    .await
    .expect("paginate_authorized offset failed");
    assert_eq!(page.total_count, 2);
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0].id, ns_c.collection.id);
}
