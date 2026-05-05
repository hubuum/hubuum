//! Trait-level smoke tests for `PermissionBackend` impls.
//!
//! These tests exercise a backend through the public trait surface so the
//! same scenarios can be reused for the Treetop backend in Phase 5.

use std::sync::Arc;

use crate::models::{Permissions, PermissionsList};
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::tests::{create_test_group, create_test_user, get_pool_and_config};

#[actix_web::test]
async fn local_backend_grants_then_authorizes_namespace_read() {
    let (pool, _) = get_pool_and_config().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(pool.clone()));

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member(&pool, &user)
        .await
        .expect("failed to add user to group");

    // Use an existing namespace owned by a different group so we control
    // the (group_id, namespace_id) row this test grants. The namespace
    // fixture helper creates an "owner" group with full permissions; we
    // grant our separate test group only ReadCollection.
    let fixture = crate::tests::create_namespace_fixture(&pool, "perm_backend_smoke").await;
    let namespace_id = fixture.namespace.id;

    let principal = PrincipalRef::new(user.id, vec![group.id]);
    let req = PermissionRequest {
        resource: ResourceRef::namespace(namespace_id),
        permissions: vec![Permissions::ReadCollection],
    };

    // Before grant: deny.
    let decision = backend
        .authorize(&principal, req.clone())
        .await
        .expect("authorize call failed");
    assert_eq!(
        decision,
        PermissionDecision::Deny,
        "unauthorized group should be denied before the grant"
    );

    // Grant ReadCollection on this namespace to our group.
    backend
        .apply_permissions(
            namespace_id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("apply_permissions failed");

    // After grant: allow.
    let decision = backend
        .authorize(&principal, req)
        .await
        .expect("post-grant authorize failed");
    assert_eq!(
        decision,
        PermissionDecision::Allow,
        "group should be allowed after the grant"
    );

    // Asking for a permission that wasn't granted still denies.
    let req_update = PermissionRequest {
        resource: ResourceRef::namespace(namespace_id),
        permissions: vec![Permissions::UpdateCollection],
    };
    let decision = backend
        .authorize(&principal, req_update)
        .await
        .expect("authorize for ungranted permission failed");
    assert_eq!(
        decision,
        PermissionDecision::Deny,
        "group should not be allowed for permissions it was never granted"
    );
}

#[actix_web::test]
async fn local_backend_authorize_many_returns_per_request_decisions() {
    let (pool, _) = get_pool_and_config().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(pool.clone()));

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member(&pool, &user)
        .await
        .expect("failed to add user to group");

    let granted_ns = crate::tests::create_namespace_fixture(&pool, "perm_batch_granted").await;
    let denied_ns = crate::tests::create_namespace_fixture(&pool, "perm_batch_denied").await;

    backend
        .apply_permissions(
            granted_ns.namespace.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("apply_permissions failed");

    let principal = PrincipalRef::new(user.id, vec![group.id]);
    let requests = vec![
        PermissionRequest {
            resource: ResourceRef::namespace(granted_ns.namespace.id),
            permissions: vec![Permissions::ReadCollection],
        },
        PermissionRequest {
            resource: ResourceRef::namespace(denied_ns.namespace.id),
            permissions: vec![Permissions::ReadCollection],
        },
    ];

    let decisions = backend
        .authorize_many(&principal, requests)
        .await
        .expect("authorize_many failed");

    assert_eq!(
        decisions,
        vec![PermissionDecision::Allow, PermissionDecision::Deny],
        "decisions must be returned in the same order as the input requests"
    );
}
