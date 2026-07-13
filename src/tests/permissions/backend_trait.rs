//! Trait-level smoke tests for `PermissionBackend` impls.
//!
//! These tests exercise a backend through the public trait surface so the
//! same scenarios can be reused for the Treetop backend in Phase 5.

#![cfg(test)]

use std::sync::Arc;

use actix_web::test as actix_test;

use crate::models::{Permissions, PermissionsList};
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::tests::{
    create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
};
use crate::utilities::auth::generate_random_password;

/// Unique fixture label so re-runs against a persistent test DB don't trip
/// the `groupname` unique constraint via `create_collection_fixture`'s
/// deterministic owner-group naming.
fn unique_label(prefix: &str) -> String {
    format!("{prefix}_{}", generate_random_password(8))
}

#[actix_test]
async fn local_backend_grants_then_authorizes_collection_read() {
    let (pool, _) = get_pool_and_config().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        "admin".to_string(),
    ));

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member_without_events(&pool, &user)
        .await
        .expect("failed to add user to group");

    // Use an existing collection owned by a different group so we control
    // the (group_id, collection_id) row this test grants. The collection
    // fixture helper creates an "owner" group with full permissions; we
    // grant our separate test group only ReadCollection.
    let fixture = create_collection_fixture(&pool, &unique_label("perm_backend_smoke")).await;
    let collection_id = fixture.collection.id;

    let principal = PrincipalRef::new(user.id, vec![group.id]);
    let req = PermissionRequest {
        resource: ResourceRef::collection(collection_id),
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

    // Grant ReadCollection on this collection to our group.
    backend
        .apply_permissions(
            collection_id,
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
        resource: ResourceRef::collection(collection_id),
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

#[actix_test]
async fn local_backend_authorize_many_returns_per_request_decisions() {
    let (pool, _) = get_pool_and_config().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        "admin".to_string(),
    ));

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member_without_events(&pool, &user)
        .await
        .expect("failed to add user to group");

    let granted_ns = create_collection_fixture(&pool, &unique_label("perm_batch_granted")).await;
    let denied_ns = create_collection_fixture(&pool, &unique_label("perm_batch_denied")).await;

    backend
        .apply_permissions(
            granted_ns.collection.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await
        .expect("apply_permissions failed");

    let principal = PrincipalRef::new(user.id, vec![group.id]);
    let requests = vec![
        PermissionRequest {
            resource: ResourceRef::collection(granted_ns.collection.id),
            permissions: vec![Permissions::ReadCollection],
        },
        PermissionRequest {
            resource: ResourceRef::collection(denied_ns.collection.id),
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
