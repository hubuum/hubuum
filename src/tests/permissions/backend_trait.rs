//! Trait-level smoke tests for `PermissionBackend` impls.
//!
//! These tests exercise a backend through the public trait surface so the
//! same scenarios can be reused for the Treetop backend in Phase 5.

#![cfg(test)]

use std::sync::Arc;

use actix_web::test as actix_test;

use crate::models::{CollectionID, GroupID, Permissions, PermissionsList};
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::{
    PermissionBackend, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};
use crate::storage::StorageHandle;
use crate::tests::permissions::conformance::{
    ConformanceBackend, ConformanceFixture, assert_backend_conformance,
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
        StorageHandle::postgres(pool.clone()),
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
            CollectionID::new(collection_id).unwrap(),
            GroupID::new(group.id).unwrap(),
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
        StorageHandle::postgres(pool.clone()),
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
            CollectionID::new(granted_ns.collection.id).unwrap(),
            GroupID::new(group.id).unwrap(),
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

#[actix_test]
async fn local_backend_satisfies_the_shared_authorization_corpus() {
    let (pool, _) = get_pool_and_config().await;
    let normal_user = create_test_user(&pool).await;
    let administrator = create_test_user(&pool).await;
    let unprivileged = create_test_user(&pool).await;
    let normal_group = create_test_group(&pool).await;
    let administrator_group = create_test_group(&pool).await;
    normal_group
        .add_member_without_events(&pool, &normal_user)
        .await
        .expect("failed to add the normal conformance user to its group");
    administrator_group
        .add_member_without_events(&pool, &administrator)
        .await
        .expect("failed to add the conformance administrator to its group");

    let granted = create_collection_fixture(&pool, &unique_label("conformance_granted")).await;
    let denied = create_collection_fixture(&pool, &unique_label("conformance_denied")).await;
    let backend = LocalPermissionBackend::new(
        StorageHandle::postgres(pool.clone()),
        administrator_group.groupname.clone(),
    );
    backend
        .apply_permissions(
            CollectionID::new(granted.collection.id).unwrap(),
            GroupID::new(normal_group.id).unwrap(),
            PermissionsList::new(Permissions::all().iter().copied()),
            false,
        )
        .await
        .expect("failed to seed the local conformance grants");

    let fixture = ConformanceFixture {
        normal: PrincipalRef::new(normal_user.id, [normal_group.id]),
        administrator: PrincipalRef::new(administrator.id, [administrator_group.id]),
        unprivileged: PrincipalRef::new(unprivileged.id, []),
        granted_collection_id: granted.collection.id,
        denied_collection_id: denied.collection.id,
        class_id: granted.collection.id + 10_000,
        object_id: granted.collection.id + 20_000,
        task_id: granted.collection.id + 30_000,
    };

    assert_backend_conformance(&backend, ConformanceBackend::Local, &fixture).await;

    granted
        .cleanup()
        .await
        .expect("granted fixture cleanup failed");
    denied
        .cleanup()
        .await
        .expect("denied fixture cleanup failed");
    normal_group
        .delete_without_events(&pool)
        .await
        .expect("normal group cleanup failed");
    administrator_group
        .delete_without_events(&pool)
        .await
        .expect("administrator group cleanup failed");
    normal_user
        .delete_without_events(&pool)
        .await
        .expect("normal user cleanup failed");
    administrator
        .delete_without_events(&pool)
        .await
        .expect("administrator cleanup failed");
    unprivileged
        .delete_without_events(&pool)
        .await
        .expect("unprivileged user cleanup failed");
}
