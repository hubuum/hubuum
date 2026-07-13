//! Tests for AuthzTarget implementations.
//!
//! These tests verify that all model types correctly implement AuthzTarget
//! and that the local backend handles relation resources with proper AND-checks
//! across both collections.

#![cfg(test)]

use std::sync::Arc;

use actix_web::test as actix_test;
use serde_json::json;

use crate::models::{
    NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation, Permissions,
    PermissionsList,
};
use crate::permissions::{
    AuthzTarget, LocalPermissionBackend, PermissionBackend, PermissionDecision, PermissionRequest,
    PrincipalRef, ResourceKind,
};
use crate::tests::{
    create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
};
use crate::traits::CanSave;
use crate::utilities::auth::generate_random_password;

/// Unique fixture label so re-runs against a persistent test DB don't collide.
fn unique_label(prefix: &str) -> String {
    format!("{prefix}_{}", generate_random_password(8))
}

#[actix_test]
async fn collection_to_resource_ref_populates_attrs() {
    let (pool, _) = get_pool_and_config().await;
    let fixture = create_collection_fixture(&pool, &unique_label("ns_authz")).await;

    let resource_ref = fixture
        .collection
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::Collection);
    assert_eq!(resource_ref.id, fixture.collection.id);
    assert_eq!(
        resource_ref.attrs.collection_id,
        Some(fixture.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.name,
        Some(fixture.collection.name.clone())
    );
}

#[actix_test]
async fn class_to_resource_ref_populates_collection_id() {
    let (pool, _) = get_pool_and_config().await;
    let fixture = create_collection_fixture(&pool, &unique_label("class_authz")).await;

    let class = NewHubuumClass {
        name: unique_label("test_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Test class".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class creation failed");

    let resource_ref = class
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::Class);
    assert_eq!(resource_ref.id, class.id);
    assert_eq!(resource_ref.attrs.collection_id, Some(class.collection_id));
    assert_eq!(resource_ref.attrs.name, Some(class.name.clone()));
}

#[actix_test]
async fn object_to_resource_ref_populates_collection_and_class_ids() {
    let (pool, _) = get_pool_and_config().await;
    let fixture = create_collection_fixture(&pool, &unique_label("object_authz")).await;

    let class = NewHubuumClass {
        name: unique_label("test_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Test class".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class creation failed");

    let object = NewHubuumObject {
        name: unique_label("test_object"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: json!({}),
        description: "Test object".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("object creation failed");

    let resource_ref = object
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::Object);
    assert_eq!(resource_ref.id, object.id);
    assert_eq!(resource_ref.attrs.collection_id, Some(object.collection_id));
    assert_eq!(resource_ref.attrs.class_id, Some(object.hubuum_class_id));
    assert_eq!(resource_ref.attrs.name, Some(object.name.clone()));
}

#[actix_test]
async fn class_relation_cross_collection_populates_from_to_collections() {
    let (pool, _) = get_pool_and_config().await;
    let fixture_a = create_collection_fixture(&pool, &unique_label("class_rel_a")).await;
    let fixture_b = create_collection_fixture(&pool, &unique_label("class_rel_b")).await;

    let class_a = NewHubuumClass {
        name: unique_label("class_a"),
        collection_id: fixture_a.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class in collection A".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_a creation failed");

    let class_b = NewHubuumClass {
        name: unique_label("class_b"),
        collection_id: fixture_b.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class in collection B".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_b creation failed");

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save_without_events(&pool)
    .await
    .expect("class relation creation failed");

    let resource_ref = relation
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::ClassRelation);
    assert_eq!(resource_ref.id, relation.id);
    assert_eq!(
        resource_ref.attrs.from_collection_id,
        Some(fixture_a.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.to_collection_id,
        Some(fixture_b.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.collection_id, None,
        "cross-collection relation should have collection_id=None"
    );
    assert_eq!(
        resource_ref.attrs.from_class_id,
        Some(relation.from_hubuum_class_id)
    );
    assert_eq!(
        resource_ref.attrs.to_class_id,
        Some(relation.to_hubuum_class_id)
    );
}

#[actix_test]
async fn class_relation_same_collection_populates_collection_id() {
    let (pool, _) = get_pool_and_config().await;
    let fixture = create_collection_fixture(&pool, &unique_label("class_rel_same")).await;

    let class_a = NewHubuumClass {
        name: unique_label("class_a"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class A".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_a creation failed");

    let class_b = NewHubuumClass {
        name: unique_label("class_b"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class B".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_b creation failed");

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save_without_events(&pool)
    .await
    .expect("class relation creation failed");

    let resource_ref = relation
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::ClassRelation);
    assert_eq!(
        resource_ref.attrs.from_collection_id,
        Some(fixture.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.to_collection_id,
        Some(fixture.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.collection_id,
        Some(fixture.collection.id),
        "same-collection relation should populate collection_id"
    );
}

#[actix_test]
async fn object_relation_cross_collection_populates_all_fields() {
    let (pool, _) = get_pool_and_config().await;
    let fixture_a = create_collection_fixture(&pool, &unique_label("obj_rel_a")).await;
    let fixture_b = create_collection_fixture(&pool, &unique_label("obj_rel_b")).await;

    let class_a = NewHubuumClass {
        name: unique_label("class_a"),
        collection_id: fixture_a.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class in collection A".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_a creation failed");

    let class_b = NewHubuumClass {
        name: unique_label("class_b"),
        collection_id: fixture_b.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class in collection B".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_b creation failed");

    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save_without_events(&pool)
    .await
    .expect("class relation creation failed");

    let object_a = NewHubuumObject {
        name: unique_label("object_a"),
        collection_id: fixture_a.collection.id,
        hubuum_class_id: class_a.id,
        data: json!({}),
        description: "Object in collection A".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("object_a creation failed");

    let object_b = NewHubuumObject {
        name: unique_label("object_b"),
        collection_id: fixture_b.collection.id,
        hubuum_class_id: class_b.id,
        data: json!({}),
        description: "Object in collection B".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("object_b creation failed");

    let object_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_a.id,
        to_hubuum_object_id: object_b.id,
        class_relation_id: class_relation.id,
    }
    .save_without_events(&pool)
    .await
    .expect("object relation creation failed");

    let resource_ref = object_relation
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");

    assert_eq!(resource_ref.kind, ResourceKind::ObjectRelation);
    assert_eq!(resource_ref.id, object_relation.id);
    assert_eq!(
        resource_ref.attrs.from_collection_id,
        Some(fixture_a.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.to_collection_id,
        Some(fixture_b.collection.id)
    );
    assert_eq!(
        resource_ref.attrs.collection_id, None,
        "cross-collection relation should have collection_id=None"
    );
    assert_eq!(
        resource_ref.attrs.from_object_id,
        Some(object_relation.from_hubuum_object_id)
    );
    assert_eq!(
        resource_ref.attrs.to_object_id,
        Some(object_relation.to_hubuum_object_id)
    );
    assert_eq!(
        resource_ref.attrs.from_class_id,
        Some(class_a.id),
        "object relation should expose the from-side class id for policy use"
    );
    assert_eq!(
        resource_ref.attrs.to_class_id,
        Some(class_b.id),
        "object relation should expose the to-side class id for policy use"
    );
    assert_eq!(
        resource_ref.attrs.class_relation_id,
        Some(object_relation.class_relation_id)
    );
}

#[actix_test]
async fn local_backend_relation_and_check_denies_partial_permission() {
    let (pool, _) = get_pool_and_config().await;
    let backend: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
        pool.clone(),
        "admin".to_string(),
    ));

    let fixture_a = create_collection_fixture(&pool, &unique_label("rel_and_a")).await;
    let fixture_b = create_collection_fixture(&pool, &unique_label("rel_and_b")).await;

    let class_a = NewHubuumClass {
        name: unique_label("class_a"),
        collection_id: fixture_a.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class A".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_a creation failed");

    let class_b = NewHubuumClass {
        name: unique_label("class_b"),
        collection_id: fixture_b.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "Class B".to_string(),
    }
    .save_without_events(&pool)
    .await
    .expect("class_b creation failed");

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save_without_events(&pool)
    .await
    .expect("class relation creation failed");

    let user = create_test_user(&pool).await;
    let group = create_test_group(&pool).await;
    group
        .add_member_without_events(&pool, &user)
        .await
        .expect("failed to add user to group");

    let principal = PrincipalRef::new(user.id, vec![group.id]);
    let resource_ref = relation
        .to_resource_ref(&pool)
        .await
        .expect("to_resource_ref failed");
    let request = PermissionRequest {
        resource: resource_ref,
        permissions: vec![Permissions::ReadClassRelation],
    };

    // Grant ReadClassRelation on collection_a only.
    backend
        .apply_permissions(
            fixture_a.collection.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadClassRelation]),
            false,
        )
        .await
        .expect("apply_permissions failed");

    // Should deny: permission on collection_a but not collection_b.
    let decision = backend
        .authorize(&principal, request.clone())
        .await
        .expect("authorize call failed");
    assert_eq!(
        decision,
        PermissionDecision::Deny,
        "relation should be denied when permission is missing on one collection"
    );

    // Grant ReadClassRelation on collection_b too.
    backend
        .apply_permissions(
            fixture_b.collection.id,
            group.id,
            PermissionsList::new(vec![Permissions::ReadClassRelation]),
            false,
        )
        .await
        .expect("apply_permissions failed");

    // Should allow: permission on both collections.
    let decision = backend
        .authorize(&principal, request)
        .await
        .expect("authorize call failed");
    assert_eq!(
        decision,
        PermissionDecision::Allow,
        "relation should be allowed when permission is granted on both collections"
    );
}
