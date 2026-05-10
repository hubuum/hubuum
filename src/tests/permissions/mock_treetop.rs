use std::sync::Arc;

use crate::models::Permissions;
use crate::permissions::backend::PermissionBackend;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::types::{
    PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
};

fn ns_request(namespace_id: i32, perms: Vec<Permissions>) -> PermissionRequest {
    PermissionRequest {
        resource: ResourceRef::namespace(namespace_id),
        permissions: perms,
    }
}

#[actix_web::test]
async fn mock_authorize_many_preserves_request_order() {
    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });

    let principal = PrincipalRef::new(1, vec![100]);
    let requests = vec![
        ns_request(7, vec![Permissions::ReadCollection]), // Allow
        ns_request(8, vec![Permissions::ReadCollection]), // Deny
        ns_request(7, vec![Permissions::UpdateCollection]), // Deny (no rule)
    ];
    let decisions = backend.authorize_many(&principal, requests).await.unwrap();
    assert_eq!(
        decisions,
        vec![
            PermissionDecision::Allow,
            PermissionDecision::Deny,
            PermissionDecision::Deny
        ],
        "decisions must come back in input order"
    );
}

#[actix_web::test]
async fn mock_authorizes_relation_via_from_to_namespace_attrs() {
    let backend = MockTreetopBackend::new();
    // A rule that requires both from_namespace_id == 5 AND to_namespace_id == 6.
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::ReadClassRelation,
        resource_kind: ResourceKind::ClassRelation,
        resource_id: None,
        attrs: ResourceAttrs {
            from_namespace_id: Some(5),
            to_namespace_id: Some(6),
            ..Default::default()
        },
    });

    let principal = PrincipalRef::new(1, vec![100]);

    // Matching relation (from=5, to=6).
    let req_match = PermissionRequest {
        resource: ResourceRef {
            kind: ResourceKind::ClassRelation,
            id: 42,
            attrs: ResourceAttrs {
                from_namespace_id: Some(5),
                to_namespace_id: Some(6),
                ..Default::default()
            },
        },
        permissions: vec![Permissions::ReadClassRelation],
    };
    assert_eq!(
        backend.authorize(&principal, req_match).await.unwrap(),
        PermissionDecision::Allow
    );

    // Wrong from-namespace.
    let req_wrong = PermissionRequest {
        resource: ResourceRef {
            kind: ResourceKind::ClassRelation,
            id: 42,
            attrs: ResourceAttrs {
                from_namespace_id: Some(99),
                to_namespace_id: Some(6),
                ..Default::default()
            },
        },
        permissions: vec![Permissions::ReadClassRelation],
    };
    assert_eq!(
        backend.authorize(&principal, req_wrong).await.unwrap(),
        PermissionDecision::Deny
    );
}

#[actix_web::test]
async fn mock_is_admin_uses_backend_rule_not_sql_group() {
    let backend = MockTreetopBackend::new();
    backend.add_admin_rule(/* group_id = */ 999);

    let admin_principal = PrincipalRef::new(1, vec![999]);
    let user_principal = PrincipalRef::new(2, vec![100]);

    assert!(
        backend.is_admin(&admin_principal).await.unwrap(),
        "admin group should be admin"
    );
    assert!(
        !backend.is_admin(&user_principal).await.unwrap(),
        "non-admin group should not be"
    );
}

#[actix_web::test]
async fn mock_mutation_methods_return_not_implemented() {
    let backend = MockTreetopBackend::new();
    use crate::models::PermissionsList;

    let result = backend
        .apply_permissions(
            7,
            100,
            PermissionsList::new(vec![Permissions::ReadCollection]),
            false,
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::errors::ApiError::NotImplemented(_))
    ));

    let result = backend
        .revoke_permissions(
            7,
            100,
            PermissionsList::new(vec![Permissions::ReadCollection]),
        )
        .await;
    assert!(matches!(
        result,
        Err(crate::errors::ApiError::NotImplemented(_))
    ));

    let result = backend.revoke_all(7, 100).await;
    assert!(matches!(
        result,
        Err(crate::errors::ApiError::NotImplemented(_))
    ));

    assert!(!backend.supports_mutation());
}

#[actix_web::test]
async fn mock_authorize_via_dyn_trait_works() {
    // Confirms the mock can stand in anywhere a real backend is expected.
    let backend: Arc<dyn PermissionBackend> = Arc::new(MockTreetopBackend::new());
    let principal = PrincipalRef::new(1, vec![100]);
    let req = ns_request(7, vec![Permissions::ReadCollection]);
    assert_eq!(
        backend.authorize(&principal, req).await.unwrap(),
        PermissionDecision::Deny,
        "no rules → deny by default"
    );
}
