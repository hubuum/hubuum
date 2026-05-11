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

#[actix_web::test]
async fn mock_group_permission_on_synthesizes_row_from_rules() {
    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::CreateClass,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });

    let row = backend.group_permission_on(7, 100).await.unwrap();
    assert!(
        row.is_some(),
        "should synthesize a row when at least one perm is Allow"
    );
    let row = row.unwrap();
    assert!(
        row.has_read_namespace,
        "ReadCollection grant should set has_read_namespace"
    );
    assert!(
        row.has_create_class,
        "CreateClass grant should set has_create_class"
    );
    assert!(
        !row.has_update_namespace,
        "ungranted permissions stay false"
    );
    assert_eq!(row.id, 0, "synthetic row id is 0");
    assert_eq!(row.namespace_id, 7);
    assert_eq!(row.group_id, 100);

    // No rules for group 200 → None.
    let none = backend.group_permission_on(7, 200).await.unwrap();
    assert!(
        none.is_none(),
        "no permissions → None (mirrors Local 'no row exists')"
    );
}

#[actix_web::test]
async fn mock_groups_with_permissions_on_filters_and_paginates() {
    use crate::models::{Group, search::QueryOptions};
    use chrono::NaiveDate;

    let backend = MockTreetopBackend::new();
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 100,
        action: Permissions::CreateClass,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 200,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Namespace,
        resource_id: Some(7),
        attrs: ResourceAttrs::default(),
    });

    // Set up synthetic groups.
    let groups = vec![
        Group {
            id: 100,
            groupname: "group100".to_string(),
            description: "test group".to_string(),
            created_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            updated_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        },
        Group {
            id: 200,
            groupname: "group200".to_string(),
            description: "test group".to_string(),
            created_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            updated_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        },
        Group {
            id: 300,
            groupname: "group300".to_string(),
            description: "test group".to_string(),
            created_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
            updated_at: NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        },
    ];
    backend.set_group_candidates(groups);

    // Empty filter: include groups with at least one permission.
    let page = QueryOptions {
        filters: vec![],
        sort: vec![],
        limit: None,
        cursor: None,
    };
    let (results, count) = backend
        .groups_with_permissions_on(7, &[], &page)
        .await
        .unwrap();
    assert_eq!(count, 2, "group 100 and 200 have permissions");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].group.id, 100);
    assert_eq!(results[1].group.id, 200);

    // Non-empty filter: only groups with ALL filter permissions.
    let (results, count) = backend
        .groups_with_permissions_on(
            7,
            &[Permissions::ReadCollection, Permissions::CreateClass],
            &page,
        )
        .await
        .unwrap();
    assert_eq!(count, 1, "only group 100 has both permissions");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].group.id, 100);

    // Limit pagination.
    let page_limited = QueryOptions {
        filters: vec![],
        sort: vec![],
        limit: Some(1),
        cursor: None,
    };
    let (results, count) = backend
        .groups_with_permissions_on(7, &[], &page_limited)
        .await
        .unwrap();
    assert_eq!(count, 2, "total count is 2 even though limit is 1");
    assert_eq!(results.len(), 1, "limit restricts returned rows");
    assert_eq!(results[0].group.id, 100);
}
