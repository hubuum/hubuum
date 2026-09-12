//! Tests for the candidate-then-authorize visibility helper.
//!
//! This forces the slow path on LocalPermissionBackend to prove that the
//! generic helper correctly filters candidates, counts the authorized set
//! (NOT the candidate set), and applies pagination to the authorized rows.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use actix_web::test as actix_test;
use async_trait::async_trait;

use crate::errors::ApiError;
use crate::models::search::QueryOptions;
use crate::models::{
    Collection, CollectionID, GroupID, GroupPermission, Permission, Permissions, PermissionsList,
    ResourceRevision,
};
use crate::pagination::{finalize_page, paginate_in_memory, prepare_db_pagination};
use crate::permissions::backend::PermissionBackend;
use crate::permissions::local::LocalPermissionBackend;
use crate::permissions::test_support::{MockAllowRule, MockTreetopBackend};
use crate::permissions::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceFields,
    ResourceKind, ResourceRef,
};
use crate::permissions::visibility::{
    AuthorizationPage, AuthorizedObjectIds, authorize_all_candidates,
    authorize_cursor_page_from_storage, authorize_resource_permissions, paginate_authorized,
};
use crate::tests::{
    create_collection_fixture, create_test_group, create_test_user, get_pool_and_config,
};
use crate::traits::CursorPaginated;
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

fn candidate_collection(id: i32) -> Collection {
    Collection {
        id,
        name: format!("collection-{id:04}"),
        description: String::new(),
        created_at: chrono::DateTime::UNIX_EPOCH.naive_utc(),
        updated_at: chrono::DateTime::UNIX_EPOCH.naive_utc(),
        parent_collection_id: None,
        revision: ResourceRevision::INITIAL,
    }
}

fn allow_all_collection_reads(backend: &MockTreetopBackend) {
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: None,
        attrs: ResourceFields::default(),
    });
}

#[actix_test]
async fn storage_backed_authorization_stops_after_one_bounded_candidate_page() {
    let backend = MockTreetopBackend::new();
    allow_all_collection_reads(&backend);
    let principal = PrincipalRef::new(1, [7]);
    let candidates = (1..=700).map(candidate_collection).collect::<Vec<_>>();
    let fetch_sizes = Arc::new(Mutex::new(Vec::new()));
    let query = QueryOptions::new(vec![], vec![], Some(3), None, false).unwrap();

    let page = authorize_cursor_page_from_storage(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &query,
        |candidate_query| {
            let candidates = candidates.clone();
            let fetch_sizes = Arc::clone(&fetch_sizes);
            async move {
                let rows = paginate_in_memory(candidates, &candidate_query)?;
                fetch_sizes.lock().unwrap().push(rows.len());
                Ok(rows)
            }
        },
        |collection| ResourceRef::collection(collection.id),
    )
    .await
    .expect("bounded authorization should succeed");

    assert_eq!(
        page.rows.len(),
        4,
        "only the response look-ahead is retained"
    );
    assert_eq!(page.total_count, crate::pagination::SKIPPED_TOTAL_COUNT);
    assert_eq!(*fetch_sizes.lock().unwrap(), vec![5]);
    assert_eq!(backend.authorization_batch_sizes(), vec![4]);
}

#[actix_test]
async fn storage_backed_authorization_keeps_exact_total_global_after_a_cursor() {
    let backend = MockTreetopBackend::new();
    allow_all_collection_reads(&backend);
    let principal = PrincipalRef::new(1, [7]);
    let candidates = (1..=700).map(candidate_collection).collect::<Vec<_>>();
    let first_query = QueryOptions::new(vec![], vec![], Some(3), None, true).unwrap();
    let first_prepared = prepare_db_pagination::<Collection>(&first_query).unwrap();
    let first_rows = paginate_in_memory(candidates.clone(), &first_prepared).unwrap();
    let first_page = finalize_page(first_rows, &first_query).unwrap();
    let mut second_query = first_query;
    second_query.set_cursor(first_page.next_cursor).unwrap();
    let fetch_sizes = Arc::new(Mutex::new(Vec::new()));

    let page = authorize_cursor_page_from_storage(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &second_query,
        |candidate_query| {
            let candidates = candidates.clone();
            let fetch_sizes = Arc::clone(&fetch_sizes);
            async move {
                let rows = paginate_in_memory(candidates, &candidate_query)?;
                fetch_sizes.lock().unwrap().push(rows.len());
                Ok(rows)
            }
        },
        |collection| ResourceRef::collection(collection.id),
    )
    .await
    .expect("exact-count authorization should succeed");
    let response = finalize_page(page.rows, &second_query).unwrap();

    assert_eq!(page.total_count, 700);
    assert_eq!(
        response
            .items
            .iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        vec![4, 5, 6]
    );
    assert_eq!(
        *fetch_sizes.lock().unwrap(),
        vec![129, 129, 129, 129, 129, 60]
    );
}

#[rstest::rstest]
#[actix_web::test]
async fn storage_json_cursor_uses_backend_order_even_without_the_boundary_row(
    #[values(false, true)] include_total: bool,
    #[values(false, true)] deleted_boundary: bool,
) {
    use crate::models::search::{FilterField, SortParam};
    use crate::pagination::{CursorValue, decode_cursor_values, encode_cursor};

    #[derive(Clone)]
    struct JsonRow {
        id: i32,
        value: &'static str,
    }
    impl CursorPaginated for JsonRow {
        fn supports_sort(field: &FilterField) -> bool {
            matches!(field, FilterField::Description | FilterField::Id)
        }
        fn default_sort() -> Vec<SortParam> {
            vec![SortParam::new(FilterField::Description, false)]
        }
        fn tie_breaker_sort() -> Vec<SortParam> {
            vec![SortParam::new(FilterField::Id, false)]
        }
        fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
            match field {
                FilterField::Description => Ok(CursorValue::Json(serde_json::json!([self.value]))),
                FilterField::Id => Ok(CursorValue::Integer(i64::from(self.id))),
                _ => unreachable!("unsupported test sort"),
            }
        }
    }

    let backend = MockTreetopBackend::new();
    allow_all_collection_reads(&backend);
    let principal = PrincipalRef::new(1, [7]);
    let boundary = JsonRow { id: 1, value: "a" };
    let sorts = [JsonRow::default_sort(), JsonRow::tie_breaker_sort()].concat();
    let query = QueryOptions::new(
        vec![],
        sorts.clone(),
        Some(1),
        Some(encode_cursor(&boundary, &sorts).unwrap()),
        include_total,
    )
    .unwrap();
    // The fake storage order deliberately differs from Rust on every test host,
    // including CI databases initialized with C collation.
    let candidates = [boundary, JsonRow { id: 2, value: "Z" }]
        .into_iter()
        .filter(|row| !deleted_boundary || row.id != 1)
        .collect::<Vec<_>>();
    let page = authorize_cursor_page_from_storage(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &query,
        |query| {
            let after_id = query
                .cursor()
                .map(|cursor| {
                    let values = decode_cursor_values(cursor, query.sort()).unwrap();
                    let CursorValue::Integer(id) = values[1] else {
                        panic!("missing tie breaker")
                    };
                    id
                })
                .unwrap_or(0);
            std::future::ready(Ok(candidates
                .iter()
                .filter(|row| i64::from(row.id) > after_id)
                .take(query.limit().unwrap())
                .cloned()
                .collect()))
        },
        |row: &JsonRow| ResourceRef::collection(row.id),
    )
    .await
    .unwrap();
    assert_eq!(
        page.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(
        page.total_count,
        if include_total {
            candidates.len() as i64
        } else {
            crate::pagination::SKIPPED_TOTAL_COUNT
        }
    );
}

#[rstest::rstest]
#[case::unused_look_ahead(false, false)]
#[case::needed_for_exact_total(true, true)]
#[case::needed_after_denial(false, true)]
#[actix_web::test]
async fn storage_continuation_size_is_checked_only_when_fetching_again(
    #[case] include_total: bool,
    #[case] deny_look_ahead: bool,
) {
    let backend = MockTreetopBackend::new();
    for id in [1, 2, 3] {
        if id == 2 && deny_look_ahead {
            continue;
        }
        backend.add_rule(MockAllowRule {
            group_id: 7,
            action: Permissions::ReadCollection,
            resource_kind: ResourceKind::Collection,
            resource_id: Some(id),
            attrs: ResourceFields::default(),
        });
    }
    let principal = PrincipalRef::new(1, [7]);
    // With exact totals the continuation falls at the full batch boundary;
    // skipped totals use just the response plus one authorized look-ahead.
    let count = if include_total { 129 } else { 3 };
    let boundary = count - 1;
    let candidates = (1..=count)
        .map(|id| {
            let mut row = candidate_collection(id);
            row.description = if id == boundary {
                "b".repeat(50_000)
            } else if id == count {
                "c".to_string()
            } else {
                format!("a{id:03}")
            };
            row
        })
        .collect::<Vec<_>>();
    let query = crate::models::search::parse_query_parameter(&format!(
        "sort=description&limit=1&include_total={include_total}"
    ))
    .unwrap();
    let result = authorize_cursor_page_from_storage(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &query,
        |query| std::future::ready(paginate_in_memory(candidates.clone(), &query)),
        |collection| ResourceRef::collection(collection.id),
    )
    .await;
    if include_total || deny_look_ahead {
        assert!(
            matches!(result, Err(ApiError::BadRequest(message)) if message.contains("maximum encoded size"))
        );
    } else {
        let page = finalize_page(result.unwrap().rows, &query).unwrap();
        assert_eq!(page.items[0].id, 1);
        assert!(page.next_cursor.is_some());
    }
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
            attrs: ResourceFields::default(),
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
        attrs: ResourceFields::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(5),
        attrs: ResourceFields::default(),
    });
    let principal = PrincipalRef::new(1, [7]);

    let authorized = authorize_all_candidates(
        &backend,
        &principal,
        vec![11],
        None,
        vec![Permissions::ReadClass, Permissions::ReadCollection],
        |class_id| ResourceRef::class(*class_id, 5, None),
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
        attrs: ResourceFields::default(),
    });
    backend.add_rule(MockAllowRule {
        group_id: 7,
        action: Permissions::ReadCollection,
        resource_kind: ResourceKind::Collection,
        resource_id: Some(5),
        attrs: ResourceFields::default(),
    });
    let principal = PrincipalRef::new(1, [7]);
    let class_resource = ResourceRef::class(11, 5, None);

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
        attrs: ResourceFields::default(),
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
        candidate_limit: crate::permissions::CompleteCollectionCandidateLimit,
    ) -> Result<Vec<Collection>, ApiError> {
        self.inner
            .collections_user_can(principal, permissions, candidate_limit)
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

#[rstest::rstest]
#[case::all_denied(false)]
#[case::resource_scope(true)]
#[actix_web::test]
async fn storage_pages_continue_through_denied_candidates(#[case] scoped: bool) {
    use crate::models::{TokenResourceScope, TokenScope};
    let backend = MockTreetopBackend::new();
    if scoped {
        allow_all_collection_reads(&backend);
    }
    let scope = TokenScope::from_request_parts(
        None,
        Some(vec![
            TokenResourceScope::Collection(CollectionID::new(299).unwrap()),
            TokenResourceScope::Collection(CollectionID::new(300).unwrap()),
        ]),
    )
    .unwrap();
    let principal = PrincipalRef::new(1, [7]);
    let query = QueryOptions::new(vec![], vec![], Some(1), None, false).unwrap();
    let (page, fetched) = crate::permissions::visibility::capture_candidate_fetches(
        authorize_cursor_page_from_storage(
            &backend,
            &principal,
            if scoped { scope.as_ref() } else { None },
            vec![Permissions::ReadCollection],
            &query,
            |query| {
                std::future::ready(paginate_in_memory(
                    (1..=300).map(candidate_collection).collect(),
                    &query,
                ))
            },
            |collection| ResourceRef::collection(collection.id),
        ),
    )
    .await;
    let page = page.unwrap();
    assert_eq!(
        page.rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        if scoped { vec![299, 300] } else { vec![] }
    );
    assert_eq!(fetched.len(), 150);
    assert!(fetched.iter().all(|count| *count <= 3));
    assert_eq!(
        backend.authorization_batch_sizes().iter().sum::<usize>(),
        if scoped { 2 } else { 300 }
    );
}

#[actix_test]
async fn exact_total_rejects_mismatched_cursor_even_without_candidates() {
    let backend = MockTreetopBackend::new();
    let principal = PrincipalRef::new(1, [7]);
    let mut query = crate::models::search::parse_query_parameter("sort=name&limit=1").unwrap();
    let wrong_cursor =
        crate::pagination::encode_cursor(&candidate_collection(1), &Collection::default_sort())
            .unwrap();
    query.set_cursor(Some(wrong_cursor)).unwrap();
    let result = authorize_cursor_page_from_storage::<Collection, _, _, _>(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &query,
        |_| async { panic!("invalid cursors must fail before fetching") },
        |collection| ResourceRef::collection(collection.id),
    )
    .await;
    assert!(matches!(result, Err(ApiError::BadRequest(_))));
}

#[actix_test]
async fn exact_totals_retain_only_a_candidate_batch_and_response_page() {
    use crate::models::search::{FilterField, SortParam};
    use crate::traits::{CursorPaginated, CursorValue};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Tracked {
        collection: Collection,
        live: Arc<AtomicUsize>,
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.live.fetch_sub(1, Ordering::SeqCst);
        }
    }
    impl CursorPaginated for Tracked {
        fn supports_sort(field: &FilterField) -> bool {
            Collection::supports_sort(field)
        }
        fn default_sort() -> Vec<SortParam> {
            Collection::default_sort()
        }
        fn tie_breaker_sort() -> Vec<SortParam> {
            Collection::tie_breaker_sort()
        }
        fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
            self.collection.cursor_value(field)
        }
    }
    let backend = MockTreetopBackend::new();
    allow_all_collection_reads(&backend);
    let principal = PrincipalRef::new(1, [7]);
    let live = Arc::new(AtomicUsize::new(0));
    let peak = AtomicUsize::new(0);
    let query = QueryOptions::new(vec![], vec![], Some(3), None, true).unwrap();
    let page = authorize_cursor_page_from_storage(
        &backend,
        &principal,
        None,
        vec![Permissions::ReadCollection],
        &query,
        |query| {
            let rows =
                paginate_in_memory((1..=700).map(candidate_collection).collect(), &query).unwrap();
            peak.fetch_max(
                live.fetch_add(rows.len(), Ordering::SeqCst) + rows.len(),
                Ordering::SeqCst,
            );
            std::future::ready(Ok(rows
                .into_iter()
                .map(|collection| Tracked {
                    collection,
                    live: live.clone(),
                })
                .collect()))
        },
        |row: &Tracked| ResourceRef::collection(row.collection.id),
    )
    .await
    .unwrap();
    assert_eq!(page.total_count, 700);
    assert_eq!(live.load(Ordering::SeqCst), 4);
    assert_eq!(peak.load(Ordering::SeqCst), 133);
    assert_eq!(
        backend.authorization_batch_sizes(),
        vec![128, 128, 128, 128, 128, 60]
    );
    drop(page);
    assert_eq!(live.load(Ordering::SeqCst), 0);
}
