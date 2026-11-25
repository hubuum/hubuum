use super::*;

#[rstest::rstest]
#[case(0)]
#[case(1)]
#[actix_web::test]
async fn every_available_storage_backend_limits_relation_id_queries(
    #[case] limit: u32,
    #[values("class_between", "class_touching", "object_between")] kind: &str,
) {
    let _permit = postgres_permit().await;
    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("limited_relations")).await;
        let ids = if kind == "object_between" {
            fixture
                .objects
                .iter()
                .map(|object| ResourceId::new(object.id().id()).unwrap())
                .collect::<Vec<_>>()
        } else {
            fixture
                .classes
                .iter()
                .map(|class| ResourceId::new(class.id().id()).unwrap())
                .collect::<Vec<_>>()
        };
        let query = StorageRelationIdsQuery::new(
            ids,
            StorageVisibility::new(
                principal_id(i32::MAX),
                true,
                None::<Vec<StorageAuthorizationPermission>>,
                None,
            ),
        )
        .with_max_results(limit);
        let count = match kind {
            "class_between" => backend
                .list_class_relations_between_ids(query)
                .await
                .unwrap()
                .len(),
            "class_touching" => backend
                .list_class_relations_touching_ids(query)
                .await
                .unwrap()
                .len(),
            "object_between" => backend
                .list_object_relations_between_ids(query)
                .await
                .unwrap()
                .len(),
            _ => unreachable!(),
        };
        assert_eq!(count, limit as usize);
        delete_backend_relation_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_relation_queries() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let fixture = create_backend_relation_fixture(&backend, &prefix("relation_query")).await;
        let class_one_id = ResourceId::new(fixture.classes[0].id().id()).unwrap();
        let class_two_id = ResourceId::new(fixture.classes[1].id().id()).unwrap();
        let class_two_typed_id = fixture.classes[1].id();
        let class_relation_id = fixture.class_relation_id;
        let object_one_id = fixture.objects[0].id();
        let object_one_resource_id = ResourceId::new(object_one_id.id()).unwrap();
        let object_two_resource_id = ResourceId::new(fixture.objects[1].id().id()).unwrap();
        let object_relation_id = fixture.object_relation_id;
        let visibility = || {
            StorageVisibility::new(
                principal_id(i32::MAX),
                true,
                None::<Vec<StorageAuthorizationPermission>>,
                None,
            )
        };
        let options = || {
            QueryOptions::new(Vec::new(), Vec::new(), Some(50), None, true)
                .expect("contract query must be valid")
        };

        let (class_relations, class_total) = backend
            .list_class_relations(StorageRelationListQuery::new(options(), visibility()))
            .await
            .expect("certified backend should list class relations")
            .into_parts();
        assert!(class_total.is_some_and(|total| total >= 1));
        assert!(class_relations.into_iter().any(|row| {
            let id = row.into_parts().id();
            id == class_relation_id
        }));

        let (object_relations, object_total) = backend
            .list_object_relations(StorageRelationListQuery::new(options(), visibility()))
            .await
            .expect("certified backend should list object relations")
            .into_parts();
        assert!(object_total.is_some_and(|total| total >= 1));
        assert!(object_relations.into_iter().any(|row| {
            let (id, ..) = row.into_parts();
            id == object_relation_id
        }));

        let (touching_classes, _) = backend
            .list_class_relations_touching(StorageRelationTouchingQuery::new(
                class_one_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should list class relations touching an id")
            .into_parts();
        assert_eq!(touching_classes.len(), 1);

        let (touching_objects, _) = backend
            .list_object_relations_touching(StorageRelationTouchingQuery::new(
                object_one_resource_id,
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should list object relations touching an id")
            .into_parts();
        assert_eq!(touching_objects.len(), 1);

        let class_ids = [class_one_id, class_two_id];
        assert_eq!(
            backend
                .list_class_relations_touching_ids(StorageRelationIdsQuery::new(
                    class_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query class relations touching ids")
                .len(),
            1
        );
        assert_eq!(
            backend
                .list_class_relations_between_ids(StorageRelationIdsQuery::new(
                    class_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query class relations between ids")
                .len(),
            1
        );

        let object_ids = [object_one_resource_id, object_two_resource_id];
        assert_eq!(
            backend
                .list_object_relations_touching_ids(StorageObjectRelationsTouchingIdsQuery::new(
                    [object_one_id],
                    10,
                    visibility(),
                ))
                .await
                .expect("certified backend should query object relations touching ids")
                .len(),
            1
        );
        assert!(
            backend
                .list_object_relations_touching_ids(
                    StorageObjectRelationsTouchingIdsQuery::new([object_one_id], 10, visibility(),)
                        .excluding_relation_ids([object_relation_id]),
                )
                .await
                .expect("certified backend should exclude previously visited relations")
                .is_empty()
        );
        assert_eq!(
            backend
                .list_object_relations_between_ids(StorageRelationIdsQuery::new(
                    object_ids,
                    visibility(),
                ))
                .await
                .expect("certified backend should query object relations between ids")
                .len(),
            1
        );

        let (list_related_classes, _) = backend
            .list_related_classes(StorageRelationGraphQuery::new(
                class_one_id,
                hubuum_query::TraversalBudget::new(1, 100).unwrap(),
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should traverse related classes")
            .into_parts();
        assert!(!list_related_classes.is_empty());

        let (list_related_objects, _) = backend
            .list_related_objects(StorageRelationGraphQuery::new(
                object_one_resource_id,
                hubuum_query::TraversalBudget::new(1, 100).unwrap(),
                options(),
                visibility(),
            ))
            .await
            .expect("certified backend should traverse related objects")
            .into_parts();
        assert!(!list_related_objects.is_empty());

        let included = backend
            .list_related_objects_for_roots(
                StorageRelatedObjectsForRootsQuery::new(
                    [object_one_id],
                    class_two_typed_id,
                    visibility(),
                )
                .class_relation_id(Some(class_relation_id))
                .direction(StorageRelatedDirection::Any)
                .sort(StorageRelatedSort::Path)
                .traversal_budget(hubuum_query::TraversalBudget::new(1, 100).unwrap())
                .limit(10),
            )
            .await
            .expect("certified backend should traverse directional root graphs");
        assert_eq!(included.len(), 1);

        let bidirectional = backend
            .list_bidirectionally_related_objects_for_roots(
                StorageBidirectionalRelatedObjectsQuery::new(
                    [object_one_id],
                    hubuum_query::TraversalBudget::new(1, 100).unwrap(),
                    10,
                    false,
                    visibility(),
                ),
            )
            .await
            .expect("certified backend should traverse bidirectional root graphs");
        assert_eq!(bidirectional.len(), 1);
        delete_backend_relation_fixture(&backend, fixture).await;
    }
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_ranked_unified_search() {
    let _permit = postgres_permit().await;

    for backend in available_backends() {
        let needle = prefix("unified_search");
        let fixture = create_backend_object_fixture(
            &backend,
            &needle,
            vec![serde_json::json!({"needle": needle})],
        )
        .await;
        let request = || {
            StorageUnifiedSearchQuery::new(
                needle.clone(),
                StorageCandidatePageLimit::try_new(10)
                    .expect("contract candidate page limit should be valid"),
                StorageVisibility::new(
                    principal_id(i32::MAX),
                    true,
                    None::<Vec<StorageAuthorizationPermission>>,
                    None,
                ),
            )
            .search_extended_document(true)
        };

        let collections = backend
            .search_collections(request())
            .await
            .expect("certified backend should search collections")
            .into_parts()
            .0;
        assert!(collections.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id == fixture.collection.id()
        }));

        let classes = backend
            .search_classes(request())
            .await
            .expect("certified backend should search classes")
            .into_parts()
            .0;
        assert!(classes.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id == fixture.class.id()
        }));

        let objects = backend
            .search_objects(request())
            .await
            .expect("certified backend should search objects")
            .into_parts()
            .0;
        assert!(objects.into_iter().any(|row| {
            let (row, _) = row.into_parts();
            let (id, ..) = row.into_parts();
            id == fixture.objects[0].id()
        }));
        delete_backend_object_fixture(&backend, fixture).await;
    }
}
