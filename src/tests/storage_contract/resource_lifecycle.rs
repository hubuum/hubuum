use super::*;

#[actix_web::test]
async fn every_available_storage_backend_supplies_collection_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let command = StorageCollectionCreate::new(
            prefix("collection_lifecycle"),
            "collection lifecycle",
            group_id(group.id),
            None,
        );
        let collections = backend.collection_store();
        let created = backend
            .collection_store()
            .create_collection(command, &EventContext::system())
            .await
            .expect("certified backend should create collections")
            .into_value();
        let updated = collections
            .update_collection(
                created.id(),
                StorageCollectionUpdate::new(
                    None,
                    Some("updated collection lifecycle".to_string()),
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update collections")
            .into_value();
        assert_eq!(updated.description(), "updated collection lifecycle");
        let moved = collections
            .move_collection(created.id(), collection_id(1), &EventContext::system())
            .await
            .expect("certified backend should move collections")
            .into_value();
        assert_eq!(moved.parent_collection_id(), Some(collection_id(1)));
        let loaded = collections
            .get_collection(created.id())
            .await
            .expect("certified backend should load collections by id");
        assert_eq!(loaded, moved);
        let children = collections
            .list_collection_children(collection_id(1))
            .await
            .expect("certified backend should list direct collection children");
        assert!(children.iter().any(|child| child.id() == created.id()));
        let ancestors = collections
            .list_collection_ancestors(created.id())
            .await
            .expect("certified backend should list collection ancestors");
        assert_eq!(
            ancestors.first().map(|ancestor| ancestor.id()),
            Some(collection_id(1))
        );
        collections
            .delete_collection(created.id(), &EventContext::system())
            .await
            .expect("certified backend should delete collections")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("collection record compatibility group should be removed");
}

#[actix_web::test]
async fn memory_storage_rejects_schema_validation_without_a_schema_as_bad_request() {
    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let classes = backend.class_store();
    let context = EventContext::system();
    let created = classes
        .create_class(
            StorageClassCreate::builder("schema_less_class", collection_id(1), "").build(),
            &context,
        )
        .await
        .expect("memory adapter should create a schema-less class")
        .into_value();
    let target = classes
        .resolve_class(StorageClassSelector::Id(created.id()))
        .await
        .expect("memory adapter should resolve the class");

    let error = classes
        .update_class(
            &target,
            StorageClassUpdate::builder()
                .validate_schema(Some(true))
                .build(),
            &context,
        )
        .await
        .err()
        .expect("memory adapter must reject validation without a schema");

    assert_eq!(error.kind(), StorageErrorKind::InvalidInput);
    let api_error = ApiError::from(error);
    assert_eq!(api_error.status_code(), http::StatusCode::BAD_REQUEST);
    assert_eq!(
        api_error.public_message(),
        "Class schema validation cannot be enabled without a schema"
    );
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_class_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("class_lifecycle_collection"),
                    "class lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the class collection")
            .into_value();
        let classes = backend.class_store();
        let class_name = prefix("class_lifecycle");
        let created = classes
            .create_class(
                StorageClassCreate::builder(&class_name, collection.id(), "class lifecycle")
                    .schema_policy(StorageClassSchemaPolicy::Enforced(serde_json::json!({
                        "type": "object",
                        "properties": {"name": {"type": "string"}}
                    })))
                    .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create classes")
            .into_value();
        assert_eq!(created.collection_id(), collection.id());
        assert!(created.validates_schema());

        let resolved_by_id = classes
            .resolve_class(StorageClassSelector::Id(created.id()))
            .await
            .expect("certified backend should resolve classes by id");
        assert_eq!(resolved_by_id.class().name(), class_name);
        let resolved_by_name = classes
            .resolve_class(StorageClassSelector::Name(class_name))
            .await
            .expect("certified backend should resolve classes by name");
        assert_eq!(resolved_by_name.class().id(), created.id());

        let updated = classes
            .update_class(
                &resolved_by_id,
                StorageClassUpdate::builder()
                    .description(Some("updated class lifecycle".to_string()))
                    .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should update classes")
            .into_value();
        assert_eq!(updated.description(), "updated class lifecycle");
        assert_eq!(
            classes
                .resolve_class_names(vec![created.id(), created.id()])
                .await
                .expect("certified backend should resolve a complete class-name set"),
            vec![(created.id(), updated.name().to_string())]
        );
        let missing = classes
            .resolve_class_names(vec![
                created.id(),
                ClassId::new(i32::MAX).expect("maximum class id is positive"),
            ])
            .await
            .expect_err("certified backend must reject a partial class-name mapping");
        assert_eq!(missing.kind(), StorageErrorKind::NotFound);

        let updated_target = classes
            .resolve_class(StorageClassSelector::Id(updated.id()))
            .await
            .expect("certified backend should resolve the updated class");
        classes
            .delete_class(&updated_target, &EventContext::system())
            .await
            .expect("certified backend should delete classes")
            .into_value();
        assert_eq!(
            classes
                .resolve_class(StorageClassSelector::Id(updated.id()))
                .await
                .err()
                .expect("deleted classes must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("class lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("class lifecycle group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_object_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("object_lifecycle_collection"),
                    "object lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the object collection")
            .into_value();
        let classes = backend.class_store();
        let class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("object_lifecycle_class"),
                    collection.id(),
                    "object lifecycle class",
                )
                .schema_policy(StorageClassSchemaPolicy::Enforced(serde_json::json!({
                    "type": "object",
                    "properties": {"value": {"type": "integer"}}
                })))
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the object class")
            .into_value();
        let resolved_class = classes
            .resolve_class(StorageClassSelector::Id(class.id()))
            .await
            .expect("certified backend should resolve the object class");
        let objects = backend.object_store();
        let object_name = prefix("object_lifecycle");
        let create = StorageObjectCreate::new(
            &object_name,
            collection.id(),
            class.id(),
            serde_json::json!({"value": 1}),
            "object lifecycle",
        );
        objects
            .validate_object_create(create.clone())
            .await
            .expect("certified backend should validate object creation");
        let created = objects
            .create_object(&resolved_class, create, &EventContext::system())
            .await
            .expect("certified backend should create objects")
            .into_value();
        objects
            .validate_object(created.clone())
            .await
            .expect("certified backend should validate stored objects");

        let loaded = objects
            .get_object(created.id())
            .await
            .expect("certified backend should load objects by id");
        assert_eq!(loaded.object(), &created);
        let resolved = objects
            .resolve_object(StorageObjectSelector::Names {
                class_name: class.name().to_string(),
                object_name: object_name.clone(),
            })
            .await
            .expect("certified backend should resolve objects by name");
        assert_eq!(resolved.object().id(), created.id());

        let update = StorageObjectUpdate::builder()
            .description(Some("updated object lifecycle".to_string()))
            .build();
        objects
            .validate_object_update(created.id(), update.clone())
            .await
            .expect("certified backend should validate object updates");
        let updated = objects
            .update_object(&resolved, update, &EventContext::system())
            .await
            .expect("certified backend should update objects")
            .into_value();
        assert_eq!(updated.description(), "updated object lifecycle");

        let patch_document: crate::models::ObjectDataPatchDocument =
            serde_json::from_value(serde_json::json!([
                {"op": "replace", "path": "/value", "value": 2}
            ]))
            .expect("object lifecycle patch should be valid");
        let patch: StorageObjectDataPatch = patch_document.into();
        let updated_target = objects
            .get_object(created.id())
            .await
            .expect("updated object should remain resolvable");
        let patched = objects
            .patch_object_data(&updated_target, patch, &EventContext::system())
            .await
            .expect("certified backend should patch object data")
            .into_value();
        assert_eq!(patched.data(), &serde_json::json!({"value": 2}));

        let delete_target = objects
            .get_object(created.id())
            .await
            .expect("patched object should remain resolvable");
        objects
            .delete_object(&delete_target, &EventContext::system())
            .await
            .expect("certified backend should delete objects")
            .into_value();
        assert_eq!(
            objects
                .get_object(created.id())
                .await
                .err()
                .expect("deleted objects must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        classes
            .delete_class(&resolved_class, &EventContext::system())
            .await
            .expect("object lifecycle class should be removable")
            .into_value();
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("object lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("object lifecycle group should be removed");
}

#[actix_web::test]
async fn every_available_storage_backend_supplies_relation_lifecycle() {
    let _permit = postgres_permit().await;
    let pool = pool();
    let group = crate::tests::create_test_group(pool.get_ref()).await;

    for backend in available_backends() {
        let collections = backend.collection_store();
        let collection = collections
            .create_collection(
                StorageCollectionCreate::new(
                    prefix("relation_lifecycle_collection"),
                    "relation lifecycle collection",
                    group_id(group.id),
                    None,
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the relation collection")
            .into_value();
        let classes = backend.class_store();
        let from_class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("relation_lifecycle_from_class"),
                    collection.id(),
                    "relation lifecycle source class",
                )
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the source class")
            .into_value();
        let to_class = classes
            .create_class(
                StorageClassCreate::builder(
                    prefix("relation_lifecycle_to_class"),
                    collection.id(),
                    "relation lifecycle target class",
                )
                .build(),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the target class")
            .into_value();

        let class_relations = backend.class_relation_store();
        let prepared_class_relation = class_relations
            .prepare_class_relation(
                StorageClassRelationCreate::builder(from_class.id(), to_class.id()).build(),
            )
            .await
            .expect("certified backend should prepare class relations");
        assert_eq!(prepared_class_relation.from_class().id(), from_class.id());
        assert_eq!(prepared_class_relation.to_class().id(), to_class.id());
        let class_relation = class_relations
            .create_class_relation(&prepared_class_relation, &EventContext::system())
            .await
            .expect("certified backend should create class relations")
            .into_value();
        let class_relation_id = ClassRelationId::from(class_relation.relation().metadata().id());
        let resolved_class_relation = class_relations
            .resolve_class_relation(class_relation_id)
            .await
            .expect("certified backend should resolve class relations");
        assert_eq!(
            resolved_class_relation.relation(),
            class_relation.relation()
        );

        let objects = backend.object_store();
        let from_object = objects
            .create_object(
                &classes
                    .resolve_class(StorageClassSelector::Id(from_class.id()))
                    .await
                    .expect("source class should resolve"),
                StorageObjectCreate::new(
                    prefix("relation_lifecycle_from_object"),
                    collection.id(),
                    from_class.id(),
                    serde_json::json!({}),
                    "relation lifecycle source object",
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the source object")
            .into_value();
        let to_object = objects
            .create_object(
                &classes
                    .resolve_class(StorageClassSelector::Id(to_class.id()))
                    .await
                    .expect("target class should resolve"),
                StorageObjectCreate::new(
                    prefix("relation_lifecycle_to_object"),
                    collection.id(),
                    to_class.id(),
                    serde_json::json!({}),
                    "relation lifecycle target object",
                ),
                &EventContext::system(),
            )
            .await
            .expect("certified backend should create the target object")
            .into_value();

        let object_relations = backend.object_relation_store();
        let prepared_object_relation = object_relations
            .prepare_object_relation(StorageObjectRelationCreateSelector::Explicit(
                StorageObjectRelationCreate::new(
                    from_object.id(),
                    to_object.id(),
                    class_relation_id,
                ),
            ))
            .await
            .expect("certified backend should prepare object relations");
        assert_eq!(prepared_object_relation.from_object(), &from_object);
        assert_eq!(prepared_object_relation.to_object(), &to_object);
        let object_relation = object_relations
            .create_object_relation(&prepared_object_relation, &EventContext::system())
            .await
            .expect("certified backend should create object relations")
            .into_value();
        let object_relation_id = ObjectRelationId::from(object_relation.relation().metadata().id());
        let resolved_object_relation = object_relations
            .resolve_object_relation(StorageObjectRelationSelector::Id(object_relation_id))
            .await
            .expect("certified backend should resolve object relations");
        assert_eq!(
            resolved_object_relation.relation(),
            object_relation.relation()
        );

        object_relations
            .delete_object_relation(&resolved_object_relation, &EventContext::system())
            .await
            .expect("certified backend should delete object relations")
            .into_value();
        assert_eq!(
            object_relations
                .resolve_object_relation(StorageObjectRelationSelector::Id(object_relation_id))
                .await
                .err()
                .expect("deleted object relations must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        for object in [from_object, to_object] {
            let target = objects
                .get_object(object.id())
                .await
                .expect("relation lifecycle object should resolve for cleanup");
            objects
                .delete_object(&target, &EventContext::system())
                .await
                .expect("relation lifecycle object should be removable")
                .into_value();
        }
        class_relations
            .delete_class_relation(&resolved_class_relation, &EventContext::system())
            .await
            .expect("certified backend should delete class relations")
            .into_value();
        assert_eq!(
            class_relations
                .resolve_class_relation(class_relation_id)
                .await
                .err()
                .expect("deleted class relations must not resolve")
                .kind(),
            StorageErrorKind::NotFound
        );
        for class in [from_class, to_class] {
            let target = classes
                .resolve_class(StorageClassSelector::Id(class.id()))
                .await
                .expect("relation lifecycle class should resolve for cleanup");
            classes
                .delete_class(&target, &EventContext::system())
                .await
                .expect("relation lifecycle class should be removable")
                .into_value();
        }
        collections
            .delete_collection(collection.id(), &EventContext::system())
            .await
            .expect("relation lifecycle collection should be removable")
            .into_value();
    }

    group
        .delete_without_events(pool.get_ref())
        .await
        .expect("relation lifecycle group should be removed");
}
