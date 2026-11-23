use super::*;

#[actix_web::test]
async fn class_storage_query_budget_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_point_read")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_point_read"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class point read query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");

    let (loaded, queries) = capture_queries(services.classes().resolve(ClassSelector::by_id(
        HubuumClassID::new(class.id).expect("valid class id"),
    )))
    .await;
    assert_eq!(loaded.expect("class should load").class(), &class);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    class
        .delete_without_events(&scope.pool)
        .await
        .expect("class fixture cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_class_create").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let command = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_create"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class create query budget".to_string(),
    };

    let (created, queries) =
        capture_queries(services.classes().create(command, &EventContext::system())).await;
    let created = created.expect("class should save with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumclass\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 2);

    created
        .delete_without_events(&scope.pool)
        .await
        .expect("class cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_storage_query_budget_no_op_avoids_writes_and_events() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_class_no_op").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_no_op"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class no-op query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let update = UpdateHubuumClass {
        name: Some(class.name.clone()),
        collection_id: Some(class.collection_id),
        json_schema: None,
        validate_schema: Some(class.validate_schema),
        description: Some(class.description.clone()),
    };

    let (updated, queries) = capture_queries(services.classes().update(
        &target,
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(updated.expect("no-op update should succeed"), class);
    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
    assert_eq!(queries.queries_matching("UPDATE \"hubuumclass\""), 0);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 0);

    class
        .delete_without_events(&scope.pool)
        .await
        .expect("class cleanup");
    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_point_resolution_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_point")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_point_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation point budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_point_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation point budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(from_class.id).unwrap(),
        to_hubuum_class_id: ClassId::new(to_class.id).unwrap(),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("relation fixture should save");

    let (loaded, queries) = capture_queries(services.class_relations().resolve(relation.id)).await;
    assert_eq!(
        loaded.expect("relation should resolve").relation(),
        &relation
    );
    assert_eq!(queries.total_queries(), 2, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 2);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass_relation\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_create")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_create_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation create budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_create_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation create budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let prepared = services
        .class_relations()
        .prepare_create(NewHubuumClassRelation {
            from_hubuum_class_id: ClassId::new(from_class.id).unwrap(),
            to_hubuum_class_id: ClassId::new(to_class.id).unwrap(),
            forward_label: None,
            reverse_label: None,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        })
        .await
        .expect("relation should prepare");

    let (created, queries) = capture_queries(
        services
            .class_relations()
            .create(&prepared, &EventContext::system()),
    )
    .await;
    created.expect("relation should create with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumclass_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn class_relation_storage_query_budget_delete_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_class_relation_delete")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let from_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_delete_from"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation delete budget from class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("from class fixture should save");
    let to_class = NewHubuumClass {
        name: scope.scoped_name("query_budget_class_relation_delete_to"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "class relation delete budget to class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("to class fixture should save");
    let prepared = services
        .class_relations()
        .prepare_create(NewHubuumClassRelation {
            from_hubuum_class_id: ClassId::new(from_class.id).unwrap(),
            to_hubuum_class_id: ClassId::new(to_class.id).unwrap(),
            forward_label: None,
            reverse_label: None,
            forward_template_alias: None,
            reverse_template_alias: None,
            from_max_relations: None,
            to_max_relations: None,
        })
        .await
        .expect("relation should prepare");
    let target = services
        .class_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("relation should create");

    let (deleted, queries) = capture_queries(
        services
            .class_relations()
            .delete(&target, &EventContext::system()),
    )
    .await;
    deleted.expect("relation should delete with an event");
    assert_eq!(queries.total_queries(), 7, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 5);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("DELETE FROM \"hubuumclass_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_point_read_uses_one_query() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_point_read")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_point_read_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object point read query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let object = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_point_read"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object point read query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("object fixture should save");

    let (loaded, queries) = capture_queries(services.objects().resolve(ObjectSelector::by_id(
        HubuumClassID::new(class.id).expect("valid class id"),
        HubuumObjectID::new(object.id).expect("valid object id"),
    )))
    .await;
    assert_eq!(loaded.expect("object should load").object(), &object);
    assert_eq!(queries.total_queries(), 1, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 1);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INNER JOIN \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_object_create").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_create_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object create query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let class_target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let command = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_create"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object create query budget".to_string(),
    };

    let (created, queries) = capture_queries(services.objects().create(
        &class_target,
        command,
        &EventContext::system(),
    ))
    .await;
    created.expect("object should save with an event");
    assert_eq!(queries.total_queries(), 10, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 8);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_transaction_reuses_the_direct_create_round_trip_budget() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_transaction_create")
        .await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let storage = StorageHandle::postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_transaction_create_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object transaction query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let class_target = services
        .classes()
        .resolve(ClassSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
        ))
        .await
        .expect("class fixture should resolve");
    let storage_class = class_target.as_storage().clone();
    let command = StorageObjectCreate::try_from(NewHubuumObject {
        name: scope.scoped_name("query_budget_object_transaction_create"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object transaction query budget".to_string(),
    })
    .expect("valid fixture");

    let (created, queries) = capture_queries(storage.with_transaction(
        EventContext::system(),
        move |transaction| {
            Box::pin(async move { transaction.objects().create(&storage_class, command).await })
        },
    ))
    .await;
    let _ = created
        .expect("transactional object should save with an event")
        .into_value();
    assert_eq!(queries.total_queries(), 10, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 8);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_storage_query_budget_no_op_avoids_object_writes_and_events() {
    let scope = TestScope::new();
    let fixture = scope.collection_fixture("query_budget_object_no_op").await;
    let services = crate::tests::services_for_postgres(scope.pool.get_ref().clone());
    let class = NewHubuumClass {
        name: scope.scoped_name("query_budget_object_no_op_class"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: None,
        description: "object no-op query budget class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class fixture should save");
    let object = NewHubuumObject {
        name: scope.scoped_name("query_budget_object_no_op"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"value": 1}),
        description: "object no-op query budget".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("object fixture should save");
    let target = services
        .objects()
        .resolve(ObjectSelector::by_id(
            HubuumClassID::new(class.id).expect("valid class id"),
            HubuumObjectID::new(object.id).expect("valid object id"),
        ))
        .await
        .expect("object fixture should resolve");
    let update = UpdateHubuumObject {
        name: Some(object.name.clone()),
        collection_id: Some(object.collection_id),
        hubuum_class_id: Some(object.hubuum_class_id),
        data: Some(object.data.clone()),
        description: Some(object.description.clone()),
    };

    let (updated, queries) = capture_queries(services.objects().update(
        &target,
        update,
        &EventContext::system(),
    ))
    .await;
    assert_eq!(updated.expect("no-op update should succeed"), object);
    assert_eq!(queries.total_queries(), 9, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 7);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("SELECT hubuum_assert_revision_precondition"),
        1
    );
    assert_eq!(queries.queries_matching("UPDATE \"hubuumobject\""), 0);
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 0);

    fixture.cleanup().await.expect("collection fixture cleanup");
}

#[actix_web::test]
async fn object_relation_create_has_a_fixed_query_and_checkout_budget() {
    let scope = TestScope::new();
    let fixture = scope
        .collection_fixture("query_budget_object_relation")
        .await;
    let class_one = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_relation_class_one"),
        description: "query budget relation class one".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first class should save");
    let class_two = NewHubuumClass {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("query_budget_relation_class_two"),
        description: "query budget relation class two".to_string(),
        json_schema: None,
        validate_schema: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second class should save");
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: ClassId::new(class_one.id).unwrap(),
        to_hubuum_class_id: ClassId::new(class_two.id).unwrap(),
        forward_label: None,
        reverse_label: None,
        forward_template_alias: Some(TemplateAlias::new("seconds").unwrap()),
        reverse_template_alias: Some(TemplateAlias::new("firsts").unwrap()),
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .expect("class relation should save");
    let object_one = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_one.id,
        name: scope.scoped_name("query_budget_relation_object_one"),
        description: "query budget relation object one".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("first object should save");
    let object_two = NewHubuumObject {
        collection_id: fixture.collection.id,
        hubuum_class_id: class_two.id,
        name: scope.scoped_name("query_budget_relation_object_two"),
        description: "query budget relation object two".to_string(),
        data: serde_json::json!({}),
    }
    .save_without_events(&scope.pool)
    .await
    .expect("second object should save");

    let relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_one.id,
        to_hubuum_object_id: object_two.id,
        class_relation_id: class_relation.id.id(),
    };
    let (saved, queries) =
        capture_queries(relation.save(&scope.pool, &EventContext::system())).await;
    saved.expect("object relation should save with an event");

    assert_eq!(queries.total_queries(), 9, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 7, "{:#?}", queries.query_counts());
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 2);
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 2);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_preparation_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_prepare").await;

    let (prepared, queries) =
        capture_queries(services.object_relations().prepare_create(selector)).await;
    prepared.expect("object relation should prepare");
    assert_eq!(queries.total_queries(), 3, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 3);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass_relation\""), 1);
    assert_eq!(queries.queries_matching("FROM \"hubuumclass\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_point_resolution_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_resolve").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");
    let created = services
        .object_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("object relation should create");

    let (resolved, queries) = capture_queries(services.object_relations().resolve(
        ObjectRelationSelector::by_id(
            HubuumObjectRelationID::new(created.relation().id).expect("valid object relation id"),
        ),
    ))
    .await;
    assert_eq!(
        resolved.expect("object relation should resolve").relation(),
        created.relation()
    );
    assert_eq!(queries.total_queries(), 4, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 0);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("FROM \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("FROM \"hubuumobject\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_create_with_event_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_service_create").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");

    let (created, queries) = capture_queries(
        services
            .object_relations()
            .create(&prepared, &EventContext::system()),
    )
    .await;
    created.expect("object relation should create with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("INSERT INTO \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}

#[actix_web::test]
async fn object_relation_storage_query_budget_delete_with_event_is_fixed() {
    let scope = TestScope::new();
    let (fixture, services, selector) =
        object_relation_budget_fixture(&scope, "query_budget_object_relation_service_delete").await;
    let prepared = services
        .object_relations()
        .prepare_create(selector)
        .await
        .expect("object relation should prepare");
    let target = services
        .object_relations()
        .create(&prepared, &EventContext::system())
        .await
        .expect("object relation should create");

    let (deleted, queries) = capture_queries(
        services
            .object_relations()
            .delete(&target, &EventContext::system()),
    )
    .await;
    deleted.expect("object relation should delete with an event");
    assert_eq!(queries.total_queries(), 6, "{:#?}", queries.query_counts());
    assert_eq!(queries.domain_queries(), 4);
    assert_eq!(queries.control_queries(), 2);
    assert_eq!(queries.connection_checkouts(), 1);
    assert_eq!(
        queries.queries_matching("DELETE FROM \"hubuumobject_relation\""),
        1
    );
    assert_eq!(queries.queries_matching("INSERT INTO \"events\""), 1);

    fixture.cleanup().await.expect("fixture cleanup");
}
