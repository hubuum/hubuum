//! Application-level event catalog and lifecycle compatibility tests.

#![cfg(test)]

use uuid::Uuid;

use crate::events::{
    Action, ActorKind, CollectionId, CorrelationId, EntityType, Event, EventContext, EventEntityId,
    NewEvent, PrincipalId, RequestProvenance, TaskId,
};
use crate::models::class::{NewHubuumClass, UpdateHubuumClass};
use crate::models::collection::{NewCollectionWithAssignee, UpdateCollection, move_collection};
use crate::models::group::{NewGroup, UpdateGroup};
use crate::models::object::{NewHubuumObject, UpdateHubuumObject};
use crate::models::token::{renew_token_by_id_for_principal, revoke_token_by_id_for_principal};
use crate::models::{
    CollectionID, ExportContentType, ExportTemplateID, ExportTemplateKind, GroupID, HubuumClassID,
    HubuumClassRelationID, HubuumObjectID, NewExportTemplate, NewHubuumClassRelation,
    NewHubuumObjectRelation, NewUser, ObjectRelationLimit, Permissions, PermissionsList,
    PrincipalID, PrincipalToken, PrincipalTokenCreateRequest, Token, TokenID, TokenScope,
    UpdateExportTemplate, UpdateUser, UserID,
};
use crate::storage::{
    RemoteTargetStorage, StorageRemoteTargetCreate, StorageRemoteTargetDefinition,
    StorageRemoteTargetDelete, StorageRemoteTargetInvocation, StorageRemoteTargetPatch,
    StorageRemoteTargetPolicy, StorageRemoteTargetTransport, StorageRemoteTargetUpdate,
};
use crate::tests::{TestScope, create_test_user, test_scope};
use crate::traits::{CanDelete, CanSave, CanUpdate, GroupIdApplicationExt, PermissionController};
use hubuum_storage_postgres::PostgresStorage;

fn principal_id(id: i32) -> PrincipalId {
    PrincipalId::new(id).expect("test principal id must be positive")
}

fn correlation_id(value: &str) -> CorrelationId {
    CorrelationId::new(value).expect("test correlation ID must be valid")
}

fn event_from_storage(event: hubuum_storage_core::StorageRecordedEvent) -> Event {
    let (event, before_revision, after_revision) = event.into_parts();
    Event {
        id: event.id().get(),
        event_id: event.event_id(),
        occurred_at: event.occurred_at().naive_utc(),
        entity_type: event.entity_type().as_str().to_string(),
        entity_id: event.entity_id().map(EventEntityId::get),
        entity_name: event.entity_name().map(ToOwned::to_owned),
        collection_id: event.collection_id().map(CollectionId::id),
        action: event.action().as_str().to_string(),
        actor_user_id: event.actor_user_id().map(PrincipalId::id),
        actor_kind: event.actor_kind().as_str().to_string(),
        request_id: event.request_id(),
        correlation_id: event.correlation_id().map(ToOwned::to_owned),
        summary: event.summary().to_string(),
        before: event.before().cloned(),
        after: event.after().cloned(),
        metadata: event.metadata().clone(),
        schema_version: event.schema_version(),
        initiator_user_id: event
            .provenance()
            .initiator
            .as_ref()
            .map(|principal| principal.principal_id.id()),
        task_id: event.provenance().task_id.map(TaskId::id),
        before_revision,
        after_revision,
    }
}

#[test]
fn new_event_rejects_invalid_action_for_type() {
    // object_relation has no Updated per the catalog.
    let err = NewEvent::new(
        EntityType::ObjectRelation,
        Action::Updated,
        ActorKind::System,
        "bad pair",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        hubuum_events_core::EventCatalogError::InvalidActionForType { .. }
    ));
}

#[test]
fn new_event_accepts_a_validated_correlation_id() {
    let ev = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::User,
        "n",
    )
    .unwrap()
    .with_correlation_id(correlation_id("bounded-client-value-!@#$%"));
    assert_eq!(ev.correlation_id(), Some("bounded-client-value-!@#$%"));
}

#[test]
fn new_event_applies_event_context() {
    let request_id = Uuid::new_v4();
    let provenance = RequestProvenance::new(request_id, Some(correlation_id("client-correlation")));
    let context = provenance.user_event_context(42);

    let ev = NewEvent::new(
        EntityType::Collection,
        Action::Created,
        ActorKind::System,
        "created collection",
    )
    .unwrap()
    .with_context(&context);

    assert_eq!(ev.actor_kind(), ActorKind::User);
    assert_eq!(ev.actor_user_id().map(PrincipalId::id), Some(42));
    assert_eq!(ev.request_id(), Some(request_id));
    assert_eq!(ev.correlation_id(), Some("client-correlation"));
}

#[actix_web::test]
async fn collection_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(7),
        Some(Uuid::new_v4()),
        Some(correlation_id("audit-correlation")),
    );
    let collection_name = scope.scoped_name("audited_collection");

    let collection = NewCollectionWithAssignee {
        name: collection_name.clone(),
        description: "before".to_string(),
        group_id: GroupID::new(fixture.owner_group.id).unwrap(),
        parent_collection_id: None,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateCollection {
        name: Some(collection_name.clone()),
        description: Some("after".to_string()),
    }
    .update(
        &scope.pool,
        CollectionID::new(collection.id).unwrap(),
        &context,
    )
    .await
    .unwrap();

    let unchanged = UpdateCollection {
        name: Some(collection_name.clone()),
        description: Some("after".to_string()),
    }
    .update(
        &scope.pool,
        CollectionID::new(collection.id).unwrap(),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "collection", collection.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(
        rows[0].entity_name.as_deref(),
        Some(collection_name.as_str())
    );
    assert_eq!(rows[0].collection_id, Some(collection.id));
    assert_eq!(rows[0].actor_user_id, Some(7));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("audit-correlation"));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(
        rows[0].metadata["assignee_group_id"],
        serde_json::json!(fixture.owner_group.id)
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn moving_a_collection_to_its_current_parent_is_a_noop() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(principal_id(8), Some(Uuid::new_v4()), None);
    let collection = fixture.collection.clone();
    let parent_id = collection.parent_collection_id.unwrap();
    let event_count = events_for(&scope, "collection", collection.id).await.len();

    let unchanged = move_collection(&scope.pool, collection.id, parent_id, &context)
        .await
        .unwrap();

    assert_eq!(unchanged.updated_at, collection.updated_at);
    assert_eq!(
        events_for(&scope, "collection", collection.id).await.len(),
        event_count
    );
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn class_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(9),
        Some(Uuid::new_v4()),
        Some(correlation_id("class-correlation")),
    );
    let class_name = scope.scoped_name("audited_class");

    let class = NewHubuumClass {
        name: class_name.clone(),
        collection_id: fixture.collection.id,
        json_schema: Some(serde_json::json!({"type": "object"})),
        validate_schema: Some(true),
        description: "before".to_string(),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateHubuumClass {
        name: Some(class_name.clone()),
        collection_id: None,
        json_schema: Some(serde_json::json!({"type": "object", "additionalProperties": true})),
        validate_schema: Some(false),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, HubuumClassID::new(class.id).unwrap(), &context)
    .await
    .unwrap();

    let unchanged = UpdateHubuumClass {
        name: Some(class_name.clone()),
        collection_id: Some(fixture.collection.id),
        json_schema: Some(serde_json::json!({
            "type": "object",
            "additionalProperties": true
        })),
        validate_schema: Some(false),
        description: Some("after".to_string()),
    }
    .update(&scope.pool, HubuumClassID::new(class.id).unwrap(), &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "class", class.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(class_name.as_str()));
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].actor_user_id, Some(9));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("class-correlation"));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[0].after.as_ref().unwrap()["validate_schema"], true);

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");
    assert_eq!(rows[1].after.as_ref().unwrap()["validate_schema"], false);

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn object_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(11),
        Some(Uuid::new_v4()),
        Some(correlation_id("object-correlation")),
    );
    let class_name = scope.scoped_name("object_event_class");
    let object_name = scope.scoped_name("audited_object");

    let class = NewHubuumClass {
        name: class_name,
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "class".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let object = NewHubuumObject {
        name: object_name.clone(),
        collection_id: fixture.collection.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"state": "before"}),
        description: "before".to_string(),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateHubuumObject {
        name: Some(object_name.clone()),
        collection_id: None,
        hubuum_class_id: None,
        data: Some(serde_json::json!({"state": "after"})),
        description: Some("after".to_string()),
    }
    .update(
        &scope.pool,
        HubuumObjectID::new(object.id).unwrap(),
        &context,
    )
    .await
    .unwrap();

    let unchanged = UpdateHubuumObject {
        name: Some(object_name.clone()),
        collection_id: Some(fixture.collection.id),
        hubuum_class_id: Some(class.id),
        data: Some(serde_json::json!({"state": "after"})),
        description: Some("after".to_string()),
    }
    .update(
        &scope.pool,
        HubuumObjectID::new(object.id).unwrap(),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "object", object.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(object_name.as_str()));
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].actor_user_id, Some(11));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("object-correlation")
    );
    assert_eq!(rows[0].metadata["class_id"], serde_json::json!(class.id));
    assert_eq!(rows[0].after.as_ref().unwrap()["data"]["state"], "before");

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["data"]["state"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["data"]["state"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    class.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn class_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(13),
        Some(Uuid::new_v4()),
        Some(correlation_id("class-relation-correlation")),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("relation_class_a"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("relation_class_b"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: Some("children".to_string()),
        reverse_template_alias: Some("parents".to_string()),
        from_max_relations: Some(ObjectRelationLimit::new(1).unwrap()),
        to_max_relations: None,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    HubuumClassRelationID::new(relation.id)
        .unwrap()
        .delete(&scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "class_relation", relation.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(13));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("class-relation-correlation")
    );
    assert_eq!(
        rows[0].metadata["from_class_id"],
        serde_json::json!(class_a.id)
    );
    assert_eq!(
        rows[0].metadata["to_class_id"],
        serde_json::json!(class_b.id)
    );
    assert_eq!(
        rows[0].metadata["related_collection_ids"],
        serde_json::json!([fixture.collection.id, fixture.collection.id])
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["forward_template_alias"],
        "children"
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["from_max_relations"],
        serde_json::json!(1)
    );

    assert_eq!(rows[1].action, "deleted");
    assert_eq!(
        rows[1].metadata["related_collection_ids"],
        serde_json::json!([fixture.collection.id, fixture.collection.id])
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["reverse_template_alias"],
        "parents"
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["from_max_relations"],
        serde_json::json!(1)
    );
    assert!(rows[1].after.is_none());

    class_a.delete_without_events(&scope.pool).await.unwrap();
    class_b.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn object_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(15),
        Some(Uuid::new_v4()),
        Some(correlation_id("object-relation-correlation")),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_a"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_b"),
        collection_id: fixture.collection.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
        from_max_relations: None,
        to_max_relations: None,
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let object_a = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_a"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class_a.id,
        data: serde_json::json!({}),
        description: "a".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let object_b = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_b"),
        collection_id: fixture.collection.id,
        hubuum_class_id: class_b.id,
        data: serde_json::json!({}),
        description: "b".to_string(),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_a.id,
        to_hubuum_object_id: object_b.id,
        class_relation_id: class_relation.id,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    relation.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "object_relation", relation.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(15));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("object-relation-correlation")
    );
    assert_eq!(
        rows[0].metadata["class_relation_id"],
        serde_json::json!(class_relation.id)
    );
    assert_eq!(
        rows[0].metadata["from_object_id"],
        serde_json::json!(object_a.id)
    );
    assert_eq!(
        rows[0].metadata["to_object_id"],
        serde_json::json!(object_b.id)
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["class_relation_id"],
        class_relation.id
    );

    assert_eq!(rows[1].action, "deleted");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["from_hubuum_object_id"],
        object_a.id
    );
    assert!(rows[1].after.is_none());

    object_a.delete_without_events(&scope.pool).await.unwrap();
    object_b.delete_without_events(&scope.pool).await.unwrap();
    class_relation
        .delete_without_events(&scope.pool)
        .await
        .unwrap();
    class_a.delete_without_events(&scope.pool).await.unwrap();
    class_b.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn group_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let context = EventContext::user(
        principal_id(21),
        Some(Uuid::new_v4()),
        Some(correlation_id("group-correlation")),
    );

    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_group"),
        description: Some("before".to_string()),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateGroup {
        groupname: Some(scope.scoped_name("event_group_after")),
    }
    .save(GroupID::new(group.id).unwrap(), &scope.pool, &context)
    .await
    .unwrap();

    let unchanged = UpdateGroup {
        groupname: Some(updated.groupname.clone()),
    }
    .save(GroupID::new(group.id).unwrap(), &scope.pool, &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    GroupID::new(unchanged.id)
        .unwrap()
        .delete(&scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "group", group.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(21));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("group-correlation"));
    assert_eq!(
        rows[0].entity_name.as_deref(),
        Some(group.groupname.as_str())
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["description"],
        serde_json::json!("before")
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["groupname"],
        serde_json::json!(group.groupname)
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["groupname"],
        serde_json::json!(updated.groupname)
    );

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(
        rows[2].before.as_ref().unwrap()["groupname"],
        serde_json::json!(updated.groupname)
    );
    assert!(rows[2].after.is_none());
}

#[actix_web::test]
async fn group_membership_writes_emit_added_removed_events_when_changed() {
    let scope = test_scope();
    let context = EventContext::user(
        principal_id(22),
        Some(Uuid::new_v4()),
        Some(correlation_id("membership-correlation")),
    );

    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_membership_group"),
        description: Some("membership group".to_string()),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let user = create_test_user(&scope.pool).await;

    group
        .add_member(&scope.pool, &user, &context)
        .await
        .unwrap();
    group
        .add_member(&scope.pool, &user, &context)
        .await
        .unwrap();
    group
        .remove_member(&user, &scope.pool, &context)
        .await
        .unwrap();
    group
        .remove_member(&user, &scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for_type(&scope, "user_group")
        .await
        .into_iter()
        .filter(|row| {
            row.metadata["principal_id"] == serde_json::json!(user.id)
                && row.metadata["group_id"] == serde_json::json!(group.id)
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "added");
    assert_eq!(rows[0].actor_user_id, Some(22));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("membership-correlation")
    );
    assert_eq!(rows[0].metadata["principal_id"], serde_json::json!(user.id));
    assert_eq!(rows[0].metadata["group_id"], serde_json::json!(group.id));

    assert_eq!(rows[1].action, "removed");
    assert_eq!(rows[1].metadata["principal_id"], serde_json::json!(user.id));
    assert_eq!(rows[1].metadata["group_id"], serde_json::json!(group.id));

    group.delete_without_events(&scope.pool).await.unwrap();
    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn user_writes_emit_lifecycle_events_without_password_material() {
    let scope = test_scope();
    let context = EventContext::user(
        principal_id(23),
        Some(Uuid::new_v4()),
        Some(correlation_id("user-correlation")),
    );
    let username = scope.scoped_name("event_user");

    let user = NewUser {
        identity_scope: None,
        name: username.clone(),
        password: "initial-password".to_string(),
        proper_name: Some("Before User".to_string()),
        email: Some("before@example.invalid".to_string()),
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateUser {
        password: Some("updated-password".to_string()),
        proper_name: Some("After User".to_string()),
        email: Some("after@example.invalid".to_string()),
    }
    .save(UserID::new(user.id).unwrap(), &scope.pool, &context)
    .await
    .unwrap();

    let unchanged = UpdateUser {
        password: None,
        proper_name: Some("After User".to_string()),
        email: Some("after@example.invalid".to_string()),
    }
    .save(UserID::new(user.id).unwrap(), &scope.pool, &context)
    .await
    .unwrap();
    assert_eq!(unchanged.updated_at, updated.updated_at);

    unchanged.delete(&scope.pool, &context).await.unwrap();

    let rows = events_for(&scope, "user", user.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(23));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("user-correlation"));
    assert_eq!(rows[0].entity_name.as_deref(), Some(username.as_str()));
    assert_eq!(
        rows[0].after.as_ref().unwrap()["proper_name"],
        serde_json::json!("Before User")
    );
    assert!(rows[0].after.as_ref().unwrap().get("password").is_none());

    assert_eq!(rows[1].action, "updated");
    assert_eq!(
        rows[1].metadata["password_changed"],
        serde_json::json!(true)
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["email"],
        serde_json::json!("before@example.invalid")
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["email"],
        serde_json::json!("after@example.invalid")
    );
    assert!(rows[1].before.as_ref().unwrap().get("password").is_none());
    assert!(rows[1].after.as_ref().unwrap().get("password").is_none());

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(
        rows[2].before.as_ref().unwrap()["proper_name"],
        serde_json::json!("After User")
    );
    assert!(rows[2].before.as_ref().unwrap().get("password").is_none());
    assert!(rows[2].after.is_none());
}

#[actix_web::test]
async fn token_writes_emit_created_revoked_events_without_token_material() {
    let scope = test_scope();
    let context = EventContext::user(
        principal_id(24),
        Some(Uuid::new_v4()),
        Some(correlation_id("token-correlation")),
    );

    let user = NewUser {
        identity_scope: None,
        name: scope.scoped_name("event_token_user"),
        password: "token-user-password".to_string(),
        proper_name: None,
        email: None,
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let raw = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
        .name(Some("automation".to_string()))
        .description(Some("for event tests".to_string()))
        .scope(
            TokenScope::from_request_parts(
                Some(vec![Permissions::ReadCollection, Permissions::ReadClass]),
                None,
            )
            .unwrap(),
        )
        .create(&scope.pool, &context)
        .await
        .unwrap();
    let token = token_by_raw_value(&scope, &raw).await;

    let revoked = revoke_token_by_id_for_principal(
        &scope.pool,
        TokenID::new(token.id).unwrap(),
        PrincipalID::new(user.id).unwrap(),
        &context,
    )
    .await
    .unwrap();
    assert_eq!(revoked, 1);

    let rows = events_for(&scope, "token", token.id).await;
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(24));
    assert_eq!(rows[0].correlation_id.as_deref(), Some("token-correlation"));
    assert_eq!(rows[0].metadata["principal_id"], serde_json::json!(user.id));
    assert!(rows[0].metadata.get("renewed_from_token_id").is_none());
    assert_eq!(rows[0].after.as_ref().unwrap()["name"], "automation");
    assert_eq!(
        rows[0].after.as_ref().unwrap()["scope"]["permissions"],
        serde_json::json!(["ReadCollection", "ReadClass"])
    );
    assert!(rows[0].after.as_ref().unwrap().get("token").is_none());

    assert_eq!(rows[1].action, "revoked");
    assert_eq!(rows[1].metadata["principal_id"], serde_json::json!(user.id));
    assert!(rows[1].before.as_ref().unwrap()["revoked_at"].is_null());
    assert!(!rows[1].after.as_ref().unwrap()["revoked_at"].is_null());
    assert!(rows[1].before.as_ref().unwrap().get("token").is_none());
    assert!(rows[1].after.as_ref().unwrap().get("token").is_none());

    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn token_renewal_event_links_source_and_copies_hash_free_scope() {
    let scope = test_scope();
    let context = EventContext::user(
        principal_id(24),
        Some(Uuid::new_v4()),
        Some(correlation_id("token-renewal")),
    );
    let user = NewUser {
        identity_scope: None,
        name: scope.scoped_name("event_token_renewal_user"),
        password: "token-renewal-password".to_string(),
        proper_name: None,
        email: None,
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();
    let source_raw = PrincipalTokenCreateRequest::new(PrincipalID::new(user.id).unwrap())
        .name(Some("renewable automation".to_string()))
        .scope(
            TokenScope::from_request_parts(Some(vec![Permissions::ReadCollection]), None).unwrap(),
        )
        .create(&scope.pool, &context)
        .await
        .unwrap();
    let source = token_by_raw_value(&scope, &source_raw).await;

    let renewed_raw = renew_token_by_id_for_principal(
        &scope.pool,
        TokenID::new(source.id).unwrap(),
        PrincipalID::new(user.id).unwrap(),
        None,
        &context,
    )
    .await
    .unwrap()
    .into_token();
    let renewed = token_by_raw_value(&scope, &renewed_raw).await;
    let rows = events_for(&scope, "token", renewed.id).await;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].action, "created");
    assert_eq!(
        rows[0].metadata["renewed_from_token_id"],
        serde_json::json!(source.id)
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["scope"]["permissions"],
        serde_json::json!(["ReadCollection"])
    );
    assert!(rows[0].after.as_ref().unwrap().get("token").is_none());
    assert!(rows[0].metadata.get("token").is_none());

    user.delete_without_events(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn permission_writes_emit_granted_revoked_events() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(25),
        Some(Uuid::new_v4()),
        Some(correlation_id("permission-correlation")),
    );
    let group = NewGroup {
        identity_scope: None,
        groupname: scope.scoped_name("event_permission_group"),
        description: Some("permission group".to_string()),
    }
    .save_without_events(&scope.pool)
    .await
    .unwrap();

    let permission = fixture
        .collection
        .grant(
            &scope.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            &context,
        )
        .await
        .unwrap();

    fixture
        .collection
        .grant(
            &scope.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            &context,
        )
        .await
        .unwrap();

    fixture
        .collection
        .apply_permissions(
            &scope.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new([Permissions::ReadCollection, Permissions::CreateClass]),
            true,
            &context,
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke(
            &scope.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new([Permissions::CreateClass]),
            &context,
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke(
            &scope.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new([Permissions::CreateClass]),
            &context,
        )
        .await
        .unwrap();

    fixture
        .collection
        .revoke_all(&scope.pool, GroupID::new(group.id).unwrap(), &context)
        .await
        .unwrap();
    fixture
        .collection
        .revoke_all(&scope.pool, GroupID::new(group.id).unwrap(), &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "permission", permission.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "granted");
    assert_eq!(
        rows[0].before.as_ref().unwrap()["granted_permissions"],
        serde_json::json!([])
    );
    assert_eq!(rows[0].actor_user_id, Some(25));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("permission-correlation")
    );
    assert_eq!(
        rows[0].metadata["collection_id"],
        serde_json::json!(fixture.collection.id)
    );
    assert_eq!(rows[0].metadata["group_id"], serde_json::json!(group.id));
    assert_eq!(
        rows[0].metadata["requested_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );

    assert_eq!(rows[1].action, "revoked");
    assert_eq!(
        rows[1].metadata["requested_permissions"],
        serde_json::json!(["CreateClass"])
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection", "CreateClass"])
    );
    assert_eq!(
        rows[1].after.as_ref().unwrap()["granted_permissions"],
        serde_json::json!(["ReadCollection"])
    );

    assert_eq!(rows[2].action, "revoked");
    assert_eq!(
        rows[2].metadata["requested_permissions"],
        serde_json::json!(["ReadCollection"])
    );
    assert_eq!(
        rows[2].after.as_ref().unwrap()["granted_permissions"],
        serde_json::json!([])
    );

    group.delete_without_events(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn export_template_writes_emit_lifecycle_events() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let context = EventContext::user(
        principal_id(26),
        Some(Uuid::new_v4()),
        Some(correlation_id("export-template-correlation")),
    );

    let template = NewExportTemplate {
        collection_id: fixture.collection.id,
        name: scope.scoped_name("event_template"),
        description: "before".to_string(),
        content_type: ExportContentType::TextPlain,
        template: "Hello {{ name }}".to_string(),
        kind: ExportTemplateKind::Fragment,
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .save(&scope.pool, &context)
    .await
    .unwrap();

    let updated = UpdateExportTemplate {
        collection_id: None,
        name: None,
        description: Some("after".to_string()),
        template: Some("Goodbye {{ name }}".to_string()),
        kind: None,
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .update(
        &scope.pool,
        ExportTemplateID::new(template.id).unwrap(),
        &context,
    )
    .await
    .unwrap();

    UpdateExportTemplate {
        collection_id: Some(fixture.collection.id),
        name: Some(updated.name.clone()),
        description: Some("after".to_string()),
        template: Some("Goodbye {{ name }}".to_string()),
        kind: Some(ExportTemplateKind::Fragment),
        scope_kind: None,
        class_id: None,
        default_query: None,
        include: None,
        relation_context: None,
        default_missing_data_policy: None,
        default_limits: None,
    }
    .update(
        &scope.pool,
        ExportTemplateID::new(template.id).unwrap(),
        &context,
    )
    .await
    .unwrap();

    ExportTemplateID::new(updated.id)
        .unwrap()
        .delete(&scope.pool, &context)
        .await
        .unwrap();

    let rows = events_for(&scope, "export_template", template.id).await;
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(26));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("export-template-correlation")
    );
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].entity_name.as_deref(), Some(template.name.as_str()));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");

    assert_eq!(rows[2].action, "deleted");
    assert_eq!(rows[2].before.as_ref().unwrap()["description"], "after");
    assert!(rows[2].after.is_none());

    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn remote_target_writes_emit_lifecycle_and_invoked_events_with_redacted_auth() {
    let scope = test_scope();
    let fixture = scope.with_collection().await;
    let backend = PostgresStorage::unobserved(scope.pool.get_ref().clone());
    let context = EventContext::user(
        principal_id(27),
        Some(Uuid::new_v4()),
        Some(correlation_id("remote-target-correlation")),
    );

    let created = backend
        .create_remote_target(StorageRemoteTargetCreate::new(
            hubuum_domain::CollectionId::new(fixture.collection.id)
                .expect("validated collection id must be positive"),
            scope.scoped_name("event_remote_target"),
            StorageRemoteTargetDefinition::new(
                "before",
                StorageRemoteTargetTransport::try_new(
                    crate::storage::StorageRemoteTargetHttpMethod::Get,
                    "https://example.invalid/{{ subject.id }}",
                    serde_json::json!({}),
                    None,
                    serde_json::json!({
                        "type": "api_key_secret",
                        "header": "X-Api-Key",
                        "secret": "super-secret"
                    }),
                    1000,
                )
                .unwrap(),
                StorageRemoteTargetPolicy::try_new(
                    None,
                    vec![crate::storage::StorageRemoteTargetSubjectType::Collection],
                    true,
                )
                .unwrap(),
            ),
            context.clone(),
        ))
        .await
        .unwrap()
        .into_value();
    let target_id = hubuum_domain::RemoteTargetId::from(created.metadata().id());

    let updated = backend
        .update_remote_target(StorageRemoteTargetUpdate::new(
            target_id,
            StorageRemoteTargetPatch::new().with_description(Some("after".to_string())),
            context.clone(),
        ))
        .await
        .unwrap()
        .into_value();
    let (updated_metadata, collection_id, name, definition) = updated.clone().into_parts();
    let (description, transport, policy) = definition.into_parts();
    let transport = transport.into_parts();
    let method = transport.method();
    let url_template = transport.url_template().to_owned();
    let headers_template = transport.headers_template().clone();
    let body_template = transport.body_template().map(str::to_owned);
    let auth_config = transport.auth_config().clone();
    let timeout_ms = transport.timeout_ms();
    let (class_id, allowed_subject_types, enabled) = policy.into_parts();
    let unchanged = backend
        .update_remote_target(StorageRemoteTargetUpdate::new(
            target_id,
            StorageRemoteTargetPatch::new()
                .with_collection_id(Some(collection_id))
                .with_class_id(Some(class_id))
                .with_name(Some(name))
                .with_description(Some(description))
                .with_method(Some(method))
                .with_url_template(Some(url_template))
                .with_headers_template(Some(headers_template))
                .with_body_template(Some(body_template))
                .with_auth_config(Some(auth_config))
                .with_allowed_subject_types(Some(allowed_subject_types))
                .with_timeout_ms(Some(timeout_ms))
                .with_enabled(Some(enabled)),
            context.clone(),
        ))
        .await
        .unwrap()
        .into_value();
    assert_eq!(
        unchanged.metadata().updated_at(),
        updated_metadata.updated_at()
    );
    backend
        .record_remote_target_invocation(StorageRemoteTargetInvocation::new(
            target_id,
            hubuum_domain::TaskId::new(12345).unwrap(),
            crate::storage::StorageRemoteTargetSubjectType::Collection,
            hubuum_domain::ResourceId::new(fixture.collection.id).unwrap(),
            context.clone(),
        ))
        .await
        .unwrap()
        .into_value();

    backend
        .delete_remote_target(StorageRemoteTargetDelete::new(target_id, context))
        .await
        .unwrap()
        .into_value();

    let rows = events_for(&scope, "remote_target", target_id.id()).await;
    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].actor_user_id, Some(27));
    assert_eq!(
        rows[0].correlation_id.as_deref(),
        Some("remote-target-correlation")
    );
    assert_eq!(rows[0].collection_id, Some(fixture.collection.id));
    assert_eq!(rows[0].after.as_ref().unwrap()["description"], "before");
    assert_eq!(
        rows[0].after.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    assert_eq!(rows[1].action, "updated");
    assert_eq!(rows[1].before.as_ref().unwrap()["description"], "before");
    assert_eq!(rows[1].after.as_ref().unwrap()["description"], "after");
    assert_eq!(
        rows[1].before.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    assert_eq!(rows[2].action, "invoked");
    assert_eq!(rows[2].metadata["task_id"], serde_json::json!(12345));
    assert_eq!(rows[2].metadata["subject_type"], "collection");
    assert_eq!(
        rows[2].metadata["subject_id"],
        serde_json::json!(fixture.collection.id)
    );
    assert!(rows[2].before.is_none());
    assert!(rows[2].after.is_none());

    assert_eq!(rows[3].action, "deleted");
    assert_eq!(rows[3].before.as_ref().unwrap()["description"], "after");
    assert_eq!(
        rows[3].before.as_ref().unwrap()["auth_config"],
        serde_json::json!("<redacted>")
    );

    fixture.cleanup().await.unwrap();
}

async fn events_for(
    scope: &TestScope,
    event_entity_type: &str,
    event_entity_id: i32,
) -> Vec<Event> {
    hubuum_storage_postgres::test_support::list_events(
        scope.pool.get_ref(),
        EntityType::parse(event_entity_type).expect("test entity type must be valid"),
        EventEntityId::new(event_entity_id).expect("test entity id must be valid"),
        None,
    )
    .await
    .expect("test events should load")
    .into_iter()
    .map(event_from_storage)
    .collect()
}

async fn token_by_raw_value(scope: &TestScope, raw: &Token) -> PrincipalToken {
    crate::tests::persisted_test_token(scope.pool.get_ref(), &raw.get_token()).await
}

async fn events_for_type(scope: &TestScope, event_entity_type: &str) -> Vec<Event> {
    hubuum_storage_postgres::test_support::list_events_by_type(
        scope.pool.get_ref(),
        EntityType::parse(event_entity_type).expect("test entity type must be valid"),
    )
    .await
    .expect("test events should load")
    .into_iter()
    .map(event_from_storage)
    .collect()
}
