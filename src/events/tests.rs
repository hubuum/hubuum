//! Tests for the events table + emit_event helper (#71).
//!
//! The load-bearing property is "recorded iff committed": an event emitted
//! inside a transaction that commits is persisted, and one emitted inside a
//! transaction that rolls back is not. These tests exercise that directly
//! against a real Postgres pool.

#![cfg(test)]

use diesel::prelude::*;
use rstest::rstest;
use uuid::Uuid;

use crate::db::{with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{
    Action, ActorKind, EntityType, Event, EventContext, NewEvent, RequestProvenance, emit_event,
};
use crate::models::class::{NewHubuumClass, UpdateHubuumClass};
use crate::models::group::{NewGroup, UpdateGroup};
use crate::models::namespace::{NewNamespaceWithAssignee, UpdateNamespace};
use crate::models::object::{NewHubuumObject, UpdateHubuumObject};
use crate::models::{
    GroupID, HubuumClassRelationID, NewHubuumClassRelation, NewHubuumObjectRelation, NewUser,
    UpdateUser,
};
use crate::schema::events::dsl::events;
use crate::tests::{TestScope, create_test_user, test_scope};
use crate::traits::{CanDelete, CanSave, CanUpdate};

/// Count event rows for a given `event_id` (0 or 1, since `event_id` is UNIQUE).
fn count_events_for(conn: &mut PgConnection, target: Uuid) -> i64 {
    use crate::schema::events::dsl::event_id;
    events
        .filter(event_id.eq(target))
        .count()
        .get_result(conn)
        .expect("count query")
}

#[rstest]
#[case::commit_persists(false)]
#[case::rollback_discards(true)]
fn emit_event_respects_transaction_outcome(#[case] rollback: bool) {
    let scope = test_scope();
    let pool = scope.pool.clone();

    let new_event = NewEvent::new(
        EntityType::Namespace,
        Action::Created,
        ActorKind::System,
        "test event",
    )
    .unwrap()
    .with_namespace_id(1)
    .with_entity_id(1)
    .with_entity_name("ns-test")
    .with_request_id(Uuid::new_v4())
    .with_correlation_id("client-provided-correlation-id")
    .with_metadata(serde_json::json!({"k": "v"}));
    let event_uuid = new_event.event_id();

    let result: Result<Event, ApiError> = with_transaction(&pool, |conn| {
        let event = emit_event(conn, &new_event)?;
        // The row is visible inside the same transaction.
        assert_eq!(count_events_for(conn, event_uuid), 1);
        if rollback {
            // Simulate a later mutation step failing, aborting the whole tx.
            return Err(ApiError::InternalServerError("simulated failure".into()));
        }
        Ok(event)
    });

    if rollback {
        assert!(result.is_err(), "expected rollback error");
    } else {
        assert!(result.is_ok(), "expected commit, got {result:?}");
    }

    // After the transaction settles, the row persists iff it committed.
    let persisted = with_connection(&pool, |conn| {
        Ok::<_, diesel::result::Error>(count_events_for(conn, event_uuid))
    })
    .unwrap();
    if rollback {
        assert_eq!(
            persisted, 0,
            "event must not survive a rolled-back transaction"
        );
    } else {
        assert_eq!(persisted, 1, "event must survive a committed transaction");
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
    assert!(
        matches!(err, ApiError::ValidationError(ref m) if m.contains("not valid for entity_type")),
        "expected ValidationError, got {err:?}"
    );
}

#[test]
fn new_event_accepts_arbitrary_correlation_id() {
    let ev = NewEvent::new(EntityType::Namespace, Action::Created, ActorKind::User, "n")
        .unwrap()
        .with_correlation_id("any-arbitrary-client-value-!@#$%");
    // correlation_id accepts arbitrary caller-provided header values (#71).
    assert_eq!(
        ev.correlation_id(),
        Some("any-arbitrary-client-value-!@#$%")
    );
}

#[test]
fn new_event_applies_event_context() {
    let request_id = Uuid::new_v4();
    let provenance = RequestProvenance::new(request_id, Some("client-correlation".to_string()));
    let context = provenance.user_event_context(42);

    let ev = NewEvent::new(
        EntityType::Namespace,
        Action::Created,
        ActorKind::System,
        "created namespace",
    )
    .unwrap()
    .with_context(&context);

    assert_eq!(ev.actor_kind(), ActorKind::User);
    assert_eq!(ev.actor_user_id(), Some(42));
    assert_eq!(ev.request_id(), Some(request_id));
    assert_eq!(ev.correlation_id(), Some("client-correlation"));
}

#[test]
fn fanout_backlog_index_exists() {
    // The partial fan-out backlog index must be present before #76 (#71 done-when).
    let scope = test_scope();
    with_connection(&scope.pool, |conn| {
        let exists: bool = diesel::sql_query(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = 'events'
                  AND indexname = 'events_fanout_backlog_idx'
                  AND indexdef LIKE '%WHERE (dispatched_at IS NULL)%'
            )",
        )
        .get_result::<IndexExistsRow>(conn)
        .map(|r| r.exists)?;
        assert!(exists, "events_fanout_backlog_idx partial index is missing");
        Ok::<_, diesel::result::Error>(())
    })
    .unwrap();
}

#[actix_web::test]
async fn namespace_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_namespace().await;
    let context = EventContext::user(7, Some(Uuid::new_v4()), Some("audit-correlation".into()));
    let namespace_name = scope.scoped_name("audited_namespace");

    let namespace = NewNamespaceWithAssignee {
        name: namespace_name.clone(),
        description: "before".to_string(),
        group_id: fixture.owner_group.id,
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateNamespace {
        name: Some(namespace_name.clone()),
        description: Some("after".to_string()),
    }
    .update_with_context(&scope.pool, namespace.id, Some(&context))
    .await
    .unwrap();

    updated
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "namespace", namespace.id);
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(
        rows[0].entity_name.as_deref(),
        Some(namespace_name.as_str())
    );
    assert_eq!(rows[0].namespace_id, Some(namespace.id));
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
async fn class_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_namespace().await;
    let context = EventContext::user(9, Some(Uuid::new_v4()), Some("class-correlation".into()));
    let class_name = scope.scoped_name("audited_class");

    let class = NewHubuumClass {
        name: class_name.clone(),
        namespace_id: fixture.namespace.id,
        json_schema: Some(serde_json::json!({"type": "object"})),
        validate_schema: Some(true),
        description: "before".to_string(),
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateHubuumClass {
        name: Some(class_name.clone()),
        namespace_id: None,
        json_schema: Some(serde_json::json!({"type": "object", "additionalProperties": true})),
        validate_schema: Some(false),
        description: Some("after".to_string()),
    }
    .update_with_context(&scope.pool, class.id, Some(&context))
    .await
    .unwrap();

    updated
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "class", class.id);
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(class_name.as_str()));
    assert_eq!(rows[0].namespace_id, Some(fixture.namespace.id));
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
    let fixture = scope.with_namespace().await;
    let context = EventContext::user(11, Some(Uuid::new_v4()), Some("object-correlation".into()));
    let class_name = scope.scoped_name("object_event_class");
    let object_name = scope.scoped_name("audited_object");

    let class = NewHubuumClass {
        name: class_name,
        namespace_id: fixture.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "class".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();

    let object = NewHubuumObject {
        name: object_name.clone(),
        namespace_id: fixture.namespace.id,
        hubuum_class_id: class.id,
        data: serde_json::json!({"state": "before"}),
        description: "before".to_string(),
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateHubuumObject {
        name: Some(object_name.clone()),
        namespace_id: None,
        hubuum_class_id: None,
        data: Some(serde_json::json!({"state": "after"})),
        description: Some("after".to_string()),
    }
    .update_with_context(&scope.pool, object.id, Some(&context))
    .await
    .unwrap();

    updated
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "object", object.id);
    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0].action, "created");
    assert_eq!(rows[0].entity_name.as_deref(), Some(object_name.as_str()));
    assert_eq!(rows[0].namespace_id, Some(fixture.namespace.id));
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

    class.delete(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn class_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_namespace().await;
    let context = EventContext::user(
        13,
        Some(Uuid::new_v4()),
        Some("class-relation-correlation".into()),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("relation_class_a"),
        namespace_id: fixture.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("relation_class_b"),
        namespace_id: fixture.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: Some("children".to_string()),
        reverse_template_alias: Some("parents".to_string()),
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    HubuumClassRelationID::new(relation.id)
        .unwrap()
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "class_relation", relation.id);
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
        rows[0].metadata["related_namespace_ids"],
        serde_json::json!([fixture.namespace.id, fixture.namespace.id])
    );
    assert_eq!(
        rows[0].after.as_ref().unwrap()["forward_template_alias"],
        "children"
    );

    assert_eq!(rows[1].action, "deleted");
    assert_eq!(
        rows[1].metadata["related_namespace_ids"],
        serde_json::json!([fixture.namespace.id, fixture.namespace.id])
    );
    assert_eq!(
        rows[1].before.as_ref().unwrap()["reverse_template_alias"],
        "parents"
    );
    assert!(rows[1].after.is_none());

    class_a.delete(&scope.pool).await.unwrap();
    class_b.delete(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn object_relation_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let fixture = scope.with_namespace().await;
    let context = EventContext::user(
        15,
        Some(Uuid::new_v4()),
        Some("object-relation-correlation".into()),
    );

    let class_a = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_a"),
        namespace_id: fixture.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "a".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();
    let class_b = NewHubuumClass {
        name: scope.scoped_name("object_relation_class_b"),
        namespace_id: fixture.namespace.id,
        json_schema: None,
        validate_schema: Some(false),
        description: "b".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();
    let class_relation = NewHubuumClassRelation {
        from_hubuum_class_id: class_a.id,
        to_hubuum_class_id: class_b.id,
        forward_template_alias: None,
        reverse_template_alias: None,
    }
    .save(&scope.pool)
    .await
    .unwrap();

    let object_a = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_a"),
        namespace_id: fixture.namespace.id,
        hubuum_class_id: class_a.id,
        data: serde_json::json!({}),
        description: "a".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();
    let object_b = NewHubuumObject {
        name: scope.scoped_name("object_relation_object_b"),
        namespace_id: fixture.namespace.id,
        hubuum_class_id: class_b.id,
        data: serde_json::json!({}),
        description: "b".to_string(),
    }
    .save(&scope.pool)
    .await
    .unwrap();

    let relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_a.id,
        to_hubuum_object_id: object_b.id,
        class_relation_id: class_relation.id,
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    relation
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "object_relation", relation.id);
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

    object_a.delete(&scope.pool).await.unwrap();
    object_b.delete(&scope.pool).await.unwrap();
    class_relation.delete(&scope.pool).await.unwrap();
    class_a.delete(&scope.pool).await.unwrap();
    class_b.delete(&scope.pool).await.unwrap();
    fixture.cleanup().await.unwrap();
}

#[actix_web::test]
async fn group_writes_emit_lifecycle_events_in_transaction() {
    let scope = test_scope();
    let context = EventContext::user(21, Some(Uuid::new_v4()), Some("group-correlation".into()));

    let group = NewGroup {
        groupname: scope.scoped_name("event_group"),
        description: Some("before".to_string()),
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateGroup {
        groupname: Some(scope.scoped_name("event_group_after")),
    }
    .save_with_context(group.id, &scope.pool, Some(&context))
    .await
    .unwrap();

    GroupID::new(updated.id)
        .unwrap()
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "group", group.id);
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
        22,
        Some(Uuid::new_v4()),
        Some("membership-correlation".into()),
    );

    let group = NewGroup {
        groupname: scope.scoped_name("event_membership_group"),
        description: Some("membership group".to_string()),
    }
    .save(&scope.pool)
    .await
    .unwrap();
    let user = create_test_user(&scope.pool).await;

    group
        .add_member_with_context(&scope.pool, &user, Some(&context))
        .await
        .unwrap();
    group
        .add_member_with_context(&scope.pool, &user, Some(&context))
        .await
        .unwrap();
    group
        .remove_member_with_context(&user, &scope.pool, Some(&context))
        .await
        .unwrap();
    group
        .remove_member_with_context(&user, &scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for_type(&scope, "user_group")
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

    group.delete(&scope.pool).await.unwrap();
    user.delete(&scope.pool).await.unwrap();
}

#[actix_web::test]
async fn user_writes_emit_lifecycle_events_without_password_material() {
    let scope = test_scope();
    let context = EventContext::user(23, Some(Uuid::new_v4()), Some("user-correlation".into()));
    let username = scope.scoped_name("event_user");

    let user = NewUser {
        name: username.clone(),
        password: "initial-password".to_string(),
        proper_name: Some("Before User".to_string()),
        email: Some("before@example.invalid".to_string()),
    }
    .save_with_context(&scope.pool, Some(&context))
    .await
    .unwrap();

    let updated = UpdateUser {
        password: Some("updated-password".to_string()),
        proper_name: Some("After User".to_string()),
        email: Some("after@example.invalid".to_string()),
    }
    .save_with_context(user.id, &scope.pool, Some(&context))
    .await
    .unwrap();

    updated
        .delete_with_context(&scope.pool, Some(&context))
        .await
        .unwrap();

    let rows = events_for(&scope, "user", user.id);
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

fn events_for(scope: &TestScope, event_entity_type: &str, event_entity_id: i32) -> Vec<Event> {
    use crate::schema::events::dsl::{entity_id, entity_type, id};

    with_connection(&scope.pool, |conn| {
        events
            .filter(entity_type.eq(event_entity_type))
            .filter(entity_id.eq(event_entity_id))
            .order(id.asc())
            .load::<Event>(conn)
    })
    .unwrap()
}

fn events_for_type(scope: &TestScope, event_entity_type: &str) -> Vec<Event> {
    use crate::schema::events::dsl::{entity_type, id};

    with_connection(&scope.pool, |conn| {
        events
            .filter(entity_type.eq(event_entity_type))
            .order(id.asc())
            .load::<Event>(conn)
    })
    .unwrap()
}

#[derive(diesel::QueryableByName)]
struct IndexExistsRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    exists: bool,
}
