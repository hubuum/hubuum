#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use serde_json::json;

    use crate::events::{Action, EntityType};
    use crate::models::{
        EventSink, EventSinkKind, EventSubscription, NewEventSink, NewEventSubscription,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{delete_request, get_request, patch_request, post_request};
    use crate::tests::asserts::assert_response_status;

    const SINKS_ENDPOINT: &str = "/api/v1/event-sinks";

    fn new_webhook_sink(name: String) -> NewEventSink {
        NewEventSink {
            delivery_policy: None,
            name,
            kind: EventSinkKind::Webhook,
            config: json!({}),
            secret_ref: None,
            enabled: true,
        }
    }

    fn disabled_sink_kind_for_feature_set() -> Option<EventSinkKind> {
        if !cfg!(feature = "amqp") {
            Some(EventSinkKind::Amqp)
        } else if !cfg!(feature = "valkey") {
            Some(EventSinkKind::ValkeyStream)
        } else if !cfg!(feature = "email") {
            Some(EventSinkKind::Email)
        } else {
            None
        }
    }

    async fn create_sink(context: &TestContext, label: &str) -> EventSink {
        let payload = new_webhook_sink(context.scoped_name(label));
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            SINKS_ENDPOINT,
            &payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        test::read_body_json(resp).await
    }

    async fn audit_event_count(
        context: &TestContext,
        entity_type_value: EntityType,
        action_value: Action,
        entity_id_value: i32,
    ) -> i64 {
        crate::test_support::audit_event_count(
            &context.pool,
            entity_type_value,
            action_value,
            entity_id_value,
        )
        .await
        .unwrap()
    }

    #[actix_web::test]
    async fn test_event_sink_crud_is_admin_only_and_rejects_disabled_kinds() {
        let context = TestContext::new().await;
        let payload = new_webhook_sink(context.scoped_name("sink_admin"));

        let resp = post_request(
            &context.pool,
            &context.normal_token,
            SINKS_ENDPOINT,
            &payload,
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            SINKS_ENDPOINT,
            &payload,
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: EventSink = test::read_body_json(resp).await;
        assert_eq!(created.kind, EventSinkKind::Webhook);
        assert_eq!(
            audit_event_count(&context, EntityType::EventSink, Action::Created, created.id).await,
            1
        );

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{SINKS_ENDPOINT}/{}", created.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: EventSink = test::read_body_json(resp).await;
        assert_eq!(fetched.id, created.id);

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &format!("{SINKS_ENDPOINT}/{}", created.id),
            json!({ "enabled": true }),
        )
        .await;
        assert_response_status(resp, StatusCode::OK).await;
        assert_eq!(
            audit_event_count(&context, EntityType::EventSink, Action::Updated, created.id).await,
            0
        );

        if let Some(kind) = disabled_sink_kind_for_feature_set() {
            let disabled_kind = NewEventSink {
                delivery_policy: None,
                name: context.scoped_name("sink_disabled"),
                kind,
                config: json!({}),
                secret_ref: None,
                enabled: true,
            };
            let resp = post_request(
                &context.pool,
                &context.admin_token,
                SINKS_ENDPOINT,
                &disabled_kind,
            )
            .await;
            assert_response_status(resp, StatusCode::BAD_REQUEST).await;
        }

        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{SINKS_ENDPOINT}/{}", created.id),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;
        assert_eq!(
            audit_event_count(&context, EntityType::EventSink, Action::Deleted, created.id).await,
            1
        );
    }

    #[actix_web::test]
    async fn test_event_subscription_validates_catalog_and_requires_permission() {
        let context = TestContext::new().await;
        let collection = context.collection_fixture("subscription_catalog").await;
        let sink = create_sink(&context, "subscription_sink").await;
        let endpoint = format!(
            "/api/v1/collections/{}/event-subscriptions",
            collection.collection.id
        );

        let valid = NewEventSubscription {
            sink_id: crate::models::EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("subscription"),
            description: "valid event subscription".to_string(),
            entity_types: vec!["collection".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: json!({"url": "https://example.test/events"}),
            enabled: true,
        };
        let resp = post_request(&context.pool, &context.normal_token, &endpoint, &valid).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(&context.pool, &context.admin_token, &endpoint, &valid).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: EventSubscription = test::read_body_json(resp).await;
        assert_eq!(created.collection_id, collection.collection.id);
        assert_eq!(created.entity_types, vec!["collection"]);
        assert_eq!(created.actions, vec!["created"]);
        assert_eq!(
            created.filter,
            hubuum_events_core::EventSubscriptionFilter::default()
        );
        assert_eq!(
            audit_event_count(
                &context,
                EntityType::EventSubscription,
                Action::Created,
                created.id
            )
            .await,
            1
        );

        let resp = patch_request(
            &context.pool,
            &context.admin_token,
            &format!("{endpoint}/{}", created.id),
            json!({ "enabled": true }),
        )
        .await;
        assert_response_status(resp, StatusCode::OK).await;
        assert_eq!(
            audit_event_count(
                &context,
                EntityType::EventSubscription,
                Action::Updated,
                created.id
            )
            .await,
            0
        );

        let invalid_pair = NewEventSubscription {
            sink_id: crate::models::EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("subscription_invalid"),
            description: "invalid event subscription".to_string(),
            entity_types: vec!["object_relation".to_string()],
            actions: vec!["updated".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter::default(),
            routing: json!({}),
            enabled: true,
        };
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &endpoint,
            &invalid_pair,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;

        let invalid_filter = NewEventSubscription {
            sink_id: crate::models::EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("subscription_invalid_filter"),
            description: "invalid event subscription filter".to_string(),
            entity_types: vec!["collection".to_string()],
            actions: vec!["created".to_string()],
            filter: hubuum_events_core::EventSubscriptionFilter {
                actor_kinds: vec!["anonymous".to_string()],
                ..hubuum_events_core::EventSubscriptionFilter::default()
            },
            routing: json!({}),
            enabled: true,
        };
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &endpoint,
            &invalid_filter,
        )
        .await;
        assert_response_status(resp, StatusCode::BAD_REQUEST).await;
    }
    #[actix_web::test]
    async fn webhook_preview_and_test_require_admin_and_preserve_real_source_event() {
        let context = TestContext::new().await;
        let resp = post_request(&context.pool, &context.admin_token, SINKS_ENDPOINT, &json!({
            "name":context.scoped_name("preview_webhook"), "kind":"webhook", "config":{"url_secret_ref":"unresolved_url", "body_template":r#"{"text":{{ (test_marker ~ summary) | tojson }}}"#},
            "secret_ref":"unresolved_preview_only", "enabled":false
        })).await;
        let sink: serde_json::Value =
            test::read_body_json(assert_response_status(resp, StatusCode::CREATED).await).await;
        let sink_id = sink["id"].as_i64().unwrap();
        let system = "/api/v1/system-event-subscriptions";
        let subscription = json!({"sink_id":sink_id,"name":context.scoped_name("system_sub"),"entity_types":["task"],"actions":["failed"],"filter":{"task_kinds":["backup"]},"enabled":false});
        let resp = post_request(&context.pool, &context.normal_token, system, &subscription).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
        let resp = post_request(&context.pool, &context.admin_token, system, &subscription).await;
        let sub: serde_json::Value =
            test::read_body_json(assert_response_status(resp, StatusCode::CREATED).await).await;
        assert!(sub.get("collection_id").is_none());
        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/events?entity_type=event_sink&entity_id={sink_id}&action=created"),
        )
        .await;
        let events: Vec<serde_json::Value> =
            test::read_body_json(assert_response_status(resp, StatusCode::OK).await).await;
        let input = json!({"subscription_id":sub["id"],"event_id":events[0]["event_id"]});
        let preview = format!("{SINKS_ENDPOINT}/{sink_id}/preview");
        let resp = post_request(&context.pool, &context.normal_token, &preview, &input).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
        let resp = post_request(&context.pool, &context.admin_token, &preview, &input).await;
        let rendered: serde_json::Value =
            test::read_body_json(assert_response_status(resp, StatusCode::OK).await).await;
        assert!(
            rendered["payload"]["text"]
                .as_str()
                .unwrap()
                .starts_with("[TEST]")
        );
        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{SINKS_ENDPOINT}/{sink_id}/test"),
            &input,
        )
        .await;
        let delivery: serde_json::Value =
            test::read_body_json(assert_response_status(resp, StatusCode::ACCEPTED).await).await;
        assert_eq!(delivery["purpose"], "test");
        assert_eq!(delivery["event_id"], events[0]["id"]);
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{system}/{}", sub["id"]),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;
        let resp = delete_request(
            &context.pool,
            &context.admin_token,
            &format!("{SINKS_ENDPOINT}/{sink_id}"),
        )
        .await;
        assert_response_status(resp, StatusCode::NO_CONTENT).await;
    }
}
