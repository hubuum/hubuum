#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use diesel::prelude::*;
    use serde_json::json;

    use crate::db::with_connection;
    use crate::events::{Action, EntityType};
    use crate::models::{
        EventSink, EventSinkKind, EventSubscription, NewEventSink, NewEventSubscription,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{delete_request, get_request, post_request};
    use crate::tests::asserts::assert_response_status;

    const SINKS_ENDPOINT: &str = "/api/v1/event-sinks";

    fn new_webhook_sink(name: String) -> NewEventSink {
        NewEventSink {
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

    fn audit_event_count(
        context: &TestContext,
        entity_type_value: EntityType,
        action_value: Action,
        entity_id_value: i32,
    ) -> i64 {
        with_connection(&context.pool, |conn| {
            use crate::schema::events::dsl::{action, entity_id, entity_type, events};

            events
                .filter(entity_type.eq(entity_type_value.as_str()))
                .filter(action.eq(action_value.as_str()))
                .filter(entity_id.eq(entity_id_value))
                .count()
                .get_result::<i64>(conn)
        })
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
            audit_event_count(&context, EntityType::EventSink, Action::Created, created.id),
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

        if let Some(kind) = disabled_sink_kind_for_feature_set() {
            let disabled_kind = NewEventSink {
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
            audit_event_count(&context, EntityType::EventSink, Action::Deleted, created.id),
            1
        );
    }

    #[actix_web::test]
    async fn test_event_subscription_validates_catalog_and_requires_permission() {
        let context = TestContext::new().await;
        let namespace = context.namespace_fixture("subscription_catalog").await;
        let sink = create_sink(&context, "subscription_sink").await;
        let endpoint = format!(
            "/api/v1/namespaces/{}/event-subscriptions",
            namespace.namespace.id
        );

        let valid = NewEventSubscription {
            sink_id: crate::models::EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("subscription"),
            description: "valid event subscription".to_string(),
            entity_types: vec!["namespace".to_string()],
            actions: vec!["created".to_string()],
            routing: json!({"url": "https://example.test/events"}),
            enabled: true,
        };
        let resp = post_request(&context.pool, &context.normal_token, &endpoint, &valid).await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = post_request(&context.pool, &context.admin_token, &endpoint, &valid).await;
        let resp = assert_response_status(resp, StatusCode::CREATED).await;
        let created: EventSubscription = test::read_body_json(resp).await;
        assert_eq!(created.namespace_id, namespace.namespace.id);
        assert_eq!(created.entity_types, vec!["namespace"]);
        assert_eq!(created.actions, vec!["created"]);
        assert_eq!(
            audit_event_count(
                &context,
                EntityType::EventSubscription,
                Action::Created,
                created.id
            ),
            1
        );

        let invalid_pair = NewEventSubscription {
            sink_id: crate::models::EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("subscription_invalid"),
            description: "invalid event subscription".to_string(),
            entity_types: vec!["object_relation".to_string()],
            actions: vec!["updated".to_string()],
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
    }
}
