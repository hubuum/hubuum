#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use diesel::prelude::*;
    use serde_json::json;

    use crate::db::traits::event_fanout::fanout_event;
    use crate::db::traits::event_subscription::{SaveEventSinkRecord, SaveEventSubscriptionRecord};
    use crate::db::with_connection;
    use crate::events::{Action, ActorKind, EntityType, NewEvent, emit_event};
    use crate::models::{
        EventDelivery, EventDeliveryHealthResponse, EventDeliveryStatus,
        EventDeliveryUpdateResponse, EventSinkID, EventSinkKind, NamespaceID, NewEventSink,
        NewEventSubscription,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{get_request, post_request};
    use crate::tests::asserts::assert_response_status;

    const DELIVERIES_ENDPOINT: &str = "/api/v1/event-deliveries";

    struct DeliveryFixture {
        delivery: EventDelivery,
        sink_id: i32,
        sink_name: String,
        subscription_id: i32,
        subscription_name: String,
        namespace_id: i32,
    }

    async fn create_delivery(context: &TestContext) -> DeliveryFixture {
        let fixture = context.namespace_fixture("delivery_api").await;
        let sink_name = context.scoped_name("delivery_api_sink");
        let sink = NewEventSink {
            name: sink_name.clone(),
            kind: EventSinkKind::Webhook,
            config: json!({}),
            secret_ref: None,
            enabled: true,
        }
        .into_row()
        .unwrap()
        .save_event_sink_record(&context.pool)
        .await
        .unwrap();

        let subscription_name = context.scoped_name("delivery_api_subscription");
        let namespace_id = NamespaceID::new(fixture.namespace.id).unwrap();
        let subscription = NewEventSubscription {
            sink_id: EventSinkID::new(sink.id).unwrap(),
            name: subscription_name.clone(),
            description: String::new(),
            entity_types: vec![EntityType::Namespace.as_str().to_string()],
            actions: vec![Action::Created.as_str().to_string()],
            routing: json!({}),
            enabled: true,
        }
        .into_row(namespace_id)
        .unwrap()
        .save_event_subscription_record(&context.pool)
        .await
        .unwrap();

        let event = NewEvent::new(
            EntityType::Namespace,
            Action::Created,
            ActorKind::System,
            "delivery api test",
        )
        .unwrap()
        .with_namespace_id(fixture.namespace.id)
        .with_entity_id(fixture.namespace.id)
        .with_entity_name(&fixture.namespace.name);
        let event = with_connection(&context.pool, |conn| emit_event(conn, &event)).unwrap();
        fanout_event(&context.pool, event.id).await.unwrap();

        let delivery = with_connection(&context.pool, |conn| {
            use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

            event_deliveries
                .filter(event_id.eq(event.id))
                .first::<EventDelivery>(conn)
        })
        .unwrap();

        DeliveryFixture {
            delivery,
            sink_id: sink.id,
            sink_name,
            subscription_id: subscription.id,
            subscription_name,
            namespace_id: fixture.namespace.id,
        }
    }

    #[actix_web::test]
    async fn test_event_delivery_operations_are_admin_only() {
        let context = TestContext::new().await;
        let fixture = create_delivery(&context).await;
        let delivery = fixture.delivery;

        let resp = get_request(
            &context.pool,
            &context.normal_token,
            &format!("{DELIVERIES_ENDPOINT}/{}", delivery.id),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{DELIVERIES_ENDPOINT}/{}", delivery.id),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let fetched: EventDelivery = test::read_body_json(resp).await;
        assert_eq!(fetched.id, delivery.id);

        let resp = get_request(&context.pool, &context.admin_token, DELIVERIES_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let deliveries: Vec<EventDelivery> = test::read_body_json(resp).await;
        assert!(deliveries.iter().any(|row| row.id == delivery.id));

        let resp = get_request(
            &context.pool,
            &context.normal_token,
            &format!("{DELIVERIES_ENDPOINT}/health"),
        )
        .await;
        assert_response_status(resp, StatusCode::FORBIDDEN).await;
    }

    #[actix_web::test]
    async fn test_event_delivery_retry_and_dead_letter_operations() {
        let context = TestContext::new().await;
        let delivery = create_delivery(&context).await.delivery;

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{DELIVERIES_ENDPOINT}/{}/dead", delivery.id),
            json!({}),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: EventDeliveryUpdateResponse = test::read_body_json(resp).await;
        assert_eq!(body.delivery.status, EventDeliveryStatus::Dead.as_str());

        let resp = post_request(
            &context.pool,
            &context.admin_token,
            &format!("{DELIVERIES_ENDPOINT}/{}/retry", delivery.id),
            json!({}),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let body: EventDeliveryUpdateResponse = test::read_body_json(resp).await;
        assert_eq!(body.delivery.status, EventDeliveryStatus::Pending.as_str());
        assert!(body.delivery.last_error.is_none());
        assert!(body.delivery.claim_token.is_none());
    }

    #[actix_web::test]
    async fn test_event_delivery_health_reports_pipeline_aggregates() {
        let context = TestContext::new().await;
        let fixture = create_delivery(&context).await;

        let resp = get_request(
            &context.pool,
            &context.admin_token,
            &format!("{DELIVERIES_ENDPOINT}/health"),
        )
        .await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let health: EventDeliveryHealthResponse = test::read_body_json(resp).await;

        assert!(health.delivery.counts.pending >= 1);
        assert!(health.delivery.oldest_due_age_seconds.is_some());

        let sink = health
            .sinks
            .iter()
            .find(|row| row.sink_id == fixture.sink_id)
            .unwrap();
        assert_eq!(sink.sink_name, fixture.sink_name);
        assert_eq!(sink.sink_kind, EventSinkKind::Webhook.as_str());
        assert_eq!(sink.subscription_count, 1);
        assert_eq!(sink.counts.pending, 1);

        let subscription = health
            .subscriptions
            .iter()
            .find(|row| row.subscription_id == fixture.subscription_id)
            .unwrap();
        assert_eq!(subscription.subscription_name, fixture.subscription_name);
        assert_eq!(subscription.namespace_id, fixture.namespace_id);
        assert_eq!(subscription.sink_id, fixture.sink_id);
        assert_eq!(subscription.counts.pending, 1);
    }
}
