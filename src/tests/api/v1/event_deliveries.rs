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
        EventDelivery, EventDeliveryStatus, EventDeliveryUpdateResponse, EventSinkID,
        EventSinkKind, NamespaceID, NewEventSink, NewEventSubscription,
    };
    use crate::tests::TestContext;
    use crate::tests::api_operations::{get_request, post_request};
    use crate::tests::asserts::assert_response_status;

    const DELIVERIES_ENDPOINT: &str = "/api/v1/event-deliveries";

    async fn create_delivery(context: &TestContext) -> EventDelivery {
        let fixture = context.namespace_fixture("delivery_api").await;
        let sink = NewEventSink {
            name: context.scoped_name("delivery_api_sink"),
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

        NewEventSubscription {
            sink_id: EventSinkID::new(sink.id).unwrap(),
            name: context.scoped_name("delivery_api_subscription"),
            description: String::new(),
            entity_types: vec![EntityType::Namespace.as_str().to_string()],
            actions: vec![Action::Created.as_str().to_string()],
            routing: json!({}),
            enabled: true,
        }
        .into_row(NamespaceID::new(fixture.namespace.id).unwrap())
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

        with_connection(&context.pool, |conn| {
            use crate::schema::event_deliveries::dsl::{event_deliveries, event_id};

            event_deliveries
                .filter(event_id.eq(event.id))
                .first::<EventDelivery>(conn)
        })
        .unwrap()
    }

    #[actix_web::test]
    async fn test_event_delivery_operations_are_admin_only() {
        let context = TestContext::new().await;
        let delivery = create_delivery(&context).await;

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
    }

    #[actix_web::test]
    async fn test_event_delivery_retry_and_dead_letter_operations() {
        let context = TestContext::new().await;
        let delivery = create_delivery(&context).await;

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
}
