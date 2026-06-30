#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test};
    use serde_json::json;

    use crate::db::with_connection;
    use crate::events::{Action, ActorKind, EntityType, EventResponse, NewEvent, emit_event};
    use crate::models::{Permissions, PermissionsList};
    use crate::tests::TestContext;
    use crate::tests::api_operations::get_request;
    use crate::tests::asserts::{assert_response_status, header_value};
    use crate::tests::{create_test_group, create_test_user};
    use crate::traits::PermissionController;

    const EVENTS_ENDPOINT: &str = "/api/v1/events";

    fn emit_test_event(pool: &crate::db::DbPool, event: &NewEvent) -> EventResponse {
        with_connection(pool, |conn| emit_event(conn, event))
            .expect("failed to emit test event")
            .into()
    }

    #[actix_web::test]
    async fn test_events_endpoint_requires_read_audit_permission() {
        let context = TestContext::new().await;
        let namespace = context.namespace_fixture("audit_requires_permission").await;

        let event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Namespace,
                Action::Created,
                ActorKind::System,
                "namespace audit permission test",
            )
            .unwrap()
            .with_namespace_id(namespace.namespace.id)
            .with_entity_id(namespace.namespace.id)
            .with_entity_name(&namespace.namespace.name),
        );

        let resp = get_request(&context.pool, &context.normal_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.is_empty());

        let group = create_test_group(&context.pool).await;
        group
            .add_member(&context.pool, &context.normal_user)
            .await
            .unwrap();
        namespace
            .namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let resp = get_request(&context.pool, &context.normal_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == event.id));
    }

    #[actix_web::test]
    async fn test_events_endpoint_filters_and_hides_namespace_less_for_non_admin() {
        let context = TestContext::new().await;
        let namespace = context.namespace_fixture("audit_filters").await;

        let user = create_test_user(&context.pool).await;
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();
        let group = create_test_group(&context.pool).await;
        group.add_member(&context.pool, &user).await.unwrap();
        namespace
            .namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let namespaced_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Namespace,
                Action::Created,
                ActorKind::System,
                "namespace audit filter test",
            )
            .unwrap()
            .with_namespace_id(namespace.namespace.id)
            .with_entity_id(namespace.namespace.id)
            .with_entity_name(&namespace.namespace.name),
        );
        let namespace_less_event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::Token,
                Action::Created,
                ActorKind::System,
                "namespace-less audit filter test",
            )
            .unwrap(),
        );

        let endpoint = format!(
            "{EVENTS_ENDPOINT}?entity_type=namespace&action=created&namespace_id={}",
            namespace.namespace.id
        );
        let resp = get_request(&context.pool, &user_token, &endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let total_count = header_value(&resp, "X-Total-Count")
            .and_then(|value| value.parse::<i64>().ok())
            .expect("events endpoint should include a valid X-Total-Count header");
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert_eq!(total_count, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, namespaced_event.id);

        let resp = get_request(&context.pool, &user_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(!rows.iter().any(|row| row.id == namespace_less_event.id));

        let admin_endpoint = format!("{EVENTS_ENDPOINT}?entity_type=token&action=created");
        let resp = get_request(&context.pool, &context.admin_token, &admin_endpoint).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == namespace_less_event.id));
    }

    #[actix_web::test]
    async fn test_events_endpoint_includes_related_namespace_events() {
        let context = TestContext::new().await;
        let namespaces = context.namespace_fixtures("audit_related", 2).await;
        let source_namespace = &namespaces[0].namespace;
        let related_namespace = &namespaces[1].namespace;

        let user = create_test_user(&context.pool).await;
        let user_token = user.create_token(&context.pool).await.unwrap().get_token();
        let group = create_test_group(&context.pool).await;
        group.add_member(&context.pool, &user).await.unwrap();
        related_namespace
            .grant(
                &context.pool,
                group.id,
                PermissionsList::new([Permissions::ReadAudit]),
            )
            .await
            .unwrap();

        let event = emit_test_event(
            &context.pool,
            &NewEvent::new(
                EntityType::ClassRelation,
                Action::Created,
                ActorKind::System,
                "related namespace audit test",
            )
            .unwrap()
            .with_namespace_id(source_namespace.id)
            .with_entity_id(source_namespace.id)
            .with_metadata(json!({
                "related_namespace_ids": [related_namespace.id],
            })),
        );

        let resp = get_request(&context.pool, &user_token, EVENTS_ENDPOINT).await;
        let resp = assert_response_status(resp, StatusCode::OK).await;
        let rows: Vec<EventResponse> = test::read_body_json(resp).await;
        assert!(rows.iter().any(|row| row.id == event.id));
    }
}
