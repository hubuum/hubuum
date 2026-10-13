use std::sync::Arc;

use actix_web::{App, http::StatusCode, test, web::Data};
use hubuum_events_core::EventSubject;
use rstest::rstest;
use serde_json::{Value, json};

use crate::events::{DefaultSinkResolver, EntityType, EventResponse, SinkResolver};
use crate::models::User;
use crate::permissions::test_support::mock_treetop::MockTreetopBackend;
use crate::tests::api_operations::{delete_request, get_request, post_request};
use crate::tests::asserts::assert_response_status;
use crate::tests::{
    TestContext, app_context, app_context_with_permission_backend, create_test_group,
};

async fn login(context: &TestContext, user: &User, password: &str) -> Value {
    let response = post_request(
        &context.pool,
        "",
        "/api/v0/auth/login",
        &json!({
            "name": user.name(&context.pool).await.unwrap(), "password": password
        }),
    )
    .await;
    test::read_body_json(assert_response_status(response, StatusCode::OK).await).await
}

async fn events(context: &TestContext, user: &User) -> Vec<EventResponse> {
    crate::test_support::audit_events(&context.pool, EntityType::Authentication, user.id, None)
        .await
        .unwrap()
}

#[rstest]
#[case::local_admin(true, None, true)]
#[case::local_non_admin(false, None, false)]
#[case::external_denies_local_admin(true, Some(false), false)]
#[case::external_grants_non_local_admin(false, Some(true), true)]
#[actix_web::test]
async fn login_snapshots_selected_admin_policy(
    #[case] local_admin: bool,
    #[case] external_admin: Option<bool>,
    #[case] expected: bool,
) {
    let context = TestContext::new().await;
    let user = if local_admin {
        &context.admin_user
    } else {
        &context.normal_user
    };
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, user)
        .await
        .unwrap();
    let selected = match external_admin {
        None => app_context(&context.pool),
        Some(allowed) => {
            let policy = MockTreetopBackend::new();
            if allowed {
                policy.add_admin_rule(group.id);
            }
            Data::new(app_context_with_permission_backend(
                context.pool.get_ref().clone(),
                Arc::new(policy),
            ))
        }
    };
    let app = test::init_service(App::new().app_data(selected).configure(crate::api::config)).await;
    let response = test::TestRequest::post()
        .uri("/api/v0/auth/login")
        .set_json(json!({
            "name": user.name(&context.pool).await.unwrap(),
            "password": if local_admin { "testadminpassword" } else { "testpassword" }
        }))
        .send_request(&app)
        .await;
    assert_response_status(response, StatusCode::OK).await;
    let events = events(&context, user).await;
    assert_eq!(events.len(), 1);
    let subject: EventSubject =
        serde_json::from_value(events[0].metadata["subject"].clone()).unwrap();
    assert_eq!(subject.is_admin(), expected);
    assert_eq!(subject.principal_id().id(), user.id);
    assert_eq!(subject.name(), user.name(&context.pool).await.unwrap());
    assert_eq!(subject.identity_scope(), "local");
    assert_eq!(subject.authentication_provider_kind(), "local");
    assert_eq!(subject.kind(), crate::models::PrincipalKind::Human);
    assert_eq!(events[0].actor_user_id, Some(user.id));
    assert_eq!(events[0].actor_kind, "user");
    group.delete_without_events(&context.pool).await.unwrap();
}

#[rstest]
#[case::failed_password(false)]
#[case::bearer_reuse(true)]
#[actix_web::test]
async fn authentication_without_login_issuance_does_not_emit_success(#[case] reuse: bool) {
    let context = TestContext::new().await;
    let response = if reuse {
        get_request(
            &context.pool,
            &context.normal_token,
            "/api/v0/auth/validate",
        )
        .await
    } else {
        post_request(&context.pool, "", "/api/v0/auth/login", &json!({
            "name": context.normal_user.name(&context.pool).await.unwrap(), "password": "wrong-password"
        })).await
    };
    assert_response_status(
        response,
        if reuse {
            StatusCode::OK
        } else {
            StatusCode::UNAUTHORIZED
        },
    )
    .await;
    assert!(events(&context, &context.normal_user).await.is_empty());
}

#[actix_web::test]
async fn non_admin_cannot_read_even_their_own_login_event() {
    let context = TestContext::new().await;
    login(&context, &context.normal_user, "testpassword").await;
    let response = get_request(
        &context.pool,
        &context.normal_token,
        &format!(
            "/api/v1/events?entity_type=authentication&entity_id={}",
            context.normal_user.id
        ),
    )
    .await;
    let rows: Vec<Value> =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert!(rows.is_empty());
}

#[rstest]
#[case::admin(true)]
#[case::non_admin(false)]
#[actix_web::test]
async fn login_notification_matches_event_time_status_and_supports_preview_and_queue(
    #[case] admin: bool,
) {
    let context = TestContext::new().await;
    let user = if admin {
        &context.admin_user
    } else {
        &context.normal_user
    };
    let password = if admin {
        "testadminpassword"
    } else {
        "testpassword"
    };
    let original_name = user.name(&context.pool).await.unwrap();
    let body = login(&context, user, password).await;
    let events = events(&context, user).await;
    let event = &events[0];
    assert_eq!(event.collection_id, None);
    assert_eq!(event.action, "succeeded");

    let response = post_request(&context.pool, &context.admin_token, "/api/v1/event-sinks", &json!({
        "name": context.scoped_name("login_webhook"), "kind": "webhook",
        "config": {"destination_url": "https://example.invalid/slack", "body_template": r#"{"text":{{ (test_marker ~ summary) | tojson }},"subject":{{ event.metadata.subject | tojson }}}"#},
        "enabled": true
    })).await;
    let sink: Value =
        test::read_body_json(assert_response_status(response, StatusCode::CREATED).await).await;
    let subscription = json!({
        "name": context.scoped_name("admin_logins"), "sink_id": sink["id"],
        "entity_types": ["authentication"], "actions": ["succeeded"],
        "filter": {"subject_is_admin": true, "subject_principal_ids": [user.id], "subject_identity_scopes": ["local"]},
        "enabled": true
    });
    let response = post_request(
        &context.pool,
        &context.normal_token,
        "/api/v1/system-event-subscriptions",
        &subscription,
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/system-event-subscriptions",
        &subscription,
    )
    .await;
    let saved: Value =
        test::read_body_json(assert_response_status(response, StatusCode::CREATED).await).await;

    // Change membership before fan-out. A different administrator manages the
    // subscription so demoting the subject cannot affect the test's API access.
    let operator = crate::tests::create_test_admin(&context.pool).await;
    let operator_token = operator
        .create_token(&context.pool)
        .await
        .unwrap()
        .get_token();
    let admins = crate::tests::ensure_admin_group(&context.pool).await;
    if admin {
        admins
            .remove_member_without_events(user, &context.pool)
            .await
            .unwrap();
    } else {
        admins
            .add_member_without_events(&context.pool, user)
            .await
            .unwrap();
    }
    hubuum_storage_postgres::test_support::fanout_event(
        &context.pool,
        event.id.try_into().unwrap(),
    )
    .await
    .unwrap();
    let count = hubuum_storage_postgres::test_support::count_event_deliveries(
        &context.pool,
        event.id.try_into().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(count, i64::from(admin));
    if admin {
        let queued = hubuum_storage_postgres::test_support::load_event_delivery_for_event(
            &context.pool,
            event.id.try_into().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            i64::from(queued.subscription_id().id()),
            saved["id"].as_i64().unwrap()
        );
        assert_eq!(queued.status().as_str(), "pending");
        let settings = hubuum_domain::EventDeliverySettings::builder()
            .batch_size(1)
            .lock_timeout_ms(30_000)
            .transport_timeout_ms(1_000)
            .retry_backoff_base_ms(100)
            .retry_backoff_max_ms(1_000)
            .max_attempts(3)
            .build()
            .unwrap();
        let work = hubuum_storage_postgres::test_support::claim_event_delivery_by_id(
            &context.pool,
            queued.id(),
            settings,
        )
        .await
        .unwrap();
        let (_, envelope, subscription, sink) = work.into_parts();
        let resolver = DefaultSinkResolver::default();
        let notification = resolver
            .resolve("webhook")
            .unwrap()
            .prepare(&envelope, &subscription, &sink)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notification.payload()["subject"], event.metadata["subject"]);
        assert!(
            !notification
                .payload()
                .to_string()
                .contains(body["token"].as_str().unwrap())
        );
    }
    let response = post_request(
        &context.pool,
        &operator_token,
        &format!("/api/v1/event-sinks/{}/preview", sink["id"]),
        &json!({"subscription_id": saved["id"], "event_id": event.event_id}),
    )
    .await;
    let preview: Value =
        test::read_body_json(assert_response_status(response, StatusCode::OK).await).await;
    assert_eq!(preview["payload"]["subject"]["is_admin"], admin);
    assert_eq!(preview["payload"]["subject"]["name"], original_name);
    assert!(
        preview["payload"]["text"]
            .as_str()
            .unwrap()
            .contains("successfully logged in")
    );
    let serialized = serde_json::to_string(&(event, &preview)).unwrap();
    for secret in [
        password,
        user.password.as_deref().unwrap(),
        body["token"].as_str().unwrap(),
    ] {
        assert!(!serialized.contains(secret));
    }

    // A preview does not enqueue another delivery or re-evaluate membership.
    let response = delete_request(
        &context.pool,
        &operator_token,
        &format!("/api/v1/system-event-subscriptions/{}", saved["id"]),
    )
    .await;
    assert_response_status(response, StatusCode::NO_CONTENT).await;
    let response = delete_request(
        &context.pool,
        &operator_token,
        &format!("/api/v1/event-sinks/{}", sink["id"]),
    )
    .await;
    assert_response_status(response, StatusCode::NO_CONTENT).await;
    operator.delete_without_events(&context.pool).await.unwrap();
}
