use actix_web::{http::StatusCode, test};
use rstest::rstest;
use serde_json::{Value, json};

use crate::models::{CollectionEventSink, Group, GroupID, Permissions, PermissionsList};
use crate::tests::api_operations::{
    delete_request, get_request, patch_request, post_request, put_request,
};
use crate::tests::asserts::assert_response_status;
use crate::tests::{CollectionFixture, TestContext, create_test_group};
use crate::traits::PermissionController;

async fn delegate(
    context: &TestContext,
    collection: &CollectionFixture,
    permissions: &[Permissions],
) -> Group {
    let group = create_test_group(&context.pool).await;
    group
        .add_member_without_events(&context.pool, &context.normal_user)
        .await
        .unwrap();
    collection
        .collection
        .grant_without_events(
            &context.pool,
            GroupID::new(group.id).unwrap(),
            PermissionsList::new(permissions.iter().copied()),
        )
        .await
        .unwrap();
    group
}

fn webhook(context: &TestContext) -> Value {
    json!({"name": context.scoped_name("owned_hook"), "kind": "webhook", "config": {"destination_url": "https://example.test/private/webhook-token"}, "enabled": true})
}

#[actix_web::test]
async fn delegated_manager_can_create_and_subscribe_to_owned_webhook() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("self_service_hook").await;
    let group = delegate(
        &context,
        &collection,
        &[Permissions::ManageEventSubscription, Permissions::ReadAudit],
    )
    .await;
    let endpoint = format!(
        "/api/v1/collections/{}/event-sinks",
        collection.collection_id()
    );
    let response = post_request(
        &context.pool,
        &context.normal_token,
        &endpoint,
        webhook(&context),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let sink: CollectionEventSink = test::read_body_json(response).await;
    let subscriptions = format!(
        "/api/v1/collections/{}/event-subscriptions",
        collection.collection_id()
    );
    let response = post_request(&context.pool, &context.normal_token, &subscriptions,
        json!({"sink_id": sink.id, "name": "object changes", "entity_types": ["object"], "actions": ["updated"], "routing": {}})).await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let subscription: Value = test::read_body_json(response).await;
    delete_request(
        &context.pool,
        &context.normal_token,
        &format!("{subscriptions}/{}", subscription["id"]),
    )
    .await;
    delete_request(
        &context.pool,
        &context.normal_token,
        &format!("{endpoint}/{}", sink.id),
    )
    .await;
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[rstest]
#[case(vec![])]
#[case(vec![Permissions::ManageEventSubscription])]
#[case(vec![Permissions::ReadAudit])]
#[actix_web::test]
async fn collection_sink_creation_requires_management_and_audit(
    #[case] permissions: Vec<Permissions>,
) {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hook_permissions").await;
    let group = delegate(&context, &collection, &permissions).await;
    let response = post_request(
        &context.pool,
        &context.normal_token,
        &format!(
            "/api/v1/collections/{}/event-sinks",
            collection.collection_id()
        ),
        webhook(&context),
    )
    .await;
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[rstest]
#[case(json!({"url_secret_ref": "server_secret"}))]
#[case(json!({"destination_url": "http://example.test/hook"}))]
#[case(json!({}))]
#[actix_web::test]
async fn collection_webhook_rejects_unsafe_configuration(#[case] config: Value) {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hook_config").await;
    let group = delegate(
        &context,
        &collection,
        &[Permissions::ManageEventSubscription, Permissions::ReadAudit],
    )
    .await;
    let mut body = webhook(&context);
    body["config"] = config;
    let response = post_request(
        &context.pool,
        &context.normal_token,
        &format!(
            "/api/v1/collections/{}/event-sinks",
            collection.collection_id()
        ),
        body,
    )
    .await;
    assert_response_status(response, StatusCode::BAD_REQUEST).await;
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[actix_web::test]
async fn discovery_omits_ungranted_sinks_and_credentials() {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hook_discovery").await;
    let group = delegate(
        &context,
        &collection,
        &[Permissions::ManageEventSubscription, Permissions::ReadAudit],
    )
    .await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/event-sinks",
        webhook(&context),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let sink: Value = test::read_body_json(response).await;
    let endpoint = format!(
        "/api/v1/collections/{}/event-sinks",
        collection.collection_id()
    );
    let response = get_request(&context.pool, &context.normal_token, &endpoint).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    assert_eq!(
        test::read_body_json::<Vec<Value>, _>(response).await,
        Vec::<Value>::new()
    );
    let grant = format!(
        "/api/v1/event-sinks/{}/collections/{}",
        sink["id"],
        collection.collection_id()
    );
    assert_response_status(
        put_request(&context.pool, &context.admin_token, &grant, json!({})).await,
        StatusCode::NO_CONTENT,
    )
    .await;
    let response = get_request(&context.pool, &context.normal_token, &endpoint).await;
    let response = assert_response_status(response, StatusCode::OK).await;
    let rows: Vec<Value> = test::read_body_json(response).await;
    assert_eq!(
        rows,
        vec![
            json!({"id": sink["id"], "name": sink["name"], "kind": "webhook", "enabled": true, "collection_id": null, "revision": sink["revision"], "routing": "fixed"})
        ]
    );
    delete_request(
        &context.pool,
        &context.admin_token,
        &format!("/api/v1/event-sinks/{}", sink["id"]),
    )
    .await;
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[actix_web::test]
async fn collection_manager_cannot_edit_another_collections_sink() {
    let context = TestContext::new().await;
    let owner = context.collection_fixture("hook_owner").await;
    let other = context.collection_fixture("hook_other").await;
    let group = delegate(
        &context,
        &other,
        &[Permissions::ManageEventSubscription, Permissions::ReadAudit],
    )
    .await;
    let endpoint = format!("/api/v1/collections/{}/event-sinks", owner.collection_id());
    let response = post_request(
        &context.pool,
        &context.admin_token,
        &endpoint,
        webhook(&context),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let sink: Value = test::read_body_json(response).await;
    let response = patch_request(
        &context.pool,
        &context.normal_token,
        &format!(
            "/api/v1/collections/{}/event-sinks/{}",
            other.collection_id(),
            sink["id"]
        ),
        json!({"name": "stolen"}),
    )
    .await;
    assert_response_status(response, StatusCode::NOT_FOUND).await;
    delete_request(
        &context.pool,
        &context.admin_token,
        &format!("{endpoint}/{}", sink["id"]),
    )
    .await;
    owner.cleanup().await.unwrap();
    other.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}

#[rstest]
#[case::ungranted_create(false)]
#[case::revoked_edit(true)]
#[actix_web::test]
async fn subscription_mutations_require_current_sink_grant(#[case] edit_existing: bool) {
    let context = TestContext::new().await;
    let collection = context.collection_fixture("hook_authority").await;
    let group = delegate(
        &context,
        &collection,
        &[Permissions::ManageEventSubscription, Permissions::ReadAudit],
    )
    .await;
    let response = post_request(
        &context.pool,
        &context.admin_token,
        "/api/v1/event-sinks",
        webhook(&context),
    )
    .await;
    let response = assert_response_status(response, StatusCode::CREATED).await;
    let sink: Value = test::read_body_json(response).await;
    let endpoint = format!(
        "/api/v1/collections/{}/event-subscriptions",
        collection.collection_id()
    );
    let body = json!({"sink_id": sink["id"], "name": "changes", "entity_types": ["object"], "actions": ["updated"], "routing": {}});
    let mut subscription_id = None;
    let response = if edit_existing {
        let grant = format!(
            "/api/v1/event-sinks/{}/collections/{}",
            sink["id"],
            collection.collection_id()
        );
        assert_response_status(
            put_request(&context.pool, &context.admin_token, &grant, json!({})).await,
            StatusCode::NO_CONTENT,
        )
        .await;
        let created = post_request(&context.pool, &context.normal_token, &endpoint, body).await;
        let created = assert_response_status(created, StatusCode::CREATED).await;
        let created: Value = test::read_body_json(created).await;
        subscription_id = Some(created["id"].clone());
        assert_response_status(
            delete_request(&context.pool, &context.admin_token, &grant).await,
            StatusCode::NO_CONTENT,
        )
        .await;
        patch_request(
            &context.pool,
            &context.normal_token,
            &format!("{endpoint}/{}", created["id"]),
            json!({"name": "changed"}),
        )
        .await
    } else {
        post_request(&context.pool, &context.normal_token, &endpoint, body).await
    };
    assert_response_status(response, StatusCode::FORBIDDEN).await;
    if let Some(id) = subscription_id {
        assert_response_status(
            delete_request(
                &context.pool,
                &context.normal_token,
                &format!("{endpoint}/{id}"),
            )
            .await,
            StatusCode::NO_CONTENT,
        )
        .await;
    }
    assert_response_status(
        delete_request(
            &context.pool,
            &context.admin_token,
            &format!("/api/v1/event-sinks/{}", sink["id"]),
        )
        .await,
        StatusCode::NO_CONTENT,
    )
    .await;
    collection.cleanup().await.unwrap();
    group.delete_without_events(&context.pool).await.unwrap();
}
