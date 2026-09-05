use actix_web::http::StatusCode;
use hubuum_domain::ClassId;
use hubuum_storage_postgres::test_support::delete_task;
use rstest::rstest;
use serde_json::json;
use tracing::instrument::WithSubscriber;

use crate::models::{NewHubuumClass, NewHubuumObject, TaskID};
use crate::observability::tracing::test_support::TraceCapture;
use crate::services::computed_fields::class_computation_state_for;
use crate::services::tasks::find_task;
use crate::tests::api_operations::post_request;
use crate::tests::asserts::assert_response_status;
use crate::tests::{TestContext, test_context};

#[rstest]
#[case::create_definition(false)]
#[case::request_rebuild(true)]
#[tokio::test]
async fn computed_reindex_task_retains_the_http_request_trace(
    #[future(awt)] test_context: TestContext,
    #[case] rebuild: bool,
) {
    let context = test_context;
    let fixture = context
        .object_fixture(
            "request trace provenance",
            NewHubuumClass {
                collection_id: 0,
                name: context.scoped_name("trace class"),
                description: "trace provenance fixture".to_string(),
                json_schema: None,
                validate_schema: Some(false),
            },
            vec![NewHubuumObject {
                collection_id: 0,
                hubuum_class_id: 0,
                name: context.scoped_name("trace object"),
                description: "trace provenance fixture".to_string(),
                data: json!({"hostname": "example"}),
            }],
        )
        .await
        .unwrap();
    let capture = TraceCapture::new("debug");
    let endpoint = format!(
        "/api/v1/classes/{}/computed-fields{}",
        fixture.class.id,
        if rebuild { "/rebuild" } else { "" }
    );
    let response = post_request(
        &context.pool,
        &context.admin_token,
        &endpoint,
        json!({
            "key": "hostname", "label": "Hostname", "description": "",
            "operation": {"type": "first_non_null", "paths": ["/hostname"]},
            "result_type": "string", "enabled": true,
        }),
    )
    .with_subscriber(capture.dispatch())
    .await;
    assert_response_status(
        response,
        if rebuild {
            StatusCode::ACCEPTED
        } else {
            StatusCode::CREATED
        },
    )
    .await;
    let state = class_computation_state_for(&context.pool, ClassId::new(fixture.class.id).unwrap())
        .await
        .unwrap();
    let task_id = TaskID::new(state.active_task_id.unwrap()).unwrap();
    let task = find_task(&context.pool, task_id).await.unwrap();
    let spans = capture.spans();
    let request = spans
        .iter()
        .find(|span| span.name == "http.server.request")
        .unwrap();
    let link = task
        .trace_link
        .expect("queued reindex task must retain request provenance");

    assert_eq!(link.trace_id(), request.span_context.trace_id().to_string());
    assert_eq!(link.span_id(), request.span_context.span_id().to_string());
    delete_task(&context.pool, task_id).await.unwrap();
    fixture.cleanup().await.unwrap();
}
