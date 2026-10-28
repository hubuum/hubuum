use super::*;
use crate::config::running::RunningConfig;
use crate::tests::integration_test_config;
use hubuum_storage_core::{
    StorageTaskKind, StorageTaskPolicyUpdate, TaskAdmissionPolicy, TaskCapacityLimit,
    TaskCapacityStorage, TaskKindCapacity,
};

fn context(storage: &StorageHandle) -> Data<AppContext> {
    Data::new(AppContext::new(
        storage.clone(),
        Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into())),
    ))
}

#[rstest::rstest]
#[case::dedicated("/api/v1/admin/task-capacity")]
#[case::configuration("/api/v1/admin/config")]
#[actix_web::test]
async fn admin_task_capacity_reads_authoritative_policy(#[case] path: &str) {
    let storage = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&storage, &prefix("capacity_admin")).await;
    let _ = storage
        .add_group_member(user.principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap();
    let policy = TaskAdmissionPolicy::default();
    let limits = policy.for_kind(StorageTaskKind::Import).capacity();
    let policy = policy.with_kind(TaskKindCapacity::new(
        StorageTaskKind::Import,
        limits,
        TaskCapacityLimit::new(7).unwrap(),
    ));
    let expected = storage.initialize_task_policy(policy).await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&storage))
            .app_data(Data::new(RunningConfig::from(
                &integration_test_config().unwrap(),
            )))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::get()
        .uri(path)
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body: serde_json::Value = test::read_body_json(response).await;
    let snapshot = if path.ends_with("/config") {
        assert_eq!(body["tasks"]["import_max_active_per_user"], 7);
        &body["task_capacity"]
    } else {
        &body
    };
    assert_eq!(snapshot["policy"], serde_json::to_value(expected).unwrap());
}

#[rstest::rstest]
#[case::read(http::Method::GET)]
#[case::write(http::Method::PUT)]
#[actix_web::test]
async fn ordinary_users_cannot_read_or_change_task_capacity(#[case] method: http::Method) {
    let storage = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&storage, &prefix("capacity_user")).await;
    let policy = storage.task_policy().await.unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&storage))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::default()
        .method(method)
        .uri("/api/v1/admin/task-capacity")
        .insert_header((
            http::header::AUTHORIZATION,
            format!("Bearer {}", user.raw_token),
        ))
        .set_json(StorageTaskPolicyUpdate::new(
            policy.revision(),
            policy.policy().clone(),
        ))
        .send_request(&app)
        .await;
    assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
}

#[rstest::rstest]
#[case::invalid(0, http::StatusCode::BAD_REQUEST)]
#[case::stale(2, http::StatusCode::CONFLICT)]
#[case::valid(1, http::StatusCode::OK)]
#[actix_web::test]
async fn administrator_policy_updates_validate_the_revision(
    #[case] revision: u64,
    #[case] expected: http::StatusCode,
) {
    let storage = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&storage, &prefix("capacity_writer")).await;
    let _ = storage
        .add_group_member(user.principal_id, group_id(1), &EventContext::system())
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(context(&storage))
            .configure(crate::api::config),
    )
    .await;
    let response = test::TestRequest::put().uri("/api/v1/admin/task-capacity")
        .insert_header((http::header::AUTHORIZATION, format!("Bearer {}", user.raw_token)))
        .set_json(serde_json::json!({"expected_revision":revision,"policy":TaskAdmissionPolicy::default()})).send_request(&app).await;
    assert_eq!(response.status(), expected);
}
