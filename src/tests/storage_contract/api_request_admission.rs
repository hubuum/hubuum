use super::*;
use hubuum_storage_core::{
    ApiRequestAdmissionStorage, ApiRequestClass, ApiRequestDuration, ApiRequestRejection,
    StorageApiRequestPolicyUpdate, TaskCapacityLimit,
};

struct DelayedAdmissionResponse;

#[actix_web::test]
async fn missing_principal_identity_has_the_same_admission_reason_on_every_backend() {
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("api_missing_identity")).await;
        let principal = user.principal_id;
        delete_backend_user(&backend, user).await;
        let error = backend
            .admit_api_request(principal, ApiRequestClass::History, 0)
            .await
            .unwrap_err();
        assert_eq!(
            error.api_request_rejection(),
            Some(ApiRequestRejection::IdentityUnavailable)
        );
    }
}

impl crate::storage::StorageObserver for DelayedAdmissionResponse {
    fn operation_finished(&self, observation: &crate::storage::StorageObservation) {
        if observation.operation() == "admit_api_request" {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[actix_web::test]
async fn delayed_admission_does_not_refresh_execution_allowance() {
    use actix_web::{HttpResponse, middleware::from_fn, web};

    let backend = StorageHandle::from_registered_backend_with_observer(
        MemoryStorage::new(),
        Arc::new(DelayedAdmissionResponse),
    );
    let user = create_backend_user(&backend, &prefix("api_admission_delay")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_duration(ApiRequestDuration::new(10).unwrap()),
        ))
        .await
        .unwrap();
    let context = Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let route_calls = calls.clone();
    let app = test::init_service(
        App::new()
            .app_data(context)
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(crate::middlewares::TracingMiddleware::new())
            .route(
                "/api/v1/classes/42/object-aggregates",
                web::get().to(move || {
                    let calls = route_calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        HttpResponse::Ok().finish()
                    }
                }),
            ),
    )
    .await;
    let request = test::TestRequest::get()
        .uri("/api/v1/classes/42/object-aggregates")
        .insert_header(("Authorization", format!("Bearer {}", user.raw_token)))
        .insert_header(("X-Correlation-ID", "delayed-admission"))
        .to_request();
    let error = test::try_call_service(&app, request).await.unwrap_err();
    let response = error.error_response();
    assert_eq!(response.status(), http::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response.headers().get("X-Correlation-ID").unwrap(),
        "delayed-admission"
    );
    assert!(response.headers().contains_key("X-Request-ID"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    delete_backend_user(&backend, user).await;
}

#[actix_web::test]
async fn expired_request_retains_occupancy_until_explicit_settlement() {
    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_expired_occupancy")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_principal(TaskCapacityLimit::new(1).unwrap())
                .with_duration(ApiRequestDuration::new(1).unwrap()),
        ))
        .await
        .unwrap();
    let permit = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Aggregate, 0)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let error = backend
        .admit_api_request(user.principal_id, ApiRequestClass::History, 0)
        .await
        .unwrap_err();
    assert_eq!(
        error.api_request_rejection(),
        Some(ApiRequestRejection::Principal)
    );
    backend.settle_api_request(permit).await.unwrap();
    let next = backend
        .admit_api_request(user.principal_id, ApiRequestClass::History, 0)
        .await
        .unwrap();
    backend.settle_api_request(next).await.unwrap();
    delete_backend_user(&backend, user).await;
}

#[actix_web::test]
async fn disconnected_socket_does_not_leak_request_occupancy() {
    use actix_web::{HttpResponse, HttpServer, middleware::from_fn, web};
    use tokio::io::AsyncWriteExt;

    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_socket_disconnect")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_deployment(TaskCapacityLimit::new(1).unwrap())
                .with_principal(TaskCapacityLimit::new(1).unwrap())
                .with_duration(ApiRequestDuration::new(500).unwrap()),
        ))
        .await
        .unwrap();
    let context = Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
    ));
    let entered = Arc::new(Notify::new());
    let route_entered = entered.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        let entered = route_entered.clone();
        App::new()
            .app_data(context.clone())
            .wrap(from_fn(crate::middlewares::actor_context))
            .route(
                "/api/v1/classes/42/object-aggregates",
                web::get().to(move || {
                    let entered = entered.clone();
                    async move {
                        entered.notify_one();
                        tokio::time::sleep(Duration::from_secs(30)).await;
                        HttpResponse::Ok().finish()
                    }
                }),
            )
    })
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("GET /api/v1/classes/42/object-aggregates HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\n\r\n",user.raw_token).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    drop(socket);
    let recovered = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match backend
                .admit_api_request(user.principal_id, ApiRequestClass::Aggregate, 0)
                .await
            {
                Ok(permit) => break permit,
                Err(error)
                    if matches!(
                        error.api_request_rejection(),
                        Some(ApiRequestRejection::Deployment | ApiRequestRejection::Principal)
                    ) =>
                {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("Unexpected request recovery error: {error}"),
            }
        }
    })
    .await;
    handle.stop(true).await;
    server.await.unwrap().unwrap();
    backend
        .settle_api_request(recovered.expect("disconnected occupancy must settle"))
        .await
        .unwrap();
    delete_backend_user(&backend, user).await;
}

#[actix_web::test]
async fn concurrent_requests_share_one_principal_ceiling() {
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("api_capacity_concurrent")).await;
        let attempts =
            futures::future::join_all((0..8).map(|_| {
                backend.admit_api_request(user.principal_id, ApiRequestClass::Aggregate, 0)
            }))
            .await;
        let permits = attempts
            .into_iter()
            .filter_map(|attempt| match attempt {
                Ok(permit) => Some(permit),
                Err(error) => {
                    assert_eq!(
                        error.api_request_rejection(),
                        Some(ApiRequestRejection::Principal)
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(permits.len(), 2);
        for permit in permits {
            backend.settle_api_request(permit).await.unwrap();
        }
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn settled_owner_cannot_execute_under_a_new_reservation() {
    for backend in available_backends() {
        let user = create_backend_user(&backend, &prefix("api_capacity_fenced")).await;
        let stale = backend
            .admit_api_request(user.principal_id, ApiRequestClass::History, 0)
            .await
            .unwrap();
        backend.settle_api_request(stale.clone()).await.unwrap();
        let current = backend
            .admit_api_request(user.principal_id, ApiRequestClass::History, 0)
            .await
            .unwrap();
        backend.settle_api_request(stale.clone()).await.unwrap();
        let scope = backend.current_execution_scope().with_api_request(stale);
        let error = backend
            .run_in_scope(scope, backend.get_user(user.id))
            .await
            .unwrap_err();
        assert_eq!(
            error.api_request_rejection(),
            Some(ApiRequestRejection::ReconciliationRequired)
        );
        backend.settle_api_request(current).await.unwrap();
        delete_backend_user(&backend, user).await;
    }
}

#[actix_web::test]
async fn policy_reduction_drains_existing_owners() {
    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_capacity_drain")).await;
    let admitted = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Audit, 0)
        .await
        .unwrap();
    let initial = backend.api_request_policy().await.unwrap();
    let policy = initial
        .policy()
        .clone()
        .with_deployment(TaskCapacityLimit::new(1).unwrap())
        .with_principal(TaskCapacityLimit::new(2).unwrap());
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            policy,
        ))
        .await
        .unwrap();
    let error = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Audit, 0)
        .await
        .unwrap_err();
    assert_eq!(
        error.api_request_rejection(),
        Some(ApiRequestRejection::Deployment)
    );
    backend
        .run_in_scope(
            backend
                .current_execution_scope()
                .with_api_request(admitted.clone()),
            backend.get_user(user.id),
        )
        .await
        .unwrap();
    backend.settle_api_request(admitted).await.unwrap();
    let replacement = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Audit, 0)
        .await
        .unwrap();
    backend.settle_api_request(replacement).await.unwrap();
}

#[actix_web::test]
async fn expired_owner_fails_before_storage_work() {
    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_capacity_deadline")).await;
    let initial = backend.api_request_policy().await.unwrap();
    let policy = initial
        .policy()
        .clone()
        .with_duration(ApiRequestDuration::new(1).unwrap());
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            policy,
        ))
        .await
        .unwrap();
    let permit = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Aggregate, 0)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let error = backend
        .run_in_scope(
            backend
                .current_execution_scope()
                .with_api_request(permit.clone()),
            backend.get_user(user.id),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.api_request_rejection(),
        Some(ApiRequestRejection::DeadlineExceeded)
    );
    backend.settle_api_request(permit).await.unwrap();
}
