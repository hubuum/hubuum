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
                web::get().to(move |context: Data<AppContext>| {
                    let entered = entered.clone();
                    async move {
                        let scope = context.backend().current_execution_scope();
                        let execution = scope
                            .task_execution_override()
                            .and_then(Option::as_ref)
                            .expect("request execution context must reach the handler");
                        entered.notify_one();
                        while execution.check().is_ok() {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
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

#[rstest::rstest]
#[case::aggregate(
    "GET",
    "/api/v1/classes/42/object-aggregates",
    "/api/v1/classes/42/%6Fbject-aggregates"
)]
#[case::import("POST", "/api/v1/imports", "/api/v1/%69mports")]
#[actix_web::test]
async fn request_admission_uses_the_router_path(
    #[case] method: &str,
    #[case] route: &'static str,
    #[case] encoded: &str,
) {
    use actix_web::{HttpResponse, HttpServer, middleware::from_fn, web};

    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_encoded_route")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_principal(TaskCapacityLimit::new(1).unwrap()),
        ))
        .await
        .unwrap();
    let occupied = backend
        .admit_api_request(user.principal_id, ApiRequestClass::Aggregate, 0)
        .await
        .unwrap();
    let context = Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
    ));
    let method: http::Method = method.parse().unwrap();
    let server_method = method.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let route_calls = calls.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        let calls = route_calls.clone();
        App::new()
            .app_data(context.clone())
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(
                crate::middlewares::maintenance::reject_during_maintenance,
            ))
            .route(
                route,
                web::route().method(server_method.clone()).to(move || {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        HttpResponse::Ok().finish()
                    }
                }),
            )
    })
    .disable_signals()
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .request(
            reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap(),
            format!("http://{address}{encoded}"),
        )
        .bearer_auth(&user.raw_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["reason"], "api_capacity_principal");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    handle.stop(true).await;
    server.await.unwrap().unwrap();
    backend.settle_api_request(occupied).await.unwrap();
    delete_backend_user(&backend, user).await;
}

#[actix_web::test]
async fn request_admission_retains_blocking_preparation_until_completion() {
    use actix_web::{HttpResponse, middleware::from_fn, web};
    use hubuum_task_core::TaskStopReason;

    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_blocking_preparation")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_principal(TaskCapacityLimit::new(1).unwrap())
                .with_duration(ApiRequestDuration::new(50).unwrap()),
        ))
        .await
        .unwrap();
    let context = Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
    ));
    let entered = Arc::new(Notify::new());
    let route_entered = entered.clone();
    let release = Arc::new(std::sync::Barrier::new(2));
    let route_release = release.clone();
    let finished = Arc::new(AtomicUsize::new(0));
    let route_finished = finished.clone();
    let app = test::init_service(
        App::new()
            .app_data(context)
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(
                crate::middlewares::maintenance::reject_during_maintenance,
            ))
            .route(
                "/api/v1/imports",
                web::post().to(move |context: Data<AppContext>| {
                    let entered = route_entered.clone();
                    let release = route_release.clone();
                    let finished = route_finished.clone();
                    async move {
                        let execution = context
                            .backend()
                            .current_execution_scope()
                            .task_execution_override()
                            .and_then(Option::as_ref)
                            .unwrap()
                            .clone();
                        let result = tokio::task::spawn_blocking(move || {
                            entered.notify_one();
                            release.wait();
                            // The original request stop reaches issued preparation.
                            let stopped = execution.check();
                            finished.fetch_add(1, Ordering::SeqCst);
                            stopped
                        })
                        .await
                        .unwrap();
                        assert_eq!(result, Err(TaskStopReason::DeadlineExceeded));
                        HttpResponse::Ok().finish()
                    }
                }),
            ),
    )
    .await;
    let request = test::TestRequest::post()
        .uri("/api/v1/imports")
        .insert_header(("Authorization", format!("Bearer {}", user.raw_token)))
        .to_request();
    let waiting = actix_web::rt::spawn(async move { test::try_call_service(&app, request).await });
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let replacement = backend
        .admit_api_request(user.principal_id, ApiRequestClass::ImportSubmission, 0)
        .await;
    let completed_early = waiting.is_finished();
    let finished_early = finished.load(Ordering::SeqCst);
    // Always unblock the independently running job before asserting, so a
    // regression cannot strand a blocking worker and hang runtime teardown.
    tokio::task::spawn_blocking(move || release.wait())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !completed_early,
        "deadline must retain the awaiting handler"
    );
    assert_eq!(finished_early, 0);
    assert_eq!(
        replacement.unwrap_err().api_request_rejection(),
        Some(ApiRequestRejection::Principal)
    );
    assert_eq!(
        result.unwrap_err().error_response().status(),
        http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(finished.load(Ordering::SeqCst), 1);
    let replacement = backend
        .admit_api_request(user.principal_id, ApiRequestClass::ImportSubmission, 0)
        .await
        .unwrap();
    backend.settle_api_request(replacement).await.unwrap();
    delete_backend_user(&backend, user).await;
}

#[rstest::rstest]
#[case::import("/api/v1/imports", ApiRequestClass::ImportSubmission)]
#[case::export("/api/v1/exports", ApiRequestClass::ExportSubmission)]
#[actix_web::test]
async fn stalled_submission_body_releases_capacity_before_client_close(
    #[case] endpoint: &'static str,
    #[case] class: ApiRequestClass,
) {
    use actix_web::{HttpResponse, HttpServer, middleware::from_fn, web};
    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use uuid::Uuid;

    let backend = StorageHandle::from_registered_backend(MemoryStorage::new());
    let user = create_backend_user(&backend, &prefix("api_stalled_body")).await;
    let initial = backend.api_request_policy().await.unwrap();
    backend
        .replace_api_request_policy(StorageApiRequestPolicyUpdate::new(
            initial.revision(),
            initial
                .policy()
                .clone()
                .with_principal(TaskCapacityLimit::new(1).unwrap())
                .with_duration(ApiRequestDuration::new(100).unwrap()),
        ))
        .await
        .unwrap();
    let context = Data::new(AppContext::new(
        backend.clone(),
        Arc::new(LocalPermissionBackend::new(backend.clone(), "admin".into())),
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let route_calls = calls.clone();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        let calls = route_calls.clone();
        App::new()
            .app_data(context.clone())
            .app_data(web::JsonConfig::default().error_handler(crate::errors::json_error_handler))
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(
                crate::middlewares::maintenance::reject_during_maintenance,
            ))
            .wrap(crate::middlewares::TracingMiddleware::new())
            .route(
                endpoint,
                web::post().to(move |_body: web::Json<Value>| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        HttpResponse::Ok().finish()
                    }
                }),
            )
    })
    .disable_signals()
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST {endpoint} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: 100\r\nX-Correlation-ID: stalled-submission\r\n\r\n{{\"data\":",user.raw_token).as_bytes()).await.unwrap();
    // The client deliberately sends neither the remaining bytes nor EOF.
    let received = tokio::time::timeout(Duration::from_secs(3), async {
        let mut reader = BufReader::new(&mut socket);
        let mut headers = String::new();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await? == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            headers.push_str(&line);
            if line == "\r\n" {
                break;
            }
        }
        let length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .expect("JSON error response must have a known body length")
            .1
            .trim()
            .parse::<usize>()
            .unwrap();
        let mut body = vec![0; length];
        reader.read_exact(&mut body).await?;
        Ok::<_, std::io::Error>((headers, body))
    })
    .await;
    // Check recovery while the original client is still open, even if reading
    // failed, then close it for cleanup before reporting any assertion failure.
    let replacement = backend.admit_api_request(user.principal_id, class, 0).await;
    drop(socket);
    handle.stop(true).await;
    server.await.unwrap().unwrap();
    backend
        .settle_api_request(replacement.expect("capacity must recover before client close"))
        .await
        .unwrap();
    delete_backend_user(&backend, user).await;
    let (headers, body) = received
        .expect("stalled body must receive its deadline response")
        .unwrap();
    assert!(headers.starts_with("HTTP/1.1 422 "), "{headers}");
    let header = |wanted: &str| {
        headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case(wanted))
            .map(|(_, value)| value.trim().to_owned())
    };
    assert_eq!(
        header("x-correlation-id").as_deref(),
        Some("stalled-submission")
    );
    Uuid::parse_str(&header("x-request-id").expect("request ID must survive deadline response"))
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["reason"], "api_capacity_deadline_exceeded");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
