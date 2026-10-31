use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use actix_web::body::to_bytes;
use actix_web::error::ErrorInternalServerError;
use actix_web::{App, HttpResponse, HttpServer, middleware::from_fn, test as actix_test, web};
use futures_util::future::{pending, poll_fn, ready};
use futures_util::{FutureExt, stream};
use hubuum_storage_core::{DeletionControlStorage, DeletionSubject};
use rstest::rstest;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use uuid::Uuid;

use crate::permissions::LocalPermissionBackend;
use crate::storage::StorageHandle;

use super::*;

fn response() -> ServiceResponse<BoxBody> {
    actix_test::TestRequest::default().to_srv_response(HttpResponse::Ok().body("response"))
}

#[actix_web::test]
async fn successful_nonempty_http_json_response_confirms_settlement() {
    let storage = StorageHandle::memory();
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage, permissions);
    let state = Arc::new(HttpWorkState::default());
    let server_state = Arc::clone(&state);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = HttpServer::new(move || {
        let request_state = Arc::clone(&server_state);
        App::new()
            .app_data(Data::new(context.clone()))
            .wrap(from_fn(move |request, next| {
                reject_with_activity(
                    request,
                    next,
                    || MaintenanceActivityGuard::isolated().1,
                    HttpWorkGuard::begin_with_state(Arc::clone(&request_state)),
                )
            }))
            .route(
                "/",
                web::get().to(|| async {
                    HttpResponse::Ok().json(serde_json::json!({ "settled": true }))
                }),
            )
    })
    .disable_signals()
    .workers(1)
    .listen(listener)
    .unwrap()
    .run();
    let handle = server.handle();
    let server = actix_rt::spawn(server);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let body = client
        .get(format!("http://{address}/"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(body, serde_json::json!({ "settled": true }));
    drop(client);
    timeout(Duration::from_secs(5), handle.stop(true))
        .await
        .expect("HTTP shutdown must finish");
    server.await.unwrap().unwrap();

    assert!(state.is_settled());
}

#[rstest]
#[case::normal(false, 200)]
#[case::quarantined(true, 503)]
#[actix_web::test]
async fn restore_status_is_uncounted_but_obeys_recovery_quarantine(
    #[case] quarantined: bool,
    #[case] expected_status: u16,
) {
    let storage = StorageHandle::memory();
    if quarantined {
        let initial = storage.read_deletion_control().await.unwrap();
        let change = initial
            .record_intent(
                Uuid::new_v4(),
                DeletionSubject::Version {
                    identity: serde_json::from_value(serde_json::json!(Uuid::new_v4())).unwrap(),
                },
                vec![Uuid::new_v4()],
            )
            .unwrap();
        storage.commit_deletion_control(change).await.unwrap();
    }
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let app = actix_test::init_service(
        App::new()
            .app_data(Data::new(AppContext::new(storage, permissions)))
            .wrap(from_fn(|request, next| {
                reject_with_activity(
                    request,
                    next,
                    || panic!("restore status polling must not acquire drain activity"),
                    HttpWorkGuard::isolated().1,
                )
            }))
            .route(
                "/api/v1/restores/12/status",
                web::get().to(|| async {
                    HttpResponse::Ok().streaming(stream::pending::<Result<Bytes, Error>>())
                }),
            ),
    )
    .await;

    let response = actix_test::call_service(
        &app,
        actix_test::TestRequest::get()
            .uri("/api/v1/restores/12/status")
            .to_request(),
    )
    .await;
    assert_eq!(response.status().as_u16(), expected_status);
    // Keep a normal status body unfinished: its lifetime must not block draining.
    if !quarantined {
        let mut body = response.into_body();
        assert!(
            poll_fn(|context| body.as_pin_mut().poll_next(context))
                .now_or_never()
                .is_none()
        );
    }
}

#[actix_web::test]
async fn pending_handler_keeps_maintenance_activity_until_response_consumption() {
    let (count, guard) = MaintenanceActivityGuard::isolated();
    let (state, http_work) = HttpWorkGuard::isolated();
    let (complete, completed) = oneshot::channel();
    let mut request = Box::pin(protect_response(Some(guard), http_work, async {
        completed.await.unwrap();
        Ok(response())
    }));

    assert!(request.as_mut().now_or_never().is_none());
    assert_eq!(count.load(Ordering::Acquire), 1);
    complete.send(()).unwrap();
    let response = request.await.unwrap();
    assert_eq!(count.load(Ordering::Acquire), 1);
    assert_eq!(to_bytes(response.into_body()).await.unwrap(), "response");
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(state.is_settled());
}

#[actix_web::test]
async fn materialized_body_drop_settles_http_work_and_retains_activity_until_drop() {
    let (count, guard) = MaintenanceActivityGuard::isolated();
    let (state, http_work) = HttpWorkGuard::isolated();
    let response = protect_response(Some(guard), http_work, ready(Ok(response())))
        .await
        .unwrap();

    assert!(state.is_settled());
    assert_eq!(count.load(Ordering::Acquire), 1);
    drop(response);
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(state.is_settled());
}

#[actix_web::test]
async fn cancelling_a_pending_handler_releases_maintenance_activity() {
    let (count, guard) = MaintenanceActivityGuard::isolated();
    let (state, http_work) = HttpWorkGuard::isolated();
    let mut request = Box::pin(protect_response(
        Some(guard),
        http_work,
        pending::<Result<ServiceResponse<BoxBody>, Error>>(),
    ));

    assert!(request.as_mut().now_or_never().is_none());
    assert_eq!(count.load(Ordering::Acquire), 1);
    drop(request);
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(!state.is_settled());
}

#[actix_web::test]
async fn handler_error_releases_maintenance_activity() {
    let (count, guard) = MaintenanceActivityGuard::isolated();
    let (state, http_work) = HttpWorkGuard::isolated();
    let response = protect_response(
        Some(guard),
        http_work,
        ready(Err::<ServiceResponse<BoxBody>, _>(
            ErrorInternalServerError("handler failed"),
        )),
    )
    .await;

    assert!(response.is_err());
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert!(state.is_settled());
}

enum StreamEnd {
    Finished,
    Error,
    Disconnected,
}

#[rstest]
#[case::finished(StreamEnd::Finished)]
#[case::error(StreamEnd::Error)]
#[case::disconnected(StreamEnd::Disconnected)]
#[actix_web::test]
async fn streaming_response_retains_activity_until_settled(#[case] end: StreamEnd) {
    let (count, guard) = MaintenanceActivityGuard::isolated();
    let (state, http_work) = HttpWorkGuard::isolated();
    let (sender, receiver) = mpsc::unbounded_channel::<Result<Bytes, Error>>();
    let stream = stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|chunk| (chunk, receiver))
    });
    let response =
        actix_test::TestRequest::default().to_srv_response(HttpResponse::Ok().streaming(stream));
    let mut body = protect_response(Some(guard), http_work, ready(Ok(response)))
        .await
        .unwrap()
        .into_body();

    assert!(
        poll_fn(|context| body.as_pin_mut().poll_next(context))
            .now_or_never()
            .is_none()
    );
    assert_eq!(count.load(Ordering::Acquire), 1);
    sender.send(Ok(Bytes::from_static(b"first"))).unwrap();
    assert_eq!(
        poll_fn(|context| body.as_pin_mut().poll_next(context))
            .await
            .unwrap()
            .unwrap(),
        "first"
    );
    assert_eq!(count.load(Ordering::Acquire), 1);

    let should_settle = !matches!(end, StreamEnd::Disconnected);
    match end {
        StreamEnd::Finished => {
            drop(sender);
            assert!(
                poll_fn(|context| body.as_pin_mut().poll_next(context))
                    .await
                    .is_none()
            );
        }
        StreamEnd::Error => {
            sender
                .send(Err(ErrorInternalServerError("body failed")))
                .unwrap();
            assert!(
                poll_fn(|context| body.as_pin_mut().poll_next(context))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(
                poll_fn(|context| body.as_pin_mut().poll_next(context))
                    .await
                    .is_none()
            );
        }
        StreamEnd::Disconnected => drop(body),
    }
    assert_eq!(count.load(Ordering::Acquire), 0);
    assert_eq!(state.is_settled(), should_settle);
}

#[rstest]
#[case::no_body(HttpResponse::Ok().finish())]
#[case::empty_body(HttpResponse::Ok().body(""))]
#[actix_web::test]
async fn empty_response_settles_without_body_polling(#[case] response: HttpResponse) {
    let (state, http_work) = HttpWorkGuard::isolated();
    let response = actix_test::TestRequest::default().to_srv_response(response);
    drop(
        protect_response(None, http_work, ready(Ok(response)))
            .await
            .unwrap(),
    );

    assert!(state.is_settled());
}

#[actix_web::test]
async fn uncounted_restore_confirmation_cancellation_remains_uncertain() {
    let storage = StorageHandle::memory();
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let state = Arc::new(HttpWorkState::default());
    let request_state = Arc::clone(&state);
    let app = actix_test::init_service(
        App::new()
            .app_data(Data::new(AppContext::new(storage, permissions)))
            .wrap(from_fn(move |request, next| {
                reject_with_activity(
                    request,
                    next,
                    || panic!("restore confirmation must not acquire drain activity"),
                    HttpWorkGuard::begin_with_state(Arc::clone(&request_state)),
                )
            }))
            .route(
                "/api/v1/restores/12/confirm",
                web::post().to(pending::<HttpResponse>),
            ),
    )
    .await;
    let mut request = Box::pin(actix_test::call_service(
        &app,
        actix_test::TestRequest::post()
            .uri("/api/v1/restores/12/confirm")
            .to_request(),
    ));
    assert!(request.as_mut().now_or_never().is_none());
    assert_eq!(state.active.load(Ordering::Acquire), 1);
    drop(request);

    assert_eq!(state.active.load(Ordering::Acquire), 0);
    assert!(!state.is_settled());
}

#[rstest]
#[case::health("/healthz", None, true)]
#[case::readiness("/readyz", None, true)]
#[case::default_metrics("/metrics", None, true)]
#[case::custom_metrics("/internal/metrics", Some("/internal/metrics"), true)]
#[case::restore_status("/api/v1/restores/12/status", None, true)]
#[case::restore_confirmation("/api/v1/restores/12/confirm", None, false)]
#[case::ordinary_api("/api/v1/classes", None, false)]
#[case::task_cancellation("/api/v1/tasks/12/cancel", None, false)]
fn maintenance_path_availability(
    #[case] path: &str,
    #[case] metrics_path: Option<&str>,
    #[case] expected: bool,
) {
    assert_eq!(allowed_during_maintenance(path, metrics_path), expected);
}

#[rstest]
#[case::restore_confirmation(Method::POST, "/api/v1/restores/12/confirm", true)]
#[case::read_confirmation(Method::GET, "/api/v1/restores/12/confirm", false)]
#[case::delete_confirmation(Method::DELETE, "/api/v1/restores/12/confirm", false)]
#[case::nested_path(Method::POST, "/api/v1/restores/12/nested/confirm", false)]
#[case::missing_id(Method::POST, "/api/v1/restores//confirm", false)]
#[case::invalid_id(Method::POST, "/api/v1/restores/not-a-number/confirm", false)]
#[case::negative_id(Method::POST, "/api/v1/restores/-1/confirm", false)]
#[case::restore_status(Method::GET, "/api/v1/restores/12/status", false)]
#[case::ordinary_api(Method::GET, "/api/v1/classes", false)]
fn restore_initiation_paths(#[case] method: Method, #[case] path: &str, #[case] expected: bool) {
    assert_eq!(initiates_restore(&method, path), expected);
}
