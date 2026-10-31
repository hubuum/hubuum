use std::cell::Cell;
use std::net::TcpListener;
use std::sync::Arc;

use actix_web::HttpResponse;
use rstest::rstest;
use tokio::net::TcpStream;
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;

use super::*;

fn settled_shutdown() -> RuntimeShutdown {
    RuntimeShutdown {
        role: Ok(()),
        workers: ShutdownReport::default(),
        templates: Ok(()),
        active_work: 0,
        http_settled: true,
    }
}

enum UnsettledWork {
    RuntimeFailure,
    PanickedWorker,
    TimedOutWorker,
    TemplateFailure,
    ActiveRequest,
    AbandonedHttp,
}

#[rstest]
#[case::runtime_failure(UnsettledWork::RuntimeFailure)]
#[case::panicked_worker(UnsettledWork::PanickedWorker)]
#[case::timed_out_worker(UnsettledWork::TimedOutWorker)]
#[case::template_failure(UnsettledWork::TemplateFailure)]
#[case::active_request(UnsettledWork::ActiveRequest)]
#[case::abandoned_http(UnsettledWork::AbandonedHttp)]
#[actix_web::test]
async fn uncertain_shutdown_never_starts_registration_removal(#[case] work: UnsettledWork) {
    let mut shutdown = settled_shutdown();
    match work {
        UnsettledWork::RuntimeFailure => shutdown.role = Err(io::Error::other("runtime failed")),
        UnsettledWork::PanickedWorker => shutdown.workers.panicked.push("task-worker".into()),
        UnsettledWork::TimedOutWorker => shutdown.workers.timed_out.push("task-worker".into()),
        UnsettledWork::TemplateFailure => {
            shutdown.templates = Err(io::Error::other("template supervisor failed"));
        }
        UnsettledWork::ActiveRequest => shutdown.active_work = 1,
        UnsettledWork::AbandonedHttp => shutdown.http_settled = false,
    }
    let removed = Cell::new(false);

    let result = shutdown
        .finish(async {
            removed.set(true);
            Ok(())
        })
        .await;

    assert!(result.is_err());
    assert!(!removed.get());
}

#[actix_web::test]
async fn settled_shutdown_removes_registration() {
    let removed = Cell::new(false);
    settled_shutdown()
        .finish(async {
            removed.set(true);
            Ok(())
        })
        .await
        .unwrap();
    assert!(removed.get());
}

#[actix_web::test]
async fn failed_registration_removal_does_not_report_successful_shutdown() {
    let result = settled_shutdown()
        .finish(async { Err(StorageError::unavailable("control unavailable")) })
        .await;

    assert!(result.is_err());
}

#[rstest]
#[case::shutdown(false)]
#[case::worker_failure(true)]
#[actix_web::test]
async fn http_shutdown_drains_in_flight_handler_and_closes_listener(#[case] worker_failed: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let server_entered = Arc::clone(&entered);
    let server_release = Arc::clone(&release);
    let server = HttpServer::new(move || {
        let entered = Arc::clone(&server_entered);
        let release = Arc::clone(&server_release);
        App::new().route(
            "/",
            web::get().to(move || {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.notify_one();
                    release.notified().await;
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
    let (failed, failure) = oneshot::channel();
    let mut supervision = actix_rt::spawn(supervise_http_server(server, async move {
        failure.await.unwrap();
        if worker_failed {
            Err(io::Error::other("worker stopped"))
        } else {
            Ok(())
        }
    }));
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let request = actix_rt::spawn(async move {
        let response = client
            .get(format!("http://{address}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        response.bytes().await.unwrap();
    });
    timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("HTTP request must reach the handler");
    failed.send(()).unwrap();
    assert!(
        timeout(Duration::from_millis(50), &mut supervision)
            .await
            .is_err()
    );
    release.notify_one();
    request.await.unwrap();
    let result = timeout(Duration::from_secs(5), supervision)
        .await
        .expect("HTTP shutdown must finish")
        .unwrap();
    assert_eq!(result.is_err(), worker_failed);
    assert!(TcpStream::connect(address).await.is_err());
}
