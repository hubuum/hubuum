//! The real HTTP middleware must settle the PostgreSQL publication after its
//! original API owner is gone, using the native exact-owner control entry.
use super::*;
use futures_util::FutureExt;
use hubuum_storage_core::{DeletionControlStorage, HistoryLifecycleStorage};
use rstest::rstest;
use std::panic::{AssertUnwindSafe, resume_unwind};

use crate::middlewares::history_publication::tests::{
    Observations, ResponseMode, verify_middleware,
};

#[actix_web::test]
async fn buffered_http_keeps_its_postgres_publication_beyond_api_settlement() {
    let observations = Arc::new(Observations::default());
    let adapter = AdapterPostgresStorage::new(
        pool().get_ref().clone(),
        Arc::new(RecordingPostgresObserver::default()),
    );
    let backend =
        StorageHandle::from_postgres_backend_with_storage_observer(adapter, observations.clone());
    let user = create_backend_user(&backend, &prefix("http_publication")).await;
    let registration = backend.register_history_runtime(None).await.unwrap();
    let storage = backend.clone().with_runtime_owner(registration.owner());
    let checked = AssertUnwindSafe(verify_middleware(
        storage,
        observations,
        user.principal_id,
        user.raw_token.clone(),
        ResponseMode::Buffered,
    ))
    .catch_unwind()
    .await;
    let cleanup = backend.unregister_runtime(registration).await;
    delete_backend_user(&backend, user).await;
    if let Err(panic) = checked {
        resume_unwind(panic);
    }
    cleanup.unwrap();
}

#[rstest]
#[case::platform_witness(true)]
#[case::missing_witness(false)]
#[actix_web::test]
async fn native_conditional_user_handler_obeys_publication_platform_support(
    #[case] install_witness: bool,
) {
    use crate::api::etag::RevisionedResource;
    use crate::middlewares::{actor_context, history_publication, reject_during_maintenance};
    use crate::permissions::{AppContext, LocalPermissionBackend};
    use actix_web::{App, HttpServer, middleware::from_fn, web};
    let test = crate::tests::TestContext::new().await;
    let observations = Arc::new(Observations::default());
    let unbound = StorageHandle::from_postgres_backend_with_storage_observer(
        AdapterPostgresStorage::unobserved(test.pool.get_ref().clone()),
        observations.clone(),
    );
    let registration = unbound.register_history_runtime(None).await.unwrap();
    let storage = unbound.clone().with_runtime_owner(registration.owner());
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage, permissions);
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(context.clone()))
            .wrap(from_fn(actor_context))
            .wrap(from_fn(history_publication::transmit))
            .wrap(from_fn(reject_during_maintenance))
            .service(
                web::scope("/api/v1/iam/users").configure(crate::api::v1::routes::users::config),
            )
    })
    .on_connect(move |io, extensions| {
        if install_witness {
            history_publication::on_connect(io, extensions);
        }
    })
    .disable_signals()
    .workers(1)
    .bind("127.0.0.1:0")
    .unwrap();
    let address = server.addrs()[0];
    let server = server.run();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let checked = AssertUnwindSafe(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        let url = format!("http://{address}/api/v1/iam/users/{}", test.normal_user.id);
        let original = test.normal_user.to_point_response(&unbound).await.unwrap();
        let qualified = install_witness && !cfg!(windows);
        let current = client
            .get(&url)
            .bearer_auth(&test.admin_token)
            .send()
            .await
            .unwrap();
        let etag = if qualified {
            assert_eq!(current.status(), reqwest::StatusCode::OK);
            current.headers()[reqwest::header::ETAG]
                .to_str()
                .unwrap()
                .to_owned()
        } else {
            assert_eq!(current.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
            assert!(current.headers().get(reqwest::header::ETAG).is_none());
            original.entity_tag().unwrap().to_string()
        };
        let _ = current.bytes().await.unwrap();
        let response = client
            .patch(&url)
            .bearer_auth(&test.admin_token)
            .header(reqwest::header::IF_MATCH, etag)
            .json(&serde_json::json!({"proper_name":"Conditional publication"}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.text().await.unwrap();
        if qualified {
            observations.wait_for_settlements(2).await;
            assert_eq!(status, reqwest::StatusCode::OK, "{body}");
        } else {
            assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
            let unchanged = test.normal_user.to_point_response(&unbound).await.unwrap();
            assert_eq!(unchanged.revision, original.revision);
            assert_eq!(unchanged.proper_name, original.proper_name);
        }
    })
    .catch_unwind()
    .await;
    handle.stop(true).await;
    server.await.unwrap().unwrap();
    unbound.unregister_runtime(registration).await.unwrap();
    if let Err(panic) = checked {
        resume_unwind(panic);
    }
}
