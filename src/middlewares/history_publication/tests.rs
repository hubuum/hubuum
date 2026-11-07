use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use actix_http::HttpService;
use actix_service::{IntoServiceFactory, Service, ServiceFactory, map_config};
use actix_web::{App, HttpResponse, middleware::from_fn, web};
use futures_util::{StreamExt, stream};
use hubuum_domain::PrincipalId;
use hubuum_storage_core::{
    PrincipalStorage, StorageObservation, StorageObserver, StorageTokenCreate, StorageTokenDigest,
    StorageTokenIssuancePolicy, StorageUserCreate, TokenStorage, UserStorage,
};
use hubuum_storage_memory::MemoryStorage;
use rstest::rstest;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use super::*;
use crate::events::EventContext;
use crate::models::Token;
use crate::permissions::LocalPermissionBackend;

#[derive(Default)]
pub(crate) struct Observations {
    captured: AtomicUsize,
    admitted: AtomicUsize,
    api_settled: AtomicUsize,
    settled: AtomicUsize,
    changed: Notify,
}
impl StorageObserver for Observations {
    fn operation_finished(&self, observation: &StorageObservation) {
        if observation.result() != "ok" {
            return;
        }
        let count = match observation.operation() {
            "capture_application_execution" => &self.captured,
            "admit_history_publication" => &self.admitted,
            "settle_api_request" => &self.api_settled,
            "settle_history_publication" => &self.settled,
            _ => return,
        };
        count.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}
impl Observations {
    async fn wait_for_settlement(&self) {
        timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if self.settled.load(Ordering::SeqCst) == 1 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}

async fn fixture() -> (StorageHandle, Arc<Observations>, PrincipalId, String) {
    let observations = Arc::new(Observations::default());
    let storage = StorageHandle::from_registered_backend_with_observer(
        MemoryStorage::new(),
        observations.clone(),
    );
    let user = storage
        .create_user(StorageUserCreate::new(
            None,
            "http-publication",
            "fixture-hash",
            None,
            None,
            EventContext::system(),
        ))
        .await
        .unwrap()
        .into_value()
        .into_parts();
    let principal = PrincipalId::new(user.id().id()).unwrap();
    let token = "http-publication-fixture-token".to_owned();
    let hash = Token::storage_hash_from_raw(&token);
    let _ = storage
        .create_token(StorageTokenCreate::new(
            principal,
            StorageTokenDigest::legacy_unidentified(&hash),
            StorageTokenIssuancePolicy::try_new(24, 24).unwrap(),
            EventContext::system(),
        ))
        .await
        .unwrap();
    let registration = storage.register_history_runtime(None).await.unwrap();
    (
        storage.with_runtime_owner(registration.owner()),
        observations,
        principal,
        token,
    )
}

fn context(storage: StorageHandle) -> AppContext {
    let permissions = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    AppContext::new(storage, permissions)
}

struct PendingWrite {
    socket: TcpStream,
    pending: Option<oneshot::Sender<Vec<u8>>>,
    dropped: Option<oneshot::Sender<()>>,
}
impl AsyncRead for PendingWrite {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, buffer)
    }
}
impl AsyncWrite for PendingWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(pending) = self.pending.take() {
            let _ = pending.send(bytes.to_vec());
        }
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}
impl Drop for PendingWrite {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(());
        }
    }
}

async fn maintenance_for_case(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    if req.app_data::<IsolatedPublicationWork>().is_some() {
        crate::middlewares::maintenance::reject_with_activity(
            req,
            next,
            || crate::restores::MaintenanceActivityGuard::isolated().1,
            HttpWorkGuard::isolated().1,
        )
        .await
    } else {
        crate::middlewares::reject_during_maintenance(req, next).await
    }
}

#[derive(Clone, Copy)]
pub(crate) enum ResponseMode {
    Buffered,
    Streaming,
    CancelledStream,
}
struct BodyRelease(Option<oneshot::Sender<()>>);
impl Drop for BodyRelease {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

#[rstest]
#[case::api_buffered(ResponseMode::Buffered)]
#[case::unbuffered_stream(ResponseMode::Streaming)]
#[case::cancelled_stream(ResponseMode::CancelledStream)]
#[actix_web::test]
async fn real_middleware_keeps_publication_after_api_settlement_until_driver_release(
    #[case] mode: ResponseMode,
) {
    let (storage, observations, principal, token) = fixture().await;
    verify_middleware(storage, observations, principal, token, mode).await;
}

pub(crate) async fn verify_middleware(
    storage: StorageHandle,
    observations: Arc<Observations>,
    principal: PrincipalId,
    token: String,
    mode: ResponseMode,
) {
    let buffered = matches!(mode, ResponseMode::Buffered);
    let cancelled = matches!(mode, ResponseMode::CancelledStream);
    let (body_release, mut body_released) = oneshot::channel();
    let body_release = Rc::new(RefCell::new(Some(body_release)));
    let app_context = context(storage.clone());
    let app = App::new()
        .app_data(web::Data::new(app_context))
        .wrap(from_fn(crate::middlewares::actor_context))
        .wrap(from_fn(transmit))
        .wrap(from_fn(maintenance_for_case))
        .default_service(web::to(move |context: AppContext| {
            let release = BodyRelease(body_release.borrow_mut().take());
            async move {
                let storage = context.backend().clone();
                let output = stream::once(async move {
                    let scope = storage.current_execution_scope();
                    let publication = scope.history_read_override().expect("admitted owner");
                    publication.epoch().validate_scope(&scope).unwrap();
                    assert_eq!(scope.api_request_override().is_some(), buffered);
                    storage.get_principal(principal).await.unwrap();
                    Ok::<_, io::Error>(Bytes::from_static(b"owned-history-payload"))
                });
                let tail = stream::once(async move {
                    let _release = release;
                    if cancelled {
                        std::future::pending::<()>().await;
                    }
                })
                .filter_map(|_| async { None::<Result<Bytes, io::Error>> });
                HttpResponse::Ok().streaming(output.chain(tail))
            }
        }));
    // A deliberately abandoned producer must poison its own shutdown barrier,
    // without making unrelated restore tests share that failure state.
    let app = if cancelled {
        app.app_data(IsolatedPublicationWork(RefCell::new(Some(
            HttpWorkGuard::isolated().1,
        ))))
    } else {
        app
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let (connection, observed_connection) = oneshot::channel();
    let connection = RefCell::new(Some(connection));
    let service = HttpService::build()
        .on_connect_ext(move |io: &PendingWrite, extensions| {
            attach(&io.socket, extensions).unwrap();
            let _ = connection
                .borrow_mut()
                .take()
                .unwrap()
                .send(extensions.get::<Connection>().unwrap().clone());
        })
        .h1(map_config(app.into_factory(), |_: ()| {
            actix_web::dev::AppConfig::default()
        }))
        .new_service(())
        .await
        .unwrap();
    let (pending, pending_write) = oneshot::channel();
    let (dropped, io_dropped) = oneshot::channel();
    let dispatcher = actix_web::rt::spawn(service.call((
        PendingWrite {
            socket,
            pending: Some(pending),
            dropped: Some(dropped),
        },
        Some(peer),
    )));
    let path = if buffered {
        "/api/v1/classes/42/history"
    } else {
        "/stream"
    };
    client
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let pending_bytes = timeout(Duration::from_secs(5), pending_write)
        .await
        .unwrap()
        .unwrap();
    assert!(
        pending_bytes
            .windows(b"owned-history-payload".len())
            .any(|bytes| bytes == b"owned-history-payload")
    );
    drop(pending_bytes);
    assert_eq!(observations.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.api_settled.load(Ordering::SeqCst),
        usize::from(buffered)
    );
    assert_eq!(observations.settled.load(Ordering::SeqCst), 0);
    if cancelled {
        assert!(matches!(
            body_released.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    let captured = observations.captured.load(Ordering::SeqCst);
    let mut connection = observed_connection.await.unwrap();
    assert!(connection.released.borrow().is_none());
    dispatcher.abort();
    assert!(dispatcher.await.unwrap_err().is_cancelled());
    timeout(Duration::from_secs(5), io_dropped)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), connection.driver_released())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), body_released)
        .await
        .unwrap()
        .unwrap();
    if cancelled {
        // The actual body future was dropped, but native completion was never
        // acknowledged. Losing that witness cannot authorize owner retirement.
        assert_eq!(observations.settled.load(Ordering::SeqCst), 0);
        return;
    }
    observations.wait_for_settlement().await;
    assert_eq!(
        observations.captured.load(Ordering::SeqCst),
        captured,
        "exact publication settlement must not capture a fresh application permit"
    );
    // Successful settlement traverses the real complete adapter after API
    // occupancy disappeared, without trying to reacquire its retired permit.
    let permit = storage.admit_application_work().await.unwrap();
    let scope = storage
        .current_execution_scope()
        .with_application_access(permit);
    let owner = storage
        .run_in_scope(
            scope,
            storage.admit_history_publication(HistoryPublicationKind::HttpResponse),
        )
        .await
        .unwrap();
    let (completed, producer) = oneshot::channel();
    completed.send(()).unwrap();
    settle(storage, owner, async { Ok(()) }, producer)
        .await
        .unwrap();
}

#[rstest]
#[case::lost_producer(true)]
#[case::lost_driver(false)]
#[actix_web::test]
async fn missing_positive_completion_retains_the_durable_owner(#[case] producer_lost: bool) {
    let (storage, observations, _, _) = fixture().await;
    let permit = storage.admit_application_work().await.unwrap();
    let scope = storage
        .current_execution_scope()
        .with_application_access(permit);
    let owner = storage
        .run_in_scope(
            scope,
            storage.admit_history_publication(HistoryPublicationKind::HttpResponse),
        )
        .await
        .unwrap();
    let (completed, producer) = oneshot::channel();
    if producer_lost {
        drop(completed);
    } else {
        completed.send(()).unwrap();
    }
    let result = settle(
        storage,
        owner,
        async {
            if producer_lost {
                Ok(())
            } else {
                Err(StorageError::unavailable("lost driver"))
            }
        },
        producer,
    )
    .await;
    assert!(result.is_err());
    assert_eq!(observations.settled.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn native_delivery_retires_plain_http_publication() {
    native_delivery(None, NativeResponse::Payload).await;
}
#[cfg(feature = "tls-rustls")]
#[actix_web::test]
async fn native_delivery_retires_rustls_publication() {
    native_delivery(
        Some(crate::config::TlsBackend::Rustls),
        NativeResponse::Payload,
    )
    .await;
}
#[cfg(feature = "tls-openssl")]
#[actix_web::test]
async fn native_delivery_retires_openssl_publication() {
    native_delivery(
        Some(crate::config::TlsBackend::Openssl),
        NativeResponse::Payload,
    )
    .await;
}

trait ClientStream: AsyncRead + AsyncWrite + Unpin {}
impl<T: AsyncRead + AsyncWrite + Unpin> ClientStream for T {}

#[derive(Clone, Copy)]
enum NativeResponse {
    Payload,
    Head,
    Empty,
}

#[actix_web::test]
async fn native_delivery_keeps_head_headers() {
    native_delivery(None, NativeResponse::Head).await;
}
#[actix_web::test]
async fn native_delivery_keeps_empty_headers() {
    native_delivery(None, NativeResponse::Empty).await;
}

async fn native_delivery(tls: Option<crate::config::TlsBackend>, response: NativeResponse) {
    crate::tls::install_default_crypto_provider().unwrap();
    let (storage, observations, principal, token) = fixture().await;
    let app_context = context(storage);
    let server = actix_web::HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(app_context.clone()))
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(transmit))
            .wrap(from_fn(crate::middlewares::reject_during_maintenance))
            .route(
                "/api/v1/classes/42/history",
                web::to(move |context: AppContext| async move {
                    context.backend().get_principal(principal).await.unwrap();
                    match response {
                        NativeResponse::Empty => HttpResponse::NoContent().finish(),
                        _ => HttpResponse::Ok().body("complete-native-publication"),
                    }
                }),
            )
    })
    .on_connect(on_connect)
    .disable_signals()
    .workers(1);
    let server = match tls {
        None => server.bind("127.0.0.1:0").unwrap(),
        Some(backend) => crate::tls::configure_server(
            server,
            "127.0.0.1:0",
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/application/history_publication_tests/fixtures/test-cert.pem"
            ),
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/application/history_publication_tests/fixtures/test-key.pem"
            ),
            None,
            Some(backend),
        )
        .unwrap(),
    };
    let address = server.addrs()[0];
    let server = server.run();
    let handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let socket = TcpStream::connect(address).await.unwrap();
    #[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
    let mut client: Box<dyn ClientStream> = if tls.is_some() {
        use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(
                CertificateDer::from_pem_slice(include_bytes!(
                    "../../application/history_publication_tests/fixtures/test-cert.pem"
                ))
                .unwrap(),
            )
            .unwrap();
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        Box::new(
            connector
                .connect(ServerName::try_from("localhost").unwrap(), socket)
                .await
                .unwrap(),
        )
    } else {
        Box::new(socket)
    };
    #[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
    let mut client: Box<dyn ClientStream> = Box::new(socket);
    let method = if matches!(response, NativeResponse::Head) {
        "HEAD"
    } else {
        "GET"
    };
    client.write_all(format!("{method} /api/v1/classes/42/history HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n").as_bytes()).await.unwrap();
    let expected_body: &[u8] = if matches!(response, NativeResponse::Payload) {
        b"complete-native-publication"
    } else {
        b""
    };
    let mut received = Vec::new();
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(client.read_buf(&mut received).await.unwrap() > 0);
            assert!(received.len() < 4096);
            if let Some(start) = received.windows(4).position(|bytes| bytes == b"\r\n\r\n")
                && received.len() >= start + 4 + expected_body.len()
            {
                assert_eq!(&received[start + 4..], expected_body);
                break;
            }
        }
    })
    .await
    .unwrap();
    let status = if matches!(response, NativeResponse::Empty) {
        204
    } else {
        200
    };
    assert!(received.starts_with(format!("HTTP/1.1 {status}").as_bytes()));
    observations.wait_for_settlement().await;
    assert_eq!(
        observations.api_settled.load(Ordering::SeqCst),
        usize::from(!matches!(response, NativeResponse::Head))
    );
    drop(client);
    handle.stop(true).await;
    server.await.unwrap().unwrap();
}

#[rstest]
#[case::upgrade(Version::HTTP_11, true, Method::GET)]
#[case::connect(Version::HTTP_11, false, Method::CONNECT)]
#[case::http2(Version::HTTP_2, false, Method::GET)]
#[case::missing_witness(Version::HTTP_11, false, Method::GET)]
#[actix_web::test]
async fn unsupported_transport_never_admits_a_publication(
    #[case] version: Version,
    #[case] upgrade: bool,
    #[case] method: Method,
) {
    use actix_web::test;
    let (storage, observations, _, _) = fixture().await;
    let app_context = context(storage);
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(app_context))
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(transmit))
            .wrap(from_fn(crate::middlewares::reject_during_maintenance))
            .route(
                "/",
                web::get().to(|| async { HttpResponse::ImATeapot().finish() }),
            ),
    )
    .await;
    let expected = if upgrade || method == Method::CONNECT || version == Version::HTTP_2 {
        "non-upgraded HTTP/1"
    } else {
        "connection release witness"
    };
    let make_request = || {
        let mut request = test::TestRequest::default()
            .method(method.clone())
            .uri("/")
            .version(version);
        if upgrade {
            request = request.insert_header((header::UPGRADE, "websocket"));
        }
        request
    };
    let error = Connection::for_request(&make_request().to_srv_request())
        .err()
        .unwrap();
    assert!(error.to_string().contains(expected));
    let response = test::try_call_service(&app, make_request().to_request()).await;
    let status = match response {
        Ok(response) => response.status(),
        Err(error) => error.as_response_error().status_code(),
    };
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(observations.admitted.load(Ordering::SeqCst), 0);
}

#[actix_web::test]
async fn legacy_runtime_does_not_require_a_transport_witness() {
    use actix_web::test;
    use hubuum_storage_core::DeletionControlStorage;
    let storage = StorageHandle::memory();
    let registration = storage.register_runtime().await.unwrap();
    let storage = storage.with_runtime_owner(registration.owner());
    assert!(!storage.uses_online_history());
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(context(storage)))
            .wrap(from_fn(crate::middlewares::actor_context))
            .wrap(from_fn(transmit))
            .wrap(from_fn(crate::middlewares::reject_during_maintenance))
            .route(
                "/",
                web::get().to(|| async { HttpResponse::Ok().body("legacy response") }),
            ),
    )
    .await;
    let response = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
    assert_eq!(test::read_body(response).await.as_ref(), b"legacy response");
}

#[actix_web::test]
async fn exact_api_settlement_does_not_admit_fresh_application_work() {
    use hubuum_storage_core::{ApiRequestAdmissionStorage, ApiRequestClass};
    let (storage, observations, principal, _) = fixture().await;
    let permit = storage.admit_application_work().await.unwrap();
    let scope = storage
        .current_execution_scope()
        .with_application_access(permit);
    let api = storage
        .run_in_scope(
            scope,
            storage.admit_api_request(principal, ApiRequestClass::History, 0),
        )
        .await
        .unwrap();
    let captured = observations.captured.load(Ordering::SeqCst);
    storage.settle_api_request(api).await.unwrap();
    assert_eq!(observations.captured.load(Ordering::SeqCst), captured);
}

#[rstest]
#[case::head(Method::HEAD, b"payload", true)]
#[case::empty(Method::GET, b"", true)]
#[case::ordinary(Method::GET, b"payload", false)]
#[actix_web::test]
async fn normal_suppression_is_not_an_abandoned_transmission(
    #[case] method: Method,
    #[case] payload: &'static [u8],
    #[case] suppressed: bool,
) {
    let (sender, mut end) = oneshot::channel();
    let body = TransmissionBody::new(
        BoxBody::new(Bytes::from_static(payload)),
        Transmission(Some(sender)),
        method == Method::HEAD,
    );
    if suppressed {
        assert!(matches!(end.try_recv(), Ok(TransmissionEnd::Eof)));
    } else {
        assert!(matches!(
            end.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(body);
        assert!(matches!(end.try_recv(), Ok(TransmissionEnd::Abandoned)));
    }
}
