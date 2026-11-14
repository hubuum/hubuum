//! Native transport proof before production publication-owner integration.
//! Body completion and a shutdown request never substitute for driver release.

mod pending_write;

use std::convert::Infallible;
use std::io;
use std::net::{Shutdown, TcpListener};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};
use std::time::Duration;

use actix_web::body::{BodySize, MessageBody};
use actix_web::{App, HttpResponse, HttpServer, web};
use bytes::Bytes;
use rstest::rstest;
use tokio::io::AsyncWriteExt;
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;

const DRIVER_FIXTURE_TIMEOUT: Duration = Duration::from_secs(30);

struct DriverWitness {
    worker: ThreadId,
    released: Option<oneshot::Sender<ThreadId>>,
}

impl Drop for DriverWitness {
    fn drop(&mut self) {
        let worker = self.worker;
        let released = self.released.take().expect("single driver witness");
        // Connection extensions precede I/O and buffers in InnerDispatcher's
        // field drop order. A same-worker task runs after that synchronous drop
        // finishes. Channel closure without this message proves no completion.
        actix_web::rt::spawn(async move {
            tokio::task::yield_now().await;
            assert_eq!(thread::current().id(), worker);
            let _ = released.send(worker);
        });
    }
}

struct ObservedBody {
    payload: Option<Bytes>,
    produced: Option<oneshot::Sender<()>>,
    eof: Option<oneshot::Sender<()>>,
}

impl MessageBody for ObservedBody {
    type Error = Infallible;

    fn size(&self) -> BodySize {
        BodySize::Stream
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        if let Some(mut payload) = self.payload.take() {
            if let Some(produced) = self.produced.take() {
                let _ = produced.send(());
            }
            // Avoid handing one oversized write to a platform's socket queue.
            // Several bounded chunks must encounter the unread receive window.
            let chunk = if payload.len() > 64 * 1024 {
                let chunk = payload.split_to(64 * 1024);
                self.payload = Some(payload);
                chunk
            } else {
                payload
            };
            return Poll::Ready(Some(Ok(chunk)));
        }
        if let Some(eof) = self.eof.take() {
            let _ = eof.send(());
        }
        Poll::Ready(None)
    }
}

fn duplicate_socket(socket: &TcpStream) -> io::Result<std::net::TcpStream> {
    #[cfg(unix)]
    {
        use std::os::fd::AsFd;
        socket.as_fd().try_clone_to_owned().map(Into::into)
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsSocket;
        socket.as_socket().try_clone_to_owned().map(Into::into)
    }
}

#[derive(Clone, Copy)]
enum Transport {
    Plain,
    #[cfg(feature = "tls-rustls")]
    Rustls,
    #[cfg(feature = "tls-openssl")]
    Openssl,
}

impl Transport {
    fn socket(self, io: &dyn std::any::Any) -> &TcpStream {
        match self {
            Self::Plain => io.downcast_ref::<TcpStream>().unwrap(),
            #[cfg(feature = "tls-rustls")]
            Self::Rustls => {
                io.downcast_ref::<actix_tls::accept::rustls_0_23::TlsStream<TcpStream>>()
                    .unwrap()
                    .get_ref()
                    .0
            }
            #[cfg(feature = "tls-openssl")]
            Self::Openssl => io
                .downcast_ref::<actix_tls::accept::openssl::TlsStream<TcpStream>>()
                .unwrap()
                .get_ref(),
        }
    }
}

// Public, deliberately non-secret loopback test fixture. Never use this key in
// a deployment or put the fixture certificate in production trust stores.
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
const TEST_CERT: &[u8] = include_bytes!("history_publication_tests/fixtures/test-cert.pem");
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
const TEST_KEY: &[u8] = include_bytes!("history_publication_tests/fixtures/test-key.pem");

#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
trait ClientStream: AsyncRead + AsyncWrite + Unpin {}
#[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
impl<T: AsyncRead + AsyncWrite + Unpin> ClientStream for T {}

#[rstest]
#[case::body_eof(false)]
#[cfg_attr(not(windows), case::backpressure(true))]
#[actix_web::test]
async fn http1_publication_waits_for_driver_release(#[case] backpressure: bool) {
    exercise_driver_release(Transport::Plain, backpressure).await;
}

#[cfg(feature = "tls-rustls")]
#[rstest]
#[case::body_eof(false)]
#[cfg_attr(not(windows), case::backpressure(true))]
#[actix_web::test]
async fn http1_publication_rustls_waits_for_driver_release(#[case] backpressure: bool) {
    exercise_driver_release(Transport::Rustls, backpressure).await;
}

#[cfg(feature = "tls-openssl")]
#[rstest]
#[case::body_eof(false)]
#[cfg_attr(not(windows), case::backpressure(true))]
#[actix_web::test]
async fn http1_publication_openssl_waits_for_driver_release(#[case] backpressure: bool) {
    exercise_driver_release(Transport::Openssl, backpressure).await;
}

async fn exercise_driver_release(transport: Transport, backpressure: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (released, mut driver_release) = oneshot::channel();
    let released = Arc::new(Mutex::new(Some(released)));
    let (stop, stop_handle) = oneshot::channel();
    let stop = Arc::new(Mutex::new(Some(stop)));
    let (produced, body_produced) = oneshot::channel();
    let (eof, mut body_eof) = oneshot::channel();
    let response = Arc::new(Mutex::new(Some(ObservedBody {
        payload: Some(Bytes::from(vec![
            b'x';
            if backpressure {
                16 * 1024 * 1024
            } else {
                8
            }
        ])),
        produced: Some(produced),
        eof: Some(eof),
    })));
    let server = HttpServer::new(move || {
        let response = Arc::clone(&response);
        App::new().route(
            "/",
            web::get().to(move || {
                let body = response.lock().unwrap().take().expect("one response");
                async move { HttpResponse::Ok().message_body(body).unwrap() }
            }),
        )
    })
    .on_connect(move |io, extensions| {
        let socket = transport.socket(io);
        if backpressure {
            // Configure the accepted socket itself; listener inheritance and
            // loopback buffer defaults differ across platforms.
            let control = TcpSocket::from_std_stream(duplicate_socket(socket).unwrap());
            control.set_send_buffer_size(4096).unwrap();
        }
        let _ = stop
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(duplicate_socket(socket).unwrap());
        extensions.insert(DriverWitness {
            worker: thread::current().id(),
            released: released.lock().unwrap().take(),
        });
    })
    .disable_signals()
    .workers(1);
    let server = match transport {
        Transport::Plain => server.listen(listener).unwrap(),
        #[cfg(feature = "tls-rustls")]
        Transport::Rustls => {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
            crate::tls::install_default_crypto_provider().unwrap();
            let config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from_pem_slice(TEST_CERT).unwrap()],
                    PrivateKeyDer::from_pem_slice(TEST_KEY).unwrap(),
                )
                .unwrap();
            server.listen_rustls_0_23(listener, config).unwrap()
        }
        #[cfg(feature = "tls-openssl")]
        Transport::Openssl => {
            use openssl::{
                pkey::PKey,
                ssl::{SslAcceptor, SslMethod},
                x509::X509,
            };
            let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
            acceptor
                .set_certificate(&X509::from_pem(TEST_CERT).unwrap())
                .unwrap();
            acceptor
                .set_private_key(&PKey::private_key_from_pem(TEST_KEY).unwrap())
                .unwrap();
            acceptor.check_private_key().unwrap();
            server.listen_openssl(listener, acceptor).unwrap()
        }
    }
    .run();
    let server_handle = server.handle();
    let server = actix_web::rt::spawn(server);
    let client = TcpSocket::new_v4().unwrap();
    if backpressure {
        client.set_recv_buffer_size(4096).unwrap();
    }
    let client = client.connect(address).await.unwrap();
    #[cfg(any(feature = "tls-rustls", feature = "tls-openssl"))]
    let mut client: Box<dyn ClientStream> = match transport {
        Transport::Plain => Box::new(client),
        _ => {
            use rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};
            crate::tls::install_default_crypto_provider().unwrap();
            let mut roots = rustls::RootCertStore::empty();
            roots
                .add(CertificateDer::from_pem_slice(TEST_CERT).unwrap())
                .unwrap();
            let config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            Box::new(
                connector
                    .connect(ServerName::try_from("localhost").unwrap(), client)
                    .await
                    .unwrap(),
            )
        }
    };
    #[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
    let mut client = client;
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .unwrap();
    let stop = timeout(DRIVER_FIXTURE_TIMEOUT, stop_handle)
        .await
        .unwrap()
        .unwrap();
    timeout(DRIVER_FIXTURE_TIMEOUT, body_produced)
        .await
        .unwrap()
        .unwrap();
    if backpressure {
        assert!(
            timeout(Duration::from_millis(100), &mut body_eof)
                .await
                .is_err()
        );
    } else {
        timeout(DRIVER_FIXTURE_TIMEOUT, &mut body_eof)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(matches!(
        driver_release.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // Abort is a request. Retirement requires the positive driver witness below.
    stop.shutdown(Shutdown::Both).unwrap();
    timeout(DRIVER_FIXTURE_TIMEOUT, &mut driver_release)
        .await
        .unwrap()
        .unwrap();
    drop(client);
    server_handle.stop(true).await;
    server.await.unwrap().unwrap();
}
