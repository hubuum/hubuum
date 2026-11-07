//! Controlled I/O verifies the dispatcher buffer lifetime directly. Native
//! plain/TLS socket probes remain separate; these I/O settings are test-only.

use std::cell::RefCell;
use std::convert::Infallible;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};
use std::time::Duration;

use actix_http::{HttpService, Request, Response};
use actix_service::{Service, ServiceFactory, fn_service};
use actix_web::body::{BodySize, MessageBody};
use bytes::Bytes;
use rstest::rstest;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};

use super::DriverWitness;

struct Body {
    payload: Option<Bytes>,
    progressed: Arc<AtomicUsize>,
    observed: bool,
    pending: bool,
    dropped: Option<oneshot::Sender<ThreadId>>,
}

impl Drop for Body {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            let _ = dropped.send(thread::current().id());
        }
    }
}

impl MessageBody for Body {
    type Error = Infallible;

    fn size(&self) -> BodySize {
        BodySize::Stream
    }

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        match self.payload.take() {
            Some(bytes) => Poll::Ready(Some(Ok(bytes))),
            None => {
                if !self.observed {
                    self.observed = true;
                    self.progressed.fetch_add(1, Ordering::AcqRel);
                }
                if self.pending {
                    Poll::Pending
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

struct PendingWrite {
    socket: TcpStream,
    progressed: Arc<AtomicUsize>,
    responses: usize,
    pending: Option<oneshot::Sender<Vec<u8>>>,
    dropped: Option<oneshot::Sender<ThreadId>>,
}

impl AsyncRead for PendingWrite {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}

impl AsyncWrite for PendingWrite {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.progressed.load(Ordering::Acquire) == self.responses
            && let Some(pending) = self.pending.take()
        {
            let _ = pending.send(bytes.to_vec());
        }
        // Deliberately retain all writes. There is no timer or synthetic EOF
        // that can make the dispatcher or its owned payload disappear.
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
            let _ = dropped.send(thread::current().id());
        }
    }
}

#[rstest]
#[case::body_eof(1, false)]
#[case::pipelined_eof(2, false)]
#[case::pipelined_pending_body(2, true)]
#[actix_web::test]
async fn cancellation_retires_pending_dispatcher_and_pipelined_bodies(
    #[case] responses: usize,
    #[case] pending_body: bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let worker = thread::current().id();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let progressed = Arc::new(AtomicUsize::new(0));
    let (body_dropped, mut body_release) = oneshot::channel();
    let body_dropped = Rc::new(RefCell::new(Some(body_dropped)));
    let issued = Rc::new(RefCell::new(0));
    let body_progress = Arc::clone(&progressed);
    let (released, mut driver_release) = oneshot::channel();
    let released = RefCell::new(Some(released));
    let service = HttpService::build()
        .on_connect_ext(move |_: &PendingWrite, extensions| {
            extensions.insert(DriverWitness {
                worker,
                released: released.borrow_mut().take(),
            });
        })
        .h1(fn_service(move |_: Request| {
            let ordinal = {
                let mut issued = issued.borrow_mut();
                *issued += 1;
                *issued
            };
            assert!(ordinal <= responses);
            let body = Body {
                payload: Some(Bytes::from(format!("publication-{ordinal}"))),
                progressed: Arc::clone(&body_progress),
                observed: false,
                pending: pending_body && ordinal == responses,
                dropped: (ordinal == responses).then(|| body_dropped.borrow_mut().take().unwrap()),
            };
            async move { Ok::<_, actix_http::Error>(Response::ok().set_body(body)) }
        }))
        .new_service(())
        .await
        .unwrap();
    let (pending, write_pending) = oneshot::channel();
    let (dropped, io_dropped) = oneshot::channel();
    let io = PendingWrite {
        socket,
        progressed,
        responses,
        pending: Some(pending),
        dropped: Some(dropped),
    };
    let dispatcher = actix_web::rt::spawn(service.call((io, Some(peer))));
    // Submit the whole pipeline together so the dispatcher owns both responses
    // while the controlled writer retains every byte from the first onward.
    let requests = b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".repeat(responses);
    client.write_all(&requests).await.unwrap();

    let pending_bytes = timeout_at(deadline, write_pending).await.unwrap().unwrap();
    for ordinal in 1..=responses {
        let payload = format!("publication-{ordinal}");
        assert!(
            pending_bytes
                .windows(payload.len())
                .any(|part| part == payload.as_bytes()),
            "all pipelined payloads must remain in the actual dispatcher buffer"
        );
    }
    drop(pending_bytes);
    if pending_body {
        assert!(matches!(
            body_release.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    assert!(matches!(
        driver_release.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));

    dispatcher.abort();
    assert!(
        timeout_at(deadline, dispatcher)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert_eq!(
        timeout_at(deadline, body_release).await.unwrap().unwrap(),
        worker
    );
    assert_eq!(
        timeout_at(deadline, io_dropped).await.unwrap().unwrap(),
        worker
    );
    // Abort only requests cancellation. Body, I/O and deferred driver release
    // must all be observed after awaiting it; channel loss or timeout cannot pass.
    assert_eq!(
        timeout_at(deadline, driver_release).await.unwrap().unwrap(),
        worker
    );
}
