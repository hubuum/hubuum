//! Controlled I/O verifies the dispatcher buffer lifetime directly. Native
//! plain/TLS socket probes remain separate; these I/O settings are test-only.

use std::cell::RefCell;
use std::convert::Infallible;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};
use std::time::Duration;

use actix_http::{HttpService, Request, Response};
use actix_service::{Service, ServiceFactory, fn_service};
use actix_web::body::{BodySize, MessageBody};
use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};

use super::DriverWitness;

struct Body {
    payload: Option<Bytes>,
    eof: Arc<AtomicBool>,
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
                self.eof.store(true, Ordering::Release);
                Poll::Ready(None)
            }
        }
    }
}

struct PendingWrite {
    socket: TcpStream,
    eof: Arc<AtomicBool>,
    pending: Option<oneshot::Sender<(usize, bool)>>,
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
        if self.eof.load(Ordering::Acquire)
            && let Some(pending) = self.pending.take()
        {
            let contains_payload = bytes.windows(8).any(|part| part == b"xxxxxxxx");
            let _ = pending.send((bytes.len(), contains_payload));
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

#[actix_web::test]
async fn body_eof_does_not_retire_pending_dispatcher_bytes() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let worker = thread::current().id();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, peer) = listener.accept().await.unwrap();
    let eof = Arc::new(AtomicBool::new(false));
    let body = Rc::new(RefCell::new(Some(Body {
        payload: Some(Bytes::from_static(b"xxxxxxxx")),
        eof: Arc::clone(&eof),
    })));
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
            let body = body.borrow_mut().take().unwrap();
            async move { Ok::<_, actix_http::Error>(Response::ok().set_body(body)) }
        }))
        .new_service(())
        .await
        .unwrap();
    let (pending, write_pending) = oneshot::channel();
    let (dropped, io_dropped) = oneshot::channel();
    let io = PendingWrite {
        socket,
        eof,
        pending: Some(pending),
        dropped: Some(dropped),
    };
    let dispatcher = actix_web::rt::spawn(service.call((io, Some(peer))));
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();

    let (pending_bytes, contains_payload) =
        timeout_at(deadline, write_pending).await.unwrap().unwrap();
    assert!(pending_bytes > 8 && contains_payload);
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
        timeout_at(deadline, io_dropped).await.unwrap().unwrap(),
        worker
    );
    // Abort only requests cancellation. Both positive observations are required
    // after awaiting the dispatcher; channel loss or timeout cannot pass.
    assert_eq!(
        timeout_at(deadline, driver_release).await.unwrap().unwrap(),
        worker
    );
}
