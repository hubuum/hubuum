//! HTTP publication owns its original producer and connection independently of
//! API occupancy. An abandoned producer or lost driver witness stays charged.

use std::any::Any;
use std::future::Future;
use std::io;
use std::net::Shutdown;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};
use std::time::Duration;

use actix_web::Error;
use actix_web::body::{BodySize, BoxBody, MessageBody};
use actix_web::dev::{Extensions, ServiceRequest, ServiceResponse};
use actix_web::http::{Method, StatusCode, Version, header};
use actix_web::middleware::Next;
use actix_web::web::Bytes;
use hubuum_storage_core::{
    HistoryLifecycleStorage, HistoryPublicationKind, HistoryPublicationOwner,
};
use hubuum_task_core::{TaskExecutionContext, TaskStopHandle, TaskStopReason};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, watch};
use tokio::time::{Instant, timeout, timeout_at};
use tracing::{Instrument, instrument::WithSubscriber};

use super::maintenance::HttpWorkGuard;
use crate::errors::ApiError;
use crate::permissions::AppContext;
use crate::storage::{ExecutionStorage, StorageError, StorageExecutionScope, StorageHandle};

// This bounds stop supervision, never the lifetime of a durable obligation.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration =
    Duration::from_millis(hubuum_storage_core::ApiRequestDuration::MAX_MILLISECONDS);

#[cfg(test)]
struct IsolatedPublicationWork(std::cell::RefCell<Option<HttpWorkGuard>>);

#[derive(Clone)]
struct Connection {
    socket: Arc<std::net::TcpStream>,
    released: watch::Receiver<Option<ThreadId>>,
    worker: ThreadId,
}

struct DriverWitness {
    released: watch::Sender<Option<ThreadId>>,
    worker: ThreadId,
}

impl Drop for DriverWitness {
    fn drop(&mut self) {
        let released = self.released.clone();
        let worker = self.worker;
        // Actix HTTP/1 drops connection data before I/O and write buffers. Run
        // on that same worker after the synchronous dispatcher drop completes.
        actix_web::rt::spawn(async move {
            tokio::task::yield_now().await;
            if thread::current().id() == worker {
                released.send_replace(Some(worker));
            }
        });
    }
}

fn attach(socket: &TcpStream, extensions: &mut Extensions) -> io::Result<()> {
    #[cfg(unix)]
    let socket = {
        use std::os::fd::AsFd;
        std::net::TcpStream::from(socket.as_fd().try_clone_to_owned()?)
    };
    #[cfg(windows)]
    let socket = {
        use std::os::windows::io::AsSocket;
        std::net::TcpStream::from(socket.as_socket().try_clone_to_owned()?)
    };
    let worker = thread::current().id();
    let (released, receiver) = watch::channel(None);
    extensions.insert(Connection {
        socket: Arc::new(socket),
        released: receiver,
        worker,
    });
    extensions.insert(DriverWitness { released, worker });
    Ok(())
}

/// Install only for a history-aware runtime. Unsupported transports have no
/// witness and cannot enter owned productive HTTP work.
pub(crate) fn on_connect(io: &dyn Any, extensions: &mut Extensions) {
    let socket = io.downcast_ref::<TcpStream>();
    #[cfg(feature = "tls-rustls")]
    let socket = socket.or_else(|| {
        io.downcast_ref::<actix_tls::accept::rustls_0_23::TlsStream<TcpStream>>()
            .map(|stream| stream.get_ref().0)
    });
    #[cfg(feature = "tls-openssl")]
    let socket = socket.or_else(|| {
        io.downcast_ref::<actix_tls::accept::openssl::TlsStream<TcpStream>>()
            .map(|stream| stream.get_ref())
    });
    if let Some(socket) = socket
        && let Err(error) = attach(socket, extensions)
    {
        tracing::warn!(%error, "HTTP publication connection witness unavailable");
    }
}

impl Connection {
    fn for_request(req: &ServiceRequest) -> Result<Self, ApiError> {
        if !matches!(req.version(), Version::HTTP_10 | Version::HTTP_11)
            || req.method() == Method::CONNECT
            || req.headers().contains_key(header::UPGRADE)
        {
            return Err(ApiError::ServiceUnavailable(
                "Owned history publication requires non-upgraded HTTP/1".into(),
            ));
        }
        req.request().conn_data::<Self>().cloned().ok_or_else(|| {
            ApiError::ServiceUnavailable(
                "HTTP publication requires a connection release witness".into(),
            )
        })
    }

    async fn driver_released(&mut self) -> Result<(), StorageError> {
        loop {
            if let Some(worker) = *self.released.borrow_and_update() {
                return if worker == self.worker && thread::current().id() == worker {
                    Ok(())
                } else {
                    Err(StorageError::unavailable(
                        "HTTP driver released on another worker",
                    ))
                };
            }
            self.released
                .changed()
                .await
                .map_err(|_| StorageError::unavailable("HTTP driver release witness was lost"))?;
        }
    }

    fn stop(&self) -> Result<(), StorageError> {
        self.socket
            .shutdown(Shutdown::Both)
            .or_else(|error| {
                if error.kind() == io::ErrorKind::NotConnected {
                    Ok(())
                } else {
                    Err(error)
                }
            })
            .map_err(|_| StorageError::unavailable("HTTP socket shutdown could not be confirmed"))
    }

    async fn retire(
        mut self,
        mut transmission: oneshot::Receiver<TransmissionEnd>,
        deadline: Instant,
    ) -> Result<(), StorageError> {
        let released = tokio::select! {
            released = self.driver_released() => Some(released),
            end = &mut transmission => {
                match end {
                    Ok(TransmissionEnd::Eof) => {
                        timeout_at(deadline, self.driver_released()).await.ok()
                    }
                    _ => None,
                }
            }
            _ = tokio::time::sleep_until(deadline) => None,
        };
        // Do not abort at body EOF: the dispatcher may still hold the final
        // bytes. Graceful close gets the original deadline to release them.
        self.stop()?;
        match released {
            Some(result) => result,
            None => timeout(DRAIN_TIMEOUT, self.driver_released())
                .await
                .map_err(|_| StorageError::unavailable("HTTP driver did not confirm retirement"))?,
        }
    }
}

#[derive(Clone, Copy)]
enum TransmissionEnd {
    Eof,
    Abandoned,
}

struct Transmission(Option<oneshot::Sender<TransmissionEnd>>);
impl Transmission {
    fn finish(&mut self, end: TransmissionEnd) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(end);
        }
    }
}
impl Drop for Transmission {
    fn drop(&mut self) {
        self.finish(TransmissionEnd::Abandoned);
    }
}

struct ProducerBody {
    body: BoxBody,
    storage: StorageHandle,
    scope: StorageExecutionScope,
    completed: Option<oneshot::Sender<()>>,
}
impl ProducerBody {
    fn new(
        body: BoxBody,
        storage: StorageHandle,
        scope: StorageExecutionScope,
        completed: oneshot::Sender<()>,
    ) -> Self {
        let (body, completed) =
            match storage.run_in_scope_sync(scope.clone(), || body.try_into_bytes()) {
                Ok(bytes) => {
                    let _ = completed.send(());
                    (BoxBody::new(bytes), None)
                }
                Err(body) => (body, Some(completed)),
            };
        Self {
            body,
            storage,
            scope,
            completed,
        }
    }
}
impl MessageBody for ProducerBody {
    type Error = <BoxBody as MessageBody>::Error;
    fn size(&self) -> BodySize {
        self.body.size()
    }
    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let this = self.get_mut();
        let result = this
            .storage
            .run_in_scope_sync(this.scope.clone(), || this.body.as_pin_mut().poll_next(cx));
        if matches!(result, Poll::Ready(None)) {
            this.body = BoxBody::new(());
            if let Some(completed) = this.completed.take() {
                let _ = completed.send(());
            }
        }
        result
    }
    fn try_into_bytes(mut self) -> Result<Bytes, Self> {
        let body = std::mem::replace(&mut self.body, BoxBody::new(()));
        match self
            .storage
            .run_in_scope_sync(self.scope.clone(), || body.try_into_bytes())
        {
            Ok(bytes) => {
                if let Some(completed) = self.completed.take() {
                    let _ = completed.send(());
                }
                Ok(bytes)
            }
            Err(body) => {
                self.body = body;
                Err(self)
            }
        }
    }
}

struct TransmissionBody {
    body: BoxBody,
    transmission: Transmission,
}
impl TransmissionBody {
    fn new(body: BoxBody, mut transmission: Transmission, head_request: bool) -> Self {
        // Actix can discard these bodies without polling them. Normal body
        // suppression still waits for independent positive driver release.
        if head_request || matches!(body.size(), BodySize::None | BodySize::Sized(0)) {
            transmission.finish(TransmissionEnd::Eof);
        }
        Self { body, transmission }
    }
}
impl MessageBody for TransmissionBody {
    type Error = <BoxBody as MessageBody>::Error;
    fn size(&self) -> BodySize {
        self.body.size()
    }
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, Self::Error>>> {
        let result = self.body.as_pin_mut().poll_next(cx);
        if matches!(result, Poll::Ready(None)) {
            self.transmission.finish(TransmissionEnd::Eof);
        }
        result
    }
    fn try_into_bytes(mut self) -> Result<Bytes, Self> {
        let body = std::mem::replace(&mut self.body, BoxBody::new(()));
        match body.try_into_bytes() {
            Ok(bytes) => {
                self.transmission.finish(TransmissionEnd::Eof);
                Ok(bytes)
            }
            Err(body) => {
                self.body = body;
                Err(self)
            }
        }
    }
}

/// Outside actor/API admission: head extensions survive its bounded buffering.
/// Transfer ownership only after API occupancy has already been settled.
pub(crate) async fn transmit(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let head_request = req.method() == Method::HEAD;
    let mut response = next.call(req).await?.map_into_boxed_body();
    let transmission = response
        .response_mut()
        .extensions_mut()
        .remove::<Transmission>();
    Ok(match transmission {
        Some(transmission) => response
            .map_body(|_, body| TransmissionBody::new(body, transmission, head_request).boxed()),
        None => response,
    })
}

struct Disconnect(Option<TaskStopHandle>);
impl Drop for Disconnect {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            stop.request_stop(TaskStopReason::Cancelled);
        }
    }
}

pub(super) async fn call(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let storage = AppContext::from_http_request(req.request())?
        .backend()
        .clone();
    let scope = storage.current_execution_scope();
    if !scope
        .application_access_override()
        .and_then(|permit| permit.owner())
        .is_some_and(|owner| owner.history_protocol().supports_online())
    {
        return next
            .call(req)
            .await
            .map(ServiceResponse::map_into_boxed_body);
    }
    if scope.api_request_override().is_some() {
        // The existing API supervisor already owns admission and handler waits.
        return owned(req, next, storage).await;
    }
    let (scope, stop) = if scope
        .task_execution_override()
        .and_then(Option::as_ref)
        .is_some()
    {
        (scope, None)
    } else {
        let (execution, stop) = TaskExecutionContext::new(REQUEST_TIMEOUT)
            .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        (scope.with_task_execution(Some(execution)), Some(stop))
    };
    let work_storage = storage.clone();
    let work = async move {
        work_storage
            .run_in_scope(scope, owned(req, next, storage))
            .await
    };
    let mut disconnect = Disconnect(stop);
    let work = work.in_current_span().with_current_subscriber();
    let work = crate::storage::SpawnObservationContext::current().run(work);
    let result = actix_web::rt::spawn(work).await.map_err(|_| {
        ApiError::ServiceUnavailable("HTTP publication producer could not be confirmed".into())
    })?;
    disconnect.0.take();
    result
}

async fn owned(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
    storage: StorageHandle,
) -> Result<ServiceResponse<BoxBody>, Error> {
    let connection = Connection::for_request(&req)?;
    let scope = storage.current_execution_scope();
    let deadline = Instant::now()
        + scope
            .task_execution_override()
            .and_then(Option::as_ref)
            .map_or(REQUEST_TIMEOUT, TaskExecutionContext::remaining);
    let owner = storage
        .admit_history_publication(HistoryPublicationKind::HttpResponse)
        .await
        .map_err(ApiError::from)?;
    let scope = scope.with_history_read(owner.context());
    let (completed, producer) = oneshot::channel();
    let (sender, transmission) = oneshot::channel();
    let transfer = Transmission(Some(sender));
    #[cfg(test)]
    let work = match req.app_data::<IsolatedPublicationWork>() {
        Some(work) => work
            .0
            .borrow_mut()
            .take()
            .expect("one isolated publication"),
        None => HttpWorkGuard::begin(),
    };
    #[cfg(not(test))]
    let work = HttpWorkGuard::begin();
    let settlement_storage = storage.clone();
    actix_web::rt::spawn(
        async move {
            let result = settle(
                settlement_storage,
                owner,
                connection.retire(transmission, deadline),
                producer,
            )
            .await;
            match result {
                Ok(()) => work.complete(),
                Err(error) => tracing::warn!(%error, "HTTP publication remains charged"),
            }
        }
        .in_current_span()
        .with_current_subscriber(),
    );
    let mut response = storage.run_in_scope(scope.clone(), next.call(req)).await?;
    if response.status() == StatusCode::SWITCHING_PROTOCOLS {
        return Err(ApiError::ServiceUnavailable(
            "Owned HTTP publication cannot upgrade its driver".into(),
        )
        .into());
    }
    response.response_mut().extensions_mut().insert(transfer);
    Ok(response.map_body(|head, body| {
        // ponytail: one connection per owned response; reuse needs a
        // per-connection publication barrier before keep-alive can be retained.
        head.set_connection_type(actix_http::ConnectionType::Close);
        ProducerBody::new(body.boxed(), storage, scope, completed).boxed()
    }))
}

async fn settle(
    storage: StorageHandle,
    owner: HistoryPublicationOwner,
    driver: impl Future<Output = Result<(), StorageError>>,
    producer: oneshot::Receiver<()>,
) -> Result<(), StorageError> {
    let settlement = owner
        .after_drained(driver, async {
            timeout(DRAIN_TIMEOUT, producer)
                .await
                .map_err(|_| StorageError::unavailable("HTTP producer did not confirm completion"))?
                .map_err(|_| StorageError::unavailable("HTTP producer completion witness was lost"))
        })
        .await?;
    storage.settle_history_publication(settlement).await
}

#[cfg(test)]
pub(crate) mod tests;
