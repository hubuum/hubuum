use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io};

use actix_web::{App, HttpResponse, HttpServer, middleware::from_fn, web};
use diesel::{OptionalExtension, QueryableByName, sql_query, sql_types};
use diesel_async::RunQueryDsl;
use futures_util::{StreamExt, stream};
use hubuum_storage_core::HistoryLifecycleStorage;
use hubuum_storage_postgres::test_support::{IsolatedRestoreTarget, integration_test_database_url};
use hubuum_storage_postgres::{PostgresPool, PostgresStorageError, with_connection};
use hubuum_templates::{TemplateExecution, TemplateLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};
use tracing::field::{Field, Visit};
use tracing_subscriber::{Layer, layer::SubscriberExt};
use uuid::Uuid;

use crate::errors::ApiError;
use crate::middlewares::{actor_context, history_publication, reject_during_maintenance};
use crate::permissions::{AppContext, LocalPermissionBackend};
use crate::services::deletion_recovery::{
    reapply, recover, require_runtime_permit,
    tests::{Fixture, after_repository_release},
};
use crate::storage::{ExecutionStorage, StorageHandle};
use crate::test_support::{
    RESTORE_TEST_LOCK, postgres_test_pool, restore_test_migration_database_url,
    restore_test_runtime_database_url,
};

const RESPONSE_CHUNK: &[u8] = b"undrained-history-response";
const WAIT: Duration = Duration::from_secs(15);

struct PauseRenderer(Mutex<Option<oneshot::Sender<()>>>);

impl<S: tracing::Subscriber> Layer<S> for PauseRenderer {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if event.metadata().target() != "hubuum_templates::isolation" {
            return;
        }
        #[derive(Default)]
        struct RendererPid(Option<i32>);
        impl Visit for RendererPid {
            fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}

            fn record_u64(&mut self, field: &Field, value: u64) {
                if field.name() == "pid" {
                    self.0 = i32::try_from(value).ok().filter(|pid| *pid > 0);
                }
            }
        }
        let mut pid = RendererPid::default();
        event.record(&mut pid);
        let Some(pid) = pid.0 else {
            return;
        };
        let paused = self.0.lock().unwrap().take();
        if let Some(paused) = paused {
            // SAFETY: the supervisor just spawned this renderer and has not
            // sent its input yet. The parent fixture resumes the process group.
            assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
            paused.send(()).unwrap();
            // Keep the test-only supervisor here so it cannot time out the
            // paused renderer before the server is killed.
            loop {
                std::thread::park();
            }
        }
    }
}

// The server starts its own process group. Clean up the whole group if the
// test fails before it resumes the orphaned renderer.
struct RendererProcessGroup(Option<i32>);

impl RendererProcessGroup {
    fn resume(mut self) {
        // SAFETY: this group was created for the fixture subprocess alone.
        assert_eq!(unsafe { libc::kill(-self.0.unwrap(), libc::SIGCONT) }, 0);
        self.0 = None;
    }
}

impl Drop for RendererProcessGroup {
    fn drop(&mut self) {
        // SAFETY: this group was created for the fixture subprocess alone.
        if let Some(group) = self.0 {
            unsafe { libc::kill(-group, libc::SIGKILL) };
        }
    }
}

#[actix_web::test]
#[ignore = "subprocess fixture for PostgreSQL HTTP crash recovery"]
async fn http_crash_server() {
    let (paused, renderer_paused) = oneshot::channel();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(PauseRenderer(Mutex::new(Some(paused)))),
    )
    .unwrap();
    let pool = postgres_test_pool(&restore_test_runtime_database_url(), 4);
    let storage = StorageHandle::postgres(pool);
    let root = std::env::current_dir().unwrap();
    let options = serde_json::from_value(serde_json::json!({
        "directory": root.join("repository"),
        "key": root.join("key"),
    }))
    .unwrap();
    let permit = require_runtime_permit(&storage, &options)
        .await
        .unwrap()
        .into_runtime_lifetime()
        .protect_renderers()
        .unwrap();
    let registration = storage
        .register_history_runtime(permit.isolation_domain())
        .await
        .unwrap();
    let runtime = registration.id();
    let storage = storage.with_runtime_owner(registration.owner());
    let permission_backend = Arc::new(LocalPermissionBackend::new(storage.clone(), "admin".into()));
    let context = AppContext::new(storage, permission_backend);
    let _render = actix_web::rt::spawn(async {
        TemplateExecution::new(
            "crash-fixture",
            "{{ 6 * 7 }}",
            TemplateLimits::new(32, 1000),
        )
        .render(&serde_json::Value::Null)
        .await
    });
    timeout(WAIT, renderer_paused).await.unwrap().unwrap();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(context.clone()))
            .wrap(from_fn(actor_context))
            .wrap(from_fn(history_publication::transmit))
            .wrap(from_fn(reject_during_maintenance))
            .default_service(web::to(|context: AppContext| async move {
                assert!(
                    context
                        .backend()
                        .current_execution_scope()
                        .history_read_override()
                        .is_some()
                );
                let first = stream::once(async {
                    Ok::<_, io::Error>(web::Bytes::from_static(RESPONSE_CHUNK))
                });
                HttpResponse::Ok().streaming(first.chain(stream::pending()))
            }))
    })
    .on_connect(history_publication::on_connect)
    .disable_signals()
    .workers(1)
    .bind("127.0.0.1:0")
    .unwrap();
    fs::write(
        "http-ready.json",
        serde_json::to_vec(&(server.addrs()[0], runtime)).unwrap(),
    )
    .unwrap();
    server.run().await.unwrap();
}

#[derive(QueryableByName)]
struct Publication {
    #[diesel(sql_type = sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = sql_types::Nullable<sql_types::Uuid>)]
    runtime_token: Option<Uuid>,
}

async fn publication(pool: &PostgresPool, runtime: Uuid) -> Option<Publication> {
    with_connection(pool, async move |connection| {
        Ok::<_, PostgresStorageError>(
            sql_query(
                "SELECT p.id, r.ownership_token AS runtime_token
             FROM hubuum_control.history_publication_owners p
             LEFT JOIN hubuum_control.runtime_registrations r ON r.id = p.runtime_id
             WHERE p.runtime_id = $1 AND p.kind = 'http_response'",
            )
            .bind::<sql_types::Uuid, _>(runtime)
            .get_result(connection)
            .await
            .optional()?,
        )
    })
    .await
    .unwrap()
}

async fn admits_old_runtime(pool: &PostgresPool, runtime: Uuid, token: Uuid) -> bool {
    #[derive(QueryableByName)]
    struct Admission {
        #[diesel(sql_type = sql_types::Bool)]
        allowed: bool,
    }
    with_connection(pool, async move |connection| {
        Ok::<_, PostgresStorageError>(
            sql_query(
                "SELECT public.hubuum_capture_application_execution($1, $2) IS NOT NULL AS allowed",
            )
            .bind::<sql_types::Uuid, _>(runtime)
            .bind::<sql_types::Uuid, _>(token)
            .get_result::<Admission>(connection)
            .await?
            .allowed,
        )
    })
    .await
    .unwrap()
}

#[actix_web::test]
async fn crashed_http_publication_retires_only_after_the_renderer_exits() {
    let _serial = RESTORE_TEST_LOCK.lock().await;
    let target = IsolatedRestoreTarget::new(
        &restore_test_runtime_database_url(),
        &integration_test_database_url(),
    )
    .unwrap();
    let pool = postgres_test_pool(&restore_test_runtime_database_url(), 4);
    let owner_pool = postgres_test_pool(&restore_test_migration_database_url(), 2);
    target
        .validate_connection(&mut pool.get().await.unwrap())
        .await
        .unwrap();
    target
        .validate_connection(&mut owner_pool.get().await.unwrap())
        .await
        .unwrap();
    let fixture = Fixture::with_storage(StorageHandle::postgres(owner_pool.clone())).await;
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "tests::storage_contract::history_http_publication::crash::http_crash_server",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(&fixture.root)
        .kill_on_drop(true);
    // A fresh session keeps the stopped renderer out of the test runner's
    // job control. Server death must not trigger an orphan-group hangup.
    // SAFETY: the forked child calls only setsid and reads errno on failure.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut server = command.spawn().unwrap();
    let renderer = RendererProcessGroup(Some(i32::try_from(server.id().unwrap()).unwrap()));
    let (address, runtime): (SocketAddr, Uuid) = timeout(WAIT, async {
        loop {
            if let Ok(bytes) = fs::read(fixture.root.join("http-ready.json"))
                && let Ok(ready) = serde_json::from_slice(&bytes)
            {
                break ready;
            }
            assert!(
                server.try_wait().unwrap().is_none(),
                "server exited before opening its socket"
            );
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(b"GET /crash HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut received = Vec::new();
    timeout(WAIT, async {
        while !received
            .windows(RESPONSE_CHUNK.len())
            .any(|bytes| bytes == RESPONSE_CHUNK)
        {
            assert!(socket.read_buf(&mut received).await.unwrap() > 0);
            assert!(received.len() < 4096);
        }
    })
    .await
    .unwrap();
    assert!(received.starts_with(b"HTTP/1.1 200"));
    let original = publication(&owner_pool, runtime).await.unwrap();
    let token = original.runtime_token.unwrap();
    assert!(admits_old_runtime(&pool, runtime, token).await);

    server.kill().await.unwrap();
    server.wait().await.unwrap();
    let Err(error) = recover(&fixture.storage, &fixture.directory, &fixture.key).await else {
        panic!("recovery succeeded while the renderer was paused");
    };
    assert!(
        matches!(&error, ApiError::ServiceUnavailable(message)
        if message == "Runtime isolation is busy; stop every server before offline recovery"),
        "{error:?}"
    );
    assert_eq!(
        publication(&owner_pool, runtime).await.unwrap().id,
        original.id
    );

    renderer.resume();
    after_repository_release(|| recover(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    assert!(publication(&owner_pool, runtime).await.is_none());
    after_repository_release(|| reapply(&fixture.storage, &fixture.directory, &fixture.key))
        .await
        .unwrap();
    StorageHandle::postgres(pool.clone())
        .admit_application_work()
        .await
        .unwrap();
    assert!(!admits_old_runtime(&pool, runtime, token).await);
}
