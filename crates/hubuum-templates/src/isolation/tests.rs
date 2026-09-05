//! Real subprocess tests: every fixture executes a fixed command, never template
//! input. PID files let assertions check OS reaping instead of trusting counters.
#![cfg(unix)]

use std::path::Path;

use rstest::rstest;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

use super::*;

const CAPACITY: usize = MAX_CONCURRENT_WORKERS + MAX_WAITING_WORKERS;

struct Fixture {
    runtime: WorkerRuntime,
    directory: TempDir,
}
impl Fixture {
    fn new() -> Self {
        Self {
            runtime: WorkerRuntime::start().unwrap(),
            directory: TempDir::new().unwrap(),
        }
    }
    fn submit(
        &self,
        command: Command,
        input: String,
        duration: Duration,
    ) -> oneshot::Receiver<Result<RenderedTemplate, TemplateError>> {
        let started = Instant::now();
        let capacity = self.runtime.admit(started).unwrap();
        let (response, receiver) = oneshot::channel();
        let job = Job {
            encoded: input,
            max_response_bytes: 1024,
            started,
            deadline: started + duration,
            response,
            capacity,
            span: tracing::Span::none(),
            dispatcher: tracing::dispatcher::get_default(Clone::clone),
            command,
        };
        assert!(self.runtime.jobs.try_send(job).is_ok());
        receiver
    }
    fn sleeping(
        &self,
        name: &str,
        input: String,
        duration: Duration,
    ) -> oneshot::Receiver<Result<RenderedTemplate, TemplateError>> {
        let mut command = shell("echo $$ > \"$1\"; exec sleep 60");
        command.arg("fixture").arg(self.directory.path().join(name));
        self.submit(command, input, duration)
    }
    async fn pid(&self, name: &str) -> i32 {
        timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(pid) = std::fs::read_to_string(self.directory.path().join(name))
                    && let Ok(pid) = pid.trim().parse()
                {
                    return pid;
                }
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fixture should start")
    }
    async fn idle(&self) {
        timeout(Duration::from_secs(3), async {
            while self.runtime.capacity.available_permits() != CAPACITY {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("all capacity should be returned after cleanup");
    }
    async fn occupy(&self) -> Vec<oneshot::Receiver<Result<RenderedTemplate, TemplateError>>> {
        let mut receivers = Vec::new();
        for index in 0..MAX_CONCURRENT_WORKERS {
            let name = format!("active-{index}");
            receivers.push(self.sleeping(&name, String::new(), Duration::from_secs(30)));
            self.pid(&name).await;
        }
        receivers
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.runtime.stopping.send(true);
    }
}
fn shell(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.env_clear().arg("-c").arg(script);
    command
}
fn assert_reaped(pid: i32) {
    // SAFETY: waitpid accepts a null status pointer; the PID is a child created
    // by this fixture. Returning ECHILD proves the supervisor already reaped it.
    assert_eq!(
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
        -1
    );
    assert_eq!(
        std::io::Error::last_os_error().raw_os_error(),
        Some(libc::ECHILD)
    );
}
fn error(result: Result<RenderedTemplate, TemplateError>) -> String {
    result.err().expect("operation must fail").to_string()
}

#[tokio::test]
async fn saturation_rejects_excess_work_without_blocking_the_caller() {
    let fixture = Fixture::new();
    let active = fixture.occupy().await;
    let waiting = (0..MAX_WAITING_WORKERS)
        .map(|index| {
            fixture.sleeping(
                &format!("queued-{index}"),
                String::new(),
                Duration::from_secs(30),
            )
        })
        .collect::<Vec<_>>();
    assert!(fixture.runtime.admit(Instant::now()).is_err());
    // Timers on this single-thread runtime continue while all worker slots and
    // the waiting queue are occupied. This would stall with Condvar admission.
    timeout(Duration::from_millis(200), sleep(Duration::from_millis(10)))
        .await
        .unwrap();
    drop(waiting);
    drop(active);
    fixture.idle().await;
    for index in 0..MAX_WAITING_WORKERS {
        assert!(
            !fixture
                .directory
                .path()
                .join(format!("queued-{index}"))
                .exists()
        );
    }
    fixture.runtime.shutdown().await;
}

#[rstest]
#[case::waiting_for_output(0)]
#[case::blocked_input_pipe(2 * 1024 * 1024)]
#[tokio::test]
async fn cancellation_reaps_the_child_before_releasing_capacity(#[case] input_bytes: usize) {
    let fixture = Fixture::new();
    let result = fixture.sleeping(
        "cancelled",
        "x".repeat(input_bytes),
        Duration::from_secs(30),
    );
    let pid = fixture.pid("cancelled").await;
    drop(result);
    fixture.idle().await;
    assert_reaped(pid);
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn execution_deadline_kills_and_reaps_a_stalled_worker() {
    let fixture = Fixture::new();
    let result = fixture.sleeping("deadline", String::new(), Duration::from_millis(500));
    let pid = fixture.pid("deadline").await;
    assert!(error(result.await.unwrap()).contains("deadline"));
    fixture.idle().await;
    assert_reaped(pid);
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn admission_deadline_never_starts_a_queued_child() {
    let fixture = Fixture::new();
    let active = fixture.occupy().await;
    let queued = fixture.sleeping("expired", String::new(), Duration::from_millis(50));
    assert!(error(queued.await.unwrap()).contains("admission deadline"));
    assert!(!fixture.directory.path().join("expired").exists());
    drop(active);
    fixture.idle().await;
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn shutdown_drains_active_and_queued_work_and_closes_admission() {
    let fixture = Fixture::new();
    let active = fixture.occupy().await;
    let queued = fixture.sleeping("shutdown-queued", String::new(), Duration::from_secs(30));
    fixture.runtime.shutdown().await;
    assert!(error(queued.await.unwrap()).contains("shutting down"));
    for (index, response) in active.into_iter().enumerate() {
        assert!(error(response.await.unwrap()).contains("shutting down"));
        assert_reaped(fixture.pid(&format!("active-{index}")).await);
    }
    assert!(!fixture.directory.path().join("shutdown-queued").exists());
    assert!(fixture.runtime.admit(Instant::now()).is_err());
    fixture.idle().await;
}

#[rstest]
#[case::malformed("printf invalid", "invalid template worker response")]
#[case::oversized("exec head -c 2048 /dev/zero", "response budget")]
#[case::exit_failure("exit 70", "heap budget")]
#[tokio::test]
async fn protocol_failures_return_capacity_and_allow_recovery(
    #[case] script: &str,
    #[case] expected: &str,
) {
    let fixture = Fixture::new();
    let result = fixture.submit(shell(script), String::new(), Duration::from_secs(3));
    assert!(error(result.await.unwrap()).contains(expected));
    fixture.idle().await;
    let result = fixture.submit(
        shell(r#"printf '%s' '{"Ok":{"output":"healthy","missing":[],"peak_heap_bytes":0}}'"#),
        String::new(),
        Duration::from_secs(3),
    );
    assert_eq!(result.await.unwrap().unwrap().into_parts().0, "healthy");
    fixture.runtime.shutdown().await;
}

#[tokio::test]
async fn spawn_failure_returns_capacity() {
    let fixture = Fixture::new();
    let result = fixture.submit(
        Command::new(Path::new("/nonexistent/hubuum-template-worker")),
        String::new(),
        Duration::from_secs(3),
    );
    assert!(error(result.await.unwrap()).contains("install it beside"));
    fixture.idle().await;
    fixture.runtime.shutdown().await;
}

#[test]
fn dropping_the_request_runtime_does_not_abandon_its_child() {
    let fixture = Fixture::new();
    let caller = Builder::new_current_thread().enable_all().build().unwrap();
    let pid = caller.block_on(async {
        let response = fixture.sleeping("runtime-dropped", String::new(), Duration::from_secs(30));
        tokio::spawn(async move {
            let _ = response.await;
        });
        fixture.pid("runtime-dropped").await
    });
    drop(caller);
    let cleanup = Builder::new_current_thread().enable_all().build().unwrap();
    cleanup.block_on(async {
        fixture.idle().await;
        assert_reaped(pid);
        fixture.runtime.shutdown().await;
    });
}

#[tokio::test]
async fn continuous_admission_drains_completed_job_tracking() {
    let fixture = Fixture::new();
    let mut responses = Vec::new();
    for _ in 0..1000 {
        // Maintain pressure as soon as a finished operation returns capacity.
        // No child needs to run: failed spawns exercise the fastest completion
        // path and can expose starvation of JoinSet cleanup under admission.
        let capacity = fixture
            .runtime
            .capacity
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        drop(capacity);
        responses.push(fixture.submit(
            Command::new("/nonexistent/hubuum-template-worker"),
            String::new(),
            Duration::from_secs(30),
        ));
    }
    for response in responses {
        assert!(
            error(response.await.expect("bounded supervisor remains alive"))
                .contains("install it beside")
        );
    }
    fixture.idle().await;
    fixture.runtime.shutdown().await;
}
