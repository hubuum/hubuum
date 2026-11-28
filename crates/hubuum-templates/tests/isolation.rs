use hubuum_templates::{MAX_WORKER_HEAP_BYTES, TemplateExecution, TemplateLimits};
use rstest::rstest;
use serde_json::{Value, json};

#[tokio::test]
async fn oversized_output_is_rejected_without_allocating_the_complete_output() {
    let error = TemplateExecution::new(
        "output",
        "{% for _ in range(10000) %}0123456789{% endfor %}",
        TemplateLimits::new(64, 500_000),
    )
    .max_output_bytes(1024)
    .render(&json!({}))
    .await
    .err()
    .expect("output limit should fail");
    assert!(error.to_string().contains("output limit"));
}

#[tokio::test]
async fn intermediate_allocation_failure_is_contained_in_the_worker() {
    // Each repeated string is legal to MiniJinja individually; retaining several
    // captures exceeds the whole-worker budget before anything reaches stdout.
    let source =
        "{% set a = 'x' * 60000000 %}{% set b = a ~ a %}{% set c = b ~ b %}{{ c | length }}";
    // Let allocation reach the independent heap guard before fuel admission.
    let error = TemplateExecution::new("allocation", source, TemplateLimits::new(64, u64::MAX))
        .render(&json!({}))
        .await
        .err()
        .expect("intermediate allocation must be bounded");
    assert!(error.to_string().contains("heap budget"), "{error}");
    let healthy =
        TemplateExecution::new("healthy", "still running", TemplateLimits::new(64, 50_000))
            .render(&json!({}))
            .await
            .unwrap();
    assert!(healthy.peak_heap_bytes() < MAX_WORKER_HEAP_BYTES);
    assert_eq!(healthy.into_parts().0, "still running");
}

#[tokio::test]
async fn macro_capture_failure_is_contained_in_the_worker() {
    let source = "{% set chunk = 'x' * 60000000 %}{% macro large() %}{% for _ in range(10) %}{{ chunk }}{% endfor %}{% endmacro %}{{ large() | length }}";
    // Let capture reach the independent heap guard before fuel admission.
    let error = TemplateExecution::new("capture", source, TemplateLimits::new(64, u64::MAX))
        .render(&json!({}))
        .await
        .err()
        .expect("macro capture must be bounded");
    assert!(error.to_string().contains("heap budget"), "{error}");
}

#[rstest]
#[case::filter_input("{{ payload | length }}", json!({ "payload": "x".repeat(64 * 1024) }))]
#[case::string_allocation("{{ 'x' * 65536 }}", json!({}))]
#[tokio::test]
async fn data_processing_is_bounded_by_fuel(#[case] source: &str, #[case] context: Value) {
    let error = TemplateExecution::new("data", source, TemplateLimits::new(64, 100))
        .render(&context)
        .await
        .err()
        .expect("data work must consume fuel");

    assert!(error.to_string().contains("fuel"), "{error}");
}

#[tokio::test]
async fn packaged_worker_renders_with_the_service_environment() {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::process::Command;

    let mut command = Command::new(env!("CARGO_BIN_EXE_hubuum-template-worker"));
    command.env_clear().kill_on_drop(true);
    #[cfg(windows)]
    {
        command.envs(hubuum_templates::windows_worker_environment());
        // Preserve the original CreateProcessW HRESULT in the test log.
        command.env("RAPPCT_DEBUG_LAUNCH", "1");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let request = json!({
        "operation": "Render",
        "max_output_bytes": 4096,
        "context": {"name": "Hubuum"},
        "templates": [{
            "source": "Hello {{ name }}", "name": "greeting", "sources": [],
            "limits": {"recursion_limit": 16, "fuel": 10000, "max_output_bytes": 4096},
            "auto_escape": "None", "keep_trailing_newline": false,
            "missing_data": "Strict", "max_output_bytes": 4096
        }]
    });
    let sent = child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&request).unwrap())
        .await;
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("worker timed out")
        .unwrap();
    assert!(
        output.status.success(),
        "worker {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    sent.unwrap();
    let response: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["Ok"][0]["output"], "Hello Hubuum");
}

#[cfg(windows)]
#[test]
fn ordinary_appcontainer_is_refused_before_rendering() {
    use std::io::{PipeReader, Read};
    use std::os::windows::io::OwnedHandle;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::Duration;

    use rappct::{
        AppContainerProfile, JobLimits, LaunchOptions, SecurityCapabilitiesBuilder, StdioConfig,
        launch_in_container_with_io,
    };

    let exe = env!("CARGO_BIN_EXE_hubuum-template-worker");
    let profile = AppContainerProfile::ensure(
        "Hubuum.TemplateWorker",
        "Hubuum template worker",
        Some("Isolated Hubuum template rendering"),
    )
    .unwrap();
    assert!(
        Command::new(exe)
            .arg("--prepare-sandbox")
            .status()
            .unwrap()
            .success()
    );
    let ordinary = SecurityCapabilitiesBuilder::new(&profile.sid)
        .lpac(false)
        .build()
        .unwrap();
    let options = LaunchOptions {
        exe: PathBuf::from(exe),
        cwd: Some(PathBuf::from(std::env::var_os("SystemRoot").unwrap()).join("System32")),
        env: Some(hubuum_templates::windows_worker_environment()),
        stdio: StdioConfig::Pipe,
        join_job: Some(JobLimits {
            kill_on_job_close: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut child = launch_in_container_with_io(&ordinary, &options).unwrap();
    drop(child.stdin.take());
    let diagnostics = child.stderr.take().unwrap();
    let status = child.wait(Some(Duration::from_secs(30))).unwrap();
    let mut message = String::new();
    PipeReader::from(OwnedHandle::from(diagnostics))
        .take(1024)
        .read_to_string(&mut message)
        .unwrap();
    assert_eq!(status, 71, "{message}");
    assert!(
        message.contains("restricted-package access check"),
        "{message}"
    );
}
