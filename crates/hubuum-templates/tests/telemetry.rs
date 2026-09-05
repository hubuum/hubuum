//! Use the same process-wide subscriber as production. Keeping this in its own
//! integration executable avoids concurrent scoped-subscriber cache changes in
//! the subprocess supervisor unit tests.
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use hubuum_templates::{TemplateExecution, TemplateLimits, shutdown_template_workers};

#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);
impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn lifecycle_logging_reports_results_without_template_content() {
    let capture = Capture(Arc::new(Mutex::new(Vec::new())));
    let writer = capture.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
    let context = serde_json::json!({ "private_context_key": "private-rendered-output" });
    TemplateExecution::new(
        "private-template-name",
        "{{ private_context_key }}",
        TemplateLimits::new(16, 50_000),
    )
    .render(&context)
    .await
    .unwrap();
    let error = TemplateExecution::new(
        "private-error-template",
        "{{ private_missing_value }}",
        TemplateLimits::new(16, 50_000),
    )
    .render(&context)
    .await
    .err()
    .expect("undefined value must fail");
    shutdown_template_workers().await;
    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    for lifecycle in ["admitted", "started", "completed", "render_failed"] {
        assert!(logs.contains(lifecycle), "missing {lifecycle}: {logs}");
    }
    assert!(logs.contains("operation_id=") && logs.contains("pid="));
    assert!(
        !logs.contains("private_")
            && !logs.contains("private-")
            && !logs.contains(&error.to_string()),
        "sensitive content in lifecycle logs"
    );
}
