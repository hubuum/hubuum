//! A separate executable gives this process-wide lifecycle test fresh state.
use hubuum_templates::{TemplateExecution, TemplateLimits, shutdown_template_workers};

#[tokio::test]
async fn shutdown_before_first_use_keeps_admission_closed() {
    shutdown_template_workers().await;
    let error = TemplateExecution::new(
        "after-shutdown",
        "must not render",
        TemplateLimits::new(16, 50_000),
    )
    .render(&serde_json::json!({}))
    .await
    .err()
    .expect("shutdown must close lazy admission");
    assert!(error.to_string().contains("shutting down"));
}
