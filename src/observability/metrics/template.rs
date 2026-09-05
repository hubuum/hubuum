use hubuum_templates::WorkerEvent;
use opentelemetry::KeyValue;

use super::current;

pub(crate) fn template_worker_event(event: WorkerEvent) {
    if let Some(metrics) = current() {
        let attributes = [KeyValue::new("event", event.kind())];
        metrics.template_worker_events.add(1, &attributes);
        if !matches!(event.kind(), "admitted" | "started") {
            metrics
                .template_worker_duration
                .record(event.elapsed().as_secs_f64(), &attributes);
        }
    }
}

#[cfg(test)]
mod tests {
    use hubuum_templates::{TemplateExecution, TemplateLimits, set_worker_event_handler};
    use prometheus::{Encoder, TextEncoder};

    use super::*;

    #[tokio::test]
    async fn completed_workers_publish_lifecycle_counts_and_duration() {
        crate::observability::metrics::init().unwrap();
        set_worker_event_handler(template_worker_event);
        TemplateExecution::new(
            "metrics-private-name",
            "metrics-private-output",
            TemplateLimits::new(16, 50_000),
        )
        .render(&serde_json::json!({}))
        .await
        .unwrap();
        let mut output = Vec::new();
        TextEncoder::new()
            .encode(&current().unwrap().registry.gather(), &mut output)
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(
            output.lines().any(
                |line| line.starts_with("hubuum_template_worker_events_total{")
                    && line.contains("event=\"completed\"")
            ),
            "{output}"
        );
        assert!(
            output.lines().any(|line| line
                .starts_with("hubuum_template_worker_duration_seconds_count{")
                && line.contains("event=\"completed\"")),
            "{output}"
        );
        assert!(
            !output.contains("metrics-private-name") && !output.contains("metrics-private-output")
        );
    }
}
