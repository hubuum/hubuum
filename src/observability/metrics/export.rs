use std::time::Duration;

use opentelemetry::KeyValue;

use super::current;

fn export_attrs(scope: &'static str, content_type: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new("scope", scope),
        KeyValue::new("content_type", content_type),
    ]
}

fn template_attrs(template_id: i32, template_name: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("template_id", template_id.to_string()),
        KeyValue::new("template_name", template_name.to_string()),
    ]
}

fn export_phase_attrs(phase: &'static str, template_id: Option<i32>) -> [KeyValue; 2] {
    [
        KeyValue::new("phase", phase),
        KeyValue::new(
            "template_id",
            template_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string()),
        ),
    ]
}

pub fn export_output_cleanup_run() {
    if let Some(metrics) = current() {
        metrics.export_output_cleanup_runs.add(1, &[]);
    }
}

pub fn export_output_cleanup_failed() {
    if let Some(metrics) = current() {
        metrics.export_output_cleanup_failures.add(1, &[]);
    }
}

pub fn export_output_cleanup_deleted(count: usize) {
    if let Some(metrics) = current() {
        metrics
            .export_output_cleanup_deleted
            .add(u64::try_from(count).unwrap_or(u64::MAX), &[]);
    }
}

pub fn export_template_observed(template_id: i32, template_name: &str) {
    if let Some(metrics) = current() {
        metrics
            .export_template_info
            .record(1, &template_attrs(template_id, template_name));
    }
}

pub fn export_phase_duration(phase: &'static str, duration: Duration, template_id: Option<i32>) {
    if let Some(metrics) = current() {
        metrics.export_duration.record(
            duration.as_secs_f64(),
            &export_phase_attrs(phase, template_id),
        );
    }
}

pub fn export_completed(scope: &'static str, content_type: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .export_completions
            .add(1, &export_attrs(scope, content_type));
    }
}

pub fn export_truncated(scope: &'static str, content_type: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .export_truncations
            .add(1, &export_attrs(scope, content_type));
    }
}

pub fn export_warnings(scope: &'static str, content_type: &'static str, count: usize) {
    if let Some(metrics) = current() {
        metrics.export_warnings.add(
            u64::try_from(count).unwrap_or(u64::MAX),
            &export_attrs(scope, content_type),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_phase_attributes_use_stable_template_id() {
        assert_eq!(
            export_phase_attrs("render", Some(42)),
            [
                KeyValue::new("phase", "render"),
                KeyValue::new("template_id", "42"),
            ]
        );
    }

    #[test]
    fn untemplated_export_phase_attributes_use_none_identity() {
        assert_eq!(
            export_phase_attrs("render", None),
            [
                KeyValue::new("phase", "render"),
                KeyValue::new("template_id", "none"),
            ]
        );
    }

    #[test]
    fn template_info_attributes_map_id_to_name() {
        assert_eq!(
            template_attrs(42, "inventory"),
            [
                KeyValue::new("template_id", "42"),
                KeyValue::new("template_name", "inventory"),
            ]
        );
    }
}
