use opentelemetry::KeyValue;

pub fn tracing_config(mode: &'static str, ratio: f64, queue_capacity: usize) {
    let Some(metrics) = super::current() else {
        return;
    };
    metrics
        .tracing_info
        .record(1, &[KeyValue::new("sampling_mode", mode)]);
    metrics.tracing_sample_ratio.record(ratio, &[]);
    metrics
        .tracing_queue_capacity
        .record(u64::try_from(queue_capacity).unwrap_or(u64::MAX), &[]);
    metrics.tracing_queue_utilization.record(0, &[]);
}

pub fn trace_span_lifecycle(category: &'static str, state: &'static str, count: usize) {
    let Some(metrics) = super::current() else {
        return;
    };
    metrics.trace_spans.add(
        u64::try_from(count).unwrap_or(u64::MAX),
        &[
            KeyValue::new("category", category),
            KeyValue::new("state", state),
        ],
    );
}

pub fn trace_spans_dropped(reason: &'static str, count: usize) {
    if let Some(metrics) = super::current() {
        metrics.trace_spans_dropped.add(
            u64::try_from(count).unwrap_or(u64::MAX),
            &[KeyValue::new("reason", reason)],
        );
    }
}

pub fn trace_export_batch(outcome: &'static str, span_count: usize) {
    if let Some(metrics) = super::current() {
        metrics
            .trace_export_batches
            .add(1, &[KeyValue::new("outcome", outcome)]);
        metrics.trace_export_spans.add(
            u64::try_from(span_count).unwrap_or(u64::MAX),
            &[KeyValue::new("outcome", outcome)],
        );
    }
}

pub fn trace_queue_utilization(current: usize) {
    if let Some(metrics) = super::current() {
        metrics
            .tracing_queue_utilization
            .record(u64::try_from(current).unwrap_or(u64::MAX), &[]);
    }
}

pub fn trace_flush(outcome: &'static str) {
    if let Some(metrics) = super::current() {
        metrics
            .trace_flushes
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}
