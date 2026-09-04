use std::time::Duration;

use opentelemetry::{KeyValue, Value};

use super::{HttpInFlightGuard, current};
use crate::operational_contracts::http_method_metric_label;

pub fn http_request_started() -> HttpInFlightGuard {
    http_request_started_for_route("unknown")
}

pub fn http_request_started_for_route(route: &str) -> HttpInFlightGuard {
    if let Some(metrics) = current() {
        metrics
            .http_in_flight
            .add(1, &[KeyValue::new("route", Value::from(route.to_owned()))]);
        return HttpInFlightGuard::new(Some(route.to_owned()));
    }
    HttpInFlightGuard::new(None)
}

pub fn http_request_finished(method: &str, route: &str, status_code: u16, duration: Duration) {
    if let Some(metrics) = current() {
        let status_family = status_family(status_code);
        let method = Value::from(http_method_metric_label(method).to_owned());
        let route = Value::from(route.to_owned());
        let count_attrs = [
            KeyValue::new("method", method.clone()),
            KeyValue::new("route", route.clone()),
            KeyValue::new("status_code", i64::from(status_code)),
            KeyValue::new("status_family", status_family),
        ];
        let duration_attrs = [
            KeyValue::new("method", method),
            KeyValue::new("route", route),
            KeyValue::new("status_family", status_family),
        ];
        metrics.http_requests.add(1, &count_attrs);
        metrics
            .http_request_duration
            .record(duration.as_secs_f64(), &duration_attrs);
    }
}

pub fn api_error(error_class: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .api_errors
            .add(1, &[KeyValue::new("class", error_class)]);
    }
}

pub fn extraction_failure(kind: &'static str) {
    if let Some(metrics) = current() {
        metrics
            .extraction_failures
            .add(1, &[KeyValue::new("kind", kind)]);
    }
}

fn status_family(status_code: u16) -> &'static str {
    match status_code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::http_method_metric_label;

    #[test]
    fn extension_http_methods_use_the_bounded_fallback_label() {
        assert_eq!(http_method_metric_label("PROPFIND"), "OTHER");
    }

    #[test]
    fn standard_http_methods_keep_their_label() {
        assert_eq!(http_method_metric_label("GET"), "GET");
    }
}
