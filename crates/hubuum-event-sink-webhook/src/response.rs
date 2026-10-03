use std::time::Duration;

use chrono::{DateTime, Utc};
use hubuum_event_sinks_common::SinkError;
use hubuum_outbound_http::OutboundResponse;
use serde::Deserialize;
use serde_json::Value;

/// Opt-in response rules. Defaults retain legacy 2xx success and retry behavior.
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResponsePolicy {
    #[serde(default)]
    success_statuses: Vec<u16>,
    #[serde(default)]
    retry_statuses: Option<Vec<u16>>,
    #[serde(default)]
    rate_limit: bool,
    #[serde(default)]
    body: Option<BodyCheck>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum BodyCheck {
    TextEquals { value: String },
    JsonEquals { pointer: String, value: Value },
}

impl ResponsePolicy {
    pub(super) fn validate(&self) -> Result<(), SinkError> {
        if self
            .success_statuses
            .iter()
            .any(|status| !(200..300).contains(status))
            || self
                .retry_statuses
                .as_ref()
                .is_some_and(|statuses| statuses.iter().any(|status| !(300..600).contains(status)))
        {
            return Err(SinkError::permanent(
                "Response success_statuses must be 2xx; retry_statuses must be 3xx, 4xx or 5xx",
            ));
        }
        if let Some(BodyCheck::JsonEquals { pointer, .. }) = &self.body {
            if (!pointer.is_empty() && !pointer.starts_with('/')) || pointer.len() > 1024 {
                return Err(SinkError::permanent(
                    "Response JSON pointer must be empty or start with / and contain at most 1024 bytes",
                ));
            }
            let mut chars = pointer.chars();
            while let Some(character) = chars.next() {
                if character == '~' && !matches!(chars.next(), Some('0' | '1')) {
                    return Err(SinkError::permanent(
                        "Response JSON pointer has an invalid escape",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn check(
        &self,
        response: &OutboundResponse,
        response_limit: usize,
    ) -> Result<(), SinkError> {
        self.check_status(
            response.status_code(),
            response
                .headers()
                .get("retry-after")
                .and_then(Value::as_str),
        )?;
        if let Some(check) = &self.body {
            // The HTTP executor retains a bounded preview, not a full body. Never
            // accept a response predicate against a potentially truncated prefix.
            if response.body_preview().len() >= response_limit {
                return Err(SinkError::new(
                    "Webhook response is too large to verify acknowledgement",
                ));
            }
            if !check.matches(response.body_preview()) {
                return Err(SinkError::permanent(
                    "Webhook response did not match its configured acknowledgement",
                ));
            }
        }
        Ok(())
    }

    fn check_status(&self, status: u16, retry_after: Option<&str>) -> Result<(), SinkError> {
        if status == 429 && self.rate_limit {
            return Err(SinkError::rate_limited(retry_delay(
                retry_after,
                Utc::now(),
            )));
        }
        let success = if self.success_statuses.is_empty() {
            (200..300).contains(&status)
        } else {
            self.success_statuses.contains(&status)
        };
        if success {
            return Ok(());
        }
        let message = format!("Webhook delivery failed with HTTP {status}");
        if self
            .retry_statuses
            .as_ref()
            .is_none_or(|statuses| statuses.contains(&status))
        {
            Err(SinkError::new(message))
        } else {
            Err(SinkError::permanent(message))
        }
    }
}

impl BodyCheck {
    fn matches(&self, body: &str) -> bool {
        match self {
            Self::TextEquals { value } => body.trim() == value,
            Self::JsonEquals { pointer, value } => {
                serde_json::from_str::<Value>(body)
                    .ok()
                    .and_then(|body| body.pointer(pointer).cloned())
                    .as_ref()
                    == Some(value)
            }
        }
    }
}

fn retry_delay(header: Option<&str>, now: DateTime<Utc>) -> Duration {
    let delay = header
        .and_then(|value| {
            value.trim().parse::<u32>().ok().map(u64::from).or_else(|| {
                DateTime::parse_from_rfc2822(value)
                    .ok()
                    .map(|date| (date.with_timezone(&Utc) - now).num_seconds().max(1) as u64)
            })
        })
        .filter(|seconds| *seconds > 0)
        .unwrap_or(60);
    Duration::from_secs(delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hubuum_event_sinks_common::SinkFailure;
    use rstest::rstest;
    use serde_json::json;

    #[rstest]
    #[case(json!({}), 400, SinkFailure::Retryable)]
    #[case(json!({}), 429, SinkFailure::Retryable)]
    #[case(json!({"retry_statuses":[408,500,503]}), 400, SinkFailure::Permanent)]
    #[case(json!({"retry_statuses":[408,500,503]}), 503, SinkFailure::Retryable)]
    #[case(json!({"rate_limit":true}), 429, SinkFailure::RateLimited(Duration::from_secs(60)))]
    fn classifies_responses_without_changing_legacy_defaults(
        #[case] config: Value,
        #[case] status: u16,
        #[case] expected: SinkFailure,
    ) {
        let policy: ResponsePolicy = serde_json::from_value(config).unwrap();
        assert_eq!(
            policy.check_status(status, None).unwrap_err().failure(),
            expected
        );
    }

    #[rstest]
    #[case(None, 60)]
    #[case(Some("5"), 5)]
    #[case(Some("86401"), 86401)]
    #[case(Some("bad"), 60)]
    #[case(Some("0"), 60)]
    #[case(Some("-1"), 60)]
    #[case(Some("Thu, 01 Oct 2026 00:01:30 GMT"), 90)]
    #[case(Some("Wed, 30 Sep 2026 00:01:30 GMT"), 1)]
    fn parses_retry_after(#[case] header: Option<&str>, #[case] expected: u64) {
        let now = DateTime::parse_from_rfc3339("2026-10-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(retry_delay(header, now), Duration::from_secs(expected));
    }

    #[rstest]
    #[case(json!({"kind":"text_equals","value":"ok"}), "ok\n", true)]
    #[case(json!({"kind":"text_equals","value":"ok"}), "not ok", false)]
    #[case(json!({"kind":"json_equals","pointer":"/ok","value":true}), r#"{"ok":true}"#, true)]
    #[case(json!({"kind":"json_equals","pointer":"/ok","value":true}), r#"{"ok":false}"#, false)]
    #[case(json!({"kind":"json_equals","pointer":"/ok","value":true}), "not json", false)]
    #[case(json!({"kind":"json_equals","pointer":"/ok","value":true}), "{}", false)]
    #[case(json!({"kind":"json_equals","pointer":"/a~1b/~0","value":null}), r#"{"a/b":{"~":null}}"#, true)]
    fn checks_acknowledgement(#[case] check: Value, #[case] body: &str, #[case] expected: bool) {
        let check: BodyCheck = serde_json::from_value(check).unwrap();
        assert_eq!(check.matches(body), expected);
    }
}
