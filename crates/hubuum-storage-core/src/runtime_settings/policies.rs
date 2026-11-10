//! Validated policies for settings consumed at request, job, and batch boundaries.
use super::{ClientAllowlist, MetricsPath, TrustedProxies};
use crate::StorageValidationError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt::Display;

fn bounded<T: PartialOrd + Display>(
    value: &T,
    minimum: T,
    maximum: T,
    name: &str,
) -> Result<(), StorageValidationError> {
    if *value < minimum || *value > maximum {
        return Err(StorageValidationError::invalid(format!(
            "{name} must be between {minimum} and {maximum}"
        )));
    }
    Ok(())
}

macro_rules! field_schema {
    (number($minimum:expr, $maximum:expr)) => { json!({"type":"integer", "minimum":$minimum, "maximum":$maximum}) };
    (text()) => { json!({"type":"string"}) };
    (boolean()) => { json!({"type":"boolean"}) };
    (text_length($minimum:expr, $maximum:expr)) => { json!({"type":"string", "minLength":$minimum, "maxLength":$maximum}) };
}
macro_rules! validate_field {
    ($value:expr, $name:expr, number($minimum:expr, $maximum:expr)) => {
        bounded($value, $minimum, $maximum, $name)
    };
    ($value:expr, $name:expr, text_length($minimum:expr, $maximum:expr)) => {
        bounded(&$value.len(), $minimum, $maximum, $name)
    };
    ($value:expr, $name:expr, $kind:ident()) => {
        Ok::<_, StorageValidationError>(())
    };
}

// The deserializer is the fallible constructor: private fields cannot bypass validation.
macro_rules! policy {
    ($name:ident { $( $field:ident: $ty:ty => ($description:literal, $kind:ident $bounds:tt) ),+ $(,)? } validate $validate:expr) => {
        #[derive(Clone, Debug, PartialEq, Eq, Serialize)]
        pub struct $name { $( $field: $ty, )+ }
        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Input { $( $field: $ty, )+ }
                let input = Input::deserialize(deserializer)?;
                $( validate_field!(&input.$field, stringify!($field), $kind $bounds).map_err(serde::de::Error::custom)?; )+
                let value = Self { $( $field: input.$field, )+ };
                ($validate)(&value).map_err(serde::de::Error::custom)?;
                Ok(value)
            }
        }
        impl $name {
            $( #[doc = $description] pub fn $field(&self) -> &$ty { &self.$field } )+
            /// JSON Schema for the persisted policy document and its validated bounds.
            pub fn schema() -> Value {
                let mut properties = serde_json::Map::new();
                $( let mut field = field_schema!($kind $bounds);
                   field["description"] = json!($description);
                   field["title"] = json!(stringify!($field).replace('_', " "));
                   properties.insert(stringify!($field).to_string(), field); )+
                json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "additionalProperties": false,
                    "properties": properties, "required": [$( stringify!($field) ),+]})
            }
        }
    };
}

policy! { StorageObservabilityPolicy {
    log_level: String => ("Logging filter, for example info or module=debug. Applied immediately on reconciliation.", text_length(1, 4096)),
    metrics_enabled: bool => ("Expose and collect metrics. Existing counters are retained while disabled.", boolean()),
    metrics_path: MetricsPath => ("Literal absolute scrape path, excluding application routes. The previous path stops serving metrics after reconciliation.", text()),
} validate |_: &StorageObservabilityPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageNetworkPolicy {
    trust_ip_headers: bool => ("Honor forwarded IP headers only through the configured proxy trust policy.", boolean()),
    trusted_proxies: TrustedProxies => ("Comma-separated trusted proxy IP addresses or CIDR networks. Empty means no trusted networks.", text()),
    trusted_proxy_hops: usize => ("Trusted proxy hop count, used when trusted proxy networks are empty. Zero disables hop-based trust.", number(0, 255)),
    client_allowlist: ClientAllowlist => ("Comma-separated client IP addresses or CIDR networks, or * for all clients. The updating administrator must remain allowed.", text()),
} validate |_: &StorageNetworkPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageQueryLimitsPolicy {
    max_transitive_depth: i32 => ("Maximum traversal depth for new queries.", number(1, 512)),
} validate |_: &StorageQueryLimitsPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageTasksPolicy {
    poll_interval_ms: u64 => ("Idle polling interval in milliseconds. Changes wake sleeping workers.", number(1, 2147483647)),
    lease_seconds: u64 => ("Lease duration for newly claimed tasks. Must exceed heartbeat_seconds.", number(1, 2147483647)),
    heartbeat_seconds: u64 => ("Heartbeat interval for newly claimed tasks. Existing claims retain their lease and heartbeat pair.", number(1, 2147483647)),
    recovery_interval_seconds: u64 => ("Interval between recovery passes. Persisted lease expiry remains authoritative.", number(1, 2147483647)),
    computed_reindex_batch_size: usize => ("Maximum objects rebuilt per computed-field reindex batch.", number(1, 1000)),
    imports_per_user: usize => ("Active import admission limit per user. Existing tasks continue.", number(1, 2147483647)),
    exports_per_user: usize => ("Active export admission limit per user. Existing tasks continue.", number(1, 2147483647)),
    remote_calls_per_user: usize => ("Active remote-call admission limit per user. Existing tasks continue.", number(1, 2147483647)),
} validate |v: &StorageTasksPolicy| { if v.heartbeat_seconds >= v.lease_seconds { return Err(StorageValidationError::invalid("heartbeat_seconds must be less than lease_seconds")); } Ok(()) } }

policy! { StorageRemoteCallsPolicy {
    timeout_ms: u64 => ("Timeout cap for newly dispatched remote calls, in milliseconds.", number(1, 2147483647)),
    max_response_bytes: usize => ("Response byte limit for newly dispatched remote calls.", number(1, 2147483647)),
    allow_private_targets: bool => ("Allow private destinations only when the deployment also permits them. Targets are checked at dispatch.", boolean()),
} validate |_: &StorageRemoteCallsPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageTokensPolicy {
    lifetime_hours: i64 => ("Default lifetime of newly issued tokens. Existing token expiry and legacy token cutoffs are unchanged.", number(1, 2147483647)),
    retention_days: i64 => ("Expired-token retention used by the next purge batch.", number(1, 2147483647)),
    purge_enabled: bool => ("Run token retention batches on existing background-worker replicas.", boolean()),
    purge_interval_seconds: u64 => ("Idle interval between token retention batches.", number(1, 2147483647)),
    purge_batch_size: usize => ("Maximum expired tokens deleted per batch.", number(10, 2147483647)),
} validate |_: &StorageTokensPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageLoginRateLimitPolicy {
    enabled: bool => ("Rate-limit new login attempts. Counters and existing lockouts are retained.", boolean()),
    attempts_per_identity: usize => ("Maximum attempts per identity in one window.", number(1, 2147483647)),
    attempts_per_account: usize => ("Maximum attempts across providers for an account. Zero disables this dimension.", number(0, 2147483647)),
    attempts_per_ip: usize => ("Maximum attempts per client IP. Zero disables this dimension.", number(0, 2147483647)),
    attempts_per_subnet: usize => ("Maximum attempts per client subnet. Zero disables this dimension.", number(0, 2147483647)),
    window_seconds: u64 => ("Window for new login attempts. Retained observations are evaluated with this window; already discarded observations cannot be restored.", number(1, 2147483647)),
    backoff_base_seconds: u64 => ("Initial backoff for new lockouts. Existing lockout deadlines are preserved.", number(1, 2147483647)),
    backoff_max_seconds: u64 => ("Maximum backoff for new lockouts. Must be at least backoff_base_seconds.", number(1, 2147483647)),
} validate |v: &StorageLoginRateLimitPolicy| { if v.backoff_base_seconds > v.backoff_max_seconds { return Err(StorageValidationError::invalid("backoff_base_seconds must not exceed backoff_max_seconds")); } Ok(()) } }

policy! { StorageEventsPolicy {
    fanout_batch_size: usize => ("Maximum events in the next fanout batch.", number(1, 2147483647)),
    fanout_poll_interval_ms: u64 => ("Idle fanout polling interval in milliseconds.", number(1, 2147483647)),
    delivery_batch_size: usize => ("Maximum deliveries claimed by the next batch.", number(1, 2147483647)),
    delivery_poll_interval_ms: u64 => ("Idle delivery polling interval in milliseconds.", number(1, 2147483647)),
    delivery_max_attempts: i32 => ("Attempt limit for future delivery attempts. Existing attempt counts are retained; terminal failures are not revived.", number(1, 2147483647)),
    delivery_transport_timeout_ms: u64 => ("Transport timeout for future delivery batches. Must fit the deployment delivery lease.", number(1, 2147483647)),
    retention_days: i64 => ("Event retention used by the next purge batch.", number(1, 2147483647)),
    delivery_retention_days: i64 => ("Delivery retention used by the next purge batch.", number(1, 2147483647)),
    purge_enabled: bool => ("Run event retention batches on existing background-worker replicas.", boolean()),
    purge_interval_seconds: u64 => ("Idle interval between event retention batches.", number(1, 2147483647)),
    purge_batch_size: usize => ("Maximum events or deliveries processed by a retention batch.", number(1, 2147483647)),
    file_archive_enabled: bool => ("Archive events before deletion using the deployment archive path. Archive failure prevents deletion.", boolean()),
} validate |_: &StorageEventsPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageExportsPolicy {
    output_retention_hours: i64 => ("Retention for newly created output. Persisted expiry timestamps are unchanged.", number(1, 2147483647)),
    cleanup_interval_seconds: u64 => ("Interval between output cleanup passes.", number(1, 2147483647)),
    max_output_bytes: usize => ("Maximum output bytes for newly started exports.", number(1, 2147483647)),
    stage_timeout_ms: u64 => ("Stage time budget for new exports. Checked after completion; this does not interrupt a running stage.", number(1, 2147483647)),
    storage_query_budget_ms: u64 => ("Budget for each storage read in a new export. Zero disables the budget.", number(0, 2147483647)),
    template_recursion_limit: usize => ("Template recursion limit for new validation and rendering operations.", number(1, 2147483647)),
    template_max_objects: usize => ("Maximum objects available to a new template render.", number(1, 2147483647)),
    template_fuel: u64 => ("Template execution fuel for new renders.", number(1, 2147483647)),
} validate |_: &StorageExportsPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageBackupsPolicy {
    max_active_per_user: usize => ("Active backup admission limit per user. Existing tasks continue.", number(1, 2147483647)),
    max_output_bytes: usize => ("Maximum output bytes for newly started backups.", number(1, 2147483647)),
    max_capture_rows: usize => ("Maximum rows captured by newly started backups.", number(1, 2147483647)),
    retention_hours: i64 => ("Retention for newly created backup outputs. Existing expiry timestamps are unchanged.", number(1, 2147483647)),
} validate |_: &StorageBackupsPolicy| Ok::<_, StorageValidationError>(()) }

policy! { StorageRestoresPolicy {
    max_upload_bytes: usize => ("Maximum bytes accepted by new restore uploads. An upload retains its initial limit.", number(1, 2147483647)),
    staging_retention_minutes: i64 => ("Retention for newly created restore stages. Existing expiry timestamps are unchanged.", number(1, 2147483647)),
} validate |_: &StorageRestoresPolicy| Ok::<_, StorageValidationError>(()) }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StorageRuntimeSettingsGroup, StorageRuntimeSettingsValue};
    use rstest::rstest;

    fn baseline(group: StorageRuntimeSettingsGroup) -> Value {
        match group {
            StorageRuntimeSettingsGroup::Observability => {
                json!({"log_level": "info", "metrics_enabled": false, "metrics_path": "/metrics"})
            }
            StorageRuntimeSettingsGroup::Network => {
                json!({"trust_ip_headers": false, "trusted_proxies": "", "trusted_proxy_hops": 0, "client_allowlist": "*"})
            }
            StorageRuntimeSettingsGroup::QueryLimits => json!({"max_transitive_depth": 1}),
            StorageRuntimeSettingsGroup::Tasks => {
                json!({"poll_interval_ms": 1, "lease_seconds": 60, "heartbeat_seconds": 20, "recovery_interval_seconds": 1, "computed_reindex_batch_size": 1, "imports_per_user": 1, "exports_per_user": 1, "remote_calls_per_user": 1})
            }
            StorageRuntimeSettingsGroup::RemoteCalls => {
                json!({"timeout_ms": 1, "max_response_bytes": 1, "allow_private_targets": false})
            }
            StorageRuntimeSettingsGroup::Tokens => {
                json!({"lifetime_hours": 1, "retention_days": 1, "purge_enabled": false, "purge_interval_seconds": 1, "purge_batch_size": 10})
            }
            StorageRuntimeSettingsGroup::LoginRateLimit => {
                json!({"enabled": false, "attempts_per_identity": 1, "attempts_per_account": 0, "attempts_per_ip": 0, "attempts_per_subnet": 0, "window_seconds": 1, "backoff_base_seconds": 1, "backoff_max_seconds": 1})
            }
            StorageRuntimeSettingsGroup::Events => {
                json!({"fanout_batch_size": 1, "fanout_poll_interval_ms": 1, "delivery_batch_size": 1, "delivery_poll_interval_ms": 1, "delivery_max_attempts": 1, "delivery_transport_timeout_ms": 1, "retention_days": 1, "delivery_retention_days": 1, "purge_enabled": false, "purge_interval_seconds": 1, "purge_batch_size": 1, "file_archive_enabled": false})
            }
            StorageRuntimeSettingsGroup::Exports => {
                json!({"output_retention_hours": 1, "cleanup_interval_seconds": 1, "max_output_bytes": 1, "stage_timeout_ms": 1, "storage_query_budget_ms": 0, "template_recursion_limit": 1, "template_max_objects": 1, "template_fuel": 1})
            }
            StorageRuntimeSettingsGroup::Backups => {
                json!({"max_active_per_user": 1, "max_output_bytes": 1, "max_capture_rows": 1, "retention_hours": 1})
            }
            StorageRuntimeSettingsGroup::Restores => {
                json!({"max_upload_bytes": 1, "staging_retention_minutes": 1})
            }
            _ => unreachable!(),
        }
    }

    #[rstest]
    #[case(StorageRuntimeSettingsGroup::Observability)]
    #[case(StorageRuntimeSettingsGroup::Network)]
    #[case(StorageRuntimeSettingsGroup::QueryLimits)]
    #[case(StorageRuntimeSettingsGroup::Tasks)]
    #[case(StorageRuntimeSettingsGroup::RemoteCalls)]
    #[case(StorageRuntimeSettingsGroup::Tokens)]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit)]
    #[case(StorageRuntimeSettingsGroup::Events)]
    #[case(StorageRuntimeSettingsGroup::Exports)]
    #[case(StorageRuntimeSettingsGroup::Backups)]
    #[case(StorageRuntimeSettingsGroup::Restores)]
    fn policies_round_trip(#[case] group: StorageRuntimeSettingsGroup) {
        let value = StorageRuntimeSettingsValue::from_document(group, &baseline(group)).unwrap();
        assert_eq!(
            StorageRuntimeSettingsValue::from_document(group, &value.to_document()).unwrap(),
            value
        );
    }

    #[rstest]
    #[case(StorageRuntimeSettingsGroup::Network, "trusted_proxy_hops", json!(-1))]
    #[case(StorageRuntimeSettingsGroup::Network, "trusted_proxy_hops", json!(256u64))]
    #[case(StorageRuntimeSettingsGroup::QueryLimits, "max_transitive_depth", json!(0))]
    #[case(StorageRuntimeSettingsGroup::QueryLimits, "max_transitive_depth", json!(513u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "poll_interval_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "poll_interval_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "lease_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "lease_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "heartbeat_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "heartbeat_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "recovery_interval_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "recovery_interval_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "computed_reindex_batch_size", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "computed_reindex_batch_size", json!(1001u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "imports_per_user", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "imports_per_user", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "exports_per_user", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "exports_per_user", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "remote_calls_per_user", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tasks, "remote_calls_per_user", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::RemoteCalls, "timeout_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::RemoteCalls, "timeout_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::RemoteCalls, "max_response_bytes", json!(0))]
    #[case(StorageRuntimeSettingsGroup::RemoteCalls, "max_response_bytes", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "lifetime_hours", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "lifetime_hours", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "retention_days", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "retention_days", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "purge_interval_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "purge_interval_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "purge_batch_size", json!(9))]
    #[case(StorageRuntimeSettingsGroup::Tokens, "purge_batch_size", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_identity", json!(0))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_identity", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_account", json!(-1))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_account", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_ip", json!(-1))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_ip", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_subnet", json!(-1))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "attempts_per_subnet", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "window_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "window_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "backoff_base_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "backoff_base_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "backoff_max_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "backoff_max_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "fanout_batch_size", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "fanout_batch_size", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "fanout_poll_interval_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "fanout_poll_interval_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_batch_size", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_batch_size", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_poll_interval_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_poll_interval_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_max_attempts", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_max_attempts", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_transport_timeout_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_transport_timeout_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "retention_days", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "retention_days", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_retention_days", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "delivery_retention_days", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "purge_interval_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "purge_interval_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Events, "purge_batch_size", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Events, "purge_batch_size", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "output_retention_hours", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "output_retention_hours", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "cleanup_interval_seconds", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "cleanup_interval_seconds", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "max_output_bytes", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "max_output_bytes", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "stage_timeout_ms", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "stage_timeout_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "storage_query_budget_ms", json!(-1))]
    #[case(StorageRuntimeSettingsGroup::Exports, "storage_query_budget_ms", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_recursion_limit", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_recursion_limit", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_max_objects", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_max_objects", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_fuel", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Exports, "template_fuel", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_active_per_user", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_active_per_user", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_output_bytes", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_output_bytes", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_capture_rows", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Backups, "max_capture_rows", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Backups, "retention_hours", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Backups, "retention_hours", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Restores, "max_upload_bytes", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Restores, "max_upload_bytes", json!(2147483648u64))]
    #[case(StorageRuntimeSettingsGroup::Restores, "staging_retention_minutes", json!(0))]
    #[case(StorageRuntimeSettingsGroup::Restores, "staging_retention_minutes", json!(2147483648u64))]
    fn policies_reject_out_of_range_fields(
        #[case] group: StorageRuntimeSettingsGroup,
        #[case] field: &str,
        #[case] value: Value,
    ) {
        let mut document = baseline(group);
        document[field] = value;
        assert!(StorageRuntimeSettingsValue::from_document(group, &document).is_err());
    }

    #[rstest]
    #[case(StorageRuntimeSettingsGroup::Tasks, "heartbeat_seconds", json!(60))]
    #[case(StorageRuntimeSettingsGroup::LoginRateLimit, "backoff_base_seconds", json!(2))]
    #[case(StorageRuntimeSettingsGroup::Observability, "metrics_path", json!("/api/v1/admin"))]
    #[case(StorageRuntimeSettingsGroup::Network, "trusted_proxies", json!("invalid/33"))]
    #[case(StorageRuntimeSettingsGroup::Network, "client_allowlist", json!(""))]
    #[case(StorageRuntimeSettingsGroup::Exports, "unknown", json!(1))]
    fn policies_reject_invalid_combinations_and_definitions(
        #[case] group: StorageRuntimeSettingsGroup,
        #[case] field: &str,
        #[case] value: Value,
    ) {
        let mut document = baseline(group);
        document[field] = value;
        assert!(StorageRuntimeSettingsValue::from_document(group, &document).is_err());
    }
}
