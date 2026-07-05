use std::fmt;

use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use serde::ser::{SerializeMap, Serializer};
use tracing_serde::AsSerde;

use crate::events::{Action, EntityType};
use crate::models::Permissions;

pub struct HubuumLoggingFormat;

impl HubuumLoggingFormat {}

pub fn init_json_logging(log_level: &str) -> Result<(), String> {
    let filter = EnvFilter::try_new(log_level)
        .map_err(|err| format!("Error parsing log level '{log_level}': {err}"))?;

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .event_format(HubuumLoggingFormat),
        )
        .try_init()
        .map_err(|err| format!("Failed to initialize logging: {err}"))
}

pub fn log_operation_mutation(
    entity_type: EntityType,
    action: Action,
    entity_id: Option<i32>,
    actor_principal_id: Option<i32>,
    request_id: Option<uuid::Uuid>,
    correlation_id: Option<&str>,
) {
    tracing::info!(
        message = "operation mutation recorded",
        operation = "mutation_recorded",
        mutation_phase = "recorded",
        entity_type = entity_type.as_str(),
        action = action.as_str(),
        entity_id,
        actor_principal_id,
        request_id = request_id.map(|id| id.to_string()),
        correlation_id,
    );
}

pub fn log_operation_read(
    entity_type: Option<EntityType>,
    action: Option<Action>,
    entity_id: Option<i32>,
) {
    let entity_type = entity_type.map(EntityType::as_str);
    let action = action.map(Action::as_str);
    tracing::debug!(
        message = "operation read",
        operation = "read",
        entity_type,
        action,
        entity_id,
    );
}

pub fn log_authorization_grant(
    principal_id: i32,
    permissions: &[Permissions],
    namespace_count: Option<usize>,
    reason: &'static str,
) {
    let permissions = permissions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let permissions = serde_json::to_string(&permissions).unwrap_or_else(|_| "[]".to_string());
    tracing::debug!(
        message = "authorization granted",
        event_type = "authorization",
        decision = "grant",
        principal_id,
        permissions_json = permissions,
        namespace_count,
        reason,
    );
}

pub fn log_authorization_denial(
    principal_id: i32,
    permissions: &[Permissions],
    namespace_count: Option<usize>,
    reason: &'static str,
) {
    let permissions = permissions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let permissions = serde_json::to_string(&permissions).unwrap_or_else(|_| "[]".to_string());
    tracing::warn!(
        message = "authorization denied",
        event_type = "authorization",
        decision = "deny",
        principal_id,
        permissions_json = permissions,
        namespace_count,
        reason,
    );
}

fn json_field_name(field_name: &str) -> Option<&str> {
    field_name.strip_suffix("_json")
}

fn serialize_entry<S, V>(
    serializer_map: &mut S,
    field_name: &str,
    value: &V,
) -> Result<(), S::Error>
where
    S: SerializeMap,
    V: serde::Serialize + ?Sized,
{
    serializer_map.serialize_entry(field_name, value)
}

fn serialize_json_aware_str_entry<S>(
    serializer_map: &mut S,
    field_name: &str,
    value: &str,
) -> Result<(), S::Error>
where
    S: SerializeMap,
{
    let Some(stripped_field_name) = json_field_name(field_name) else {
        return serializer_map.serialize_entry(field_name, value);
    };

    match serde_json::from_str::<serde_json::Value>(value) {
        Ok(value) => serializer_map.serialize_entry(stripped_field_name, &value),
        Err(_) => serializer_map.serialize_entry(stripped_field_name, value),
    }
}

fn serialize_json_aware_value_entry<S>(
    serializer_map: &mut S,
    field_name: &str,
    value: &serde_json::Value,
) -> Result<(), S::Error>
where
    S: SerializeMap,
{
    match (json_field_name(field_name), value.as_str()) {
        (Some(stripped_field_name), Some(value)) => {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(value) => serializer_map.serialize_entry(stripped_field_name, &value),
                Err(_) => serializer_map.serialize_entry(stripped_field_name, value),
            }
        }
        (Some(stripped_field_name), None) => {
            serializer_map.serialize_entry(stripped_field_name, value)
        }
        (None, _) => serializer_map.serialize_entry(field_name, value),
    }
}

struct JsonFieldVisitor<S: SerializeMap> {
    serializer: S,
    state: Result<(), S::Error>,
}

impl<S> JsonFieldVisitor<S>
where
    S: SerializeMap,
{
    fn new(serializer: S) -> Self {
        Self {
            serializer,
            state: Ok(()),
        }
    }

    fn take_serializer(self) -> Result<S, S::Error> {
        self.state.map(|_| self.serializer)
    }

    fn record_entry<V>(&mut self, field: &Field, value: &V)
    where
        V: serde::Serialize + ?Sized,
    {
        if self.state.is_ok() {
            self.state = serialize_entry(&mut self.serializer, field.name(), value);
        }
    }

    fn record_str_entry(&mut self, field: &Field, value: &str) {
        if self.state.is_ok() {
            self.state = serialize_json_aware_str_entry(&mut self.serializer, field.name(), value);
        }
    }
}

impl<S> Visit for JsonFieldVisitor<S>
where
    S: SerializeMap,
{
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_entry(field, &value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_entry(field, &value);
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_entry(field, &value);
    }

    fn record_i128(&mut self, field: &Field, value: i128) {
        self.record_entry(field, &value.to_string());
    }

    fn record_u128(&mut self, field: &Field, value: u128) {
        self.record_entry(field, &value.to_string());
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record_entry(field, &value);
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_str_entry(field, value);
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.state.is_ok() {
            self.state = serialize_entry(
                &mut self.serializer,
                field.name(),
                &format_args!("{value:?}"),
            );
        }
    }
}

impl<S, N> FormatEvent<S, N> for HubuumLoggingFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let meta = event.metadata();

        let mut s = Vec::<u8>::new();
        let mut serializer = serde_json::Serializer::new(&mut s);
        let mut serializer_map = serializer
            .serialize_map(None)
            .map_err(|_| std::fmt::Error)?;

        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        serializer_map
            .serialize_entry("time", &timestamp)
            .map_err(|_| std::fmt::Error)?;
        serializer_map
            .serialize_entry("severity", &meta.level().as_serde())
            .map_err(|_| std::fmt::Error)?;

        if let Some(leaf_span) = ctx.lookup_current() {
            for span in leaf_span.scope().from_root() {
                let ext = span.extensions();
                if let Some(data) = ext.get::<FormattedFields<N>>()
                    && let Ok(serde_json::Value::Object(fields)) =
                        serde_json::from_str::<serde_json::Value>(data)
                {
                    for (field_name, value) in fields {
                        serialize_json_aware_value_entry(&mut serializer_map, &field_name, &value)
                            .map_err(|_| std::fmt::Error)?;
                    }
                }
            }
        }

        let mut visitor = JsonFieldVisitor::new(serializer_map);
        event.record(&mut visitor);

        visitor
            .take_serializer()
            .map_err(|_| std::fmt::Error)?
            .end()
            .map_err(|_| std::fmt::Error)?;

        let s_str = std::str::from_utf8(&s).map_err(|_| std::fmt::Error)?;
        writer.write_str(s_str)?;
        writeln!(writer)
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::io;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    pub(crate) struct JsonLogWriter {
        lines: Arc<Mutex<Vec<u8>>>,
    }

    impl JsonLogWriter {
        pub(crate) fn output(&self) -> Vec<serde_json::Value> {
            let bytes = self.lines.lock().expect("writer lock").clone();
            String::from_utf8(bytes)
                .expect("utf8 logs")
                .lines()
                .map(|line| serde_json::from_str(line).expect("json log line"))
                .collect()
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for JsonLogWriter {
        type Writer = JsonLogWriterGuard;

        fn make_writer(&'a self) -> Self::Writer {
            JsonLogWriterGuard {
                lines: Arc::clone(&self.lines),
            }
        }
    }

    pub(crate) struct JsonLogWriterGuard {
        lines: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for JsonLogWriterGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.lines
                .lock()
                .expect("writer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use test_support::JsonLogWriter;
    use tracing::{info, info_span};
    use tracing_subscriber::layer::SubscriberExt;

    fn capture_logs(run: impl FnOnce()) -> Vec<serde_json::Value> {
        let writer = JsonLogWriter::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(writer.clone())
                .event_format(HubuumLoggingFormat),
        );

        tracing::subscriber::with_default(subscriber, run);
        writer.output()
    }

    #[test]
    fn logging_format_emits_json_fields() {
        let logs = capture_logs(|| {
            info!(
                message = "Test with fields",
                user_id = 123,
                action = "test_action"
            );
        });

        let event = logs.first().expect("log event");
        assert_eq!(event["severity"], "INFO");
        assert_eq!(event["message"], "Test with fields");
        assert_eq!(event["user_id"], 123);
        assert_eq!(event["action"], "test_action");
        assert!(event["time"].as_str().expect("timestamp").ends_with('Z'));
    }

    #[test]
    fn logging_format_inherits_span_fields() {
        let logs = capture_logs(|| {
            let span = info_span!(
                "request",
                request_id = "request-1",
                correlation_id = "correlation-1",
                principal = 42
            );
            let _guard = span.enter();
            info!(message = "inside request");
        });

        let event = logs.first().expect("log event");
        assert_eq!(event["request_id"], "request-1");
        assert_eq!(event["correlation_id"], "correlation-1");
        assert_eq!(event["principal"], 42);
        assert_eq!(event["message"], "inside request");
    }

    #[test]
    fn logging_format_serializes_json_suffixed_fields_as_json_values() {
        let logs = capture_logs(|| {
            info!(
                message = "structured field",
                permissions_json = "[\"ReadCollection\",\"UpdateCollection\"]",
            );
        });

        let event = logs.first().expect("log event");
        assert_eq!(
            event["permissions"],
            json!(["ReadCollection", "UpdateCollection"])
        );
        assert!(event.get("permissions_json").is_none());
    }

    #[test]
    fn test_logging_format_handles_special_characters() {
        let logs = capture_logs(|| {
            info!(
                message = "Test with \"quotes\" and \n newlines",
                path = "/some/path/with\\backslashes"
            );
        });

        let event = logs.first().expect("log event");
        assert_eq!(event["message"], "Test with \"quotes\" and \n newlines");
        assert_eq!(event["path"], "/some/path/with\\backslashes");
    }

    #[test]
    fn authorization_helpers_log_grant_and_denial_levels() {
        let logs = capture_logs(|| {
            log_authorization_grant(12, &[Permissions::ReadCollection], Some(1), "permissions");
            log_authorization_denial(12, &[Permissions::UpdateCollection], Some(1), "permissions");
        });

        let grant = logs
            .iter()
            .find(|event| event["decision"] == "grant")
            .expect("grant event");
        assert_eq!(grant["severity"], "DEBUG");
        assert_eq!(grant["event_type"], "authorization");
        assert_eq!(grant["principal_id"], 12);
        assert_eq!(grant["permissions"], json!(["ReadCollection"]));

        let denial = logs
            .iter()
            .find(|event| event["decision"] == "deny")
            .expect("denial event");
        assert_eq!(denial["severity"], "WARN");
        assert_eq!(denial["event_type"], "authorization");
        assert_eq!(denial["principal_id"], 12);
        assert_eq!(denial["permissions"], json!(["UpdateCollection"]));
    }

    #[test]
    fn operation_mutation_helper_uses_catalog_labels_without_payloads() {
        let request_id = uuid::Uuid::new_v4();
        let logs = capture_logs(|| {
            log_operation_mutation(
                EntityType::Namespace,
                Action::Created,
                Some(9),
                Some(12),
                Some(request_id),
                Some("operation-correlation"),
            );
        });

        let event = logs.first().expect("operation event");
        assert_eq!(event["severity"], "INFO");
        assert_eq!(event["message"], "operation mutation recorded");
        assert_eq!(event["operation"], "mutation_recorded");
        assert_eq!(event["mutation_phase"], "recorded");
        assert_eq!(event["entity_type"], "namespace");
        assert_eq!(event["action"], "created");
        assert_eq!(event["entity_id"], 9);
        assert_eq!(event["actor_principal_id"], 12);
        assert_eq!(event["request_id"], request_id.to_string());
        assert_eq!(event["correlation_id"], "operation-correlation");
        assert!(event.get("before").is_none());
        assert!(event.get("after").is_none());
    }
}
