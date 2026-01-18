use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::FormattedFields;
use tracing_subscriber::registry::LookupSpan;

use serde::ser::{SerializeMap, Serializer};
use tracing_serde::AsSerde;

pub struct HubuumLoggingFormat;

impl HubuumLoggingFormat {}

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
                if let Some(data) = ext.get::<FormattedFields<N>>() {
                    if let Ok(serde_json::Value::Object(fields)) =
                        serde_json::from_str::<serde_json::Value>(data)
                    {
                        for field in fields {
                            serializer_map
                                .serialize_entry(&field.0, &field.1)
                                .map_err(|_| std::fmt::Error)?;
                        }
                    }
                }
            }
        }

        let mut visitor = tracing_serde::SerdeMapVisitor::new(serializer_map);
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
mod tests {
    use super::*;
    use tracing::info;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn test_logging_format_handles_simple_event() {
        // Test that basic logging works without panicking
        // We use a writer that discards output to avoid polluting test output
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::sink) // Discard output
                .with_span_events(FmtSpan::CLOSE)
                .event_format(HubuumLoggingFormat),
        );

        let _guard = subscriber.set_default();

        // This should not panic
        info!("Test message");
    }

    #[test]
    fn test_logging_format_handles_event_with_fields() {
        // Test that logging with structured fields works
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::sink) // Discard output
                .with_span_events(FmtSpan::CLOSE)
                .event_format(HubuumLoggingFormat),
        );

        let _guard = subscriber.set_default();

        // This should not panic even with multiple fields
        info!(
            message = "Test with fields",
            user_id = 123,
            action = "test_action"
        );
    }

    #[test]
    fn test_logging_format_handles_special_characters() {
        // Test that special characters don't cause panics
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(std::io::sink) // Discard output
                .with_span_events(FmtSpan::CLOSE)
                .event_format(HubuumLoggingFormat),
        );

        let _guard = subscriber.set_default();

        // This should handle various special characters without panicking
        info!(
            message = "Test with \"quotes\" and \n newlines",
            path = "/some/path/with\\backslashes"
        );
    }
}
