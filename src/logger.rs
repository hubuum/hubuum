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
        let mut serializer_map = serializer.serialize_map(None).unwrap();

        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        serializer_map.serialize_entry("time", &timestamp).unwrap();
        serializer_map
            .serialize_entry("severity", &meta.level().as_serde())
            .unwrap();

        if let Some(leaf_span) = ctx.lookup_current() {
            for span in leaf_span.scope().from_root() {
                let ext = span.extensions();
                let data = ext
                    .get::<FormattedFields<N>>()
                    .expect("Unable to find FormattedFields in extensions; this is a bug");

                let serde_json::Value::Object(fields) =
                    serde_json::from_str::<serde_json::Value>(data).unwrap()
                else {
                    panic!()
                };
                for field in fields {
                    serializer_map.serialize_entry(&field.0, &field.1).unwrap();
                }
            }
        }

        let mut visitor = tracing_serde::SerdeMapVisitor::new(serializer_map);
        event.record(&mut visitor);

        visitor.take_serializer().unwrap().end().unwrap();

        writer.write_str(std::str::from_utf8(&s).unwrap()).unwrap();
        writeln!(writer)
    }
}
