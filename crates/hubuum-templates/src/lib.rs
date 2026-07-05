//! Bounded MiniJinja helpers shared by Hubuum templating surfaces.
//!
//! This crate owns the reusable template execution boundary: fuel and recursion
//! limits, simple validation/rendering helpers, and the curated filters/functions
//! that should behave consistently across reports, remote targets, webhook
//! sinks, and future template consumers.
//!
//! It intentionally does not own Hubuum-specific concerns such as report
//! database models, collection template loading, permission checks, report output
//! persistence, API error types, or global app configuration. Callers pass
//! `TemplateLimits` explicitly, usually through `prepare_template`, and may
//! provide an optional missing-value recorder callback when they need
//! app-specific warning collection.

use std::fmt;

use minijinja::value::Value;
use minijinja::{Environment, Error as MiniJinjaError, ErrorKind as MiniJinjaErrorKind, State};

pub type MissingValueRecorder = fn(MissingValue);

pub fn prepare_template(source: &str) -> PreparedTemplate<'_> {
    PreparedTemplate::new(source)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingValue {
    template_name: String,
    path: Option<String>,
}

impl MissingValue {
    pub fn new(template_name: impl Into<String>, path: Option<String>) -> Self {
        Self {
            template_name: template_name.into(),
            path,
        }
    }

    pub fn template_name(&self) -> &str {
        &self.template_name
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    pub fn into_path(self) -> Option<String> {
        self.path
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TemplateLimits {
    recursion_limit: usize,
    fuel: u64,
}

impl TemplateLimits {
    pub fn new(recursion_limit: usize, fuel: u64) -> Self {
        Self {
            recursion_limit,
            fuel,
        }
    }

    pub fn recursion_limit(self) -> usize {
        self.recursion_limit
    }

    pub fn fuel(self) -> u64 {
        self.fuel
    }
}

#[derive(Debug)]
pub struct TemplateError {
    message: String,
    source: Option<MiniJinjaError>,
}

impl TemplateError {
    fn validation(source: MiniJinjaError) -> Self {
        Self {
            message: source.to_string(),
            source: Some(source),
        }
    }

    fn render(source: MiniJinjaError) -> Self {
        Self {
            message: source.to_string(),
            source: Some(source),
        }
    }

    fn missing_limit(name: &str) -> Self {
        Self {
            message: format!("template {name} limit is not configured"),
            source: None,
        }
    }
}

impl fmt::Display for TemplateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TemplateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

pub struct PreparedTemplate<'source> {
    source: &'source str,
    recursion_limit: Option<usize>,
    fuel: Option<u64>,
}

impl<'source> PreparedTemplate<'source> {
    fn new(source: &'source str) -> Self {
        Self {
            source,
            recursion_limit: None,
            fuel: None,
        }
    }

    pub fn limits(mut self, limits: TemplateLimits) -> Self {
        self.recursion_limit = Some(limits.recursion_limit());
        self.fuel = Some(limits.fuel());
        self
    }

    pub fn limit_recursion(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = Some(recursion_limit);
        self
    }

    pub fn limit_fuel(mut self, fuel: u64) -> Self {
        self.fuel = Some(fuel);
        self
    }

    pub fn context<'context>(
        self,
        context: &'context serde_json::Value,
    ) -> PreparedTemplateRender<'source, 'context> {
        PreparedTemplateRender {
            template: self,
            context,
        }
    }

    pub fn validate(self) -> Result<(), TemplateError> {
        let limits = self.limits_or_error()?;
        bounded_environment(limits)
            .template_from_str(self.source)
            .map(|_| ())
            .map_err(TemplateError::validation)
    }

    fn limits_or_error(&self) -> Result<TemplateLimits, TemplateError> {
        let recursion_limit = self
            .recursion_limit
            .ok_or_else(|| TemplateError::missing_limit("recursion"))?;
        let fuel = self
            .fuel
            .ok_or_else(|| TemplateError::missing_limit("fuel"))?;
        Ok(TemplateLimits::new(recursion_limit, fuel))
    }
}

pub struct PreparedTemplateRender<'source, 'context> {
    template: PreparedTemplate<'source>,
    context: &'context serde_json::Value,
}

impl PreparedTemplateRender<'_, '_> {
    pub fn render(self) -> Result<String, TemplateError> {
        let limits = self.template.limits_or_error()?;
        bounded_environment(limits)
            .template_from_str(self.template.source)
            .and_then(|compiled| compiled.render(self.context))
            .map_err(TemplateError::render)
    }
}

fn bounded_environment(limits: TemplateLimits) -> Environment<'static> {
    let mut env = Environment::new();
    env.set_recursion_limit(limits.recursion_limit);
    env.set_fuel(Some(limits.fuel));
    register_curated_helpers(&mut env, None);
    env
}

pub fn register_curated_helpers(
    env: &mut Environment<'static>,
    missing_value_recorder: Option<MissingValueRecorder>,
) {
    env.add_filter("csv_cell", csv_cell_filter);
    env.add_filter("tojson", tojson_filter);
    env.add_filter(
        "default_if_empty",
        move |state: &State<'_, '_>, value: Value, fallback: Value| {
            default_if_empty_filter(state, value, fallback, missing_value_recorder)
        },
    );
    env.add_filter("format_datetime", format_datetime_filter);
    env.add_filter("join_nonempty", join_nonempty_filter);
    env.add_function(
        "coalesce",
        move |state: &State<'_, '_>, values: minijinja::value::Rest<Value>| {
            coalesce_function(state, values, missing_value_recorder)
        },
    );
}

pub fn csv_cell_filter(value: Value) -> String {
    let rendered = if value.is_none() || value.is_undefined() {
        String::new()
    } else {
        value.to_string()
    };
    let guarded = if csv_cell_needs_formula_guard(&rendered) {
        format!("'{rendered}")
    } else {
        rendered
    };
    if guarded.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", guarded.replace('"', "\"\""))
    } else {
        guarded
    }
}

fn csv_cell_needs_formula_guard(rendered: &str) -> bool {
    match rendered.chars().next() {
        Some('\t' | '\r' | '\n') => true,
        Some(_) => rendered
            .trim_start_matches([' ', '\t'])
            .starts_with(['=', '+', '-', '@']),
        None => false,
    }
}

fn tojson_filter(value: Value) -> Result<String, MiniJinjaError> {
    serde_json::to_string(&value).map_err(|error| {
        MiniJinjaError::new(
            MiniJinjaErrorKind::InvalidOperation,
            "unable to serialize template value as JSON",
        )
        .with_source(error)
    })
}

fn default_if_empty_filter(
    state: &State<'_, '_>,
    value: Value,
    fallback: Value,
    missing_value_recorder: Option<MissingValueRecorder>,
) -> Value {
    if value.is_undefined()
        && let Some(record) = missing_value_recorder
    {
        record(MissingValue::new(state.name(), None));
    }
    if value_is_missing_or_empty(&value) {
        fallback
    } else {
        value
    }
}

fn format_datetime_filter(value: Value, format: Option<String>) -> Result<String, MiniJinjaError> {
    let format = format.unwrap_or_else(|| "iso".to_string());
    let raw = match value.as_str() {
        Some(raw) if !raw.trim().is_empty() => raw.trim(),
        _ if value.is_none() || value.is_undefined() => return Ok(String::new()),
        _ => {
            return Err(MiniJinjaError::new(
                MiniJinjaErrorKind::InvalidOperation,
                "format_datetime expects an RFC3339 or Hubuum timestamp string",
            ));
        }
    };

    let parsed = parse_template_datetime(raw).ok_or_else(|| {
        MiniJinjaError::new(
            MiniJinjaErrorKind::InvalidOperation,
            format!("Unable to parse '{raw}' as a supported datetime"),
        )
    })?;

    Ok(match format.as_str() {
        "iso" => parsed.to_rfc3339(),
        "date" => parsed.format("%Y-%m-%d").to_string(),
        "datetime" => parsed.format("%Y-%m-%d %H:%M:%S").to_string(),
        "time" => parsed.format("%H:%M:%S").to_string(),
        other => {
            return Err(MiniJinjaError::new(
                MiniJinjaErrorKind::InvalidOperation,
                format!("Unsupported datetime format '{other}'"),
            ));
        }
    })
}

fn join_nonempty_filter(value: Value, sep: Option<String>) -> Result<String, MiniJinjaError> {
    let joiner = sep.unwrap_or_else(|| ", ".to_string());
    let items = value.try_iter().map_err(|_| {
        MiniJinjaError::new(
            MiniJinjaErrorKind::InvalidOperation,
            "join_nonempty expects a list-like value",
        )
    })?;

    Ok(items
        .filter(|item| !value_is_missing_or_empty(item))
        .map(|item| item.to_string())
        .filter(|item| !item.is_empty())
        .collect::<Vec<_>>()
        .join(&joiner))
}

fn coalesce_function(
    state: &State<'_, '_>,
    values: minijinja::value::Rest<Value>,
    missing_value_recorder: Option<MissingValueRecorder>,
) -> Value {
    if values.iter().any(Value::is_undefined)
        && let Some(record) = missing_value_recorder
    {
        record(MissingValue::new(state.name(), None));
    }
    values
        .iter()
        .find(|value| !value_is_missing_or_empty(value))
        .cloned()
        .unwrap_or(Value::UNDEFINED)
}

fn value_is_missing_or_empty(value: &Value) -> bool {
    if value.is_undefined() || value.is_none() {
        return true;
    }

    if let Some(text) = value.as_str() {
        return text.is_empty();
    }

    matches!(value.len(), Some(0))
}

fn parse_template_datetime(raw: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S")
                .ok()
                .and_then(|value| {
                    chrono::FixedOffset::east_opt(0)
                        .map(|offset| value.and_utc().with_timezone(&offset))
                })
        })
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(raw, "%Y-%m-%d")
                .ok()
                .and_then(|value| value.and_hms_opt(0, 0, 0))
                .and_then(|value| {
                    chrono::FixedOffset::east_opt(0)
                        .map(|offset| value.and_utc().with_timezone(&offset))
                })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_cell_neutralizes_formula_injection() {
        assert_eq!(
            csv_cell_filter(Value::from("=HYPERLINK(\"http://evil\")")),
            "\"'=HYPERLINK(\"\"http://evil\"\")\""
        );
        assert_eq!(csv_cell_filter(Value::from("@SUM(A1:A9)")), "'@SUM(A1:A9)");
        assert_eq!(csv_cell_filter(Value::from("+1+1")), "'+1+1");
        assert_eq!(csv_cell_filter(Value::from("-2+3")), "'-2+3");
        assert_eq!(csv_cell_filter(Value::from("  =1+1")), "'  =1+1");
        assert_eq!(csv_cell_filter(Value::from("\t=1+1")), "'\t=1+1");
        assert_eq!(csv_cell_filter(Value::from("\rdata")), "\"'\rdata\"");
        assert_eq!(csv_cell_filter(Value::from("plain value")), "plain value");
        assert_eq!(csv_cell_filter(Value::from("a,b")), "\"a,b\"");
        assert_eq!(csv_cell_filter(Value::from("3.14")), "3.14");
    }

    #[test]
    fn render_template_supports_curated_filters() {
        let context = serde_json::json!({ "object": { "data": { "host": "h1" } } });
        let rendered = prepare_template("{{ object.data | tojson }}")
            .limit_recursion(64)
            .limit_fuel(50_000)
            .context(&context)
            .render()
            .unwrap();
        assert_eq!(rendered, "{\"host\":\"h1\"}");
    }

    #[test]
    fn render_template_is_fuel_bounded() {
        let context = serde_json::json!({});
        let error = prepare_template("{% for _ in range(1000000000) %}x{% endfor %}")
            .limit_recursion(64)
            .limit_fuel(100)
            .context(&context)
            .render()
            .unwrap_err();

        assert!(
            error.to_string().contains("fuel")
                || error.to_string().contains("operation")
                || error.to_string().contains("limit")
        );
    }
}
