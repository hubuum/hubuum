use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use minijinja::value::Value;
use minijinja::{
    AutoEscape, Environment, Error as MiniJinjaError, ErrorKind as MiniJinjaErrorKind, State,
    UndefinedBehavior, escape_formatter,
};

use crate::config::get_config;
use crate::errors::ApiError;
use crate::models::{ReportContentType, ReportMissingDataPolicy, ReportTemplate, ReportWarning};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TemplateEnvCacheKey {
    namespace_id: i32,
    namespace_signature: String,
    template_name: String,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
}

struct CachedTemplateEnvironment {
    env: Environment<'static>,
    template_name: String,
}

static TEMPLATE_ENV_CACHE: OnceLock<
    RwLock<HashMap<TemplateEnvCacheKey, Arc<CachedTemplateEnvironment>>>,
> = OnceLock::new();

#[derive(Debug, Default)]
struct TemplateWarningCapture {
    missing_value_keys: HashSet<(String, Option<String>)>,
    warnings: Vec<ReportWarning>,
}

thread_local! {
    static TEMPLATE_WARNING_CAPTURE: RefCell<Option<TemplateWarningCapture>> = const { RefCell::new(None) };
}

pub fn validate_template(
    template_name: &str,
    template_source: &str,
    namespace_id: i32,
    namespace_templates: &[ReportTemplate],
    content_type: ReportContentType,
) -> Result<(), ApiError> {
    let env = build_environment(
        template_name,
        template_source,
        namespace_id,
        namespace_templates,
        content_type,
        ReportMissingDataPolicy::Omit,
    )?;

    env.env
        .get_template(&env.template_name)
        .map_err(|error| template_error("Template validation failed", error))?
        .render(validation_context(content_type))
        .map_err(|error| template_error("Template validation failed", error))?;

    Ok(())
}

pub fn render_template(
    template: &ReportTemplate,
    namespace_templates: &[ReportTemplate],
    context: &serde_json::Value,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
) -> Result<(String, Vec<ReportWarning>), ApiError> {
    let cache_key = TemplateEnvCacheKey {
        namespace_id: template.namespace_id,
        namespace_signature: namespace_signature(template.namespace_id, namespace_templates),
        template_name: template.name.clone(),
        content_type,
        missing_data_policy,
    };

    let cached = {
        let cache = template_env_cache().read().map_err(|_| {
            ApiError::InternalServerError("Template cache lock poisoned".to_string())
        })?;
        cache.get(&cache_key).cloned()
    };

    let env = match cached {
        Some(env) => env,
        None => {
            let built = Arc::new(build_environment(
                &template.name,
                &template.template,
                template.namespace_id,
                namespace_templates,
                content_type,
                missing_data_policy,
            )?);
            let mut cache = template_env_cache().write().map_err(|_| {
                ApiError::InternalServerError("Template cache lock poisoned".to_string())
            })?;
            cache.insert(cache_key, built.clone());
            built
        }
    };

    begin_template_warning_capture();
    let rendered = env
        .env
        .get_template(&env.template_name)
        .map_err(|error| template_error("Template lookup failed", error))?
        .render(context)
        .map_err(|error| template_error("Template render failed", error));
    let warnings = finish_template_warning_capture();

    Ok((rendered?, warnings))
}

fn template_env_cache()
-> &'static RwLock<HashMap<TemplateEnvCacheKey, Arc<CachedTemplateEnvironment>>> {
    TEMPLATE_ENV_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn namespace_signature(namespace_id: i32, namespace_templates: &[ReportTemplate]) -> String {
    let mut templates = namespace_templates
        .iter()
        .filter(|template| template.namespace_id == namespace_id)
        .map(|template| {
            format!(
                "{}:{}:{}",
                template.id,
                template.updated_at.and_utc().timestamp_micros(),
                template.name
            )
        })
        .collect::<Vec<_>>();
    templates.sort();
    templates.join("|")
}

fn build_environment(
    template_name: &str,
    template_source: &str,
    namespace_id: i32,
    namespace_templates: &[ReportTemplate],
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
) -> Result<CachedTemplateEnvironment, ApiError> {
    let mut env = Environment::new();
    let template_map = Arc::new(build_namespace_template_map(
        namespace_id,
        template_name,
        template_source,
        namespace_templates,
    ));

    env.set_keep_trailing_newline(true);
    env.set_undefined_behavior(undefined_behavior(missing_data_policy));
    let recursion_limit = get_config()
        .map(|config| config.report_template_recursion_limit)
        .unwrap_or(64);
    env.set_recursion_limit(recursion_limit);
    let fuel = get_config()
        .map(|config| config.report_template_fuel)
        .unwrap_or(50_000);
    env.set_fuel(Some(fuel));
    env.set_auto_escape_callback(move |_| match content_type {
        ReportContentType::TextHtml => AutoEscape::Html,
        _ => AutoEscape::None,
    });
    env.set_formatter(move |out, state, value| match missing_data_policy {
        ReportMissingDataPolicy::Strict => escape_formatter(out, state, value),
        ReportMissingDataPolicy::Omit => format_nullable_value(out, state, value, None),
        ReportMissingDataPolicy::Null => format_nullable_value(out, state, value, Some("null")),
    });
    env.set_loader(move |name| {
        if name.contains('/') || name.contains("::") {
            return Ok(None);
        }
        Ok(template_map.get(name).cloned())
    });
    register_curated_helpers(&mut env);
    env.add_template_owned(template_name.to_string(), template_source.to_string())
        .map_err(|error| template_error("Template load failed", error))?;

    Ok(CachedTemplateEnvironment {
        env,
        template_name: template_name.to_string(),
    })
}

fn build_namespace_template_map(
    namespace_id: i32,
    template_name: &str,
    template_source: &str,
    namespace_templates: &[ReportTemplate],
) -> HashMap<String, String> {
    let mut templates = namespace_templates
        .iter()
        .filter(|template| template.namespace_id == namespace_id)
        .map(|template| (template.name.clone(), template.template.clone()))
        .collect::<HashMap<_, _>>();
    templates.insert(template_name.to_string(), template_source.to_string());
    templates
}

fn validation_context(content_type: ReportContentType) -> serde_json::Value {
    serde_json::json!({
        "items": [],
        "meta": {
            "count": 0,
            "truncated": false,
            "scope": {
                "kind": "objects_in_class",
                "class_id": 0,
                "object_id": 0,
            },
            "content_type": content_type.as_mime(),
        },
        "warnings": [],
        "request": {
            "scope": {
                "kind": "objects_in_class",
                "class_id": 0,
                "object_id": 0,
            },
            "query": "",
        },
        "source": {
            "id": 0,
            "name": "",
            "description": "",
            "namespace_id": 0,
            "hubuum_class_id": 0,
            "data": {},
            "path": [],
            "path_objects": [],
            "related": {},
            "reachable": {},
            "paths": {},
        },
    })
}

fn undefined_behavior(missing_data_policy: ReportMissingDataPolicy) -> UndefinedBehavior {
    match missing_data_policy {
        ReportMissingDataPolicy::Strict => UndefinedBehavior::Strict,
        ReportMissingDataPolicy::Null | ReportMissingDataPolicy::Omit => {
            UndefinedBehavior::Chainable
        }
    }
}

fn format_nullable_value(
    out: &mut minijinja::Output,
    state: &minijinja::State,
    value: &Value,
    replacement: Option<&str>,
) -> Result<(), MiniJinjaError> {
    if value.is_undefined() {
        record_missing_value_warning(state, None);
        if let Some(replacement) = replacement {
            out.write_str(replacement)?;
        }
        return Ok(());
    }

    if value.is_none() {
        if let Some(replacement) = replacement {
            out.write_str(replacement)?;
        }
        return Ok(());
    }

    escape_formatter(out, state, value)
}

fn register_curated_helpers(env: &mut Environment<'static>) {
    env.add_filter("csv_cell", csv_cell_filter);
    env.add_filter("tojson", tojson_filter);
    env.add_filter("default_if_empty", default_if_empty_filter);
    env.add_filter("format_datetime", format_datetime_filter);
    env.add_filter("join_nonempty", join_nonempty_filter);
    env.add_function("coalesce", coalesce_function);
}

fn csv_cell_filter(value: Value) -> String {
    let rendered = if value.is_none() || value.is_undefined() {
        String::new()
    } else {
        value.to_string()
    };
    if rendered.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", rendered.replace('"', "\"\""))
    } else {
        rendered
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

fn default_if_empty_filter(state: &State<'_, '_>, value: Value, fallback: Value) -> Value {
    if value.is_undefined() {
        record_missing_value_warning(state, None);
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

fn coalesce_function(state: &State<'_, '_>, values: minijinja::value::Rest<Value>) -> Value {
    if values.iter().any(Value::is_undefined) {
        record_missing_value_warning(state, None);
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

fn template_error(prefix: &str, error: MiniJinjaError) -> ApiError {
    ApiError::BadRequest(format!("{prefix}: {error}"))
}

fn begin_template_warning_capture() {
    TEMPLATE_WARNING_CAPTURE.with(|capture| {
        *capture.borrow_mut() = Some(TemplateWarningCapture::default());
    });
}

fn finish_template_warning_capture() -> Vec<ReportWarning> {
    TEMPLATE_WARNING_CAPTURE.with(|capture| {
        capture
            .borrow_mut()
            .take()
            .map(|capture| capture.warnings)
            .unwrap_or_default()
    })
}

fn record_missing_value_warning(state: &State<'_, '_>, path: Option<String>) {
    TEMPLATE_WARNING_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(capture) = capture.as_mut() else {
            return;
        };
        let template_name = state.name().to_string();
        let key = (template_name.clone(), path.clone());
        if capture.missing_value_keys.contains(&key) {
            return;
        }
        capture.missing_value_keys.insert(key);
        capture.warnings.push(ReportWarning {
            code: "template_missing_value".to_string(),
            message: format!("Template '{template_name}' rendered one or more missing values"),
            path,
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn template(
        id: i32,
        namespace_id: i32,
        name: &str,
        content_type: ReportContentType,
        source: &str,
    ) -> ReportTemplate {
        let now = chrono::Utc::now().naive_utc();
        ReportTemplate {
            id,
            namespace_id,
            name: name.to_string(),
            description: "template".to_string(),
            content_type,
            template: source.to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn renders_jinja_loops_and_nested_values() {
        let template = template(
            1,
            10,
            "servers.txt",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        );
        let context = serde_json::json!({
            "items": [
                {"name": "srv-01", "data": {"owner": "alice"}},
                {"name": "srv-02", "data": {"owner": "bob"}}
            ]
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=alice\nsrv-02=bob\n");
        assert!(warnings.is_empty());
    }

    #[test]
    fn supports_same_namespace_includes() {
        let layout = template(
            2,
            10,
            "layout.html",
            ReportContentType::TextHtml,
            "<ul>{% block body %}{% endblock %}</ul>",
        );
        let child = template(
            3,
            10,
            "child.html",
            ReportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}{% for item in items %}<li>{{ item.name }}</li>{% endfor %}{% endblock %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, _) = render_template(
            &child,
            &[layout],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "<ul><li>srv-01</li></ul>");
    }

    #[test]
    fn omits_missing_values_when_requested() {
        let template = template(
            4,
            10,
            "missing.txt",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Omit,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=\n");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "template_missing_value");
        assert!(warnings[0].message.contains("missing.txt"));
    }

    #[test]
    fn renders_null_for_missing_values_and_reports_warning() {
        let template = template(
            5,
            10,
            "missing-null.txt",
            ReportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Null,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=null\n");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "template_missing_value");
        assert!(warnings[0].message.contains("missing-null.txt"));
    }

    #[test]
    fn autoescapes_html_but_not_plain_text() {
        let html_template = template(
            6,
            10,
            "escape.html",
            ReportContentType::TextHtml,
            "{{ items[0].name }}",
        );
        let text_template = template(
            7,
            10,
            "escape.txt",
            ReportContentType::TextPlain,
            "{{ items[0].name }}",
        );
        let context = serde_json::json!({
            "items": [{"name": "<b>srv&01</b>"}]
        });

        let (html, html_warnings) = render_template(
            &html_template,
            &[],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();
        let (text, text_warnings) = render_template(
            &text_template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(html, "&lt;b&gt;srv&amp;01&lt;&#x2f;b&gt;");
        assert_eq!(text, "<b>srv&01</b>");
        assert!(html_warnings.is_empty());
        assert!(text_warnings.is_empty());
    }

    #[test]
    fn supports_same_namespace_imports() {
        let macros = template(
            8,
            10,
            "macros.txt",
            ReportContentType::TextPlain,
            "{% macro owner(item) %}{{ item.data.owner }}{% endmacro %}",
        );
        let child = template(
            9,
            10,
            "child.txt",
            ReportContentType::TextPlain,
            "{% import \"macros.txt\" as macros %}{{ items[0].name }}={{ macros.owner(items[0]) }}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01", "data": {"owner": "alice"}}]
        });

        let (rendered, warnings) = render_template(
            &child,
            &[macros],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=alice");
        assert!(warnings.is_empty());
    }

    #[test]
    fn missing_value_warning_identifies_composed_template_name() {
        let partial = template(
            15,
            10,
            "partial.owner.txt",
            ReportContentType::TextPlain,
            "{{ items[0].data.owner }}",
        );
        let report = template(
            16,
            10,
            "report.hosts.txt",
            ReportContentType::TextPlain,
            "{% include \"partial.owner.txt\" %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &report,
            &[partial],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Omit,
        )
        .unwrap();

        assert_eq!(rendered, "");
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("partial.owner.txt"));
    }

    #[test]
    fn supports_curated_template_helpers() {
        let template = template(
            14,
            10,
            "helpers.txt",
            ReportContentType::TextPlain,
            "{{ items|join_nonempty(\" | \") }}\n{{ missing|default_if_empty(\"fallback\") }}\n{{ coalesce(missing, none, \"owner\") }}\n{{ when|format_datetime(\"date\") }}\n{{ csv|csv_cell }}\n{{ payload|tojson }}",
        );
        let context = serde_json::json!({
            "items": ["alice", "", null, "bob"],
            "when": "2026-03-30T10:15:23Z",
            "csv": "alice,bob",
            "payload": {"host": "srv-01", "enabled": true}
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Omit,
        )
        .unwrap();

        assert_eq!(
            rendered,
            "alice | bob\nfallback\nowner\n2026-03-30\n\"alice,bob\"\n{\"enabled\":true,\"host\":\"srv-01\"}"
        );
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("helpers.txt"));
    }

    #[test]
    fn rejects_cross_namespace_template_loading() {
        let layout = template(
            10,
            20,
            "layout.html",
            ReportContentType::TextHtml,
            "<ul>{% block body %}{% endblock %}</ul>",
        );
        let child = template(
            11,
            10,
            "child.html",
            ReportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let error = render_template(
            &child,
            &[layout],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap_err();

        assert!(error.to_string().contains("template not found"));
    }

    #[test]
    fn invalidates_cached_environment_when_templates_change() {
        let child = template(
            12,
            10,
            "child.html",
            ReportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let mut layout_v1 = template(
            13,
            10,
            "layout.html",
            ReportContentType::TextHtml,
            "<ul class=\"v1\">{% block body %}{% endblock %}</ul>",
        );
        let mut layout_v2 = layout_v1.clone();
        layout_v2.template = "<ol class=\"v2\">{% block body %}{% endblock %}</ol>".to_string();
        layout_v2.updated_at += chrono::Duration::seconds(1);
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (first, _) = render_template(
            &child,
            &[layout_v1.clone()],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();
        layout_v1.updated_at += chrono::Duration::seconds(2);
        let (second, _) = render_template(
            &child,
            &[layout_v2],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(first, "<ul class=\"v1\"><li>srv-01</li></ul>");
        assert_eq!(second, "<ol class=\"v2\"><li>srv-01</li></ol>");
    }
}
