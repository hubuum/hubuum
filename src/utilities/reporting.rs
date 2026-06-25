use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};

use hubuum_templates::{
    MissingValue, MissingValueRecorder, TemplateLimits, register_curated_helpers,
};
use lru::LruCache;
use minijinja::value::Value;
use minijinja::{
    AutoEscape, Environment, Error as MiniJinjaError, UndefinedBehavior, escape_formatter,
};

use crate::config::get_config;
use crate::errors::ApiError;
use crate::models::{ReportContentType, ReportMissingDataPolicy, ReportTemplate, ReportWarning};

const TEMPLATE_ENV_CACHE_MAX_ENTRIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TemplateEnvCacheKey {
    namespace_id: i32,
    namespace_signature: NamespaceTemplateSignature,
    template_name: String,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NamespaceTemplateSignature {
    template_count: usize,
    max_updated_at_micros: i64,
    template_hash: u64,
}

struct CachedTemplateEnvironment {
    env: Environment<'static>,
    template_name: String,
}

static TEMPLATE_ENV_CACHE: OnceLock<
    RwLock<LruCache<TemplateEnvCacheKey, Arc<CachedTemplateEnvironment>>>,
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
    let (recursion_limit, fuel) = template_limits_from_config();
    validate_template_with_limits(
        template_name,
        template_source,
        namespace_id,
        namespace_templates,
        content_type,
        recursion_limit,
        fuel,
    )
}

pub fn validate_template_with_limits(
    template_name: &str,
    template_source: &str,
    namespace_id: i32,
    namespace_templates: &[ReportTemplate],
    content_type: ReportContentType,
    recursion_limit: usize,
    fuel: u64,
) -> Result<(), ApiError> {
    let env = build_environment(
        template_name,
        template_source,
        namespace_id,
        namespace_templates,
        content_type,
        ReportMissingDataPolicy::Omit,
        TemplateLimits::new(recursion_limit, fuel),
    )?;

    env.env
        .get_template(&env.template_name)
        .map_err(|error| template_error("Template validation failed", error))?
        .render(validation_context(content_type))
        .map_err(|error| template_error("Template validation failed", error))?;

    Ok(())
}

/// Size-bounded `Write` sink shared by both report output paths. minijinja's
/// `render_captured_to` (text/html/csv) and `serde_json::to_writer` (JSON) both write into this as
/// they produce bytes, so an oversized report aborts at the configured byte budget instead of being
/// fully materialized before the 413. Memory stays bounded by `max_bytes` because writing stops at
/// the cap. The JSON size check ignores the captured bytes; the template path keeps them via
/// `into_string`.
pub struct SizeLimitedWriter {
    max_bytes: usize,
    buffer: Vec<u8>,
    exceeded: bool,
}

impl SizeLimitedWriter {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            buffer: Vec::new(),
            exceeded: false,
        }
    }

    pub fn exceeded(&self) -> bool {
        self.exceeded
    }

    pub fn into_string(self) -> Result<String, ApiError> {
        String::from_utf8(self.buffer).map_err(|error| {
            ApiError::InternalServerError(format!("Rendered report was not valid UTF-8: {error}"))
        })
    }
}

impl Write for SizeLimitedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buffer.len().saturating_add(buf.len()) > self.max_bytes {
            self.exceeded = true;
            return Err(io::Error::other("report output limit exceeded"));
        }

        self.buffer.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn render_template(
    template: &ReportTemplate,
    namespace_templates: &[ReportTemplate],
    context: &serde_json::Value,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    max_output_bytes: usize,
) -> Result<(String, Vec<ReportWarning>), ApiError> {
    let (recursion_limit, fuel) = template_limits_from_config();
    let cache_key = TemplateEnvCacheKey {
        namespace_id: template.namespace_id,
        namespace_signature: namespace_signature(template.namespace_id, namespace_templates),
        template_name: template.name.clone(),
        content_type,
        missing_data_policy,
    };

    let cached = {
        // `LruCache::get` updates recency, so the read path needs a write lock. Cheap at this
        // cache size and keeps eviction honest about what was most recently used.
        let mut cache = template_env_cache()
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                TemplateLimits::new(recursion_limit, fuel),
            )?);
            let mut cache = template_env_cache()
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Drop now-stale environments for this namespace (templates changed) before inserting.
            let stale_keys = cache
                .iter()
                .filter(|(key, _)| {
                    key.namespace_id == cache_key.namespace_id
                        && key.namespace_signature != cache_key.namespace_signature
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for key in stale_keys {
                cache.pop(&key);
            }
            // `put` evicts the least-recently-used entry when the cache is at capacity.
            cache.put(cache_key, built.clone());
            built
        }
    };

    begin_template_warning_capture();
    let mut writer = SizeLimitedWriter::new(max_output_bytes);
    let lookup = env.env.get_template(&env.template_name);
    let render_result = match lookup {
        Ok(template) => template.render_captured_to(context, &mut writer),
        Err(error) => {
            let _ = finish_template_warning_capture();
            return Err(template_error("Template lookup failed", error));
        }
    };
    let warnings = finish_template_warning_capture();

    match render_result {
        Ok(_captured) => Ok((writer.into_string()?, warnings)),
        Err(error) => {
            if writer.exceeded() {
                return Err(ApiError::PayloadTooLarge(format!(
                    "Rendered report exceeded max_output_bytes (> {max_output_bytes})"
                )));
            }
            Err(template_error("Template render failed", error))
        }
    }
}

fn template_env_cache()
-> &'static RwLock<LruCache<TemplateEnvCacheKey, Arc<CachedTemplateEnvironment>>> {
    TEMPLATE_ENV_CACHE.get_or_init(|| {
        let capacity = NonZeroUsize::new(TEMPLATE_ENV_CACHE_MAX_ENTRIES)
            .expect("TEMPLATE_ENV_CACHE_MAX_ENTRIES must be non-zero");
        RwLock::new(LruCache::new(capacity))
    })
}

fn namespace_signature(
    namespace_id: i32,
    namespace_templates: &[ReportTemplate],
) -> NamespaceTemplateSignature {
    let mut templates = namespace_templates
        .iter()
        .filter(|template| template.namespace_id == namespace_id)
        .map(|template| {
            (
                template.id,
                template.updated_at.and_utc().timestamp_micros(),
                template.name.as_str(),
                template.template.as_str(),
            )
        })
        .collect::<Vec<_>>();
    templates.sort_unstable();

    let max_updated_at_micros = templates
        .iter()
        .map(|(_, updated_at_micros, _, _)| *updated_at_micros)
        .max()
        .unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    templates.hash(&mut hasher);

    NamespaceTemplateSignature {
        template_count: templates.len(),
        max_updated_at_micros,
        template_hash: hasher.finish(),
    }
}

fn build_environment(
    template_name: &str,
    template_source: &str,
    namespace_id: i32,
    namespace_templates: &[ReportTemplate],
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    limits: TemplateLimits,
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
    env.set_recursion_limit(limits.recursion_limit());
    env.set_fuel(Some(limits.fuel()));
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
    register_curated_helpers(
        &mut env,
        Some(record_missing_value_warning as MissingValueRecorder),
    );
    env.add_template_owned(template_name.to_string(), template_source.to_string())
        .map_err(|error| template_error("Template load failed", error))?;

    Ok(CachedTemplateEnvironment {
        env,
        template_name: template_name.to_string(),
    })
}

fn template_limits_from_config() -> (usize, u64) {
    get_config()
        .map(|config| {
            (
                config.report_template_recursion_limit,
                config.report_template_fuel,
            )
        })
        .unwrap_or((
            crate::config::DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT,
            crate::config::DEFAULT_REPORT_TEMPLATE_FUEL,
        ))
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
        record_missing_value_warning(MissingValue::new(state.name(), None));
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

fn record_missing_value_warning(missing: MissingValue) {
    TEMPLATE_WARNING_CAPTURE.with(|capture| {
        let mut capture = capture.borrow_mut();
        let Some(capture) = capture.as_mut() else {
            return;
        };
        let template_name = missing.template_name().to_string();
        let path = missing.into_path();
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
    use crate::models::{ReportScopeKind, ReportTemplateKind};

    #[test]
    fn limited_string_writer_accumulates_under_limit() {
        let mut writer = SizeLimitedWriter::new(16);
        writer.write_all(b"hello ").unwrap();
        writer.write_all(b"world").unwrap();
        assert!(!writer.exceeded());
        assert_eq!(writer.into_string().unwrap(), "hello world");
    }

    #[test]
    fn limited_string_writer_aborts_over_limit() {
        let mut writer = SizeLimitedWriter::new(4);
        let err = writer.write_all(b"toolong").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert!(writer.exceeded());
    }

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
            kind: ReportTemplateKind::Report,
            scope_kind: Some(ReportScopeKind::ObjectsInClass),
            class_id: Some(1),
            default_query: None,
            include: None,
            relation_context: None,
            default_missing_data_policy: None,
            default_limits: None,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
        )
        .unwrap();
        let (text, text_warnings) = render_template(
            &text_template,
            &[],
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
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
            usize::MAX,
        )
        .unwrap();
        layout_v1.updated_at += chrono::Duration::seconds(2);
        let (second, _) = render_template(
            &child,
            &[layout_v2],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(first, "<ul class=\"v1\"><li>srv-01</li></ul>");
        assert_eq!(second, "<ol class=\"v2\"><li>srv-01</li></ol>");
    }

    #[test]
    fn invalidates_cached_environment_when_template_body_changes_without_timestamp_change() {
        let child = template(
            17,
            10,
            "child.html",
            ReportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let layout_v1 = template(
            18,
            10,
            "layout.html",
            ReportContentType::TextHtml,
            "<ul class=\"v1\">{% block body %}{% endblock %}</ul>",
        );
        let mut layout_v2 = layout_v1.clone();
        layout_v2.template = "<ol class=\"v2\">{% block body %}{% endblock %}</ol>".to_string();
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (first, _) = render_template(
            &child,
            &[layout_v1],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();
        let (second, _) = render_template(
            &child,
            &[layout_v2],
            &context,
            ReportContentType::TextHtml,
            ReportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(first, "<ul class=\"v1\"><li>srv-01</li></ul>");
        assert_eq!(second, "<ol class=\"v2\"><li>srv-01</li></ol>");
    }
}
