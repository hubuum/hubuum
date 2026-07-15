use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock, RwLock};

use hubuum_templates::{
    MissingValue, MissingValueRecorder, SizeLimitedWriter, TemplateLimits, prepare_template,
    register_curated_helpers,
};
use lru::LruCache;
use minijinja::value::Value;
use minijinja::{
    AutoEscape, Environment, Error as MiniJinjaError, UndefinedBehavior, escape_formatter,
};

use crate::config::get_config;
use crate::errors::ApiError;
use crate::models::{ExportContentType, ExportMissingDataPolicy, ExportTemplate, ExportWarning};

const TEMPLATE_ENV_CACHE_MAX_ENTRIES: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TemplateEnvCacheKey {
    collection_id: i32,
    collection_signature: CollectionTemplateSignature,
    template_name: String,
    content_type: ExportContentType,
    missing_data_policy: ExportMissingDataPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CollectionTemplateSignature {
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
    warnings: Vec<ExportWarning>,
}

thread_local! {
    static TEMPLATE_WARNING_CAPTURE: RefCell<Option<TemplateWarningCapture>> = const { RefCell::new(None) };
}

pub fn validate_template(
    template_name: &str,
    template_source: &str,
    collection_id: i32,
    collection_templates: &[ExportTemplate],
    content_type: ExportContentType,
) -> Result<(), ApiError> {
    validate_template_syntax(template_name, template_source)?;
    let (recursion_limit, fuel) = template_limits_from_config();
    validate_template_with_limits(
        template_name,
        template_source,
        collection_id,
        collection_templates,
        content_type,
        recursion_limit,
        fuel,
    )
}

pub fn validate_template_syntax(
    template_name: &str,
    template_source: &str,
) -> Result<(), ApiError> {
    let (recursion_limit, fuel) = template_limits_from_config();
    prepare_template(template_source)
        .limit_recursion(recursion_limit)
        .limit_fuel(fuel)
        .validate()
        .map_err(|error| {
            ApiError::BadRequest(format!(
                "Invalid export template '{template_name}': {error}"
            ))
        })
}

pub fn validate_template_with_limits(
    template_name: &str,
    template_source: &str,
    collection_id: i32,
    collection_templates: &[ExportTemplate],
    content_type: ExportContentType,
    recursion_limit: usize,
    fuel: u64,
) -> Result<(), ApiError> {
    let env = build_environment(
        template_name,
        template_source,
        collection_id,
        collection_templates,
        content_type,
        ExportMissingDataPolicy::Omit,
        TemplateLimits::new(recursion_limit, fuel),
    )?;

    env.env
        .get_template(&env.template_name)
        .map_err(|error| template_error("Template validation failed", error))?
        .render(validation_context(content_type))
        .map_err(|error| template_error("Template validation failed", error))?;

    Ok(())
}

pub(crate) fn validate_template_sources(
    template_name: &str,
    template_source: &str,
    collection_templates: &[(String, String)],
    content_type: ExportContentType,
) -> Result<(), ApiError> {
    validate_template_syntax(template_name, template_source)?;
    let (recursion_limit, fuel) = template_limits_from_config();
    let mut template_map = collection_templates
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
    template_map.insert(template_name.to_string(), template_source.to_string());
    let env = build_environment_from_map(
        template_name,
        template_source,
        template_map,
        content_type,
        ExportMissingDataPolicy::Omit,
        TemplateLimits::new(recursion_limit, fuel),
    )?;

    env.env
        .get_template(&env.template_name)
        .map_err(|error| template_error("Template validation failed", error))?
        .render(validation_context(content_type))
        .map_err(|error| template_error("Template validation failed", error))?;

    Ok(())
}

pub fn render_template(
    template: &ExportTemplate,
    collection_templates: &[ExportTemplate],
    context: &serde_json::Value,
    content_type: ExportContentType,
    missing_data_policy: ExportMissingDataPolicy,
    max_output_bytes: usize,
) -> Result<(String, Vec<ExportWarning>), ApiError> {
    let (recursion_limit, fuel) = template_limits_from_config();
    let cache_key = TemplateEnvCacheKey {
        collection_id: template.collection_id,
        collection_signature: collection_signature(template.collection_id, collection_templates),
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
                template.collection_id,
                collection_templates,
                content_type,
                missing_data_policy,
                TemplateLimits::new(recursion_limit, fuel),
            )?);
            let mut cache = template_env_cache()
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Drop now-stale environments for this collection (templates changed) before inserting.
            let stale_keys = cache
                .iter()
                .filter(|(key, _)| {
                    key.collection_id == cache_key.collection_id
                        && key.collection_signature != cache_key.collection_signature
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
        Ok(_captured) => Ok((
            writer.into_string().map_err(|error| {
                ApiError::InternalServerError(format!(
                    "Rendered export was not valid UTF-8: {error}"
                ))
            })?,
            warnings,
        )),
        Err(error) => {
            if writer.exceeded() {
                return Err(ApiError::PayloadTooLarge(format!(
                    "Rendered export exceeded max_output_bytes (> {max_output_bytes})"
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

fn collection_signature(
    collection_id: i32,
    collection_templates: &[ExportTemplate],
) -> CollectionTemplateSignature {
    let mut templates = collection_templates
        .iter()
        .filter(|template| template.collection_id == collection_id)
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

    CollectionTemplateSignature {
        template_count: templates.len(),
        max_updated_at_micros,
        template_hash: hasher.finish(),
    }
}

fn build_environment(
    template_name: &str,
    template_source: &str,
    collection_id: i32,
    collection_templates: &[ExportTemplate],
    content_type: ExportContentType,
    missing_data_policy: ExportMissingDataPolicy,
    limits: TemplateLimits,
) -> Result<CachedTemplateEnvironment, ApiError> {
    let template_map = build_collection_template_map(
        collection_id,
        template_name,
        template_source,
        collection_templates,
    );
    build_environment_from_map(
        template_name,
        template_source,
        template_map,
        content_type,
        missing_data_policy,
        limits,
    )
}

fn build_environment_from_map(
    template_name: &str,
    template_source: &str,
    template_map: HashMap<String, String>,
    content_type: ExportContentType,
    missing_data_policy: ExportMissingDataPolicy,
    limits: TemplateLimits,
) -> Result<CachedTemplateEnvironment, ApiError> {
    let mut env = Environment::new();
    let template_map = Arc::new(template_map);

    env.set_keep_trailing_newline(true);
    env.set_undefined_behavior(undefined_behavior(missing_data_policy));
    env.set_recursion_limit(limits.recursion_limit());
    env.set_fuel(Some(limits.fuel()));
    env.set_auto_escape_callback(move |_| match content_type {
        ExportContentType::TextHtml => AutoEscape::Html,
        _ => AutoEscape::None,
    });
    env.set_formatter(move |out, state, value| match missing_data_policy {
        ExportMissingDataPolicy::Strict => escape_formatter(out, state, value),
        ExportMissingDataPolicy::Omit => format_nullable_value(out, state, value, None),
        ExportMissingDataPolicy::Null => format_nullable_value(out, state, value, Some("null")),
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
                config.export_template_recursion_limit,
                config.export_template_fuel,
            )
        })
        .unwrap_or((
            crate::config::DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
            crate::config::DEFAULT_EXPORT_TEMPLATE_FUEL,
        ))
}

fn build_collection_template_map(
    collection_id: i32,
    template_name: &str,
    template_source: &str,
    collection_templates: &[ExportTemplate],
) -> HashMap<String, String> {
    let mut templates = collection_templates
        .iter()
        .filter(|template| template.collection_id == collection_id)
        .map(|template| (template.name.clone(), template.template.clone()))
        .collect::<HashMap<_, _>>();
    templates.insert(template_name.to_string(), template_source.to_string());
    templates
}

fn validation_context(content_type: ExportContentType) -> serde_json::Value {
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
            "collection_id": 0,
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

fn undefined_behavior(missing_data_policy: ExportMissingDataPolicy) -> UndefinedBehavior {
    match missing_data_policy {
        ExportMissingDataPolicy::Strict => UndefinedBehavior::Strict,
        ExportMissingDataPolicy::Null | ExportMissingDataPolicy::Omit => {
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

fn finish_template_warning_capture() -> Vec<ExportWarning> {
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
        capture.warnings.push(ExportWarning {
            code: "template_missing_value".to_string(),
            message: format!("Template '{template_name}' rendered one or more missing values"),
            path,
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExportScopeKind, ExportTemplateKind};
    use crate::tests::docs_examples::required_labeled_block;
    use rstest::rstest;

    const TEMPLATE_GUIDE: &str = include_str!("../../docs/export_template_guide.md");

    fn template(
        id: i32,
        collection_id: i32,
        name: &str,
        content_type: ExportContentType,
        source: &str,
    ) -> ExportTemplate {
        let now = chrono::Utc::now().naive_utc();
        ExportTemplate {
            id,
            collection_id,
            name: name.to_string(),
            description: "template".to_string(),
            content_type,
            template: source.to_string(),
            kind: ExportTemplateKind::Export,
            scope_kind: Some(ExportScopeKind::ObjectsInClass),
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

    fn template_guide_context() -> serde_json::Value {
        serde_json::json!({
            "items": [
                {
                    "id": 101,
                    "name": "srv-app-01",
                    "description": "Application server",
                    "collection_id": 7,
                    "hubuum_class_id": 42,
                    "data": {
                        "owner": "alice",
                        "hostname": "srv-app-01.example.org",
                        "environment": "prod",
                        "tags": ["prod", "app"]
                    }
                },
                {
                    "id": 102,
                    "name": "srv-db-01",
                    "description": "Database server",
                    "collection_id": 7,
                    "hubuum_class_id": 42,
                    "data": {
                        "owner": "bob",
                        "hostname": "srv-db-01.example.org",
                        "environment": "prod",
                        "tags": ["prod", "db"]
                    }
                }
            ],
            "meta": {
                "count": 2,
                "truncated": false,
                "scope": {
                    "kind": "objects_in_class",
                    "class_id": 42,
                    "object_id": null
                },
                "content_type": "text/plain"
            },
            "warnings": [],
            "request": {
                "scope": {
                    "kind": "objects_in_class",
                    "class_id": 42,
                    "object_id": null
                },
                "query": "name__contains=srv-&sort=name"
            }
        })
    }

    #[rstest]
    #[case::same_import(
        vec![("fragment.txt".to_string(), "fragment".to_string())],
        true
    )]
    #[case::missing(Vec::new(), false)]
    fn validates_composed_template_sources(
        #[case] sources: Vec<(String, String)>,
        #[case] expected_valid: bool,
    ) {
        let result = validate_template_sources(
            "export.txt",
            "{% include \"fragment.txt\" %}",
            &sources,
            ExportContentType::TextPlain,
        );

        assert_eq!(result.is_ok(), expected_valid);
    }

    fn template_guide_block(label: &str) -> String {
        required_labeled_block(TEMPLATE_GUIDE, label).unwrap().body
    }

    fn assert_template_guide_example(
        name: &str,
        content_type: ExportContentType,
        missing_data_policy: ExportMissingDataPolicy,
    ) -> (String, Vec<ExportWarning>) {
        let template = template(
            100,
            10,
            &format!("{name}.txt"),
            content_type,
            &template_guide_block(&format!("template-guide/{name}/template")),
        );

        render_template(
            &template,
            &[],
            &template_guide_context(),
            content_type,
            missing_data_policy,
            usize::MAX,
        )
        .unwrap()
    }

    #[test]
    fn template_guide_plain_text_example_matches_renderer() {
        let (rendered, warnings) = assert_template_guide_example(
            "plain",
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Strict,
        );

        assert_eq!(
            rendered,
            template_guide_block("template-guide/plain/output")
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn template_guide_html_example_matches_renderer() {
        let (rendered, warnings) = assert_template_guide_example(
            "html",
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
        );

        assert_eq!(rendered, template_guide_block("template-guide/html/output"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn template_guide_csv_example_matches_renderer() {
        let (rendered, warnings) = assert_template_guide_example(
            "csv",
            ExportContentType::TextCsv,
            ExportMissingDataPolicy::Strict,
        );

        assert_eq!(rendered, template_guide_block("template-guide/csv/output"));
        assert!(warnings.is_empty());
    }

    #[test]
    fn template_guide_nested_array_example_matches_renderer() {
        let (rendered, warnings) = assert_template_guide_example(
            "nested-array",
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Strict,
        );

        assert_eq!(
            rendered,
            template_guide_block("template-guide/nested-array/output")
        );
        assert!(warnings.is_empty());
    }

    #[test]
    fn template_guide_missing_data_policy_examples_match_renderer() {
        let (null_rendered, null_warnings) = assert_template_guide_example(
            "missing-data",
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Null,
        );
        let (omit_rendered, omit_warnings) = assert_template_guide_example(
            "missing-data",
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Omit,
        );

        assert_eq!(
            null_rendered,
            template_guide_block("template-guide/missing-data/null-output")
        );
        assert_eq!(
            omit_rendered,
            template_guide_block("template-guide/missing-data/omit-output")
        );
        assert_eq!(null_warnings.len(), 1);
        assert_eq!(omit_warnings.len(), 1);
        assert_eq!(null_warnings[0].code, "template_missing_value");
        assert_eq!(omit_warnings[0].code, "template_missing_value");
    }

    #[test]
    fn renders_jinja_loops_and_nested_values() {
        let template = template(
            1,
            10,
            "servers.txt",
            ExportContentType::TextPlain,
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
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=alice\nsrv-02=bob\n");
        assert!(warnings.is_empty());
    }

    #[test]
    fn supports_same_collection_includes() {
        let layout = template(
            2,
            10,
            "layout.html",
            ExportContentType::TextHtml,
            "<ul>{% block body %}{% endblock %}</ul>",
        );
        let child = template(
            3,
            10,
            "child.html",
            ExportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}{% for item in items %}<li>{{ item.name }}</li>{% endfor %}{% endblock %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, _) = render_template(
            &child,
            &[layout],
            &context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
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
            ExportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Omit,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=\n");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "template_missing_value");
        assert!(warnings[0].message.contains("missing.txt"));
    }

    #[test]
    fn renders_null_for_missing_values_and_exports_warning() {
        let template = template(
            5,
            10,
            "missing-null.txt",
            ExportContentType::TextPlain,
            "{% for item in items %}{{ item.name }}={{ item.data.owner }}\n{% endfor %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &template,
            &[],
            &context,
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Null,
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
            ExportContentType::TextHtml,
            "{{ items[0].name }}",
        );
        let text_template = template(
            7,
            10,
            "escape.txt",
            ExportContentType::TextPlain,
            "{{ items[0].name }}",
        );
        let context = serde_json::json!({
            "items": [{"name": "<b>srv&01</b>"}]
        });

        let (html, html_warnings) = render_template(
            &html_template,
            &[],
            &context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();
        let (text, text_warnings) = render_template(
            &text_template,
            &[],
            &context,
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(html, "&lt;b&gt;srv&amp;01&lt;&#x2f;b&gt;");
        assert_eq!(text, "<b>srv&01</b>");
        assert!(html_warnings.is_empty());
        assert!(text_warnings.is_empty());
    }

    #[test]
    fn supports_same_collection_imports() {
        let macros = template(
            8,
            10,
            "macros.txt",
            ExportContentType::TextPlain,
            "{% macro owner(item) %}{{ item.data.owner }}{% endmacro %}",
        );
        let child = template(
            9,
            10,
            "child.txt",
            ExportContentType::TextPlain,
            "{% import \"macros.txt\" as macros %}{{ items[0].name }}={{ macros.owner(items[0]) }}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01", "data": {"owner": "alice"}}]
        });

        let (rendered, warnings) = render_template(
            &child,
            &[macros],
            &context,
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Strict,
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
            ExportContentType::TextPlain,
            "{{ items[0].data.owner }}",
        );
        let export = template(
            16,
            10,
            "export.hosts.txt",
            ExportContentType::TextPlain,
            "{% include \"partial.owner.txt\" %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            &export,
            &[partial],
            &context,
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Omit,
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
            ExportContentType::TextPlain,
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
            ExportContentType::TextPlain,
            ExportMissingDataPolicy::Omit,
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
    fn rejects_cross_collection_template_loading() {
        let layout = template(
            10,
            20,
            "layout.html",
            ExportContentType::TextHtml,
            "<ul>{% block body %}{% endblock %}</ul>",
        );
        let child = template(
            11,
            10,
            "child.html",
            ExportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let error = render_template(
            &child,
            &[layout],
            &context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
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
            ExportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let mut layout_v1 = template(
            13,
            10,
            "layout.html",
            ExportContentType::TextHtml,
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
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();
        layout_v1.updated_at += chrono::Duration::seconds(2);
        let (second, _) = render_template(
            &child,
            &[layout_v2],
            &context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
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
            ExportContentType::TextHtml,
            "{% extends \"layout.html\" %}{% block body %}<li>{{ items[0].name }}</li>{% endblock %}",
        );
        let layout_v1 = template(
            18,
            10,
            "layout.html",
            ExportContentType::TextHtml,
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
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();
        let (second, _) = render_template(
            &child,
            &[layout_v2],
            &context,
            ExportContentType::TextHtml,
            ExportMissingDataPolicy::Strict,
            usize::MAX,
        )
        .unwrap();

        assert_eq!(first, "<ul class=\"v1\"><li>srv-01</li></ul>");
        assert_eq!(second, "<ol class=\"v2\"><li>srv-01</li></ol>");
    }
}
