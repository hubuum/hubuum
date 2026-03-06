use std::collections::HashMap;

use crate::errors::ApiError;
use crate::models::{ReportContentType, ReportMissingDataPolicy, ReportWarning};

pub fn render_template(
    template: &str,
    context: &serde_json::Value,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
) -> Result<(String, Vec<ReportWarning>), ApiError> {
    let mut warnings = Vec::new();
    let rendered = render_section(
        template,
        context,
        context,
        content_type,
        missing_data_policy,
        &mut warnings,
    )?;
    Ok((rendered, warnings))
}

fn render_section(
    template: &str,
    root: &serde_json::Value,
    current: &serde_json::Value,
    content_type: ReportContentType,
    missing_data_policy: ReportMissingDataPolicy,
    warnings: &mut Vec<ReportWarning>,
) -> Result<String, ApiError> {
    let mut out = String::new();
    let mut index = 0usize;

    while let Some(relative_start) = template[index..].find("{{") {
        let start = index + relative_start;
        out.push_str(&template[index..start]);

        let end = template[start + 2..]
            .find("}}")
            .map(|offset| start + 2 + offset)
            .ok_or_else(|| ApiError::BadRequest("Unterminated template tag".to_string()))?;

        let tag = template[start + 2..end].trim();
        if let Some(expr) = tag.strip_prefix("#each ") {
            let block_start = end + 2;
            let (inner, next_index) = extract_each_block(template, block_start)?;
            let Some(value) = lookup_value(root, current, expr.trim()) else {
                handle_missing_loop(expr.trim(), missing_data_policy, warnings)?;
                index = next_index;
                continue;
            };

            let Some(items) = value.as_array() else {
                match missing_data_policy {
                    ReportMissingDataPolicy::Strict => {
                        return Err(ApiError::BadRequest(format!(
                            "Template path '{}' is not iterable",
                            expr.trim()
                        )));
                    }
                    ReportMissingDataPolicy::Null | ReportMissingDataPolicy::Omit => {
                        warnings.push(ReportWarning {
                            code: "invalid_each_target".to_string(),
                            message: format!(
                                "Template path '{}' did not resolve to an array",
                                expr.trim()
                            ),
                            path: Some(expr.trim().to_string()),
                        });
                        index = next_index;
                        continue;
                    }
                }
            };

            for item in items {
                out.push_str(&render_section(
                    inner,
                    root,
                    item,
                    content_type,
                    missing_data_policy,
                    warnings,
                )?);
            }
            index = next_index;
            continue;
        }

        if tag == "/each" {
            return Err(ApiError::BadRequest(
                "Unexpected closing template tag '{{/each}}'".to_string(),
            ));
        }

        let rendered = match lookup_value(root, current, tag) {
            Some(value) => stringify_value(value, content_type),
            None => handle_missing_value(tag, missing_data_policy, warnings)?,
        };
        out.push_str(&rendered);
        index = end + 2;
    }

    out.push_str(&template[index..]);
    Ok(out)
}

fn extract_each_block(template: &str, block_start: usize) -> Result<(&str, usize), ApiError> {
    let mut cursor = block_start;
    let mut depth = 1usize;

    while let Some(relative_start) = template[cursor..].find("{{") {
        let start = cursor + relative_start;
        let end = template[start + 2..]
            .find("}}")
            .map(|offset| start + 2 + offset)
            .ok_or_else(|| ApiError::BadRequest("Unterminated template tag".to_string()))?;
        let tag = template[start + 2..end].trim();

        if tag.starts_with("#each ") {
            depth += 1;
        } else if tag == "/each" {
            depth -= 1;
            if depth == 0 {
                return Ok((&template[block_start..start], end + 2));
            }
        }

        cursor = end + 2;
    }

    Err(ApiError::BadRequest(
        "Missing closing template tag '{{/each}}'".to_string(),
    ))
}

fn lookup_value<'a>(
    root: &'a serde_json::Value,
    current: &'a serde_json::Value,
    expr: &str,
) -> Option<&'a serde_json::Value> {
    let expr = expr.trim();
    if expr.is_empty() {
        return None;
    }

    if expr == "this" {
        return Some(current);
    }

    let candidates = candidate_paths(expr);
    for (base, path) in candidates {
        let value = match base.as_str() {
            "root" => walk_path(root, &path),
            "this" => walk_path(current, &path),
            _ => None,
        };

        if value.is_some() {
            return value;
        }
    }

    None
}

fn candidate_paths(expr: &str) -> Vec<(String, Vec<&str>)> {
    let parts = expr
        .split('.')
        .filter(|piece| !piece.is_empty())
        .collect::<Vec<_>>();

    if parts.is_empty() {
        return Vec::new();
    }

    if parts[0] == "root" || parts[0] == "this" {
        return vec![(parts[0].to_string(), parts[1..].to_vec())];
    }

    vec![
        ("this".to_string(), parts.clone()),
        ("root".to_string(), parts),
    ]
}

fn walk_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut current = value;

    for piece in path {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(*piece)?;
            }
            serde_json::Value::Array(items) => {
                let index = piece.parse::<usize>().ok()?;
                current = items.get(index)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

fn stringify_value(value: &serde_json::Value, content_type: ReportContentType) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(boolean) => boolean.to_string(),
        serde_json::Value::Number(number) => number.to_string(),
        serde_json::Value::String(text) => match content_type {
            ReportContentType::TextHtml => escape_html(text),
            _ => text.clone(),
        },
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let json = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            match content_type {
                ReportContentType::TextHtml => escape_html(&json),
                _ => json,
            }
        }
    }
}

fn handle_missing_loop(
    path: &str,
    missing_data_policy: ReportMissingDataPolicy,
    warnings: &mut Vec<ReportWarning>,
) -> Result<(), ApiError> {
    match missing_data_policy {
        ReportMissingDataPolicy::Strict => Err(ApiError::BadRequest(format!(
            "Template path '{}' did not resolve to an array",
            path
        ))),
        ReportMissingDataPolicy::Null | ReportMissingDataPolicy::Omit => {
            warnings.push(ReportWarning {
                code: "missing_value".to_string(),
                message: format!("Template path '{}' was not found", path),
                path: Some(path.to_string()),
            });
            Ok(())
        }
    }
}

fn handle_missing_value(
    path: &str,
    missing_data_policy: ReportMissingDataPolicy,
    warnings: &mut Vec<ReportWarning>,
) -> Result<String, ApiError> {
    match missing_data_policy {
        ReportMissingDataPolicy::Strict => Err(ApiError::BadRequest(format!(
            "Template path '{}' was not found",
            path
        ))),
        ReportMissingDataPolicy::Null => {
            warnings.push(ReportWarning {
                code: "missing_value".to_string(),
                message: format!("Template path '{}' was not found", path),
                path: Some(path.to_string()),
            });
            Ok("null".to_string())
        }
        ReportMissingDataPolicy::Omit => {
            warnings.push(ReportWarning {
                code: "missing_value".to_string(),
                message: format!("Template path '{}' was not found", path),
                path: Some(path.to_string()),
            });
            Ok(String::new())
        }
    }
}

fn escape_html(input: &str) -> String {
    let replacements = HashMap::from([
        ('&', "&amp;"),
        ('<', "&lt;"),
        ('>', "&gt;"),
        ('\"', "&quot;"),
        ('\'', "&#39;"),
    ]);

    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        if let Some(replacement) = replacements.get(&ch) {
            escaped.push_str(replacement);
        } else {
            escaped.push(ch);
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_each_blocks_and_nested_values() {
        let context = serde_json::json!({
            "items": [
                {"name": "srv-01", "data": {"owner": "alice"}},
                {"name": "srv-02", "data": {"owner": "bob"}}
            ]
        });

        let (rendered, warnings) = render_template(
            "{{#each items}}{{this.name}}={{this.data.owner}}\n{{/each}}",
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=alice\nsrv-02=bob\n");
        assert!(warnings.is_empty());
    }

    #[test]
    fn omits_missing_values_when_requested() {
        let context = serde_json::json!({
            "items": [{"name": "srv-01"}]
        });

        let (rendered, warnings) = render_template(
            "{{#each items}}{{this.name}}={{this.data.owner}}\n{{/each}}",
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Omit,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=\n");
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "missing_value");
    }
}
