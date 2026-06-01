use std::collections::HashMap;

use crate::errors::ApiError;
use crate::models::{ReportContentType, ReportMissingDataPolicy, ReportWarning};

#[derive(Clone, Debug, PartialEq)]
enum PathSegment {
    Key(String),
    Index(usize),
}

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
            let Some(value) = lookup_value(root, current, expr.trim())? else {
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

        let rendered = match lookup_value(root, current, tag)? {
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
) -> Result<Option<&'a serde_json::Value>, ApiError> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(None);
    }

    if expr == "this" {
        return Ok(Some(current));
    }

    let candidates = candidate_paths(expr)?;
    for (base, path) in candidates {
        let value = match base.as_str() {
            "root" => walk_path(root, &path),
            "this" => walk_path(current, &path),
            _ => None,
        };

        if value.is_some() {
            return Ok(value);
        }
    }

    Ok(None)
}

fn candidate_paths(expr: &str) -> Result<Vec<(String, Vec<PathSegment>)>, ApiError> {
    let mut parts = parse_path(expr)?;

    if parts.is_empty() {
        return Ok(Vec::new());
    }

    if matches!(parts.first(), Some(PathSegment::Key(base)) if base == "root" || base == "this") {
        let PathSegment::Key(base) = parts.remove(0) else {
            unreachable!("first path segment was matched as a key");
        };
        return Ok(vec![(base, parts)]);
    }

    Ok(vec![
        ("this".to_string(), parts.clone()),
        ("root".to_string(), parts),
    ])
}

fn parse_path(expr: &str) -> Result<Vec<PathSegment>, ApiError> {
    let chars = expr.chars().collect::<Vec<_>>();
    let mut parts = Vec::new();
    let mut index = 0usize;

    while index < chars.len() {
        match chars[index] {
            '.' => {
                index += 1;
            }
            '[' => {
                let (segment, next_index) = parse_bracket_segment(&chars, index, expr)?;
                parts.push(segment);
                index = next_index;
                let is_invalid_next = chars
                    .get(index)
                    .is_some_and(|next| *next != '.' && *next != '[');
                if is_invalid_next {
                    return Err(invalid_template_path(expr));
                }
            }
            _ => {
                let start = index;
                while index < chars.len() && chars[index] != '.' && chars[index] != '[' {
                    index += 1;
                }
                let key = chars[start..index].iter().collect::<String>();
                if !key.is_empty() {
                    parts.push(PathSegment::Key(key));
                }
            }
        }
    }

    Ok(parts)
}

fn parse_bracket_segment(
    chars: &[char],
    start: usize,
    expr: &str,
) -> Result<(PathSegment, usize), ApiError> {
    let mut index = start + 1;
    while index < chars.len() && chars[index].is_whitespace() {
        index += 1;
    }

    if index >= chars.len() {
        return Err(invalid_template_path(expr));
    }

    if chars[index] == '"' || chars[index] == '\'' {
        let quote = chars[index];
        index += 1;
        let mut key = String::new();
        while index < chars.len() {
            match chars[index] {
                '\\' => {
                    index += 1;
                    if index >= chars.len() {
                        return Err(invalid_template_path(expr));
                    }
                    key.push(chars[index]);
                    index += 1;
                }
                ch if ch == quote => {
                    index += 1;
                    while index < chars.len() && chars[index].is_whitespace() {
                        index += 1;
                    }
                    if chars.get(index) != Some(&']') {
                        return Err(invalid_template_path(expr));
                    }
                    return Ok((PathSegment::Key(key), index + 1));
                }
                ch => {
                    key.push(ch);
                    index += 1;
                }
            }
        }

        return Err(invalid_template_path(expr));
    }

    let value_start = index;
    while index < chars.len() && chars[index] != ']' {
        index += 1;
    }
    if index >= chars.len() {
        return Err(invalid_template_path(expr));
    }

    let value = chars[value_start..index]
        .iter()
        .collect::<String>()
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(invalid_template_path(expr));
    }

    let segment = value
        .parse::<usize>()
        .map(PathSegment::Index)
        .unwrap_or(PathSegment::Key(value));
    Ok((segment, index + 1))
}

fn invalid_template_path(expr: &str) -> ApiError {
    ApiError::BadRequest(format!("Invalid template path syntax: '{expr}'"))
}

fn walk_path<'a>(
    value: &'a serde_json::Value,
    path: &[PathSegment],
) -> Option<&'a serde_json::Value> {
    let mut current = value;

    for segment in path {
        match (current, segment) {
            (serde_json::Value::Object(map), PathSegment::Key(key)) => {
                current = map.get(key)?;
            }
            (serde_json::Value::Array(items), PathSegment::Index(index)) => {
                current = items.get(*index)?;
            }
            (serde_json::Value::Array(items), PathSegment::Key(key)) => {
                let index = key.parse::<usize>().ok()?;
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
    fn renders_array_indexes_in_dotted_paths() {
        let context = serde_json::json!({
            "items": [
                {"name": "srv-01", "data": {"tags": ["prod", "app"]}},
            ]
        });

        let (rendered, warnings) = render_template(
            "{{#each items}}{{this.name}}={{this.data.tags.0}}/{{this.data.tags[1]}}\n{{/each}}",
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "srv-01=prod/app\n");
        assert!(warnings.is_empty());
    }

    #[test]
    fn renders_quoted_hash_keys_in_bracket_paths() {
        let context = serde_json::json!({
            "items": [
                {
                    "name": "srv-01",
                    "data": {
                        "owner.name": "alice",
                        "service-list": [{"display name": "frontend"}]
                    }
                },
            ]
        });

        let (rendered, warnings) = render_template(
            "{{#each items}}{{this.data[\"owner.name\"]}}/{{this.data['service-list'][0]['display name']}}\n{{/each}}",
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap();

        assert_eq!(rendered, "alice/frontend\n");
        assert!(warnings.is_empty());
    }

    #[test]
    fn rejects_bare_path_segment_after_bracket_segment() {
        let context = serde_json::json!({
            "items": [{"data": [{"name": "srv-01"}]}]
        });

        let error = render_template(
            "{{#each items}}{{this.data[0]name}}{{/each}}",
            &context,
            ReportContentType::TextPlain,
            ReportMissingDataPolicy::Strict,
        )
        .unwrap_err();

        assert!(
            matches!(error, ApiError::BadRequest(message) if message == "Invalid template path syntax: 'this.data[0]name'")
        );
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
