//! Shared normalization for export template aliases.
//!
//! Relation creation (`db::traits::relations`) and import processing
//! (`db::traits::task_import`) both validate and canonicalize user-supplied template aliases the
//! same way, so the rule lives here once.

use crate::errors::ApiError;

/// Normalize a template alias to `snake_case`, rejecting empty or otherwise invalid input.
///
/// Letters are lowercased, runs of spaces/hyphens/underscores collapse to a single underscore,
/// and an underscore is inserted at `camelCase` boundaries. Any other character is an error, as is
/// an alias that normalizes to empty or starts with a digit.
pub(crate) fn normalize_template_alias(alias: &str) -> Result<String, ApiError> {
    let trimmed = alias.trim();
    if trimmed.is_empty() {
        return Err(ApiError::BadRequest(
            "template aliases cannot be empty".to_string(),
        ));
    }

    let mut normalized = String::new();
    let mut previous_was_separator = true;

    for character in trimmed.chars() {
        if character.is_ascii_alphanumeric() {
            if character.is_ascii_uppercase()
                && !previous_was_separator
                && !normalized.ends_with('_')
            {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
            previous_was_separator = false;
        } else if matches!(character, ' ' | '-' | '_') {
            if !normalized.is_empty() && !normalized.ends_with('_') {
                normalized.push('_');
            }
            previous_was_separator = true;
        } else {
            return Err(ApiError::BadRequest(format!(
                "template aliases may only contain letters, numbers, spaces, hyphens, and underscores: '{alias}'"
            )));
        }
    }

    let normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() || normalized.starts_with(|ch: char| ch.is_ascii_digit()) {
        return Err(ApiError::BadRequest(format!(
            "template aliases must start with a letter and contain at least one alphanumeric character: '{alias}'"
        )));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases_and_inserts_camel_case_boundaries() {
        assert_eq!(normalize_template_alias("HostName").unwrap(), "host_name");
        assert_eq!(normalize_template_alias("hostName").unwrap(), "host_name");
    }

    #[test]
    fn collapses_separators_and_trims() {
        assert_eq!(
            normalize_template_alias("  host - name__alias ").unwrap(),
            "host_name_alias"
        );
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(
            normalize_template_alias("   "),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_invalid_characters() {
        assert!(matches!(
            normalize_template_alias("host/name"),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn rejects_leading_digit() {
        assert!(matches!(
            normalize_template_alias("1host"),
            Err(ApiError::BadRequest(_))
        ));
    }
}
