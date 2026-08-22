//! Backend-neutral normalization for export template aliases.

use std::fmt;

/// Invalid template alias supplied at an application or storage boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateAliasError {
    message: String,
}

impl TemplateAliasError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Consume the error and return its caller-safe explanation.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }
}

impl fmt::Display for TemplateAliasError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TemplateAliasError {}

/// Normalize a template alias to `snake_case`.
///
/// Letters are lowercased, runs of spaces, hyphens, and underscores collapse
/// to one underscore, and camel-case boundaries gain an underscore. The alias
/// must begin with a letter after normalization.
pub fn normalize_template_alias(alias: &str) -> Result<String, TemplateAliasError> {
    let trimmed = alias.trim();
    if trimmed.is_empty() {
        return Err(TemplateAliasError::new("template aliases cannot be empty"));
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
            return Err(TemplateAliasError::new(format!(
                "template aliases may only contain letters, numbers, spaces, hyphens, and underscores: '{alias}'"
            )));
        }
    }

    let normalized = normalized.trim_matches('_').to_string();
    if normalized.is_empty() || normalized.starts_with(|character: char| character.is_ascii_digit())
    {
        return Err(TemplateAliasError::new(format!(
            "template aliases must start with a letter and contain at least one alphanumeric character: '{alias}'"
        )));
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_case_boundaries_and_separators() {
        assert_eq!(normalize_template_alias("HostName").unwrap(), "host_name");
        assert_eq!(
            normalize_template_alias("  host - name__alias ").unwrap(),
            "host_name_alias"
        );
    }

    #[test]
    fn rejects_empty_invalid_and_leading_digit_aliases() {
        for alias in ["   ", "host/name", "1host"] {
            assert!(
                normalize_template_alias(alias).is_err(),
                "accepted {alias:?}"
            );
        }
    }
}
