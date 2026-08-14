//! Application error adapter for backend-neutral template alias normalization.

use crate::errors::ApiError;

/// Normalize a template alias to `snake_case`, rejecting empty or otherwise invalid input.
///
/// Letters are lowercased, runs of spaces/hyphens/underscores collapse to a single underscore,
/// and an underscore is inserted at `camelCase` boundaries. Any other character is an error, as is
/// an alias that normalizes to empty or starts with a digit.
pub(crate) fn normalize_template_alias(alias: &str) -> Result<String, ApiError> {
    hubuum_domain::normalize_template_alias(alias)
        .map_err(|error| ApiError::BadRequest(error.into_message()))
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
