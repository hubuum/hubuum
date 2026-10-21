use std::fmt;

use serde::{Deserialize, Serialize};

/// Optional human-facing relationship wording, independent of template aliases.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[cfg_attr(feature = "openapi", schema(value_type = String, min_length = 1, max_length = 128))]
pub struct RelationLabel(String);

/// A label must be nonblank, bounded, and contain no control characters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelationLabelError;

impl fmt::Display for RelationLabelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("relation label must contain 1–128 characters, no control characters, and no leading or trailing whitespace")
    }
}

impl std::error::Error for RelationLabelError {}

impl RelationLabel {
    pub fn new(value: String) -> Result<Self, RelationLabelError> {
        if value.is_empty()
            || value.chars().count() > 128
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(RelationLabelError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Neutral wording for old data and an unspecified direction.
    #[must_use]
    pub fn display(value: Option<&Self>) -> &str {
        value.map_or("related to", Self::as_str)
    }
}

impl TryFrom<String> for RelationLabel {
    type Error = RelationLabelError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<RelationLabel> for String {
    fn from(value: RelationLabel) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("")]
    #[case(" ")]
    #[case(" contains")]
    #[case("contains ")]
    #[case("is\nin")]
    #[case("is\tin")]
    #[case("\u{00a0}contains")]
    #[case("contains\u{202f}")]
    #[case("is\u{0085}in")]
    fn rejects_invalid_labels(#[case] value: &str) {
        assert!(RelationLabel::new(value.to_owned()).is_err());
    }

    #[rstest]
    #[case(128, true)]
    #[case(129, false)]
    fn bounds_unicode_characters(#[case] length: usize, #[case] valid: bool) {
        assert_eq!(RelationLabel::new("ø".repeat(length)).is_ok(), valid);
    }

    #[test]
    fn absent_label_is_neutral() {
        assert_eq!(RelationLabel::display(None), "related to");
    }
}
