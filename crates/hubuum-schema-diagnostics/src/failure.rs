use jsonschema::error::{ValidationError, ValidationErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One failure, containing only a keyword and bounded schema-owned metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "FailureSnapshot")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaFailure {
    keyword: String,
    schema_path: Option<String>,
    missing_property: Option<String>,
}

#[derive(Deserialize)]
struct FailureSnapshot {
    keyword: String,
    schema_path: Option<String>,
    missing_property: Option<String>,
}

impl TryFrom<FailureSnapshot> for SchemaFailure {
    type Error = &'static str;

    fn try_from(raw: FailureSnapshot) -> Result<Self, Self::Error> {
        if raw.keyword.is_empty()
            || raw.keyword.len() > 64
            || raw
                .schema_path
                .as_ref()
                .is_some_and(|path| path.len() > 512)
            || raw
                .missing_property
                .as_ref()
                .is_some_and(|name| name.len() > 128)
        {
            return Err("Schema failure exceeds diagnostic bounds");
        }
        Ok(Self {
            keyword: raw.keyword,
            schema_path: raw.schema_path,
            missing_property: raw.missing_property,
        })
    }
}

impl SchemaFailure {
    pub fn keyword(&self) -> &str {
        &self.keyword
    }
    pub fn schema_path(&self) -> Option<&str> {
        self.schema_path.as_deref()
    }
    pub fn missing_property(&self) -> Option<&str> {
        self.missing_property.as_deref()
    }

    pub fn from_error(document: &Value, error: &ValidationError<'_>) -> Self {
        // The legacy grouping key remains independent of instance data.
        let path = error.schema_path().as_str();
        let location = document.pointer(path);
        let missing_property = match (error.kind(), location) {
            (ValidationErrorKind::Required { property }, Some(Value::Array(required)))
                if required.contains(property) =>
            {
                property
                    .as_str()
                    .filter(|name| name.len() <= 128)
                    .map(str::to_owned)
            }
            _ => None,
        };
        Self {
            keyword: match error.kind().keyword() {
                keyword if !keyword.is_empty() && keyword.len() <= 64 => keyword.to_owned(),
                _ => "custom".to_owned(),
            },
            schema_path: (path.len() <= 512 && location.is_some()).then(|| path.to_owned()),
            missing_property,
        }
    }
}

pub fn evaluation_failed(kind: &ValidationErrorKind) -> bool {
    match kind {
        ValidationErrorKind::BacktrackLimitExceeded { .. }
        | ValidationErrorKind::RegexEngineFailure { .. }
        | ValidationErrorKind::Referencing(_) => true,
        ValidationErrorKind::AnyOf { context }
        | ValidationErrorKind::OneOfMultipleValid { context }
        | ValidationErrorKind::OneOfNotValid { context } => context
            .iter()
            .flatten()
            .any(|error| evaluation_failed(error.kind())),
        ValidationErrorKind::PropertyNames { error } => evaluation_failed(error.kind()),
        _ => false,
    }
}
