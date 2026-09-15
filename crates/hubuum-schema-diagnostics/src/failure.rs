use jsonschema::error::{ValidationError, ValidationErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Proof that a validator location can be read from the supplied document.
pub(crate) struct DocumentSchemaLocation<'a> {
    path: &'a str,
    constraint: &'a Value,
}

impl<'a> DocumentSchemaLocation<'a> {
    pub(crate) fn from_error(document: &'a Value, error: &'a ValidationError<'_>) -> Option<Self> {
        // The property-name wrapper copies the child's canonical path but loses
        // references traversed inside the naming schema. Its child retains the
        // original evaluation path needed to establish the resource boundary.
        if let ValidationErrorKind::PropertyNames { error } = error.kind() {
            return Self::from_error(document, error);
        }
        let path = error.schema_path().as_str();
        // Canonical paths after a reference may be relative to another resource.
        // Without the caller's reference registry, only a document with local
        // fragment references and no nested resource IDs proves their origin.
        if path != error.evaluation_path().as_str() && !references_stay_in_document(document, true)
        {
            return None;
        }
        Some(Self {
            path,
            constraint: document.pointer(path)?,
        })
    }

    pub(crate) fn constraint(&self) -> &Value {
        self.constraint
    }
}

fn references_stay_in_document(value: &Value, root: bool) -> bool {
    match value {
        Value::Object(map) => {
            if !root
                && ["$id", "id"]
                    .iter()
                    .any(|key| map.get(*key).is_some_and(Value::is_string))
            {
                return false;
            }
            map.iter().all(|(key, value)| {
                !(matches!(key.as_str(), "$ref" | "$recursiveRef" | "$dynamicRef")
                    && value
                        .as_str()
                        .is_some_and(|reference| !reference.starts_with('#')))
                    && references_stay_in_document(value, false)
            })
        }
        Value::Array(values) => values
            .iter()
            .all(|value| references_stay_in_document(value, false)),
        _ => true,
    }
}

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
        let location = DocumentSchemaLocation::from_error(document, error);
        Self::from_location(error, location.as_ref())
    }

    pub(crate) fn from_location(
        error: &ValidationError<'_>,
        location: Option<&DocumentSchemaLocation<'_>>,
    ) -> Self {
        // The legacy grouping key remains independent of instance data.
        let missing_property = match (
            error.kind(),
            location.map(DocumentSchemaLocation::constraint),
        ) {
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
            schema_path: location
                .filter(|location| location.path.len() <= 512)
                .map(|location| location.path.to_owned()),
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
