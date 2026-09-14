//! Saved, bounded repair diagnostics. Validator display strings are never copied.

#![doc = include_str!("../README.md")]

use std::collections::HashSet;

use jsonschema::error::{ValidationError, ValidationErrorKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod failure;
pub use failure::{SchemaFailure, evaluation_failed};

const MAX_ISSUES: usize = 32;
const MAX_PATH_BYTES: usize = 512;
const MAX_EXPECTED_BYTES: usize = 1024;

/// A complete inspection or an explicitly inconclusive evaluation.
#[derive(Clone, Debug)]
pub enum SchemaDiagnosticInspection {
    Valid,
    Invalid(SchemaDiagnostics),
    Uninspectable,
}

/// Diagnostics belong to one inspected snapshot, never to current object data.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "DiagnosticsSnapshot")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaDiagnostics {
    issues: Vec<SchemaIssue>,
    /// At least one further issue exists; the unvisited remainder is not counted.
    truncated: bool,
}

#[derive(Deserialize)]
struct DiagnosticsSnapshot {
    issues: Vec<SchemaIssue>,
    truncated: bool,
}

impl TryFrom<DiagnosticsSnapshot> for SchemaDiagnostics {
    type Error = &'static str;

    fn try_from(raw: DiagnosticsSnapshot) -> Result<Self, Self::Error> {
        if raw.issues.is_empty() || raw.issues.len() > MAX_ISSUES {
            return Err("Schema diagnostics require between 1 and 32 issues");
        }
        Ok(Self {
            issues: raw.issues,
            truncated: raw.truncated,
        })
    }
}

impl SchemaDiagnostics {
    pub fn issues(&self) -> &[SchemaIssue] {
        &self.issues
    }
    pub const fn truncated(&self) -> bool {
        self.truncated
    }

    pub fn first_failure(&self) -> &SchemaFailure {
        &self.issues[0].reason
    }
}

/// Context deliberately includes types and sizes, never actual scalar values.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum SchemaActualValue {
    Null,
    Boolean,
    Number,
    String { characters: usize },
    Array { items: usize },
    Object { properties: usize },
}

impl From<&Value> for SchemaActualValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(_) => Self::Number,
            Value::String(value) => Self::String {
                characters: value.chars().count(),
            },
            Value::Array(value) => Self::Array { items: value.len() },
            Value::Object(value) => Self::Object {
                properties: value.len(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum SchemaDiagnosticOmission {
    ActualValueRedacted,
    InstancePathRedactedOrTooLong,
    SchemaConstraintUnavailableOrTooLarge,
}

/// Distinguishes an expected JSON null from an omitted schema constraint.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub enum SchemaExpectedValue {
    Available(Value),
    Omitted,
}

/// JSON Pointers use zero-based array indexes; an empty pointer means the root.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(try_from = "IssueSnapshot")]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SchemaIssue {
    reason: SchemaFailure,
    instance_path: Option<String>,
    message: String,
    expected: SchemaExpectedValue,
    actual: SchemaActualValue,
    /// A failed alternative is explanatory, not an independently required repair.
    alternative: bool,
    omissions: Vec<SchemaDiagnosticOmission>,
}

#[derive(Deserialize)]
struct IssueSnapshot {
    reason: SchemaFailure,
    instance_path: Option<String>,
    message: String,
    expected: SchemaExpectedValue,
    actual: SchemaActualValue,
    alternative: bool,
    omissions: Vec<SchemaDiagnosticOmission>,
}

impl SchemaIssue {
    pub const fn reason(&self) -> &SchemaFailure {
        &self.reason
    }
    pub fn instance_path(&self) -> Option<&str> {
        self.instance_path.as_deref()
    }
    pub fn message(&self) -> &str {
        &self.message
    }
    pub const fn expected(&self) -> &SchemaExpectedValue {
        &self.expected
    }
    pub const fn actual(&self) -> &SchemaActualValue {
        &self.actual
    }
    pub const fn alternative(&self) -> bool {
        self.alternative
    }
    pub fn omissions(&self) -> &[SchemaDiagnosticOmission] {
        &self.omissions
    }
}

impl TryFrom<IssueSnapshot> for SchemaIssue {
    type Error = &'static str;

    fn try_from(raw: IssueSnapshot) -> Result<Self, Self::Error> {
        let path_omitted = raw.instance_path.is_none();
        let constraint_omitted = matches!(raw.expected, SchemaExpectedValue::Omitted);
        if raw.message.is_empty()
            || raw.message.len() > 1024
            || raw.instance_path.as_ref().is_some_and(|path| {
                path.len() > MAX_PATH_BYTES || (!path.is_empty() && !path.starts_with('/'))
            })
            || matches!(&raw.expected, SchemaExpectedValue::Available(value) if value.to_string().len() > MAX_EXPECTED_BYTES)
            || raw.omissions.len()
                != 1 + usize::from(path_omitted) + usize::from(constraint_omitted)
            || !raw
                .omissions
                .contains(&SchemaDiagnosticOmission::ActualValueRedacted)
            || raw
                .omissions
                .contains(&SchemaDiagnosticOmission::InstancePathRedactedOrTooLong)
                != path_omitted
            || raw
                .omissions
                .contains(&SchemaDiagnosticOmission::SchemaConstraintUnavailableOrTooLarge)
                != constraint_omitted
        {
            return Err("Schema issue exceeds diagnostic bounds");
        }
        Ok(Self {
            reason: raw.reason,
            instance_path: raw.instance_path,
            message: raw.message,
            expected: raw.expected,
            actual: raw.actual,
            alternative: raw.alternative,
            omissions: raw.omissions,
        })
    }
}

impl SchemaDiagnosticInspection {
    /// Collect errors from validation of `value` against `document`.
    ///
    /// The iterator must come from that schema and instance. Compilation, reference
    /// policy, and validation work budgets belong to the caller. Collection stops
    /// at 32 retained issues plus one lookahead and explicitly flags omissions.
    pub fn from_errors<'a>(
        document: &Value,
        value: &Value,
        errors: impl IntoIterator<Item = ValidationError<'a>>,
    ) -> Self {
        let mut names = HashSet::new();
        declared_property_names(document, &mut names);
        let mut diagnostics = SchemaDiagnostics {
            issues: Vec::new(),
            truncated: false,
        };
        for error in errors {
            if evaluation_failed(error.kind()) {
                return SchemaDiagnosticInspection::Uninspectable;
            }
            diagnostics.push(document, value, &names, &error, false);
            if diagnostics.truncated {
                break;
            }
        }
        if diagnostics.issues.is_empty() {
            SchemaDiagnosticInspection::Valid
        } else {
            SchemaDiagnosticInspection::Invalid(diagnostics)
        }
    }
}

impl SchemaDiagnostics {
    fn push(
        &mut self,
        document: &Value,
        value: &Value,
        names: &HashSet<&str>,
        error: &ValidationError<'_>,
        alternative: bool,
    ) {
        if self.issues.len() == MAX_ISSUES {
            self.truncated = true;
            return;
        }
        let path = error.instance_path().as_str();
        let instance_path = safe_instance_path(path, value, names).then(|| path.to_owned());
        let constraint = document.pointer(error.schema_path().as_str());
        let expected = constraint
            .filter(|value| value.to_string().len() <= MAX_EXPECTED_BYTES)
            .cloned();
        let mut omissions = vec![SchemaDiagnosticOmission::ActualValueRedacted];
        if instance_path.is_none() {
            omissions.push(SchemaDiagnosticOmission::InstancePathRedactedOrTooLong);
        }
        if expected.is_none() {
            omissions.push(SchemaDiagnosticOmission::SchemaConstraintUnavailableOrTooLarge);
        }
        self.issues.push(SchemaIssue {
            reason: SchemaFailure::from_error(document, error),
            instance_path,
            message: explanation(error.kind()).into(),
            expected: expected.map_or(SchemaExpectedValue::Omitted, SchemaExpectedValue::Available),
            actual: SchemaActualValue::from(error.instance().as_ref()),
            alternative,
            omissions,
        });
        match error.kind() {
            ValidationErrorKind::AnyOf { context }
            | ValidationErrorKind::OneOfNotValid { context } => {
                for child in context.iter().flatten() {
                    self.push(document, value, names, child, true);
                    if self.truncated {
                        break;
                    }
                }
            }
            ValidationErrorKind::PropertyNames { error } => {
                self.push(document, value, names, error, alternative)
            }
            _ => {}
        }
    }
}

fn declared_property_names<'a>(schema: &'a Value, names: &mut HashSet<&'a str>) {
    match schema {
        Value::Object(map) => {
            if let Some(Value::Object(properties)) = map.get("properties") {
                names.extend(properties.keys().map(String::as_str));
            }
            for value in map.values() {
                declared_property_names(value, names);
            }
        }
        Value::Array(values) => {
            for value in values {
                declared_property_names(value, names);
            }
        }
        _ => {}
    }
}

fn safe_instance_path(path: &str, mut value: &Value, names: &HashSet<&str>) -> bool {
    if path.len() > MAX_PATH_BYTES {
        return false;
    }
    if path.is_empty() {
        return true;
    }
    for token in path[1..].split('/') {
        let key = token.replace("~1", "/").replace("~0", "~");
        value = match value {
            Value::Object(map) if names.contains(key.as_str()) => match map.get(&key) {
                Some(value) => value,
                None => return false,
            },
            Value::Array(array) => {
                match key.parse::<usize>().ok().and_then(|index| array.get(index)) {
                    Some(value) => value,
                    None => return false,
                }
            }
            _ => return false,
        };
    }
    true
}

fn explanation(kind: &ValidationErrorKind) -> &'static str {
    use ValidationErrorKind::*;
    match kind {
        Required { .. } => {
            "A required property is missing from this object; add the named property."
        }
        Type { .. } => "The value has the wrong JSON type; use the expected type.",
        Maximum { .. } => "The number exceeds the permitted maximum.",
        Minimum { .. } => "The number is below the permitted minimum.",
        ExclusiveMaximum { .. } => "The number must be strictly less than the limit.",
        ExclusiveMinimum { .. } => "The number must be strictly greater than the limit.",
        MaxLength { .. } => "The string contains too many characters.",
        MinLength { .. } => "The string contains too few characters.",
        MaxItems { .. } | AdditionalItems { .. } => {
            "The array contains more entries than permitted."
        }
        MinItems { .. } => "The array contains too few entries.",
        MaxProperties { .. } => "The object contains too many properties.",
        MinProperties { .. } => "The object contains too few properties.",
        Pattern { .. } => "The string does not match the required pattern.",
        Format { .. } => "The value does not match the required format.",
        Constant { .. } => "The value must equal the schema's constant.",
        Enum { .. } => "The value must be one of the permitted choices.",
        MultipleOf { .. } => "The number must be a multiple of the specified value.",
        UniqueItems => "The array contains duplicate entries; all entries must be unique.",
        Contains => "The array does not contain the required number of matching entries.",
        AdditionalProperties { .. } | UnevaluatedProperties { .. } => {
            "The object contains properties that the schema does not permit."
        }
        UnevaluatedItems { .. } => "The array contains entries that the schema does not permit.",
        FalseSchema => "The schema does not permit a value at this location.",
        AnyOf { .. } => {
            "The value must satisfy at least one alternative; none matched. Branch issues describe alternatives."
        }
        OneOfNotValid { .. } => {
            "The value must satisfy exactly one alternative; none matched. Branch issues describe alternatives."
        }
        OneOfMultipleValid { .. } => {
            "The value matches several alternatives but must match exactly one."
        }
        Not { .. } => "The value matches a schema that is explicitly prohibited.",
        PropertyNames { .. } => {
            "A property name does not satisfy the naming constraint; instance-owned names are redacted."
        }
        ContentEncoding { .. } | FromUtf8 { .. } => {
            "The value does not use the required content encoding."
        }
        ContentMediaType { .. } => "The value does not use the required content media type.",
        _ => "The value failed the referenced schema constraint.",
    }
}

#[cfg(test)]
mod tests;
