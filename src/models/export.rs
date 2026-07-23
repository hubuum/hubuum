use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::{HubuumClassID, HubuumObjectID};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportScopeKind {
    Collections,
    Classes,
    ObjectsInClass,
    ClassRelations,
    ObjectRelations,
    RelatedObjects,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_scope_example)]
pub struct ExportScope {
    pub kind: ExportScopeKind,
    #[schema(minimum = 1)]
    pub class_id: Option<i32>,
    #[schema(minimum = 1)]
    pub object_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidatedExportScope {
    Collections,
    Classes,
    ObjectsInClass(HubuumClassID),
    ClassRelations,
    ObjectRelations,
    RelatedObjects {
        class_id: HubuumClassID,
        object_id: HubuumObjectID,
    },
}

impl ValidatedExportScope {
    pub fn kind(self) -> ExportScopeKind {
        match self {
            Self::Collections => ExportScopeKind::Collections,
            Self::Classes => ExportScopeKind::Classes,
            Self::ObjectsInClass(_) => ExportScopeKind::ObjectsInClass,
            Self::ClassRelations => ExportScopeKind::ClassRelations,
            Self::ObjectRelations => ExportScopeKind::ObjectRelations,
            Self::RelatedObjects { .. } => ExportScopeKind::RelatedObjects,
        }
    }
}

impl ExportScope {
    pub fn validate(&self) -> Result<ValidatedExportScope, ApiError> {
        match self.kind {
            ExportScopeKind::Collections => {
                self.reject_ids()?;
                Ok(ValidatedExportScope::Collections)
            }
            ExportScopeKind::Classes => {
                self.reject_ids()?;
                Ok(ValidatedExportScope::Classes)
            }
            ExportScopeKind::ObjectsInClass => {
                let class_id = self.class_id.ok_or_else(|| {
                    ApiError::BadRequest("Scope 'objects_in_class' requires class_id".to_string())
                })?;
                if self.object_id.is_some() {
                    return Err(ApiError::BadRequest(
                        "Scope 'objects_in_class' does not accept object_id".to_string(),
                    ));
                }
                Ok(ValidatedExportScope::ObjectsInClass(HubuumClassID::new(
                    class_id,
                )?))
            }
            ExportScopeKind::ClassRelations => {
                self.reject_ids()?;
                Ok(ValidatedExportScope::ClassRelations)
            }
            ExportScopeKind::ObjectRelations => {
                self.reject_ids()?;
                Ok(ValidatedExportScope::ObjectRelations)
            }
            ExportScopeKind::RelatedObjects => {
                let (Some(class_id), Some(object_id)) = (self.class_id, self.object_id) else {
                    return Err(ApiError::BadRequest(
                        "Scope 'related_objects' requires both class_id and object_id".to_string(),
                    ));
                };
                Ok(ValidatedExportScope::RelatedObjects {
                    class_id: HubuumClassID::new(class_id)?,
                    object_id: HubuumObjectID::new(object_id)?,
                })
            }
        }
    }

    fn reject_ids(&self) -> Result<(), ApiError> {
        if self.class_id.is_some() || self.object_id.is_some() {
            return Err(ApiError::BadRequest(format!(
                "Scope '{}' does not accept class_id or object_id",
                self.kind.as_str()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
pub enum ExportContentType {
    #[serde(rename = "application/json")]
    ApplicationJson,
    #[serde(rename = "text/plain")]
    TextPlain,
    #[serde(rename = "text/html")]
    TextHtml,
    #[serde(rename = "text/csv")]
    TextCsv,
}

impl ExportContentType {
    pub fn as_mime(self) -> &'static str {
        match self {
            ExportContentType::ApplicationJson => "application/json",
            ExportContentType::TextPlain => "text/plain",
            ExportContentType::TextHtml => "text/html",
            ExportContentType::TextCsv => "text/csv",
        }
    }

    pub fn from_mime(value: &str) -> Result<Self, ApiError> {
        match value {
            "application/json" => Ok(ExportContentType::ApplicationJson),
            "text/plain" => Ok(ExportContentType::TextPlain),
            "text/html" => Ok(ExportContentType::TextHtml),
            "text/csv" => Ok(ExportContentType::TextCsv),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported export content type: '{}'",
                value
            ))),
        }
    }

    pub fn ensure_template_output(self) -> Result<Self, ApiError> {
        match self {
            ExportContentType::ApplicationJson => Err(ApiError::BadRequest(
                "Stored templates only support text/plain, text/html, and text/csv".to_string(),
            )),
            _ => Ok(self),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportMissingDataPolicy {
    Strict,
    Null,
    Omit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_limits_example)]
pub struct ExportLimits {
    pub max_items: Option<usize>,
    pub max_output_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportIncludeRelatedDirection {
    Any,
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExportIncludeRelatedSort {
    Path,
    Name,
    CreatedAt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ExportIncludeRelatedObject {
    pub class_id: i32,
    pub class_relation_id: Option<i32>,
    pub direction: Option<ExportIncludeRelatedDirection>,
    pub sort: Option<ExportIncludeRelatedSort>,
    pub max_depth: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportIncludeRelatedQuery {
    pub class_id: i32,
    pub class_relation_id: Option<i32>,
    pub direction: ExportIncludeRelatedDirection,
    pub sort: ExportIncludeRelatedSort,
    pub max_depth: i32,
    pub limit: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ExportInclude {
    pub related_objects: Option<HashMap<String, ExportIncludeRelatedObject>>,
}

/// Bounds for `include.related_objects` hydration, shared by ad-hoc export requests
/// (`POST /api/v1/exports`) and stored executable export templates so the two paths cannot drift.
pub const RELATED_INCLUDE_DEFAULT_MAX_DEPTH: i32 = 1;
pub const RELATED_INCLUDE_MAX_DEPTH_LIMIT: i32 = 10;
pub const RELATED_INCLUDE_DEFAULT_LIMIT: i32 = 1;
pub const RELATED_INCLUDE_MAX_LIMIT: i32 = 50;
pub const RELATED_INCLUDE_MAX_ALIASES: usize = 8;

impl ExportInclude {
    /// Validate the `related_objects` block: alias count, alias syntax, and per-alias option
    /// bounds. Callers enforce scope-specific rules (e.g. that `include` is only valid for
    /// `objects_in_class`).
    pub fn validate_related_objects(&self) -> Result<(), ApiError> {
        let Some(related_objects) = &self.related_objects else {
            return Ok(());
        };

        if related_objects.len() > RELATED_INCLUDE_MAX_ALIASES {
            return Err(ApiError::BadRequest(format!(
                "include.related_objects supports at most {RELATED_INCLUDE_MAX_ALIASES} aliases"
            )));
        }

        for (alias, include) in related_objects {
            validate_related_include_alias(alias)?;
            include.validate(alias)?;
        }

        Ok(())
    }
}

impl ExportIncludeRelatedObject {
    fn validate(&self, alias: &str) -> Result<(), ApiError> {
        if self.class_id <= 0 {
            return Err(ApiError::BadRequest(format!(
                "include.related_objects.{alias}.class_id must be greater than 0"
            )));
        }

        if let Some(class_relation_id) = self.class_relation_id
            && class_relation_id <= 0
        {
            return Err(ApiError::BadRequest(format!(
                "include.related_objects.{alias}.class_relation_id must be greater than 0"
            )));
        }

        let max_depth = self.max_depth.unwrap_or(RELATED_INCLUDE_DEFAULT_MAX_DEPTH);
        if !(1..=RELATED_INCLUDE_MAX_DEPTH_LIMIT).contains(&max_depth) {
            return Err(ApiError::BadRequest(format!(
                "include.related_objects.{alias}.max_depth must be between 1 and {RELATED_INCLUDE_MAX_DEPTH_LIMIT}"
            )));
        }

        let limit = self.limit.unwrap_or(RELATED_INCLUDE_DEFAULT_LIMIT);
        if !(1..=RELATED_INCLUDE_MAX_LIMIT).contains(&limit) {
            return Err(ApiError::BadRequest(format!(
                "include.related_objects.{alias}.limit must be between 1 and {RELATED_INCLUDE_MAX_LIMIT}"
            )));
        }

        Ok(())
    }
}

fn validate_related_include_alias(alias: &str) -> Result<(), ApiError> {
    let mut chars = alias.chars();
    let Some(first) = chars.next() else {
        return Err(ApiError::BadRequest(
            "include.related_objects aliases must not be empty".to_string(),
        ));
    };

    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return Err(ApiError::BadRequest(format!(
            "Invalid include.related_objects alias '{alias}'; expected [A-Za-z_][A-Za-z0-9_]*"
        )));
    }

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_relation_context_example)]
#[serde(deny_unknown_fields)]
pub struct ExportRelationContext {
    pub depth: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_request_example)]
pub struct ExportRequest {
    pub scope: ExportScope,
    pub query: Option<String>,
    pub missing_data_policy: Option<ExportMissingDataPolicy>,
    pub limits: Option<ExportLimits>,
    pub include: Option<ExportInclude>,
    pub relation_context: Option<ExportRelationContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_warning_example)]
pub struct ExportWarning {
    pub code: String,
    pub message: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_meta_example)]
pub struct ExportMeta {
    pub count: usize,
    pub truncated: bool,
    pub scope: ExportScope,
    pub content_type: ExportContentType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[schema(example = openapi_examples::export_json_response_example)]
pub struct ExportJsonResponse {
    pub items: Vec<serde_json::Value>,
    pub meta: ExportMeta,
    pub warnings: Vec<ExportWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::export_template_run_request_example)]
#[serde(deny_unknown_fields)]
pub struct ExportTemplateRunRequest {
    pub query: Option<String>,
    pub object_id: Option<i32>,
    pub missing_data_policy: Option<ExportMissingDataPolicy>,
    pub limits: Option<ExportLimits>,
}

impl ExportScopeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ExportScopeKind::Collections => "collections",
            ExportScopeKind::Classes => "classes",
            ExportScopeKind::ObjectsInClass => "objects_in_class",
            ExportScopeKind::ClassRelations => "class_relations",
            ExportScopeKind::ObjectRelations => "object_relations",
            ExportScopeKind::RelatedObjects => "related_objects",
        }
    }

    /// Whether this scope targets a single class and therefore needs a `class_id`.
    /// The collection scopes (`collections`, `classes`, `class_relations`,
    /// `object_relations`) are class-agnostic.
    pub fn requires_class_id(self) -> bool {
        matches!(self, Self::ObjectsInClass | Self::RelatedObjects)
    }
}

impl FromStr for ExportScopeKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "collections" => Ok(Self::Collections),
            "classes" => Ok(Self::Classes),
            "objects_in_class" => Ok(Self::ObjectsInClass),
            "class_relations" => Ok(Self::ClassRelations),
            "object_relations" => Ok(Self::ObjectRelations),
            "related_objects" => Ok(Self::RelatedObjects),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported export scope kind: '{value}'"
            ))),
        }
    }
}

impl ExportMissingDataPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Null => "null",
            Self::Omit => "omit",
        }
    }
}

impl FromStr for ExportMissingDataPolicy {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "strict" => Ok(Self::Strict),
            "null" => Ok(Self::Null),
            "omit" => Ok(Self::Omit),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported export missing data policy: '{value}'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::collections(
        ExportScopeKind::Collections,
        None,
        None,
        ValidatedExportScope::Collections
    )]
    #[case::objects_in_class(
        ExportScopeKind::ObjectsInClass,
        Some(11),
        None,
        ValidatedExportScope::ObjectsInClass(HubuumClassID::new(11).unwrap())
    )]
    #[case::related_objects(
        ExportScopeKind::RelatedObjects,
        Some(11),
        Some(22),
        ValidatedExportScope::RelatedObjects {
            class_id: HubuumClassID::new(11).unwrap(),
            object_id: HubuumObjectID::new(22).unwrap(),
        }
    )]
    fn scope_validation_returns_typed_domain_state(
        #[case] kind: ExportScopeKind,
        #[case] class_id: Option<i32>,
        #[case] object_id: Option<i32>,
        #[case] expected: ValidatedExportScope,
    ) {
        let scope = ExportScope {
            kind,
            class_id,
            object_id,
        };

        assert_eq!(scope.validate().unwrap(), expected);
    }

    #[rstest]
    #[case::ids_on_collection(
        ExportScopeKind::Collections,
        Some(1),
        None,
        "Scope 'collections' does not accept class_id or object_id"
    )]
    #[case::missing_class(
        ExportScopeKind::ObjectsInClass,
        None,
        None,
        "Scope 'objects_in_class' requires class_id"
    )]
    #[case::extra_object(
        ExportScopeKind::ObjectsInClass,
        Some(1),
        Some(2),
        "Scope 'objects_in_class' does not accept object_id"
    )]
    #[case::incomplete_relation(
        ExportScopeKind::RelatedObjects,
        Some(1),
        None,
        "Scope 'related_objects' requires both class_id and object_id"
    )]
    #[case::invalid_class(
        ExportScopeKind::ObjectsInClass,
        Some(0),
        None,
        "Invalid class id '0': must be a positive integer"
    )]
    #[case::invalid_object(
        ExportScopeKind::RelatedObjects,
        Some(1),
        Some(-1),
        "Invalid object id '-1': must be a positive integer"
    )]
    fn scope_validation_rejects_invalid_domain_state(
        #[case] kind: ExportScopeKind,
        #[case] class_id: Option<i32>,
        #[case] object_id: Option<i32>,
        #[case] expected_message: &str,
    ) {
        let scope = ExportScope {
            kind,
            class_id,
            object_id,
        };

        match scope.validate().unwrap_err() {
            ApiError::BadRequest(message) => assert_eq!(message, expected_message),
            error => panic!("unexpected error: {error:?}"),
        }
    }
}

// Used by utoipa's `#[schema(example = ...)]` hooks to populate the generated OpenAPI examples.
// The compiler does not see those macro references as normal function calls.
mod openapi_examples {
    use super::*;

    pub(super) fn export_scope_example() -> ExportScope {
        ExportScope {
            kind: ExportScopeKind::ObjectsInClass,
            class_id: Some(42),
            object_id: None,
        }
    }

    pub(super) fn export_limits_example() -> ExportLimits {
        ExportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }
    }

    pub(super) fn export_request_example() -> ExportRequest {
        ExportRequest {
            scope: export_scope_example(),
            query: Some("name__icontains=server&sort=name".to_string()),
            missing_data_policy: Some(ExportMissingDataPolicy::Strict),
            limits: Some(export_limits_example()),
            include: None,
            relation_context: None,
        }
    }

    pub(super) fn export_relation_context_example() -> ExportRelationContext {
        ExportRelationContext { depth: Some(2) }
    }

    pub(super) fn export_warning_example() -> ExportWarning {
        ExportWarning {
            code: "missing_value".to_string(),
            message: "Template lookup failed".to_string(),
            path: Some("item.data.owner".to_string()),
        }
    }

    pub(super) fn export_meta_example() -> ExportMeta {
        ExportMeta {
            count: 2,
            truncated: false,
            scope: export_scope_example(),
            content_type: ExportContentType::ApplicationJson,
        }
    }

    pub(super) fn export_json_response_example() -> ExportJsonResponse {
        ExportJsonResponse {
            items: vec![
                serde_json::json!({"id": 1, "name": "srv-01"}),
                serde_json::json!({"id": 2, "name": "srv-02"}),
            ],
            meta: export_meta_example(),
            warnings: vec![],
        }
    }

    pub(super) fn export_template_run_request_example() -> ExportTemplateRunRequest {
        ExportTemplateRunRequest {
            query: Some("name__icontains=server&sort=name".to_string()),
            object_id: None,
            missing_data_policy: Some(ExportMissingDataPolicy::Strict),
            limits: Some(export_limits_example()),
        }
    }
}
