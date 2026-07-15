use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;

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
    pub class_id: Option<i32>,
    pub object_id: Option<i32>,
}

impl ExportScope {
    pub fn validate(&self) -> Result<(), ApiError> {
        match self.kind {
            ExportScopeKind::Collections
            | ExportScopeKind::Classes
            | ExportScopeKind::ClassRelations
            | ExportScopeKind::ObjectRelations => {
                if self.class_id.is_some() || self.object_id.is_some() {
                    return Err(ApiError::BadRequest(format!(
                        "Scope '{}' does not accept class_id or object_id",
                        self.kind.as_str()
                    )));
                }
            }
            ExportScopeKind::ObjectsInClass => {
                if self.class_id.is_none() {
                    return Err(ApiError::BadRequest(
                        "Scope 'objects_in_class' requires class_id".to_string(),
                    ));
                }
                if self.object_id.is_some() {
                    return Err(ApiError::BadRequest(
                        "Scope 'objects_in_class' does not accept object_id".to_string(),
                    ));
                }
            }
            ExportScopeKind::RelatedObjects => {
                if self.class_id.is_none() || self.object_id.is_none() {
                    return Err(ApiError::BadRequest(
                        "Scope 'related_objects' requires both class_id and object_id".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub fn class_id_required(&self) -> Result<i32, ApiError> {
        self.class_id.ok_or_else(|| {
            ApiError::BadRequest(format!("Scope '{}' requires class_id", self.kind.as_str()))
        })
    }

    pub fn object_id_required(&self) -> Result<i32, ApiError> {
        self.object_id.ok_or_else(|| {
            ApiError::BadRequest(format!("Scope '{}' requires object_id", self.kind.as_str()))
        })
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
