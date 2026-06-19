use std::collections::HashMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportScopeKind {
    Namespaces,
    Classes,
    ObjectsInClass,
    ClassRelations,
    ObjectRelations,
    RelatedObjects,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_scope_example)]
pub struct ReportScope {
    pub kind: ReportScopeKind,
    pub class_id: Option<i32>,
    pub object_id: Option<i32>,
}

impl ReportScope {
    pub fn validate(&self) -> Result<(), ApiError> {
        match self.kind {
            ReportScopeKind::Namespaces
            | ReportScopeKind::Classes
            | ReportScopeKind::ClassRelations
            | ReportScopeKind::ObjectRelations => {
                if self.class_id.is_some() || self.object_id.is_some() {
                    return Err(ApiError::BadRequest(format!(
                        "Scope '{}' does not accept class_id or object_id",
                        self.kind.as_str()
                    )));
                }
            }
            ReportScopeKind::ObjectsInClass => {
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
            ReportScopeKind::RelatedObjects => {
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
pub enum ReportContentType {
    #[serde(rename = "application/json")]
    ApplicationJson,
    #[serde(rename = "text/plain")]
    TextPlain,
    #[serde(rename = "text/html")]
    TextHtml,
    #[serde(rename = "text/csv")]
    TextCsv,
}

impl ReportContentType {
    pub fn as_mime(self) -> &'static str {
        match self {
            ReportContentType::ApplicationJson => "application/json",
            ReportContentType::TextPlain => "text/plain",
            ReportContentType::TextHtml => "text/html",
            ReportContentType::TextCsv => "text/csv",
        }
    }

    pub fn from_mime(value: &str) -> Result<Self, ApiError> {
        match value {
            "application/json" => Ok(ReportContentType::ApplicationJson),
            "text/plain" => Ok(ReportContentType::TextPlain),
            "text/html" => Ok(ReportContentType::TextHtml),
            "text/csv" => Ok(ReportContentType::TextCsv),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported report content type: '{}'",
                value
            ))),
        }
    }

    pub fn ensure_template_output(self) -> Result<Self, ApiError> {
        match self {
            ReportContentType::ApplicationJson => Err(ApiError::BadRequest(
                "Stored templates only support text/plain, text/html, and text/csv".to_string(),
            )),
            _ => Ok(self),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportMissingDataPolicy {
    Strict,
    Null,
    Omit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_limits_example)]
pub struct ReportLimits {
    pub max_items: Option<usize>,
    pub max_output_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportIncludeRelatedDirection {
    Any,
    Outgoing,
    Incoming,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReportIncludeRelatedSort {
    Path,
    Name,
    CreatedAt,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ReportIncludeRelatedObject {
    pub class_id: i32,
    pub class_relation_id: Option<i32>,
    pub direction: Option<ReportIncludeRelatedDirection>,
    pub sort: Option<ReportIncludeRelatedSort>,
    pub max_depth: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReportIncludeRelatedQuery {
    pub class_id: i32,
    pub class_relation_id: Option<i32>,
    pub direction: ReportIncludeRelatedDirection,
    pub sort: ReportIncludeRelatedSort,
    pub max_depth: i32,
    pub limit: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct ReportInclude {
    pub related_objects: Option<HashMap<String, ReportIncludeRelatedObject>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_relation_context_example)]
#[serde(deny_unknown_fields)]
pub struct ReportRelationContext {
    pub depth: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_request_example)]
pub struct ReportRequest {
    pub scope: ReportScope,
    pub query: Option<String>,
    pub missing_data_policy: Option<ReportMissingDataPolicy>,
    pub limits: Option<ReportLimits>,
    pub include: Option<ReportInclude>,
    pub relation_context: Option<ReportRelationContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_warning_example)]
pub struct ReportWarning {
    pub code: String,
    pub message: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_meta_example)]
pub struct ReportMeta {
    pub count: usize,
    pub truncated: bool,
    pub scope: ReportScope,
    pub content_type: ReportContentType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
#[schema(example = openapi_examples::report_json_response_example)]
pub struct ReportJsonResponse {
    pub items: Vec<serde_json::Value>,
    pub meta: ReportMeta,
    pub warnings: Vec<ReportWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_template_run_request_example)]
#[serde(deny_unknown_fields)]
pub struct ReportTemplateRunRequest {
    pub query: Option<String>,
    pub object_id: Option<i32>,
    pub missing_data_policy: Option<ReportMissingDataPolicy>,
    pub limits: Option<ReportLimits>,
}

impl ReportScopeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ReportScopeKind::Namespaces => "namespaces",
            ReportScopeKind::Classes => "classes",
            ReportScopeKind::ObjectsInClass => "objects_in_class",
            ReportScopeKind::ClassRelations => "class_relations",
            ReportScopeKind::ObjectRelations => "object_relations",
            ReportScopeKind::RelatedObjects => "related_objects",
        }
    }
}

impl FromStr for ReportScopeKind {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "namespaces" => Ok(Self::Namespaces),
            "classes" => Ok(Self::Classes),
            "objects_in_class" => Ok(Self::ObjectsInClass),
            "class_relations" => Ok(Self::ClassRelations),
            "object_relations" => Ok(Self::ObjectRelations),
            "related_objects" => Ok(Self::RelatedObjects),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported report scope kind: '{value}'"
            ))),
        }
    }
}

impl ReportMissingDataPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::Null => "null",
            Self::Omit => "omit",
        }
    }
}

impl FromStr for ReportMissingDataPolicy {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "strict" => Ok(Self::Strict),
            "null" => Ok(Self::Null),
            "omit" => Ok(Self::Omit),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported report missing data policy: '{value}'"
            ))),
        }
    }
}

// Used by utoipa's `#[schema(example = ...)]` hooks to populate the generated OpenAPI examples.
// The compiler does not see those macro references as normal function calls.
#[allow(dead_code)]
mod openapi_examples {
    use super::*;

    pub(super) fn report_scope_example() -> ReportScope {
        ReportScope {
            kind: ReportScopeKind::ObjectsInClass,
            class_id: Some(42),
            object_id: None,
        }
    }

    pub(super) fn report_limits_example() -> ReportLimits {
        ReportLimits {
            max_items: Some(100),
            max_output_bytes: Some(262_144),
        }
    }

    pub(super) fn report_request_example() -> ReportRequest {
        ReportRequest {
            scope: report_scope_example(),
            query: Some("name__icontains=server&sort=name".to_string()),
            missing_data_policy: Some(ReportMissingDataPolicy::Strict),
            limits: Some(report_limits_example()),
            include: None,
            relation_context: None,
        }
    }

    pub(super) fn report_relation_context_example() -> ReportRelationContext {
        ReportRelationContext { depth: Some(2) }
    }

    pub(super) fn report_warning_example() -> ReportWarning {
        ReportWarning {
            code: "missing_value".to_string(),
            message: "Template lookup failed".to_string(),
            path: Some("item.data.owner".to_string()),
        }
    }

    pub(super) fn report_meta_example() -> ReportMeta {
        ReportMeta {
            count: 2,
            truncated: false,
            scope: report_scope_example(),
            content_type: ReportContentType::ApplicationJson,
        }
    }

    pub(super) fn report_json_response_example() -> ReportJsonResponse {
        ReportJsonResponse {
            items: vec![
                serde_json::json!({"id": 1, "name": "srv-01"}),
                serde_json::json!({"id": 2, "name": "srv-02"}),
            ],
            meta: report_meta_example(),
            warnings: vec![],
        }
    }

    pub(super) fn report_template_run_request_example() -> ReportTemplateRunRequest {
        ReportTemplateRunRequest {
            query: Some("name__icontains=server&sort=name".to_string()),
            object_id: None,
            missing_data_policy: Some(ReportMissingDataPolicy::Strict),
            limits: Some(report_limits_example()),
        }
    }
}
