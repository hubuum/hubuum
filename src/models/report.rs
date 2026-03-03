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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_output_example)]
pub struct ReportOutputRequest {
    pub content_type: Option<ReportContentType>,
    pub template: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[schema(example = openapi_examples::report_request_example)]
pub struct ReportRequest {
    pub scope: ReportScope,
    pub query: Option<String>,
    pub output: Option<ReportOutputRequest>,
    pub missing_data_policy: Option<ReportMissingDataPolicy>,
    pub limits: Option<ReportLimits>,
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

    pub(super) fn report_output_example() -> ReportOutputRequest {
        ReportOutputRequest {
            content_type: Some(ReportContentType::TextPlain),
            template: Some("{{#each items}}{{this.name}}\n{{/each}}".to_string()),
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
            output: Some(report_output_example()),
            missing_data_policy: Some(ReportMissingDataPolicy::Strict),
            limits: Some(report_limits_example()),
        }
    }

    pub(super) fn report_warning_example() -> ReportWarning {
        ReportWarning {
            code: "missing_value".to_string(),
            message: "Template lookup failed".to_string(),
            path: Some("this.data.owner".to_string()),
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
}
