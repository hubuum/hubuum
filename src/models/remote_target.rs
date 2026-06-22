use std::str::FromStr;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::{remote_call_results, remote_targets};

crate::int_id_newtype! {
    /// Identifier wrapper for a remote target.
    pub struct RemoteTargetID;
    noun = "remote target id";
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteHttpMethod {
    Get,
    Post,
    Patch,
    Delete,
}

impl RemoteHttpMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Patch => "patch",
            Self::Delete => "delete",
        }
    }
}

impl FromStr for RemoteHttpMethod {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "get" => Ok(Self::Get),
            "post" => Ok(Self::Post),
            "patch" => Ok(Self::Patch),
            "delete" => Ok(Self::Delete),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported remote HTTP method: '{value}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteAuthConfig {
    #[default]
    None,
    BearerSecret {
        secret: String,
    },
    BasicSecret {
        username: String,
        secret: String,
    },
    ApiKeySecret {
        header: String,
        secret: String,
    },
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = remote_targets)]
pub(crate) struct RemoteTargetRow {
    pub id: i32,
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RemoteTarget {
    pub id: i32,
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub method: RemoteHttpMethod,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: RemoteAuthConfig,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewRemoteTarget {
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub method: RemoteHttpMethod,
    pub url_template: String,
    #[serde(default = "empty_json_object")]
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    #[serde(default)]
    pub auth_config: RemoteAuthConfig,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: i32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateRemoteTarget {
    pub namespace_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub method: Option<RemoteHttpMethod>,
    pub url_template: Option<String>,
    pub headers_template: Option<serde_json::Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<String>)]
    pub body_template: Option<Option<String>>,
    pub auth_config: Option<RemoteAuthConfig>,
    pub timeout_ms: Option<i32>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = remote_targets)]
pub(crate) struct NewRemoteTargetRow {
    pub namespace_id: i32,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = remote_targets)]
pub(crate) struct UpdateRemoteTargetRow {
    pub namespace_id: Option<i32>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub method: Option<String>,
    pub url_template: Option<String>,
    pub headers_template: Option<serde_json::Value>,
    pub body_template: Option<Option<String>>,
    pub auth_config: Option<serde_json::Value>,
    pub timeout_ms: Option<i32>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct RemoteTargetInvokeRequest {
    #[serde(default)]
    pub parameters: serde_json::Value,
    #[serde(default)]
    pub body_override: serde_json::Value,
}

impl Default for RemoteTargetInvokeRequest {
    fn default() -> Self {
        Self {
            parameters: serde_json::json!({}),
            body_override: serde_json::json!({}),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRemoteCallTaskPayload {
    pub target_id: i32,
    pub class_id: i32,
    pub object_id: i32,
    pub parameters: serde_json::Value,
    pub body_override: serde_json::Value,
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize, Deserialize, PartialEq, ToSchema)]
#[diesel(table_name = remote_call_results)]
pub struct RemoteCallResult {
    pub id: i32,
    pub task_id: i32,
    pub target_id: Option<i32>,
    pub object_id: Option<i32>,
    pub method: String,
    pub rendered_url: String,
    pub response_status: Option<i32>,
    pub response_headers: Option<serde_json::Value>,
    pub response_body_preview: Option<String>,
    pub duration_ms: i32,
    pub success: bool,
    pub error: Option<String>,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = remote_call_results)]
pub struct NewRemoteCallResult {
    pub task_id: i32,
    pub target_id: Option<i32>,
    pub object_id: Option<i32>,
    pub method: String,
    pub rendered_url: String,
    pub response_status: Option<i32>,
    pub response_headers: Option<serde_json::Value>,
    pub response_body_preview: Option<String>,
    pub duration_ms: i32,
    pub success: bool,
    pub error: Option<String>,
}

impl TryFrom<RemoteTargetRow> for RemoteTarget {
    type Error = ApiError;

    fn try_from(row: RemoteTargetRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            namespace_id: row.namespace_id,
            name: row.name,
            description: row.description,
            method: RemoteHttpMethod::from_str(&row.method)?,
            url_template: row.url_template,
            headers_template: row.headers_template,
            body_template: row.body_template,
            auth_config: serde_json::from_value(row.auth_config)?,
            timeout_ms: row.timeout_ms,
            enabled: row.enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl NewRemoteTarget {
    pub(crate) fn into_row(self) -> Result<NewRemoteTargetRow, ApiError> {
        validate_target_parts(
            &self.url_template,
            &self.headers_template,
            self.body_template.as_deref(),
            &self.auth_config,
            self.timeout_ms,
        )?;

        Ok(NewRemoteTargetRow {
            namespace_id: self.namespace_id,
            name: self.name,
            description: self.description,
            method: self.method.as_str().to_string(),
            url_template: self.url_template,
            headers_template: self.headers_template,
            body_template: self.body_template,
            auth_config: serde_json::to_value(self.auth_config)?,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
        })
    }
}

impl UpdateRemoteTarget {
    pub fn is_empty(&self) -> bool {
        self.namespace_id.is_none()
            && self.name.is_none()
            && self.description.is_none()
            && self.method.is_none()
            && self.url_template.is_none()
            && self.headers_template.is_none()
            && self.body_template.is_none()
            && self.auth_config.is_none()
            && self.timeout_ms.is_none()
            && self.enabled.is_none()
    }

    pub(crate) fn into_row(
        self,
        existing: &RemoteTarget,
    ) -> Result<UpdateRemoteTargetRow, ApiError> {
        let url_template = self
            .url_template
            .clone()
            .unwrap_or_else(|| existing.url_template.clone());
        let headers_template = self
            .headers_template
            .clone()
            .unwrap_or_else(|| existing.headers_template.clone());
        let body_template = self
            .body_template
            .clone()
            .unwrap_or_else(|| existing.body_template.clone());
        let auth_config = self
            .auth_config
            .clone()
            .unwrap_or_else(|| existing.auth_config.clone());
        let timeout_ms = self.timeout_ms.unwrap_or(existing.timeout_ms);

        validate_target_parts(
            &url_template,
            &headers_template,
            body_template.as_deref(),
            &auth_config,
            timeout_ms,
        )?;

        Ok(UpdateRemoteTargetRow {
            namespace_id: self.namespace_id,
            name: self.name,
            description: self.description,
            method: self.method.map(|method| method.as_str().to_string()),
            url_template: self.url_template,
            headers_template: self.headers_template,
            body_template: self.body_template,
            auth_config: self.auth_config.map(serde_json::to_value).transpose()?,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
        })
    }
}

pub fn validate_target_parts(
    url_template: &str,
    headers_template: &serde_json::Value,
    body_template: Option<&str>,
    auth_config: &RemoteAuthConfig,
    timeout_ms: i32,
) -> Result<(), ApiError> {
    if timeout_ms <= 0 {
        return Err(ApiError::BadRequest(
            "timeout_ms must be greater than 0".to_string(),
        ));
    }
    if !headers_template.is_object() {
        return Err(ApiError::BadRequest(
            "headers_template must be a JSON object".to_string(),
        ));
    }
    validate_template("url_template", url_template)?;
    if let Some(body_template) = body_template {
        validate_template("body_template", body_template)?;
    }
    validate_header_templates(headers_template)?;
    validate_auth_config(auth_config)?;
    Ok(())
}

pub fn validate_rendered_remote_url(url: &str) -> Result<(), ApiError> {
    if !url.starts_with("https://") {
        return Err(ApiError::BadRequest(
            "remote target URLs must use https".to_string(),
        ));
    }
    if url.trim() != url || url.len() <= "https://".len() {
        return Err(ApiError::BadRequest(
            "remote target URL is invalid".to_string(),
        ));
    }
    Ok(())
}

fn validate_header_templates(value: &serde_json::Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(|| {
        ApiError::BadRequest("headers_template must be a JSON object".to_string())
    })?;
    for (name, value) in object {
        if name.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "header names must not be empty".to_string(),
            ));
        }
        match value {
            serde_json::Value::String(template) => validate_template("header template", template)?,
            _ => {
                return Err(ApiError::BadRequest(
                    "header template values must be strings".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_auth_config(auth_config: &RemoteAuthConfig) -> Result<(), ApiError> {
    let valid_secret = |secret: &str| {
        !secret.trim().is_empty()
            && secret
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    };
    match auth_config {
        RemoteAuthConfig::None => Ok(()),
        RemoteAuthConfig::BearerSecret { secret }
        | RemoteAuthConfig::BasicSecret { secret, .. }
        | RemoteAuthConfig::ApiKeySecret { secret, .. } => {
            if valid_secret(secret) {
                Ok(())
            } else {
                Err(ApiError::BadRequest(
                    "remote auth secret references must contain only letters, numbers, and underscores"
                        .to_string(),
                ))
            }
        }
    }
}

fn validate_template(label: &str, source: &str) -> Result<(), ApiError> {
    minijinja::Environment::new()
        .template_from_str(source)
        .map(|_| ())
        .map_err(|error| ApiError::BadRequest(format!("Invalid {label}: {error}")))
}

fn deserialize_double_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

fn empty_json_object() -> serde_json::Value {
    serde_json::json!({})
}

fn default_timeout_ms() -> i32 {
    10_000
}

fn default_enabled() -> bool {
    true
}

impl CursorPaginated for RemoteTarget {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Description
                | FilterField::NamespaceId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::Description => Ok(CursorValue::String(self.description.clone())),
            FilterField::NamespaceId => Ok(CursorValue::Integer(self.namespace_id as i64)),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::UpdatedAt => Ok(CursorValue::DateTime(self.updated_at)),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported sort field '{}' for remote targets",
                field
            ))),
        }
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::Id,
            descending: false,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}

impl CursorSqlMapping for RemoteTarget {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "remote_targets.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "remote_targets.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "remote_targets.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::NamespaceId => CursorSqlField {
                column: "remote_targets.namespace_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "remote_targets.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "remote_targets.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for remote targets",
                    field
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn remote_http_method_parses_supported_methods() {
        assert_eq!(
            RemoteHttpMethod::from_str("get").unwrap(),
            RemoteHttpMethod::Get
        );
        assert_eq!(
            RemoteHttpMethod::from_str("post").unwrap(),
            RemoteHttpMethod::Post
        );
        assert!(RemoteHttpMethod::from_str("put").is_err());
    }

    #[test]
    fn rendered_remote_urls_must_be_https() {
        assert!(validate_rendered_remote_url("https://example.com/hook").is_ok());
        assert!(validate_rendered_remote_url("http://example.com/hook").is_err());
        assert!(validate_rendered_remote_url("https://").is_err());
    }

    #[test]
    fn target_parts_validate_templates_and_auth_references() {
        assert!(
            validate_target_parts(
                "https://example.com/{{ object.id }}",
                &serde_json::json!({ "X-Object": "{{ object.name }}" }),
                Some("{\"id\": {{ object.id }}}"),
                &RemoteAuthConfig::BearerSecret {
                    secret: "servicenow_token".to_string(),
                },
                1000,
            )
            .is_ok()
        );

        assert!(
            validate_target_parts(
                "https://example.com/{{",
                &serde_json::json!({}),
                None,
                &RemoteAuthConfig::None,
                1000,
            )
            .is_err()
        );
        assert!(
            validate_target_parts(
                "https://example.com",
                &serde_json::json!([]),
                None,
                &RemoteAuthConfig::None,
                1000,
            )
            .is_err()
        );
        assert!(
            validate_target_parts(
                "https://example.com",
                &serde_json::json!({}),
                None,
                &RemoteAuthConfig::ApiKeySecret {
                    header: "X-API-Key".to_string(),
                    secret: "bad-secret".to_string(),
                },
                1000,
            )
            .is_err()
        );
    }
}
