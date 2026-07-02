use std::str::FromStr;

use chrono::NaiveDateTime;
use diesel::prelude::*;
use hubuum_templates::prepare_template;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::config::{
    DEFAULT_REPORT_TEMPLATE_FUEL, DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT, get_config,
};
use crate::db::DbPool;
use crate::db::traits::UserPermissions;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    HubuumClassID, HubuumClassRelationID, HubuumObjectID, HubuumObjectRelationID, Namespace,
    NamespaceID, Permissions,
};
use crate::pagination::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};
use crate::schema::{remote_call_results, remote_targets};
use crate::traits::{ClassAccessors, NamespaceAccessors, ObjectAccessors, SelfAccessors};

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteTargetSubjectType {
    Namespace,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
}

impl RemoteTargetSubjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Namespace => "namespace",
            Self::Class => "class",
            Self::Object => "object",
            Self::ClassRelation => "class_relation",
            Self::ObjectRelation => "object_relation",
        }
    }
}

impl FromStr for RemoteTargetSubjectType {
    type Err = ApiError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "namespace" => Ok(Self::Namespace),
            "class" => Ok(Self::Class),
            "object" => Ok(Self::Object),
            "class_relation" => Ok(Self::ClassRelation),
            "object_relation" => Ok(Self::ObjectRelation),
            _ => Err(ApiError::BadRequest(format!(
                "Unsupported remote target subject type: '{value}'"
            ))),
        }
    }
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
    pub class_id: Option<i32>,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub allowed_subject_types: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl RemoteTargetRow {
    pub(crate) fn audit_snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "namespace_id": self.namespace_id,
            "class_id": self.class_id,
            "name": self.name,
            "description": self.description,
            "method": self.method,
            "url_template": self.url_template,
            "headers_template": self.headers_template,
            "body_template": self.body_template,
            "auth_config": "<redacted>",
            "allowed_subject_types": self.allowed_subject_types,
            "timeout_ms": self.timeout_ms,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RemoteTarget {
    pub id: i32,
    pub namespace_id: i32,
    pub class_id: Option<i32>,
    pub name: String,
    pub description: String,
    pub method: RemoteHttpMethod,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: RemoteAuthConfig,
    pub allowed_subject_types: Vec<RemoteTargetSubjectType>,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewRemoteTarget {
    pub namespace_id: NamespaceID,
    pub class_id: Option<HubuumClassID>,
    pub name: String,
    pub description: String,
    pub method: RemoteHttpMethod,
    pub url_template: String,
    #[serde(default = "empty_json_object")]
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    #[serde(default)]
    pub auth_config: RemoteAuthConfig,
    pub allowed_subject_types: Vec<RemoteTargetSubjectType>,
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: i32,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateRemoteTarget {
    pub namespace_id: Option<NamespaceID>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_double_option"
    )]
    #[schema(value_type = Option<i32>)]
    pub class_id: Option<Option<HubuumClassID>>,
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
    pub allowed_subject_types: Option<Vec<RemoteTargetSubjectType>>,
    pub timeout_ms: Option<i32>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = remote_targets)]
pub(crate) struct NewRemoteTargetRow {
    pub namespace_id: i32,
    pub class_id: Option<i32>,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub allowed_subject_types: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
}

#[derive(Debug, Clone, AsChangeset)]
#[diesel(table_name = remote_targets)]
pub(crate) struct UpdateRemoteTargetRow {
    pub namespace_id: Option<i32>,
    pub class_id: Option<Option<i32>>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub method: Option<String>,
    pub url_template: Option<String>,
    pub headers_template: Option<serde_json::Value>,
    pub body_template: Option<Option<String>>,
    pub auth_config: Option<serde_json::Value>,
    pub allowed_subject_types: Option<serde_json::Value>,
    pub timeout_ms: Option<i32>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct RemoteTargetInvokeRequest {
    pub subject: RemoteInvocationSubject,
    #[serde(default)]
    pub parameters: RemoteInvocationParameters,
    #[serde(default)]
    pub body_override: RemoteInvocationBodyOverride,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
pub struct RemoteInvocationParameters(serde_json::Value);

impl RemoteInvocationParameters {
    pub fn new(value: serde_json::Value) -> Result<Self, ApiError> {
        if value.is_object() {
            Ok(Self(value))
        } else {
            Err(ApiError::BadRequest(
                "parameters must be a JSON object".to_string(),
            ))
        }
    }

    pub fn into_value(self) -> serde_json::Value {
        self.0
    }
}

impl Default for RemoteInvocationParameters {
    fn default() -> Self {
        Self(serde_json::json!({}))
    }
}

impl<'de> Deserialize<'de> for RemoteInvocationParameters {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
pub struct RemoteInvocationBodyOverride(serde_json::Value);

impl RemoteInvocationBodyOverride {
    pub fn new(value: serde_json::Value) -> Result<Self, ApiError> {
        if value.is_object() {
            Ok(Self(value))
        } else {
            Err(ApiError::BadRequest(
                "body_override must be a JSON object".to_string(),
            ))
        }
    }

    pub fn into_value(self) -> serde_json::Value {
        self.0
    }
}

impl Default for RemoteInvocationBodyOverride {
    fn default() -> Self {
        Self(serde_json::json!({}))
    }
}

impl<'de> Deserialize<'de> for RemoteInvocationBodyOverride {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteInvocationSubject {
    Namespace {
        namespace_id: NamespaceID,
    },
    Class {
        class_id: HubuumClassID,
    },
    Object {
        class_id: HubuumClassID,
        object_id: HubuumObjectID,
    },
    ClassRelation {
        relation_id: HubuumClassRelationID,
    },
    ObjectRelation {
        relation_id: HubuumObjectRelationID,
    },
}

impl RemoteInvocationSubject {
    pub fn subject_type(&self) -> RemoteTargetSubjectType {
        match self {
            Self::Namespace { .. } => RemoteTargetSubjectType::Namespace,
            Self::Class { .. } => RemoteTargetSubjectType::Class,
            Self::Object { .. } => RemoteTargetSubjectType::Object,
            Self::ClassRelation { .. } => RemoteTargetSubjectType::ClassRelation,
            Self::ObjectRelation { .. } => RemoteTargetSubjectType::ObjectRelation,
        }
    }

    pub fn subject_id(&self) -> i32 {
        match self {
            Self::Namespace { namespace_id } => namespace_id.id(),
            Self::Class { class_id } => class_id.id(),
            Self::Object { object_id, .. } => object_id.id(),
            Self::ClassRelation { relation_id } => relation_id.id(),
            Self::ObjectRelation { relation_id } => relation_id.id(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredRemoteCallTaskPayload {
    pub target_id: RemoteTargetID,
    pub subject: RemoteInvocationSubject,
    pub parameters: RemoteInvocationParameters,
    pub body_override: RemoteInvocationBodyOverride,
}

pub struct ResolvedRemoteInvocationSubject {
    pub subject_type: RemoteTargetSubjectType,
    pub subject_id: i32,
    pub namespaces: Vec<Namespace>,
    pub required_read_permission: Permissions,
    pub context: RemoteTemplateContext,
}

#[derive(Debug, Clone)]
pub struct RemoteTemplateContext {
    value: serde_json::Value,
}

impl RemoteTemplateContext {
    pub fn new(value: serde_json::Value) -> Result<Self, ApiError> {
        if value.is_object() {
            Ok(Self { value })
        } else {
            Err(ApiError::InternalServerError(
                "remote template context must be a JSON object".to_string(),
            ))
        }
    }

    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<(), ApiError> {
        let object = self.value.as_object_mut().ok_or_else(|| {
            ApiError::InternalServerError("remote template context is not an object".to_string())
        })?;
        object.insert(key.into(), value);
        Ok(())
    }

    pub fn into_value(self) -> serde_json::Value {
        self.value
    }
}

#[derive(Debug, Clone, Queryable, Selectable, Serialize, Deserialize, PartialEq, ToSchema)]
#[diesel(table_name = remote_call_results)]
pub struct RemoteCallResult {
    pub id: i32,
    pub task_id: i32,
    pub target_id: Option<i32>,
    pub subject_type: String,
    pub subject_id: i32,
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
    pub subject_type: String,
    pub subject_id: i32,
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
            class_id: row.class_id,
            name: row.name,
            description: row.description,
            method: RemoteHttpMethod::from_str(&row.method)?,
            url_template: row.url_template,
            headers_template: row.headers_template,
            body_template: row.body_template,
            auth_config: serde_json::from_value(row.auth_config)?,
            allowed_subject_types: serde_json::from_value(row.allowed_subject_types)?,
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
            self.class_id.map(HubuumClassID::id),
            &self.url_template,
            &self.headers_template,
            self.body_template.as_deref(),
            &self.auth_config,
            &self.allowed_subject_types,
            self.timeout_ms,
        )?;

        Ok(NewRemoteTargetRow {
            namespace_id: self.namespace_id.id(),
            class_id: self.class_id.map(HubuumClassID::id),
            name: self.name,
            description: self.description,
            method: self.method.as_str().to_string(),
            url_template: self.url_template,
            headers_template: self.headers_template,
            body_template: self.body_template,
            auth_config: serde_json::to_value(self.auth_config)?,
            allowed_subject_types: serde_json::to_value(self.allowed_subject_types)?,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
        })
    }
}

impl UpdateRemoteTarget {
    pub fn is_empty(&self) -> bool {
        self.namespace_id.is_none()
            && self.class_id.is_none()
            && self.name.is_none()
            && self.description.is_none()
            && self.method.is_none()
            && self.url_template.is_none()
            && self.headers_template.is_none()
            && self.body_template.is_none()
            && self.auth_config.is_none()
            && self.allowed_subject_types.is_none()
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
        let allowed_subject_types = self
            .allowed_subject_types
            .clone()
            .unwrap_or_else(|| existing.allowed_subject_types.clone());
        let timeout_ms = self.timeout_ms.unwrap_or(existing.timeout_ms);
        let class_id = match self.class_id {
            Some(Some(class_id)) => Some(class_id.id()),
            Some(None) => None,
            None => existing.class_id,
        };

        validate_target_parts(
            class_id,
            &url_template,
            &headers_template,
            body_template.as_deref(),
            &auth_config,
            &allowed_subject_types,
            timeout_ms,
        )?;

        Ok(UpdateRemoteTargetRow {
            namespace_id: self.namespace_id.map(NamespaceID::id),
            class_id: self
                .class_id
                .map(|class_id| class_id.map(HubuumClassID::id)),
            name: self.name,
            description: self.description,
            method: self.method.map(|method| method.as_str().to_string()),
            url_template: self.url_template,
            headers_template: self.headers_template,
            body_template: self.body_template,
            auth_config: self.auth_config.map(serde_json::to_value).transpose()?,
            allowed_subject_types: self
                .allowed_subject_types
                .map(serde_json::to_value)
                .transpose()?,
            timeout_ms: self.timeout_ms,
            enabled: self.enabled,
        })
    }
}

pub fn validate_target_parts(
    class_id: Option<i32>,
    url_template: &str,
    headers_template: &serde_json::Value,
    body_template: Option<&str>,
    auth_config: &RemoteAuthConfig,
    allowed_subject_types: &[RemoteTargetSubjectType],
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
    validate_allowed_subject_types(allowed_subject_types)?;
    validate_class_scope(class_id, allowed_subject_types)?;
    Ok(())
}

pub fn validate_class_scope(
    class_id: Option<i32>,
    allowed_subject_types: &[RemoteTargetSubjectType],
) -> Result<(), ApiError> {
    let allows_objects = allowed_subject_types.contains(&RemoteTargetSubjectType::Object);
    match (allows_objects, class_id) {
        (true, None) => Err(ApiError::BadRequest(
            "class_id is required when allowed_subject_types includes 'object'".to_string(),
        )),
        (false, Some(_)) => Err(ApiError::BadRequest(
            "class_id is only valid when allowed_subject_types includes 'object'".to_string(),
        )),
        _ => Ok(()),
    }
}

pub fn validate_allowed_subject_types(
    allowed_subject_types: &[RemoteTargetSubjectType],
) -> Result<(), ApiError> {
    if allowed_subject_types.is_empty() {
        return Err(ApiError::BadRequest(
            "allowed_subject_types must include at least one subject type".to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for subject_type in allowed_subject_types {
        if !seen.insert(*subject_type) {
            return Err(ApiError::BadRequest(format!(
                "allowed_subject_types contains duplicate '{}'",
                subject_type.as_str()
            )));
        }
    }
    Ok(())
}

impl RemoteTarget {
    pub fn allows_subject_type(&self, subject_type: RemoteTargetSubjectType) -> bool {
        self.allowed_subject_types.contains(&subject_type)
    }
}

pub async fn authorize_remote_invocation(
    pool: &DbPool,
    actor: &impl crate::db::traits::authz::AuthzSubject,
    scopes: Option<&[Permissions]>,
    target: &RemoteTarget,
    subject: &RemoteInvocationSubject,
) -> Result<ResolvedRemoteInvocationSubject, ApiError> {
    let target_namespace_id = NamespaceID::new(target.namespace_id)?;
    actor
        .can(
            pool,
            [Permissions::ExecuteRemoteTarget],
            [target_namespace_id],
            scopes,
        )
        .await?;

    if !target.enabled {
        return Err(ApiError::BadRequest(
            "Remote target is disabled".to_string(),
        ));
    }

    let resolved = subject.resolve(pool).await?;
    if !target.allows_subject_type(resolved.subject_type) {
        return Err(ApiError::BadRequest(format!(
            "Remote target does not allow '{}' subjects",
            resolved.subject_type.as_str()
        )));
    }
    if let RemoteInvocationSubject::Object { class_id, .. } = subject
        && target.class_id != Some(class_id.id())
    {
        return Err(ApiError::NotFound(
            "Remote target not found for invocation subject class".to_string(),
        ));
    }
    if !resolved
        .namespaces
        .iter()
        .any(|namespace| namespace.id == target.namespace_id)
    {
        return Err(ApiError::NotFound(
            "Remote target not found for invocation subject".to_string(),
        ));
    }
    actor
        .can(
            pool,
            [resolved.required_read_permission],
            resolved.namespaces.clone(),
            scopes,
        )
        .await?;

    Ok(resolved)
}

impl RemoteInvocationSubject {
    pub async fn resolve(
        &self,
        pool: &DbPool,
    ) -> Result<ResolvedRemoteInvocationSubject, ApiError> {
        match self {
            Self::Namespace { namespace_id } => {
                let namespace = namespace_id.namespace(pool).await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": namespace.clone(),
                    "namespace": namespace.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    namespaces: vec![namespace],
                    required_read_permission: Permissions::ReadCollection,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::Class { class_id } => {
                let class = class_id.class(pool).await?;
                let namespace = NamespaceID::new(class.namespace_id)?
                    .namespace(pool)
                    .await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": class.clone(),
                    "class": class.clone(),
                    "namespace": namespace.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    namespaces: vec![namespace],
                    required_read_permission: Permissions::ReadClass,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::Object {
                class_id,
                object_id,
            } => {
                let class = class_id.class(pool).await?;
                let object = object_id.instance(pool).await?;
                if object.hubuum_class_id != class.id {
                    return Err(ApiError::NotFound("Object not found in class".to_string()));
                }
                let namespace = NamespaceID::new(object.namespace_id)?
                    .namespace(pool)
                    .await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": object.clone(),
                    "object": object.clone(),
                    "class": class.clone(),
                    "namespace": namespace.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    namespaces: vec![namespace],
                    required_read_permission: Permissions::ReadObject,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::ClassRelation { relation_id } => {
                let relation = relation_id.instance(pool).await?;
                let (from_class, to_class) = relation_id.class(pool).await?;
                let namespaces = relation_id.namespace(pool).await?;
                let subject_namespaces =
                    unique_namespaces(vec![namespaces.0.clone(), namespaces.1.clone()]);
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": relation.clone(),
                    "class_relation": relation.clone(),
                    "from_class": from_class.clone(),
                    "to_class": to_class.clone(),
                    "namespaces": [namespaces.0.clone(), namespaces.1.clone()],
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    namespaces: subject_namespaces,
                    required_read_permission: Permissions::ReadClassRelation,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::ObjectRelation { relation_id } => {
                let relation = relation_id.instance(pool).await?;
                let (from_object, to_object) = relation_id.object(pool).await?;
                let class_relation_id = HubuumClassRelationID::new(relation.class_relation_id)?;
                let class_relation = class_relation_id.instance(pool).await?;
                let (from_class, to_class) = class_relation_id.class(pool).await?;
                let namespaces = relation_id.namespace(pool).await?;
                let subject_namespaces =
                    unique_namespaces(vec![namespaces.0.clone(), namespaces.1.clone()]);
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": relation.clone(),
                    "object_relation": relation.clone(),
                    "from_object": from_object.clone(),
                    "to_object": to_object.clone(),
                    "class_relation": class_relation.clone(),
                    "from_class": from_class.clone(),
                    "to_class": to_class.clone(),
                    "namespaces": [namespaces.0.clone(), namespaces.1.clone()],
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    namespaces: subject_namespaces,
                    required_read_permission: Permissions::ReadObjectRelation,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
        }
    }
}

fn unique_namespaces(namespaces: Vec<Namespace>) -> Vec<Namespace> {
    let mut seen = std::collections::HashSet::new();
    namespaces
        .into_iter()
        .filter(|namespace| seen.insert(namespace.id))
        .collect()
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
        reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| ApiError::BadRequest(format!("Invalid header name: {name}")))?;
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
    let secret = match auth_config {
        RemoteAuthConfig::None => return Ok(()),
        RemoteAuthConfig::ApiKeySecret { header, secret } => {
            // Reject an unusable header name now rather than at invocation time.
            reqwest::header::HeaderName::from_bytes(header.as_bytes()).map_err(|_| {
                ApiError::BadRequest(format!("Invalid API key header name: {header}"))
            })?;
            secret
        }
        RemoteAuthConfig::BearerSecret { secret }
        | RemoteAuthConfig::BasicSecret { secret, .. } => secret,
    };

    if valid_secret(secret) {
        Ok(())
    } else {
        Err(ApiError::BadRequest(
            "remote auth secret references must contain only letters, numbers, and underscores"
                .to_string(),
        ))
    }
}

fn validate_template(label: &str, source: &str) -> Result<(), ApiError> {
    let (recursion_limit, fuel) = remote_template_limits();
    prepare_template(source)
        .limit_recursion(recursion_limit)
        .limit_fuel(fuel)
        .validate()
        .map_err(|error| ApiError::BadRequest(format!("Invalid {label}: {error}")))
}

fn remote_template_limits() -> (usize, u64) {
    get_config()
        .map(|config| {
            (
                config.report_template_recursion_limit,
                config.report_template_fuel,
            )
        })
        .unwrap_or((
            DEFAULT_REPORT_TEMPLATE_RECURSION_LIMIT,
            DEFAULT_REPORT_TEMPLATE_FUEL,
        ))
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
    fn target_parts_validate_templates_and_auth_references() {
        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com/{{ object.id }}",
                &serde_json::json!({ "X-Object": "{{ object.name }}" }),
                Some("{\"id\": {{ object.id }}}"),
                &RemoteAuthConfig::BearerSecret {
                    secret: "servicenow_token".to_string(),
                },
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_ok()
        );

        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com/{{",
                &serde_json::json!({}),
                None,
                &RemoteAuthConfig::None,
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_err()
        );
        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com",
                &serde_json::json!([]),
                None,
                &RemoteAuthConfig::None,
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_err()
        );
        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com",
                &serde_json::json!({ "Invalid Header": "{{ object.id }}" }),
                None,
                &RemoteAuthConfig::None,
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_err()
        );
        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com",
                &serde_json::json!({}),
                None,
                &RemoteAuthConfig::ApiKeySecret {
                    header: "X-API-Key".to_string(),
                    secret: "bad-secret".to_string(),
                },
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_err()
        );
    }

    #[test]
    fn curated_filters_are_accepted_in_templates() {
        // The `tojson` filter is documented for remote targets; validation must accept it.
        assert!(
            validate_target_parts(
                Some(1),
                "https://example.com/{{ object.id }}",
                &serde_json::json!({ "X-Object": "{{ object.name }}" }),
                Some("{\"data\": {{ object.data | tojson }}}"),
                &RemoteAuthConfig::None,
                &[RemoteTargetSubjectType::Object],
                1000,
            )
            .is_ok()
        );
    }

    #[test]
    fn object_targets_require_class_scope() {
        assert!(
            validate_class_scope(None, &[RemoteTargetSubjectType::Object])
                .unwrap_err()
                .to_string()
                .contains("class_id is required")
        );
        assert!(
            validate_class_scope(Some(1), &[RemoteTargetSubjectType::Class])
                .unwrap_err()
                .to_string()
                .contains("class_id is only valid")
        );
        assert!(validate_class_scope(Some(1), &[RemoteTargetSubjectType::Object]).is_ok());
        assert!(validate_class_scope(None, &[RemoteTargetSubjectType::Class]).is_ok());
    }

    #[test]
    fn api_key_header_name_is_validated() {
        // A valid header name passes.
        assert!(
            validate_auth_config(&RemoteAuthConfig::ApiKeySecret {
                header: "X-API-Key".to_string(),
                secret: "inventory_api_key".to_string(),
            })
            .is_ok()
        );

        // An invalid header name is rejected at validation time, not at invocation.
        assert!(
            validate_auth_config(&RemoteAuthConfig::ApiKeySecret {
                header: "Invalid Header".to_string(),
                secret: "inventory_api_key".to_string(),
            })
            .is_err()
        );
    }
}

#[derive(serde::Serialize, diesel::Queryable, Clone, Debug)]
#[diesel(table_name = crate::schema::remote_targets_history)]
pub struct RemoteTargetHistory {
    pub id: i32,
    pub namespace_id: i32,
    pub class_id: Option<i32>,
    pub name: String,
    pub description: String,
    pub method: String,
    pub url_template: String,
    pub headers_template: serde_json::Value,
    pub body_template: Option<String>,
    pub auth_config: serde_json::Value,
    pub allowed_subject_types: serde_json::Value,
    pub timeout_ms: i32,
    pub enabled: bool,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(RemoteTargetHistory, "remote_targets_history");
