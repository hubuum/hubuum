use crate::models::token_scope::TokenScope;
use std::{fmt, str::FromStr};

use chrono::NaiveDateTime;
use hubuum_outbound_http::OutboundHeaderName;
use hubuum_templates::prepare_template;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::config::{
    DEFAULT_EXPORT_TEMPLATE_FUEL, DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT, get_config,
};
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    Collection, CollectionID, HubuumClassID, HubuumClassRelationID, HubuumObjectID,
    HubuumObjectRelationID, Permissions, REDACTED_DEBUG_VALUE, ResourceRevision,
    redacted_debug_option,
};
use crate::pagination::{CursorPaginated, CursorValue};
use crate::traits::UserPermissions;
use crate::traits::{ClassAccessors, CollectionAccessors, ObjectAccessors, SelfAccessors};

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
    Collection,
    Class,
    Object,
    ClassRelation,
    ObjectRelation,
}

impl RemoteTargetSubjectType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Collection => "collection",
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
            "collection" => Ok(Self::Collection),
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

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
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

impl fmt::Debug for RemoteAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::BearerSecret { .. } => formatter
                .debug_struct("BearerSecret")
                .field("secret", &REDACTED_DEBUG_VALUE)
                .finish(),
            Self::BasicSecret { username, .. } => formatter
                .debug_struct("BasicSecret")
                .field("username", username)
                .field("secret", &REDACTED_DEBUG_VALUE)
                .finish(),
            Self::ApiKeySecret { header, .. } => formatter
                .debug_struct("ApiKeySecret")
                .field("header", header)
                .field("secret", &REDACTED_DEBUG_VALUE)
                .finish(),
        }
    }
}

macro_rules! impl_redacted_remote_target_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("configuration", &REDACTED_DEBUG_VALUE)
                    .finish()
            }
        }
    };
}

macro_rules! impl_redacted_remote_call_result_debug {
    ($target:ty, $($field:ident),+ $(,)?) => {
        impl fmt::Debug for $target {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                let mut debug = formatter.debug_struct(stringify!($target));
                $(debug.field(stringify!($field), &self.$field);)+
                debug
                    .field("rendered_url", &REDACTED_DEBUG_VALUE)
                    .field("response_headers", &REDACTED_DEBUG_VALUE)
                    .field("response_body_preview", &REDACTED_DEBUG_VALUE)
                    .field("error", &redacted_debug_option(&self.error))
                    .finish()
            }
        }
    };
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct RemoteTarget {
    pub id: i32,
    pub collection_id: i32,
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
    pub revision: ResourceRevision,
}

impl_redacted_remote_target_debug!(
    RemoteTarget,
    id,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
    created_at,
    updated_at,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct NewRemoteTarget {
    pub collection_id: CollectionID,
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

impl_redacted_remote_target_debug!(
    NewRemoteTarget,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct UpdateRemoteTarget {
    pub collection_id: Option<CollectionID>,
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

impl_redacted_remote_target_debug!(
    UpdateRemoteTarget,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema)]
pub struct RemoteTargetInvokeRequest {
    pub subject: RemoteInvocationSubject,
    #[serde(default)]
    pub parameters: RemoteInvocationParameters,
    #[serde(default)]
    pub body_override: RemoteInvocationBodyOverride,
}

#[derive(Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
pub struct RemoteInvocationParameters(serde_json::Value);

impl fmt::Debug for RemoteInvocationParameters {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RemoteInvocationParameters")
            .field(&REDACTED_DEBUG_VALUE)
            .finish()
    }
}

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

#[derive(Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
pub struct RemoteInvocationBodyOverride(serde_json::Value);

impl fmt::Debug for RemoteInvocationBodyOverride {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RemoteInvocationBodyOverride")
            .field(&REDACTED_DEBUG_VALUE)
            .finish()
    }
}

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
    Collection {
        collection_id: CollectionID,
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
            Self::Collection { .. } => RemoteTargetSubjectType::Collection,
            Self::Class { .. } => RemoteTargetSubjectType::Class,
            Self::Object { .. } => RemoteTargetSubjectType::Object,
            Self::ClassRelation { .. } => RemoteTargetSubjectType::ClassRelation,
            Self::ObjectRelation { .. } => RemoteTargetSubjectType::ObjectRelation,
        }
    }

    pub fn subject_id(&self) -> i32 {
        match self {
            Self::Collection { collection_id } => collection_id.id(),
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
    pub collections: Vec<Collection>,
    pub required_read_permission: Permissions,
    pub context: RemoteTemplateContext,
}

#[derive(Clone)]
pub struct RemoteTemplateContext {
    value: serde_json::Value,
}

impl fmt::Debug for RemoteTemplateContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoteTemplateContext")
            .finish_non_exhaustive()
    }
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

#[derive(Clone, Serialize, Deserialize, PartialEq, ToSchema)]
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

impl_redacted_remote_call_result_debug!(
    RemoteCallResult,
    id,
    task_id,
    target_id,
    subject_type,
    subject_id,
    method,
    response_status,
    duration_ms,
    success,
    created_at,
);

impl UpdateRemoteTarget {
    pub fn is_empty(&self) -> bool {
        self.collection_id.is_none()
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

pub async fn authorize_remote_invocation<C>(
    backend: &C,
    actor: &impl crate::traits::AuthzSubject,
    scopes: Option<&TokenScope>,
    target: &RemoteTarget,
    subject: &RemoteInvocationSubject,
) -> Result<ResolvedRemoteInvocationSubject, ApiError>
where
    C: crate::permissions::AuthorizationContext,
{
    let pool = backend;
    let target_collection_id = CollectionID::new(target.collection_id)?;
    crate::can!(
        backend,
        actor,
        scopes,
        [Permissions::ExecuteRemoteTarget],
        target_collection_id
    );

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
        .collections
        .iter()
        .any(|collection| collection.id == target.collection_id)
    {
        return Err(ApiError::NotFound(
            "Remote target not found for invocation subject".to_string(),
        ));
    }
    for collection in &resolved.collections {
        crate::can!(
            backend,
            actor,
            scopes,
            [resolved.required_read_permission],
            collection
        );
    }

    Ok(resolved)
}

impl RemoteInvocationSubject {
    pub async fn resolve(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResolvedRemoteInvocationSubject, ApiError> {
        match self {
            Self::Collection { collection_id } => {
                let collection = collection_id.collection(pool).await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": collection.clone(),
                    "collection": collection.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    collections: vec![collection],
                    required_read_permission: Permissions::ReadCollection,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::Class { class_id } => {
                let class = class_id.class(pool).await?;
                let collection = CollectionID::new(class.collection_id)?
                    .collection(pool)
                    .await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": class.clone(),
                    "class": class.clone(),
                    "collection": collection.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    collections: vec![collection],
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
                let collection = CollectionID::new(object.collection_id)?
                    .collection(pool)
                    .await?;
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": object.clone(),
                    "object": object.clone(),
                    "class": class.clone(),
                    "collection": collection.clone(),
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    collections: vec![collection],
                    required_read_permission: Permissions::ReadObject,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
            Self::ClassRelation { relation_id } => {
                let relation = relation_id.instance(pool).await?;
                let (from_class, to_class) = relation_id.class(pool).await?;
                let collections = relation_id.collection(pool).await?;
                let subject_collections =
                    unique_collections(vec![collections.0.clone(), collections.1.clone()]);
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": relation.clone(),
                    "class_relation": relation.clone(),
                    "from_class": from_class.clone(),
                    "to_class": to_class.clone(),
                    "collections": [collections.0.clone(), collections.1.clone()],
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    collections: subject_collections,
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
                let collections = relation_id.collection(pool).await?;
                let subject_collections =
                    unique_collections(vec![collections.0.clone(), collections.1.clone()]);
                let context = serde_json::json!({
                    "subject_type": self.subject_type().as_str(),
                    "subject": relation.clone(),
                    "object_relation": relation.clone(),
                    "from_object": from_object.clone(),
                    "to_object": to_object.clone(),
                    "class_relation": class_relation.clone(),
                    "from_class": from_class.clone(),
                    "to_class": to_class.clone(),
                    "collections": [collections.0.clone(), collections.1.clone()],
                });
                Ok(ResolvedRemoteInvocationSubject {
                    subject_type: self.subject_type(),
                    subject_id: self.subject_id(),
                    collections: subject_collections,
                    required_read_permission: Permissions::ReadObjectRelation,
                    context: RemoteTemplateContext::new(context)?,
                })
            }
        }
    }
}

fn unique_collections(collections: Vec<Collection>) -> Vec<Collection> {
    let mut seen = std::collections::HashSet::new();
    collections
        .into_iter()
        .filter(|collection| seen.insert(collection.id))
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
        OutboundHeaderName::new(name).map_err(|error| ApiError::BadRequest(error.to_string()))?;
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
            OutboundHeaderName::new(header)
                .map_err(|error| ApiError::BadRequest(error.to_string()))?;
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
                config.export_template_recursion_limit,
                config.export_template_fuel,
            )
        })
        .unwrap_or((
            DEFAULT_EXPORT_TEMPLATE_RECURSION_LIMIT,
            DEFAULT_EXPORT_TEMPLATE_FUEL,
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
                | FilterField::CollectionId
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        match field {
            FilterField::Id => Ok(CursorValue::Integer(self.id as i64)),
            FilterField::Name => Ok(CursorValue::String(self.name.clone())),
            FilterField::Description => Ok(CursorValue::String(self.description.clone())),
            FilterField::CollectionId => Ok(CursorValue::Integer(self.collection_id as i64)),
            FilterField::CreatedAt => Ok(CursorValue::DateTime(self.created_at)),
            FilterField::UpdatedAt => Ok(CursorValue::DateTime(self.updated_at)),
            FilterField::Revision => Ok(CursorValue::Integer(self.revision.get())),
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

    #[test]
    fn transport_controlled_header_template_is_rejected() {
        let error = validate_target_parts(
            Some(1),
            "https://example.com",
            &serde_json::json!({ "Host": "internal.example" }),
            None,
            &RemoteAuthConfig::None,
            &[RemoteTargetSubjectType::Object],
            1000,
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("controlled by the HTTP transport")
        );
    }

    #[test]
    fn transport_controlled_api_key_header_is_rejected() {
        let error = validate_auth_config(&RemoteAuthConfig::ApiKeySecret {
            header: "Proxy-Authorization".to_string(),
            secret: "inventory_api_key".to_string(),
        })
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("controlled by the HTTP transport")
        );
    }

    #[test]
    fn remote_auth_debug_redacts_secret_references() {
        let cases = [
            RemoteAuthConfig::BearerSecret {
                secret: "bearer-secret-reference".to_string(),
            },
            RemoteAuthConfig::BasicSecret {
                username: "integration-user".to_string(),
                secret: "basic-secret-reference".to_string(),
            },
            RemoteAuthConfig::ApiKeySecret {
                header: "x-api-key".to_string(),
                secret: "api-key-secret-reference".to_string(),
            },
        ];

        for auth in cases {
            let debug = format!("{auth:?}");

            assert!(debug.contains(REDACTED_DEBUG_VALUE));
            assert!(!debug.contains("secret-reference"));
        }
    }

    #[test]
    fn remote_target_debug_redacts_all_outbound_configuration() {
        let target = NewRemoteTarget {
            collection_id: CollectionID::new(1).unwrap(),
            class_id: None,
            name: "inventory-hook".to_string(),
            description: "Inventory integration".to_string(),
            method: RemoteHttpMethod::Post,
            url_template: "https://example.invalid/hook?key=url-secret".to_string(),
            headers_template: serde_json::json!({"authorization": "header-secret"}),
            body_template: Some("body-secret".to_string()),
            auth_config: RemoteAuthConfig::BearerSecret {
                secret: "auth-secret-reference".to_string(),
            },
            allowed_subject_types: vec![RemoteTargetSubjectType::Object],
            timeout_ms: 1_000,
            enabled: true,
        };

        let debug = format!("{target:?}");

        assert!(debug.contains("inventory-hook"));
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        for secret in [
            "url-secret",
            "header-secret",
            "body-secret",
            "auth-secret-reference",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn remote_invocation_debug_redacts_parameters_body_and_context() {
        let payload = StoredRemoteCallTaskPayload {
            target_id: RemoteTargetID::new(1).unwrap(),
            subject: RemoteInvocationSubject::Collection {
                collection_id: CollectionID::new(2).unwrap(),
            },
            parameters: RemoteInvocationParameters::new(serde_json::json!({
                "token": "parameter-secret"
            }))
            .unwrap(),
            body_override: RemoteInvocationBodyOverride::new(serde_json::json!({
                "password": "body-override-secret"
            }))
            .unwrap(),
        };
        let context = RemoteTemplateContext::new(serde_json::json!({
            "private": "context-secret"
        }))
        .unwrap();

        let payload_debug = format!("{payload:?}");
        let context_debug = format!("{context:?}");

        assert!(payload_debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!payload_debug.contains("parameter-secret"));
        assert!(!payload_debug.contains("body-override-secret"));
        assert!(!context_debug.contains("context-secret"));
    }

    #[test]
    fn remote_call_result_debug_redacts_url_headers_and_body() {
        let result = RemoteCallResult {
            id: 1,
            task_id: 1,
            target_id: Some(2),
            subject_type: "object".to_string(),
            subject_id: 3,
            method: "post".to_string(),
            rendered_url: "https://example.invalid/hook?key=result-url-secret".to_string(),
            response_status: Some(200),
            response_headers: Some(serde_json::json!({
                "set-cookie": "result-header-secret"
            })),
            response_body_preview: Some("result-body-secret".to_string()),
            duration_ms: 12,
            success: true,
            error: Some("result-error-secret".to_string()),
            created_at: chrono::NaiveDate::from_ymd_opt(2026, 8, 10)
                .and_then(|date| date.and_hms_opt(12, 0, 0))
                .expect("static test timestamp must be valid"),
        };

        let debug = format!("{result:?}");

        assert!(debug.contains("response_status: Some(200)"));
        assert!(debug.contains(REDACTED_DEBUG_VALUE));
        assert!(!debug.contains("result-url-secret"));
        assert!(!debug.contains("result-header-secret"));
        assert!(!debug.contains("result-body-secret"));
        assert!(!debug.contains("result-error-secret"));
    }
}

#[derive(serde::Serialize, Clone, ToSchema)]
pub struct RemoteTargetHistory {
    pub id: i32,
    pub collection_id: i32,
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
    pub actor_kind: Option<String>,
    pub initiator_user_id: Option<i32>,
    pub task_id: Option<i32>,
    pub revision: ResourceRevision,
}

impl_redacted_remote_target_debug!(
    RemoteTargetHistory,
    id,
    collection_id,
    class_id,
    name,
    description,
    method,
    allowed_subject_types,
    timeout_ms,
    enabled,
    created_at,
    updated_at,
    op,
    valid_from,
    valid_to,
    actor_id,
    history_id,
    actor_kind,
    initiator_user_id,
    task_id,
);

impl CursorPaginated for RemoteTargetHistory {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(field, FilterField::HistoryId | FilterField::Revision)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::HistoryId => CursorValue::Integer(self.history_id),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for history"
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        vec![SortParam {
            field: FilterField::HistoryId,
            descending: true,
        }]
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Self::default_sort()
    }
}
