//! Discovery metadata and partial updates for generic administrator clients.
use hubuum_domain::ResourceRevision;
use hubuum_storage_core::StorageRuntimeSettingsGroup;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use utoipa::{PartialSchema, ToSchema};

use super::{
    AuthenticationSettingsRequest, AuthorizationSettingsRequest, PaginationSettingsRequest,
    QueryObservationPolicyValue, QueryObservationSettingsRequest, RuntimeAuthorizationBackend,
    RuntimeSettingsDeployment, RuntimeSettingsEffect, RuntimeSettingsGroup,
    RuntimeSettingsOwnership, RuntimeSettingsReplicaStatus, RuntimeSettingsReport,
};
use crate::errors::ApiError;

/// Version 1 uses JSON Schema 2020-12 field descriptions. New groups and fields
/// are additive. Clients must leave unsupported schemas read-only.
#[derive(Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsCatalogue {
    pub schema_version: u32,
    pub groups: Vec<RuntimeSettingsDescriptor>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsDescriptor {
    pub key: RuntimeSettingsGroup,
    pub title: String,
    pub description: String,
    /// Schema for the group's values, excluding revision metadata. Every field
    /// has a plain-text title and description. String enums also provide
    /// x-enum-descriptions, mapping each wire value to its plain-text explanation.
    #[schema(value_type = Object)]
    pub schema: Value,
    pub patch_supported: bool,
    pub requires_local_admin: bool,
    pub takes_effect: RuntimeSettingsEffect,
    /// Why a generic client cannot edit this group.
    pub read_only_reason: Option<String>,
}

/// Partial replacement of named top-level fields. Omitted fields are preserved;
/// null is a value, never a reset command. Release uses the existing DELETE API.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSettingsPatch {
    pub expected_revision: ResourceRevision,
    #[schema(value_type = Object)]
    pub changes: Map<String, Value>,
}

/// A uniform, redacted view of one group, applied by the responding replica.
#[derive(Debug, Serialize, ToSchema)]
pub struct RuntimeSettingsGroupState {
    pub group: RuntimeSettingsGroup,
    #[serde(flatten)]
    pub ownership: RuntimeSettingsOwnership,
    pub replica: RuntimeSettingsReplicaStatus,
    #[schema(value_type = Object)]
    pub effective: Value,
    /// Requested shared values, before deployment ceilings. Authentication
    /// definitions are deliberately omitted, including encrypted secrets.
    #[schema(value_type = Option<Object>)]
    pub runtime: Option<Value>,
    #[schema(value_type = Option<Object>)]
    pub deployment: Option<Value>,
    pub constrained_by_ceiling: bool,
}

impl RuntimeSettingsGroupState {
    pub fn from_report(
        group: RuntimeSettingsGroup,
        report: RuntimeSettingsReport,
    ) -> Result<Self, ApiError> {
        let (ownership, effective, runtime, deployment, constrained_by_ceiling) = match group {
            RuntimeSettingsGroup::Pagination => {
                let value = report.pagination;
                (
                    value.ownership,
                    serde_json::to_value(value.effective)?,
                    value.runtime.map(serde_json::to_value).transpose()?,
                    Some(serde_json::to_value(value.deployment)?),
                    value.constrained_by_ceiling,
                )
            }
            RuntimeSettingsGroup::QueryObservations => {
                let value = report.query_observations;
                (
                    value.ownership,
                    serde_json::to_value(value.effective)?,
                    value.runtime.map(serde_json::to_value).transpose()?,
                    Some(serde_json::to_value(value.deployment)?),
                    value.constrained_by_ceiling,
                )
            }
            RuntimeSettingsGroup::Authorization => {
                let value = report.authorization;
                (
                    value.ownership,
                    json!({"backend": value.effective_backend}),
                    value
                        .runtime_backend
                        .map(|backend| json!({"backend": backend})),
                    Some(json!({"backend": value.deployment_backend})),
                    false,
                )
            }
            RuntimeSettingsGroup::Authentication => {
                let value = report.authentication;
                (
                    value.ownership,
                    json!({"providers": value.providers}),
                    None,
                    None,
                    false,
                )
            }
            group => {
                let key = StorageRuntimeSettingsGroup::from(group).as_str();
                let value = report.policies.get(key).ok_or_else(|| {
                    ApiError::InternalServerError(format!("Missing policy report for {key}"))
                })?;
                (
                    value.ownership.clone(),
                    value.effective.clone(),
                    value.runtime.clone(),
                    Some(value.deployment.clone()),
                    value.constrained_by_ceiling,
                )
            }
        };
        Ok(Self {
            group,
            ownership,
            replica: report.replica,
            effective,
            runtime,
            deployment,
            constrained_by_ceiling,
        })
    }
}

/// The request types remain the source of field names, types, and static bounds.
/// Deployment bounds narrow those schemas for this responding replica.
impl RuntimeSettingsCatalogue {
    pub fn new(deployment: RuntimeSettingsDeployment) -> Result<Self, ApiError> {
        let groups = StorageRuntimeSettingsGroup::ALL
            .into_iter()
            .map(|group| descriptor(group.into(), deployment))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            schema_version: 1,
            groups,
        })
    }
}

fn value_schema<T: PartialSchema>() -> Result<Value, ApiError> {
    let mut schema = serde_json::to_value(T::schema())?;
    schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
    schema["additionalProperties"] = json!(false);
    if let Some(properties) = schema["properties"].as_object_mut() {
        properties.remove("expected_revision");
        // Optional PUT fields use null for deployment fallback. The generic
        // PATCH contract deliberately accepts only concrete field values.
        for property in properties.values_mut() {
            if let Some(types) = property["type"].as_array() {
                let non_null: Vec<_> = types
                    .iter()
                    .filter(|value| **value != "null")
                    .cloned()
                    .collect();
                if non_null.len() == 1 {
                    property["type"] = non_null[0].clone();
                }
            }
        }
    }
    if let Some(required) = schema["required"].as_array_mut() {
        required.retain(|value| value != "expected_revision");
    }
    Ok(schema)
}

fn descriptor(
    group: RuntimeSettingsGroup,
    deployment: RuntimeSettingsDeployment,
) -> Result<RuntimeSettingsDescriptor, ApiError> {
    if let Some(mut schema) = crate::config::runtime::policy_schema(group.into()) {
        let config = crate::config::get_config()?.clone();
        let defaults = crate::config::runtime::deployment_document(&config, group.into())
            .expect("policy group");
        for (name, value) in defaults.as_object().expect("policy object") {
            schema["properties"][name]["default"] = value.clone();
        }
        match group {
            RuntimeSettingsGroup::Tokens => {
                schema["properties"]["lifetime_hours"]["maximum"] =
                    json!(config.max_token_lifetime_hours)
            }
            RuntimeSettingsGroup::Events => {
                schema["properties"]["delivery_transport_timeout_ms"]["maximum"] =
                    json!(config.event_delivery_lock_timeout_ms.saturating_sub(1));
                if config.event_retention_archive_path.is_none() {
                    schema["properties"]["file_archive_enabled"]["const"] = json!(false);
                }
            }
            RuntimeSettingsGroup::RemoteCalls if !config.remote_call_allow_private_targets => {
                schema["properties"]["allow_private_targets"]["const"] = json!(false)
            }
            _ => {}
        }
        let key = StorageRuntimeSettingsGroup::from(group);
        return Ok(RuntimeSettingsDescriptor {
            key: group, title: key.as_str().replace('_', " "),
            description: "Validated policy shared across replicas. Running operations retain their initial settings; new operations use the latest reconciled policy.".into(),
            schema, patch_supported: true,
            requires_local_admin: matches!(group, RuntimeSettingsGroup::Network | RuntimeSettingsGroup::Tokens | RuntimeSettingsGroup::LoginRateLimit | RuntimeSettingsGroup::RemoteCalls),
            takes_effect: policy_effect(key), read_only_reason: None,
        });
    }
    let (title, description, mut schema, defaults, effect) = match group {
        RuntimeSettingsGroup::Pagination => (
            "Pagination",
            "Default and maximum page sizes, bounded by this replica's deployment ceiling.",
            value_schema::<PaginationSettingsRequest>()?,
            json!({
                "default_page_limit": deployment.pagination.default_limit(),
                "max_page_limit": deployment.pagination.maximum_limit()
            }),
            RuntimeSettingsEffect::NextRequest,
        ),
        RuntimeSettingsGroup::QueryObservations => (
            "Query observations",
            "Bounded query observation collection. Patterns per class must not exceed total patterns.",
            value_schema::<QueryObservationSettingsRequest>()?,
            serde_json::to_value(QueryObservationPolicyValue::from(
                deployment.query_observations,
            ))?,
            RuntimeSettingsEffect::NextQuery,
        ),
        RuntimeSettingsGroup::Authorization => (
            "Authorization",
            "Select the policy backend. The local administrator making the change must retain access.",
            value_schema::<AuthorizationSettingsRequest>()?,
            json!({"backend": deployment.authorization_backend.as_str()}),
            RuntimeSettingsEffect::NextRequest,
        ),
        RuntimeSettingsGroup::Authentication => (
            "Authentication",
            "Provider activation and ownership require the authentication preflight workflow.",
            value_schema::<AuthenticationSettingsRequest>()?,
            json!({}),
            RuntimeSettingsEffect::NextRequest,
        ),
        _ => unreachable!("operational policy handled above"),
    };
    // Utoipa field annotations do not support titles. Keep presentation labels
    // here; descriptions and validation constraints come from the request types.
    let titles: &[(&str, &str)] = match group {
        RuntimeSettingsGroup::Pagination => &[
            ("default_page_limit", "Default page size"),
            ("max_page_limit", "Maximum page size"),
        ],
        RuntimeSettingsGroup::QueryObservations => &[
            ("enabled", "Collect query observations"),
            ("sample_every", "Sampling interval"),
            ("max_patterns", "Total pattern limit"),
            ("max_patterns_per_class", "Patterns per class"),
            ("retention_seconds", "Observation retention"),
            ("max_predicates_per_query", "Predicates per observed query"),
        ],
        RuntimeSettingsGroup::Authorization => &[("backend", "Authorization backend")],
        RuntimeSettingsGroup::Authentication => &[("providers", "Authentication providers")],
        _ => unreachable!("operational policy handled above"),
    };
    for (name, title) in titles {
        schema["properties"][name]["title"] = json!(title);
    }
    match group {
        RuntimeSettingsGroup::Pagination => {
            schema["properties"]["default_page_limit"]["maximum"] =
                json!((deployment.pagination.maximum_limit() as u64).min(u32::MAX as u64));
            schema["properties"]["default_page_limit"]["x-unit"] = json!("items");
            schema["properties"]["max_page_limit"]["maximum"] =
                json!(deployment.pagination.maximum_limit());
            schema["properties"]["max_page_limit"]["x-unit"] = json!("items");
        }
        RuntimeSettingsGroup::QueryObservations => {
            let ceilings = deployment.query_observations.ceilings();
            let properties = &mut schema["properties"];
            properties["sample_every"]["minimum"] = json!(ceilings.min_sample_every());
            properties["max_patterns"]["maximum"] = json!(ceilings.max_patterns());
            properties["max_patterns_per_class"]["maximum"] =
                json!(ceilings.max_patterns_per_class());
            properties["retention_seconds"]["maximum"] = json!(ceilings.max_retention_seconds());
            properties["retention_seconds"]["x-unit"] = json!("seconds");
            properties["max_predicates_per_query"]["maximum"] =
                json!(ceilings.max_predicates_per_query());
            properties["sample_every"]["x-unit"] = json!("queries");
            properties["max_patterns"]["x-unit"] = json!("patterns");
            properties["max_patterns_per_class"]["x-unit"] = json!("patterns");
            properties["max_predicates_per_query"]["x-unit"] = json!("predicates");
        }
        RuntimeSettingsGroup::Authorization => {
            let property = &mut schema["properties"]["backend"];
            let mut backend_schema = serde_json::to_value(RuntimeAuthorizationBackend::schema())?;
            backend_schema["title"] = property["title"].clone();
            backend_schema["description"] = property["description"].clone();
            *property = backend_schema;
            if !cfg!(feature = "permissions-treetop") {
                property["enum"] = json!(["local"]);
            }
            let descriptions = property["enum"]
                .as_array()
                .expect("backend is an enum")
                .iter()
                .map(|value| {
                    let backend: RuntimeAuthorizationBackend =
                        serde_json::from_value(value.clone())?;
                    Ok((
                        value
                            .as_str()
                            .expect("backend uses string values")
                            .to_owned(),
                        json!(backend.description()),
                    ))
                })
                .collect::<Result<Map<_, _>, ApiError>>()?;
            property["x-enum-descriptions"] = Value::Object(descriptions);
        }
        RuntimeSettingsGroup::Authentication => {
            // Do not publish dangling references to the full OpenAPI document.
            // Generic clients inspect the redacted state; specialized clients
            // use the separately documented preflight request schema.
            let title = schema["properties"]["providers"]["title"].clone();
            let description = schema["properties"]["providers"]["description"].clone();
            schema = json!({"$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object", "readOnly": true, "properties": {
                "providers": {"type": "array", "title": title, "description": description,
                    "items": {"type": "object"}, "readOnly": true}
            }});
        }
        _ => unreachable!("operational policy handled above"),
    }
    if let Some(properties) = schema["properties"].as_object_mut() {
        for (name, property) in properties {
            if let Some(default) = defaults.get(name) {
                property["default"] = default.clone();
            }
        }
    }
    Ok(RuntimeSettingsDescriptor {
        key: group, title: title.into(), description: description.into(), schema,
        patch_supported: group != RuntimeSettingsGroup::Authentication,
        requires_local_admin: matches!(group, RuntimeSettingsGroup::Authentication | RuntimeSettingsGroup::Authorization),
        takes_effect: effect,
        read_only_reason: (group == RuntimeSettingsGroup::Authentication).then(||
            "Use POST /api/v1/admin/runtime-settings/authentication/preflight to validate provider ownership, secrets, and connectivity before committing.".into()),
    })
}

pub(crate) fn policy_effect(group: StorageRuntimeSettingsGroup) -> RuntimeSettingsEffect {
    match group {
        StorageRuntimeSettingsGroup::Observability => RuntimeSettingsEffect::OnReconciliation,
        StorageRuntimeSettingsGroup::Network
        | StorageRuntimeSettingsGroup::QueryLimits
        | StorageRuntimeSettingsGroup::LoginRateLimit
        | StorageRuntimeSettingsGroup::Tokens => RuntimeSettingsEffect::NextRequest,
        _ => RuntimeSettingsEffect::NextOperation,
    }
}
