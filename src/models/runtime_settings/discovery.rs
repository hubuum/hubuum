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
    let (title, description, mut schema, defaults, effect) = match group {
        RuntimeSettingsGroup::Pagination => (
            "Pagination",
            "Default page size for requests that omit a limit.",
            value_schema::<PaginationSettingsRequest>()?,
            json!({"default_page_limit": deployment.pagination.default_limit()}),
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
    };
    // Utoipa field annotations do not support titles. Keep presentation labels
    // here; descriptions and validation constraints come from the request types.
    let titles: &[(&str, &str)] = match group {
        RuntimeSettingsGroup::Pagination => &[("default_page_limit", "Default page size")],
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
    };
    for (name, title) in titles {
        schema["properties"][name]["title"] = json!(title);
    }
    match group {
        RuntimeSettingsGroup::Pagination => {
            schema["properties"]["default_page_limit"]["maximum"] =
                json!((deployment.pagination.maximum_limit() as u64).min(u32::MAX as u64));
            schema["properties"]["default_page_limit"]["x-unit"] = json!("items");
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
