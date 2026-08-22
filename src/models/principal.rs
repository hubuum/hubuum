use serde::{Deserialize, Serialize};
use utoipa::openapi::{RefOr, schema::Schema};
use utoipa::{PartialSchema, ToSchema};

pub use hubuum_domain::PrincipalKind;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::ResourceRevision;
use crate::models::json_patch::{
    BoundedJsonPatch, MAX_JSON_PATCH_BYTES, MAX_JSON_PATCH_OPERATIONS,
    MAX_JSON_PATCH_POINTER_DEPTH, MAX_JSON_PATCH_RESULT_NESTING_DEPTH, MAX_JSON_PATCH_WORK_BYTES,
    bounded_json_patch_openapi_schema, register_bounded_json_patch_openapi_schemas,
};
use crate::models::search::{FilterField, SortParam};
use crate::services::storage_boundary::{
    principal_from_storage, principal_settings_from_storage, principal_settings_mutation_to_storage,
};
use crate::storage::{
    PrincipalStorage, StorageContext, StoragePrincipalSettingsMutation, storage_handle,
};
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{CursorPaginated, CursorValue};

/// The identity parent shared by both users and service accounts. A principal id
/// IS the user/service-account id (class-table inheritance), and `(identity_scope_id,
/// name)` is the race-safe authority for cross-kind identity-name uniqueness.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct Principal {
    pub id: i32,
    pub kind: PrincipalKind,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub identity_scope_id: i32,
    pub provider_managed: bool,
    #[serde(skip, default = "empty_principal_settings_value")]
    #[schema(ignore)]
    pub(crate) settings: serde_json::Value,
    pub external_subject: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub revision: ResourceRevision,
}

/// An object-only JSON document containing a principal's local preferences.
///
/// Values below the document root may be any JSON type. The private
/// representation keeps callers from constructing an invalid non-object root.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
#[schema(value_type = Object)]
pub struct PrincipalSettings(serde_json::Value);

/// Maximum number of operations accepted in one principal-settings JSON Patch document.
pub const MAX_PRINCIPAL_SETTINGS_PATCH_OPERATIONS: usize = MAX_JSON_PATCH_OPERATIONS;

/// Maximum pointer depth accepted in a principal-settings JSON Patch operation.
pub const MAX_PRINCIPAL_SETTINGS_PATCH_POINTER_DEPTH: usize = MAX_JSON_PATCH_POINTER_DEPTH;

/// Maximum request or result size accepted for principal-settings JSON Patch.
pub const MAX_PRINCIPAL_SETTINGS_PATCH_BYTES: usize = MAX_JSON_PATCH_BYTES;

/// Maximum cumulative application work accepted for principal-settings JSON Patch.
pub const MAX_PRINCIPAL_SETTINGS_PATCH_WORK_BYTES: usize = MAX_JSON_PATCH_WORK_BYTES;

/// Maximum result nesting accepted for principal-settings JSON Patch.
pub const MAX_PRINCIPAL_SETTINGS_PATCH_RESULT_NESTING_DEPTH: usize =
    MAX_JSON_PATCH_RESULT_NESTING_DEPTH;

/// An RFC 6902 operation array applied relative to the principal-settings root.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct PrincipalSettingsPatchDocument(BoundedJsonPatch);

impl PartialSchema for PrincipalSettingsPatchDocument {
    fn schema() -> RefOr<Schema> {
        bounded_json_patch_openapi_schema(
            "RFC 6902 operations applied relative to the principal-settings document root. Supports add, remove, replace, move, copy, and test; test compares JSON numbers by numeric value. The final root must remain an object. The request and result are limited to 2 MiB and 64 nested containers, with a bounded cumulative application-work budget.",
            serde_json::json!([
                {"op": "test", "path": "/theme", "value": "light"},
                {"op": "replace", "path": "/theme", "value": "dark"}
            ]),
        )
    }
}

impl ToSchema for PrincipalSettingsPatchDocument {
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        register_bounded_json_patch_openapi_schemas(schemas);
    }
}

/// Content-type-selected semantics for a principal-settings PATCH request.
#[derive(Clone, Debug)]
pub(crate) enum PrincipalSettingsPatch {
    MergePatch(PrincipalSettings),
    JsonPatch(PrincipalSettingsPatchDocument),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct PrincipalSettingsResponse {
    #[serde(skip, default)]
    #[schema(ignore)]
    principal_id: i32,
    pub revision: ResourceRevision,
    pub settings: PrincipalSettings,
}

impl PrincipalSettingsResponse {
    pub(crate) fn new(
        principal_id: i32,
        revision: ResourceRevision,
        settings: PrincipalSettings,
    ) -> Self {
        Self {
            principal_id,
            revision,
            settings,
        }
    }

    pub fn principal_id(&self) -> i32 {
        self.principal_id
    }

    pub fn as_value(&self) -> &serde_json::Value {
        self.settings.as_value()
    }
}

impl PrincipalSettings {
    pub fn new(value: serde_json::Value) -> Result<Self, ApiError> {
        if value.is_object() {
            Ok(Self(value))
        } else {
            Err(ApiError::BadRequest(
                "principal settings must be a JSON object".to_string(),
            ))
        }
    }

    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }
}

impl Default for PrincipalSettings {
    fn default() -> Self {
        Self(serde_json::json!({}))
    }
}

impl<'de> Deserialize<'de> for PrincipalSettings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

fn empty_principal_settings_value() -> serde_json::Value {
    serde_json::json!({})
}

impl Principal {
    /// The typed kind of this principal.
    pub const fn principal_kind(&self) -> PrincipalKind {
        self.kind
    }

    pub fn is_human(&self) -> bool {
        self.kind.is_human()
    }

    pub fn is_service_account(&self) -> bool {
        self.kind.is_service_account()
    }

    pub fn is_provider_managed(&self) -> bool {
        self.provider_managed
    }

    pub fn settings(&self) -> Result<PrincipalSettings, ApiError> {
        PrincipalSettings::new(self.settings.clone()).map_err(|_| {
            ApiError::InternalServerError(format!(
                "Principal '{}' has invalid settings in the database",
                self.id
            ))
        })
    }
}

/// Public principal details nested in a membership entity. Its revision is the
/// principal revision, independent of the enclosing membership revision.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct MembershipPrincipalResponse {
    pub principal_id: i32,
    pub identity_scope: String,
    pub kind: PrincipalKind,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl MembershipPrincipalResponse {
    pub async fn from_principal<C>(backend: &C, principal: Principal) -> Result<Self, ApiError>
    where
        C: StorageContext,
    {
        let identity_scope = crate::services::identity::resolve_identity_scope_name(
            backend,
            principal.identity_scope_id,
        )
        .await?;
        Ok(Self {
            principal_id: principal.id,
            identity_scope,
            kind: principal.kind,
            name: principal.name,
            created_at: principal.created_at,
            updated_at: principal.updated_at,
            revision: principal.revision,
        })
    }
}

/// Public representation of the revision-owned membership between a principal
/// and a group.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct PrincipalMemberResponse {
    pub principal_id: i32,
    pub group_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<MembershipPrincipalResponse>,
}

impl PrincipalMemberResponse {
    /// Build the strongly tagged membership point representation without
    /// embedding independently mutable principal data.
    pub fn point(membership: crate::models::PrincipalGroup) -> Self {
        Self {
            principal_id: membership.principal_id,
            group_id: membership.group_id,
            created_at: membership.created_at,
            updated_at: membership.updated_at,
            revision: membership.revision,
            principal: None,
        }
    }

    pub async fn from_memberships<C>(
        backend: &C,
        memberships: Vec<(crate::models::PrincipalGroup, Principal)>,
    ) -> Result<Vec<Self>, ApiError>
    where
        C: StorageContext,
    {
        let scope_ids = memberships
            .iter()
            .map(|(_, principal)| principal.identity_scope_id)
            .collect::<Vec<_>>();
        let scope_names =
            crate::services::identity::resolve_identity_scope_names(backend, &scope_ids).await?;

        memberships
            .into_iter()
            .map(|(membership, principal)| {
                let identity_scope = scope_names
                    .get(&principal.identity_scope_id)
                    .cloned()
                    .ok_or_else(|| {
                        ApiError::InternalServerError(format!(
                            "Identity scope '{}' was not resolved",
                            principal.identity_scope_id
                        ))
                    })?;
                Ok(Self {
                    principal_id: membership.principal_id,
                    group_id: membership.group_id,
                    created_at: membership.created_at,
                    updated_at: membership.updated_at,
                    revision: membership.revision,
                    principal: Some(MembershipPrincipalResponse {
                        principal_id: principal.id,
                        identity_scope,
                        kind: principal.kind,
                        name: principal.name,
                        created_at: principal.created_at,
                        updated_at: principal.updated_at,
                        revision: principal.revision,
                    }),
                })
            })
            .collect()
    }
}

impl CursorPaginated for PrincipalMemberResponse {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Username
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.principal_id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(
                self.principal
                    .as_ref()
                    .map(|principal| principal.name.clone())
                    .unwrap_or_default(),
            ),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for principals",
                    field
                )));
            }
        })
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

impl IdAccessor for Principal {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Principal> for Principal {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<Principal, ApiError> {
        Ok(self.clone())
    }
}

/// Insertable row for creating the parent principal. The id is assigned by the
/// serial sequence; subtype tables (users/service_accounts) reference it.
pub struct NewPrincipal<'a> {
    pub identity_scope_id: i32,
    pub kind: PrincipalKind,
    pub name: &'a str,
}

pub use hubuum_domain::PrincipalId as PrincipalID;

impl IdAccessor for PrincipalID {
    fn accessor_id(&self) -> i32 {
        (*self).id()
    }
}

impl InstanceAdapter<Principal> for PrincipalID {
    async fn instance_adapter(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Principal, ApiError> {
        load_principal_by_id(pool, self.id()).await
    }
}

/// Application behavior for a backend-neutral principal identifier.
pub trait PrincipalIdApplicationExt {
    async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext;

    async fn settings<C>(&self, backend: &C) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext;

    async fn replace_settings<C>(
        &self,
        backend: &C,
        settings: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext;

    async fn patch_settings<C>(
        &self,
        backend: &C,
        patch: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext;

    async fn reset_settings<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext;
}

impl PrincipalIdApplicationExt for PrincipalID {
    async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: StorageContext,
    {
        load_principal_by_id(backend, self.id()).await
    }

    async fn settings<C>(&self, backend: &C) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .get_principal_settings(crate::services::storage_boundary::principal_id_to_storage(
                self.id(),
            ))
            .await
            .map_err(ApiError::from)
            .and_then(principal_settings_from_storage)
    }

    async fn replace_settings<C>(
        &self,
        backend: &C,
        settings: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .update_principal_settings(
                crate::services::storage_boundary::principal_id_to_storage(self.id()),
                StoragePrincipalSettingsMutation::Replace(settings.as_value().clone()),
                event_context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_settings_from_storage)
    }

    async fn patch_settings<C>(
        &self,
        backend: &C,
        patch: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .update_principal_settings(
                crate::services::storage_boundary::principal_id_to_storage(self.id()),
                StoragePrincipalSettingsMutation::MergePatch(patch.as_value().clone()),
                event_context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_settings_from_storage)
    }

    async fn reset_settings<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<PrincipalSettingsResponse, ApiError>
    where
        C: StorageContext,
    {
        storage_handle(backend)
            .update_principal_settings(
                crate::services::storage_boundary::principal_id_to_storage(self.id()),
                StoragePrincipalSettingsMutation::Reset,
                event_context,
            )
            .await
            .map_err(ApiError::from)
            .map(|outcome| outcome.into_value())
            .and_then(principal_settings_from_storage)
    }
}

pub(crate) async fn apply_principal_settings_patch<C>(
    principal_id: PrincipalID,
    backend: &C,
    patch: PrincipalSettingsPatch,
    event_context: &EventContext,
) -> Result<PrincipalSettingsResponse, ApiError>
where
    C: StorageContext,
{
    storage_handle(backend)
        .update_principal_settings(
            crate::services::storage_boundary::principal_id_to_storage(principal_id.id()),
            principal_settings_mutation_to_storage(patch)?,
            event_context,
        )
        .await
        .map_err(ApiError::from)
        .map(|outcome| outcome.into_value())
        .and_then(principal_settings_from_storage)
}

/// Load a principal by id.
pub async fn load_principal_by_id(
    pool: &impl crate::storage::StorageContext,
    principal_id: i32,
) -> Result<Principal, ApiError> {
    storage_handle(pool)
        .get_principal(crate::services::storage_boundary::principal_id_to_storage(
            principal_id,
        ))
        .await
        .map_err(ApiError::from)
        .and_then(principal_from_storage)
}

impl CursorPaginated for Principal {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::Username
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for principals",
                    field
                )));
            }
        })
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
