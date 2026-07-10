use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::principal::PrincipalSettingsMutation;
use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::search::{FilterField, SortParam};
use crate::schema::principals;
use crate::traits::BackendContext;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

/// The kind of a principal: a human user or a non-human service account.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    Human,
    ServiceAccount,
}

impl PrincipalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PrincipalKind::Human => "human",
            PrincipalKind::ServiceAccount => "service_account",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, ApiError> {
        match value {
            "human" => Ok(PrincipalKind::Human),
            "service_account" => Ok(PrincipalKind::ServiceAccount),
            other => Err(ApiError::InternalServerError(format!(
                "Unknown principal kind '{other}'"
            ))),
        }
    }

    pub fn is_human(self) -> bool {
        matches!(self, PrincipalKind::Human)
    }

    pub fn is_service_account(self) -> bool {
        matches!(self, PrincipalKind::ServiceAccount)
    }
}

/// The identity parent shared by both users and service accounts. A principal id
/// IS the user/service-account id (class-table inheritance), and `(identity_scope_id,
/// name)` is the race-safe authority for cross-kind identity-name uniqueness.
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = principals)]
pub struct Principal {
    pub id: i32,
    pub kind: String,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub identity_scope_id: i32,
    pub provider_managed: bool,
    #[serde(skip, default = "empty_principal_settings_value")]
    #[schema(ignore)]
    settings: serde_json::Value,
    pub external_subject: Option<String>,
    pub last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub last_sync_success_at: Option<chrono::NaiveDateTime>,
}

/// An object-only JSON document containing a principal's local preferences.
///
/// Values below the document root may be any JSON type. The private
/// representation keeps callers from constructing an invalid non-object root.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, ToSchema)]
#[serde(transparent)]
#[schema(value_type = Object)]
pub struct PrincipalSettings(serde_json::Value);

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

    /// Apply JSON Merge Patch object semantics to this document.
    ///
    /// Object values merge recursively, `null` removes a key, and every other
    /// value replaces the value currently stored at that key.
    pub fn merge_patch(mut self, patch: &Self) -> Self {
        let target = self
            .0
            .as_object_mut()
            .expect("PrincipalSettings always contains an object");
        let patch = patch
            .0
            .as_object()
            .expect("PrincipalSettings always contains an object");
        merge_settings_objects(target, patch);
        self
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

fn merge_settings_objects(
    target: &mut serde_json::Map<String, serde_json::Value>,
    patch: &serde_json::Map<String, serde_json::Value>,
) {
    for (key, patch_value) in patch {
        match patch_value {
            serde_json::Value::Null => {
                target.remove(key);
            }
            serde_json::Value::Object(patch_object) => {
                let target_value = target
                    .entry(key.clone())
                    .or_insert_with(|| serde_json::json!({}));
                if !target_value.is_object() {
                    *target_value = serde_json::json!({});
                }
                merge_settings_objects(
                    target_value
                        .as_object_mut()
                        .expect("replacement settings value is an object"),
                    patch_object,
                );
            }
            _ => {
                target.insert(key.clone(), patch_value.clone());
            }
        }
    }
}

fn empty_principal_settings_value() -> serde_json::Value {
    serde_json::json!({})
}

impl Principal {
    /// The typed kind of this principal.
    pub fn principal_kind(&self) -> Result<PrincipalKind, ApiError> {
        PrincipalKind::from_db(&self.kind)
    }

    pub fn is_human(&self) -> bool {
        matches!(self.principal_kind(), Ok(kind) if kind.is_human())
    }

    pub fn is_service_account(&self) -> bool {
        matches!(self.principal_kind(), Ok(kind) if kind.is_service_account())
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

/// Public representation of a group member (a principal of either kind).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct PrincipalMemberResponse {
    pub principal_id: i32,
    pub identity_scope: String,
    pub kind: String,
    pub name: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl PrincipalMemberResponse {
    pub async fn from_principal<C>(backend: &C, principal: Principal) -> Result<Self, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let metadata = crate::db::traits::principal::principal_identity_metadata(
            backend.db_pool(),
            principal.id,
        )
        .await?;
        Ok(Self {
            principal_id: principal.id,
            identity_scope: metadata.identity_scope,
            kind: principal.kind,
            name: principal.name,
            created_at: principal.created_at,
            updated_at: principal.updated_at,
        })
    }

    pub async fn from_principals<C>(
        backend: &C,
        principals: Vec<Principal>,
    ) -> Result<Vec<Self>, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let scope_ids = principals
            .iter()
            .map(|principal| principal.identity_scope_id)
            .collect::<Vec<_>>();
        let scope_names =
            crate::db::traits::identity::identity_scope_names_by_ids(backend.db_pool(), &scope_ids)
                .await?;

        principals
            .into_iter()
            .map(|principal| {
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
                    principal_id: principal.id,
                    identity_scope,
                    kind: principal.kind,
                    name: principal.name,
                    created_at: principal.created_at,
                    updated_at: principal.updated_at,
                })
            })
            .collect()
    }
}

impl CursorPaginated for PrincipalMemberResponse {
    fn supports_sort(field: &FilterField) -> bool {
        Principal::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.principal_id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for principals",
                    field
                )));
            }
        })
    }

    fn default_sort() -> Vec<SortParam> {
        Principal::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Principal::tie_breaker_sort()
    }
}

impl IdAccessor for Principal {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<Principal> for Principal {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<Principal, ApiError> {
        Ok(self.clone())
    }
}

/// Insertable row for creating the parent principal. The id is assigned by the
/// serial sequence; subtype tables (users/service_accounts) reference it.
#[derive(Insertable)]
#[diesel(table_name = principals)]
pub struct NewPrincipal<'a> {
    pub identity_scope_id: i32,
    pub kind: &'a str,
    pub name: &'a str,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`Principal`].
    pub struct PrincipalID;
    noun = "principal id";
}

impl IdAccessor for PrincipalID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

impl InstanceAdapter<Principal> for PrincipalID {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<Principal, ApiError> {
        load_principal_by_id(pool, self.id()).await
    }
}

impl PrincipalID {
    pub async fn principal<C>(&self, backend: &C) -> Result<Principal, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        load_principal_by_id(backend.db_pool(), self.id()).await
    }

    pub async fn settings<C>(&self, backend: &C) -> Result<PrincipalSettings, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::principal::load_principal_settings(backend.db_pool(), self.id()).await
    }

    pub async fn replace_settings<C>(
        &self,
        backend: &C,
        settings: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettings, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::principal::mutate_principal_settings(
            backend.db_pool(),
            self.id(),
            PrincipalSettingsMutation::Replace,
            settings,
            event_context,
        )
        .await
    }

    pub async fn patch_settings<C>(
        &self,
        backend: &C,
        patch: PrincipalSettings,
        event_context: &EventContext,
    ) -> Result<PrincipalSettings, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::principal::mutate_principal_settings(
            backend.db_pool(),
            self.id(),
            PrincipalSettingsMutation::Patch,
            patch,
            event_context,
        )
        .await
    }

    pub async fn reset_settings<C>(
        &self,
        backend: &C,
        event_context: &EventContext,
    ) -> Result<PrincipalSettings, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        crate::db::traits::principal::mutate_principal_settings(
            backend.db_pool(),
            self.id(),
            PrincipalSettingsMutation::Reset,
            PrincipalSettings::default(),
            event_context,
        )
        .await
    }
}

/// Load a principal by id.
pub async fn load_principal_by_id(pool: &DbPool, principal_id: i32) -> Result<Principal, ApiError> {
    crate::db::traits::principal::load_principal_by_id(pool, principal_id).await
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
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id as i64),
            FilterField::Name | FilterField::Username => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
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

impl CursorSqlMapping for Principal {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "principals.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name | FilterField::Username => CursorSqlField {
                column: "principals.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "principals.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "principals.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for principals",
                    field
                )));
            }
        })
    }
}
