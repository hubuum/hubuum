use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{GroupID, ResourceRevision};
use crate::storage::StorageContext;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{CursorPaginated, CursorValue};

/// A non-human principal used by automation/integrations. Its id is the
/// principal id and its name lives on `principals.name`; this row carries the
/// service-account-specific lifecycle (owner group, disabled state).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct ServiceAccount {
    pub id: i32,
    pub kind: String,
    pub description: String,
    pub owner_group_id: i32,
    pub created_by: Option<i32>,
    pub disabled_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

impl ServiceAccount {
    pub fn is_disabled(&self) -> bool {
        self.disabled_at.is_some()
    }

    /// Build the strongly tagged point representation in one database snapshot.
    pub async fn to_point_response<C>(
        &self,
        backend: &C,
    ) -> Result<ServiceAccountPointResponse, ApiError>
    where
        C: StorageContext,
    {
        crate::services::identity::load_service_account_point(backend, self.id).await
    }
}

impl IdAccessor for ServiceAccount {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<ServiceAccount> for ServiceAccount {
    async fn instance_adapter(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<ServiceAccount, ApiError> {
        Ok(self.clone())
    }
}

/// Public response shape, combining the service-account row with its principal
/// name (the name lives on `principals`).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct ServiceAccountResponse {
    pub id: i32,
    pub identity_scope: String,
    pub name: String,
    pub description: String,
    pub owner_group_id: i32,
    pub created_by: Option<i32>,
    pub disabled_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

/// Strongly tagged point representation of a service account.
///
/// The identity-scope name is omitted because that independently revisioned
/// resource is not covered by the service account's principal revision.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct ServiceAccountPointResponse {
    pub id: i32,
    pub identity_scope_id: i32,
    pub name: String,
    pub description: String,
    pub owner_group_id: i32,
    pub created_by: Option<i32>,
    pub disabled_at: Option<chrono::NaiveDateTime>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl ServiceAccountPointResponse {
    pub fn from_parts(
        service_account: ServiceAccount,
        identity_scope_id: i32,
        name: String,
        revision: ResourceRevision,
    ) -> Self {
        Self {
            id: service_account.id,
            identity_scope_id,
            name,
            description: service_account.description,
            owner_group_id: service_account.owner_group_id,
            created_by: service_account.created_by,
            disabled_at: service_account.disabled_at,
            created_at: service_account.created_at,
            updated_at: service_account.updated_at,
            revision,
        }
    }
}

impl ServiceAccountResponse {
    pub fn from_parts(
        sa: &ServiceAccount,
        identity_scope: String,
        name: String,
        revision: ResourceRevision,
    ) -> Self {
        Self {
            id: sa.id,
            identity_scope,
            name,
            description: sa.description.clone(),
            owner_group_id: sa.owner_group_id,
            created_by: sa.created_by,
            disabled_at: sa.disabled_at,
            created_at: sa.created_at,
            updated_at: sa.updated_at,
            revision,
        }
    }
}

/// List/search projection: the `service_accounts` row plus the principal name
/// (the name lives on `principals`). Drives cursor pagination without smuggling a
/// non-table field into the `ServiceAccount` Diesel mapping.
#[derive(Debug, Clone)]
pub struct ServiceAccountWithName {
    pub service_account: ServiceAccount,
    pub identity_scope: String,
    pub name: String,
    pub revision: ResourceRevision,
}

impl ServiceAccountWithName {
    pub fn from_tuple(t: (ServiceAccount, String, String, ResourceRevision)) -> Self {
        Self {
            service_account: t.0,
            identity_scope: t.1,
            name: t.2,
            revision: t.3,
        }
    }
}

impl From<ServiceAccountWithName> for ServiceAccountResponse {
    fn from(value: ServiceAccountWithName) -> Self {
        ServiceAccountResponse::from_parts(
            &value.service_account,
            value.identity_scope,
            value.name,
            value.revision,
        )
    }
}

impl CursorPaginated for ServiceAccountWithName {
    fn supports_sort(field: &FilterField) -> bool {
        matches!(
            field,
            FilterField::Id
                | FilterField::Name
                | FilterField::IdentityScope
                | FilterField::CreatedAt
                | FilterField::UpdatedAt
                | FilterField::Revision
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.service_account.id as i64),
            FilterField::IdentityScope => CursorValue::String(self.identity_scope.clone()),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.service_account.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.service_account.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for service accounts",
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

/// Request body to create a service account.
#[derive(Deserialize, Serialize, Debug, ToSchema)]
#[schema(example = new_service_account_example)]
pub struct NewServiceAccount {
    pub identity_scope: Option<String>,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[schema(value_type = i32, minimum = 1)]
    pub owner_group_id: GroupID,
}

/// Mutable fields on a service account.
#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct UpdateServiceAccount {
    pub description: Option<String>,
    pub owner_group_id: Option<i32>,
}

impl UpdateServiceAccount {
    pub(crate) fn has_changes(&self, current: &ServiceAccount) -> bool {
        self.description
            .as_ref()
            .is_some_and(|value| value != &current.description)
            || self
                .owner_group_id
                .is_some_and(|value| value != current.owner_group_id)
    }
}

pub use hubuum_domain::ServiceAccountId as ServiceAccountID;

impl IdAccessor for ServiceAccountID {
    fn accessor_id(&self) -> i32 {
        (*self).id()
    }
}

fn new_service_account_example() -> NewServiceAccount {
    NewServiceAccount {
        identity_scope: None,
        name: "dns-sync".to_string(),
        description: Some("Production DNS importer".to_string()),
        owner_group_id: GroupID::new(1).expect("valid example owner group id"),
    }
}
