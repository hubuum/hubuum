use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::schema::service_accounts;
use crate::traits::accessors::{IdAccessor, InstanceAdapter};
use crate::traits::{
    CursorPaginated, CursorSqlField, CursorSqlMapping, CursorSqlType, CursorValue,
};

/// A non-human principal used by automation/integrations. Its id is the
/// principal id and its name lives on `principals.name`; this row carries the
/// service-account-specific lifecycle (owner group, disabled state).
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Insertable, PartialEq, Debug, Clone, ToSchema,
)]
#[diesel(table_name = service_accounts)]
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
}

impl IdAccessor for ServiceAccount {
    fn accessor_id(&self) -> i32 {
        self.id
    }
}

impl InstanceAdapter<ServiceAccount> for ServiceAccount {
    async fn instance_adapter(&self, _pool: &DbPool) -> Result<ServiceAccount, ApiError> {
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
}

impl ServiceAccountResponse {
    pub fn from_parts(sa: &ServiceAccount, identity_scope: String, name: String) -> Self {
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
}

impl ServiceAccountWithName {
    pub fn from_tuple(t: (ServiceAccount, String, String)) -> Self {
        Self {
            service_account: t.0,
            identity_scope: t.1,
            name: t.2,
        }
    }
}

impl From<ServiceAccountWithName> for ServiceAccountResponse {
    fn from(value: ServiceAccountWithName) -> Self {
        ServiceAccountResponse::from_parts(&value.service_account, value.identity_scope, value.name)
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
        )
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.service_account.id as i64),
            FilterField::IdentityScope => CursorValue::String(self.identity_scope.clone()),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.service_account.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.service_account.updated_at),
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

impl CursorSqlMapping for ServiceAccountWithName {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "service_accounts.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "principals.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::IdentityScope => CursorSqlField {
                column: "identity_scopes.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "service_accounts.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "service_accounts.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for service accounts",
                    field
                )));
            }
        })
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
    pub owner_group_id: i32,
}

/// Mutable fields on a service account.
#[derive(Deserialize, Serialize, AsChangeset, Debug, ToSchema)]
#[diesel(table_name = service_accounts)]
pub struct UpdateServiceAccount {
    pub description: Option<String>,
    pub owner_group_id: Option<i32>,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`ServiceAccount`].
    pub struct ServiceAccountID;
    noun = "service account id";
}

impl IdAccessor for ServiceAccountID {
    fn accessor_id(&self) -> i32 {
        self.0
    }
}

#[allow(dead_code)]
fn new_service_account_example() -> NewServiceAccount {
    NewServiceAccount {
        identity_scope: None,
        name: "dns-sync".to_string(),
        description: Some("Production DNS importer".to_string()),
        owner_group_id: 1,
    }
}
