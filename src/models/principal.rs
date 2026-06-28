use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
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
/// IS the user/service-account id (class-table inheritance), and `name` is the
/// single, race-safe authority for cross-kind identity-name uniqueness.
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
}

/// Public representation of a group member (a principal of either kind).
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, ToSchema)]
pub struct PrincipalMemberResponse {
    pub principal_id: i32,
    pub kind: String,
    pub name: String,
}

impl From<Principal> for PrincipalMemberResponse {
    fn from(principal: Principal) -> Self {
        Self {
            principal_id: principal.id,
            kind: principal.kind,
            name: principal.name,
        }
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
    pub kind: &'a str,
    pub name: &'a str,
}

impl NewPrincipal<'_> {
    /// Insert the principal row and return it (principal-first id allocation).
    pub fn insert(&self, conn: &mut PgConnection) -> Result<Principal, diesel::result::Error> {
        diesel::insert_into(principals::table)
            .values((
                principals::kind.eq(self.kind),
                principals::name.eq(self.name),
            ))
            .get_result::<Principal>(conn)
    }
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
}

/// Load a principal by id.
pub async fn load_principal_by_id(pool: &DbPool, principal_id: i32) -> Result<Principal, ApiError> {
    use crate::schema::principals::dsl::{id, principals as principals_table};
    with_connection(pool, |conn| {
        principals_table
            .filter(id.eq(principal_id))
            .first::<Principal>(conn)
    })
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
