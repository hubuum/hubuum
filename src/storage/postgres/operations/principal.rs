use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::models::search::{FilterField, SortParam};
use crate::models::{
    NewPrincipal, Principal, PrincipalMemberResponse, ServiceAccountPointResponse,
    UserPointResponse,
};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::operations::service_account::ServiceAccountRow;
use crate::storage::postgres::operations::user::UserRow;
use crate::storage::postgres::prelude::*;
use crate::storage::postgres::{PostgresConnection, with_connection};
use crate::traits::{CursorPaginated, CursorValue};

#[derive(Debug, Queryable, Selectable, Clone)]
#[diesel(table_name = crate::schema::principals)]
pub(crate) struct PrincipalRow {
    pub(crate) id: i32,
    pub(crate) kind: String,
    pub(crate) name: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) identity_scope_id: i32,
    pub(crate) provider_managed: bool,
    pub(crate) settings: serde_json::Value,
    pub(crate) external_subject: Option<String>,
    pub(crate) last_sync_attempted_at: Option<chrono::NaiveDateTime>,
    pub(crate) last_sync_success_at: Option<chrono::NaiveDateTime>,
    pub(crate) revision: PostgresRevision,
}

impl From<PrincipalRow> for Principal {
    fn from(row: PrincipalRow) -> Self {
        Self {
            id: row.id,
            kind: row.kind,
            name: row.name,
            created_at: row.created_at,
            updated_at: row.updated_at,
            identity_scope_id: row.identity_scope_id,
            provider_managed: row.provider_managed,
            settings: row.settings,
            external_subject: row.external_subject,
            last_sync_attempted_at: row.last_sync_attempted_at,
            last_sync_success_at: row.last_sync_success_at,
            revision: row.revision.into_domain(),
        }
    }
}

impl CursorPaginated for PrincipalRow {
    fn supports_sort(field: &FilterField) -> bool {
        Principal::supports_sort(field)
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
        Principal::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        Principal::tie_breaker_sort()
    }
}

impl CursorSqlMapping for PrincipalRow {
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
            FilterField::Revision => CursorSqlField {
                column: "principals.revision",
                sql_type: CursorSqlType::BigInt,
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

pub(crate) struct PrincipalMemberQueryRow(PrincipalMemberResponse);

impl CursorPaginated for PrincipalMemberQueryRow {
    fn supports_sort(field: &FilterField) -> bool {
        PrincipalMemberResponse::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        self.0.cursor_value(field)
    }

    fn default_sort() -> Vec<SortParam> {
        PrincipalMemberResponse::default_sort()
    }

    fn tie_breaker_sort() -> Vec<SortParam> {
        PrincipalMemberResponse::tie_breaker_sort()
    }
}

impl CursorSqlMapping for PrincipalMemberQueryRow {
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
                column: "group_memberships.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "group_memberships.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "group_memberships.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' is not orderable for memberships",
                    field
                )));
            }
        })
    }
}

pub trait InsertPrincipalRecord {
    /// Insert the principal row and return it (principal-first id allocation).
    async fn insert(&self, conn: &mut PostgresConnection) -> Result<Principal, ApiError>;
}

impl InsertPrincipalRecord for NewPrincipal<'_> {
    async fn insert(&self, conn: &mut PostgresConnection) -> Result<Principal, ApiError> {
        use crate::schema::principals;

        diesel::insert_into(principals::table)
            .values((
                principals::identity_scope_id.eq(self.identity_scope_id),
                principals::kind.eq(self.kind),
                principals::name.eq(self.name),
            ))
            .get_result::<PrincipalRow>(conn)
            .await
            .map(Into::into)
            .map_err(ApiError::from)
    }
}

pub(crate) async fn principal_revision_conn(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
) -> Result<PostgresRevision, ApiError> {
    use crate::schema::principals::dsl::{id, principals, revision};

    Ok(principals
        .filter(id.eq(principal_id_value))
        .select(revision)
        .first(conn)
        .await?)
}

pub(crate) async fn lock_principal_revision_conn(
    conn: &mut PostgresConnection,
    principal_id_value: i32,
) -> Result<PostgresRevision, ApiError> {
    use crate::schema::principals::dsl::{id, principals, revision};

    let owner_revision = principals
        .filter(id.eq(principal_id_value))
        .select(revision)
        .for_update()
        .first(conn)
        .await?;
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &RevisionOwner::Principal.key(principal_id_value),
        owner_revision,
    )
    .await?;
    Ok(owner_revision)
}

/// Load the user point body and its validator revision in one SQL statement.
pub(crate) async fn load_user_point_response(
    pool: &crate::storage::postgres::PostgresPool,
    user_id_value: i32,
) -> Result<UserPointResponse, ApiError> {
    use crate::schema::{principals, users};

    let (user, identity_scope_id, name, provider_managed, revision) =
        with_connection(pool, async |conn| {
            users::table
                .inner_join(principals::table.on(principals::id.eq(users::id)))
                .filter(users::id.eq(user_id_value))
                .select((
                    UserRow::as_select(),
                    principals::identity_scope_id,
                    principals::name,
                    principals::provider_managed,
                    principals::revision,
                ))
                .first::<(UserRow, i32, String, bool, PostgresRevision)>(conn)
                .await
        })
        .await?;

    Ok(UserPointResponse::from_parts(
        user.into(),
        identity_scope_id,
        name,
        provider_managed,
        revision.into_domain(),
    ))
}

/// Load the service-account point body and revision in one SQL statement.
pub(crate) async fn load_service_account_point_response(
    pool: &crate::storage::postgres::PostgresPool,
    service_account_id_value: i32,
) -> Result<ServiceAccountPointResponse, ApiError> {
    use crate::schema::{principals, service_accounts};

    let (service_account, identity_scope_id, name, revision) =
        with_connection(pool, async |conn| {
            service_accounts::table
                .inner_join(principals::table.on(principals::id.eq(service_accounts::id)))
                .filter(service_accounts::id.eq(service_account_id_value))
                .select((
                    ServiceAccountRow::as_select(),
                    principals::identity_scope_id,
                    principals::name,
                    principals::revision,
                ))
                .first::<(ServiceAccountRow, i32, String, PostgresRevision)>(conn)
                .await
        })
        .await?;

    Ok(ServiceAccountPointResponse::from_parts(
        service_account.into(),
        identity_scope_id,
        name,
        revision.into_domain(),
    ))
}
