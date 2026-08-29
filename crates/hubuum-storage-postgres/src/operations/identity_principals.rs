//! PostgreSQL-owned principal state and effective-membership lookups.

use diesel::prelude::{ExpressionMethods, JoinOnDsl, OptionalExtension, QueryDsl};
use diesel::{Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::{GroupId, PrincipalId};
use hubuum_storage_core::StoragePrincipalGroup;

use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

const HUMAN_PRINCIPAL_KIND: &str = "human";

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::group_memberships)]
struct PrincipalGroupRow {
    principal_id: i32,
    group_id: i32,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: PostgresRevision,
}

impl PrincipalGroupRow {
    fn into_storage(self) -> Result<StoragePrincipalGroup, PostgresStorageError> {
        crate::validate_persisted(
            "principal-group membership",
            StoragePrincipalGroup::try_new(
                PrincipalId::new(self.principal_id)?,
                GroupId::new(self.group_id)?,
                self.created_at.and_utc(),
                self.updated_at.and_utc(),
                self.revision.into_domain(),
            ),
        )
    }
}

pub async fn get_principal_group(
    runtime: &PostgresRuntime,
    principal_id: i32,
    group_id: i32,
) -> Result<StoragePrincipalGroup, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    validate_positive_id(group_id, "group id")?;
    runtime
        .with_connection(async move |connection| {
            crate::schema::group_memberships::table
                .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                .filter(crate::schema::group_memberships::group_id.eq(group_id))
                .select(PrincipalGroupRow::as_select())
                .first::<PrincipalGroupRow>(connection)
                .await?
                .into_storage()
        })
        .await
}

/// Return whether the principal is both human and an effective member of the
/// owner group.
pub async fn is_human_owner_group_member(
    runtime: &PostgresRuntime,
    principal_id: i32,
    owner_group_id: i32,
) -> Result<bool, PostgresStorageError> {
    use diesel::dsl::{exists, select};

    validate_positive_id(principal_id, "principal id")?;
    validate_positive_id(owner_group_id, "owner group id")?;
    runtime
        .with_connection(async move |connection| {
            select(exists(
                crate::schema::group_memberships::table
                    .inner_join(
                        crate::schema::principals::table.on(crate::schema::principals::id
                            .eq(crate::schema::group_memberships::principal_id)),
                    )
                    .filter(crate::schema::group_memberships::group_id.eq(owner_group_id))
                    .filter(crate::schema::group_memberships::principal_id.eq(principal_id))
                    .filter(crate::schema::principals::kind.eq(HUMAN_PRINCIPAL_KIND)),
            ))
            .get_result::<bool>(connection)
            .await
            .map_err(PostgresStorageError::from)
        })
        .await
}

/// Service accounts are disabled when their subtype row carries a timestamp;
/// human principals and unknown principal ids are not disabled.
pub async fn is_service_account_disabled(
    runtime: &PostgresRuntime,
    principal_id: i32,
) -> Result<bool, PostgresStorageError> {
    validate_positive_id(principal_id, "principal id")?;
    runtime
        .with_connection(async move |connection| {
            crate::schema::service_accounts::table
                .find(principal_id)
                .select(crate::schema::service_accounts::disabled_at)
                .first::<Option<chrono::NaiveDateTime>>(connection)
                .await
                .optional()
                .map(|disabled_at| disabled_at.flatten().is_some())
                .map_err(PostgresStorageError::from)
        })
        .await
}

fn validate_positive_id(id: i32, field: &str) -> Result<(), PostgresStorageError> {
    if id > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}
