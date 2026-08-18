//! Transitional principal-centric authorization helpers.
//!
//! Token scope persistence is owned by `hubuum-storage-postgres`; this module
//! only retains root application composition needed by legacy authorization
//! queries that have not yet moved into the adapter crate.

use crate::storage::postgres::prelude::*;
use diesel::{pg::Pg, sql_types::Integer};

use crate::errors::ApiError;
use crate::schema::group_memberships;
pub use crate::traits::{scope_allows, scope_allows_resource, scope_allows_resources};

/// Identity-only authorization subject: principal id, group membership, admin
/// status, and kind. Scope state is passed independently to each operation.
#[allow(async_fn_in_trait)]
pub trait AuthzSubject: crate::traits::AuthzSubject {
    /// Boxed subquery of the groups this principal belongs to.
    fn group_ids_subquery<'a>(&self) -> group_memberships::BoxedQuery<'a, Pg, Integer> {
        use crate::schema::group_memberships::dsl::{group_id, group_memberships, principal_id};
        group_memberships
            .filter(principal_id.eq(self.principal_id()))
            .select(group_id)
            .into_boxed()
    }
}

impl<T: crate::traits::AuthzSubject + ?Sized> AuthzSubject for T {}

/// Resolve Hubuum's configured administrator membership inside composition.
pub(crate) async fn principal_is_admin(
    pool: &crate::storage::postgres::PostgresPool,
    principal_id: i32,
) -> Result<bool, ApiError> {
    use crate::models::identity::LOCAL_IDENTITY_SCOPE;
    use crate::storage::{AuthorizationGroupMembershipQuery, StorageError};

    let config = crate::config::get_config()?;
    let identity_scope = config
        .admin_identity_scope
        .clone()
        .unwrap_or_else(|| LOCAL_IDENTITY_SCOPE.to_string());
    hubuum_storage_postgres::operations::authorization::authorization_principal_is_group_member(
        &hubuum_storage_postgres::PostgresRuntime::unobserved(pool.clone()),
        AuthorizationGroupMembershipQuery::new(
            principal_id,
            config.admin_groupname.clone(),
            identity_scope,
        ),
    )
    .await
    .map_err(StorageError::from)
    .map_err(ApiError::from)
}
