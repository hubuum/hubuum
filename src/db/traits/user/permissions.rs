use super::*;
use crate::permissions::{AuthzTarget, PermissionDecision, PermissionRequest, PrincipalRef};
use crate::traits::BackendContext;

pub trait UserPermissions: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    /// ## Check if a user has a set of permissions on a set of targets
    ///
    /// All permissions must be present on all targets for the function to return true.
    ///
    /// ### Parameters
    ///
    /// * `ctx` - A backend context providing access to the DB pool and permission backend
    /// * `permissions` - An iterable of permissions to check for
    /// * `targets` - An iterable of authorization targets to check against
    ///
    /// ### Returns
    ///
    /// * Nothing if the user has the required permissions, or an ApiError::Forbidden if they do not.
    async fn can<C, P, N, I>(&self, ctx: &C, permissions: P, targets: I) -> Result<(), ApiError>
    where
        C: BackendContext + ?Sized,
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: AuthzTarget,
    {
        let permissions_vec: Vec<Permissions> = permissions.into_iter().collect();
        let principal = PrincipalRef::new(self.id(), self.group_ids(ctx.db_pool()).await?);

        let mut requests = Vec::new();
        for target in targets {
            let resource = target.to_resource_ref(ctx.db_pool()).await?;
            requests.push(PermissionRequest {
                resource,
                permissions: permissions_vec.clone(),
            });
        }

        let results = ctx
            .permission_backend()
            .authorize_candidates(&principal, requests)
            .await?;

        if results
            .iter()
            .all(|r| r.decision == PermissionDecision::Allow)
        {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "User does not have the required permissions".to_string(),
            ))
        }
    }
}

impl UserPermissions for User {}
impl UserPermissions for UserID {}

pub trait GroupMemberships: SelfAccessors<User> {
    /// At some point, we need to get the name of the admin group. Right now it's hard coded.
    async fn admin_groupname(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?.admin_groupname.clone())
    }

    /// Check if the user is in a group by name
    ///
    /// This function checks if the user is a member of a group with the specified name.
    ///
    /// ## Parameters
    ///
    /// * `groupname_queried` - The name of the group to check for membership.
    /// * `pool` - The database connection pool.
    ///
    /// ## Returns
    ///
    /// * Ok(true) if the user is in the group
    /// * Ok(false) if the user is not in the group
    /// * Err(ApiError) if something failed.
    async fn is_in_group_by_name(
        &self,
        groupname_queried: &str,
        pool: &DbPool,
    ) -> Result<bool, ApiError> {
        use crate::schema::groups::dsl::{groupname, groups};
        use crate::schema::user_groups::dsl::{user_groups, user_id as ug_user_id};
        use diesel::dsl::{exists, select};

        let is_in_group = with_connection(pool, |conn| {
            select(exists(
                user_groups
                    .inner_join(groups)
                    .filter(ug_user_id.eq(self.id()))
                    .filter(groupname.eq(groupname_queried)),
            ))
            .get_result(conn)
        })?;

        trace!(
            message = "Group by name check result",
            user_id = self.id(),
            groupname = groupname_queried,
            is_in_group = is_in_group,
        );

        Ok(is_in_group)
    }

    /// Check if the user is an admin
    ///
    /// This function checks the user's admin status in the database, but checking if they are
    /// a member of the group with the name "admin".
    async fn is_admin(&self, pool: &DbPool) -> Result<bool, ApiError> {
        let is_admin = self
            .is_in_group_by_name(&self.admin_groupname().await?, pool)
            .await?;

        trace!(
            message = "Admin check result",
            user_id = self.id(),
            is_admin = is_admin,
        );

        Ok(is_admin)
    }
}

impl GroupMemberships for User {}
impl GroupMemberships for UserID {}
