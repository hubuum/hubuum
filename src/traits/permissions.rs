use crate::models::token_scope::TokenScope;
use serde::Serialize;

use crate::errors::ApiError;
use crate::events::EventContext;
use crate::models::{GroupID, Permission, Permissions, PermissionsList};
use crate::permissions::{grant_from_storage, permission_to_storage};
use crate::services::storage_boundary::principal_id_to_storage;
use crate::storage::{
    AuthorizationDataStorage, StorageAuthorizationCollectionAccessQuery,
    StorageAuthorizationGrantDelete, StorageAuthorizationGrantKey,
    StorageAuthorizationGrantMutation, StorageContext, storage_handle,
};

use super::{AuthzSubject, CollectionAccessors, scope_allows};

pub trait PermissionController: Serialize + CollectionAccessors {
    /// Check if the user has all the given permissions on the object.
    ///
    /// - If the trait is called on a collection, check against self.
    /// - If the trait is called on a HubuumClass or a HubuumObject,
    ///   check against the collection of the class or object.
    /// - If the trait is called on a HubuumClassID or a HubuumObjectID,
    ///   create a HubuumClass or HubuumObject and check against the collection
    ///   of the class or object.
    ///
    /// If this is called on a *ID, a full class is created to extract
    /// the collection_id. To avoid creating the class multiple times during use
    /// do this:
    /// ```text
    /// permissions = vec![Permissions::ReadClass, Permissions::UpdateClass];
    /// class = class_id.class(backend).await?;
    /// if (class.user_can_all(backend, subject, permissions, scopes).await?) {
    ///     return Ok(class);
    /// }
    /// ```
    /// And not this:
    /// ```text
    /// permissions = vec![Permissions::ReadClass, Permissions::UpdateClass];
    /// if (class_id.user_can_all(backend, subject, permissions, scopes).await?) {
    ///    return Ok(class_id.class(backend).await?);
    /// }
    /// ```
    ///
    /// ## Arguments
    ///
    /// * `backend` - The backend context to use for the query.
    /// * `subject` - The principal (impl `AuthzSubject`) to check permissions for.
    /// * `permission` - The permissions to check (all must be present).
    /// * `scopes` - The token scope set as `Option<&TokenScope>`; `None` = unscoped
    ///   (full authority), `Some(..)` intersects the check fail-closed (even for admins).
    ///
    /// ## Returns
    ///
    /// * `Ok(true)` if the subject has all the given permissions on this class.
    /// * `Ok(false)` if the subject does not.
    /// * `Err(_)` if the lookup fails or a permission is invalid.
    ///
    /// ## Example
    ///
    /// ```text
    /// if (hubuum_class_or_classid.user_can_all(backend, subject, permissions, scopes).await?) {
    ///     // Do something
    /// }
    async fn user_can_all<C, S>(
        &self,
        backend: &C,
        subject: S,
        permission: Vec<Permissions>,
        scopes: Option<&TokenScope>,
    ) -> Result<bool, ApiError>
    where
        C: StorageContext,
        S: AuthzSubject,
    {
        if !scope_allows(scopes, &permission) {
            return Ok(false);
        }
        if subject.is_admin(backend).await? {
            return Ok(true);
        }
        let query = StorageAuthorizationCollectionAccessQuery::new(
            principal_id_to_storage(subject.principal_id()),
            self.collection_id(backend).await?,
            permission.into_iter().map(permission_to_storage),
        );
        Ok(storage_handle(backend)
            .authorize_local_collection(query)
            .await?)
    }

    /// Grant a set of permissions to a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions are added to the existing permission object for
    ///   the group.
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_grant` - The group ID to grant the permissions to.
    /// - `permission_list` - A list of permissions to grant, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    ///
    /// This compatibility path attributes the audit event to the system actor.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn grant_without_events<C>(
        &self,
        backend: &C,
        group_id_for_grant: GroupID,
        permission_list: PermissionsList,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        let key = StorageAuthorizationGrantKey::new(
            self.collection_id(backend).await?,
            group_id_for_grant,
        );
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            permission_list.iter().copied().map(permission_to_storage),
            false,
            EventContext::system(),
        );
        Ok(grant_from_storage(
            storage_handle(backend)
                .apply_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    async fn grant<C>(
        &self,
        backend: &C,
        group_id_for_grant: GroupID,
        permission_list: PermissionsList,
        context: &EventContext,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        self.apply_permissions(backend, group_id_for_grant, permission_list, false, context)
            .await
    }

    /// Apply permissions to a group, optionally replacing existing permissions.
    ///
    /// - When `replace_existing` is false, no permissions are removed from the group.
    /// - When `replace_existing` is true, any existing permissions are cleared first.
    ///
    /// This compatibility path attributes the audit event to the system actor.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn apply_permissions_without_events<C>(
        &self,
        backend: &C,
        group_id_for_grant: GroupID,
        permission_list: PermissionsList,
        replace_existing: bool,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        let key = StorageAuthorizationGrantKey::new(
            self.collection_id(backend).await?,
            group_id_for_grant,
        );
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            permission_list.iter().copied().map(permission_to_storage),
            replace_existing,
            EventContext::system(),
        );
        Ok(grant_from_storage(
            storage_handle(backend)
                .apply_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    async fn apply_permissions<C>(
        &self,
        backend: &C,
        group_id_for_grant: GroupID,
        permission_list: PermissionsList,
        replace_existing: bool,
        context: &EventContext,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        let key = StorageAuthorizationGrantKey::new(
            self.collection_id(backend).await?,
            group_id_for_grant,
        );
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            permission_list.iter().copied().map(permission_to_storage),
            replace_existing,
            context.clone(),
        );
        Ok(grant_from_storage(
            storage_handle(backend)
                .apply_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    /// Revoke a set of permissions from a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions are removed from the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have any permissions, no permissions are modified
    ///   and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_revoke` - The group ID to revoke the permissions from.
    /// - `permission_list` - A list of permissions to revoke, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have any permissions, an ApiError::NotFound is returned.
    ///
    /// This compatibility path attributes the audit event to the system actor.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn revoke_without_events<C>(
        &self,
        backend: &C,
        group_id_for_revoke: GroupID,
        permission_list: PermissionsList,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        let key = StorageAuthorizationGrantKey::new(
            self.collection_id(backend).await?,
            group_id_for_revoke,
        );
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            permission_list.iter().copied().map(permission_to_storage),
            false,
            EventContext::system(),
        );
        Ok(grant_from_storage(
            storage_handle(backend)
                .revoke_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    async fn revoke<C>(
        &self,
        backend: &C,
        group_id_for_revoke: GroupID,
        permission_list: PermissionsList,
        context: &EventContext,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        let key = StorageAuthorizationGrantKey::new(
            self.collection_id(backend).await?,
            group_id_for_revoke,
        );
        let mutation = StorageAuthorizationGrantMutation::new(
            key,
            permission_list.iter().copied().map(permission_to_storage),
            false,
            context.clone(),
        );
        Ok(grant_from_storage(
            storage_handle(backend)
                .revoke_local_collection_grant(mutation)
                .await?
                .into_value(),
        ))
    }

    /// Grant a specific permission to a group.
    ///
    /// - If the group previously had the permission, the requested
    ///   permission is added to the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have the permission, a new permission
    ///   object is created for the group, with the requested permission.
    ///
    /// - No permissions are removed from the group.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to grant the permission to.
    /// - `permission` - The permission to grant.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn grant_one<C>(
        &self,
        backend: &C,
        group_identifier: GroupID,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        self.grant_without_events(
            backend,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Revoke a specific permission from a group.
    ///
    /// - If the group previously had the permission, the requested
    ///   permission is removed from the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have the permission, no permissions are modified
    ///   and an ApiError::NotFound is returned.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to revoke the permission from.
    /// - `permission` - The permission to revoke.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group. If the group
    /// did not have the permission, an ApiError::NotFound is returned.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn revoke_one<C>(
        &self,
        backend: &C,
        group_identifier: GroupID,
        permission: Permissions,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        self.revoke_without_events(
            backend,
            group_identifier,
            PermissionsList::new(vec![permission]),
        )
        .await
    }

    /// Set the permissions for a group.
    ///
    /// - If the group previously had any permissions, the requested
    ///   permissions *replace* the existing permission object for
    ///   the group.
    ///
    /// - If the group did not have any permissions, a new permission
    ///   object is created for the group, with the requested permissions.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_identifier` - The group ID to set the permissions for.
    /// - `permission_list` - A list of permissions to set, wrapped in a PermissionsList.
    ///
    /// ## Returns
    ///
    /// The permission object that holds the permissions for the group.
    ///
    /// This compatibility path emits an event attributed to the system actor.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn set_permissions_without_events<C>(
        &self,
        backend: &C,
        group_identifier: GroupID,
        permission_list: PermissionsList,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        self.apply_permissions_without_events(backend, group_identifier, permission_list, true)
            .await
    }

    async fn set_permissions<C>(
        &self,
        backend: &C,
        group_identifier: GroupID,
        permission_list: PermissionsList,
        context: &EventContext,
    ) -> Result<Permission, ApiError>
    where
        C: StorageContext,
    {
        self.apply_permissions(backend, group_identifier, permission_list, true, context)
            .await
    }

    /// Revoke all permissions from a group.
    ///
    /// - If the group previously had any permissions, these are removed.
    ///
    /// - If the group did not have any permissions, no action is taken.
    ///
    /// ## Arguments
    ///
    /// - `backend` - The backend context to use for the query.
    /// - `group_id_for_revoke` - The group ID to revoke the permissions from.
    ///
    /// ## Returns
    ///
    /// An empty result.
    ///
    /// This compatibility path attributes the audit event to the system actor.
    #[cfg(any(test, feature = "integration-test-support"))]
    async fn revoke_all_without_events<C>(
        &self,
        backend: &C,
        group_id_for_revoke: GroupID,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        let request = StorageAuthorizationGrantDelete::new(
            StorageAuthorizationGrantKey::new(
                self.collection_id(backend).await?,
                group_id_for_revoke,
            ),
            EventContext::system(),
        );
        storage_handle(backend)
            .revoke_all_local_collection_grants(request)
            .await?
            .into_value();
        Ok(())
    }

    async fn revoke_all<C>(
        &self,
        backend: &C,
        group_id_for_revoke: GroupID,
        context: &EventContext,
    ) -> Result<(), ApiError>
    where
        C: StorageContext,
    {
        let request = StorageAuthorizationGrantDelete::new(
            StorageAuthorizationGrantKey::new(
                self.collection_id(backend).await?,
                group_id_for_revoke,
            ),
            context.clone(),
        );
        storage_handle(backend)
            .revoke_all_local_collection_grants(request)
            .await?
            .into_value();
        Ok(())
    }
}
