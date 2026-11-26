use std::collections::HashMap;

use crate::models::{CollectionID, Permissions, token_scope::TokenScope};
use crate::permissions::{
    AuthorizationContext, AuthorizationMode, PrincipalRef, ResourceRef, authorize_resources,
};
use crate::traits::{AuthzSubject, UserPermissions, scope_allows, scope_allows_resource};

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum ImportAdminStatus {
    #[default]
    Unknown,
    Known(bool),
}

impl ImportAdminStatus {
    fn known(self) -> Option<bool> {
        match self {
            Self::Unknown => None,
            Self::Known(is_admin) => Some(is_admin),
        }
    }
}

/// The submitting credential and its permission decisions for one planning pass.
#[derive(Default)]
pub(super) struct ImportAuthorization {
    admin_status: ImportAdminStatus,
    scopes: Option<TokenScope>,
    collection_permission_cache: HashMap<(i32, Permissions), Result<(), String>>,
}

impl ImportAuthorization {
    pub(super) fn new(scopes: Option<&TokenScope>, admin_status: ImportAdminStatus) -> Self {
        Self {
            scopes: scopes.cloned(),
            admin_status,
            collection_permission_cache: HashMap::new(),
        }
    }

    pub(super) fn is_scoped(&self) -> bool {
        self.scopes.is_some()
    }

    pub(super) async fn is_admin<C>(
        &mut self,
        backend: &C,
        user: &impl AuthzSubject,
    ) -> Result<bool, String>
    where
        C: AuthorizationContext,
    {
        if let Some(is_admin) = self.admin_status.known() {
            return Ok(is_admin);
        }
        let is_admin = match backend.authorization_mode() {
            AuthorizationMode::Delegated(permission_backend) => {
                let principal = PrincipalRef::load(backend, user)
                    .await
                    .map_err(|err| err.to_string())?;
                permission_backend
                    .is_admin(&principal)
                    .await
                    .map_err(|err| err.to_string())?
            }
            AuthorizationMode::LocalStorage => user
                .is_admin(backend)
                .await
                .map_err(|err| err.to_string())?,
        };
        self.admin_status = ImportAdminStatus::Known(is_admin);
        Ok(is_admin)
    }

    pub(super) async fn require_collection<C>(
        &mut self,
        backend: &C,
        user: &impl AuthzSubject,
        collection_id: i32,
        collection_exists_in_db: bool,
        permission: Permissions,
    ) -> Result<(), String>
    where
        C: AuthorizationContext,
    {
        if self.scopes.is_none() && self.is_admin(backend, user).await? {
            return Ok(());
        }

        if !collection_exists_in_db {
            return Err(
                "Only admins may operate on newly created collections within an import".into(),
            );
        }

        let key = (collection_id, permission);
        if let Some(result) = self.collection_permission_cache.get(&key) {
            return result.clone();
        }

        let collection = CollectionID::new(collection_id).map_err(|err| err.to_string())?;
        let scopes = self.scopes.as_ref();
        let result: Result<(), crate::errors::ApiError> = async {
            crate::can!(backend, user, scopes, [permission], collection);
            Ok(())
        }
        .await;
        let result = result.map_err(|err| err.to_string());
        self.collection_permission_cache.insert(key, result.clone());
        result
    }

    /// Existing resources must retain their identity for external policy and token
    /// resource checks. Only local collection grants may share a cached decision.
    pub(super) async fn require_resource<C>(
        &mut self,
        backend: &C,
        user: &impl AuthzSubject,
        resource: ResourceRef,
        permission: Permissions,
    ) -> Result<(), String>
    where
        C: AuthorizationContext,
    {
        if self.scopes.is_none() && self.is_admin(backend, user).await? {
            return Ok(());
        }
        if !scope_allows(self.scopes.as_ref(), &[permission])
            || !scope_allows_resource(self.scopes.as_ref(), &resource)
        {
            return Err("Permission denied".into());
        }
        match backend.authorization_mode() {
            AuthorizationMode::Delegated(permission_backend) => authorize_resources(
                permission_backend,
                backend,
                user,
                self.scopes.as_ref(),
                vec![permission],
                vec![resource],
            )
            .await
            .map_err(|error| error.to_string()),
            AuthorizationMode::LocalStorage => {
                let collection_id = resource
                    .collection_id()
                    .ok_or_else(|| "Import resource has no collection".to_string())?;
                let key = (collection_id, permission);
                if let Some(result) = self.collection_permission_cache.get(&key) {
                    return result.clone();
                }
                let collection =
                    CollectionID::new(collection_id).map_err(|error| error.to_string())?;
                let result = user
                    .can(backend, [permission], [collection], self.scopes.as_ref())
                    .await
                    .map_err(|error| error.to_string());
                self.collection_permission_cache.insert(key, result.clone());
                result
            }
        }
    }
}
