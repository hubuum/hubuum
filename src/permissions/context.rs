use std::sync::{Arc, RwLock};

use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};
use hubuum_domain::ResourceRevision;

use crate::errors::ApiError;
use crate::services::{
    ClassRelationService, ClassService, CollectionService, ObjectRelationService, ObjectService,
    Services,
};
use crate::storage::StorageContext;
use crate::storage::StorageHandle;
use crate::storage::runtime_settings::AppliedRuntimeSettings;
use crate::traits::PrincipalIdAccessor;

use super::backend::{CompleteCollectionCandidateLimit, PermissionBackend};
use super::types::{PermissionDecision, PermissionRequest, PrincipalRef};

/// Authorization policy selected for one permission-aware application call.
#[derive(Clone, Copy)]
pub enum AuthorizationMode<'a> {
    /// Evaluate permissions from the storage backend's local authorization data.
    LocalStorage,
    /// Delegate policy decisions to the configured external permission backend.
    Delegated(&'a dyn PermissionBackend),
}

impl<'a> AuthorizationMode<'a> {
    #[must_use]
    pub fn from_backend(backend: &'a dyn PermissionBackend) -> Self {
        if backend.uses_local_permission_store() {
            Self::LocalStorage
        } else {
            Self::Delegated(backend)
        }
    }
}

struct PermissionBackendState {
    backend: Option<Arc<dyn PermissionBackend>>,
    failure: Option<String>,
}

struct SwappablePermissionBackend {
    state: RwLock<PermissionBackendState>,
}

impl SwappablePermissionBackend {
    fn new(backend: Arc<dyn PermissionBackend>) -> Self {
        Self {
            state: RwLock::new(PermissionBackendState {
                backend: Some(backend),
                failure: None,
            }),
        }
    }

    fn backend(&self) -> Result<Arc<dyn PermissionBackend>, ApiError> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.backend.clone().ok_or_else(|| {
            ApiError::PermissionBackendUnavailable(
                state
                    .failure
                    .clone()
                    .unwrap_or_else(|| "authorization backend is unavailable".to_string()),
            )
        })
    }

    fn replace(&self, backend: Arc<dyn PermissionBackend>) {
        *self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = PermissionBackendState {
            backend: Some(backend),
            failure: None,
        };
    }

    fn fail(&self, failure: String) {
        *self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = PermissionBackendState {
            backend: None,
            failure: Some(failure),
        };
    }

    fn is_ready(&self) -> bool {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .backend
            .is_some()
    }
}

#[async_trait::async_trait]
impl PermissionBackend for SwappablePermissionBackend {
    async fn authorize_many(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        self.backend()?.authorize_many(principal, requests).await
    }

    async fn collections_user_can(
        &self,
        principal: &PrincipalRef,
        permissions: &[crate::models::Permissions],
        candidate_limit: CompleteCollectionCandidateLimit,
    ) -> Result<Vec<crate::models::Collection>, ApiError> {
        self.backend()?
            .collections_user_can(principal, permissions, candidate_limit)
            .await
    }

    async fn groups_with_permissions_on(
        &self,
        collection_id: crate::models::CollectionID,
        permissions_filter: &[crate::models::Permissions],
        page: &crate::models::search::QueryOptions,
    ) -> Result<(Vec<crate::models::GroupPermission>, i64), ApiError> {
        self.backend()?
            .groups_with_permissions_on(collection_id, permissions_filter, page)
            .await
    }

    async fn group_permission_on(
        &self,
        collection_id: crate::models::CollectionID,
        group_id: crate::models::GroupID,
    ) -> Result<Option<crate::models::Permission>, ApiError> {
        self.backend()?
            .group_permission_on(collection_id, group_id)
            .await
    }

    async fn apply_permissions(
        &self,
        collection_id: crate::models::CollectionID,
        group_id: crate::models::GroupID,
        list: crate::models::PermissionsList,
        replace_existing: bool,
    ) -> Result<crate::models::Permission, ApiError> {
        self.backend()?
            .apply_permissions(collection_id, group_id, list, replace_existing)
            .await
    }

    async fn revoke_permissions(
        &self,
        collection_id: crate::models::CollectionID,
        group_id: crate::models::GroupID,
        list: crate::models::PermissionsList,
    ) -> Result<crate::models::Permission, ApiError> {
        self.backend()?
            .revoke_permissions(collection_id, group_id, list)
            .await
    }

    async fn revoke_all(
        &self,
        collection_id: crate::models::CollectionID,
        group_id: crate::models::GroupID,
    ) -> Result<(), ApiError> {
        self.backend()?.revoke_all(collection_id, group_id).await
    }

    fn supports_mutation(&self) -> bool {
        self.backend()
            .is_ok_and(|backend| backend.supports_mutation())
    }

    fn kind(&self) -> &'static str {
        self.backend()
            .map_or("unavailable", |backend| backend.kind())
    }

    async fn is_admin(&self, principal: &PrincipalRef) -> Result<bool, ApiError> {
        self.backend()?.is_admin(principal).await
    }

    fn supports_storage_visibility_filtering(&self) -> bool {
        self.backend()
            .is_ok_and(|backend| backend.supports_storage_visibility_filtering())
    }

    fn uses_local_permission_store(&self) -> bool {
        self.backend()
            .is_ok_and(|backend| backend.uses_local_permission_store())
    }

    fn supports_permission_provenance(&self) -> bool {
        self.backend()
            .is_ok_and(|backend| backend.supports_permission_provenance())
    }
}

pub struct AppContext {
    backend: StorageHandle,
    permissions: Arc<SwappablePermissionBackend>,
    request_permissions: Option<Arc<dyn PermissionBackend>>,
    permission_revision: Arc<RwLock<Option<ResourceRevision>>>,
    authentication_revision: Arc<RwLock<Option<ResourceRevision>>>,
    permission_target_revision: Arc<RwLock<Option<ResourceRevision>>>,
    authentication_target_revision: Arc<RwLock<Option<ResourceRevision>>>,
    settings_apply: Arc<tokio::sync::Mutex<()>>,
    services: Services,
}

impl Clone for AppContext {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            permissions: self.permissions.clone(),
            request_permissions: self
                .request_permissions
                .clone()
                .or_else(|| self.permissions.backend().ok()),
            permission_revision: self.permission_revision.clone(),
            authentication_revision: self.authentication_revision.clone(),
            permission_target_revision: self.permission_target_revision.clone(),
            authentication_target_revision: self.authentication_target_revision.clone(),
            settings_apply: self.settings_apply.clone(),
            services: self.services.clone(),
        }
    }
}

impl AppContext {
    pub(crate) fn new(backend: StorageHandle, permissions: Arc<dyn PermissionBackend>) -> Self {
        let services = Services::from_storage(backend.clone());
        Self {
            backend,
            permissions: Arc::new(SwappablePermissionBackend::new(permissions)),
            request_permissions: None,
            permission_revision: Arc::new(RwLock::new(None)),
            authentication_revision: Arc::new(RwLock::new(None)),
            permission_target_revision: Arc::new(RwLock::new(None)),
            authentication_target_revision: Arc::new(RwLock::new(None)),
            settings_apply: Arc::new(tokio::sync::Mutex::new(())),
            services,
        }
    }

    pub(crate) fn from_http_request(req: &HttpRequest) -> Result<Self, ApiError> {
        if let Some(context) = req.app_data::<Data<Self>>() {
            return Ok(context.get_ref().clone());
        }

        Err(ApiError::InternalServerError(
            "Application context not found".to_string(),
        ))
    }
}

impl AppContext {
    pub(crate) fn backend(&self) -> &StorageHandle {
        &self.backend
    }

    pub(crate) fn clone_backend(&self) -> StorageHandle {
        self.backend.clone()
    }

    pub(crate) fn storage_backend_descriptor(&self) -> crate::storage::StorageBackendDescriptor {
        self.backend.descriptor()
    }

    pub fn permission_backend(&self) -> &dyn PermissionBackend {
        self.request_permissions
            .as_deref()
            .unwrap_or(self.permissions.as_ref())
    }

    pub(crate) fn current_permission_backend_kind(&self) -> &'static str {
        self.permissions.kind()
    }

    pub(crate) fn replace_permission_backend(&self, backend: Arc<dyn PermissionBackend>) {
        self.permissions.replace(backend);
    }

    pub(crate) fn mark_permission_backend_unavailable(&self, failure: String) {
        self.permissions.fail(failure);
    }

    pub(crate) fn permission_backend_is_ready(&self) -> bool {
        self.permissions.is_ready()
    }

    pub(crate) async fn refresh_runtime_settings(
        &self,
    ) -> Result<AppliedRuntimeSettings, crate::storage::StorageError> {
        let applied = self.backend.refresh_runtime_settings().await?;
        self.apply_runtime_settings(&applied).await;
        Ok(applied)
    }

    pub(crate) fn start_runtime_settings_reconciler(&self) {
        let context = self.clone();
        self.backend
            .start_runtime_settings_reconciler_with(move |applied| {
                let context = context.clone();
                Box::pin(async move {
                    context.apply_runtime_settings(&applied).await;
                })
            });
    }

    pub(crate) async fn apply_runtime_settings(&self, applied: &AppliedRuntimeSettings) {
        let _apply_guard = self.settings_apply.lock().await;
        let Some(snapshot) = applied.snapshot() else {
            return;
        };
        let authentication_revision = snapshot
            .record(hubuum_storage_core::StorageRuntimeSettingsGroup::Authentication)
            .revision();
        let current_authentication_target_revision = *self
            .authentication_target_revision
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !current_authentication_target_revision
            .is_some_and(|revision| revision > authentication_revision)
        {
            *self
                .authentication_target_revision
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(authentication_revision);
        }
        let applied_authentication_revision = *self
            .authentication_revision
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current_authentication_target_revision
            .is_none_or(|revision| revision <= authentication_revision)
            && !applied_authentication_revision
                .is_some_and(|revision| revision >= authentication_revision)
        {
            match crate::auth::apply_runtime_authentication_settings(snapshot.authentication())
                .await
            {
                Ok(()) => {
                    *self
                        .authentication_revision
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(authentication_revision);
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "Authentication settings could not be applied; retaining the last valid providers"
                    );
                }
            }
        }

        let revision = snapshot
            .record(hubuum_storage_core::StorageRuntimeSettingsGroup::Authorization)
            .revision();
        let current_permission_target_revision = *self
            .permission_target_revision
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current_permission_target_revision.is_some_and(|current| current > revision) {
            return;
        }
        *self
            .permission_target_revision
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(revision);
        let Ok(config) = crate::config::get_config() else {
            self.mark_permission_backend_unavailable(
                "authorization backend configuration is unavailable".to_string(),
            );
            return;
        };
        let config = config.clone();
        let selected = snapshot.authorization();
        let selected_name = selected.map_or_else(
            || match config.permission_backend {
                crate::config::PermissionBackendKind::Local => "local",
                crate::config::PermissionBackendKind::Treetop => "treetop",
            },
            hubuum_storage_core::StorageAuthorizationBackend::as_str,
        );
        let current_permission_revision = *self
            .permission_revision
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.permission_backend_is_ready()
            && self.current_permission_backend_kind() == selected_name
            && current_permission_revision == Some(revision)
        {
            return;
        }
        let backend = match selected {
            Some(selected) => {
                crate::permissions::build_permission_backend_for_runtime(
                    &config,
                    self.backend.clone(),
                    selected,
                )
                .await
            }
            None => {
                crate::permissions::build_permission_backend(&config, self.backend.clone()).await
            }
        };
        match backend {
            Ok(backend) => {
                self.replace_permission_backend(backend);
                *self
                    .permission_revision
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(revision);
            }
            Err(_) => {
                tracing::error!(
                    "Authorization backend reinitialization failed; permission checks are disabled"
                );
                self.mark_permission_backend_unavailable(
                    "authorization backend reinitialization failed".to_string(),
                );
            }
        }
    }

    /// Resolve administrator authority through the configured permission backend.
    /// Callers must separately enforce token scope and human-only IAM boundaries.
    pub(crate) async fn is_admin<S: PrincipalIdAccessor + ?Sized>(
        &self,
        subject: &S,
    ) -> Result<bool, ApiError> {
        let principal = PrincipalRef::load(self, subject).await?;
        self.permission_backend().is_admin(&principal).await
    }

    pub fn collection_service(&self) -> &CollectionService {
        self.services.collections()
    }

    pub fn class_service(&self) -> &ClassService {
        self.services.classes()
    }

    pub fn class_relation_service(&self) -> &ClassRelationService {
        self.services.class_relations()
    }

    pub fn object_service(&self) -> &ObjectService {
        self.services.objects()
    }

    pub fn object_relation_service(&self) -> &ObjectRelationService {
        self.services.object_relations()
    }
}

impl StorageContext for AppContext {}

/// Application capability that combines storage access with authorization
/// backend selection.
///
/// Storage-only services should accept [`StorageContext`]. Use this stronger
/// contract only when a use case must choose between local storage-backed
/// authorization and an external policy backend.
pub trait AuthorizationContext: StorageContext {
    fn authorization_mode(&self) -> AuthorizationMode<'_>;
}

impl AuthorizationContext for AppContext {
    fn authorization_mode(&self) -> AuthorizationMode<'_> {
        AuthorizationMode::from_backend(self.permission_backend())
    }
}

// Focused tests may use a bare storage handle to exercise the local policy
// implementation. Production permission-aware workflows must receive an
// AppContext so configured external-policy selection cannot be bypassed.
#[cfg(any(test, feature = "integration-test-support"))]
impl AuthorizationContext for StorageHandle {
    fn authorization_mode(&self) -> AuthorizationMode<'_> {
        AuthorizationMode::LocalStorage
    }
}

impl<T> AuthorizationContext for &T
where
    T: AuthorizationContext + ?Sized,
{
    fn authorization_mode(&self) -> AuthorizationMode<'_> {
        (*self).authorization_mode()
    }
}

impl<T> AuthorizationContext for Data<T>
where
    T: AuthorizationContext + ?Sized + 'static,
{
    fn authorization_mode(&self) -> AuthorizationMode<'_> {
        self.as_ref().authorization_mode()
    }
}

impl FromRequest for AppContext {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(Self::from_http_request(req))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::LocalPermissionBackend;

    #[test]
    fn permission_backend_is_unavailable_after_failed_reinitialization() {
        let storage = StorageHandle::memory();
        let backend = Arc::new(SwappablePermissionBackend::new(Arc::new(
            LocalPermissionBackend::new(storage, "admin".to_string()),
        )));
        backend.fail("authorization backend reinitialization failed".to_string());

        assert!(!backend.is_ready());
        assert_eq!(backend.kind(), "unavailable");
        assert!(backend.backend().is_err());
    }

    #[test]
    fn cloned_request_context_keeps_its_permission_backend_snapshot() {
        let storage = StorageHandle::memory();
        let first: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            storage.clone(),
            "admin".to_string(),
        ));
        let context = AppContext::new(storage.clone(), first.clone());
        let existing_request = context.clone();

        let second: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            storage,
            "other-admin".to_string(),
        ));
        context.replace_permission_backend(second.clone());
        let next_request = context.clone();

        assert!(std::ptr::eq(
            existing_request.permission_backend(),
            first.as_ref()
        ));
        assert!(std::ptr::eq(
            next_request.permission_backend(),
            second.as_ref()
        ));
    }
}
