use std::sync::{Arc, RwLock};

use actix_web::{FromRequest, HttpMessage, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};
use hubuum_domain::ResourceRevision;

use crate::config::{ControlRecoveryOptions, PermissionBackendKind};
use crate::errors::ApiError;
use crate::services::deletion_recovery::TaskReceiptService;
use crate::services::{
    ClassRelationService, ClassService, CollectionService, ObjectRelationService, ObjectService,
    Services,
};
use crate::storage::StorageContext;
use crate::storage::StorageHandle;
use crate::storage::runtime_settings::AppliedRuntimeSettings;
use crate::traits::PrincipalIdAccessor;

use super::backend::{CompleteCollectionCandidateLimit, PermissionBackend};
use super::types::{
    AuthorizationResult, PermissionDecision, PermissionRequest, PrincipalRef, ResourceRef,
};

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

struct RequestPermissionSnapshot(Arc<dyn PermissionBackend>);

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

    fn snapshot(&self) -> Arc<dyn PermissionBackend> {
        let state = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &state.backend {
            Some(backend) => backend.clone(),
            None => Arc::new(Self {
                state: RwLock::new(PermissionBackendState {
                    backend: None,
                    failure: state.failure.clone(),
                }),
            }),
        }
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

    async fn authorize(
        &self,
        principal: &PrincipalRef,
        request: PermissionRequest,
    ) -> Result<PermissionDecision, ApiError> {
        self.backend()?.authorize(principal, request).await
    }

    async fn authorize_candidates(
        &self,
        principal: &PrincipalRef,
        requests: Vec<PermissionRequest>,
    ) -> Result<Vec<AuthorizationResult>, ApiError> {
        self.backend()?
            .authorize_candidates(principal, requests)
            .await
    }

    async fn authorize_task(
        &self,
        principal: &PrincipalRef,
        task: &ResourceRef,
    ) -> Result<PermissionDecision, ApiError> {
        self.backend()?.authorize_task(principal, task).await
    }

    async fn authorize_task_cancellation(
        &self,
        principal: &PrincipalRef,
        task: &ResourceRef,
    ) -> Result<PermissionDecision, ApiError> {
        self.backend()?
            .authorize_task_cancellation(principal, task)
            .await
    }

    async fn authorize_tasks(
        &self,
        principal: &PrincipalRef,
        tasks: &[ResourceRef],
    ) -> Result<Vec<PermissionDecision>, ApiError> {
        self.backend()?.authorize_tasks(principal, tasks).await
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

/// Keep explicit deployment selection distinct from runtime assignments.
#[derive(Clone)]
enum DeploymentAuthorization {
    Supplied(Arc<dyn PermissionBackend>),
    Configured(PermissionBackendKind),
}

impl DeploymentAuthorization {
    fn kind(&self) -> &'static str {
        match self {
            Self::Supplied(backend) => backend.kind(),
            Self::Configured(PermissionBackendKind::Local) => "local",
            Self::Configured(PermissionBackendKind::Treetop) => "treetop",
        }
    }
}

#[derive(Clone)]
pub struct AppContext {
    backend: StorageHandle,
    permissions: Arc<SwappablePermissionBackend>,
    deployment_authorization: DeploymentAuthorization,
    request_permissions: Option<Arc<dyn PermissionBackend>>,
    permission_revision: Arc<RwLock<Option<ResourceRevision>>>,
    authentication_revision: Arc<RwLock<Option<ResourceRevision>>>,
    permission_target_revision: Arc<RwLock<Option<ResourceRevision>>>,
    authentication_target_revision: Arc<RwLock<Option<ResourceRevision>>>,
    settings_apply: Arc<tokio::sync::Mutex<()>>,
    services: Services,
    task_receipts: TaskReceiptService,
    history_retention: ControlRecoveryOptions,
}

impl AppContext {
    pub(crate) fn new(backend: StorageHandle, permissions: Arc<dyn PermissionBackend>) -> Self {
        let services = Services::from_storage(backend.clone());
        Self {
            backend,
            deployment_authorization: DeploymentAuthorization::Supplied(permissions.clone()),
            permissions: Arc::new(SwappablePermissionBackend::new(permissions)),
            request_permissions: None,
            permission_revision: Arc::new(RwLock::new(None)),
            authentication_revision: Arc::new(RwLock::new(None)),
            permission_target_revision: Arc::new(RwLock::new(None)),
            authentication_target_revision: Arc::new(RwLock::new(None)),
            settings_apply: Arc::new(tokio::sync::Mutex::new(())),
            services,
            task_receipts: TaskReceiptService::default(),
            history_retention: ControlRecoveryOptions::default(),
        }
    }

    /// Production may start with a runtime-managed backend already selected.
    /// Releasing that assignment must restore the configured deployment kind.
    pub(crate) fn with_configured_deployment_authorization(
        mut self,
        kind: PermissionBackendKind,
    ) -> Self {
        self.deployment_authorization = DeploymentAuthorization::Configured(kind);
        self
    }

    pub(crate) fn with_task_receipts(mut self, service: TaskReceiptService) -> Self {
        self.task_receipts = service;
        self
    }

    pub(crate) fn with_history_retention(mut self, options: ControlRecoveryOptions) -> Self {
        self.history_retention = options;
        self
    }
    pub(crate) fn history_retention(&self) -> &ControlRecoveryOptions {
        &self.history_retention
    }
    pub(crate) fn task_receipts(&self) -> &TaskReceiptService {
        &self.task_receipts
    }

    pub(crate) fn from_http_request(req: &HttpRequest) -> Result<Self, ApiError> {
        if let Some(context) = req.app_data::<Data<Self>>() {
            let mut context = context.get_ref().clone();
            let existing = req
                .extensions()
                .get::<RequestPermissionSnapshot>()
                .map(|snapshot| snapshot.0.clone());
            let snapshot = match existing {
                Some(snapshot) => snapshot,
                None => {
                    let backend = context.permissions.snapshot();
                    req.extensions_mut()
                        .insert(RequestPermissionSnapshot(backend.clone()));
                    backend
                }
            };
            context.request_permissions = Some(snapshot);
            return Ok(context);
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

    pub(crate) fn applied_authentication_revision(&self) -> Option<ResourceRevision> {
        *self
            .authentication_revision
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
        if let Ok(config) = crate::config::runtime::for_storage(&self.backend) {
            if let Err(error) = crate::logger::reload_log_filter(&config.log_level) {
                tracing::warn!(%error, "Log filter could not be applied");
            }
            if let Err(error) = crate::observability::metrics::apply_runtime_config(
                &config,
                self.storage_backend_descriptor().kind().as_str(),
            ) {
                tracing::warn!(%error, "Metrics could not be applied");
            }
        }
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
                Ok(true) => {
                    *self
                        .authentication_revision
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(authentication_revision);
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "Authentication settings could not be applied; authentication fails closed"
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
        let selected = snapshot.authorization();
        let selected_name = selected.map_or_else(
            || self.deployment_authorization.kind(),
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
        let backend = match (&self.deployment_authorization, selected) {
            (DeploymentAuthorization::Supplied(backend), None) => Ok(backend.clone()),
            (deployment, selected) => {
                let Ok(config) = crate::config::get_config() else {
                    self.mark_permission_backend_unavailable(
                        "authorization backend configuration is unavailable".to_string(),
                    );
                    return;
                };
                let mut config = config.clone();
                if let DeploymentAuthorization::Configured(kind) = deployment {
                    config.permission_backend = *kind;
                }
                match selected {
                    Some(selected) => {
                        crate::permissions::build_permission_backend_for_runtime(
                            &config,
                            self.backend.clone(),
                            selected,
                        )
                        .await
                    }
                    None => {
                        crate::permissions::build_permission_backend(&config, self.backend.clone())
                            .await
                    }
                }
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

    #[rstest::rstest]
    #[case::supplied_unmanaged(false, false)]
    #[case::supplied_released(false, true)]
    #[case::configured_unmanaged(true, false)]
    #[case::configured_released(true, true)]
    #[actix_web::test]
    async fn runtime_settings_restore_the_captured_deployment_authorization(
        #[case] configured: bool,
        #[case] release: bool,
    ) {
        use crate::events::EventContext;
        use crate::permissions::test_support::MockTreetopBackend;
        use hubuum_storage_core::{
            RuntimeSettingsStorage, StorageAuthorizationBackend, StorageRuntimeSettingsAssignment,
            StorageRuntimeSettingsGroup, StorageRuntimeSettingsUpdate, StorageRuntimeSettingsValue,
        };

        let storage = StorageHandle::memory();
        let supplied: Arc<dyn PermissionBackend> = Arc::new(MockTreetopBackend::new());
        let mut context = AppContext::new(storage.clone(), supplied.clone());
        if configured {
            context =
                context.with_configured_deployment_authorization(PermissionBackendKind::Local);
        }
        if release {
            storage
                .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                    StorageRuntimeSettingsAssignment::Managed(
                        StorageRuntimeSettingsValue::Authorization(
                            StorageAuthorizationBackend::Local,
                        ),
                    ),
                    ResourceRevision::new(1).unwrap(),
                    EventContext::system(),
                ))
                .await
                .unwrap();
            context.refresh_runtime_settings().await.unwrap();
            assert!(
                context.permission_backend().uses_local_permission_store(),
                "managed precondition"
            );
            storage
                .update_runtime_settings(StorageRuntimeSettingsUpdate::new(
                    StorageRuntimeSettingsAssignment::Deployment(
                        StorageRuntimeSettingsGroup::Authorization,
                    ),
                    ResourceRevision::new(2).unwrap(),
                    EventContext::system(),
                ))
                .await
                .unwrap();
        }
        context.refresh_runtime_settings().await.unwrap();
        let selected = context.permissions.backend().unwrap();
        if configured {
            assert!(selected.uses_local_permission_store());
        } else {
            assert!(
                Arc::ptr_eq(&selected, &supplied),
                "Explicit external deployment policy must be retained"
            );
        }
    }

    #[rstest::rstest]
    #[case::single(false)]
    #[case::batch(true)]
    #[actix_web::test]
    async fn external_task_policy_can_deny_its_owner_through_the_runtime_proxy(
        #[case] batch: bool,
    ) {
        use crate::permissions::test_support::MockTreetopBackend;

        let context = AppContext::new(StorageHandle::memory(), Arc::new(MockTreetopBackend::new()));
        let principal = PrincipalRef::new(7, []);
        let task = ResourceRef::task(5, Some(principal.user_id));
        let decisions = if batch {
            context
                .permission_backend()
                .authorize_tasks(&principal, &[task])
                .await
                .unwrap()
        } else {
            vec![
                context
                    .permission_backend()
                    .authorize_task(&principal, &task)
                    .await
                    .unwrap(),
            ]
        };
        assert_eq!(decisions, vec![PermissionDecision::Deny]);
    }

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
    fn request_context_keeps_its_permission_backend_snapshot() {
        let storage = StorageHandle::memory();
        let first: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            storage.clone(),
            "admin".to_string(),
        ));
        let context = AppContext::new(storage.clone(), first.clone());
        let app_data = Data::new(context.clone());
        let request = actix_web::test::TestRequest::default()
            .app_data(app_data.clone())
            .to_http_request();
        let existing_request = AppContext::from_http_request(&request).unwrap();

        let second: Arc<dyn PermissionBackend> = Arc::new(LocalPermissionBackend::new(
            storage,
            "other-admin".to_string(),
        ));
        context.replace_permission_backend(second.clone());
        let same_request = AppContext::from_http_request(&request).unwrap();
        let next_request = AppContext::from_http_request(
            &actix_web::test::TestRequest::default()
                .app_data(app_data)
                .to_http_request(),
        )
        .unwrap();

        assert!(std::ptr::eq(
            existing_request.permission_backend(),
            first.as_ref()
        ));
        assert!(std::ptr::eq(
            next_request.permission_backend(),
            second.as_ref()
        ));
        assert!(std::ptr::eq(
            same_request.permission_backend(),
            first.as_ref()
        ));
    }

    #[test]
    fn new_request_snapshots_unavailable_backend() {
        let storage = StorageHandle::memory();
        let first = Arc::new(LocalPermissionBackend::new(storage, "admin".to_string()));
        let context = AppContext::new(StorageHandle::memory(), first.clone());
        let app_data = Data::new(context.clone());
        context.mark_permission_backend_unavailable("unavailable".to_string());
        let request = actix_web::test::TestRequest::default()
            .app_data(app_data)
            .to_http_request();
        let unavailable = AppContext::from_http_request(&request).unwrap();
        context.replace_permission_backend(first);
        assert_eq!(unavailable.permission_backend().kind(), "unavailable");
        assert!(
            !unavailable
                .permission_backend()
                .uses_local_permission_store()
        );
    }
}
