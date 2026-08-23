use std::sync::Arc;

use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};

use crate::errors::ApiError;
use crate::services::{
    ClassRelationService, ClassService, CollectionService, ObjectRelationService, ObjectService,
    Services,
};
use crate::storage::StorageContext;
use crate::storage::StorageHandle;

use super::backend::PermissionBackend;

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

#[derive(Clone)]
pub struct AppContext {
    backend: StorageHandle,
    permissions: Arc<dyn PermissionBackend>,
    services: Services,
}

impl AppContext {
    pub(crate) fn new(backend: StorageHandle, permissions: Arc<dyn PermissionBackend>) -> Self {
        let services = Services::from_storage(backend.clone());
        Self {
            backend,
            permissions,
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
        self.permissions.as_ref()
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
        AuthorizationMode::from_backend(self.permissions.as_ref())
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
