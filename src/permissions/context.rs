use std::sync::Arc;

use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};

#[cfg(any(test, feature = "integration-test-support"))]
use crate::config::get_config;
use crate::errors::ApiError;
use crate::services::{
    ClassRelationService, ClassService, CollectionService, ObjectRelationService, ObjectService,
    Services,
};
use crate::storage::StorageContext;
use crate::storage::StorageHandle;
#[cfg(any(test, feature = "integration-test-support"))]
use crate::storage::postgres::PostgresPool;

use super::backend::PermissionBackend;
#[cfg(any(test, feature = "integration-test-support"))]
use super::local::LocalPermissionBackend;

#[derive(Clone)]
pub struct AppContext {
    backend: StorageHandle,
    permissions: Arc<dyn PermissionBackend>,
    services: Services,
}

impl AppContext {
    pub(crate) fn new(backend: StorageHandle, permissions: Arc<dyn PermissionBackend>) -> Self {
        let services = Services::from_lifecycle_storage(backend.lifecycle_storage());
        Self {
            backend,
            permissions,
            services,
        }
    }

    /// PostgreSQL composition helper retained for integration tests.
    #[doc(hidden)]
    #[cfg(any(test, feature = "integration-test-support"))]
    pub fn postgres(db_pool: PostgresPool, permissions: Arc<dyn PermissionBackend>) -> Self {
        Self::new(StorageHandle::postgres(db_pool), permissions)
    }

    pub(crate) fn from_http_request(req: &HttpRequest) -> Result<Self, ApiError> {
        if let Some(context) = req.app_data::<Data<Self>>() {
            return Ok(context.get_ref().clone());
        }

        #[cfg(any(test, feature = "integration-test-support"))]
        if let Some(pool) = req.app_data::<Data<PostgresPool>>() {
            let admin_groupname = get_config()
                .map(|config| config.admin_groupname.clone())
                .unwrap_or_else(|_| "admin".to_string());
            return Ok(Self::postgres(
                pool.get_ref().clone(),
                Arc::new(LocalPermissionBackend::postgres(
                    pool.get_ref().clone(),
                    admin_groupname,
                )),
            ));
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

impl StorageContext for AppContext {
    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        Some(self.permissions.as_ref())
    }
}

impl FromRequest for AppContext {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        ready(Self::from_http_request(req))
    }
}
