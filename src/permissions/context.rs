use std::sync::Arc;

use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};

#[cfg(any(test, feature = "integration-test-support"))]
use crate::config::get_config;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::services::{
    ClassRelationService, ClassService, CollectionService, ObjectRelationService, ObjectService,
    Services,
};
use crate::traits::{BackendContext, BackendHandle};

use super::backend::PermissionBackend;
#[cfg(any(test, feature = "integration-test-support"))]
use super::local::LocalPermissionBackend;

#[derive(Clone)]
pub struct AppContext {
    backend: BackendHandle,
    permissions: Arc<dyn PermissionBackend>,
    services: Services,
}

impl AppContext {
    pub fn new(db_pool: DbPool, permissions: Arc<dyn PermissionBackend>) -> Self {
        let backend = BackendHandle::postgres(db_pool);
        let services = Services::from_lifecycle_storage(backend.lifecycle_storage());
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

        #[cfg(any(test, feature = "integration-test-support"))]
        if let Some(pool) = req.app_data::<Data<DbPool>>() {
            let admin_groupname = get_config()
                .map(|config| config.admin_groupname.clone())
                .unwrap_or_else(|_| "admin".to_string());
            return Ok(Self::new(
                pool.get_ref().clone(),
                Arc::new(LocalPermissionBackend::new(
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
    pub(crate) fn backend(&self) -> &BackendHandle {
        &self.backend
    }

    pub(crate) fn clone_backend(&self) -> BackendHandle {
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

impl BackendContext for AppContext {
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
