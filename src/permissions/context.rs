use std::ops::Deref;
use std::sync::Arc;

use actix_web::{FromRequest, HttpRequest, dev::Payload, web::Data};
use futures_util::future::{Ready, ready};

#[cfg(test)]
use crate::config::get_config;
use crate::db::DbPool;
use crate::errors::ApiError;
use crate::traits::BackendContext;

use super::backend::PermissionBackend;
#[cfg(test)]
use super::local::LocalPermissionBackend;

#[derive(Clone)]
pub struct AppContext {
    pub db_pool: DbPool,
    pub permissions: Arc<dyn PermissionBackend>,
}

impl AppContext {
    pub fn new(db_pool: DbPool, permissions: Arc<dyn PermissionBackend>) -> Self {
        Self {
            db_pool,
            permissions,
        }
    }
}

impl Deref for AppContext {
    type Target = DbPool;

    fn deref(&self) -> &Self::Target {
        &self.db_pool
    }
}

impl AppContext {
    pub fn permission_backend(&self) -> &dyn PermissionBackend {
        self.permissions.as_ref()
    }
}

impl BackendContext for AppContext {
    fn db_pool(&self) -> &DbPool {
        &self.db_pool
    }

    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        Some(self.permissions.as_ref())
    }
}

impl FromRequest for AppContext {
    type Error = ApiError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _: &mut Payload) -> Self::Future {
        if let Some(context) = req.app_data::<Data<Self>>() {
            return ready(Ok(context.get_ref().clone()));
        }

        #[cfg(test)]
        if let Some(pool) = req.app_data::<Data<DbPool>>() {
            let admin_groupname = get_config()
                .map(|config| config.admin_groupname.clone())
                .unwrap_or_else(|_| "admin".to_string());
            return ready(Ok(Self::new(
                pool.get_ref().clone(),
                Arc::new(LocalPermissionBackend::new(
                    pool.get_ref().clone(),
                    admin_groupname,
                )),
            )));
        }

        ready(Err(ApiError::InternalServerError(
            "Application permission context not found".to_string(),
        )))
    }
}
