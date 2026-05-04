use std::sync::Arc;

use crate::db::DbPool;
use crate::traits::BackendContext;

use super::backend::PermissionBackend;

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

impl BackendContext for AppContext {
    fn db_pool(&self) -> &DbPool {
        &self.db_pool
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.permissions.as_ref()
    }
}
