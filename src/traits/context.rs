use actix_web::web::Data;

use crate::db::DbPool;
use crate::permissions::PermissionBackend;

/// A thin adapter for public model APIs that need access to the shared database pool.
///
/// This trait exists so model-facing traits can accept either a raw [`DbPool`] or wrappers such
/// as `actix_web::web::Data<DbPool>` without naming those concrete types in every signature.
///
/// This is intentionally small and pragmatic. It is not a backend-agnostic capability model; it
/// simply provides access to the current database pool.
pub trait BackendContext {
    fn db_pool(&self) -> &DbPool;

    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        None
    }
}

impl BackendContext for DbPool {
    fn db_pool(&self) -> &DbPool {
        self
    }
}

impl<T> BackendContext for &T
where
    T: BackendContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        (*self).db_pool()
    }

    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        (*self).permission_backend()
    }
}

impl<T> BackendContext for Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }

    fn permission_backend(&self) -> Option<&dyn PermissionBackend> {
        self.as_ref().permission_backend()
    }
}
