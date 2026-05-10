use crate::db::DbPool;
use crate::permissions::backend::PermissionBackend;

/// Read-only access to the shared database pool.
///
/// Code paths that only need the DB connection — most helper functions
/// in `db/traits/*`, `models/*`, and `tasks/*` — should bound on
/// `&dyn DbPoolContext` rather than `&dyn BackendContext`. That way
/// callers can pass a bare `&DbPool` directly without going through
/// `AppContext`.
pub trait DbPoolContext {
    fn db_pool(&self) -> &DbPool;
}

/// Full application context: DB pool plus the active permission backend.
///
/// Permission-aware code (the `PermissionController` defaults, the
/// `can!` / `check_permissions!` macros, the `AdminAccess` extractor)
/// bounds on `&dyn BackendContext`. Production code receives this
/// through `AppContext`; tests construct one with `LocalPermissionBackend`
/// (or a mock) by default.
pub trait BackendContext: DbPoolContext {
    /// The active permission backend. Production code receives this through
    /// `AppContext`; tests construct an `AppContext` with the local backend
    /// (or a mock) by default.
    fn permission_backend(&self) -> &dyn PermissionBackend;
}

impl<T> DbPoolContext for &T
where
    T: DbPoolContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        (*self).db_pool()
    }
}

impl<T> DbPoolContext for std::sync::Arc<T>
where
    T: DbPoolContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }
}

impl<T> DbPoolContext for actix_web::web::Data<T>
where
    T: DbPoolContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }
}

impl<T> BackendContext for &T
where
    T: BackendContext + ?Sized,
{
    fn permission_backend(&self) -> &dyn PermissionBackend {
        (*self).permission_backend()
    }
}

impl<T> BackendContext for std::sync::Arc<T>
where
    T: BackendContext + ?Sized,
{
    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.as_ref().permission_backend()
    }
}

impl<T> BackendContext for actix_web::web::Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.as_ref().permission_backend()
    }
}

impl DbPoolContext for DbPool {
    fn db_pool(&self) -> &DbPool {
        self
    }
}
