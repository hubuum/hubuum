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

/// TEMPORARY: kept until Phase 4.x finishes downgrading DB-only call
/// sites from `&dyn BackendContext` to `&dyn DbPoolContext`. After all
/// such sites are migrated, this impl is removed (Task 3.8). The panic
/// surfaces any code path still treating a bare `DbPool` as full
/// permission-aware context.
impl BackendContext for DbPool {
    fn permission_backend(&self) -> &dyn PermissionBackend {
        panic!(
            "DbPool used as BackendContext for a permission-aware operation; \
             switch the caller to AppContext (see plans/2026-05-02-pluggable-permissions.md)"
        )
    }
}
