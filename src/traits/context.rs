use crate::db::DbPool;
use crate::permissions::backend::PermissionBackend;

/// Provides access to shared application services.
///
/// `db_pool()` is always available. `permission_backend()` is only available
/// from `AppContext` and similar full-context wrappers — pure DB-only code
/// paths must take a `&DbPool` directly rather than going through this trait.
pub trait BackendContext {
    fn db_pool(&self) -> &DbPool;

    /// The active permission backend. Production code receives this through
    /// `AppContext`; tests construct an `AppContext` with the local backend
    /// (or a mock) by default.
    fn permission_backend(&self) -> &dyn PermissionBackend;
}

impl<T> BackendContext for &T
where
    T: BackendContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        (*self).db_pool()
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        (*self).permission_backend()
    }
}

impl<T> BackendContext for actix_web::web::Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.as_ref().permission_backend()
    }
}

/// TEMPORARY: kept so the multi-phase refactor in
/// `docs/superpowers/plans/2026-05-02-pluggable-permissions.md` can land
/// incrementally. Will be removed in Phase 2 (Task 3.8) once every call site
/// receives either an `AppContext` or a bare `&DbPool`. Calling
/// `permission_backend()` here panics; that surfaces any code path that
/// needs migration.
impl BackendContext for crate::db::DbPool {
    fn db_pool(&self) -> &crate::db::DbPool {
        self
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        panic!(
            "DbPool used as BackendContext for a permission-aware operation; \
             switch the caller to AppContext (see plans/2026-05-02-pluggable-permissions.md)"
        )
    }
}
