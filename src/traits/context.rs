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

impl<T> BackendContext for std::sync::Arc<T>
where
    T: BackendContext + ?Sized,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }

    fn permission_backend(&self) -> &dyn PermissionBackend {
        self.as_ref().permission_backend()
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
/// incrementally. Removing this surfaces ~680 sites that take
/// `&dyn BackendContext` but only call `db_pool()`. The proper fix is to
/// split the trait into a `DbPoolContext` (just `db_pool()`) and
/// `BackendContext: DbPoolContext` (adds `permission_backend()`) — see the
/// follow-up to Task 3.8 for that decomposition.
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
