use crate::db::DbPool;

pub trait BackendContext {
    fn db_pool(&self) -> &DbPool;
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
}

impl<T> BackendContext for actix_web::web::Data<T>
where
    T: BackendContext + ?Sized + 'static,
{
    fn db_pool(&self) -> &DbPool {
        self.as_ref().db_pool()
    }
}
