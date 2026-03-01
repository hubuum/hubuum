use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::{HubuumClass, HubuumObject, Namespace};

// This trait is used to provide a uniform interface for both EntityID
// and Entity types, ie User and UserID.
#[allow(async_fn_in_trait)]
pub trait SelfAccessors<T> {
    fn id(&self) -> i32;
    async fn instance(&self, pool: &DbPool) -> Result<T, ApiError>;
}

#[allow(async_fn_in_trait)]
pub trait NamespaceAccessors<N = Namespace, I = i32> {
    async fn namespace(&self, pool: &DbPool) -> Result<N, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

pub trait ClassAccessors<C = HubuumClass, I = i32> {
    async fn class(&self, pool: &DbPool) -> Result<C, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

pub trait ObjectAccessors<O = HubuumObject, I = i32> {
    #[allow(dead_code)]
    async fn object(&self, pool: &DbPool) -> Result<O, ApiError>;
    async fn object_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}
