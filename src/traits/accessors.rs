#![allow(async_fn_in_trait)]

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

#[doc(hidden)]
pub trait IdAccessor {
    fn accessor_id(&self) -> i32;
}

#[doc(hidden)]
pub trait InstanceAdapter<T> {
    async fn instance_adapter(&self, pool: &DbPool) -> Result<T, ApiError>;
}

impl<T, U> SelfAccessors<T> for U
where
    U: IdAccessor + InstanceAdapter<T>,
{
    fn id(&self) -> i32 {
        self.accessor_id()
    }

    async fn instance(&self, pool: &DbPool) -> Result<T, ApiError> {
        self.instance_adapter(pool).await
    }
}

impl<T> IdAccessor for &T
where
    T: IdAccessor + ?Sized,
{
    fn accessor_id(&self) -> i32 {
        (*self).accessor_id()
    }
}

impl<T, U> InstanceAdapter<U> for &T
where
    T: InstanceAdapter<U> + ?Sized,
{
    async fn instance_adapter(&self, pool: &DbPool) -> Result<U, ApiError> {
        (*self).instance_adapter(pool).await
    }
}

#[allow(async_fn_in_trait)]
pub trait NamespaceAccessors<N = Namespace, I = i32> {
    async fn namespace(&self, pool: &DbPool) -> Result<N, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

#[doc(hidden)]
pub trait NamespaceAdapter<N = Namespace, I = i32> {
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<N, ApiError>;
    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError>;
}

impl<T, N, I> NamespaceAccessors<N, I> for T
where
    T: NamespaceAdapter<N, I>,
{
    async fn namespace(&self, pool: &DbPool) -> Result<N, ApiError> {
        self.namespace_adapter(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<I, ApiError> {
        self.namespace_id_adapter(pool).await
    }
}

impl<T, N, I> NamespaceAdapter<N, I> for &T
where
    T: NamespaceAdapter<N, I> + ?Sized,
{
    async fn namespace_adapter(&self, pool: &DbPool) -> Result<N, ApiError> {
        (*self).namespace_adapter(pool).await
    }

    async fn namespace_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError> {
        (*self).namespace_id_adapter(pool).await
    }
}

pub trait ClassAccessors<C = HubuumClass, I = i32> {
    async fn class(&self, pool: &DbPool) -> Result<C, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

#[doc(hidden)]
pub trait ClassAdapter<C = HubuumClass, I = i32> {
    async fn class_adapter(&self, pool: &DbPool) -> Result<C, ApiError>;
    async fn class_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError>;
}

impl<T, C, I> ClassAccessors<C, I> for T
where
    T: ClassAdapter<C, I>,
{
    async fn class(&self, pool: &DbPool) -> Result<C, ApiError> {
        self.class_adapter(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<I, ApiError> {
        self.class_id_adapter(pool).await
    }
}

impl<T, C, I> ClassAdapter<C, I> for &T
where
    T: ClassAdapter<C, I> + ?Sized,
{
    async fn class_adapter(&self, pool: &DbPool) -> Result<C, ApiError> {
        (*self).class_adapter(pool).await
    }

    async fn class_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError> {
        (*self).class_id_adapter(pool).await
    }
}

pub trait ObjectAccessors<O = HubuumObject, I = i32> {
    #[allow(dead_code)]
    async fn object(&self, pool: &DbPool) -> Result<O, ApiError>;
    async fn object_id(&self, pool: &DbPool) -> Result<I, ApiError>;
}

#[doc(hidden)]
pub trait ObjectAdapter<O = HubuumObject, I = i32> {
    async fn object_adapter(&self, pool: &DbPool) -> Result<O, ApiError>;
    async fn object_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError>;
}

impl<T, O, I> ObjectAccessors<O, I> for T
where
    T: ObjectAdapter<O, I>,
{
    async fn object(&self, pool: &DbPool) -> Result<O, ApiError> {
        self.object_adapter(pool).await
    }

    async fn object_id(&self, pool: &DbPool) -> Result<I, ApiError> {
        self.object_id_adapter(pool).await
    }
}

impl<T, O, I> ObjectAdapter<O, I> for &T
where
    T: ObjectAdapter<O, I> + ?Sized,
{
    async fn object_adapter(&self, pool: &DbPool) -> Result<O, ApiError> {
        (*self).object_adapter(pool).await
    }

    async fn object_id_adapter(&self, pool: &DbPool) -> Result<I, ApiError> {
        (*self).object_id_adapter(pool).await
    }
}
