use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::object::HubuumObject;
use crate::models::user::UserID;

pub trait CanDelete {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError>;
}

pub trait CanSave {
    type Output;
    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError>;
}

pub trait CanUpdate {
    type Output;

    async fn update(&self, pool: &DbPool, entry_id: i32) -> Result<Self::Output, ApiError>;
}

// This trait is used to provide a uniform interface for both EntityID
// and Entity types, ie User and UserID.
pub trait SelfAccessors<T> {
    fn id(&self) -> i32;
    async fn instance(&self, pool: &DbPool) -> Result<T, ApiError>;
}

pub trait NamespaceAccessors {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait ClassAccessors {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait ObjectAccessors {
    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;
    async fn object_id(&self, pool: &DbPool) -> Result<i32, ApiError>;
}

pub trait PermissionCheck {
    type PermissionType;

    async fn user_can(
        &self,
        pool: &DbPool,
        user_id: UserID,
        permission: Self::PermissionType,
    ) -> Result<bool, ApiError>;
}
