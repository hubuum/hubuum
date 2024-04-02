use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::traits::GroupAccessors;

use crate::models::class::HubuumClass;
use crate::models::namespace::Namespace;
use crate::models::object::{HubuumObject, HubuumObjectID, NewHubuumObject, UpdateHubuumObject};
use crate::models::permissions::{NewPermission, Permission, Permissions, PermissionsList};
use crate::models::user::User;
use crate::traits::{
    CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors, ObjectAccessors,
    PermissionController, SelfAccessors,
};
use diesel::prelude::*;

//
// Save/Update/Delete
//
impl CanSave for HubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };
        updated_object.update(pool, self.id).await
    }
}

impl CanSave for NewHubuumObject {
    type Output = HubuumObject;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumobject)
            .values(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanUpdate for UpdateHubuumObject {
    type Output = HubuumObject;

    async fn update(&self, pool: &DbPool, object_id: i32) -> Result<Self::Output, ApiError> {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::update(hubuumobject)
            .filter(id.eq(object_id))
            .set(self)
            .get_result::<Self::Output>(&mut conn)?;

        Ok(result)
    }
}

impl CanDelete for HubuumObject {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        diesel::delete(hubuumobject.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

//
// Accessors
//
impl SelfAccessors<HubuumObject> for HubuumObject {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for HubuumObject {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let mut conn = pool.get()?;
        let namespace = namespaces
            .filter(id.eq(self.namespace_id))
            .first::<Namespace>(&mut conn)?;

        Ok(namespace)
    }

    async fn namespace_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace_id)
    }
}

impl ClassAccessors for HubuumObject {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.hubuum_class_id))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.hubuum_class_id)
    }
}

impl ObjectAccessors for HubuumObject {
    async fn object(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }

    async fn object_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl SelfAccessors<HubuumObject> for HubuumObjectID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        Ok(object)
    }
}

impl NamespaceAccessors for HubuumObjectID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl ClassAccessors for HubuumObjectID {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        let mut conn = pool.get()?;
        let object = hubuumobject
            .filter(id.eq(self.0))
            .first::<HubuumObject>(&mut conn)?;

        object.class(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.class(pool).await?.id)
    }
}

impl ObjectAccessors for HubuumObjectID {
    async fn object(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        self.instance(pool).await
    }

    async fn object_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }
}

impl PermissionController for HubuumObject {}
impl PermissionController for HubuumObjectID {}
