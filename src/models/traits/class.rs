use diesel::prelude::*;

use crate::traits::{
    CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors, PermissionController,
    SelfAccessors,
};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::traits::GroupAccessors;

use crate::models::{
    HubuumClass, HubuumClassID, Namespace, NewHubuumClass, PermissionsList, UpdateHubuumClass, User,
};

impl CanSave for HubuumClass {
    type Output = HubuumClass;

    async fn save(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let update = UpdateHubuumClass {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            json_schema: Some(self.json_schema.clone()),
            validate_schema: Some(self.validate_schema),
            description: Some(self.description.clone()),
        };

        update.update(pool, self.id).await
    }
}

impl CanDelete for HubuumClass {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumclass.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanSave for NewHubuumClass {
    type Output = HubuumClass;

    async fn save(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::*;

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumclass)
            .values(self)
            .get_result(&mut conn)?;

        Ok(result)
    }
}

impl CanUpdate for UpdateHubuumClass {
    type Output = HubuumClass;

    async fn update(&self, pool: &DbPool, class_id: i32) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let result = diesel::update(hubuumclass.filter(id.eq(class_id)))
            .set(self)
            .get_result(&mut conn)?;

        Ok(result)
    }
}

impl SelfAccessors<HubuumClass> for HubuumClass {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumClass, ApiError> {
        Ok(self.clone())
    }
}

impl ClassAccessors for HubuumClass {
    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }

    async fn class(&self, _pool: &DbPool) -> Result<HubuumClass, ApiError> {
        Ok(self.clone())
    }
}

impl NamespaceAccessors for HubuumClass {
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

impl SelfAccessors<HubuumClass> for HubuumClassID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.class(pool).await
    }
}

impl ClassAccessors for HubuumClassID {
    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }

    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.0))
            .first::<HubuumClass>(&mut conn)?;

        Ok(class)
    }
}

impl NamespaceAccessors for HubuumClassID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let mut conn = pool.get()?;
        let class = hubuumclass
            .filter(id.eq(self.0))
            .first::<HubuumClass>(&mut conn)?;

        class.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl PermissionController for HubuumClass {}
