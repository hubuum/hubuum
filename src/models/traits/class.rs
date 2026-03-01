use diesel::prelude::*;

use crate::traits::{
    CanUpdate, ClassAccessors, NamespaceAccessors, PermissionController, SelfAccessors,
};

use crate::db::traits::class::{
    ClassNamespaceLookup, CreateClassRecord, DeleteClassRecord, LoadClassRecord, UpdateClassRecord,
};
use crate::traits::crud::{DeleteAdapter, SaveAdapter, UpdateAdapter};
use crate::db::DbPool;
use crate::errors::ApiError;

use crate::models::{
    HubuumClass, HubuumClassID, Namespace, NewHubuumClass, UpdateHubuumClass,
};

impl SaveAdapter for HubuumClass {
    type Output = HubuumClass;

    async fn save_adapter(&self, pool: &DbPool) -> Result<Self::Output, ApiError> {
        let update = UpdateHubuumClass {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            json_schema: self.json_schema.clone(),
            validate_schema: Some(self.validate_schema),
            description: Some(self.description.clone()),
        };

        update.update(pool, self.id).await
    }
}

impl DeleteAdapter for HubuumClass {
    async fn delete_adapter(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.delete_class_record(pool).await
    }
}

impl SaveAdapter for NewHubuumClass {
    type Output = HubuumClass;

    async fn save_adapter(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.create_class_record(pool).await
    }
}

impl UpdateAdapter for UpdateHubuumClass {
    type Output = HubuumClass;

    async fn update_adapter(&self, pool: &DbPool, class_id: i32) -> Result<HubuumClass, ApiError> {
        self.update_class_record(pool, class_id).await
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

impl SelfAccessors<HubuumClass> for &HubuumClass {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumClass, ApiError> {
        Ok((**self).clone())
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

impl ClassAccessors for &HubuumClass {
    async fn class(&self, _pool: &DbPool) -> Result<HubuumClass, ApiError> {
        Ok((**self).clone())
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.id)
    }
}

impl NamespaceAccessors for HubuumClass {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.lookup_class_namespace(pool).await
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

impl SelfAccessors<HubuumClass> for &HubuumClassID {
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
        self.load_class_record(pool).await
    }
}

impl ClassAccessors for &HubuumClassID {
    async fn class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        (*self).class(pool).await
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.0)
    }
}

impl NamespaceAccessors for HubuumClassID {
    async fn namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.lookup_class_namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<i32, ApiError> {
        Ok(self.namespace(pool).await?.id)
    }
}

impl PermissionController for HubuumClass {}
