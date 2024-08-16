use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use tracing::{debug, trace};

use serde::{Deserialize, Serialize};

use crate::db::traits::GetNamespace;
use crate::db::DbPool;
use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

use crate::models::{
    ClassClosureView, HubuumClass, HubuumClassRelation, HubuumClassRelationID, HubuumClassWithPath,
    Namespace, NewHubuumClassRelation,
};
use crate::traits::{CanDelete, CanSave, ClassAccessors, NamespaceAccessors, SelfAccessors};

impl SelfAccessors<HubuumClassRelation> for HubuumClassRelationID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumclass_relation
            .filter(id.eq(self.0))
            .first::<HubuumClassRelation>(&mut conn)?;

        Ok(class)
    }
}
impl SelfAccessors<HubuumClassRelation> for HubuumClassRelation {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        Ok(*self)
    }
}

impl CanDelete for HubuumClassRelation {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumclass_relation.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanSave for NewHubuumClassRelation {
    type Output = HubuumClassRelation;

    async fn save(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        if self.from_hubuum_class_id == self.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_class_id and to_hubuum_class_id cannot be the same".to_string(),
            ));
        }

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumclass_relation)
            .values(self)
            .get_result(&mut conn)?;

        Ok(result)
    }
}

impl CanDelete for HubuumClassRelationID {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.instance(pool).await?.delete(pool).await
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for NewHubuumClassRelation {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for HubuumClassRelation {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelation {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for HubuumClassRelationID {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        self.instance(pool).await?.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.namespace_id(pool).await
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelationID {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.class_id(pool).await
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for NewHubuumClassRelation {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}

impl ClassClosureView {
    pub fn to_ascendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.ancestor_class_id,
            name: self.ancestor_name.clone(),
            namespace_id: self.ancestor_namespace_id,
            description: self.ancestor_description.clone(),
            json_schema: self.ancestor_json_schema.clone(),
            validate_schema: self.ancestor_validate_schema,
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
        }
    }

    pub fn to_descendant_class(&self) -> HubuumClass {
        HubuumClass {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
        }
    }

    pub fn to_descendant_class_with_path(&self) -> HubuumClassWithPath {
        HubuumClassWithPath {
            id: self.descendant_class_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            description: self.descendant_description.clone(),
            json_schema: self.descendant_json_schema.clone(),
            validate_schema: self.descendant_validate_schema,
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }
}

#[allow(dead_code)]
pub trait ToHubuumClasses {
    fn to_descendant_classes(self) -> Vec<HubuumClass>;
    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath>;
    fn to_ascendant_classes(self) -> Vec<HubuumClass>;
}

impl ToHubuumClasses for Vec<ClassClosureView> {
    fn to_descendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class())
            .collect()
    }

    fn to_descendant_classes_with_path(self) -> Vec<HubuumClassWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_class_with_path())
            .collect()
    }

    fn to_ascendant_classes(self) -> Vec<HubuumClass> {
        self.into_iter()
            .map(|ocv| ocv.to_ascendant_class())
            .collect()
    }
}
