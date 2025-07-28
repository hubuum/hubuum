use diesel::prelude::*;

use crate::db::DbPool;

use crate::errors::ApiError;

use crate::models::{
    HubuumClassRelationID, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    HubuumObjectRelationID, HubuumObjectWithPath, NewHubuumObjectRelation, ObjectClosureView,
};
use crate::traits::{CanDelete, CanSave, SelfAccessors};

impl SelfAccessors<HubuumObjectRelation> for HubuumObjectRelationID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumobject_relation
            .filter(id.eq(self.0))
            .first::<HubuumObjectRelation>(&mut conn)?;

        Ok(class)
    }
}
impl SelfAccessors<HubuumObjectRelation> for HubuumObjectRelation {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        Ok(*self)
    }
}

impl CanDelete for HubuumObjectRelation {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumobject_relation.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanDelete for HubuumObjectRelationID {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumobject_relation.filter(id.eq(self.0))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanSave for NewHubuumObjectRelation {
    type Output = HubuumObjectRelation;

    async fn save(&self, pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }

        let obj1 = match HubuumObjectID(self.from_hubuum_object_id)
            .instance(pool)
            .await
        {
            Ok(obj1) => obj1,
            Err(_) => {
                return Err(ApiError::NotFound(
                    "from_hubuum_object_id not found".to_string(),
                ));
            }
        };

        let obj2 = match HubuumObjectID(self.to_hubuum_object_id)
            .instance(pool)
            .await
        {
            Ok(obj2) => obj2,
            Err(_) => {
                return Err(ApiError::NotFound(
                    "to_hubuum_object_id not found".to_string(),
                ));
            }
        };

        if obj1.hubuum_class_id == obj2.hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id must not have the same class"
                    .to_string(),
            ));
        }

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumobject_relation)
            .values(self)
            .get_result(&mut conn)?;

        Ok(result)
    }
}

impl ObjectClosureView {
    #[allow(dead_code)]
    pub fn to_descendant_object(&self) -> HubuumObject {
        HubuumObject {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
        }
    }

    pub fn to_descendant_object_with_path(&self) -> HubuumObjectWithPath {
        HubuumObjectWithPath {
            id: self.descendant_object_id,
            name: self.descendant_name.clone(),
            namespace_id: self.descendant_namespace_id,
            hubuum_class_id: self.descendant_class_id,
            data: self.descendant_data.clone(),
            description: self.descendant_description.clone(),
            created_at: self.descendant_created_at,
            updated_at: self.descendant_updated_at,
            path: self.path.clone(),
        }
    }

    #[allow(dead_code)]
    pub fn to_ascendant_object(&self) -> HubuumObject {
        HubuumObject {
            id: self.ancestor_object_id,
            name: self.ancestor_name.clone(),
            namespace_id: self.ancestor_namespace_id,
            hubuum_class_id: self.ancestor_class_id,
            data: self.ancestor_data.clone(),
            description: self.ancestor_description.clone(),
            created_at: self.ancestor_created_at,
            updated_at: self.ancestor_updated_at,
        }
    }
}

// Trait for converting iterators of ObjectClosureView to Vec<HubuumObject>
#[allow(dead_code)]
pub trait ToHubuumObjects {
    fn to_descendant_objects(self) -> Vec<HubuumObject>;
    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath>;
    fn to_ascendant_objects(self) -> Vec<HubuumObject>;
}

impl ToHubuumObjects for Vec<ObjectClosureView> {
    fn to_descendant_objects(self) -> Vec<HubuumObject> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object())
            .collect()
    }

    fn to_descendant_objects_with_path(self) -> Vec<HubuumObjectWithPath> {
        self.into_iter()
            .map(|ocv| ocv.to_descendant_object_with_path())
            .collect()
    }

    fn to_ascendant_objects(self) -> Vec<HubuumObject> {
        self.into_iter()
            .map(|ocv| ocv.to_ascendant_object())
            .collect()
    }
}
