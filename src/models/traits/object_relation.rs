use diesel::prelude::*;

use crate::db::DbPool;

use crate::errors::ApiError;

use crate::models::{
    HubuumClassRelationID, HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID,
    NewHubuumObjectRelation,
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
        Ok(self.clone())
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

impl CanSave for NewHubuumObjectRelation {
    type Output = HubuumObjectRelation;

    async fn save(&self, pool: &DbPool) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }

        let class_rel = match HubuumClassRelationID(self.class_relation)
            .instance(&pool)
            .await
        {
            Ok(class_rel) => class_rel,
            Err(_) => {
                return Err(ApiError::NotFound("class_relation not found".to_string()));
            }
        };

        let obj1 = match HubuumObjectID(self.from_hubuum_object_id)
            .instance(&pool)
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
            .instance(&pool)
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

        if obj1.hubuum_class_id != class_rel.from_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "The class of from_hubuum_object_id must match the from_hubuum_class_id of class_relation"
                    .to_string(),
            ));
        }

        if obj2.hubuum_class_id != class_rel.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "The class of to_hubuum_object_id must match the to_hubuum_class_id of class_relation"
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
