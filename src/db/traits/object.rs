use diesel::prelude::*;

use crate::db::traits::GetObject;
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{
    HubuumObject, HubuumObjectRelation, HubuumObjectRelationID, NewHubuumObjectRelation,
};

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelationID {
    async fn object_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use diesel::prelude::*;

        let objects = with_connection(pool, |conn| {
            obj_rel::hubuumobject_relation
                .filter(obj_rel::id.eq(self.0))
                .inner_join(
                    obj::hubuumobject.on(obj::id
                        .eq(obj_rel::from_hubuum_object_id)
                        .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                )
                .select(obj::hubuumobject::all_columns())
                .load::<HubuumObject>(conn)
        })?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                "Could not find two objects for object relation".to_string(),
            ));
        }

        Ok((objects[0].clone(), objects[1].clone()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for NewHubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        let objects = with_connection(pool, |conn| {
            hubuumobject
                .filter(id.eq_any(vec![self.from_hubuum_object_id, self.to_hubuum_object_id]))
                .load::<HubuumObject>(conn)
        })?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                format!(
                    "Could not find objects ({}, {}) for object relation",
                    self.from_hubuum_object_id, self.to_hubuum_object_id,
                )
                .to_string(),
            ));
        }
        Ok((objects[0].clone(), objects[1].clone()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use diesel::prelude::*;

        let objects = with_connection(pool, |conn| {
            obj_rel::hubuumobject_relation
                .filter(obj_rel::id.eq(self.id))
                .inner_join(
                    obj::hubuumobject.on(obj::id
                        .eq(obj_rel::from_hubuum_object_id)
                        .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                )
                .select(obj::hubuumobject::all_columns())
                .load::<HubuumObject>(conn)
        })?;

        if objects.len() != 2 {
            return Err(ApiError::NotFound(
                "Could not find two objects for object relation".to_string(),
            ));
        }

        Ok((objects[0].clone(), objects[1].clone()))
    }
}
