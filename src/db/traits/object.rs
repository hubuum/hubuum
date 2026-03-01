use diesel::prelude::*;
use diesel::sql_query;
use jsonschema;
use serde_json;

use crate::db::traits::GetObject;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    HubuumObjectRelationID, Namespace, NewHubuumObject, NewHubuumObjectRelation, ObjectsByClass,
    UpdateHubuumObject,
};
use crate::traits::{ClassAccessors, SelfAccessors};

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

pub trait LoadObjectRecord {
    async fn load_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;
}

impl LoadObjectRecord for HubuumObject {
    async fn load_object_record(&self, _pool: &DbPool) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl LoadObjectRecord for HubuumObjectID {
    async fn load_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, |conn| {
            hubuumobject
                .filter(id.eq(self.0))
                .first::<HubuumObject>(conn)
        })
    }
}

pub trait CreateObjectRecord {
    async fn create_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;
}

impl CreateObjectRecord for NewHubuumObject {
    async fn create_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::hubuumobject;

        with_connection(pool, |conn| {
            diesel::insert_into(hubuumobject)
                .values(self)
                .get_result::<HubuumObject>(conn)
        })
    }
}

pub trait ValidateObjectSchema {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError>;
}

impl ValidateObjectSchema for HubuumObject {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        jsonschema::validate(schema, &self.data)
            .map_err(|err| ApiError::ValidationError(err.to_string()))?;
        Ok(())
    }
}

impl ValidateObjectSchema for NewHubuumObject {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        jsonschema::validate(schema, &self.data)
            .map_err(|err| ApiError::ValidationError(err.to_string()))?;
        Ok(())
    }
}

pub trait ValidateObjectRecord {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl ValidateObjectRecord for HubuumObject {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID(self.hubuum_class_id).class(pool).await?;

        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                self.validate_object_schema(schema)?;
            }
        }
        Ok(())
    }
}

impl ValidateObjectRecord for NewHubuumObject {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID(self.hubuum_class_id).class(pool).await?;

        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                self.validate_object_schema(schema)?;
            }
        }
        Ok(())
    }
}

impl ValidateObjectRecord for (&UpdateHubuumObject, i32) {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let (update_obj, object_id) = self;
        let original = HubuumObjectID(*object_id).instance(pool).await?;
        let merged = original.merge_update(update_obj);
        let class = HubuumClassID(merged.hubuum_class_id).class(pool).await?;

        if class.validate_schema {
            if let Some(ref schema) = class.json_schema {
                merged.validate_object_schema(schema)?;
            }
        }
        Ok(())
    }
}

pub trait SaveObjectRecord {
    async fn save_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError>;
}

impl SaveObjectRecord for HubuumObject {
    async fn save_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            namespace_id: Some(self.namespace_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };

        (&updated_object, self.id)
            .validate_object_record(pool)
            .await?;
        updated_object.update_object_record(pool, self.id).await
    }
}

impl SaveObjectRecord for NewHubuumObject {
    async fn save_object_record(&self, pool: &DbPool) -> Result<HubuumObject, ApiError> {
        self.validate_object_record(pool).await?;
        self.create_object_record(pool).await
    }
}

pub trait UpdateObjectRecord {
    async fn update_object_record(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError>;
}

impl UpdateObjectRecord for UpdateHubuumObject {
    async fn update_object_record(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, |conn| {
            diesel::update(hubuumobject)
                .filter(id.eq(object_id))
                .set(self)
                .get_result::<HubuumObject>(conn)
        })
    }
}

pub trait DeleteObjectRecord {
    async fn delete_object_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteObjectRecord for HubuumObject {
    async fn delete_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumobject.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait ObjectNamespaceLookup {
    async fn lookup_object_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
}

impl ObjectNamespaceLookup for HubuumObject {
    async fn lookup_object_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            namespaces
                .filter(id.eq(self.namespace_id))
                .first::<Namespace>(conn)
        })
    }
}

impl ObjectNamespaceLookup for HubuumObjectID {
    async fn lookup_object_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.load_object_record(pool)
            .await?
            .lookup_object_namespace(pool)
            .await
    }
}

pub trait ObjectClassLookup {
    async fn lookup_object_class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
}

impl ObjectClassLookup for HubuumObject {
    async fn lookup_object_class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_connection(pool, |conn| {
            hubuumclass
                .filter(id.eq(self.hubuum_class_id))
                .first::<HubuumClass>(conn)
        })
    }
}

impl ObjectClassLookup for HubuumObjectID {
    async fn lookup_object_class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.load_object_record(pool)
            .await?
            .lookup_object_class(pool)
            .await
    }
}

pub async fn total_object_count_from_backend(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumobject::dsl::*;

    with_connection(pool, |conn| hubuumobject.count().get_result::<i64>(conn))
}

pub async fn objects_per_class_count_from_backend(
    pool: &DbPool,
) -> Result<Vec<ObjectsByClass>, ApiError> {
    let raw_query =
        "SELECT hubuum_class_id, COUNT(*) as count FROM hubuumobject GROUP BY hubuum_class_id";
    with_connection(pool, |conn| {
        sql_query(raw_query).load::<ObjectsByClass>(conn)
    })
}
