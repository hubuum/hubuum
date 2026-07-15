use crate::db::prelude::*;
use diesel::sql_query;
use serde_json;

use crate::db::traits::GetObject;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::{
    Collection, HubuumClass, HubuumClassID, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    HubuumObjectRelationID, NewHubuumObject, NewHubuumObjectRelation, ObjectsByClass,
    UpdateHubuumObject,
};
use crate::traits::{ClassAccessors, SelfAccessors};

fn object_snapshot(object: &HubuumObject) -> serde_json::Value {
    serde_json::json!({
        "id": object.id,
        "name": object.name,
        "collection_id": object.collection_id,
        "hubuum_class_id": object.hubuum_class_id,
        "data": object.data,
        "description": object.description,
        "created_at": object.created_at,
        "updated_at": object.updated_at,
    })
}

fn object_event(
    object: &HubuumObject,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::Object, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(object.id)
            .with_entity_name(object.name.clone())
            .with_collection_id(object.collection_id)
            .with_metadata(serde_json::json!({ "class_id": object.hubuum_class_id })),
    )
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelationID {
    async fn object_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::db::prelude::*;
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObject>(
                obj_rel::hubuumobject_relation
                    .filter(obj_rel::id.eq(self.id()))
                    .inner_join(
                        obj::hubuumobject.on(obj::id
                            .eq(obj_rel::from_hubuum_object_id)
                            .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                    )
                    .select(obj::hubuumobject::all_columns()),
                conn,
            )
            .await
        })
        .await?;

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
        let objects = with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq_any(vec![self.from_hubuum_object_id, self.to_hubuum_object_id]))
                .load::<HubuumObject>(conn)
                .await
        })
        .await?;

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
        use crate::db::prelude::*;
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObject>(
                obj_rel::hubuumobject_relation
                    .filter(obj_rel::id.eq(self.id))
                    .inner_join(
                        obj::hubuumobject.on(obj::id
                            .eq(obj_rel::from_hubuum_object_id)
                            .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                    )
                    .select(obj::hubuumobject::all_columns()),
                conn,
            )
            .await
        })
        .await?;

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

        with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq(self.id()))
                .first::<HubuumObject>(conn)
                .await
        })
        .await
    }
}

pub trait CreateObjectRecord {
    async fn create_object_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObject, ApiError>;

    async fn create_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let _ = context;
        self.create_object_record_without_events(pool).await
    }
}

impl CreateObjectRecord for NewHubuumObject {
    async fn create_object_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::hubuumobject;

        with_connection(pool, async |conn| {
            diesel::insert_into(hubuumobject)
                .values(self)
                .get_result::<HubuumObject>(conn)
                .await
        })
        .await
    }

    async fn create_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let Some(context) = context else {
            return self.create_object_record_without_events(pool).await;
        };

        use crate::schema::hubuumobject::dsl::hubuumobject;

        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let object = diesel::insert_into(hubuumobject)
                .values(self)
                .get_result::<HubuumObject>(conn)
                .await?;
            let event = object_event(
                &object,
                Action::Created,
                context,
                format!("Object '{}' created", object.name),
            )?
            .with_after(object_snapshot(&object));
            emit_event(conn, &event).await?;
            Ok(object)
        })
        .await
    }
}

pub trait ValidateObjectSchema {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError>;
}

impl ValidateObjectSchema for HubuumObject {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        crate::utilities::json_schema::validate_json_value(schema, &self.data)
    }
}

impl ValidateObjectSchema for NewHubuumObject {
    fn validate_object_schema(&self, schema: &serde_json::Value) -> Result<(), ApiError> {
        crate::utilities::json_schema::validate_json_value(schema, &self.data)
    }
}

pub trait ValidateObjectRecord {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl ValidateObjectRecord for HubuumObject {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID::new(self.hubuum_class_id)?
            .class(pool)
            .await?;

        if class.validate_schema
            && let Some(ref schema) = class.json_schema
        {
            self.validate_object_schema(schema)?;
        }
        Ok(())
    }
}

impl ValidateObjectRecord for NewHubuumObject {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let class = HubuumClassID::new(self.hubuum_class_id)?
            .class(pool)
            .await?;

        if self.collection_id != class.collection_id {
            return Err(ApiError::BadRequest(format!(
                "Object collection_id {} does not match class collection_id {}",
                self.collection_id, class.collection_id
            )));
        }

        if class.validate_schema
            && let Some(ref schema) = class.json_schema
        {
            self.validate_object_schema(schema)?;
        }
        Ok(())
    }
}

impl ValidateObjectRecord for (&UpdateHubuumObject, i32) {
    async fn validate_object_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        let (update_obj, object_id) = self;
        let original = HubuumObjectID::new(*object_id)?.instance(pool).await?;
        let merged = original.merge_update(update_obj);
        let class = HubuumClassID::new(merged.hubuum_class_id)?
            .class(pool)
            .await?;

        if merged.collection_id != class.collection_id {
            return Err(ApiError::BadRequest(format!(
                "Object collection_id {} does not match class collection_id {}",
                merged.collection_id, class.collection_id
            )));
        }

        if class.validate_schema
            && let Some(ref schema) = class.json_schema
        {
            merged.validate_object_schema(schema)?;
        }
        Ok(())
    }
}

pub trait SaveObjectRecord {
    async fn save_object_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObject, ApiError>;

    async fn save_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let _ = context;
        self.save_object_record_without_events(pool).await
    }
}

impl SaveObjectRecord for HubuumObject {
    async fn save_object_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObject, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            collection_id: Some(self.collection_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };

        (&updated_object, self.id)
            .validate_object_record(pool)
            .await?;
        updated_object
            .update_object_record_without_events(pool, self.id)
            .await
    }

    async fn save_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let updated_object = UpdateHubuumObject {
            name: Some(self.name.clone()),
            collection_id: Some(self.collection_id),
            hubuum_class_id: Some(self.hubuum_class_id),
            data: Some(self.data.clone()),
            description: Some(self.description.clone()),
        };

        (&updated_object, self.id)
            .validate_object_record(pool)
            .await?;
        updated_object
            .update_object_record(pool, self.id, context)
            .await
    }
}

impl SaveObjectRecord for NewHubuumObject {
    async fn save_object_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObject, ApiError> {
        self.validate_object_record(pool).await?;
        self.create_object_record_without_events(pool).await
    }

    async fn save_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        self.validate_object_record(pool).await?;
        self.create_object_record(pool, context).await
    }
}

pub trait UpdateObjectRecord {
    async fn update_object_record_without_events(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError>;

    async fn update_object_record(
        &self,
        pool: &DbPool,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let _ = context;
        self.update_object_record_without_events(pool, object_id)
            .await
    }
}

impl UpdateObjectRecord for UpdateHubuumObject {
    async fn update_object_record_without_events(
        &self,
        pool: &DbPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, async |conn| {
            crate::db::updated_or_current(
                diesel::update(hubuumobject)
                    .filter(id.eq(object_id))
                    .set(self)
                    .get_result::<HubuumObject>(conn)
                    .await
                    .optional(),
                async || hubuumobject.filter(id.eq(object_id)).first(conn).await,
            )
            .await
        })
        .await
    }

    async fn update_object_record(
        &self,
        pool: &DbPool,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let Some(context) = context else {
            return self
                .update_object_record_without_events(pool, object_id)
                .await;
        };

        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let before = hubuumobject
                .filter(id.eq(object_id))
                .for_update()
                .first::<HubuumObject>(conn)
                .await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(hubuumobject.filter(id.eq(object_id)))
                .set(self)
                .get_result::<HubuumObject>(conn)
                .await?;
            let event = object_event(
                &updated,
                Action::Updated,
                context,
                format!("Object '{}' updated", updated.name),
            )?
            .with_before(object_snapshot(&before))
            .with_after(object_snapshot(&updated));
            emit_event(conn, &event).await?;
            Ok(updated)
        })
        .await
    }
}

pub trait DeleteObjectRecord {
    async fn delete_object_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError>;

    async fn delete_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_object_record_without_events(pool).await
    }
}

impl DeleteObjectRecord for HubuumObject {
    async fn delete_object_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumobject.filter(id.eq(self.id)))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_object_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_object_record_without_events(pool).await;
        };

        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let before = hubuumobject
                .filter(id.eq(self.id))
                .for_update()
                .first::<HubuumObject>(conn)
                .await?;
            diesel::delete(hubuumobject.filter(id.eq(self.id)))
                .execute(conn)
                .await?;
            let event = object_event(
                &before,
                Action::Deleted,
                context,
                format!("Object '{}' deleted", before.name),
            )?
            .with_before(object_snapshot(&before));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait ObjectCollectionLookup {
    async fn lookup_object_collection(&self, pool: &DbPool) -> Result<Collection, ApiError>;
}

impl ObjectCollectionLookup for HubuumObject {
    async fn lookup_object_collection(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, async |conn| {
            collections
                .filter(id.eq(self.collection_id))
                .first::<Collection>(conn)
                .await
        })
        .await
    }
}

impl ObjectCollectionLookup for HubuumObjectID {
    async fn lookup_object_collection(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        self.load_object_record(pool)
            .await?
            .lookup_object_collection(pool)
            .await
    }
}

pub trait ObjectClassLookup {
    async fn lookup_object_class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
}

impl ObjectClassLookup for HubuumObject {
    async fn lookup_object_class(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_connection(pool, async |conn| {
            hubuumclass
                .filter(id.eq(self.hubuum_class_id))
                .first::<HubuumClass>(conn)
                .await
        })
        .await
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

    with_connection(pool, async |conn| {
        hubuumobject.count().get_result::<i64>(conn).await
    })
    .await
}

pub async fn objects_per_class_count_from_backend(
    pool: &DbPool,
) -> Result<Vec<ObjectsByClass>, ApiError> {
    let raw_query =
        "SELECT hubuum_class_id, COUNT(*) as count FROM hubuumobject GROUP BY hubuum_class_id";
    with_connection(pool, async |conn| {
        sql_query(raw_query).load::<ObjectsByClass>(conn).await
    })
    .await
}
