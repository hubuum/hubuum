use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;
use serde_json;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::{
    Collection, HubuumClass, HubuumClassID, HubuumObject, HubuumObjectID, HubuumObjectRelation,
    HubuumObjectRelationID, NewHubuumObject, NewHubuumObjectRelation, ObjectDataPatchDocument,
    ObjectSelector, ObjectSelectorKind, ResolvedClassTarget, ResolvedObjectTarget,
    UpdateHubuumObject,
};
use crate::storage::postgres::operations::GetObject;
use crate::storage::postgres::operations::class::{
    HubuumClassRow, LoadClassRecord, lock_resolved_class_target,
};
use crate::storage::postgres::operations::collection::CollectionRow;
use crate::storage::postgres::operations::computed_field::{
    acquire_computed_class_shared_lock, materialize_object_in_transaction,
};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{PostgresConnection, with_connection, with_transaction};
use crate::traits::{CursorPaginated, CursorValue, SelfAccessors};

/// PostgreSQL representation of an object row.
///
/// The domain object is intentionally free of Diesel and schema bindings. All
/// query construction and row decoding stay inside the PostgreSQL adapter.
#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct HubuumObjectRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) collection_id: i32,
    pub(crate) hubuum_class_id: i32,
    pub(crate) data: serde_json::Value,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl From<HubuumObjectRow> for HubuumObject {
    fn from(row: HubuumObjectRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            collection_id: row.collection_id,
            hubuum_class_id: row.hubuum_class_id,
            data: row.data,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision.into_domain(),
        }
    }
}

impl CursorPaginated for HubuumObjectRow {
    fn supports_sort(field: &crate::models::search::FilterField) -> bool {
        HubuumObject::supports_sort(field)
    }

    fn cursor_value(
        &self,
        field: &crate::models::search::FilterField,
    ) -> Result<CursorValue, ApiError> {
        Ok(match field {
            crate::models::search::FilterField::Id => CursorValue::Integer(self.id.into()),
            crate::models::search::FilterField::Name => CursorValue::String(self.name.clone()),
            crate::models::search::FilterField::Description => {
                CursorValue::String(self.description.clone())
            }
            crate::models::search::FilterField::Collections
            | crate::models::search::FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id.into())
            }
            crate::models::search::FilterField::ClassId
            | crate::models::search::FilterField::Classes => {
                CursorValue::Integer(self.hubuum_class_id.into())
            }
            crate::models::search::FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            crate::models::search::FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            crate::models::search::FilterField::Revision => {
                CursorValue::Integer(self.revision.get())
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for objects"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        HubuumObject::default_sort()
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        HubuumObject::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumObjectRow {
    fn sql_field(field: &crate::models::search::FilterField) -> Result<CursorSqlField, ApiError> {
        use crate::models::search::FilterField;

        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumobject.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "hubuumobject.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "hubuumobject.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "hubuumobject.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::ClassId | FilterField::Classes => CursorSqlField {
                column: "hubuumobject.hubuum_class_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumobject.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumobject.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "hubuumobject.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for objects"
                )));
            }
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct NewHubuumObjectRow<'a> {
    name: &'a str,
    collection_id: i32,
    hubuum_class_id: i32,
    data: &'a serde_json::Value,
    description: &'a str,
}

impl<'a> From<&'a NewHubuumObject> for NewHubuumObjectRow<'a> {
    fn from(object: &'a NewHubuumObject) -> Self {
        Self {
            name: &object.name,
            collection_id: object.collection_id,
            hubuum_class_id: object.hubuum_class_id,
            data: &object.data,
            description: &object.description,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct UpdateHubuumObjectRow<'a> {
    name: Option<&'a str>,
    collection_id: Option<i32>,
    hubuum_class_id: Option<i32>,
    data: Option<&'a serde_json::Value>,
    description: Option<&'a str>,
}

impl<'a> From<&'a UpdateHubuumObject> for UpdateHubuumObjectRow<'a> {
    fn from(update: &'a UpdateHubuumObject) -> Self {
        Self {
            name: update.name.as_deref(),
            collection_id: update.collection_id,
            hubuum_class_id: update.hubuum_class_id,
            data: update.data.as_ref(),
            description: update.description.as_deref(),
        }
    }
}

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
        "revision": object.revision,
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

async fn acquire_object_write_class_advisory_lock(
    conn: &mut PostgresConnection,
    class_id: i32,
) -> Result<(), ApiError> {
    // Computed-definition mutations take the advisory lock before the class row lock.
    // Object writes must use the same order to avoid an advisory/row-lock cycle.
    acquire_computed_class_shared_lock(conn, class_id).await
}

async fn persist_new_object(
    conn: &mut PostgresConnection,
    object: &NewHubuumObject,
    context: Option<&EventContext>,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::hubuumobject;

    let object = diesel::insert_into(hubuumobject)
        .values(NewHubuumObjectRow::from(object))
        .get_result::<HubuumObjectRow>(conn)
        .await?
        .into();
    materialize_object_in_transaction(conn, &object).await?;

    if let Some(context) = context {
        let event = object_event(
            &object,
            Action::Created,
            context,
            format!("Object '{}' created", object.name),
        )?
        .with_after(object_snapshot(&object));
        emit_event(conn, &event).await?;
    }

    Ok(object)
}

async fn persist_locked_object_update(
    conn: &mut PostgresConnection,
    update: &UpdateHubuumObject,
    class: &HubuumClass,
    before: HubuumObject,
    context: Option<&EventContext>,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    update.validate_for_class(&before, class)?;
    if !update.has_changes(&before) {
        materialize_object_in_transaction(conn, &before).await?;
        return Ok(before);
    }
    let updated = diesel::update(hubuumobject.filter(id.eq(before.id)))
        .set(UpdateHubuumObjectRow::from(update))
        .get_result::<HubuumObjectRow>(conn)
        .await?
        .into();
    materialize_object_in_transaction(conn, &updated).await?;
    if let Some(context) = context {
        let event = object_event(
            &updated,
            Action::Updated,
            context,
            format!("Object '{}' updated", updated.name),
        )?
        .with_before(object_snapshot(&before))
        .with_after(object_snapshot(&updated));
        emit_event(conn, &event).await?;
    }
    Ok(updated)
}

async fn lock_object_and_update_class_by_id(
    conn: &mut PostgresConnection,
    object_id: i32,
    update: &UpdateHubuumObject,
) -> Result<(HubuumClass, HubuumObject), ApiError> {
    use crate::schema::hubuumclass::dsl as class;
    use crate::schema::hubuumobject::dsl as object;

    let current: HubuumObject = object::hubuumobject
        .filter(object::id.eq(object_id))
        .first::<HubuumObjectRow>(conn)
        .await?
        .into();
    let class_id = update.hubuum_class_id.unwrap_or(current.hubuum_class_id);
    acquire_object_write_class_advisory_lock(conn, class_id).await?;
    let class = class::hubuumclass
        .filter(class::id.eq(class_id))
        .for_update()
        .first::<HubuumClassRow>(conn)
        .await?
        .into();
    let object: HubuumObject = object::hubuumobject
        .filter(object::id.eq(object_id))
        .filter(object::hubuum_class_id.eq(current.hubuum_class_id))
        .filter(object::collection_id.eq(current.collection_id))
        .for_update()
        .first::<HubuumObjectRow>(conn)
        .await?
        .into();
    Ok((class, object))
}

async fn persist_locked_object_delete(
    conn: &mut PostgresConnection,
    before: HubuumObject,
    context: &EventContext,
) -> Result<(), ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    diesel::delete(hubuumobject.filter(id.eq(before.id)))
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
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelationID {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use crate::storage::postgres::prelude::*;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectRow>(
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

        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for NewHubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};
        let objects = with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq_any(vec![self.from_hubuum_object_id, self.to_hubuum_object_id]))
                .load::<HubuumObjectRow>(conn)
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
        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

impl GetObject<(HubuumObject, HubuumObject)> for HubuumObjectRelation {
    async fn object_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumObject, HubuumObject), ApiError> {
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;
        use crate::storage::postgres::prelude::*;

        let objects = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectRow>(
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

        Ok((objects[0].clone().into(), objects[1].clone().into()))
    }
}

pub trait LoadObjectRecord {
    async fn load_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError>;
}

impl LoadObjectRecord for HubuumObject {
    async fn load_object_record(
        &self,
        _pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        Ok(self.clone())
    }
}

impl LoadObjectRecord for HubuumObjectID {
    async fn load_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        use crate::schema::hubuumobject::dsl::{hubuumobject, id};

        with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq(self.id()))
                .first::<HubuumObjectRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }
}

pub trait CreateObjectRecord {
    async fn create_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError>;

    async fn create_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let _ = context;
        self.create_object_record_without_events(pool).await
    }
}

impl CreateObjectRecord for NewHubuumObject {
    async fn create_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            persist_new_object(conn, self, None).await
        })
        .await
    }

    async fn create_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let Some(context) = context else {
            return self.create_object_record_without_events(pool).await;
        };

        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            persist_new_object(conn, self, Some(context)).await
        })
        .await
    }
}

pub trait CreateObjectInResolvedClassRecord {
    async fn create_object_in_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>;
}

impl CreateObjectInResolvedClassRecord for NewHubuumObject {
    async fn create_object_in_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            acquire_object_write_class_advisory_lock(conn, target.class().id).await?;
            let class = lock_resolved_class_target(conn, target).await?;
            self.validate_for_class(&class)?;
            persist_new_object(conn, self, Some(context)).await
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
    async fn validate_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;
}

impl ValidateObjectRecord for HubuumObject {
    async fn validate_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        let class = HubuumClassID::new(self.hubuum_class_id)?
            .load_class_record(pool)
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
    async fn validate_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        let class = HubuumClassID::new(self.hubuum_class_id)?
            .load_class_record(pool)
            .await?;

        self.validate_for_class(&class)
    }
}

impl ValidateObjectRecord for (&UpdateHubuumObject, i32) {
    async fn validate_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        let (update_obj, object_id) = self;
        let original = HubuumObjectID::new(*object_id)?
            .load_object_record(pool)
            .await?;
        let merged = original.merge_update(update_obj);
        let class = HubuumClassID::new(merged.hubuum_class_id)?
            .load_class_record(pool)
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError>;

    async fn save_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let _ = context;
        self.save_object_record_without_events(pool).await
    }
}

impl SaveObjectRecord for HubuumObject {
    async fn save_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObject, ApiError> {
        self.validate_object_record(pool).await?;
        self.create_object_record_without_events(pool).await
    }

    async fn save_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        self.validate_object_record(pool).await?;
        self.create_object_record(pool, context).await
    }
}

pub trait UpdateObjectRecord {
    async fn update_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError>;

    async fn update_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
        object_id: i32,
    ) -> Result<HubuumObject, ApiError> {
        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let (class, before) = lock_object_and_update_class_by_id(conn, object_id, self).await?;
            persist_locked_object_update(conn, self, &class, before, None).await
        })
        .await
    }

    async fn update_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        object_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumObject, ApiError> {
        let Some(context) = context else {
            return self
                .update_object_record_without_events(pool, object_id)
                .await;
        };

        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let (class, before) = lock_object_and_update_class_by_id(conn, object_id, self).await?;
            persist_locked_object_update(conn, self, &class, before, Some(context)).await
        })
        .await
    }
}

pub trait PatchObjectDataRecord {
    async fn patch_object_data_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>;
}

async fn persist_locked_object_data_patch(
    conn: &mut PostgresConnection,
    patch: &ObjectDataPatchDocument,
    class: &HubuumClass,
    before: HubuumObject,
    context: &EventContext,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::{data, hubuumobject, id};

    let patched_data = patch.apply(&before.data)?;
    if class.validate_schema
        && let Some(schema) = class.json_schema.as_ref()
    {
        crate::utilities::json_schema::validate_json_value(schema, &patched_data)?;
    }

    if patched_data == before.data {
        materialize_object_in_transaction(conn, &before).await?;
        return Ok(before);
    }

    let updated = diesel::update(hubuumobject.filter(id.eq(before.id)))
        .set(data.eq(patched_data))
        .get_result::<HubuumObjectRow>(conn)
        .await?
        .into();
    materialize_object_in_transaction(conn, &updated).await?;
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
}

pub trait ResolveObjectSelectorRecord {
    async fn resolve_object_selector_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumClass, HubuumObject), ApiError>;
}

impl ResolveObjectSelectorRecord for ObjectSelector {
    async fn resolve_object_selector_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumClass, HubuumObject), ApiError> {
        use crate::schema::hubuumclass::dsl as class;
        use crate::schema::hubuumobject::dsl as object;

        with_connection(pool, async |conn| match self.kind() {
            ObjectSelectorKind::ById {
                class_id,
                object_id,
            } => {
                object::hubuumobject
                    .inner_join(class::hubuumclass)
                    .filter(object::id.eq(object_id.id()))
                    .filter(object::hubuum_class_id.eq(class_id.id()))
                    .select((
                        class::hubuumclass::all_columns(),
                        object::hubuumobject::all_columns(),
                    ))
                    .first::<(HubuumClassRow, HubuumObjectRow)>(conn)
                    .await
            }
            ObjectSelectorKind::ByName {
                class_name,
                object_name,
            } => {
                object::hubuumobject
                    .inner_join(class::hubuumclass)
                    .filter(class::name.eq(class_name))
                    .filter(object::name.eq(object_name))
                    .select((
                        class::hubuumclass::all_columns(),
                        object::hubuumobject::all_columns(),
                    ))
                    .first::<(HubuumClassRow, HubuumObjectRow)>(conn)
                    .await
            }
        })
        .await
        .map(|(class, object)| (class.into(), object.into()))
    }
}

async fn lock_resolved_object_target(
    conn: &mut PostgresConnection,
    target: &ResolvedObjectTarget,
) -> Result<(HubuumClass, HubuumObject), ApiError> {
    use crate::schema::hubuumclass::dsl as class;
    use crate::schema::hubuumobject::dsl as object;

    let resolved_class = target.class();
    let resolved = target.object();
    let owner_key = RevisionOwner::Object.key(resolved.id);
    acquire_object_write_class_advisory_lock(conn, resolved_class.id).await?;
    let locked_class = match target.selector().kind() {
        ObjectSelectorKind::ById {
            class_id,
            object_id: _,
        } => class::hubuumclass
            .filter(class::id.eq(class_id.id()))
            .filter(class::id.eq(resolved_class.id))
            .filter(class::name.eq(&resolved_class.name))
            .filter(class::collection_id.eq(resolved_class.collection_id))
            .for_update()
            .first::<HubuumClassRow>(conn)
            .await
            .optional()?,
        ObjectSelectorKind::ByName {
            class_name,
            object_name: _,
        } => class::hubuumclass
            .filter(class::id.eq(resolved_class.id))
            .filter(class::name.eq(class_name))
            .filter(class::collection_id.eq(resolved_class.collection_id))
            .for_update()
            .first::<HubuumClassRow>(conn)
            .await
            .optional()?,
    };
    let locked_class = locked_class.map(Into::into);
    let locked_class =
        crate::storage::postgres::require_existing_revision_target(locked_class, &owner_key)?;

    let locked_object = match target.selector().kind() {
        ObjectSelectorKind::ById {
            class_id,
            object_id,
        } => object::hubuumobject
            .filter(object::id.eq(object_id.id()))
            .filter(object::id.eq(resolved.id))
            .filter(object::name.eq(&resolved.name))
            .filter(object::collection_id.eq(resolved.collection_id))
            .filter(object::hubuum_class_id.eq(class_id.id()))
            .filter(object::hubuum_class_id.eq(resolved.hubuum_class_id))
            .for_update()
            .first::<HubuumObjectRow>(conn)
            .await
            .optional()?,
        ObjectSelectorKind::ByName {
            class_name: _,
            object_name,
        } => object::hubuumobject
            .filter(object::id.eq(resolved.id))
            .filter(object::hubuum_class_id.eq(resolved.hubuum_class_id))
            .filter(object::collection_id.eq(resolved.collection_id))
            .filter(object::name.eq(object_name))
            .for_update()
            .first::<HubuumObjectRow>(conn)
            .await
            .optional()?,
    };
    let locked_object =
        crate::storage::postgres::require_existing_revision_target(locked_object, &owner_key)?;

    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &owner_key,
        locked_object.revision,
    )
    .await?;

    Ok((locked_class, locked_object.into()))
}

impl PatchObjectDataRecord for ObjectDataPatchDocument {
    async fn patch_object_data_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let (class, before) = lock_resolved_object_target(conn, target).await?;
            persist_locked_object_data_patch(conn, self, &class, before, context).await
        })
        .await
    }
}

pub trait UpdateResolvedObjectRecord {
    async fn update_resolved_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError>;
}

impl UpdateResolvedObjectRecord for UpdateHubuumObject {
    async fn update_resolved_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedObjectTarget,
        context: &EventContext,
    ) -> Result<HubuumObject, ApiError> {
        with_transaction(pool, async |conn| -> Result<HubuumObject, ApiError> {
            let (class, before) = lock_resolved_object_target(conn, target).await?;
            persist_locked_object_update(conn, self, &class, before, Some(context)).await
        })
        .await
    }
}

pub trait DeleteResolvedObjectRecord {
    async fn delete_resolved_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteResolvedObjectRecord for ResolvedObjectTarget {
    async fn delete_resolved_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let (_, before) = lock_resolved_object_target(conn, self).await?;
            persist_locked_object_delete(conn, before, context).await
        })
        .await
    }
}

pub trait DeleteObjectRecord {
    async fn delete_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn delete_object_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_object_record_without_events(pool).await
    }
}

impl DeleteObjectRecord for HubuumObject {
    async fn delete_object_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
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
        pool: &crate::storage::postgres::PostgresPool,
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
                .first::<HubuumObjectRow>(conn)
                .await?
                .into();
            persist_locked_object_delete(conn, before, context).await
        })
        .await
    }
}

pub trait ObjectCollectionLookup {
    async fn lookup_object_collection(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError>;
}

impl ObjectCollectionLookup for HubuumObject {
    async fn lookup_object_collection(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, async |conn| {
            collections
                .filter(id.eq(self.collection_id))
                .first::<CollectionRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }
}

impl ObjectCollectionLookup for HubuumObjectID {
    async fn lookup_object_collection(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError> {
        self.load_object_record(pool)
            .await?
            .lookup_object_collection(pool)
            .await
    }
}

pub trait ObjectClassLookup {
    async fn lookup_object_class(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError>;
}

impl ObjectClassLookup for HubuumObject {
    async fn lookup_object_class(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        let row = with_connection(pool, async |conn| {
            hubuumclass
                .filter(id.eq(self.hubuum_class_id))
                .first::<HubuumClassRow>(conn)
                .await
        })
        .await?;
        Ok(row.into())
    }
}

impl ObjectClassLookup for HubuumObjectID {
    async fn lookup_object_class(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        self.load_object_record(pool)
            .await?
            .lookup_object_class(pool)
            .await
    }
}
