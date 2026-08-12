use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::{
    ClassIdSet, ClassSelector, ClassSelectorKind, Collection, HubuumClass, HubuumClassExpanded,
    HubuumClassID, HubuumClassRelation, HubuumClassRelationID, NewHubuumClass,
    NewHubuumClassRelation, ResolvedClassTarget, UpdateHubuumClass,
};
use crate::storage::postgres::operations::GetClass;
use crate::storage::postgres::operations::collection::CollectionRow;
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::operations::relation_rows::HubuumClassRelationRow;
use crate::storage::postgres::{with_connection, with_transaction};
use crate::traits::{CursorPaginated, CursorValue};

/// PostgreSQL representation of a class row.
///
/// Domain classes remain backend-neutral; Diesel schema bindings and physical
/// cursor columns are confined to this adapter row.
#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::hubuumclass)]
pub(crate) struct HubuumClassRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) collection_id: i32,
    pub(crate) json_schema: Option<serde_json::Value>,
    pub(crate) validate_schema: bool,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) revision: crate::models::ResourceRevision,
}

impl From<HubuumClassRow> for HubuumClass {
    fn from(row: HubuumClassRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            collection_id: row.collection_id,
            json_schema: row.json_schema,
            validate_schema: row.validate_schema,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            revision: row.revision,
        }
    }
}

impl CursorPaginated for HubuumClassRow {
    fn supports_sort(field: &crate::models::search::FilterField) -> bool {
        HubuumClassExpanded::supports_sort(field)
    }

    fn cursor_value(
        &self,
        field: &crate::models::search::FilterField,
    ) -> Result<CursorValue, ApiError> {
        use crate::models::search::FilterField;

        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id.into()),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::Collections | FilterField::CollectionId => {
                CursorValue::Integer(self.collection_id.into())
            }
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for classes"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        HubuumClassExpanded::default_sort()
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        HubuumClassExpanded::tie_breaker_sort()
    }
}

impl CursorSqlMapping for HubuumClassRow {
    fn sql_field(field: &crate::models::search::FilterField) -> Result<CursorSqlField, ApiError> {
        use crate::models::search::FilterField;

        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "hubuumclass.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "hubuumclass.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "hubuumclass.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Collections | FilterField::CollectionId => CursorSqlField {
                column: "hubuumclass.collection_id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "hubuumclass.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "hubuumclass.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "hubuumclass.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for classes"
                )));
            }
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumclass)]
pub(crate) struct NewHubuumClassRow<'a> {
    name: &'a str,
    collection_id: i32,
    json_schema: Option<&'a serde_json::Value>,
    validate_schema: Option<bool>,
    description: &'a str,
}

impl<'a> From<&'a NewHubuumClass> for NewHubuumClassRow<'a> {
    fn from(class: &'a NewHubuumClass) -> Self {
        Self {
            name: &class.name,
            collection_id: class.collection_id,
            json_schema: class.json_schema.as_ref(),
            validate_schema: class.validate_schema,
            description: &class.description,
        }
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::hubuumclass)]
pub(crate) struct UpdateHubuumClassRow<'a> {
    name: Option<&'a str>,
    collection_id: Option<i32>,
    json_schema: Option<&'a serde_json::Value>,
    validate_schema: Option<bool>,
    description: Option<&'a str>,
}

impl<'a> From<&'a UpdateHubuumClass> for UpdateHubuumClassRow<'a> {
    fn from(update: &'a UpdateHubuumClass) -> Self {
        Self {
            name: update.name.as_deref(),
            collection_id: update.collection_id,
            json_schema: update.json_schema.as_ref(),
            validate_schema: update.validate_schema,
            description: update.description.as_deref(),
        }
    }
}

fn class_snapshot(class: &HubuumClass) -> serde_json::Value {
    serde_json::json!({
        "id": class.id,
        "name": class.name,
        "collection_id": class.collection_id,
        "json_schema": class.json_schema,
        "validate_schema": class.validate_schema,
        "description": class.description,
        "created_at": class.created_at,
        "updated_at": class.updated_at,
        "revision": class.revision,
    })
}

fn class_event(
    class: &HubuumClass,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(
        NewEvent::new(EntityType::Class, action, context.actor_kind(), summary)?
            .with_context(context)
            .with_entity_id(class.id)
            .with_entity_name(class.name.clone())
            .with_collection_id(class.collection_id),
    )
}

impl GetClass for HubuumClass {
    async fn class_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<HubuumClass, diesel::result::Error> {
                let class = hubuumclass
                    .filter(id.eq(self.id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                Ok(class)
            },
        )
        .await
    }
}

impl GetClass for HubuumClassID {
    async fn class_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<HubuumClass, diesel::result::Error> {
                let class = hubuumclass
                    .filter(id.eq(self.id()))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                Ok(class)
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(id.eq(self.from_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                let to_class = hubuumclass
                    .filter(id.eq(self.to_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelationID {
    async fn class_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id as rel_id};

        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let relation = hubuumclass_relation
                    .filter(rel_id.eq(self.id()))
                    .first::<HubuumClassRelationRow>(conn)
                    .await?;

                let from_class = hubuumclass
                    .filter(hid.eq(relation.from_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                let to_class = hubuumclass
                    .filter(hid.eq(relation.to_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for NewHubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};

        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(hid.eq(self.from_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                let to_class = hubuumclass
                    .filter(hid.eq(self.to_hubuum_class_id))
                    .first::<HubuumClassRow>(conn)
                    .await?
                    .into();
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

pub trait LoadClassRecord {
    async fn load_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError>;
}

impl LoadClassRecord for HubuumClass {
    async fn load_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

impl LoadClassRecord for HubuumClassID {
    async fn load_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

pub trait ResolveClassSelectorRecord {
    async fn resolve_class_selector_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError>;
}

impl ResolveClassSelectorRecord for ClassSelector {
    async fn resolve_class_selector_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id, name};

        let row = with_connection(pool, async |conn| match self.kind() {
            ClassSelectorKind::ById(class_id) => {
                hubuumclass
                    .filter(id.eq(class_id.id()))
                    .first::<HubuumClassRow>(conn)
                    .await
            }
            ClassSelectorKind::ByName(class_name) => {
                hubuumclass
                    .filter(name.eq(class_name))
                    .first::<HubuumClassRow>(conn)
                    .await
            }
        })
        .await?;
        Ok(row.into())
    }
}

pub trait CreateClassRecord {
    async fn create_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError>;

    async fn create_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let _ = context;
        self.create_class_record_without_events(pool).await
    }
}

impl CreateClassRecord for NewHubuumClass {
    async fn create_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::hubuumclass;

        with_connection(pool, async |conn| {
            diesel::insert_into(hubuumclass)
                .values(NewHubuumClassRow::from(self))
                .get_result::<HubuumClassRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }

    async fn create_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let Some(context) = context else {
            return self.create_class_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::hubuumclass;

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let class = diesel::insert_into(hubuumclass)
                .values(NewHubuumClassRow::from(self))
                .get_result::<HubuumClassRow>(conn)
                .await?
                .into();
            let event = class_event(
                &class,
                Action::Created,
                context,
                format!("Class '{}' created", class.name),
            )?
            .with_after(class_snapshot(&class));
            emit_event(conn, &event).await?;
            Ok(class)
        })
        .await
    }
}

pub trait UpdateClassRecord {
    async fn update_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError>;

    async fn update_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let _ = context;
        self.update_class_record_without_events(pool, class_id)
            .await
    }
}

impl UpdateClassRecord for UpdateHubuumClass {
    async fn update_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let before = hubuumclass
                .filter(id.eq(class_id))
                .for_update()
                .first::<HubuumClassRow>(conn)
                .await?
                .into();
            self.validate_schema_update(&before)?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            Ok(diesel::update(hubuumclass.filter(id.eq(class_id)))
                .set(UpdateHubuumClassRow::from(self))
                .get_result::<HubuumClassRow>(conn)
                .await?
                .into())
        })
        .await
    }

    async fn update_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_id: i32,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let Some(context) = context else {
            return self
                .update_class_record_without_events(pool, class_id)
                .await;
        };

        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let before = hubuumclass
                .filter(id.eq(class_id))
                .for_update()
                .first::<HubuumClassRow>(conn)
                .await?
                .into();
            self.validate_schema_update(&before)?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(hubuumclass.filter(id.eq(class_id)))
                .set(UpdateHubuumClassRow::from(self))
                .get_result::<HubuumClassRow>(conn)
                .await?
                .into();
            let event = class_event(
                &updated,
                Action::Updated,
                context,
                format!("Class '{}' updated", updated.name),
            )?
            .with_before(class_snapshot(&before))
            .with_after(class_snapshot(&updated));
            emit_event(conn, &event).await?;
            Ok(updated)
        })
        .await
    }
}

pub(crate) async fn lock_resolved_class_target(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target: &ResolvedClassTarget,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, id, name};

    let resolved = target.class();
    let locked = match target.selector().kind() {
        ClassSelectorKind::ById(class_id) => hubuumclass
            .filter(id.eq(class_id.id()))
            .filter(id.eq(resolved.id))
            .filter(name.eq(&resolved.name))
            .filter(collection_id.eq(resolved.collection_id))
            .for_update()
            .first::<HubuumClassRow>(conn)
            .await
            .optional()?,
        ClassSelectorKind::ByName(class_name) => hubuumclass
            .filter(id.eq(resolved.id))
            .filter(name.eq(class_name))
            .filter(collection_id.eq(resolved.collection_id))
            .for_update()
            .first::<HubuumClassRow>(conn)
            .await
            .optional()?,
    };
    let locked = locked.map(Into::into);
    let owner_key = RevisionOwner::Class.key(resolved.id);
    let locked: HubuumClass =
        crate::storage::postgres::require_existing_revision_target(locked, &owner_key)?;
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &owner_key,
        locked.revision,
    )
    .await?;
    Ok(locked)
}

pub trait UpdateResolvedClassRecord {
    async fn update_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError>;
}

impl UpdateResolvedClassRecord for UpdateHubuumClass {
    async fn update_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let before = lock_resolved_class_target(conn, target).await?;
            self.validate_schema_update(&before)?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(hubuumclass.filter(id.eq(before.id)))
                .set(UpdateHubuumClassRow::from(self))
                .get_result::<HubuumClassRow>(conn)
                .await?
                .into();
            let event = class_event(
                &updated,
                Action::Updated,
                context,
                format!("Class '{}' updated", updated.name),
            )?
            .with_before(class_snapshot(&before))
            .with_after(class_snapshot(&updated));
            emit_event(conn, &event).await?;
            Ok(updated)
        })
        .await
    }
}

pub trait DeleteResolvedClassRecord {
    async fn delete_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteResolvedClassRecord for ResolvedClassTarget {
    async fn delete_resolved_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let before = lock_resolved_class_target(conn, self).await?;
            diesel::delete(hubuumclass.filter(id.eq(before.id)))
                .execute(conn)
                .await?;
            let event = class_event(
                &before,
                Action::Deleted,
                context,
                format!("Class '{}' deleted", before.name),
            )?
            .with_before(class_snapshot(&before));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait DeleteClassRecord {
    async fn delete_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn delete_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_class_record_without_events(pool).await
    }
}

impl DeleteClassRecord for HubuumClass {
    async fn delete_class_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumclass.filter(id.eq(self.id)))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_class_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_class_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let before = hubuumclass
                .filter(id.eq(self.id))
                .for_update()
                .first::<HubuumClassRow>(conn)
                .await?
                .into();
            diesel::delete(hubuumclass.filter(id.eq(self.id)))
                .execute(conn)
                .await?;
            let event = class_event(
                &before,
                Action::Deleted,
                context,
                format!("Class '{}' deleted", before.name),
            )?
            .with_before(class_snapshot(&before));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait ClassCollectionLookup {
    async fn lookup_class_collection(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError>;
}

impl ClassCollectionLookup for HubuumClass {
    async fn lookup_class_collection(
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

impl ClassCollectionLookup for HubuumClassID {
    async fn lookup_class_collection(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError> {
        self.load_class_record(pool)
            .await?
            .lookup_class_collection(pool)
            .await
    }
}

/// Load `(id, name)` pairs for a normalized class set. Missing ids are absent;
/// callers that require completeness must check the returned keys.
pub(crate) async fn load_class_names(
    pool: &crate::storage::postgres::PostgresPool,
    class_ids: &ClassIdSet,
) -> Result<Vec<(i32, String)>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id, name};

    if class_ids.is_empty() {
        return Ok(Vec::new());
    }

    let ids = class_ids.as_slice().to_vec();
    with_connection(pool, async |conn| {
        hubuumclass
            .filter(id.eq_any(ids))
            .select((id, name))
            .load::<(i32, String)>(conn)
            .await
    })
    .await
}
