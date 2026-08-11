use crate::storage::postgres::prelude::*;

use crate::api::etag::RevisionOwner;
use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::models::{
    ClassIdSet, ClassSelector, ClassSelectorKind, Collection, HubuumClass, HubuumClassID,
    HubuumClassRelation, HubuumClassRelationID, NewHubuumClass, NewHubuumClassRelation,
    ResolvedClassTarget, UpdateHubuumClass,
};
use crate::storage::postgres::operations::GetClass;
use crate::storage::postgres::operations::event_record::emit_event;
use crate::storage::postgres::{with_connection, with_transaction};

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
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<HubuumClass, diesel::result::Error> {
                let class = hubuumclass
                    .filter(id.eq(self.id))
                    .first::<HubuumClass>(conn)
                    .await?;
                Ok(class)
            },
        )
        .await
    }
}

impl GetClass for HubuumClassID {
    async fn class_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<HubuumClass, diesel::result::Error> {
                let class = hubuumclass
                    .filter(id.eq(self.id()))
                    .first::<HubuumClass>(conn)
                    .await?;
                Ok(class)
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(id.eq(self.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                let to_class = hubuumclass
                    .filter(id.eq(self.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelationID {
    async fn class_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id as rel_id};

        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let relation = hubuumclass_relation
                    .filter(rel_id.eq(self.id()))
                    .first::<HubuumClassRelation>(conn)
                    .await?;

                let from_class = hubuumclass
                    .filter(hid.eq(relation.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                let to_class = hubuumclass
                    .filter(hid.eq(relation.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for NewHubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};

        with_connection(
            pool,
            async |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(hid.eq(self.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                let to_class = hubuumclass
                    .filter(hid.eq(self.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                Ok((from_class, to_class))
            },
        )
        .await
    }
}

pub trait LoadClassRecord {
    async fn load_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError>;
}

impl LoadClassRecord for HubuumClass {
    async fn load_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

impl LoadClassRecord for HubuumClassID {
    async fn load_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

pub trait ResolveClassSelectorRecord {
    async fn resolve_class_selector_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError>;
}

impl ResolveClassSelectorRecord for ClassSelector {
    async fn resolve_class_selector_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id, name};

        with_connection(pool, async |conn| match self.kind() {
            ClassSelectorKind::ById(class_id) => {
                hubuumclass
                    .filter(id.eq(class_id.id()))
                    .first::<HubuumClass>(conn)
                    .await
            }
            ClassSelectorKind::ByName(class_name) => {
                hubuumclass
                    .filter(name.eq(class_name))
                    .first::<HubuumClass>(conn)
                    .await
            }
        })
        .await
    }
}

pub trait CreateClassRecord {
    async fn create_class_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError>;

    async fn create_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let _ = context;
        self.create_class_record_without_events(pool).await
    }
}

impl CreateClassRecord for NewHubuumClass {
    async fn create_class_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::hubuumclass;

        with_connection(pool, async |conn| {
            diesel::insert_into(hubuumclass)
                .values(self)
                .get_result(conn)
                .await
        })
        .await
    }

    async fn create_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumClass, ApiError> {
        let Some(context) = context else {
            return self.create_class_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::hubuumclass;

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let class = diesel::insert_into(hubuumclass)
                .values(self)
                .get_result::<HubuumClass>(conn)
                .await?;
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
        pool: &impl crate::storage::StorageContext,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError>;

    async fn update_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_transaction(pool, async |conn| -> Result<HubuumClass, ApiError> {
            let before = hubuumclass
                .filter(id.eq(class_id))
                .for_update()
                .first::<HubuumClass>(conn)
                .await?;
            self.validate_schema_update(&before)?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            Ok(diesel::update(hubuumclass.filter(id.eq(class_id)))
                .set(self)
                .get_result::<HubuumClass>(conn)
                .await?)
        })
        .await
    }

    async fn update_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
                .first::<HubuumClass>(conn)
                .await?;
            self.validate_schema_update(&before)?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(hubuumclass.filter(id.eq(class_id)))
                .set(self)
                .get_result::<HubuumClass>(conn)
                .await?;
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
            .first::<HubuumClass>(conn)
            .await
            .optional()?,
        ClassSelectorKind::ByName(class_name) => hubuumclass
            .filter(id.eq(resolved.id))
            .filter(name.eq(class_name))
            .filter(collection_id.eq(resolved.collection_id))
            .for_update()
            .first::<HubuumClass>(conn)
            .await
            .optional()?,
    };
    let owner_key = RevisionOwner::Class.key(resolved.id);
    let locked = crate::storage::postgres::require_existing_revision_target(locked, &owner_key)?;
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
        pool: &impl crate::storage::StorageContext,
        target: &ResolvedClassTarget,
        context: &EventContext,
    ) -> Result<HubuumClass, ApiError>;
}

impl UpdateResolvedClassRecord for UpdateHubuumClass {
    async fn update_resolved_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
                .set(self)
                .get_result::<HubuumClass>(conn)
                .await?;
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
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteResolvedClassRecord for ResolvedClassTarget {
    async fn delete_resolved_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError>;

    async fn delete_class_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_class_record_without_events(pool).await
    }
}

impl DeleteClassRecord for HubuumClass {
    async fn delete_class_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
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
                .first::<HubuumClass>(conn)
                .await?;
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
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError>;
}

impl ClassCollectionLookup for HubuumClass {
    async fn lookup_class_collection(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
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

impl ClassCollectionLookup for HubuumClassID {
    async fn lookup_class_collection(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        self.load_class_record(pool)
            .await?
            .lookup_class_collection(pool)
            .await
    }
}

pub async fn total_class_count_from_backend(
    pool: &impl crate::storage::StorageContext,
) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    with_connection(pool, async |conn| {
        hubuumclass.count().get_result::<i64>(conn).await
    })
    .await
}

impl ClassIdSet {
    /// Load the `(id, name)` pairs for the classes in this set. Ids without a matching row are
    /// simply absent from the result; callers that require completeness must check themselves.
    pub(crate) async fn load_names(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<(i32, String)>, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id, name};

        if self.is_empty() {
            return Ok(Vec::new());
        }

        let ids = self.as_slice().to_vec();
        with_connection(pool, async |conn| {
            hubuumclass
                .filter(id.eq_any(ids))
                .select((id, name))
                .load::<(i32, String)>(conn)
                .await
        })
        .await
    }
}
