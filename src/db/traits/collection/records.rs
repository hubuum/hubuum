use super::*;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};

fn collection_snapshot(collection: &Collection) -> serde_json::Value {
    serde_json::json!({
        "id": collection.id,
        "name": collection.name,
        "description": collection.description,
        "created_at": collection.created_at,
        "updated_at": collection.updated_at,
    })
}

fn collection_event(
    collection: &Collection,
    action: Action,
    context: &EventContext,
    summary: impl Into<String>,
) -> Result<NewEvent, ApiError> {
    Ok(NewEvent::new(
        EntityType::Collection,
        action,
        context.actor_kind(),
        summary,
    )?
    .with_context(context)
    .with_entity_id(collection.id)
    .with_entity_name(collection.name.clone())
    .with_collection_id(collection.id))
}

pub trait DeleteCollectionRecord {
    async fn delete_collection_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError>;

    async fn delete_collection_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_collection_record_without_events(pool).await
    }
}

impl DeleteCollectionRecord for Collection {
    async fn delete_collection_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, |conn| {
            diesel::delete(collections.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }

    async fn delete_collection_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_collection_record_without_events(pool).await;
        };

        use crate::schema::collections::dsl::{collections, id};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            diesel::delete(collections.filter(id.eq(self.id))).execute(conn)?;
            let event = collection_event(
                self,
                Action::Deleted,
                context,
                format!("Collection '{}' deleted", self.name),
            )?
            .with_before(collection_snapshot(self));
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

impl DeleteCollectionRecord for CollectionID {
    async fn delete_collection_record_without_events(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, |conn| {
            diesel::delete(collections.filter(id.eq(self.id()))).execute(conn)
        })?;
        Ok(())
    }

    async fn delete_collection_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_collection_record_without_events(pool).await;
        };

        use crate::schema::collections::dsl::{collections, id};

        with_transaction(pool, |conn| -> Result<(), ApiError> {
            let collection = collections
                .filter(id.eq(self.id()))
                .first::<Collection>(conn)?;
            diesel::delete(collections.filter(id.eq(collection.id))).execute(conn)?;
            let event = collection_event(
                &collection,
                Action::Deleted,
                context,
                format!("Collection '{}' deleted", collection.name),
            )?
            .with_before(collection_snapshot(&collection));
            emit_event(conn, &event)?;
            Ok(())
        })
    }
}

pub trait UpdateCollectionRecord {
    async fn update_collection_record_without_events(
        &self,
        pool: &DbPool,
        collection_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn update_collection_record(
        &self,
        pool: &DbPool,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let _ = context;
        self.update_collection_record_without_events(pool, collection_id)
            .await
    }
}

impl UpdateCollectionRecord for UpdateCollection {
    async fn update_collection_record_without_events(
        &self,
        pool: &DbPool,
        collection_id: i32,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, |conn| {
            crate::db::updated_or_current(
                diesel::update(collections)
                    .filter(id.eq(collection_id))
                    .set(self)
                    .get_result::<Collection>(conn)
                    .optional(),
                || collections.filter(id.eq(collection_id)).first(conn),
            )
        })
    }

    async fn update_collection_record(
        &self,
        pool: &DbPool,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let Some(context) = context else {
            return self
                .update_collection_record_without_events(pool, collection_id)
                .await;
        };

        use crate::schema::collections::dsl::{collections, id};

        with_transaction(pool, |conn| -> Result<Collection, ApiError> {
            let before = collections
                .filter(id.eq(collection_id))
                .first::<Collection>(conn)?;
            let updated = diesel::update(collections.filter(id.eq(collection_id)))
                .set(self)
                .get_result::<Collection>(conn)?;
            let event = collection_event(
                &updated,
                Action::Updated,
                context,
                format!("Collection '{}' updated", updated.name),
            )?
            .with_before(collection_snapshot(&before))
            .with_after(collection_snapshot(&updated));
            emit_event(conn, &event)?;
            Ok(updated)
        })
    }
}

pub trait SaveCollectionWithAssigneeRecord {
    async fn save_collection_with_assignee_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_with_assignee_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let _ = context;
        self.save_collection_with_assignee_record_without_events(pool)
            .await
    }
}

impl SaveCollectionWithAssigneeRecord for NewCollectionWithAssignee {
    async fn save_collection_with_assignee_record_without_events(
        &self,
        pool: &DbPool,
    ) -> Result<Collection, ApiError> {
        let new_collection = NewCollection {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_collection
            .save_collection_for_group_record_without_events(pool, self.group_id)
            .await
    }

    async fn save_collection_with_assignee_record(
        &self,
        pool: &DbPool,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let new_collection = NewCollection {
            name: self.name.clone(),
            description: self.description.clone(),
        };

        new_collection
            .save_collection_for_group_record(pool, self.group_id, context)
            .await
    }
}

pub trait SaveCollectionForGroupRecord {
    async fn save_collection_for_group_record_without_events(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let _ = context;
        self.save_collection_for_group_record_without_events(pool, group_id)
            .await
    }
}

impl SaveCollectionForGroupRecord for NewCollection {
    async fn save_collection_for_group_record_without_events(
        &self,
        pool: &DbPool,
        group_id: i32,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::collections;
        use crate::schema::permissions::dsl::permissions;

        with_transaction(pool, |conn| -> Result<Collection, ApiError> {
            let collection = diesel::insert_into(collections)
                .values(self)
                .get_result::<Collection>(conn)?;

            let group_permission = NewPermission {
                collection_id: collection.id,
                group_id,
                has_read_collection: true,
                has_update_collection: true,
                has_delete_collection: true,
                has_delegate_collection: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
                has_create_class_relation: true,
                has_read_class_relation: true,
                has_update_class_relation: true,
                has_delete_class_relation: true,
                has_create_object_relation: true,
                has_read_object_relation: true,
                has_update_object_relation: true,
                has_delete_object_relation: true,
                has_read_template: true,
                has_create_template: true,
                has_update_template: true,
                has_delete_template: true,
                has_read_remote_target: true,
                has_create_remote_target: true,
                has_update_remote_target: true,
                has_delete_remote_target: true,
                has_execute_remote_target: true,
                has_read_audit: true,
                has_manage_event_subscription: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            Ok(collection)
        })
    }

    async fn save_collection_for_group_record(
        &self,
        pool: &DbPool,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let Some(context) = context else {
            return self
                .save_collection_for_group_record_without_events(pool, group_id)
                .await;
        };

        use crate::schema::collections::dsl::collections;
        use crate::schema::permissions::dsl::permissions;

        with_transaction(pool, |conn| -> Result<Collection, ApiError> {
            let collection = diesel::insert_into(collections)
                .values(self)
                .get_result::<Collection>(conn)?;

            let group_permission = NewPermission {
                collection_id: collection.id,
                group_id,
                has_read_collection: true,
                has_update_collection: true,
                has_delete_collection: true,
                has_delegate_collection: true,
                has_create_class: true,
                has_read_class: true,
                has_update_class: true,
                has_delete_class: true,
                has_create_object: true,
                has_read_object: true,
                has_update_object: true,
                has_delete_object: true,
                has_create_class_relation: true,
                has_read_class_relation: true,
                has_update_class_relation: true,
                has_delete_class_relation: true,
                has_create_object_relation: true,
                has_read_object_relation: true,
                has_update_object_relation: true,
                has_delete_object_relation: true,
                has_read_template: true,
                has_create_template: true,
                has_update_template: true,
                has_delete_template: true,
                has_read_remote_target: true,
                has_create_remote_target: true,
                has_update_remote_target: true,
                has_delete_remote_target: true,
                has_execute_remote_target: true,
                has_read_audit: true,
                has_manage_event_subscription: true,
            };

            diesel::insert_into(permissions)
                .values(&group_permission)
                .execute(conn)?;

            let event = collection_event(
                &collection,
                Action::Created,
                context,
                format!("Collection '{}' created", collection.name),
            )?
            .with_after(collection_snapshot(&collection))
            .with_metadata(serde_json::json!({ "assignee_group_id": group_id }));
            emit_event(conn, &event)?;

            Ok(collection)
        })
    }
}
