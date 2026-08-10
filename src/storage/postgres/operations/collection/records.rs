use super::*;
use crate::api::etag::RevisionOwner;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use chrono::NaiveDateTime;
use diesel_async::RunQueryDsl;

fn collection_snapshot(collection: &Collection) -> serde_json::Value {
    serde_json::json!({
        "id": collection.id,
        "name": collection.name,
        "description": collection.description,
        "created_at": collection.created_at,
        "updated_at": collection.updated_at,
        "parent_collection_id": collection.parent_collection_id,
        "revision": collection.revision,
    })
}

pub(crate) async fn root_collection_id(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<i32, ApiError> {
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    collections
        .filter(parent_collection_id.is_null())
        .select(id)
        .first::<i32>(conn)
        .await
        .map_err(ApiError::from)
}

async fn resolve_parent_collection_id(
    conn: &mut crate::storage::postgres::PostgresConnection,
    requested_parent_collection_id: Option<i32>,
) -> Result<i32, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    match requested_parent_collection_id {
        Some(parent_id) => {
            collections
                .filter(id.eq(parent_id))
                .select(id)
                .first::<i32>(conn)
                .await?;
            Ok(parent_id)
        }
        None => root_collection_id(conn).await,
    }
}

async fn validate_collection_can_be_deleted(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target_collection_id: i32,
) -> Result<(), ApiError> {
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    let target_parent = collections
        .filter(id.eq(target_collection_id))
        .select(parent_collection_id)
        .first::<Option<i32>>(conn)
        .await?;

    if target_parent.is_none() {
        return Err(ApiError::Conflict(
            "The root collection cannot be deleted".to_string(),
        ));
    }

    let child_count = collections
        .filter(parent_collection_id.eq(target_collection_id))
        .count()
        .get_result::<i64>(conn)
        .await?;

    if child_count > 0 {
        return Err(ApiError::Conflict(
            "Collections with child collections cannot be deleted".to_string(),
        ));
    }

    Ok(())
}

async fn lock_and_validate_collection_for_delete(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_id: i32,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    let owner_key = RevisionOwner::Collection.key(collection_id);
    let collection = collections
        .filter(id.eq(collection_id))
        .for_update()
        .first::<Collection>(conn)
        .await
        .optional()?;
    let collection =
        crate::storage::postgres::require_existing_revision_target(collection, &owner_key)?;
    crate::storage::postgres::assert_locked_revision_precondition(
        conn,
        &owner_key,
        collection.revision,
    )
    .await?;
    validate_collection_can_be_deleted(conn, collection.id).await?;
    Ok(collection)
}

pub(crate) async fn insert_collection_closure_rows(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target_collection_id: i32,
    parent_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query(
        "INSERT INTO collection_closure (ancestor_collection_id, descendant_collection_id, depth)
         SELECT ancestor_collection_id, $1, depth + 1
         FROM collection_closure
         WHERE descendant_collection_id = $2
         UNION ALL
         SELECT $1, $1, 0",
    )
    .bind::<diesel::sql_types::Integer, _>(target_collection_id)
    .bind::<diesel::sql_types::Integer, _>(parent_id)
    .execute(conn)
    .await?;

    Ok(())
}

pub(crate) struct CollectionRowInsert<'a> {
    name: &'a str,
    description: &'a str,
    parent_collection_id: Option<i32>,
    timestamps: Option<(NaiveDateTime, NaiveDateTime)>,
}

impl<'a> CollectionRowInsert<'a> {
    pub(crate) fn new(name: &'a str, description: &'a str) -> Self {
        Self {
            name,
            description,
            parent_collection_id: None,
            timestamps: None,
        }
    }

    pub(crate) fn parent_collection_id(mut self, parent_collection_id: Option<i32>) -> Self {
        self.parent_collection_id = parent_collection_id;
        self
    }

    pub(crate) fn timestamps(
        mut self,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        self.timestamps = Some((created_at, updated_at));
        self
    }
}

pub(crate) async fn insert_collection_row_with_closure(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: CollectionRowInsert<'_>,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{
        collections, created_at, description, name, parent_collection_id, updated_at,
    };

    let resolved_parent_id = resolve_parent_collection_id(conn, input.parent_collection_id).await?;

    let collection = match input.timestamps {
        Some((created, updated)) => {
            diesel::insert_into(collections)
                .values((
                    name.eq(input.name),
                    description.eq(input.description),
                    parent_collection_id.eq(resolved_parent_id),
                    created_at.eq(created),
                    updated_at.eq(updated),
                ))
                .get_result::<Collection>(conn)
                .await?
        }
        None => {
            diesel::insert_into(collections)
                .values((
                    name.eq(input.name),
                    description.eq(input.description),
                    parent_collection_id.eq(resolved_parent_id),
                ))
                .get_result::<Collection>(conn)
                .await?
        }
    };

    insert_collection_closure_rows(conn, collection.id, resolved_parent_id).await?;

    Ok(collection)
}

async fn move_collection_closure_rows(
    conn: &mut crate::storage::postgres::PostgresConnection,
    target_collection_id: i32,
    new_parent_collection_id: i32,
) -> Result<(), ApiError> {
    diesel::sql_query(
        "DELETE FROM collection_closure
         WHERE descendant_collection_id IN (
             SELECT descendant_collection_id
             FROM collection_closure
             WHERE ancestor_collection_id = $1
         )
           AND ancestor_collection_id IN (
             SELECT ancestor_collection_id
             FROM collection_closure
             WHERE descendant_collection_id = $1
             EXCEPT
             SELECT descendant_collection_id
             FROM collection_closure
             WHERE ancestor_collection_id = $1
         )",
    )
    .bind::<diesel::sql_types::Integer, _>(target_collection_id)
    .execute(conn)
    .await?;

    diesel::sql_query(
        "INSERT INTO collection_closure (ancestor_collection_id, descendant_collection_id, depth)
         SELECT supertree.ancestor_collection_id,
                subtree.descendant_collection_id,
                supertree.depth + subtree.depth + 1
         FROM collection_closure supertree
         INNER JOIN collection_closure subtree ON subtree.ancestor_collection_id = $1
         WHERE supertree.descendant_collection_id = $2",
    )
    .bind::<diesel::sql_types::Integer, _>(target_collection_id)
    .bind::<diesel::sql_types::Integer, _>(new_parent_collection_id)
    .execute(conn)
    .await?;

    Ok(())
}

async fn insert_collection_for_group(
    conn: &mut crate::storage::postgres::PostgresConnection,
    new_collection: &NewCollection,
    group_id: i32,
) -> Result<Collection, ApiError> {
    use crate::schema::permissions::dsl::permissions;

    let collection = insert_collection_row_with_closure(
        conn,
        CollectionRowInsert::new(&new_collection.name, &new_collection.description)
            .parent_collection_id(new_collection.parent_collection_id),
    )
    .await?;

    let group_permission =
        crate::storage::postgres::operations::permissions::new_permission_from_list(
            collection.id,
            group_id,
            &PermissionsList::new(Permissions::ALL),
        );

    diesel::insert_into(permissions)
        .values(&group_permission)
        .execute(conn)
        .await?;

    Ok(collection)
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

async fn delete_collection_by_id(
    pool: &impl crate::storage::StorageContext,
    collection_id: i32,
    context: Option<&EventContext>,
) -> Result<(), ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    with_transaction(pool, async |conn| -> Result<(), ApiError> {
        let collection = lock_and_validate_collection_for_delete(conn, collection_id).await?;
        diesel::delete(collections.filter(id.eq(collection.id)))
            .execute(conn)
            .await?;

        if let Some(context) = context {
            let event = collection_event(
                &collection,
                Action::Deleted,
                context,
                format!("Collection '{}' deleted", collection.name),
            )?
            .with_before(collection_snapshot(&collection));
            emit_event(conn, &event).await?;
        }

        Ok(())
    })
    .await
}

pub trait DeleteCollectionRecord {
    async fn delete_collection_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError>;

    async fn delete_collection_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_collection_record_without_events(pool).await
    }
}

impl DeleteCollectionRecord for Collection {
    async fn delete_collection_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id, None).await
    }

    async fn delete_collection_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id, context).await
    }
}

impl DeleteCollectionRecord for CollectionID {
    async fn delete_collection_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id(), None).await
    }

    async fn delete_collection_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id(), context).await
    }
}

pub trait UpdateCollectionRecord {
    async fn update_collection_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
        collection_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn update_collection_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
        collection_id: i32,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(collections)
                    .filter(id.eq(collection_id))
                    .set(self)
                    .get_result::<Collection>(conn)
                    .await
                    .optional(),
                async || collections.filter(id.eq(collection_id)).first(conn).await,
            )
            .await
        })
        .await
    }

    async fn update_collection_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        collection_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let Some(context) = context else {
            return self
                .update_collection_record_without_events(pool, collection_id)
                .await;
        };

        use crate::schema::collections::dsl::{collections, id};

        with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
            let owner_key = RevisionOwner::Collection.key(collection_id);
            let before = collections
                .filter(id.eq(collection_id))
                .for_update()
                .first::<Collection>(conn)
                .await
                .optional()?;
            let before =
                crate::storage::postgres::require_existing_revision_target(before, &owner_key)?;
            crate::storage::postgres::assert_locked_revision_precondition(
                conn,
                &owner_key,
                before.revision,
            )
            .await?;
            if !self.has_changes(&before) {
                return Ok(before);
            }
            let updated = diesel::update(collections.filter(id.eq(collection_id)))
                .set(self)
                .get_result::<Collection>(conn)
                .await?;
            let event = collection_event(
                &updated,
                Action::Updated,
                context,
                format!("Collection '{}' updated", updated.name),
            )?
            .with_before(collection_snapshot(&before))
            .with_after(collection_snapshot(&updated));
            emit_event(conn, &event).await?;
            Ok(updated)
        })
        .await
    }
}

pub trait SaveCollectionWithAssigneeRecord {
    async fn save_collection_with_assignee_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_with_assignee_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Collection, ApiError> {
        let new_collection = NewCollection {
            name: self.name.clone(),
            description: self.description.clone(),
            parent_collection_id: self.parent_collection_id.map(CollectionID::id),
        };

        new_collection
            .save_collection_for_group_record_without_events(pool, self.group_id.id())
            .await
    }

    async fn save_collection_with_assignee_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let new_collection = NewCollection {
            name: self.name.clone(),
            description: self.description.clone(),
            parent_collection_id: self.parent_collection_id.map(CollectionID::id),
        };

        new_collection
            .save_collection_for_group_record(pool, self.group_id.id(), context)
            .await
    }
}

pub trait SaveCollectionForGroupRecord {
    async fn save_collection_for_group_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
        group_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_for_group_record(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
        group_id: i32,
    ) -> Result<Collection, ApiError> {
        with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
            insert_collection_for_group(conn, self, group_id).await
        })
        .await
    }

    async fn save_collection_for_group_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        group_id: i32,
        context: Option<&EventContext>,
    ) -> Result<Collection, ApiError> {
        let Some(context) = context else {
            return self
                .save_collection_for_group_record_without_events(pool, group_id)
                .await;
        };

        with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
            let collection = insert_collection_for_group(conn, self, group_id).await?;

            let event = collection_event(
                &collection,
                Action::Created,
                context,
                format!("Collection '{}' created", collection.name),
            )?
            .with_after(collection_snapshot(&collection))
            .with_metadata(serde_json::json!({ "assignee_group_id": group_id }));
            emit_event(conn, &event).await?;

            Ok(collection)
        })
        .await
    }
}

pub async fn collection_children_from_backend<T: CollectionAccessors>(
    pool: &impl crate::storage::StorageContext,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    let target_collection_id = collection_ref.collection_id(pool).await?.id();
    with_connection(pool, async |conn| {
        collections
            .filter(parent_collection_id.eq(target_collection_id))
            .order(crate::schema::collections::name.asc())
            .load::<Collection>(conn)
            .await
    })
    .await
}

pub async fn collection_ancestors_from_backend<T: CollectionAccessors>(
    pool: &impl crate::storage::StorageContext,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, depth, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id};

    let target_collection_id = collection_ref.collection_id(pool).await?.id();
    with_connection(pool, async |conn| {
        collection_closure
            .inner_join(collections.on(id.eq(ancestor_collection_id)))
            .filter(descendant_collection_id.eq(target_collection_id))
            .filter(depth.gt(0))
            .order(depth.asc())
            .select(collections::all_columns())
            .load::<Collection>(conn)
            .await
    })
    .await
}

pub async fn move_collection_record_from_backend(
    pool: &impl crate::storage::StorageContext,
    target_collection_id: i32,
    new_parent_collection_id: i32,
    context: Option<&EventContext>,
) -> Result<Collection, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
        let before = collections
            .filter(id.eq(target_collection_id))
            .for_update()
            .first::<Collection>(conn)
            .await?;

        if before.parent_collection_id.is_none() {
            return Err(ApiError::Conflict(
                "The root collection cannot be moved".to_string(),
            ));
        }

        if before.parent_collection_id == Some(new_parent_collection_id) {
            return Ok(before);
        }

        if target_collection_id == new_parent_collection_id {
            return Err(ApiError::BadRequest(
                "A collection cannot be moved under itself".to_string(),
            ));
        }

        collections
            .filter(id.eq(new_parent_collection_id))
            .select(id)
            .first::<i32>(conn)
            .await?;

        let new_parent_is_descendant = collection_closure
            .filter(ancestor_collection_id.eq(target_collection_id))
            .filter(descendant_collection_id.eq(new_parent_collection_id))
            .count()
            .get_result::<i64>(conn)
            .await?
            > 0;

        if new_parent_is_descendant {
            return Err(ApiError::BadRequest(
                "A collection cannot be moved under one of its descendants".to_string(),
            ));
        }

        diesel::update(collections.filter(id.eq(target_collection_id)))
            .set(parent_collection_id.eq(new_parent_collection_id))
            .execute(conn)
            .await?;

        move_collection_closure_rows(conn, target_collection_id, new_parent_collection_id).await?;

        let updated = collections
            .filter(id.eq(target_collection_id))
            .first::<Collection>(conn)
            .await?;

        if let Some(context) = context {
            let event = collection_event(
                &updated,
                Action::Updated,
                context,
                format!("Collection '{}' moved", updated.name),
            )?
            .with_before(collection_snapshot(&before))
            .with_after(collection_snapshot(&updated));
            emit_event(conn, &event).await?;
        }

        Ok(updated)
    })
    .await
}
