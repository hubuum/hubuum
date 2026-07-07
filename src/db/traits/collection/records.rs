use super::*;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};

fn collection_snapshot(collection: &Collection) -> serde_json::Value {
    serde_json::json!({
        "id": collection.id,
        "name": collection.name,
        "description": collection.description,
        "created_at": collection.created_at,
        "updated_at": collection.updated_at,
        "parent_collection_id": collection.parent_collection_id,
    })
}

pub(crate) fn root_collection_id(conn: &mut diesel::PgConnection) -> Result<i32, ApiError> {
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    collections
        .filter(parent_collection_id.is_null())
        .select(id)
        .first::<i32>(conn)
        .map_err(ApiError::from)
}

fn resolve_parent_collection_id(
    conn: &mut diesel::PgConnection,
    requested_parent_collection_id: Option<i32>,
) -> Result<i32, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    match requested_parent_collection_id {
        Some(parent_id) => {
            collections
                .filter(id.eq(parent_id))
                .select(id)
                .first::<i32>(conn)?;
            Ok(parent_id)
        }
        None => root_collection_id(conn),
    }
}

fn validate_collection_can_be_deleted(
    conn: &mut diesel::PgConnection,
    target_collection_id: i32,
) -> Result<(), ApiError> {
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    let target_parent = collections
        .filter(id.eq(target_collection_id))
        .select(parent_collection_id)
        .first::<Option<i32>>(conn)?;

    if target_parent.is_none() {
        return Err(ApiError::Conflict(
            "The root collection cannot be deleted".to_string(),
        ));
    }

    let child_count = collections
        .filter(parent_collection_id.eq(target_collection_id))
        .count()
        .get_result::<i64>(conn)?;

    if child_count > 0 {
        return Err(ApiError::Conflict(
            "Collections with child collections cannot be deleted".to_string(),
        ));
    }

    Ok(())
}

pub(crate) fn insert_collection_closure_rows(
    conn: &mut diesel::PgConnection,
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
    .execute(conn)?;

    Ok(())
}

pub(crate) fn insert_collection_row_with_closure(
    conn: &mut diesel::PgConnection,
    name_value: &str,
    description_value: &str,
    requested_parent_collection_id: Option<i32>,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    let resolved_parent_id = resolve_parent_collection_id(conn, requested_parent_collection_id)?;

    let collection = diesel::insert_into(collections)
        .values((
            crate::schema::collections::name.eq(name_value),
            crate::schema::collections::description.eq(description_value),
            parent_collection_id.eq(resolved_parent_id),
        ))
        .get_result::<Collection>(conn)?;

    insert_collection_closure_rows(conn, collection.id, resolved_parent_id)?;

    Ok(collection)
}

fn move_collection_closure_rows(
    conn: &mut diesel::PgConnection,
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
    .execute(conn)?;

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
    .execute(conn)?;

    Ok(())
}

fn insert_collection_for_group(
    conn: &mut diesel::PgConnection,
    new_collection: &NewCollection,
    group_id: i32,
) -> Result<Collection, ApiError> {
    use crate::schema::permissions::dsl::permissions;

    let collection = insert_collection_row_with_closure(
        conn,
        &new_collection.name,
        &new_collection.description,
        new_collection.parent_collection_id,
    )?;

    let group_permission = crate::db::traits::permissions::new_permission_from_list(
        collection.id,
        group_id,
        &PermissionsList::new(Permissions::ALL),
    );

    diesel::insert_into(permissions)
        .values(&group_permission)
        .execute(conn)?;

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

        with_connection(pool, |conn| -> Result<_, ApiError> {
            validate_collection_can_be_deleted(conn, self.id)?;
            Ok(diesel::delete(collections.filter(id.eq(self.id))).execute(conn)?)
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
            validate_collection_can_be_deleted(conn, self.id)?;
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

        with_connection(pool, |conn| -> Result<_, ApiError> {
            validate_collection_can_be_deleted(conn, self.id())?;
            Ok(diesel::delete(collections.filter(id.eq(self.id()))).execute(conn)?)
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
            validate_collection_can_be_deleted(conn, collection.id)?;
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
            parent_collection_id: self.parent_collection_id,
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
            parent_collection_id: self.parent_collection_id,
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
        with_transaction(pool, |conn| -> Result<Collection, ApiError> {
            insert_collection_for_group(conn, self, group_id)
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

        with_transaction(pool, |conn| -> Result<Collection, ApiError> {
            let collection = insert_collection_for_group(conn, self, group_id)?;

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

pub async fn collection_children_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    let target_collection_id = collection_ref.collection_id(pool).await?.id();
    with_connection(pool, |conn| {
        collections
            .filter(parent_collection_id.eq(target_collection_id))
            .order(crate::schema::collections::name.asc())
            .load::<Collection>(conn)
    })
}

pub async fn collection_ancestors_from_backend<T: CollectionAccessors>(
    pool: &DbPool,
    collection_ref: T,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, depth, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id};

    let target_collection_id = collection_ref.collection_id(pool).await?.id();
    with_connection(pool, |conn| {
        collection_closure
            .inner_join(collections.on(id.eq(ancestor_collection_id)))
            .filter(descendant_collection_id.eq(target_collection_id))
            .filter(depth.gt(0))
            .order(depth.asc())
            .select(collections::all_columns())
            .load::<Collection>(conn)
    })
}

pub async fn move_collection_record_from_backend(
    pool: &DbPool,
    target_collection_id: i32,
    new_parent_collection_id: i32,
    context: Option<&EventContext>,
) -> Result<Collection, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    with_transaction(pool, |conn| -> Result<Collection, ApiError> {
        let before = collections
            .filter(id.eq(target_collection_id))
            .first::<Collection>(conn)?;

        if before.parent_collection_id.is_none() {
            return Err(ApiError::Conflict(
                "The root collection cannot be moved".to_string(),
            ));
        }

        if target_collection_id == new_parent_collection_id {
            return Err(ApiError::BadRequest(
                "A collection cannot be moved under itself".to_string(),
            ));
        }

        collections
            .filter(id.eq(new_parent_collection_id))
            .select(id)
            .first::<i32>(conn)?;

        let new_parent_is_descendant = collection_closure
            .filter(ancestor_collection_id.eq(target_collection_id))
            .filter(descendant_collection_id.eq(new_parent_collection_id))
            .count()
            .get_result::<i64>(conn)?
            > 0;

        if new_parent_is_descendant {
            return Err(ApiError::BadRequest(
                "A collection cannot be moved under one of its descendants".to_string(),
            ));
        }

        diesel::update(collections.filter(id.eq(target_collection_id)))
            .set(parent_collection_id.eq(new_parent_collection_id))
            .execute(conn)?;

        move_collection_closure_rows(conn, target_collection_id, new_parent_collection_id)?;

        let updated = collections
            .filter(id.eq(target_collection_id))
            .first::<Collection>(conn)?;

        if let Some(context) = context {
            let event = collection_event(
                &updated,
                Action::Updated,
                context,
                format!("Collection '{}' moved", updated.name),
            )?
            .with_before(collection_snapshot(&before))
            .with_after(collection_snapshot(&updated));
            emit_event(conn, &event)?;
        }

        Ok(updated)
    })
}
