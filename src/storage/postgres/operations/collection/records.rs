use super::*;
use crate::api::etag::RevisionOwner;
use crate::events::{Action, EntityType, EventContext, NewEvent};
use crate::pagination::{CursorSqlField, CursorSqlMapping, CursorSqlType};
use crate::storage::postgres::operations::event_record::emit_event;
use crate::traits::{CursorPaginated, CursorValue};
use chrono::NaiveDateTime;
use diesel_async::RunQueryDsl;

/// PostgreSQL representation of a collection row.
///
/// The domain value is persistence-neutral; schema bindings and physical
/// cursor columns remain private to the adapter.
#[derive(Clone, Queryable, QueryableByName, Selectable)]
#[diesel(table_name = crate::schema::collections)]
pub(crate) struct CollectionRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) created_at: chrono::NaiveDateTime,
    pub(crate) updated_at: chrono::NaiveDateTime,
    pub(crate) parent_collection_id: Option<i32>,
    pub(crate) revision: PostgresRevision,
}

impl From<CollectionRow> for Collection {
    fn from(row: CollectionRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            description: row.description,
            created_at: row.created_at,
            updated_at: row.updated_at,
            parent_collection_id: row.parent_collection_id,
            revision: row.revision.into_domain(),
        }
    }
}

impl CursorPaginated for CollectionRow {
    fn supports_sort(field: &FilterField) -> bool {
        Collection::supports_sort(field)
    }

    fn cursor_value(&self, field: &FilterField) -> Result<CursorValue, ApiError> {
        Ok(match field {
            FilterField::Id => CursorValue::Integer(self.id.into()),
            FilterField::Name => CursorValue::String(self.name.clone()),
            FilterField::Description => CursorValue::String(self.description.clone()),
            FilterField::CreatedAt => CursorValue::DateTime(self.created_at),
            FilterField::UpdatedAt => CursorValue::DateTime(self.updated_at),
            FilterField::Revision => CursorValue::Integer(self.revision.get()),
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for collections"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        Collection::default_sort()
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        Collection::tie_breaker_sort()
    }
}

impl CursorSqlMapping for CollectionRow {
    fn sql_field(field: &FilterField) -> Result<CursorSqlField, ApiError> {
        Ok(match field {
            FilterField::Id => CursorSqlField {
                column: "collections.id",
                sql_type: CursorSqlType::Integer,
                nullable: false,
            },
            FilterField::Name => CursorSqlField {
                column: "collections.name",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::Description => CursorSqlField {
                column: "collections.description",
                sql_type: CursorSqlType::String,
                nullable: false,
            },
            FilterField::CreatedAt => CursorSqlField {
                column: "collections.created_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::UpdatedAt => CursorSqlField {
                column: "collections.updated_at",
                sql_type: CursorSqlType::DateTime,
                nullable: false,
            },
            FilterField::Revision => CursorSqlField {
                column: "collections.revision",
                sql_type: CursorSqlType::BigInt,
                nullable: false,
            },
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for collections"
                )));
            }
        })
    }
}

struct NewCollectionRow<'a> {
    name: &'a str,
    description: &'a str,
    parent_collection_id: i32,
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::collections)]
pub(crate) struct UpdateCollectionRow<'a> {
    name: Option<&'a str>,
    description: Option<&'a str>,
}

impl<'a> From<&'a UpdateCollection> for UpdateCollectionRow<'a> {
    fn from(update: &'a UpdateCollection) -> Self {
        Self {
            name: update.name.as_deref(),
            description: update.description.as_deref(),
        }
    }
}

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
        .first::<CollectionRow>(conn)
        .await
        .optional()?
        .map(Into::into);
    let collection: Collection =
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
}

pub(crate) async fn insert_collection_row_with_closure(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: CollectionRowInsert<'_>,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{
        collections, created_at, description, name, parent_collection_id, updated_at,
    };

    let resolved_parent_id = resolve_parent_collection_id(conn, input.parent_collection_id).await?;
    let row = NewCollectionRow {
        name: input.name,
        description: input.description,
        parent_collection_id: resolved_parent_id,
    };

    let collection: Collection = match input.timestamps {
        Some((created, updated)) => diesel::insert_into(collections)
            .values((
                name.eq(row.name),
                description.eq(row.description),
                parent_collection_id.eq(row.parent_collection_id),
                created_at.eq(created),
                updated_at.eq(updated),
            ))
            .get_result::<CollectionRow>(conn)
            .await?
            .into(),
        None => diesel::insert_into(collections)
            // Keep the adapter DTO private, then express its resolved fields as
            // fixed `Eq` values. Deriving `Insertable` for an optional parent
            // produces a dynamic DEFAULT-capable statement and bypasses
            // Diesel's prepared-statement cache.
            .values((
                name.eq(row.name),
                description.eq(row.description),
                parent_collection_id.eq(row.parent_collection_id),
            ))
            .get_result::<CollectionRow>(conn)
            .await?
            .into(),
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
    pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError>;

    async fn delete_collection_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_collection_record_without_events(pool).await
    }
}

impl DeleteCollectionRecord for Collection {
    async fn delete_collection_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id, None).await
    }

    async fn delete_collection_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id, context).await
    }
}

impl DeleteCollectionRecord for CollectionID {
    async fn delete_collection_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id(), None).await
    }

    async fn delete_collection_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        delete_collection_by_id(pool, self.id(), context).await
    }
}

pub trait UpdateCollectionRecord {
    async fn update_collection_record_without_events(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        collection_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn update_collection_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
        collection_id: i32,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        with_connection(pool, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(collections)
                    .filter(id.eq(collection_id))
                    .set(UpdateCollectionRow::from(self))
                    .get_result::<CollectionRow>(conn)
                    .await
                    .optional(),
                async || {
                    collections
                        .filter(id.eq(collection_id))
                        .first::<CollectionRow>(conn)
                        .await
                },
            )
            .await
        })
        .await
        .map(Into::into)
    }

    async fn update_collection_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
                .first::<CollectionRow>(conn)
                .await
                .optional()?
                .map(Into::into);
            let before: Collection =
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
                .set(UpdateCollectionRow::from(self))
                .get_result::<CollectionRow>(conn)
                .await?
                .into();
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
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_with_assignee_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
        group_id: i32,
    ) -> Result<Collection, ApiError>;

    async fn save_collection_for_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
        pool: &crate::storage::postgres::PostgresPool,
        group_id: i32,
    ) -> Result<Collection, ApiError> {
        with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
            insert_collection_for_group(conn, self, group_id).await
        })
        .await
    }

    async fn save_collection_for_group_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
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
            crate::storage::postgres::failpoints::check(
                crate::storage::postgres::failpoints::PostgresFailpoint::CollectionCreateAfterRecords,
            )?;

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

pub async fn collection_children_from_backend(
    pool: &crate::storage::postgres::PostgresPool,
    target_collection_id: i32,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    with_connection(pool, async |conn| {
        collections
            .filter(parent_collection_id.eq(target_collection_id))
            .order(crate::schema::collections::name.asc())
            .load::<CollectionRow>(conn)
            .await
    })
    .await
    .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub async fn collection_ancestors_from_backend(
    pool: &crate::storage::postgres::PostgresPool,
    target_collection_id: i32,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, depth, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id};

    with_connection(pool, async |conn| {
        collection_closure
            .inner_join(collections.on(id.eq(ancestor_collection_id)))
            .filter(descendant_collection_id.eq(target_collection_id))
            .filter(depth.gt(0))
            .order(depth.asc())
            .select(collections::all_columns())
            .load::<CollectionRow>(conn)
            .await
    })
    .await
    .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub async fn move_collection_record_from_backend(
    pool: &crate::storage::postgres::PostgresPool,
    target_collection_id: i32,
    new_parent_collection_id: i32,
    context: Option<&EventContext>,
) -> Result<Collection, ApiError> {
    use crate::schema::collection_closure::dsl::{
        ancestor_collection_id, collection_closure, descendant_collection_id,
    };
    use crate::schema::collections::dsl::{collections, id, parent_collection_id};

    with_transaction(pool, async |conn| -> Result<Collection, ApiError> {
        let before: Collection = collections
            .filter(id.eq(target_collection_id))
            .for_update()
            .first::<CollectionRow>(conn)
            .await?
            .into();

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
            .first::<CollectionRow>(conn)
            .await?
            .into();

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

#[cfg(test)]
mod tests {
    use diesel::query_builder::QueryId;

    use super::*;

    fn assert_static_query_id<T: QueryId>(_: &T) {
        assert!(
            T::HAS_STATIC_QUERY_ID,
            "collection inserts must remain eligible for prepared-statement caching"
        );
    }

    #[test]
    fn ordinary_collection_insert_has_a_static_query_shape() {
        use crate::schema::collections::dsl::{
            collections, description, name, parent_collection_id,
        };

        let query = diesel::insert_into(collections).values((
            name.eq("cacheable-collection"),
            description.eq("cacheable collection insert"),
            parent_collection_id.eq(1),
        ));

        assert_static_query_id(&query);
    }
}
