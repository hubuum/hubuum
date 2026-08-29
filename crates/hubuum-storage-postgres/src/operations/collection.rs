//! PostgreSQL implementation of the collection lifecycle contract.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::CollectionId;
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    StorageCollection, StorageCollectionCreate, StorageCollectionUpdate, StorageMutationOutcome,
};
use serde_json::json;

use crate::operations::authorization::insert_full_collection_grant;
use crate::operations::event_record::append_event;
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::{assert_locked_revision_precondition, require_existing_revision_target};
use crate::{
    PostgresConnection, PostgresFaultPoint, PostgresRevision, PostgresRuntime,
    PostgresStorageError, reach_fault_point,
};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::collections)]
pub(crate) struct CollectionRow {
    id: i32,
    name: String,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_collection_id: Option<i32>,
    revision: PostgresRevision,
}

impl CollectionRow {
    pub(crate) fn into_storage(self) -> Result<StorageCollection, PostgresStorageError> {
        crate::validate_persisted(
            "collection",
            StorageCollection::try_new(
                record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
                self.name,
                self.description,
                self.parent_collection_id
                    .map(CollectionId::new)
                    .transpose()?,
            ),
        )
    }

    fn snapshot(&self) -> serde_json::Value {
        json!({
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "parent_collection_id": self.parent_collection_id,
            "revision": self.revision,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::collections)]
struct UpdateCollectionRow<'value> {
    name: Option<&'value str>,
    description: Option<&'value str>,
}

impl<'value> From<&'value StorageCollectionUpdate> for UpdateCollectionRow<'value> {
    fn from(update: &'value StorageCollectionUpdate) -> Self {
        Self {
            name: update.name(),
            description: update.description(),
        }
    }
}

impl UpdateCollectionRow<'_> {
    const fn has_fields(&self) -> bool {
        self.name.is_some() || self.description.is_some()
    }

    fn changes(&self, collection: &CollectionRow) -> bool {
        self.name.is_some_and(|name| name != collection.name)
            || self
                .description
                .is_some_and(|description| description != collection.description)
    }
}

pub async fn get_collection(
    runtime: &PostgresRuntime,
    collection_id: i32,
) -> Result<StorageCollection, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    runtime
        .with_connection(async |connection| get_collection_on(connection, collection_id).await)
        .await
}

pub(crate) async fn get_collection_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<StorageCollection, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    load_collection(connection, collection_id)
        .await
        .map(CollectionRow::into_storage)
        .and_then(|collection| collection)
}

pub async fn create_collection(
    runtime: &PostgresRuntime,
    command: StorageCollectionCreate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            create_collection_on(connection, command, &context).await
        })
        .await
}

pub(crate) async fn create_collection_on(
    connection: &mut PostgresConnection,
    command: StorageCollectionCreate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    let parent_id = resolve_parent_collection_id(
        connection,
        command
            .parent_collection_id()
            .map(hubuum_domain::CollectionId::id),
    )
    .await?;
    let created =
        insert_collection(connection, command.name(), command.description(), parent_id).await?;
    insert_collection_closure_rows(connection, created.id, parent_id).await?;
    insert_full_collection_grant(connection, created.id, command.owner_group_id().id()).await?;

    reach_fault_point(
        PostgresFaultPoint::CollectionCreateAfterRecords,
        Some(connection),
    )
    .await?;
    let event = collection_event(
        &created,
        Action::Created,
        context,
        format!("Collection '{}' created", created.name),
    )?
    .with_after(created.snapshot())
    .with_metadata(json!({ "assignee_group_id": command.owner_group_id() }));
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        created.into_storage()?,
        audit,
    ))
}

pub async fn update_collection(
    runtime: &PostgresRuntime,
    collection_id: i32,
    changes: StorageCollectionUpdate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    let update = UpdateCollectionRow::from(&changes);
    if !update.has_fields() {
        return get_collection(runtime, collection_id)
            .await
            .map(StorageMutationOutcome::unchanged);
    }
    let context = context.clone();

    runtime
        .with_transaction(async move |connection| {
            update_collection_on(connection, collection_id, changes, &context).await
        })
        .await
}

pub(crate) async fn update_collection_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
    changes: StorageCollectionUpdate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    let update = UpdateCollectionRow::from(&changes);
    if !update.has_fields() {
        return get_collection_on(connection, collection_id)
            .await
            .map(StorageMutationOutcome::unchanged);
    }

    let before = lock_revisioned_collection(connection, collection_id).await?;
    let update = UpdateCollectionRow::from(&changes);
    if !update.changes(&before) {
        return Ok(StorageMutationOutcome::unchanged(before.into_storage()?));
    }
    let updated = diesel::update(
        crate::schema::collections::table.filter(crate::schema::collections::id.eq(collection_id)),
    )
    .set(update)
    .get_result::<CollectionRow>(connection)
    .await?;
    let event = collection_event(
        &updated,
        Action::Updated,
        context,
        format!("Collection '{}' updated", updated.name),
    )?
    .with_before(before.snapshot())
    .with_after(updated.snapshot());
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        updated.into_storage()?,
        audit,
    ))
}

pub async fn delete_collection(
    runtime: &PostgresRuntime,
    collection_id: i32,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            delete_collection_on(connection, collection_id, &context).await
        })
        .await
}

pub(crate) async fn delete_collection_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    let collection = lock_revisioned_collection(connection, collection_id).await?;
    validate_collection_can_be_deleted(connection, &collection).await?;
    diesel::delete(
        crate::schema::collections::table.filter(crate::schema::collections::id.eq(collection_id)),
    )
    .execute(connection)
    .await?;
    let event = collection_event(
        &collection,
        Action::Deleted,
        context,
        format!("Collection '{}' deleted", collection.name),
    )?
    .with_before(collection.snapshot());
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed((), audit))
}

pub async fn list_collection_children(
    runtime: &PostgresRuntime,
    collection_id: i32,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    runtime
        .with_connection(async |connection| collection_children_on(connection, collection_id).await)
        .await
}

pub(crate) async fn collection_children_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    let rows = crate::schema::collections::table
        .filter(crate::schema::collections::parent_collection_id.eq(collection_id))
        .order(crate::schema::collections::name.asc())
        .load::<CollectionRow>(connection)
        .await?;
    rows.into_iter().map(CollectionRow::into_storage).collect()
}

pub async fn list_collection_ancestors(
    runtime: &PostgresRuntime,
    collection_id: i32,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    runtime
        .with_connection(async |connection| {
            collection_ancestors_on(connection, collection_id).await
        })
        .await
}

pub(crate) async fn collection_ancestors_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    use crate::schema::{collection_closure, collections};

    validate_positive_id(collection_id, "collection id")?;
    let rows = collection_closure::table
        .inner_join(
            collections::table.on(collections::id.eq(collection_closure::ancestor_collection_id)),
        )
        .filter(collection_closure::descendant_collection_id.eq(collection_id))
        .filter(collection_closure::depth.gt(0))
        .order(collection_closure::depth.asc())
        .select(CollectionRow::as_select())
        .load::<CollectionRow>(connection)
        .await?;
    rows.into_iter().map(CollectionRow::into_storage).collect()
}

pub async fn move_collection(
    runtime: &PostgresRuntime,
    collection_id: i32,
    new_parent_id: i32,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    validate_positive_id(new_parent_id, "parent collection id")?;
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            move_collection_on(connection, collection_id, new_parent_id, &context).await
        })
        .await
}

pub(crate) async fn move_collection_on(
    connection: &mut PostgresConnection,
    collection_id: i32,
    new_parent_id: i32,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageCollection>, PostgresStorageError> {
    use crate::schema::{collection_closure, collections};

    validate_positive_id(collection_id, "collection id")?;
    validate_positive_id(new_parent_id, "parent collection id")?;
    let before = collections::table
        .filter(collections::id.eq(collection_id))
        .for_update()
        .first::<CollectionRow>(connection)
        .await?;
    if before.parent_collection_id.is_none() {
        return Err(PostgresStorageError::conflict(
            "The root collection cannot be moved",
        ));
    }
    if before.parent_collection_id == Some(new_parent_id) {
        return Ok(StorageMutationOutcome::unchanged(before.into_storage()?));
    }
    if collection_id == new_parent_id {
        return Err(PostgresStorageError::invalid_input(
            "A collection cannot be moved under itself",
        ));
    }
    collections::table
        .filter(collections::id.eq(new_parent_id))
        .select(collections::id)
        .first::<i32>(connection)
        .await?;
    let new_parent_is_descendant = collection_closure::table
        .filter(collection_closure::ancestor_collection_id.eq(collection_id))
        .filter(collection_closure::descendant_collection_id.eq(new_parent_id))
        .count()
        .get_result::<i64>(connection)
        .await?
        > 0;
    if new_parent_is_descendant {
        return Err(PostgresStorageError::invalid_input(
            "A collection cannot be moved under one of its descendants",
        ));
    }
    diesel::update(collections::table.filter(collections::id.eq(collection_id)))
        .set(collections::parent_collection_id.eq(new_parent_id))
        .execute(connection)
        .await?;
    move_collection_closure_rows(connection, collection_id, new_parent_id).await?;
    let updated = load_collection(connection, collection_id).await?;
    let event = collection_event(
        &updated,
        Action::Updated,
        context,
        format!("Collection '{}' moved", updated.name),
    )?
    .with_before(before.snapshot())
    .with_after(updated.snapshot());
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        updated.into_storage()?,
        audit,
    ))
}

async fn load_collection(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<CollectionRow, PostgresStorageError> {
    crate::schema::collections::table
        .filter(crate::schema::collections::id.eq(collection_id))
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_revisioned_collection(
    connection: &mut PostgresConnection,
    collection_id: i32,
) -> Result<CollectionRow, PostgresStorageError> {
    let owner_key = RevisionOwner::Collection.key(collection_id);
    let collection = crate::schema::collections::table
        .filter(crate::schema::collections::id.eq(collection_id))
        .for_update()
        .first::<CollectionRow>(connection)
        .await
        .optional()?;
    let collection = require_existing_revision_target(collection, &owner_key)?;
    assert_locked_revision_precondition(connection, &owner_key, collection.revision).await?;
    Ok(collection)
}

async fn resolve_parent_collection_id(
    connection: &mut PostgresConnection,
    requested_parent_id: Option<i32>,
) -> Result<i32, PostgresStorageError> {
    let query = crate::schema::collections::table.select(crate::schema::collections::id);
    match requested_parent_id {
        Some(parent_id) => query
            .filter(crate::schema::collections::id.eq(parent_id))
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
        None => query
            .filter(crate::schema::collections::parent_collection_id.is_null())
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
    }
}

async fn insert_collection(
    connection: &mut PostgresConnection,
    name_value: &str,
    description_value: &str,
    parent_id: i32,
) -> Result<CollectionRow, PostgresStorageError> {
    use crate::schema::collections::{description, name, parent_collection_id};

    diesel::insert_into(crate::schema::collections::table)
        .values((
            name.eq(name_value),
            description.eq(description_value),
            parent_collection_id.eq(parent_id),
        ))
        .get_result(connection)
        .await
        .map_err(PostgresStorageError::from)
}

pub(crate) async fn insert_collection_closure_rows(
    connection: &mut PostgresConnection,
    collection_id: i32,
    parent_id: i32,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Integer;

    diesel::sql_query(
        "INSERT INTO collection_closure (ancestor_collection_id, descendant_collection_id, depth)
         SELECT ancestor_collection_id, $1, depth + 1
         FROM collection_closure
         WHERE descendant_collection_id = $2
         UNION ALL
         SELECT $1, $1, 0",
    )
    .bind::<Integer, _>(collection_id)
    .bind::<Integer, _>(parent_id)
    .execute(connection)
    .await?;
    Ok(())
}

async fn move_collection_closure_rows(
    connection: &mut PostgresConnection,
    collection_id: i32,
    new_parent_id: i32,
) -> Result<(), PostgresStorageError> {
    use diesel::sql_types::Integer;

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
    .bind::<Integer, _>(collection_id)
    .execute(connection)
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
    .bind::<Integer, _>(collection_id)
    .bind::<Integer, _>(new_parent_id)
    .execute(connection)
    .await?;
    Ok(())
}

async fn validate_collection_can_be_deleted(
    connection: &mut PostgresConnection,
    collection: &CollectionRow,
) -> Result<(), PostgresStorageError> {
    if collection.parent_collection_id.is_none() {
        return Err(PostgresStorageError::conflict(
            "The root collection cannot be deleted",
        ));
    }
    let child_count = crate::schema::collections::table
        .filter(crate::schema::collections::parent_collection_id.eq(collection.id))
        .count()
        .get_result::<i64>(connection)
        .await?;
    if child_count > 0 {
        return Err(PostgresStorageError::conflict(
            "Collections with child collections cannot be deleted",
        ));
    }
    Ok(())
}

fn collection_event(
    collection: &CollectionRow,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(
        EntityType::Collection,
        action,
        context.actor_kind(),
        summary,
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_context(context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(collection.id)?)
            .with_entity_name(&collection.name)
            .with_collection_id(hubuum_domain::CollectionId::new(collection.id)?))
    })
}

fn validate_positive_id(value: i32, noun: &str) -> Result<(), PostgresStorageError> {
    if value > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "Invalid {noun}: expected a positive integer"
        )))
    }
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
        use crate::schema::collections::{description, name, parent_collection_id};

        let query = diesel::insert_into(crate::schema::collections::table).values((
            name.eq("cacheable-collection"),
            description.eq("cacheable collection insert"),
            parent_collection_id.eq(1),
        ));
        assert_static_query_id(&query);
    }
}
