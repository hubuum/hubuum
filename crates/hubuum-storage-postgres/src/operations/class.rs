//! PostgreSQL implementation of the class lifecycle contract.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{CollectionId, validate_json_schema, validate_json_schema_for_instances};
use hubuum_events_core::{Action, AuditDocument, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    StorageClass, StorageClassCreate, StorageClassSchemaPolicy, StorageClassSelector,
    StorageClassUpdate, StorageMutationOutcome, StorageResolvedClass,
};
use serde_json::json;

use crate::operations::event_record::append_event;
use crate::revision::{RevisionOwner, record_metadata};
use crate::runtime::{assert_locked_revision_precondition, require_existing_revision_target};
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::hubuumclass)]
pub(crate) struct ClassRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) collection_id: i32,
    pub(crate) json_schema: Option<serde_json::Value>,
    pub(crate) validate_schema: bool,
    pub(crate) description: String,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl ClassRow {
    pub(crate) fn into_storage(self) -> Result<StorageClass, PostgresStorageError> {
        let schema_policy =
            StorageClassSchemaPolicy::try_from_parts(self.json_schema, self.validate_schema)
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value("class schema policy", error)
                })?;
        Ok(StorageClass::builder(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            self.name,
            CollectionId::new(self.collection_id)?,
            self.description,
        )
        .schema_policy(schema_policy)
        .build())
    }

    fn snapshot(&self) -> serde_json::Value {
        json!({
            "id": self.id,
            "name": self.name,
            "collection_id": self.collection_id,
            "json_schema": self.json_schema,
            "validate_schema": self.validate_schema,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::hubuumclass)]
struct UpdateClassRow<'value> {
    name: Option<&'value str>,
    collection_id: Option<i32>,
    json_schema: Option<&'value serde_json::Value>,
    validate_schema: Option<bool>,
    description: Option<&'value str>,
}

impl<'value> From<&'value StorageClassUpdate> for UpdateClassRow<'value> {
    fn from(update: &'value StorageClassUpdate) -> Self {
        Self {
            name: update.name(),
            collection_id: update.collection_id().map(CollectionId::id),
            json_schema: update.json_schema(),
            validate_schema: update.validate_schema(),
            description: update.description(),
        }
    }
}

impl UpdateClassRow<'_> {
    fn changes(&self, class: &ClassRow) -> bool {
        self.name.is_some_and(|value| value != class.name)
            || self
                .collection_id
                .is_some_and(|value| value != class.collection_id)
            || self
                .json_schema
                .is_some_and(|value| Some(value) != class.json_schema.as_ref())
            || self
                .validate_schema
                .is_some_and(|value| value != class.validate_schema)
            || self
                .description
                .is_some_and(|value| value != class.description)
    }
}

pub async fn resolve_class(
    runtime: &PostgresRuntime,
    selector: StorageClassSelector,
) -> Result<StorageResolvedClass, PostgresStorageError> {
    validate_class_selector(&selector)?;
    runtime
        .with_connection(async move |connection| resolve_class_on(connection, selector).await)
        .await
}

pub(crate) async fn resolve_class_on(
    connection: &mut PostgresConnection,
    selector: StorageClassSelector,
) -> Result<StorageResolvedClass, PostgresStorageError> {
    validate_class_selector(&selector)?;
    let class = load_class_by_selector(connection, &selector).await?;
    crate::validate_persisted(
        "resolved class",
        StorageResolvedClass::try_new(selector, class.into_storage()?),
    )
}

pub async fn create_class(
    runtime: &PostgresRuntime,
    command: StorageClassCreate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageClass>, PostgresStorageError> {
    validate_class_create(&command)?;
    let context = context.clone();

    runtime
        .with_transaction(async move |connection| {
            create_class_on(connection, command, &context).await
        })
        .await
}

pub(crate) async fn create_class_on(
    connection: &mut PostgresConnection,
    command: StorageClassCreate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageClass>, PostgresStorageError> {
    validate_class_create(&command)?;
    let class = insert_class(connection, &command).await?;
    let document = AuditDocument::try_new(
        format!("Class '{}' created", class.name),
        None,
        Some(class.snapshot()),
        json!({}),
    )?;
    let event = class_event(&class, Action::Created, context, document)?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        class.into_storage()?,
        audit,
    ))
}

pub async fn update_class(
    runtime: &PostgresRuntime,
    target: &StorageResolvedClass,
    changes: StorageClassUpdate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageClass>, PostgresStorageError> {
    let target = target.clone();
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            update_class_on(connection, &target, changes, &context).await
        })
        .await
}

pub(crate) async fn update_class_on(
    connection: &mut PostgresConnection,
    target: &StorageResolvedClass,
    changes: StorageClassUpdate,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageClass>, PostgresStorageError> {
    let before = lock_resolved_class(connection, target).await?;
    validate_class_update(&changes, &before)?;
    let update = UpdateClassRow::from(&changes);
    if !update.changes(&before) {
        return Ok(StorageMutationOutcome::unchanged(before.into_storage()?));
    }
    let updated = diesel::update(
        crate::schema::hubuumclass::table.filter(crate::schema::hubuumclass::id.eq(before.id)),
    )
    .set(update)
    .get_result::<ClassRow>(connection)
    .await?;
    let document = AuditDocument::try_new(
        format!("Class '{}' updated", updated.name),
        Some(before.snapshot()),
        Some(updated.snapshot()),
        json!({}),
    )?;
    let event = class_event(&updated, Action::Updated, context, document)?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        updated.into_storage()?,
        audit,
    ))
}

pub async fn delete_class(
    runtime: &PostgresRuntime,
    target: &StorageResolvedClass,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    let target = target.clone();
    let context = context.clone();

    runtime
        .with_transaction(async move |connection| {
            delete_class_on(connection, &target, &context).await
        })
        .await
}

pub(crate) async fn delete_class_on(
    connection: &mut PostgresConnection,
    target: &StorageResolvedClass,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    let before = lock_resolved_class(connection, target).await?;
    diesel::delete(
        crate::schema::hubuumclass::table.filter(crate::schema::hubuumclass::id.eq(before.id)),
    )
    .execute(connection)
    .await?;
    let document = AuditDocument::try_new(
        format!("Class '{}' deleted", before.name),
        Some(before.snapshot()),
        None,
        json!({}),
    )?;
    let event = class_event(&before, Action::Deleted, context, document)?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed((), audit))
}

pub async fn resolve_class_names(
    runtime: &PostgresRuntime,
    class_ids: Vec<i32>,
) -> Result<Vec<(i32, String)>, PostgresStorageError> {
    if class_ids.iter().any(|id| *id <= 0) {
        return Err(PostgresStorageError::invalid_input(
            "class ids must be greater than zero",
        ));
    }
    runtime
        .with_connection(async move |connection| class_names_on(connection, class_ids).await)
        .await
}

pub(crate) async fn class_names_on(
    connection: &mut PostgresConnection,
    mut class_ids: Vec<i32>,
) -> Result<Vec<(i32, String)>, PostgresStorageError> {
    if class_ids.iter().any(|id| *id <= 0) {
        return Err(PostgresStorageError::invalid_input(
            "class ids must be greater than zero",
        ));
    }
    class_ids.sort_unstable();
    class_ids.dedup();
    if class_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq_any(&class_ids))
        .select((
            crate::schema::hubuumclass::id,
            crate::schema::hubuumclass::name,
        ))
        .order(crate::schema::hubuumclass::id.asc())
        .load::<(i32, String)>(connection)
        .await?;
    if rows.len() != class_ids.len() {
        let missing = class_ids
            .into_iter()
            .find(|id| {
                rows.binary_search_by_key(id, |(row_id, _)| *row_id)
                    .is_err()
            })
            .expect("row count mismatch must identify a missing class");
        return Err(PostgresStorageError::not_found(format!(
            "Class {missing} was not found"
        )));
    }
    Ok(rows)
}

async fn load_class_by_selector(
    connection: &mut PostgresConnection,
    selector: &StorageClassSelector,
) -> Result<ClassRow, PostgresStorageError> {
    match selector {
        StorageClassSelector::Id(class_id) => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(class_id.id()))
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
        StorageClassSelector::Name(class_name) => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::name.eq(class_name))
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
    }
}

async fn insert_class(
    connection: &mut PostgresConnection,
    command: &StorageClassCreate,
) -> Result<ClassRow, PostgresStorageError> {
    use crate::schema::hubuumclass::{
        collection_id, description, json_schema, name, validate_schema,
    };

    diesel::insert_into(crate::schema::hubuumclass::table)
        .values((
            name.eq(command.name()),
            collection_id.eq(command.collection_id().id()),
            json_schema.eq(command.json_schema()),
            validate_schema.eq(command.validates_schema()),
            description.eq(command.description()),
        ))
        .get_result(connection)
        .await
        .map_err(PostgresStorageError::from)
}

pub(crate) async fn lock_resolved_class(
    connection: &mut PostgresConnection,
    target: &StorageResolvedClass,
) -> Result<ClassRow, PostgresStorageError> {
    let resolved = target.class();
    let locked = match target.selector() {
        StorageClassSelector::Id(class_id) => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(class_id.id()))
            .filter(crate::schema::hubuumclass::id.eq(resolved.id().id()))
            .filter(crate::schema::hubuumclass::name.eq(resolved.name()))
            .filter(crate::schema::hubuumclass::collection_id.eq(resolved.collection_id().id()))
            .for_update()
            .first::<ClassRow>(connection)
            .await
            .optional()?,
        StorageClassSelector::Name(class_name) => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(resolved.id().id()))
            .filter(crate::schema::hubuumclass::name.eq(class_name))
            .filter(crate::schema::hubuumclass::collection_id.eq(resolved.collection_id().id()))
            .for_update()
            .first::<ClassRow>(connection)
            .await
            .optional()?,
    };
    let owner_key = RevisionOwner::Class.key(resolved.id().id());
    let locked = require_existing_revision_target(locked, &owner_key)?;
    assert_locked_revision_precondition(connection, &owner_key, locked.revision).await?;
    Ok(locked)
}

fn validate_class_create(command: &StorageClassCreate) -> Result<(), PostgresStorageError> {
    let Some(schema) = command.json_schema() else {
        return Ok(());
    };
    validate_json_schema(schema)?;
    if command.validates_schema() {
        validate_json_schema_for_instances(schema)?;
    }
    Ok(())
}

fn validate_class_update(
    changes: &StorageClassUpdate,
    current: &ClassRow,
) -> Result<(), PostgresStorageError> {
    if changes.json_schema().is_none() && changes.validate_schema().is_none() {
        return Ok(());
    }
    let current_policy = StorageClassSchemaPolicy::try_from_parts(
        current.json_schema.clone(),
        current.validate_schema,
    )
    .map_err(|error| PostgresStorageError::invalid_persisted_value("class schema policy", error))?;
    let schema_policy = changes
        .resolve_schema_policy(&current_policy)
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    if let Some(schema) = schema_policy.json_schema() {
        validate_json_schema(schema)?;
        if schema_policy.validates_schema() {
            validate_json_schema_for_instances(schema)?;
        }
    }
    Ok(())
}

fn class_event(
    class: &ClassRow,
    action: Action,
    context: &EventContext,
    document: AuditDocument,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::from_document(EntityType::Class, action, context.actor_kind(), document)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .and_then(|event| {
            Ok(event
                .with_context(context)
                .with_entity_id(hubuum_events_core::EventEntityId::new(class.id)?)
                .with_entity_name(&class.name)
                .with_collection_id(hubuum_domain::CollectionId::new(class.collection_id)?))
        })
}

fn validate_class_selector(selector: &StorageClassSelector) -> Result<(), PostgresStorageError> {
    let _ = selector;
    Ok(())
}

#[cfg(test)]
mod tests {
    use diesel::query_builder::QueryId;

    use super::*;

    fn assert_static_query_id<T: QueryId>(_: &T) {
        assert!(
            T::HAS_STATIC_QUERY_ID,
            "class inserts must remain eligible for prepared-statement caching"
        );
    }

    #[test]
    fn ordinary_class_insert_has_a_static_query_shape() {
        use crate::schema::hubuumclass::{
            collection_id, description, json_schema, name, validate_schema,
        };

        let schema = serde_json::json!({"type": "object"});
        let query = diesel::insert_into(crate::schema::hubuumclass::table).values((
            name.eq("cacheable-class"),
            collection_id.eq(1),
            json_schema.eq(Some(&schema)),
            validate_schema.eq(true),
            description.eq("cacheable class insert"),
        ));
        assert_static_query_id(&query);
    }
}
