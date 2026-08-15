//! PostgreSQL implementation of the object lifecycle contract.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::{AsChangeset, JoinOnDsl, Queryable, Selectable, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_domain::validate_json_value;
use hubuum_events_core::{Action, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    StorageError, StorageObject, StorageObjectCreate, StorageObjectDataPatch,
    StorageObjectSelector, StorageObjectUpdate, StorageRecordMetadata, StorageResolvedClass,
    StorageResolvedObject,
};
use serde_json::json;

use crate::operations::class::{ClassRow, lock_resolved_class};
use crate::operations::computed_materialization::{
    ComputedEvaluationSummary, ObjectMaterializationInput, acquire_computed_class_shared_lock,
    materialize_object,
};
use crate::operations::event_record::append_event;
use crate::revision::RevisionOwner;
use crate::runtime::{assert_locked_revision_precondition, require_existing_revision_target};
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::hubuumobject)]
pub(crate) struct ObjectRow {
    pub(crate) id: i32,
    pub(crate) name: String,
    pub(crate) collection_id: i32,
    pub(crate) hubuum_class_id: i32,
    pub(crate) data: serde_json::Value,
    pub(crate) description: String,
    pub(crate) created_at: NaiveDateTime,
    pub(crate) updated_at: NaiveDateTime,
    pub(crate) revision: PostgresRevision,
}

impl ObjectRow {
    pub(crate) fn into_storage(self) -> StorageObject {
        StorageObject::new(
            StorageRecordMetadata::new(
                self.id,
                self.created_at,
                self.updated_at,
                self.revision.get(),
            ),
            self.name,
            self.collection_id,
            self.hubuum_class_id,
            self.data,
            self.description,
        )
    }

    fn snapshot(&self) -> serde_json::Value {
        json!({
            "id": self.id,
            "name": self.name,
            "collection_id": self.collection_id,
            "hubuum_class_id": self.hubuum_class_id,
            "data": self.data,
            "description": self.description,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(AsChangeset)]
#[diesel(table_name = crate::schema::hubuumobject)]
struct UpdateObjectRow<'value> {
    name: Option<&'value str>,
    collection_id: Option<i32>,
    hubuum_class_id: Option<i32>,
    data: Option<&'value serde_json::Value>,
    description: Option<&'value str>,
}

impl<'value> From<&'value StorageObjectUpdate> for UpdateObjectRow<'value> {
    fn from(update: &'value StorageObjectUpdate) -> Self {
        Self {
            name: update.name(),
            collection_id: update.collection_id(),
            hubuum_class_id: update.class_id(),
            data: update.data(),
            description: update.description(),
        }
    }
}

impl UpdateObjectRow<'_> {
    fn changes(&self, object: &ObjectRow) -> bool {
        self.name.is_some_and(|value| value != object.name)
            || self
                .collection_id
                .is_some_and(|value| value != object.collection_id)
            || self
                .hubuum_class_id
                .is_some_and(|value| value != object.hubuum_class_id)
            || self.data.is_some_and(|value| value != &object.data)
            || self
                .description
                .is_some_and(|value| value != object.description)
    }
}

pub async fn get_object(
    runtime: &PostgresRuntime,
    object_id: i32,
) -> Result<StorageResolvedObject, PostgresStorageError> {
    validate_positive_id(object_id, "object id")?;
    runtime
        .with_connection(async move |connection| get_object_on(connection, object_id).await)
        .await
}

pub(crate) async fn get_object_on(
    connection: &mut PostgresConnection,
    object_id: i32,
) -> Result<StorageResolvedObject, PostgresStorageError> {
    validate_positive_id(object_id, "object id")?;
    let (class, object) = load_object_and_class_by_id(connection, object_id).await?;
    Ok(StorageResolvedObject::new(
        StorageObjectSelector::Ids {
            class_id: class.id,
            object_id: object.id,
        },
        class.into_storage(),
        object.into_storage(),
    ))
}

pub async fn resolve_object(
    runtime: &PostgresRuntime,
    selector: StorageObjectSelector,
) -> Result<StorageResolvedObject, PostgresStorageError> {
    validate_object_selector(&selector)?;
    runtime
        .with_connection(async move |connection| resolve_object_on(connection, selector).await)
        .await
}

pub(crate) async fn resolve_object_on(
    connection: &mut PostgresConnection,
    selector: StorageObjectSelector,
) -> Result<StorageResolvedObject, PostgresStorageError> {
    validate_object_selector(&selector)?;
    let (class, object) = load_object_by_selector(connection, &selector).await?;
    Ok(StorageResolvedObject::new(
        selector,
        class.into_storage(),
        object.into_storage(),
    ))
}

pub async fn create_object(
    runtime: &PostgresRuntime,
    target: &StorageResolvedClass,
    command: StorageObjectCreate,
    context: Option<&EventContext>,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(command.class_id(), "class id")?;
    validate_positive_id(command.collection_id(), "collection id")?;
    let target = target.clone();
    let context = context.cloned();
    let operation_runtime = runtime.clone();
    runtime
        .with_transaction(async move |connection| {
            create_object_on(
                &operation_runtime,
                connection,
                &target,
                command,
                context.as_ref(),
            )
            .await
        })
        .await
}

pub(crate) async fn create_object_on(
    runtime: &PostgresRuntime,
    connection: &mut PostgresConnection,
    target: &StorageResolvedClass,
    command: StorageObjectCreate,
    context: Option<&EventContext>,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(command.class_id(), "class id")?;
    validate_positive_id(command.collection_id(), "collection id")?;
    acquire_computed_class_shared_lock(connection, target.class().id()).await?;
    let class = lock_resolved_class(connection, target).await?;
    validate_object_create(&command, &class)?;
    let object = insert_object(connection, &command).await?;
    let evaluation = materialize_object(
        connection,
        ObjectMaterializationInput::new(object.id, object.hubuum_class_id, &object.data),
    )
    .await?;
    if let Some(context) = context {
        let event = object_event(
            &object,
            Action::Created,
            context,
            format!("Object '{}' created", object.name),
        )?
        .with_after(object.snapshot());
        append_event(connection, &event).await?;
    }
    record_computed_evaluation(runtime, evaluation.as_ref());
    Ok(object.into_storage())
}

pub async fn update_object(
    runtime: &PostgresRuntime,
    target: &StorageResolvedObject,
    changes: StorageObjectUpdate,
    context: Option<&EventContext>,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    if let Some(class_id) = changes.class_id() {
        validate_positive_id(class_id, "class id")?;
    }
    if let Some(collection_id) = changes.collection_id() {
        validate_positive_id(collection_id, "collection id")?;
    }
    let target = target.clone();
    let context = context.cloned();
    let operation_runtime = runtime.clone();
    runtime
        .with_transaction(async move |connection| {
            update_object_on(
                &operation_runtime,
                connection,
                &target,
                changes,
                context.as_ref(),
            )
            .await
        })
        .await
}

pub(crate) async fn update_object_on(
    runtime: &PostgresRuntime,
    connection: &mut PostgresConnection,
    target: &StorageResolvedObject,
    changes: StorageObjectUpdate,
    context: Option<&EventContext>,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    if let Some(class_id) = changes.class_id() {
        validate_positive_id(class_id, "class id")?;
    }
    if let Some(collection_id) = changes.collection_id() {
        validate_positive_id(collection_id, "collection id")?;
    }
    let (class, before) = if context.is_some() {
        lock_resolved_object(connection, target).await?
    } else {
        lock_object_and_update_class(connection, target.object().id(), &changes).await?
    };
    let (object, evaluation) =
        persist_object_update(connection, &changes, &class, before, context).await?;
    record_computed_evaluation(runtime, evaluation.as_ref());
    Ok(object.into_storage())
}

pub async fn patch_object_data(
    runtime: &PostgresRuntime,
    target: &StorageResolvedObject,
    patch: StorageObjectDataPatch,
    context: &EventContext,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    let target = target.clone();
    let context = context.clone();
    let operation_runtime = runtime.clone();
    runtime
        .with_transaction(async move |connection| {
            patch_object_data_on(&operation_runtime, connection, &target, patch, &context).await
        })
        .await
}

pub(crate) async fn patch_object_data_on(
    runtime: &PostgresRuntime,
    connection: &mut PostgresConnection,
    target: &StorageResolvedObject,
    patch: StorageObjectDataPatch,
    context: &EventContext,
) -> Result<StorageObject, PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    let (class, before) = lock_resolved_object(connection, target).await?;
    let patched_data = patch
        .apply(&before.data)
        .map_err(postgres_error_from_storage)?;
    validate_object_state(
        before.hubuum_class_id,
        before.collection_id,
        &patched_data,
        &class,
    )?;
    if patched_data == before.data {
        let evaluation = materialize_object(
            connection,
            ObjectMaterializationInput::new(before.id, before.hubuum_class_id, &before.data),
        )
        .await?;
        record_computed_evaluation(runtime, evaluation.as_ref());
        return Ok(before.into_storage());
    }
    let updated = diesel::update(
        crate::schema::hubuumobject::table.filter(crate::schema::hubuumobject::id.eq(before.id)),
    )
    .set(crate::schema::hubuumobject::data.eq(patched_data))
    .get_result::<ObjectRow>(connection)
    .await?;
    let evaluation = materialize_object(
        connection,
        ObjectMaterializationInput::new(updated.id, updated.hubuum_class_id, &updated.data),
    )
    .await?;
    let event = object_event(
        &updated,
        Action::Updated,
        context,
        format!("Object '{}' updated", updated.name),
    )?
    .with_before(before.snapshot())
    .with_after(updated.snapshot());
    append_event(connection, &event).await?;
    record_computed_evaluation(runtime, evaluation.as_ref());
    Ok(updated.into_storage())
}

pub async fn delete_object(
    runtime: &PostgresRuntime,
    target: &StorageResolvedObject,
    context: Option<&EventContext>,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    let target = target.clone();
    let context = context.cloned();
    if context.is_none() {
        return runtime
            .with_connection(async move |connection| {
                delete_object_on(connection, &target, context.as_ref()).await
            })
            .await;
    }
    runtime
        .with_transaction(async move |connection| {
            delete_object_on(connection, &target, context.as_ref()).await
        })
        .await
}

pub(crate) async fn delete_object_on(
    connection: &mut PostgresConnection,
    target: &StorageResolvedObject,
    context: Option<&EventContext>,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(target.object().id(), "object id")?;
    if context.is_none() {
        diesel::delete(
            crate::schema::hubuumobject::table
                .filter(crate::schema::hubuumobject::id.eq(target.object().id())),
        )
        .execute(connection)
        .await?;
        return Ok(());
    }
    let (_, before) = lock_resolved_object(connection, target).await?;
    diesel::delete(
        crate::schema::hubuumobject::table.filter(crate::schema::hubuumobject::id.eq(before.id)),
    )
    .execute(connection)
    .await?;
    if let Some(context) = context {
        let event = object_event(
            &before,
            Action::Deleted,
            context,
            format!("Object '{}' deleted", before.name),
        )?
        .with_before(before.snapshot());
        append_event(connection, &event).await?;
    }
    Ok(())
}

pub async fn validate_object(
    runtime: &PostgresRuntime,
    object: StorageObject,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(object.class_id(), "class id")?;
    validate_positive_id(object.collection_id(), "collection id")?;
    runtime
        .with_connection(async move |connection| validate_object_on(connection, object).await)
        .await
}

pub(crate) async fn validate_object_on(
    connection: &mut PostgresConnection,
    object: StorageObject,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(object.class_id(), "class id")?;
    validate_positive_id(object.collection_id(), "collection id")?;
    let class = load_class(connection, object.class_id()).await?;
    validate_object_state(
        object.class_id(),
        object.collection_id(),
        object.data(),
        &class,
    )
}

pub async fn validate_object_create_command(
    runtime: &PostgresRuntime,
    command: StorageObjectCreate,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(command.class_id(), "class id")?;
    validate_positive_id(command.collection_id(), "collection id")?;
    runtime
        .with_connection(async move |connection| {
            validate_object_create_command_on(connection, command).await
        })
        .await
}

pub(crate) async fn validate_object_create_command_on(
    connection: &mut PostgresConnection,
    command: StorageObjectCreate,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(command.class_id(), "class id")?;
    validate_positive_id(command.collection_id(), "collection id")?;
    let class = load_class(connection, command.class_id()).await?;
    validate_object_create(&command, &class)
}

pub async fn validate_object_update_command(
    runtime: &PostgresRuntime,
    object_id: i32,
    changes: StorageObjectUpdate,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(object_id, "object id")?;
    if let Some(class_id) = changes.class_id() {
        validate_positive_id(class_id, "class id")?;
    }
    if let Some(collection_id) = changes.collection_id() {
        validate_positive_id(collection_id, "collection id")?;
    }
    runtime
        .with_connection(async move |connection| {
            validate_object_update_command_on(connection, object_id, changes).await
        })
        .await
}

pub(crate) async fn validate_object_update_command_on(
    connection: &mut PostgresConnection,
    object_id: i32,
    changes: StorageObjectUpdate,
) -> Result<(), PostgresStorageError> {
    validate_positive_id(object_id, "object id")?;
    if let Some(class_id) = changes.class_id() {
        validate_positive_id(class_id, "class id")?;
    }
    if let Some(collection_id) = changes.collection_id() {
        validate_positive_id(collection_id, "collection id")?;
    }
    let object = load_object(connection, object_id).await?;
    let class_id = changes.class_id().unwrap_or(object.hubuum_class_id);
    let class = load_class(connection, class_id).await?;
    validate_object_update(&changes, &object, &class)
}

async fn load_object_and_class_by_id(
    connection: &mut PostgresConnection,
    object_id: i32,
) -> Result<(ClassRow, ObjectRow), PostgresStorageError> {
    crate::schema::hubuumobject::table
        .inner_join(
            crate::schema::hubuumclass::table.on(
                crate::schema::hubuumclass::id.eq(crate::schema::hubuumobject::hubuum_class_id),
            ),
        )
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .select((ClassRow::as_select(), ObjectRow::as_select()))
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_object_by_selector(
    connection: &mut PostgresConnection,
    selector: &StorageObjectSelector,
) -> Result<(ClassRow, ObjectRow), PostgresStorageError> {
    let query = crate::schema::hubuumobject::table.inner_join(
        crate::schema::hubuumclass::table
            .on(crate::schema::hubuumclass::id.eq(crate::schema::hubuumobject::hubuum_class_id)),
    );
    match selector {
        StorageObjectSelector::Ids {
            class_id,
            object_id,
        } => query
            .filter(crate::schema::hubuumobject::id.eq(*object_id))
            .filter(crate::schema::hubuumobject::hubuum_class_id.eq(*class_id))
            .select((ClassRow::as_select(), ObjectRow::as_select()))
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
        StorageObjectSelector::Names {
            class_name,
            object_name,
        } => query
            .filter(crate::schema::hubuumclass::name.eq(class_name))
            .filter(crate::schema::hubuumobject::name.eq(object_name))
            .select((ClassRow::as_select(), ObjectRow::as_select()))
            .first(connection)
            .await
            .map_err(PostgresStorageError::from),
    }
}

async fn load_class(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<ClassRow, PostgresStorageError> {
    crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq(class_id))
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_object(
    connection: &mut PostgresConnection,
    object_id: i32,
) -> Result<ObjectRow, PostgresStorageError> {
    crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .first(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn insert_object(
    connection: &mut PostgresConnection,
    command: &StorageObjectCreate,
) -> Result<ObjectRow, PostgresStorageError> {
    use crate::schema::hubuumobject::{collection_id, data, description, hubuum_class_id, name};

    diesel::insert_into(crate::schema::hubuumobject::table)
        .values((
            name.eq(command.name()),
            collection_id.eq(command.collection_id()),
            hubuum_class_id.eq(command.class_id()),
            data.eq(command.data()),
            description.eq(command.description()),
        ))
        .get_result(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_object_and_update_class(
    connection: &mut PostgresConnection,
    object_id: i32,
    changes: &StorageObjectUpdate,
) -> Result<(ClassRow, ObjectRow), PostgresStorageError> {
    let current = load_object(connection, object_id).await?;
    let class_id = changes.class_id().unwrap_or(current.hubuum_class_id);
    acquire_computed_class_shared_lock(connection, class_id).await?;
    let class = crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq(class_id))
        .for_update()
        .first::<ClassRow>(connection)
        .await?;
    let object = crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq(object_id))
        .filter(crate::schema::hubuumobject::hubuum_class_id.eq(current.hubuum_class_id))
        .filter(crate::schema::hubuumobject::collection_id.eq(current.collection_id))
        .for_update()
        .first::<ObjectRow>(connection)
        .await?;
    Ok((class, object))
}

async fn lock_resolved_object(
    connection: &mut PostgresConnection,
    target: &StorageResolvedObject,
) -> Result<(ClassRow, ObjectRow), PostgresStorageError> {
    let resolved_class = target.class();
    let resolved = target.object();
    let owner_key = RevisionOwner::Object.key(resolved.id());
    acquire_computed_class_shared_lock(connection, resolved_class.id()).await?;
    let locked_class = match target.selector() {
        StorageObjectSelector::Ids { class_id, .. } => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(*class_id))
            .filter(crate::schema::hubuumclass::id.eq(resolved_class.id()))
            .filter(crate::schema::hubuumclass::name.eq(resolved_class.name()))
            .filter(crate::schema::hubuumclass::collection_id.eq(resolved_class.collection_id()))
            .for_update()
            .first::<ClassRow>(connection)
            .await
            .optional()?,
        StorageObjectSelector::Names { class_name, .. } => crate::schema::hubuumclass::table
            .filter(crate::schema::hubuumclass::id.eq(resolved_class.id()))
            .filter(crate::schema::hubuumclass::name.eq(class_name))
            .filter(crate::schema::hubuumclass::collection_id.eq(resolved_class.collection_id()))
            .for_update()
            .first::<ClassRow>(connection)
            .await
            .optional()?,
    };
    let locked_class = require_existing_revision_target(locked_class, &owner_key)?;

    let locked_object = match target.selector() {
        StorageObjectSelector::Ids {
            class_id,
            object_id,
        } => crate::schema::hubuumobject::table
            .filter(crate::schema::hubuumobject::id.eq(*object_id))
            .filter(crate::schema::hubuumobject::id.eq(resolved.id()))
            .filter(crate::schema::hubuumobject::name.eq(resolved.name()))
            .filter(crate::schema::hubuumobject::collection_id.eq(resolved.collection_id()))
            .filter(crate::schema::hubuumobject::hubuum_class_id.eq(*class_id))
            .filter(crate::schema::hubuumobject::hubuum_class_id.eq(resolved.class_id()))
            .for_update()
            .first::<ObjectRow>(connection)
            .await
            .optional()?,
        StorageObjectSelector::Names { object_name, .. } => crate::schema::hubuumobject::table
            .filter(crate::schema::hubuumobject::id.eq(resolved.id()))
            .filter(crate::schema::hubuumobject::hubuum_class_id.eq(resolved.class_id()))
            .filter(crate::schema::hubuumobject::collection_id.eq(resolved.collection_id()))
            .filter(crate::schema::hubuumobject::name.eq(object_name))
            .for_update()
            .first::<ObjectRow>(connection)
            .await
            .optional()?,
    };
    let locked_object = require_existing_revision_target(locked_object, &owner_key)?;
    assert_locked_revision_precondition(connection, &owner_key, locked_object.revision).await?;
    Ok((locked_class, locked_object))
}

async fn persist_object_update(
    connection: &mut PostgresConnection,
    changes: &StorageObjectUpdate,
    class: &ClassRow,
    before: ObjectRow,
    context: Option<&EventContext>,
) -> Result<(ObjectRow, Option<ComputedEvaluationSummary>), PostgresStorageError> {
    validate_object_update(changes, &before, class)?;
    let update = UpdateObjectRow::from(changes);
    if !update.changes(&before) {
        let evaluation = materialize_object(
            connection,
            ObjectMaterializationInput::new(before.id, before.hubuum_class_id, &before.data),
        )
        .await?;
        return Ok((before, evaluation));
    }
    let updated = diesel::update(
        crate::schema::hubuumobject::table.filter(crate::schema::hubuumobject::id.eq(before.id)),
    )
    .set(update)
    .get_result::<ObjectRow>(connection)
    .await?;
    let evaluation = materialize_object(
        connection,
        ObjectMaterializationInput::new(updated.id, updated.hubuum_class_id, &updated.data),
    )
    .await?;
    if let Some(context) = context {
        let event = object_event(
            &updated,
            Action::Updated,
            context,
            format!("Object '{}' updated", updated.name),
        )?
        .with_before(before.snapshot())
        .with_after(updated.snapshot());
        append_event(connection, &event).await?;
    }
    Ok((updated, evaluation))
}

fn validate_object_create(
    command: &StorageObjectCreate,
    class: &ClassRow,
) -> Result<(), PostgresStorageError> {
    validate_object_state(
        command.class_id(),
        command.collection_id(),
        command.data(),
        class,
    )
}

fn validate_object_update(
    changes: &StorageObjectUpdate,
    current: &ObjectRow,
    class: &ClassRow,
) -> Result<(), PostgresStorageError> {
    validate_object_state(
        changes.class_id().unwrap_or(current.hubuum_class_id),
        changes.collection_id().unwrap_or(current.collection_id),
        changes.data().unwrap_or(&current.data),
        class,
    )
}

fn validate_object_state(
    class_id: i32,
    collection_id: i32,
    data: &serde_json::Value,
    class: &ClassRow,
) -> Result<(), PostgresStorageError> {
    if class_id != class.id {
        return Err(PostgresStorageError::bad_request(format!(
            "Object hubuum_class_id {class_id} does not match class {}",
            class.id
        )));
    }
    if collection_id != class.collection_id {
        return Err(PostgresStorageError::bad_request(format!(
            "Object collection_id {collection_id} does not match class collection_id {}",
            class.collection_id
        )));
    }
    if class.validate_schema
        && let Some(schema) = class.json_schema.as_ref()
    {
        validate_json_value(schema, data)?;
    }
    Ok(())
}

fn object_event(
    object: &ObjectRow,
    action: Action,
    context: &EventContext,
    summary: String,
) -> Result<NewEvent, PostgresStorageError> {
    NewEvent::new(EntityType::Object, action, context.actor_kind(), summary)
        .map_err(|error| PostgresStorageError::database(error.to_string()))
        .map(|event| {
            event
                .with_context(context)
                .with_entity_id(object.id)
                .with_entity_name(&object.name)
                .with_collection_id(object.collection_id)
                .with_metadata(json!({ "class_id": object.hubuum_class_id }))
        })
}

fn record_computed_evaluation(
    runtime: &PostgresRuntime,
    summary: Option<&ComputedEvaluationSummary>,
) {
    if let Some(summary) = summary {
        runtime.record_computed_evaluation("shared", summary.error_codes());
    }
}

fn postgres_error_from_storage(error: StorageError) -> PostgresStorageError {
    let (kind, message, current_etag) = error.into_parts();
    PostgresStorageError::new(kind, message, current_etag)
}

fn validate_object_selector(selector: &StorageObjectSelector) -> Result<(), PostgresStorageError> {
    if let StorageObjectSelector::Ids {
        class_id,
        object_id,
    } = selector
    {
        validate_positive_id(*class_id, "class id")?;
        validate_positive_id(*object_id, "object id")?;
    }
    Ok(())
}

fn validate_positive_id(value: i32, noun: &str) -> Result<(), PostgresStorageError> {
    if value > 0 {
        Ok(())
    } else {
        Err(PostgresStorageError::bad_request(format!(
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
            "object inserts must remain eligible for prepared-statement caching"
        );
    }

    #[test]
    fn ordinary_object_insert_has_a_static_query_shape() {
        use crate::schema::hubuumobject::{
            collection_id, data, description, hubuum_class_id, name,
        };

        let object_data = serde_json::json!({"name": "cacheable"});
        let query = diesel::insert_into(crate::schema::hubuumobject::table).values((
            name.eq("cacheable-object"),
            collection_id.eq(1),
            hubuum_class_id.eq(2),
            data.eq(&object_data),
            description.eq("cacheable object insert"),
        ));
        assert_static_query_id(&query);
    }
}
