//! PostgreSQL implementation of class- and object-relation lifecycles.

use chrono::NaiveDateTime;
use diesel::prelude::{ExpressionMethods, QueryDsl};
use diesel::{Insertable, Queryable, Selectable};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, ClassRelationId, ObjectId, normalize_template_alias};
use hubuum_events_core::{Action, AuditDocument, EntityType, EventContext, NewEvent};
use hubuum_storage_core::{
    StorageClassRelation, StorageClassRelationCreate, StorageMutationOutcome, StorageObject,
    StorageObjectRelation, StorageObjectRelationCreate, StorageObjectRelationCreateSelector,
    StorageObjectRelationEndpoint, StorageObjectRelationSelector, StoragePreparedClassRelation,
    StoragePreparedObjectRelation, StorageResolvedClassRelation, StorageResolvedObjectRelation,
};
use serde_json::json;

use crate::operations::class::ClassRow;
use crate::operations::event_record::append_event;
use crate::operations::object::ObjectRow;
use crate::revision::record_metadata;
use crate::{PostgresConnection, PostgresRevision, PostgresRuntime, PostgresStorageError};

#[derive(Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::hubuumclass_relation)]
pub(crate) struct ClassRelationRow {
    id: i32,
    from_hubuum_class_id: i32,
    to_hubuum_class_id: i32,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
    revision: PostgresRevision,
}

impl ClassRelationRow {
    pub(crate) fn into_storage(self) -> Result<StorageClassRelation, PostgresStorageError> {
        let relation = crate::validate_persisted(
            "class relation endpoints",
            StorageClassRelation::try_new(
                record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
                ClassId::new(self.from_hubuum_class_id)?,
                ClassId::new(self.to_hubuum_class_id)?,
            ),
        )?;
        let relation = crate::validate_persisted(
            "class relation template aliases",
            relation.try_with_template_aliases(
                self.forward_template_alias,
                self.reverse_template_alias,
            ),
        )?;
        crate::validate_persisted(
            "class relation limits",
            relation.try_with_relation_limits(self.from_max_relations, self.to_max_relations),
        )
    }

    fn snapshot(&self) -> serde_json::Value {
        json!({
            "id": self.id,
            "from_hubuum_class_id": self.from_hubuum_class_id,
            "to_hubuum_class_id": self.to_hubuum_class_id,
            "forward_template_alias": self.forward_template_alias,
            "reverse_template_alias": self.reverse_template_alias,
            "from_max_relations": self.from_max_relations,
            "to_max_relations": self.to_max_relations,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumclass_relation)]
struct NewClassRelationRow<'command> {
    from_hubuum_class_id: i32,
    to_hubuum_class_id: i32,
    forward_template_alias: Option<&'command str>,
    reverse_template_alias: Option<&'command str>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl<'command> From<&'command StorageClassRelationCreate> for NewClassRelationRow<'command> {
    fn from(command: &'command StorageClassRelationCreate) -> Self {
        Self {
            from_hubuum_class_id: command.from_class_id().id(),
            to_hubuum_class_id: command.to_class_id().id(),
            forward_template_alias: command.forward_template_alias(),
            reverse_template_alias: command.reverse_template_alias(),
            from_max_relations: command.from_max_relations(),
            to_max_relations: command.to_max_relations(),
        }
    }
}

#[derive(Clone, Copy, Queryable, Selectable)]
#[diesel(table_name = crate::schema::hubuumobject_relation)]
pub(crate) struct ObjectRelationRow {
    id: i32,
    from_hubuum_object_id: i32,
    to_hubuum_object_id: i32,
    class_relation_id: i32,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: PostgresRevision,
}

impl ObjectRelationRow {
    pub(crate) fn into_storage(self) -> Result<StorageObjectRelation, PostgresStorageError> {
        crate::validate_persisted(
            "object relation",
            StorageObjectRelation::try_new(
                record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
                ObjectId::new(self.from_hubuum_object_id)?,
                ObjectId::new(self.to_hubuum_object_id)?,
                ClassRelationId::new(self.class_relation_id)?,
            ),
        )
    }

    fn snapshot(self) -> serde_json::Value {
        json!({
            "id": self.id,
            "from_hubuum_object_id": self.from_hubuum_object_id,
            "to_hubuum_object_id": self.to_hubuum_object_id,
            "class_relation_id": self.class_relation_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "revision": self.revision,
        })
    }
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::hubuumobject_relation)]
struct NewObjectRelationRow {
    from_hubuum_object_id: i32,
    to_hubuum_object_id: i32,
    class_relation_id: i32,
}

impl From<StorageObjectRelationCreate> for NewObjectRelationRow {
    fn from(command: StorageObjectRelationCreate) -> Self {
        Self {
            from_hubuum_object_id: command.from_object_id().id(),
            to_hubuum_object_id: command.to_object_id().id(),
            class_relation_id: command.class_relation_id().id(),
        }
    }
}

/// Resolve and validate a prospective class relation before authorization.
pub async fn prepare_class_relation(
    runtime: &PostgresRuntime,
    command: StorageClassRelationCreate,
) -> Result<StoragePreparedClassRelation, PostgresStorageError> {
    let command = normalize_class_relation_create(command)?;
    runtime
        .with_connection(async move |connection| {
            prepare_class_relation_on(connection, command).await
        })
        .await
}

pub(crate) async fn prepare_class_relation_on(
    connection: &mut PostgresConnection,
    command: StorageClassRelationCreate,
) -> Result<StoragePreparedClassRelation, PostgresStorageError> {
    let command = normalize_class_relation_create(command)?;
    let (from_class, to_class) = load_class_endpoints(
        connection,
        command.from_class_id().id(),
        command.to_class_id().id(),
    )
    .await?;
    crate::validate_persisted(
        "prepared class relation",
        StoragePreparedClassRelation::try_new(
            command,
            from_class.into_storage()?,
            to_class.into_storage()?,
        ),
    )
}

/// Resolve a persisted class relation and both endpoint classes.
pub async fn resolve_class_relation(
    runtime: &PostgresRuntime,
    relation_id: i32,
) -> Result<StorageResolvedClassRelation, PostgresStorageError> {
    validate_positive_id(relation_id, "class relation id")?;
    runtime
        .with_connection(async move |connection| {
            resolve_class_relation_on(connection, relation_id).await
        })
        .await
}

pub(crate) async fn resolve_class_relation_on(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<StorageResolvedClassRelation, PostgresStorageError> {
    validate_positive_id(relation_id, "class relation id")?;
    load_resolved_class_relation(connection, relation_id).await
}

/// Create a class relation from the aggregate that was authorized.
pub async fn create_class_relation(
    runtime: &PostgresRuntime,
    prepared: &StoragePreparedClassRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, PostgresStorageError> {
    let prepared = prepared.clone();
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            create_class_relation_on(connection, &prepared, &context).await
        })
        .await
}

pub(crate) async fn create_class_relation_on(
    connection: &mut PostgresConnection,
    prepared: &StoragePreparedClassRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, PostgresStorageError> {
    let command = normalize_class_relation_create(prepared.command().clone())?;
    let from_class = lock_class(connection, command.from_class_id().id()).await?;
    let to_class = lock_class(connection, command.to_class_id().id()).await?;
    if from_class.clone().into_storage()? != *prepared.from_class()
        || to_class.clone().into_storage()? != *prepared.to_class()
    {
        return Err(PostgresStorageError::not_found(
            "Class relation endpoints no longer match the prepared target",
        ));
    }
    let relation = insert_class_relation(connection, &command).await?;
    let event = class_relation_event(
        &relation,
        Action::Created,
        context,
        &from_class,
        &to_class,
        None,
        Some(relation.snapshot()),
    )?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        crate::validate_persisted(
            "resolved class relation",
            StorageResolvedClassRelation::try_new(
                relation.into_storage()?,
                from_class.into_storage()?,
                to_class.into_storage()?,
            ),
        )?,
        audit,
    ))
}

/// Delete the exact class-relation aggregate that was authorized.
pub async fn delete_class_relation(
    runtime: &PostgresRuntime,
    target: &StorageResolvedClassRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(target.relation().metadata().id().id(), "class relation id")?;
    let target = target.clone();
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            delete_class_relation_on(connection, &target, &context).await
        })
        .await
}

pub(crate) async fn delete_class_relation_on(
    connection: &mut PostgresConnection,
    target: &StorageResolvedClassRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(target.relation().metadata().id().id(), "class relation id")?;
    let relation = lock_class_relation(connection, target.relation().metadata().id().id()).await?;
    let from_class = lock_class(connection, relation.from_hubuum_class_id).await?;
    let to_class = lock_class(connection, relation.to_hubuum_class_id).await?;
    if relation.clone().into_storage()? != *target.relation()
        || from_class.clone().into_storage()? != *target.from_class()
        || to_class.clone().into_storage()? != *target.to_class()
    {
        return Err(PostgresStorageError::not_found(
            "Class relation no longer matches the resolved target",
        ));
    }
    delete_class_relation_row(connection, relation.id).await?;
    let event = class_relation_event(
        &relation,
        Action::Deleted,
        context,
        &from_class,
        &to_class,
        Some(relation.snapshot()),
        None,
    )?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed((), audit))
}

/// Resolve and validate a prospective object relation before authorization.
pub async fn prepare_object_relation(
    runtime: &PostgresRuntime,
    selector: StorageObjectRelationCreateSelector,
) -> Result<StoragePreparedObjectRelation, PostgresStorageError> {
    validate_object_relation_create_selector(&selector)?;
    runtime
        .with_connection(async move |connection| {
            prepare_object_relation_on(connection, selector).await
        })
        .await
}

pub(crate) async fn prepare_object_relation_on(
    connection: &mut PostgresConnection,
    selector: StorageObjectRelationCreateSelector,
) -> Result<StoragePreparedObjectRelation, PostgresStorageError> {
    validate_object_relation_create_selector(&selector)?;
    let (command, from_object, to_object, class_relation) = match selector {
        StorageObjectRelationCreateSelector::Explicit(command) => {
            let command = normalize_object_relation_create(command)?;
            let (from_object, to_object) = load_object_endpoints(
                connection,
                command.from_object_id().id(),
                command.to_object_id().id(),
            )
            .await?;
            let class_relation =
                load_resolved_class_relation(connection, command.class_relation_id().id()).await?;
            (command, from_object, to_object, class_relation)
        }
        StorageObjectRelationCreateSelector::Between { from, to } => {
            let (route_from, route_to) =
                load_object_endpoints(connection, from.object_id().id(), to.object_id().id())
                    .await?;
            validate_route_objects(&from, &to, &route_from, &route_to)?;
            let class_relation =
                load_direct_class_relation(connection, from.class_id().id(), to.class_id().id())
                    .await?;
            let command = normalize_object_relation_create(StorageObjectRelationCreate::new(
                ObjectId::new(route_from.id)?,
                ObjectId::new(route_to.id)?,
                ClassRelationId::from(class_relation.relation().metadata().id()),
            ))?;
            let (from_object, to_object) = order_object_endpoints(command, route_from, route_to)?;
            (command, from_object, to_object, class_relation)
        }
    };
    validate_object_relation_membership(command, &from_object, &to_object, &class_relation)?;
    crate::validate_persisted(
        "prepared object relation",
        StoragePreparedObjectRelation::try_new(
            command,
            from_object.into_storage()?,
            to_object.into_storage()?,
            class_relation,
        ),
    )
}

/// Resolve a persisted object relation and its authorization aggregate.
pub async fn resolve_object_relation(
    runtime: &PostgresRuntime,
    selector: StorageObjectRelationSelector,
) -> Result<StorageResolvedObjectRelation, PostgresStorageError> {
    validate_object_relation_selector(&selector)?;
    runtime
        .with_connection(async move |connection| {
            resolve_object_relation_on(connection, selector).await
        })
        .await
}

pub(crate) async fn resolve_object_relation_on(
    connection: &mut PostgresConnection,
    selector: StorageObjectRelationSelector,
) -> Result<StorageResolvedObjectRelation, PostgresStorageError> {
    validate_object_relation_selector(&selector)?;
    let (relation, from_object, to_object) = match selector {
        StorageObjectRelationSelector::Id(relation_id) => {
            let relation = load_object_relation(connection, relation_id.id()).await?;
            let (from_object, to_object) = load_object_endpoints(
                connection,
                relation.from_hubuum_object_id,
                relation.to_hubuum_object_id,
            )
            .await?;
            (relation, from_object, to_object)
        }
        StorageObjectRelationSelector::Between { from, to } => {
            let (route_from, route_to) =
                load_object_endpoints(connection, from.object_id().id(), to.object_id().id())
                    .await?;
            validate_relation_route_objects(&from, &to, &route_from, &route_to)?;
            let relation = load_object_relation_between(
                connection,
                from.object_id().id(),
                to.object_id().id(),
            )
            .await?;
            let command = StorageObjectRelationCreate::new(
                ObjectId::new(relation.from_hubuum_object_id)?,
                ObjectId::new(relation.to_hubuum_object_id)?,
                ClassRelationId::new(relation.class_relation_id)?,
            );
            let (from_object, to_object) = order_object_endpoints(command, route_from, route_to)?;
            (relation, from_object, to_object)
        }
    };
    let class_relation =
        load_resolved_class_relation(connection, relation.class_relation_id).await?;
    validate_object_relation_membership(
        StorageObjectRelationCreate::new(
            ObjectId::new(relation.from_hubuum_object_id)?,
            ObjectId::new(relation.to_hubuum_object_id)?,
            ClassRelationId::new(relation.class_relation_id)?,
        ),
        &from_object,
        &to_object,
        &class_relation,
    )?;
    crate::validate_persisted(
        "resolved object relation",
        StorageResolvedObjectRelation::try_new(
            relation.into_storage()?,
            from_object.into_storage()?,
            to_object.into_storage()?,
            class_relation,
        ),
    )
}

/// Create an object relation from the aggregate that was authorized.
pub async fn create_object_relation(
    runtime: &PostgresRuntime,
    prepared: &StoragePreparedObjectRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, PostgresStorageError> {
    let prepared = prepared.clone();
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            create_object_relation_on(connection, &prepared, &context).await
        })
        .await
}

pub(crate) async fn create_object_relation_on(
    connection: &mut PostgresConnection,
    prepared: &StoragePreparedObjectRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, PostgresStorageError> {
    let command = normalize_object_relation_create(*prepared.command())?;
    let (from_object, to_object) = load_object_endpoints(
        connection,
        command.from_object_id().id(),
        command.to_object_id().id(),
    )
    .await?;
    if !object_scope_matches(&from_object, prepared.from_object())
        || !object_scope_matches(&to_object, prepared.to_object())
    {
        return Err(PostgresStorageError::not_found(
            "Object relation endpoints no longer match the prepared target",
        ));
    }
    let class_relation =
        lock_class_relation_shared(connection, command.class_relation_id().id()).await?;
    if class_relation.clone().into_storage()? != *prepared.class_relation().relation() {
        return Err(PostgresStorageError::not_found(
            "Class relation no longer matches the prepared object relation",
        ));
    }
    validate_object_relation_membership(
        command,
        &from_object,
        &to_object,
        prepared.class_relation(),
    )?;
    let relation = insert_object_relation(connection, command).await?;
    let event = object_relation_event(
        relation,
        Action::Created,
        context,
        &from_object,
        &to_object,
        None,
        Some(relation.snapshot()),
    )?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed(
        crate::validate_persisted(
            "resolved object relation",
            StorageResolvedObjectRelation::try_new(
                relation.into_storage()?,
                from_object.into_storage()?,
                to_object.into_storage()?,
                prepared.class_relation().clone(),
            ),
        )?,
        audit,
    ))
}

/// Delete the exact object-relation aggregate that was authorized.
pub async fn delete_object_relation(
    runtime: &PostgresRuntime,
    target: &StorageResolvedObjectRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(target.relation().metadata().id().id(), "object relation id")?;
    let target = target.clone();
    let context = context.clone();
    runtime
        .with_transaction(async move |connection| {
            delete_object_relation_on(connection, &target, &context).await
        })
        .await
}

pub(crate) async fn delete_object_relation_on(
    connection: &mut PostgresConnection,
    target: &StorageResolvedObjectRelation,
    context: &EventContext,
) -> Result<StorageMutationOutcome<()>, PostgresStorageError> {
    validate_positive_id(target.relation().metadata().id().id(), "object relation id")?;
    let relation = lock_object_relation(connection, target.relation().metadata().id().id()).await?;
    let (from_object, to_object) = load_object_endpoints(
        connection,
        relation.from_hubuum_object_id,
        relation.to_hubuum_object_id,
    )
    .await?;
    if relation.into_storage()? != *target.relation()
        || !object_scope_matches(&from_object, target.from_object())
        || !object_scope_matches(&to_object, target.to_object())
    {
        return Err(PostgresStorageError::not_found(
            "Object relation no longer matches the resolved target",
        ));
    }
    delete_object_relation_row(connection, relation.id).await?;
    let event = object_relation_event(
        relation,
        Action::Deleted,
        context,
        &from_object,
        &to_object,
        Some(relation.snapshot()),
        None,
    )?;
    let audit = append_event(connection, &event).await?.into_audit_receipt();
    Ok(StorageMutationOutcome::committed((), audit))
}

pub(crate) fn normalize_class_relation_create(
    command: StorageClassRelationCreate,
) -> Result<StorageClassRelationCreate, PostgresStorageError> {
    if command.from_class_id() == command.to_class_id() {
        return Err(PostgresStorageError::invalid_input(
            "from_hubuum_class_id and to_hubuum_class_id cannot be the same",
        ));
    }
    validate_relation_limit(command.from_max_relations(), "from_max_relations")?;
    validate_relation_limit(command.to_max_relations(), "to_max_relations")?;
    let mut from_class_id = command.from_class_id();
    let mut to_class_id = command.to_class_id();
    let mut forward_alias = normalize_alias(command.forward_template_alias())?;
    let mut reverse_alias = normalize_alias(command.reverse_template_alias())?;
    let mut from_limit = command.from_max_relations();
    let mut to_limit = command.to_max_relations();
    if from_class_id > to_class_id {
        std::mem::swap(&mut from_class_id, &mut to_class_id);
        std::mem::swap(&mut forward_alias, &mut reverse_alias);
        std::mem::swap(&mut from_limit, &mut to_limit);
    }
    Ok(
        StorageClassRelationCreate::builder(from_class_id, to_class_id)
            .template_aliases(forward_alias, reverse_alias)
            .relation_limits(from_limit, to_limit)
            .build(),
    )
}

fn normalize_object_relation_create(
    command: StorageObjectRelationCreate,
) -> Result<StorageObjectRelationCreate, PostgresStorageError> {
    if command.from_object_id() == command.to_object_id() {
        return Err(PostgresStorageError::invalid_input(
            "from_hubuum_object_id and to_hubuum_object_id cannot be the same",
        ));
    }
    let from_object_id = command.from_object_id().min(command.to_object_id());
    let to_object_id = command.from_object_id().max(command.to_object_id());
    Ok(StorageObjectRelationCreate::new(
        from_object_id,
        to_object_id,
        command.class_relation_id(),
    ))
}

fn normalize_alias(value: Option<&str>) -> Result<Option<String>, PostgresStorageError> {
    value
        .map(normalize_template_alias)
        .transpose()
        .map_err(|error| PostgresStorageError::invalid_input(error.into_message()))
}

fn validate_relation_limit(value: Option<i32>, field: &str) -> Result<(), PostgresStorageError> {
    if value.is_none_or(|value| value > 0) {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(format!(
            "{field} must be greater than zero"
        )))
    }
}

fn validate_object_relation_create_selector(
    selector: &StorageObjectRelationCreateSelector,
) -> Result<(), PostgresStorageError> {
    match selector {
        StorageObjectRelationCreateSelector::Explicit(command) => {
            normalize_object_relation_create(*command).map(|_| ())
        }
        StorageObjectRelationCreateSelector::Between { from, to } => {
            validate_relation_endpoints(from, to)
        }
    }
}

fn validate_object_relation_selector(
    selector: &StorageObjectRelationSelector,
) -> Result<(), PostgresStorageError> {
    match selector {
        StorageObjectRelationSelector::Id(_) => Ok(()),
        StorageObjectRelationSelector::Between { from, to } => {
            validate_relation_endpoints(from, to)
        }
    }
}

fn validate_relation_endpoints(
    from: &StorageObjectRelationEndpoint,
    to: &StorageObjectRelationEndpoint,
) -> Result<(), PostgresStorageError> {
    if from.object_id() == to.object_id() {
        return Err(PostgresStorageError::invalid_input(
            "from_hubuum_object_id and to_hubuum_object_id cannot be the same",
        ));
    }
    if from.class_id() == to.class_id() {
        return Err(PostgresStorageError::invalid_input(
            "from_hubuum_object_id and to_hubuum_object_id must not have the same class",
        ));
    }
    Ok(())
}

fn validate_route_objects(
    from: &StorageObjectRelationEndpoint,
    to: &StorageObjectRelationEndpoint,
    route_from: &ObjectRow,
    route_to: &ObjectRow,
) -> Result<(), PostgresStorageError> {
    if route_from.hubuum_class_id == from.class_id().id()
        && route_to.hubuum_class_id == to.class_id().id()
    {
        Ok(())
    } else {
        Err(PostgresStorageError::not_found(
            "Object was not found in the selected class",
        ))
    }
}

fn validate_relation_route_objects(
    from: &StorageObjectRelationEndpoint,
    to: &StorageObjectRelationEndpoint,
    route_from: &ObjectRow,
    route_to: &ObjectRow,
) -> Result<(), PostgresStorageError> {
    validate_route_objects(from, to, route_from, route_to).map_err(|_| {
        PostgresStorageError::not_found("Object relation was not found for the selected classes")
    })
}

fn validate_object_relation_membership(
    command: StorageObjectRelationCreate,
    from_object: &ObjectRow,
    to_object: &ObjectRow,
    class_relation: &StorageResolvedClassRelation,
) -> Result<(), PostgresStorageError> {
    if command.from_object_id().id() != from_object.id
        || command.to_object_id().id() != to_object.id
        || command.class_relation_id().id() != class_relation.relation().metadata().id().id()
    {
        return Err(PostgresStorageError::internal(
            "Object relation aggregate does not match its command",
        ));
    }
    validate_direct_object_endpoints(from_object, to_object)?;
    let relation = class_relation.relation();
    let matches_class_relation = (from_object.hubuum_class_id == relation.from_class_id().id()
        && to_object.hubuum_class_id == relation.to_class_id().id())
        || (from_object.hubuum_class_id == relation.to_class_id().id()
            && to_object.hubuum_class_id == relation.from_class_id().id());
    if matches_class_relation {
        Ok(())
    } else {
        Err(PostgresStorageError::invalid_input(
            "objects do not match the specified class relation",
        ))
    }
}

fn validate_direct_object_endpoints(
    from_object: &ObjectRow,
    to_object: &ObjectRow,
) -> Result<(), PostgresStorageError> {
    if from_object.hubuum_class_id == to_object.hubuum_class_id {
        Err(PostgresStorageError::invalid_input(
            "from_hubuum_object_id and to_hubuum_object_id must not have the same class",
        ))
    } else {
        Ok(())
    }
}

fn object_scope_matches(current: &ObjectRow, expected: &StorageObject) -> bool {
    current.id == expected.id().id()
        && current.collection_id == expected.collection_id().id()
        && current.hubuum_class_id == expected.class_id().id()
}

fn order_object_endpoints(
    command: StorageObjectRelationCreate,
    first: ObjectRow,
    second: ObjectRow,
) -> Result<(ObjectRow, ObjectRow), PostgresStorageError> {
    if first.id == command.from_object_id().id() && second.id == command.to_object_id().id() {
        Ok((first, second))
    } else if second.id == command.from_object_id().id() && first.id == command.to_object_id().id()
    {
        Ok((second, first))
    } else {
        Err(PostgresStorageError::internal(
            "Loaded object relation endpoints do not match the command",
        ))
    }
}

async fn load_class_endpoints(
    connection: &mut PostgresConnection,
    from_class_id: i32,
    to_class_id: i32,
) -> Result<(ClassRow, ClassRow), PostgresStorageError> {
    let classes = crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq_any([from_class_id, to_class_id]))
        .load::<ClassRow>(connection)
        .await?;
    let from_class = classes
        .iter()
        .find(|class| class.id == from_class_id)
        .cloned()
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Class {from_class_id} was not found"))
        })?;
    let to_class = classes
        .into_iter()
        .find(|class| class.id == to_class_id)
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Class {to_class_id} was not found"))
        })?;
    Ok((from_class, to_class))
}

async fn load_object_endpoints(
    connection: &mut PostgresConnection,
    from_object_id: i32,
    to_object_id: i32,
) -> Result<(ObjectRow, ObjectRow), PostgresStorageError> {
    let objects = crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::id.eq_any([from_object_id, to_object_id]))
        .load::<ObjectRow>(connection)
        .await?;
    let from_object = objects
        .iter()
        .find(|object| object.id == from_object_id)
        .cloned()
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Object {from_object_id} was not found"))
        })?;
    let to_object = objects
        .into_iter()
        .find(|object| object.id == to_object_id)
        .ok_or_else(|| {
            PostgresStorageError::not_found(format!("Object {to_object_id} was not found"))
        })?;
    Ok((from_object, to_object))
}

async fn lock_class(
    connection: &mut PostgresConnection,
    class_id: i32,
) -> Result<ClassRow, PostgresStorageError> {
    crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::id.eq(class_id))
        .for_update()
        .first::<ClassRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_class_relation(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ClassRelationRow, PostgresStorageError> {
    crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::id.eq(relation_id))
        .first::<ClassRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_class_relation(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ClassRelationRow, PostgresStorageError> {
    crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::id.eq(relation_id))
        .for_update()
        .first::<ClassRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_class_relation_shared(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ClassRelationRow, PostgresStorageError> {
    crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::id.eq(relation_id))
        .for_share()
        .first::<ClassRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_resolved_class_relation(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<StorageResolvedClassRelation, PostgresStorageError> {
    let relation = load_class_relation(connection, relation_id).await?;
    let (from_class, to_class) = load_class_endpoints(
        connection,
        relation.from_hubuum_class_id,
        relation.to_hubuum_class_id,
    )
    .await?;
    crate::validate_persisted(
        "resolved class relation",
        StorageResolvedClassRelation::try_new(
            relation.into_storage()?,
            from_class.into_storage()?,
            to_class.into_storage()?,
        ),
    )
}

async fn load_direct_class_relation(
    connection: &mut PostgresConnection,
    first_class_id: i32,
    second_class_id: i32,
) -> Result<StorageResolvedClassRelation, PostgresStorageError> {
    let from_class_id = first_class_id.min(second_class_id);
    let to_class_id = first_class_id.max(second_class_id);
    let relation = crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(from_class_id))
        .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(to_class_id))
        .first::<ClassRelationRow>(connection)
        .await
        .map_err(|error| match error {
            diesel::result::Error::NotFound => PostgresStorageError::not_found(format!(
                "No direct class relation exists between classes {first_class_id} and {second_class_id}"
            )),
            error => PostgresStorageError::from(error),
        })?;
    let (from_class, to_class) =
        load_class_endpoints(connection, from_class_id, to_class_id).await?;
    crate::validate_persisted(
        "resolved class relation",
        StorageResolvedClassRelation::try_new(
            relation.into_storage()?,
            from_class.into_storage()?,
            to_class.into_storage()?,
        ),
    )
}

async fn insert_class_relation(
    connection: &mut PostgresConnection,
    command: &StorageClassRelationCreate,
) -> Result<ClassRelationRow, PostgresStorageError> {
    diesel::insert_into(crate::schema::hubuumclass_relation::table)
        .values(NewClassRelationRow::from(command))
        .get_result::<ClassRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn delete_class_relation_row(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<(), PostgresStorageError> {
    diesel::delete(
        crate::schema::hubuumclass_relation::table
            .filter(crate::schema::hubuumclass_relation::id.eq(relation_id)),
    )
    .execute(connection)
    .await?;
    Ok(())
}

async fn load_object_relation(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ObjectRelationRow, PostgresStorageError> {
    crate::schema::hubuumobject_relation::table
        .filter(crate::schema::hubuumobject_relation::id.eq(relation_id))
        .first::<ObjectRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn lock_object_relation(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ObjectRelationRow, PostgresStorageError> {
    crate::schema::hubuumobject_relation::table
        .filter(crate::schema::hubuumobject_relation::id.eq(relation_id))
        .for_update()
        .first::<ObjectRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn load_object_relation_between(
    connection: &mut PostgresConnection,
    first_object_id: i32,
    second_object_id: i32,
) -> Result<ObjectRelationRow, PostgresStorageError> {
    let from_object_id = first_object_id.min(second_object_id);
    let to_object_id = first_object_id.max(second_object_id);
    crate::schema::hubuumobject_relation::table
        .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(from_object_id))
        .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(to_object_id))
        .first::<ObjectRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn insert_object_relation(
    connection: &mut PostgresConnection,
    command: StorageObjectRelationCreate,
) -> Result<ObjectRelationRow, PostgresStorageError> {
    diesel::insert_into(crate::schema::hubuumobject_relation::table)
        .values(NewObjectRelationRow::from(command))
        .get_result::<ObjectRelationRow>(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn delete_object_relation_row(
    connection: &mut PostgresConnection,
    relation_id: i32,
) -> Result<(), PostgresStorageError> {
    diesel::delete(
        crate::schema::hubuumobject_relation::table
            .filter(crate::schema::hubuumobject_relation::id.eq(relation_id)),
    )
    .execute(connection)
    .await?;
    Ok(())
}

fn class_relation_event(
    relation: &ClassRelationRow,
    action: Action,
    context: &EventContext,
    from_class: &ClassRow,
    to_class: &ClassRow,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> Result<NewEvent, PostgresStorageError> {
    let document = AuditDocument::try_new(
        format!(
            "Class relation {} -> {} {}",
            relation.from_hubuum_class_id,
            relation.to_hubuum_class_id,
            action_verb(action)
        ),
        before,
        after,
        json!({
            "from_class_id": from_class.id,
            "to_class_id": to_class.id,
            "related_collection_ids": [from_class.collection_id, to_class.collection_id],
        }),
    )?;
    NewEvent::from_document(
        EntityType::ClassRelation,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_context(context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(relation.id)?))
    })
}

fn object_relation_event(
    relation: ObjectRelationRow,
    action: Action,
    context: &EventContext,
    from_object: &ObjectRow,
    to_object: &ObjectRow,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
) -> Result<NewEvent, PostgresStorageError> {
    let document = AuditDocument::try_new(
        format!(
            "Object relation {} -> {} {}",
            relation.from_hubuum_object_id,
            relation.to_hubuum_object_id,
            action_verb(action)
        ),
        before,
        after,
        json!({
            "class_relation_id": relation.class_relation_id,
            "from_object_id": from_object.id,
            "to_object_id": to_object.id,
            "from_class_id": from_object.hubuum_class_id,
            "to_class_id": to_object.hubuum_class_id,
            "related_collection_ids": [from_object.collection_id, to_object.collection_id],
        }),
    )?;
    NewEvent::from_document(
        EntityType::ObjectRelation,
        action,
        context.actor_kind(),
        document,
    )
    .map_err(|error| PostgresStorageError::database(error.to_string()))
    .and_then(|event| {
        Ok(event
            .with_context(context)
            .with_entity_id(hubuum_events_core::EventEntityId::new(relation.id)?))
    })
}

const fn action_verb(action: Action) -> &'static str {
    match action {
        Action::Created => "created",
        Action::Deleted => "deleted",
        _ => "changed",
    }
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
    use super::*;

    #[test]
    fn class_relation_normalization_keeps_directional_values_with_their_classes() {
        let command =
            StorageClassRelationCreate::builder(ClassId::new(9).unwrap(), ClassId::new(3).unwrap())
                .template_aliases(
                    Some("Forward Name".to_string()),
                    Some("ReverseName".to_string()),
                )
                .relation_limits(Some(4), Some(7))
                .build();

        let normalized = normalize_class_relation_create(command).unwrap();

        assert_eq!(normalized.from_class_id(), ClassId::new(3).unwrap());
        assert_eq!(normalized.to_class_id(), ClassId::new(9).unwrap());
        assert_eq!(normalized.forward_template_alias(), Some("reverse_name"));
        assert_eq!(normalized.reverse_template_alias(), Some("forward_name"));
        assert_eq!(normalized.from_max_relations(), Some(7));
        assert_eq!(normalized.to_max_relations(), Some(4));
    }

    #[test]
    fn object_relation_normalization_orders_object_ids() {
        let normalized = normalize_object_relation_create(StorageObjectRelationCreate::new(
            ObjectId::new(9).unwrap(),
            ObjectId::new(3).unwrap(),
            ClassRelationId::new(2).unwrap(),
        ))
        .unwrap();

        assert_eq!(normalized.from_object_id(), ObjectId::new(3).unwrap());
        assert_eq!(normalized.to_object_id(), ObjectId::new(9).unwrap());
        assert_eq!(
            normalized.class_relation_id(),
            ClassRelationId::new(2).unwrap()
        );
    }

    #[test]
    fn relation_commands_reject_non_positive_limits() {
        assert!(
            normalize_class_relation_create(
                StorageClassRelationCreate::builder(
                    ClassId::new(1).unwrap(),
                    ClassId::new(2).unwrap(),
                )
                .relation_limits(Some(0), None)
                .build(),
            )
            .is_err()
        );
    }
}
