use super::{
    ImportRuntime, assert_import_create_condition, assert_import_revision, import_timestamp_pair,
    require_existing, resolve_class, resolve_object, with_imported_timestamp_override,
};
use crate::operations::relation::normalize_class_relation_create;
use crate::{PostgresConnection, PostgresRevision, PostgresStorageError};
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ObjectRelationLimit, RelationLabel, TemplateAlias};
use hubuum_storage_core::{
    StorageClass, StorageClassRelationCreate, StorageImportClassRelation,
    StorageImportObjectRelation, StorageImportTimestamps, StorageImportWriteCondition,
    StorageObject,
};

async fn resolve_class_relation_endpoints(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
) -> Result<(StorageClass, StorageClass), PostgresStorageError> {
    let parts = input.clone().into_parts();
    let from = resolve_class(
        connection,
        state,
        parts.from_class_ref.as_deref(),
        parts.from_class_key.as_ref(),
    )
    .await?;
    let to = resolve_class(
        connection,
        state,
        parts.to_class_ref.as_deref(),
        parts.to_class_key.as_ref(),
    )
    .await?;
    Ok((from, to))
}

pub(super) async fn create_class_relation(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportClassRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, &input).await?;
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    let command = normalize_class_relation_create(
        StorageClassRelationCreate::builder(from.id(), to.id())
            .labels(parts.forward_label, parts.reverse_label)
            .template_aliases(parts.forward_template_alias, parts.reverse_template_alias)
            .relation_limits(parts.from_max_relations, parts.to_max_relations)
            .build(),
    )?;
    match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumclass_relation::table)
                .values((
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq(command.from_class_id().id()),
                    crate::schema::hubuumclass_relation::to_hubuum_class_id
                        .eq(command.to_class_id().id()),
                    crate::schema::hubuumclass_relation::forward_label
                        .eq(command.forward_label().map(RelationLabel::as_str)),
                    crate::schema::hubuumclass_relation::reverse_label
                        .eq(command.reverse_label().map(RelationLabel::as_str)),
                    crate::schema::hubuumclass_relation::forward_template_alias
                        .eq(command.forward_template_alias().map(TemplateAlias::as_str)),
                    crate::schema::hubuumclass_relation::reverse_template_alias
                        .eq(command.reverse_template_alias().map(TemplateAlias::as_str)),
                    crate::schema::hubuumclass_relation::from_max_relations
                        .eq(command.from_max_relations().map(ObjectRelationLimit::value)),
                    crate::schema::hubuumclass_relation::to_max_relations
                        .eq(command.to_max_relations().map(ObjectRelationLimit::value)),
                    crate::schema::hubuumclass_relation::created_at.eq(created_at),
                    crate::schema::hubuumclass_relation::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
        None => {
            diesel::insert_into(crate::schema::hubuumclass_relation::table)
                .values((
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq(command.from_class_id().id()),
                    crate::schema::hubuumclass_relation::to_hubuum_class_id
                        .eq(command.to_class_id().id()),
                    crate::schema::hubuumclass_relation::forward_label
                        .eq(command.forward_label().map(RelationLabel::as_str)),
                    crate::schema::hubuumclass_relation::reverse_label
                        .eq(command.reverse_label().map(RelationLabel::as_str)),
                    crate::schema::hubuumclass_relation::forward_template_alias
                        .eq(command.forward_template_alias().map(TemplateAlias::as_str)),
                    crate::schema::hubuumclass_relation::reverse_template_alias
                        .eq(command.reverse_template_alias().map(TemplateAlias::as_str)),
                    crate::schema::hubuumclass_relation::from_max_relations
                        .eq(command.from_max_relations().map(ObjectRelationLimit::value)),
                    crate::schema::hubuumclass_relation::to_max_relations
                        .eq(command.to_max_relations().map(ObjectRelationLimit::value)),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn update_class_relation_timestamps(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
    timestamps: StorageImportTimestamps,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, input).await?;
    let parts = input.clone().into_parts();
    let pair = normalize_pair(from.id().id(), to.id().id());
    assert_relation_condition(
        class_relation_revision(connection, pair).await?,
        parts.condition,
    )?;
    let (created_at, updated_at) = import_timestamp_pair(timestamps);
    with_imported_timestamp_override(connection, async |connection| {
        diesel::update(
            crate::schema::hubuumclass_relation::table
                .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(pair.0))
                .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(pair.1)),
        )
        .set((
            crate::schema::hubuumclass_relation::created_at.eq(created_at),
            crate::schema::hubuumclass_relation::updated_at.eq(updated_at),
        ))
        .execute(connection)
        .await?;
        Ok(())
    })
    .await
}

pub(super) async fn class_relation_observed_revision(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    let (from, to) = resolve_class_relation_endpoints(connection, state, input).await?;
    class_relation_revision(connection, normalize_pair(from.id().id(), to.id().id())).await
}

pub(super) async fn check_class_relation_condition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportClassRelation,
) -> Result<(), PostgresStorageError> {
    assert_relation_condition(
        class_relation_observed_revision(connection, state, input).await?,
        input.clone().into_parts().condition,
    )
}

async fn class_relation_revision(
    connection: &mut PostgresConnection,
    pair: (i32, i32),
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    Ok(crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(pair.0))
        .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(pair.1))
        .select(crate::schema::hubuumclass_relation::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?)
}

async fn resolve_object_relation_endpoints(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
) -> Result<(StorageObject, StorageObject), PostgresStorageError> {
    let parts = input.clone().into_parts();
    let from = resolve_object(
        connection,
        state,
        parts.from_object_ref.as_deref(),
        parts.from_object_key.as_ref(),
    )
    .await?;
    let to = resolve_object(
        connection,
        state,
        parts.to_object_ref.as_deref(),
        parts.to_object_key.as_ref(),
    )
    .await?;
    Ok((from, to))
}

pub(super) async fn create_object_relation(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: StorageImportObjectRelation,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, &input).await?;
    let parts = input.into_parts();
    assert_import_create_condition(parts.condition)?;
    if from.id().id() == to.id().id() {
        return Err(PostgresStorageError::invalid_input(
            "from and to object ids cannot be the same",
        ));
    }
    let class_pair = normalize_pair(from.class_id().id(), to.class_id().id());
    let class_relation_id = crate::schema::hubuumclass_relation::table
        .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(class_pair.0))
        .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(class_pair.1))
        .select(crate::schema::hubuumclass_relation::id)
        .first::<i32>(connection)
        .await?;
    let object_pair = normalize_pair(from.id().id(), to.id().id());
    match parts.timestamps {
        Some(timestamps) => {
            let (created_at, updated_at) = import_timestamp_pair(timestamps);
            diesel::insert_into(crate::schema::hubuumobject_relation::table)
                .values((
                    crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(object_pair.0),
                    crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(object_pair.1),
                    crate::schema::hubuumobject_relation::class_relation_id.eq(class_relation_id),
                    crate::schema::hubuumobject_relation::created_at.eq(created_at),
                    crate::schema::hubuumobject_relation::updated_at.eq(updated_at),
                ))
                .execute(connection)
                .await?;
        }
        None => {
            diesel::insert_into(crate::schema::hubuumobject_relation::table)
                .values((
                    crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(object_pair.0),
                    crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(object_pair.1),
                    crate::schema::hubuumobject_relation::class_relation_id.eq(class_relation_id),
                ))
                .execute(connection)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn update_object_relation_timestamps(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
    timestamps: StorageImportTimestamps,
) -> Result<(), PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, input).await?;
    let parts = input.clone().into_parts();
    let pair = normalize_pair(from.id().id(), to.id().id());
    assert_relation_condition(
        object_relation_revision(connection, pair).await?,
        parts.condition,
    )?;
    let (created_at, updated_at) = import_timestamp_pair(timestamps);
    with_imported_timestamp_override(connection, async |connection| {
        diesel::update(
            crate::schema::hubuumobject_relation::table
                .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(pair.0))
                .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(pair.1)),
        )
        .set((
            crate::schema::hubuumobject_relation::created_at.eq(created_at),
            crate::schema::hubuumobject_relation::updated_at.eq(updated_at),
        ))
        .execute(connection)
        .await?;
        Ok(())
    })
    .await
}

pub(super) async fn object_relation_observed_revision(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    let (from, to) = resolve_object_relation_endpoints(connection, state, input).await?;
    object_relation_revision(connection, normalize_pair(from.id().id(), to.id().id())).await
}

pub(super) async fn check_object_relation_condition(
    connection: &mut PostgresConnection,
    state: &ImportRuntime,
    input: &StorageImportObjectRelation,
) -> Result<(), PostgresStorageError> {
    assert_relation_condition(
        object_relation_observed_revision(connection, state, input).await?,
        input.clone().into_parts().condition,
    )
}

async fn object_relation_revision(
    connection: &mut PostgresConnection,
    pair: (i32, i32),
) -> Result<Option<PostgresRevision>, PostgresStorageError> {
    Ok(crate::schema::hubuumobject_relation::table
        .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq(pair.0))
        .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(pair.1))
        .select(crate::schema::hubuumobject_relation::revision)
        .for_update()
        .first::<PostgresRevision>(connection)
        .await
        .optional()?)
}

fn assert_relation_condition(
    revision: Option<PostgresRevision>,
    condition: Option<StorageImportWriteCondition>,
) -> Result<(), PostgresStorageError> {
    assert_import_revision(condition, require_existing(revision, condition)?)
}

const fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}
