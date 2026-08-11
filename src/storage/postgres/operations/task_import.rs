use crate::storage::postgres::prelude::*;

use chrono::{NaiveDateTime, Utc};

use crate::errors::ApiError;
use crate::models::event_subscription::validate_subscription_parts;
use crate::models::{
    CONDITIONAL_IMPORT_TARGET_MISSING, Collection, CollectionKey, Group, HubuumClass,
    HubuumClassRelation, HubuumObject, HubuumObjectRelation, IdentityScope, ImportClassInput,
    ImportCollectionInput, ImportComputedFieldInput, ImportComputedFieldVisibility,
    ImportEventSinkInput, ImportEventSubscriptionInput, ImportExportTemplateInput,
    ImportGroupInput, ImportGroupMembershipInput, ImportIdentityScopeInput, ImportObjectInput,
    ImportPrincipalInput, ImportPrincipalSubtype, ImportRemoteTargetInput, ImportWriteCondition,
    NewHubuumClass, NewHubuumClassRelation, NewHubuumObject, NewHubuumObjectRelation,
    NewPermission, Permission, Permissions, PermissionsList, Principal, ResourceRevision,
    RestoreTimestamps, ServiceAccount, UpdateCollection, UpdateHubuumClass, UpdateHubuumObject,
    UpdatePermission, User,
};
use crate::storage::postgres::operations::collection::CollectionRowInsert;
use crate::storage::postgres::operations::object::{
    HubuumObjectRow, NewHubuumObjectRow, UpdateHubuumObjectRow,
};
use crate::storage::postgres::operations::relation_rows::{
    HubuumClassRelationRow, HubuumObjectRelationRow, NewHubuumClassRelationRow,
    NewHubuumObjectRelationRow,
};
use crate::storage::postgres::{SendAsyncFn, with_connection};

fn assert_import_revision(
    condition: Option<ImportWriteCondition>,
    current_revision: ResourceRevision,
) -> Result<(), ApiError> {
    let Some(expected_revision) = condition.and_then(ImportWriteCondition::expected_revision)
    else {
        return Ok(());
    };
    if expected_revision == current_revision {
        return Ok(());
    }
    crate::observability::metrics::revision_condition("async_stale");
    Err(ApiError::PreconditionFailed(
        format!(
            "stale_revision: expected revision {expected_revision}, observed {current_revision}"
        ),
        None,
    ))
}

fn assert_import_create_condition(condition: Option<ImportWriteCondition>) -> Result<(), ApiError> {
    if condition.is_some_and(ImportWriteCondition::requires_existing) {
        crate::observability::metrics::revision_condition("async_stale");
        return Err(ApiError::PreconditionFailed(
            CONDITIONAL_IMPORT_TARGET_MISSING.to_string(),
            None,
        ));
    }
    Ok(())
}

fn require_existing_import_target<T>(
    target: Option<T>,
    condition: Option<ImportWriteCondition>,
) -> Result<T, ApiError> {
    match target {
        Some(target) => Ok(target),
        None => {
            assert_import_create_condition(condition)?;
            Err(diesel::result::Error::NotFound.into())
        }
    }
}

pub async fn lookup_collections_by_name(
    pool: &impl crate::storage::StorageContext,
    value: &str,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    with_connection(pool, async |conn| {
        collections
            .filter(name.eq(value))
            .order(crate::schema::collections::id.asc())
            .load::<Collection>(conn)
            .await
    })
    .await
}

pub async fn lookup_root_collection(
    pool: &impl crate::storage::StorageContext,
) -> Result<Collection, ApiError> {
    with_connection(pool, lookup_root_collection_db).await
}

pub async fn lookup_collection_by_key(
    pool: &impl crate::storage::StorageContext,
    key: &CollectionKey,
) -> Result<Option<Collection>, ApiError> {
    with_connection(pool, async |conn| {
        lookup_collection_by_key_db(conn, key).await
    })
    .await
}

pub async fn lookup_collection_by_id(
    pool: &impl crate::storage::StorageContext,
    collection_id: i32,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, id};

    with_connection(pool, async |conn| {
        collections
            .filter(id.eq(collection_id))
            .first::<Collection>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_class_by_collection_and_name(
    pool: &impl crate::storage::StorageContext,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq(class_name))
            .first::<HubuumClass>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_classes_by_collection_and_names(
    pool: &impl crate::storage::StorageContext,
    collection_id_value: i32,
    class_names: &[String],
) -> Result<Vec<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    if class_names.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, async |conn| {
        hubuumclass
            .filter(collection_id.eq(collection_id_value))
            .filter(name.eq_any(class_names))
            .load::<HubuumClass>(conn)
            .await
    })
    .await
}

pub async fn lookup_object_by_class_and_name(
    pool: &impl crate::storage::StorageContext,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    with_connection(pool, async |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq(object_name))
            .first::<HubuumObjectRow>(conn)
            .await
            .optional()
    })
    .await
    .map(|row| row.map(Into::into))
}

pub async fn lookup_objects_by_class_and_names(
    pool: &impl crate::storage::StorageContext,
    class_id_value: i32,
    object_names: &[String],
) -> Result<Vec<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    if object_names.is_empty() {
        return Ok(Vec::new());
    }

    with_connection(pool, async |conn| {
        hubuumobject
            .filter(hubuum_class_id.eq(class_id_value))
            .filter(name.eq_any(object_names))
            .load::<HubuumObjectRow>(conn)
            .await
    })
    .await
    .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub async fn lookup_direct_class_relation(
    pool: &impl crate::storage::StorageContext,
    left: i32,
    right: i32,
) -> Result<Option<HubuumClassRelation>, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    let pair = normalize_pair(left, right);

    let row = with_connection(pool, async |conn| {
        hubuumclass_relation
            .filter(from_hubuum_class_id.eq(pair.0))
            .filter(to_hubuum_class_id.eq(pair.1))
            .first::<HubuumClassRelationRow>(conn)
            .await
            .optional()
    })
    .await?;
    row.map(TryInto::try_into).transpose()
}

pub async fn lookup_object_relation(
    pool: &impl crate::storage::StorageContext,
    left: i32,
    right: i32,
) -> Result<Option<HubuumObjectRelation>, ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id,
    };
    let pair = normalize_pair(left, right);

    let row = with_connection(pool, async |conn| {
        hubuumobject_relation
            .filter(from_hubuum_object_id.eq(pair.0))
            .filter(to_hubuum_object_id.eq(pair.1))
            .first::<HubuumObjectRelationRow>(conn)
            .await
            .optional()
    })
    .await?;
    Ok(row.map(Into::into))
}

pub async fn lookup_group_by_name(
    pool: &impl crate::storage::StorageContext,
    identity_scope: &str,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::{groups, identity_scopes};

    with_connection(pool, async |conn| {
        groups::table
            .inner_join(identity_scopes::table)
            .filter(groups::groupname.eq(value))
            .filter(identity_scopes::name.eq(identity_scope))
            .select(groups::all_columns)
            .first::<Group>(conn)
            .await
            .optional()
    })
    .await
}

pub async fn lookup_collection_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    value: &str,
) -> Result<Option<Collection>, ApiError> {
    let matches = lookup_collections_by_name_db(conn, value).await?;
    match matches.as_slice() {
        [] => Ok(None),
        [collection] => Ok(Some(collection.clone())),
        _ => Err(ApiError::BadRequest(format!(
            "Collection name '{value}' is ambiguous; use collection_key.path"
        ))),
    }
}

pub async fn lookup_collections_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    value: &str,
) -> Result<Vec<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name};

    collections
        .filter(name.eq(value))
        .order(crate::schema::collections::id.asc())
        .load::<Collection>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn lookup_root_collection_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, parent_collection_id};

    collections
        .filter(parent_collection_id.is_null())
        .first::<Collection>(conn)
        .await
        .map_err(ApiError::from)
}

pub async fn lookup_collection_child_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    parent_id_value: i32,
    child_name: &str,
) -> Result<Option<Collection>, ApiError> {
    use crate::schema::collections::dsl::{collections, name, parent_collection_id};

    collections
        .filter(parent_collection_id.eq(parent_id_value))
        .filter(name.eq(child_name))
        .first::<Collection>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

fn validate_collection_key_path(key: &CollectionKey) -> Result<(), ApiError> {
    if let Some(path) = &key.path {
        match path.last() {
            Some(last) if last == &key.name => Ok(()),
            Some(_) => Err(ApiError::BadRequest(format!(
                "collection_key.path must end with collection name '{}'",
                key.name
            ))),
            None if key.name == "root" => Ok(()),
            None => Err(ApiError::BadRequest(
                "collection_key.path may be empty only for the root collection".to_string(),
            )),
        }
    } else {
        Ok(())
    }
}

pub async fn lookup_collection_by_key_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    key: &CollectionKey,
) -> Result<Option<Collection>, ApiError> {
    validate_collection_key_path(key)?;

    let Some(path) = &key.path else {
        return lookup_collection_by_name_db(conn, &key.name).await;
    };

    if path.is_empty() {
        return lookup_root_collection_db(conn).await.map(Some);
    }

    let mut parent = lookup_root_collection_db(conn).await?;
    let mut current = None;
    for segment in path {
        let child = lookup_collection_child_by_name_db(conn, parent.id, segment).await?;
        let Some(child) = child else {
            return Ok(None);
        };
        parent = child.clone();
        current = Some(child);
    }

    Ok(current)
}

pub async fn lookup_class_by_collection_and_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_id_value: i32,
    class_name: &str,
) -> Result<Option<HubuumClass>, ApiError> {
    use crate::schema::hubuumclass::dsl::{collection_id, hubuumclass, name};

    hubuumclass
        .filter(collection_id.eq(collection_id_value))
        .filter(name.eq(class_name))
        .first::<HubuumClass>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_object_by_class_and_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    class_id_value: i32,
    object_name: &str,
) -> Result<Option<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuum_class_id, hubuumobject, name};

    hubuumobject
        .filter(hubuum_class_id.eq(class_id_value))
        .filter(name.eq(object_name))
        .first::<HubuumObjectRow>(conn)
        .await
        .optional()
        .map(|row| row.map(Into::into))
        .map_err(ApiError::from)
}

pub async fn lookup_group_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    identity_scope: &str,
    value: &str,
) -> Result<Option<Group>, ApiError> {
    use crate::schema::{groups, identity_scopes};

    groups::table
        .inner_join(identity_scopes::table)
        .filter(groups::groupname.eq(value))
        .filter(identity_scopes::name.eq(identity_scope))
        .select(groups::all_columns)
        .first::<Group>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_identity_scope_id_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    scope_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::identity_scopes::dsl::{id, identity_scopes, name};

    identity_scopes
        .filter(name.eq(scope_name))
        .select(id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_principal_id_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    identity_scope: &str,
    principal_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::{identity_scopes, principals};

    principals::table
        .inner_join(identity_scopes::table)
        .filter(principals::name.eq(principal_name))
        .filter(identity_scopes::name.eq(identity_scope))
        .select(principals::id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn lookup_event_sink_id_by_name_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    sink_name: &str,
) -> Result<Option<i32>, ApiError> {
    use crate::schema::event_sinks::dsl::{event_sinks, id, name};

    event_sinks
        .filter(name.eq(sink_name))
        .select(id)
        .first::<i32>(conn)
        .await
        .optional()
        .map_err(ApiError::from)
}

pub async fn create_collection_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportCollectionInput,
    parent_collection_id: Option<i32>,
) -> Result<Collection, ApiError> {
    assert_import_create_condition(input.condition)?;
    let insert = CollectionRowInsert::new(&input.name, &input.description)
        .parent_collection_id(parent_collection_id);
    let insert = match input.timestamps.as_ref() {
        Some(timestamps) => insert.timestamps(timestamps.created_at(), timestamps.updated_at()),
        None => insert,
    };
    crate::storage::postgres::operations::collection::insert_collection_row_with_closure(
        conn, insert,
    )
    .await
}

pub async fn update_collection_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_id_value: i32,
    input: &ImportCollectionInput,
) -> Result<Collection, ApiError> {
    use crate::schema::collections::dsl::{collections, created_at, id, updated_at};

    let current_revision = collections
        .filter(id.eq(collection_id_value))
        .select(crate::schema::collections::revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let current_revision = require_existing_import_target(current_revision, input.condition)?;
    assert_import_revision(input.condition, current_revision)?;

    let update = UpdateCollection {
        name: Some(input.name.clone()),
        description: Some(input.description.clone()),
    };

    if let Some(timestamps) = input.timestamps.as_ref() {
        return with_imported_timestamp_override(conn, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(collections.filter(id.eq(collection_id_value)))
                    .set((
                        &update,
                        created_at.eq(timestamps.created_at()),
                        updated_at.eq(timestamps.updated_at()),
                    ))
                    .get_result::<Collection>(conn)
                    .await
                    .optional(),
                async || {
                    collections
                        .filter(id.eq(collection_id_value))
                        .first(conn)
                        .await
                },
            )
            .await
            .map_err(ApiError::from)
        })
        .await;
    }

    crate::storage::postgres::updated_or_current(
        diesel::update(collections.filter(id.eq(collection_id_value)))
            .set(&update)
            .get_result::<Collection>(conn)
            .await
            .optional(),
        async || {
            collections
                .filter(id.eq(collection_id_value))
                .first(conn)
                .await
        },
    )
    .await
    .map_err(ApiError::from)
}

pub async fn create_class_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportClassInput,
    collection_id_value: i32,
) -> Result<HubuumClass, ApiError> {
    assert_import_create_condition(input.condition)?;
    use crate::schema::hubuumclass::dsl::{created_at, hubuumclass, updated_at};

    let new_class = NewHubuumClass {
        name: input.name.clone(),
        collection_id: collection_id_value,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: input.description.clone(),
    };

    match input.timestamps.as_ref() {
        Some(timestamps) => diesel::insert_into(hubuumclass)
            .values((
                &new_class,
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumClass>(conn)
            .await
            .map_err(ApiError::from),
        None => diesel::insert_into(hubuumclass)
            .values(&new_class)
            .get_result::<HubuumClass>(conn)
            .await
            .map_err(ApiError::from),
    }
}

pub async fn update_class_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    class_id_value: i32,
    input: &ImportClassInput,
) -> Result<HubuumClass, ApiError> {
    use crate::schema::hubuumclass::dsl::{created_at, hubuumclass, id, updated_at};

    let current_revision = hubuumclass
        .filter(id.eq(class_id_value))
        .select(crate::schema::hubuumclass::revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let current_revision = require_existing_import_target(current_revision, input.condition)?;
    assert_import_revision(input.condition, current_revision)?;

    let update = UpdateHubuumClass {
        name: Some(input.name.clone()),
        collection_id: None,
        json_schema: input.json_schema.clone(),
        validate_schema: input.validate_schema,
        description: Some(input.description.clone()),
    };

    if let Some(timestamps) = input.timestamps.as_ref() {
        return with_imported_timestamp_override(conn, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(hubuumclass.filter(id.eq(class_id_value)))
                    .set((
                        &update,
                        created_at.eq(timestamps.created_at()),
                        updated_at.eq(timestamps.updated_at()),
                    ))
                    .get_result::<HubuumClass>(conn)
                    .await
                    .optional(),
                async || hubuumclass.filter(id.eq(class_id_value)).first(conn).await,
            )
            .await
            .map_err(ApiError::from)
        })
        .await;
    }

    crate::storage::postgres::updated_or_current(
        diesel::update(hubuumclass.filter(id.eq(class_id_value)))
            .set(&update)
            .get_result::<HubuumClass>(conn)
            .await
            .optional(),
        async || hubuumclass.filter(id.eq(class_id_value)).first(conn).await,
    )
    .await
    .map_err(ApiError::from)
}

pub async fn create_object_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportObjectInput,
    class: &HubuumClass,
) -> Result<HubuumObject, ApiError> {
    assert_import_create_condition(input.condition)?;
    use crate::schema::hubuumobject::dsl::{created_at, hubuumobject, updated_at};

    let new_object = NewHubuumObject {
        name: input.name.clone(),
        collection_id: class.collection_id,
        hubuum_class_id: class.id,
        data: input.data.clone(),
        description: input.description.clone(),
    };

    let object = match input.timestamps.as_ref() {
        Some(timestamps) => diesel::insert_into(hubuumobject)
            .values((
                NewHubuumObjectRow::from(&new_object),
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumObjectRow>(conn)
            .await
            .map_err(ApiError::from)?,
        None => diesel::insert_into(hubuumobject)
            .values(NewHubuumObjectRow::from(&new_object))
            .get_result::<HubuumObjectRow>(conn)
            .await
            .map_err(ApiError::from)?,
    };
    let object = object.into();
    crate::storage::postgres::operations::computed_field::materialize_object_in_transaction(
        conn, &object,
    )
    .await?;
    Ok(object)
}

pub async fn update_object_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    object_id_value: i32,
    input: &ImportObjectInput,
) -> Result<HubuumObject, ApiError> {
    use crate::schema::hubuumobject::dsl::{created_at, hubuumobject, id, updated_at};

    let current_revision = hubuumobject
        .filter(id.eq(object_id_value))
        .select(crate::schema::hubuumobject::revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let current_revision = require_existing_import_target(current_revision, input.condition)?;
    assert_import_revision(input.condition, current_revision)?;

    let update = UpdateHubuumObject {
        name: Some(input.name.clone()),
        collection_id: None,
        hubuum_class_id: None,
        data: Some(input.data.clone()),
        description: Some(input.description.clone()),
    };

    let object = if let Some(timestamps) = input.timestamps.as_ref() {
        with_imported_timestamp_override(conn, async |conn| {
            crate::storage::postgres::updated_or_current(
                diesel::update(hubuumobject.filter(id.eq(object_id_value)))
                    .set((
                        UpdateHubuumObjectRow::from(&update),
                        created_at.eq(timestamps.created_at()),
                        updated_at.eq(timestamps.updated_at()),
                    ))
                    .get_result::<HubuumObjectRow>(conn)
                    .await
                    .optional(),
                async || {
                    hubuumobject
                        .filter(id.eq(object_id_value))
                        .first(conn)
                        .await
                },
            )
            .await
            .map_err(ApiError::from)
        })
        .await?
    } else {
        crate::storage::postgres::updated_or_current(
            diesel::update(hubuumobject.filter(id.eq(object_id_value)))
                .set(UpdateHubuumObjectRow::from(&update))
                .get_result::<HubuumObjectRow>(conn)
                .await
                .optional(),
            async || {
                hubuumobject
                    .filter(id.eq(object_id_value))
                    .first(conn)
                    .await
            },
        )
        .await
        .map_err(ApiError::from)?
    };
    let object = object.into();
    crate::storage::postgres::operations::computed_field::materialize_object_in_transaction(
        conn, &object,
    )
    .await?;
    Ok(object)
}

fn imported_timestamps(timestamps: Option<&RestoreTimestamps>) -> (NaiveDateTime, NaiveDateTime) {
    let now = Utc::now().naive_utc();
    timestamps
        .map(RestoreTimestamps::as_pair)
        .unwrap_or((now, now))
}

async fn set_imported_timestamp_override(
    conn: &mut crate::storage::postgres::PostgresConnection,
    value: &str,
) -> Result<(), ApiError> {
    diesel::sql_query("SELECT set_config('hubuum.preserve_imported_timestamps', $1, true)")
        .bind::<diesel::sql_types::Text, _>(value)
        .execute(conn)
        .await?;
    Ok(())
}

async fn with_imported_timestamp_override<F, R>(
    conn: &mut crate::storage::postgres::PostgresConnection,
    operation: F,
) -> Result<R, ApiError>
where
    F: for<'conn> AsyncFnOnce(
            &'conn mut crate::storage::postgres::PostgresConnection,
        ) -> Result<R, ApiError>
        + for<'conn> SendAsyncFn<
            &'conn mut crate::storage::postgres::PostgresConnection,
            Result<R, ApiError>,
            Fut: Send,
        > + Send,
    R: Send,
{
    conn.transaction::<R, ApiError, _>(async move |conn| {
        let previous = diesel::select(diesel::dsl::sql::<
            diesel::sql_types::Nullable<diesel::sql_types::Text>,
        >(
            "current_setting('hubuum.preserve_imported_timestamps', true)",
        ))
        .get_result::<Option<String>>(conn)
        .await?;
        set_imported_timestamp_override(conn, "on").await?;
        let result = operation(conn).await?;
        set_imported_timestamp_override(conn, previous.as_deref().unwrap_or("off")).await?;
        Ok(result)
    })
    .await
}

pub async fn upsert_identity_scope_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportIdentityScopeInput,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::identity_scopes::dsl::{
        created_at, id, identity_scopes, name, provider_kind, updated_at,
    };
    let existing = identity_scopes
        .filter(name.eq(&input.name))
        .for_update()
        .first::<IdentityScope>(conn)
        .await
        .optional()?;
    let row = match existing {
        Some(existing) if !overwrite => {
            assert_import_revision(input.condition, existing.revision)?;
            return Err(ApiError::Conflict(format!(
                "Identity scope '{}' already exists",
                input.name
            )));
        }
        Some(existing) => {
            assert_import_revision(input.condition, existing.revision)?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(conn, async |conn| {
                crate::storage::postgres::updated_or_current(
                    diesel::update(identity_scopes.filter(id.eq(existing.id)))
                        .set((
                            provider_kind.eq(&input.provider_kind),
                            created_at.eq(created),
                            updated_at.eq(updated),
                        ))
                        .get_result::<IdentityScope>(conn)
                        .await
                        .optional(),
                    async || identity_scopes.filter(id.eq(existing.id)).first(conn).await,
                )
                .await
                .map_err(ApiError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(input.condition)?;
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(identity_scopes)
                .values((
                    name.eq(&input.name),
                    provider_kind.eq(&input.provider_kind),
                    created_at.eq(created),
                    updated_at.eq(updated),
                ))
                .get_result::<IdentityScope>(conn)
                .await?
        }
    };
    Ok(row.id)
}

pub async fn upsert_group_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportGroupInput,
    identity_scope_id_value: i32,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::groups::dsl::*;
    let existing = groups
        .filter(identity_scope_id.eq(identity_scope_id_value))
        .filter(groupname.eq(&input.groupname))
        .for_update()
        .first::<Group>(conn)
        .await
        .optional()?;
    let row = match existing {
        Some(existing) if !overwrite => {
            assert_import_revision(input.condition, existing.revision)?;
            return Err(ApiError::Conflict(format!(
                "Group '{}' already exists in its identity scope",
                input.groupname
            )));
        }
        Some(existing) => {
            assert_import_revision(input.condition, existing.revision)?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(conn, async |conn| {
                crate::storage::postgres::updated_or_current(
                    diesel::update(groups.filter(id.eq(existing.id)))
                        .set((
                            description.eq(&input.description),
                            managed_by.eq(&input.managed_by),
                            external_key.eq(&input.external_key),
                            last_sync_attempted_at.eq(input.last_sync_attempted_at),
                            last_sync_success_at.eq(input.last_sync_success_at),
                            created_at.eq(created),
                            updated_at.eq(updated),
                        ))
                        .get_result::<Group>(conn)
                        .await
                        .optional(),
                    async || groups.filter(id.eq(existing.id)).first(conn).await,
                )
                .await
                .map_err(ApiError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(input.condition)?;
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(groups)
                .values((
                    groupname.eq(&input.groupname),
                    description.eq(&input.description),
                    identity_scope_id.eq(identity_scope_id_value),
                    managed_by.eq(&input.managed_by),
                    external_key.eq(&input.external_key),
                    last_sync_attempted_at.eq(input.last_sync_attempted_at),
                    last_sync_success_at.eq(input.last_sync_success_at),
                    created_at.eq(created),
                    updated_at.eq(updated),
                ))
                .get_result::<Group>(conn)
                .await?
        }
    };
    Ok(row.id)
}

pub async fn upsert_principal_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportPrincipalInput,
    identity_scope_id_value: i32,
    owner_group_id_value: Option<i32>,
    created_by_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    input.validate_credentials()?;
    let supplied_password = match &input.subtype {
        ImportPrincipalSubtype::Human {
            password: Some(password),
            password_hash: None,
            ..
        } => Some(
            crate::utilities::auth::hash_password_async(password.clone())
                .await
                .map_err(|error| ApiError::HashError(error.to_string()))?,
        ),
        ImportPrincipalSubtype::Human {
            password: None,
            password_hash,
            ..
        } => password_hash.clone(),
        _ => None,
    };
    use crate::schema::principals::dsl as p;
    let expected_kind = match &input.subtype {
        ImportPrincipalSubtype::Human { .. } => "human",
        ImportPrincipalSubtype::ServiceAccount { .. } => "service_account",
    };
    let existing = p::principals
        .filter(p::identity_scope_id.eq(identity_scope_id_value))
        .filter(p::name.eq(&input.name))
        .for_update()
        .first::<Principal>(conn)
        .await
        .optional()?;
    if let Some(existing) = &existing {
        assert_import_revision(input.condition, existing.revision)?;
        if !overwrite {
            return Err(ApiError::Conflict(format!(
                "Principal '{}' already exists in its identity scope",
                input.name
            )));
        }
        if existing.kind != expected_kind {
            return Err(ApiError::Conflict(format!(
                "Principal '{}' exists with kind '{}' instead of '{}'",
                input.name, existing.kind, expected_kind
            )));
        }
    }
    let principal = match existing {
        Some(existing) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing.created_at, existing.updated_at));
            with_imported_timestamp_override(conn, async |conn| {
                crate::storage::postgres::updated_or_current(
                    diesel::update(p::principals.filter(p::id.eq(existing.id)))
                        .set((
                            p::provider_managed.eq(input.provider_managed),
                            p::settings.eq(&input.settings),
                            p::external_subject.eq(&input.external_subject),
                            p::last_sync_attempted_at.eq(input.last_sync_attempted_at),
                            p::last_sync_success_at.eq(input.last_sync_success_at),
                            p::created_at.eq(created),
                            p::updated_at.eq(updated),
                        ))
                        .get_result::<Principal>(conn)
                        .await
                        .optional(),
                    async || {
                        p::principals
                            .filter(p::id.eq(existing.id))
                            .first(conn)
                            .await
                    },
                )
                .await
                .map_err(ApiError::from)
            })
            .await?
        }
        None => {
            assert_import_create_condition(input.condition)?;
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            diesel::insert_into(p::principals)
                .values((
                    p::kind.eq(expected_kind),
                    p::name.eq(&input.name),
                    p::identity_scope_id.eq(identity_scope_id_value),
                    p::provider_managed.eq(input.provider_managed),
                    p::settings.eq(&input.settings),
                    p::external_subject.eq(&input.external_subject),
                    p::last_sync_attempted_at.eq(input.last_sync_attempted_at),
                    p::last_sync_success_at.eq(input.last_sync_success_at),
                    p::created_at.eq(created),
                    p::updated_at.eq(updated),
                ))
                .get_result::<Principal>(conn)
                .await?
        }
    };

    match &input.subtype {
        ImportPrincipalSubtype::Human {
            password: _,
            password_hash: _,
            proper_name,
            email,
            anonymized_at,
        } => {
            use crate::schema::users::dsl as u;
            let existing_user = u::users
                .filter(u::id.eq(principal.id))
                .first::<User>(conn)
                .await
                .optional()?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .or_else(|| {
                    existing_user
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if let Some(existing_user) = existing_user {
                with_imported_timestamp_override(conn, async |conn| {
                    diesel::update(u::users.filter(u::id.eq(principal.id)))
                        .set((
                            u::password.eq(supplied_password.or(existing_user.password)),
                            u::proper_name.eq(proper_name),
                            u::email.eq(email),
                            u::anonymized_at.eq(*anonymized_at),
                            u::created_at.eq(created),
                            u::updated_at.eq(updated),
                        ))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(u::users)
                    .values((
                        u::id.eq(principal.id),
                        u::kind.eq("human"),
                        u::password.eq(supplied_password),
                        u::proper_name.eq(proper_name),
                        u::email.eq(email),
                        u::anonymized_at.eq(*anonymized_at),
                        u::created_at.eq(created),
                        u::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
        ImportPrincipalSubtype::ServiceAccount {
            description: account_description,
            disabled_at,
            ..
        } => {
            let owner_group_id_value = owner_group_id_value.ok_or_else(|| {
                ApiError::BadRequest("Service-account import requires an owner group".to_string())
            })?;
            use crate::schema::service_accounts::dsl as s;
            let existing_account = s::service_accounts
                .filter(s::id.eq(principal.id))
                .first::<ServiceAccount>(conn)
                .await
                .optional()?;
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .or_else(|| {
                    existing_account
                        .as_ref()
                        .map(|row| (row.created_at, row.updated_at))
                })
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_account.is_some() {
                with_imported_timestamp_override(conn, async |conn| {
                    diesel::update(s::service_accounts.filter(s::id.eq(principal.id)))
                        .set((
                            s::description.eq(account_description),
                            s::owner_group_id.eq(owner_group_id_value),
                            s::created_by.eq(created_by_value),
                            s::disabled_at.eq(*disabled_at),
                            s::created_at.eq(created),
                            s::updated_at.eq(updated),
                        ))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
                .await?;
            } else {
                diesel::insert_into(s::service_accounts)
                    .values((
                        s::id.eq(principal.id),
                        s::kind.eq("service_account"),
                        s::description.eq(account_description),
                        s::owner_group_id.eq(owner_group_id_value),
                        s::created_by.eq(created_by_value),
                        s::disabled_at.eq(*disabled_at),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
    }
    Ok(principal.id)
}

pub async fn upsert_group_membership_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportGroupMembershipInput,
    principal_id_value: i32,
    group_id_value: i32,
    source_scope_ids: &[i32],
    overwrite: bool,
) -> Result<(), ApiError> {
    use crate::schema::group_membership_sources::dsl as s;
    use crate::schema::group_memberships::dsl as m;

    let existing_membership = m::group_memberships
        .filter(m::principal_id.eq(principal_id_value))
        .filter(m::group_id.eq(group_id_value))
        .select((m::created_at, m::updated_at, m::revision))
        .for_update()
        .first::<(NaiveDateTime, NaiveDateTime, ResourceRevision)>(conn)
        .await
        .optional()?;
    if let Some((_, _, revision)) = existing_membership {
        assert_import_revision(input.condition, revision)?;
        if !overwrite {
            return Err(ApiError::Conflict(format!(
                "Principal {principal_id_value} is already a member of group {group_id_value}"
            )));
        }
    } else {
        assert_import_create_condition(input.condition)?;
    }

    with_imported_timestamp_override(conn, async |conn| {
        let membership_timestamps = input
            .timestamps
            .as_ref()
            .map(RestoreTimestamps::as_pair)
            .or(existing_membership.map(|(created, updated, _)| (created, updated)))
            .unwrap_or_else(|| imported_timestamps(None));
        match existing_membership {
            Some(_) => {
                diesel::update(
                    m::group_memberships
                        .filter(m::principal_id.eq(principal_id_value))
                        .filter(m::group_id.eq(group_id_value)),
                )
                .set((
                    m::created_at.eq(membership_timestamps.0),
                    m::updated_at.eq(membership_timestamps.1),
                ))
                .execute(conn)
                .await?;
            }
            None => {
                diesel::insert_into(m::group_memberships)
                    .values((
                        m::principal_id.eq(principal_id_value),
                        m::group_id.eq(group_id_value),
                        m::created_at.eq(membership_timestamps.0),
                        m::updated_at.eq(membership_timestamps.1),
                    ))
                    .execute(conn)
                    .await?;
            }
        }

        for (source, source_scope_id_value) in input.sources.iter().zip(source_scope_ids) {
            let existing_source = s::group_membership_sources
                .filter(s::principal_id.eq(principal_id_value))
                .filter(s::group_id.eq(group_id_value))
                .filter(s::source.eq(&source.source))
                .filter(s::source_scope_id.eq(*source_scope_id_value))
                .filter(s::source_key.eq(&source.source_key))
                .select((s::created_at, s::updated_at))
                .first::<(NaiveDateTime, NaiveDateTime)>(conn)
                .await
                .optional()?;
            let source_timestamps = source
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .or(existing_source)
                .unwrap_or_else(|| imported_timestamps(None));
            if existing_source.is_some() {
                diesel::update(
                    s::group_membership_sources
                        .filter(s::principal_id.eq(principal_id_value))
                        .filter(s::group_id.eq(group_id_value))
                        .filter(s::source.eq(&source.source))
                        .filter(s::source_scope_id.eq(*source_scope_id_value))
                        .filter(s::source_key.eq(&source.source_key)),
                )
                .set((
                    s::created_at.eq(source_timestamps.0),
                    s::updated_at.eq(source_timestamps.1),
                ))
                .execute(conn)
                .await?;
            } else {
                diesel::insert_into(s::group_membership_sources)
                    .values((
                        s::principal_id.eq(principal_id_value),
                        s::group_id.eq(group_id_value),
                        s::source.eq(&source.source),
                        s::source_scope_id.eq(*source_scope_id_value),
                        s::source_key.eq(&source.source_key),
                        s::created_at.eq(source_timestamps.0),
                        s::updated_at.eq(source_timestamps.1),
                    ))
                    .execute(conn)
                    .await?;
            }
        }
        Ok(())
    })
    .await
}

pub async fn upsert_computed_field_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportComputedFieldInput,
    class_id_value: i32,
    owner_id_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::computed_field_definitions::dsl as d;

    crate::models::ComputedFieldDefinitionRequest {
        key: input.key.clone(),
        label: input.label.clone(),
        description: input.description.clone(),
        operation: input.operation.clone(),
        result_type: input.result_type,
        enabled: input.enabled,
    }
    .validate()?;

    let visibility = match input.visibility {
        ImportComputedFieldVisibility::Shared => "shared",
        ImportComputedFieldVisibility::Personal => "personal",
    };
    let existing = d::computed_field_definitions
        .filter(d::class_id.eq(class_id_value))
        .filter(d::visibility.eq(visibility))
        .filter(d::key.eq(&input.key))
        .filter(d::owner_user_id.is_not_distinct_from(owner_id_value))
        .for_update()
        .select(crate::models::ComputedFieldDefinition::as_select())
        .first(conn)
        .await
        .optional()?;

    if let Some(existing) = existing {
        assert_import_revision(input.condition, existing.revision)?;
        if !overwrite {
            return Err(ApiError::Conflict(format!(
                "Computed field '{}' already exists in its scope",
                input.key
            )));
        }
        let (created, updated) = input
            .timestamps
            .as_ref()
            .map(RestoreTimestamps::as_pair)
            .unwrap_or((existing.created_at, existing.updated_at));
        let (definition, changed) = with_imported_timestamp_override(conn, async |conn| {
            let updated_definition =
                diesel::update(d::computed_field_definitions.filter(d::id.eq(existing.id)))
                    .set((
                        d::label.eq(&input.label),
                        d::description.eq(&input.description),
                        d::operation.eq(&input.operation),
                        d::result_type.eq(input.result_type.as_str()),
                        d::enabled.eq(input.enabled),
                        d::created_at.eq(created),
                        d::updated_at.eq(updated),
                    ))
                    .returning(crate::models::ComputedFieldDefinition::as_returning())
                    .get_result(conn)
                    .await
                    .optional()?;
            match updated_definition {
                Some(definition) => Ok((definition, true)),
                None => Ok((
                    d::computed_field_definitions
                        .find(existing.id)
                        .select(crate::models::ComputedFieldDefinition::as_select())
                        .first(conn)
                        .await?,
                    false,
                )),
            }
        })
        .await?;
        if changed && matches!(input.visibility, ImportComputedFieldVisibility::Shared) {
            crate::storage::postgres::operations::computed_field::advance_revision_and_enqueue(
                conn,
                class_id_value,
                None,
            )
            .await?;
        }
        return Ok(definition.id);
    }

    assert_import_create_condition(input.condition)?;
    let (created, updated) = imported_timestamps(input.timestamps.as_ref());
    let definition = diesel::insert_into(d::computed_field_definitions)
        .values((
            d::class_id.eq(class_id_value),
            d::visibility.eq(visibility),
            d::owner_user_id.eq(owner_id_value),
            d::key.eq(&input.key),
            d::label.eq(&input.label),
            d::description.eq(&input.description),
            d::operation.eq(&input.operation),
            d::result_type.eq(input.result_type.as_str()),
            d::enabled.eq(input.enabled),
            d::semantics_version.eq(hubuum_computed_fields::SEMANTICS_VERSION),
            d::created_by.eq(owner_id_value),
            d::updated_by.eq(owner_id_value),
            d::created_at.eq(created),
            d::updated_at.eq(updated),
        ))
        .returning(crate::models::ComputedFieldDefinition::as_returning())
        .get_result::<crate::models::ComputedFieldDefinition>(conn)
        .await?;
    if matches!(input.visibility, ImportComputedFieldVisibility::Shared) {
        crate::storage::postgres::operations::computed_field::advance_revision_and_enqueue(
            conn,
            class_id_value,
            None,
        )
        .await?;
    }
    Ok(definition.id)
}

pub async fn load_export_template_sources_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_id_value: i32,
) -> Result<Vec<(String, String)>, ApiError> {
    use crate::schema::export_templates::dsl as t;

    Ok(t::export_templates
        .filter(t::collection_id.eq(collection_id_value))
        .order(t::id.asc())
        .select((t::name, t::template))
        .load::<(String, String)>(conn)
        .await?)
}

pub async fn upsert_export_template_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportExportTemplateInput,
    collection_id_value: i32,
    class_id_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::export_templates::dsl as t;
    let existing = t::export_templates
        .filter(t::collection_id.eq(collection_id_value))
        .filter(t::name.eq(&input.name))
        .select((t::id, t::created_at, t::updated_at, t::revision))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, ResourceRevision)>(conn)
        .await
        .optional()?;
    if let Some((_, _, _, revision)) = existing {
        assert_import_revision(input.condition, revision)?;
    } else {
        assert_import_create_condition(input.condition)?;
    }
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Export template '{}' already exists in the collection",
            input.name
        )));
    }
    let include = input
        .include
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let relation_context = input
        .relation_context
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let default_limits = input
        .default_limits
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;
    let scope_kind = input.scope_kind.map(|value| value.as_str().to_string());
    let missing_policy = input
        .default_missing_data_policy
        .map(|value| value.as_str().to_string());
    match existing {
        Some((existing_id, existing_created, existing_updated, _)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing_created, existing_updated));
            with_imported_timestamp_override(conn, async |conn| {
                diesel::update(t::export_templates.filter(t::id.eq(existing_id)))
                    .set((
                        t::description.eq(&input.description),
                        t::content_type.eq(input.content_type.as_mime()),
                        t::template.eq(&input.template),
                        t::kind.eq(input.kind.as_str()),
                        t::scope_kind.eq(scope_kind),
                        t::class_id.eq(class_id_value),
                        t::default_query.eq(&input.default_query),
                        t::include.eq(include),
                        t::relation_context.eq(relation_context),
                        t::default_missing_data_policy.eq(missing_policy),
                        t::default_limits.eq(default_limits),
                        t::created_at.eq(created),
                        t::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(t::export_templates)
                .values((
                    t::collection_id.eq(collection_id_value),
                    t::name.eq(&input.name),
                    t::description.eq(&input.description),
                    t::content_type.eq(input.content_type.as_mime()),
                    t::template.eq(&input.template),
                    t::kind.eq(input.kind.as_str()),
                    t::scope_kind.eq(scope_kind),
                    t::class_id.eq(class_id_value),
                    t::default_query.eq(&input.default_query),
                    t::include.eq(include),
                    t::relation_context.eq(relation_context),
                    t::default_missing_data_policy.eq(missing_policy),
                    t::default_limits.eq(default_limits),
                    t::created_at.eq(created),
                    t::updated_at.eq(updated),
                ))
                .returning(t::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_remote_target_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportRemoteTargetInput,
    collection_id_value: i32,
    class_id_value: Option<i32>,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::remote_targets::dsl as r;
    let existing = r::remote_targets
        .filter(r::collection_id.eq(collection_id_value))
        .filter(r::name.eq(&input.name))
        .select((r::id, r::created_at, r::updated_at, r::revision))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, ResourceRevision)>(conn)
        .await
        .optional()?;
    if let Some((_, _, _, revision)) = existing {
        assert_import_revision(input.condition, revision)?;
    } else {
        assert_import_create_condition(input.condition)?;
    }
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Remote target '{}' already exists in the collection",
            input.name
        )));
    }
    let auth_config = serde_json::to_value(&input.auth_config)?;
    let subject_types = serde_json::to_value(&input.allowed_subject_types)?;
    match existing {
        Some((existing_id, existing_created, existing_updated, _)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing_created, existing_updated));
            with_imported_timestamp_override(conn, async |conn| {
                diesel::update(r::remote_targets.filter(r::id.eq(existing_id)))
                    .set((
                        r::class_id.eq(class_id_value),
                        r::description.eq(&input.description),
                        r::method.eq(input.method.as_str()),
                        r::url_template.eq(&input.url_template),
                        r::headers_template.eq(&input.headers_template),
                        r::body_template.eq(&input.body_template),
                        r::auth_config.eq(auth_config),
                        r::allowed_subject_types.eq(subject_types),
                        r::timeout_ms.eq(input.timeout_ms),
                        r::enabled.eq(input.enabled),
                        r::created_at.eq(created),
                        r::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(r::remote_targets)
                .values((
                    r::collection_id.eq(collection_id_value),
                    r::class_id.eq(class_id_value),
                    r::name.eq(&input.name),
                    r::description.eq(&input.description),
                    r::method.eq(input.method.as_str()),
                    r::url_template.eq(&input.url_template),
                    r::headers_template.eq(&input.headers_template),
                    r::body_template.eq(&input.body_template),
                    r::auth_config.eq(auth_config),
                    r::allowed_subject_types.eq(subject_types),
                    r::timeout_ms.eq(input.timeout_ms),
                    r::enabled.eq(input.enabled),
                    r::created_at.eq(created),
                    r::updated_at.eq(updated),
                ))
                .returning(r::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_event_sink_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportEventSinkInput,
    overwrite: bool,
) -> Result<i32, ApiError> {
    use crate::schema::event_sinks::dsl as s;
    let existing = s::event_sinks
        .filter(s::name.eq(&input.name))
        .select((s::id, s::created_at, s::updated_at, s::revision))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, ResourceRevision)>(conn)
        .await
        .optional()?;
    if let Some((_, _, _, revision)) = existing {
        assert_import_revision(input.condition, revision)?;
    } else {
        assert_import_create_condition(input.condition)?;
    }
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Event sink '{}' already exists",
            input.name
        )));
    }
    match existing {
        Some((existing_id, existing_created, existing_updated, _)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing_created, existing_updated));
            with_imported_timestamp_override(conn, async |conn| {
                diesel::update(s::event_sinks.filter(s::id.eq(existing_id)))
                    .set((
                        s::kind.eq(input.kind.as_str()),
                        s::config.eq(&input.config),
                        s::secret_ref.eq(&input.secret_ref),
                        s::enabled.eq(input.enabled),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(s::event_sinks)
                .values((
                    s::name.eq(&input.name),
                    s::kind.eq(input.kind.as_str()),
                    s::config.eq(&input.config),
                    s::secret_ref.eq(&input.secret_ref),
                    s::enabled.eq(input.enabled),
                    s::created_at.eq(created),
                    s::updated_at.eq(updated),
                ))
                .returning(s::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn upsert_event_subscription_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    input: &ImportEventSubscriptionInput,
    collection_id_value: i32,
    sink_id_value: i32,
    overwrite: bool,
) -> Result<i32, ApiError> {
    let filter =
        serde_json::from_value::<hubuum_events_core::EventSubscriptionFilter>(input.filter.clone())
            .map_err(|error| {
                ApiError::BadRequest(format!("Invalid event subscription filter: {error}"))
            })?;
    validate_subscription_parts(&input.entity_types, &input.actions, &filter, &input.routing)?;

    use crate::schema::event_subscriptions::dsl as s;
    let existing = s::event_subscriptions
        .filter(s::collection_id.eq(collection_id_value))
        .filter(s::name.eq(&input.name))
        .select((s::id, s::created_at, s::updated_at, s::revision))
        .for_update()
        .first::<(i32, NaiveDateTime, NaiveDateTime, ResourceRevision)>(conn)
        .await
        .optional()?;
    if let Some((_, _, _, revision)) = existing {
        assert_import_revision(input.condition, revision)?;
    } else {
        assert_import_create_condition(input.condition)?;
    }
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Event subscription '{}' already exists in the collection",
            input.name
        )));
    }
    let entity_types = serde_json::to_value(&input.entity_types)?;
    let actions = serde_json::to_value(&input.actions)?;
    match existing {
        Some((existing_id, existing_created, existing_updated, _)) => {
            let (created, updated) = input
                .timestamps
                .as_ref()
                .map(RestoreTimestamps::as_pair)
                .unwrap_or((existing_created, existing_updated));
            with_imported_timestamp_override(conn, async |conn| {
                diesel::update(s::event_subscriptions.filter(s::id.eq(existing_id)))
                    .set((
                        s::sink_id.eq(sink_id_value),
                        s::description.eq(&input.description),
                        s::entity_types.eq(entity_types),
                        s::actions.eq(actions),
                        s::filter.eq(&input.filter),
                        s::routing.eq(&input.routing),
                        s::enabled.eq(input.enabled),
                        s::created_at.eq(created),
                        s::updated_at.eq(updated),
                    ))
                    .execute(conn)
                    .await?;
                Ok(())
            })
            .await?;
            Ok(existing_id)
        }
        None => {
            let (created, updated) = imported_timestamps(input.timestamps.as_ref());
            Ok(diesel::insert_into(s::event_subscriptions)
                .values((
                    s::collection_id.eq(collection_id_value),
                    s::sink_id.eq(sink_id_value),
                    s::name.eq(&input.name),
                    s::description.eq(&input.description),
                    s::entity_types.eq(entity_types),
                    s::actions.eq(actions),
                    s::filter.eq(&input.filter),
                    s::routing.eq(&input.routing),
                    s::enabled.eq(input.enabled),
                    s::created_at.eq(created),
                    s::updated_at.eq(updated),
                ))
                .returning(s::id)
                .get_result::<i32>(conn)
                .await?)
        }
    }
}

pub async fn create_class_relation_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    new_relation: NewHubuumClassRelation,
    timestamps: Option<&RestoreTimestamps>,
    condition: Option<ImportWriteCondition>,
) -> Result<HubuumClassRelation, ApiError> {
    assert_import_create_condition(condition)?;
    use crate::schema::hubuumclass_relation::dsl::{created_at, hubuumclass_relation, updated_at};
    let new_relation = new_relation.normalized()?;

    let row = match timestamps {
        Some(timestamps) => diesel::insert_into(hubuumclass_relation)
            .values((
                NewHubuumClassRelationRow::from(&new_relation),
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumClassRelationRow>(conn)
            .await
            .map_err(ApiError::from),
        None => diesel::insert_into(hubuumclass_relation)
            .values(NewHubuumClassRelationRow::from(&new_relation))
            .get_result::<HubuumClassRelationRow>(conn)
            .await
            .map_err(ApiError::from),
    }?;
    row.try_into()
}

pub async fn update_class_relation_timestamps_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    left: i32,
    right: i32,
    timestamps: &RestoreTimestamps,
    condition: Option<ImportWriteCondition>,
) -> Result<HubuumClassRelation, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        created_at, from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id, updated_at,
    };
    let pair = normalize_pair(left, right);
    check_class_relation_import_condition_db(conn, pair.0, pair.1, condition).await?;

    let row = with_imported_timestamp_override(conn, async |conn| {
        crate::storage::postgres::updated_or_current(
            diesel::update(
                hubuumclass_relation
                    .filter(from_hubuum_class_id.eq(pair.0))
                    .filter(to_hubuum_class_id.eq(pair.1)),
            )
            .set((
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumClassRelationRow>(conn)
            .await
            .optional(),
            async || {
                hubuumclass_relation
                    .filter(from_hubuum_class_id.eq(pair.0))
                    .filter(to_hubuum_class_id.eq(pair.1))
                    .first::<HubuumClassRelationRow>(conn)
                    .await
            },
        )
        .await
        .map_err(ApiError::from)
    })
    .await?;
    row.try_into()
}

pub async fn check_class_relation_import_condition_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    left: i32,
    right: i32,
    condition: Option<ImportWriteCondition>,
) -> Result<(), ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, revision, to_hubuum_class_id,
    };
    let pair = normalize_pair(left, right);
    let current_revision = hubuumclass_relation
        .filter(from_hubuum_class_id.eq(pair.0))
        .filter(to_hubuum_class_id.eq(pair.1))
        .select(revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let current_revision = require_existing_import_target(current_revision, condition)?;
    assert_import_revision(condition, current_revision)
}

pub async fn create_object_relation_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
    timestamps: Option<&RestoreTimestamps>,
    condition: Option<ImportWriteCondition>,
) -> Result<HubuumObjectRelation, ApiError> {
    assert_import_create_condition(condition)?;
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };
    use crate::schema::hubuumobject_relation::dsl::{
        created_at, hubuumobject_relation, updated_at,
    };
    let class_pair = normalize_pair(from_object.hubuum_class_id, to_object.hubuum_class_id);
    let relation: HubuumClassRelation = hubuumclass_relation
        .filter(from_hubuum_class_id.eq(class_pair.0))
        .filter(to_hubuum_class_id.eq(class_pair.1))
        .first::<HubuumClassRelationRow>(conn)
        .await?
        .try_into()?;

    let object_pair = normalize_pair(from_object.id, to_object.id);
    let new_relation = NewHubuumObjectRelation {
        from_hubuum_object_id: object_pair.0,
        to_hubuum_object_id: object_pair.1,
        class_relation_id: relation.id,
    };

    let row = match timestamps {
        Some(timestamps) => diesel::insert_into(hubuumobject_relation)
            .values((
                NewHubuumObjectRelationRow::from(&new_relation),
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumObjectRelationRow>(conn)
            .await
            .map_err(ApiError::from),
        None => diesel::insert_into(hubuumobject_relation)
            .values(NewHubuumObjectRelationRow::from(&new_relation))
            .get_result::<HubuumObjectRelationRow>(conn)
            .await
            .map_err(ApiError::from),
    }?;
    Ok(row.into())
}

pub async fn update_object_relation_timestamps_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
    timestamps: &RestoreTimestamps,
    condition: Option<ImportWriteCondition>,
) -> Result<HubuumObjectRelation, ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        created_at, from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id, updated_at,
    };
    let pair = normalize_pair(from_object.id, to_object.id);
    check_object_relation_import_condition_db(conn, from_object, to_object, condition).await?;

    let row = with_imported_timestamp_override(conn, async |conn| {
        crate::storage::postgres::updated_or_current(
            diesel::update(
                hubuumobject_relation
                    .filter(from_hubuum_object_id.eq(pair.0))
                    .filter(to_hubuum_object_id.eq(pair.1)),
            )
            .set((
                created_at.eq(timestamps.created_at()),
                updated_at.eq(timestamps.updated_at()),
            ))
            .get_result::<HubuumObjectRelationRow>(conn)
            .await
            .optional(),
            async || {
                hubuumobject_relation
                    .filter(from_hubuum_object_id.eq(pair.0))
                    .filter(to_hubuum_object_id.eq(pair.1))
                    .first::<HubuumObjectRelationRow>(conn)
                    .await
            },
        )
        .await
        .map_err(ApiError::from)
    })
    .await?;
    Ok(row.into())
}

pub async fn check_object_relation_import_condition_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
    condition: Option<ImportWriteCondition>,
) -> Result<(), ApiError> {
    use crate::schema::hubuumobject_relation::dsl::{
        from_hubuum_object_id, hubuumobject_relation, revision, to_hubuum_object_id,
    };
    let pair = normalize_pair(from_object.id, to_object.id);
    let current_revision = hubuumobject_relation
        .filter(from_hubuum_object_id.eq(pair.0))
        .filter(to_hubuum_object_id.eq(pair.1))
        .select(revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let current_revision = require_existing_import_target(current_revision, condition)?;
    assert_import_revision(condition, current_revision)
}

pub async fn apply_permissions_db(
    conn: &mut crate::storage::postgres::PostgresConnection,
    collection_id_value: i32,
    group_id_value: i32,
    permissions: &[Permissions],
    replace_existing: bool,
    condition: Option<ImportWriteCondition>,
    overwrite: bool,
) -> Result<Permission, ApiError> {
    use crate::schema::permissions::dsl::{
        collection_id, group_id, permissions as permissions_table,
    };

    let authorization_revision = crate::schema::collection_authorization_state::table
        .find(collection_id_value)
        .select(crate::schema::collection_authorization_state::revision)
        .for_update()
        .first::<ResourceRevision>(conn)
        .await
        .optional()?;
    let authorization_revision = require_existing_import_target(authorization_revision, condition)?;
    assert_import_revision(condition, authorization_revision)?;

    let existing = permissions_table
        .filter(collection_id.eq(collection_id_value))
        .filter(group_id.eq(group_id_value))
        .first::<Permission>(conn)
        .await
        .optional()?;
    if existing.is_some() && !overwrite {
        return Err(ApiError::Conflict(format!(
            "Permissions for group {group_id_value} already exist on collection {collection_id_value}"
        )));
    }

    let permission_list = PermissionsList::new(permissions.to_vec());
    match existing {
        Some(existing) => {
            let mut update = if replace_existing {
                UpdatePermission {
                    has_read_collection: Some(false),
                    has_update_collection: Some(false),
                    has_delete_collection: Some(false),
                    has_delegate_collection: Some(false),
                    has_create_class: Some(false),
                    has_read_class: Some(false),
                    has_update_class: Some(false),
                    has_delete_class: Some(false),
                    has_create_object: Some(false),
                    has_read_object: Some(false),
                    has_update_object: Some(false),
                    has_delete_object: Some(false),
                    has_create_class_relation: Some(false),
                    has_read_class_relation: Some(false),
                    has_update_class_relation: Some(false),
                    has_delete_class_relation: Some(false),
                    has_create_object_relation: Some(false),
                    has_read_object_relation: Some(false),
                    has_update_object_relation: Some(false),
                    has_delete_object_relation: Some(false),
                    has_read_template: Some(false),
                    has_create_template: Some(false),
                    has_update_template: Some(false),
                    has_delete_template: Some(false),
                    has_read_remote_target: Some(false),
                    has_create_remote_target: Some(false),
                    has_update_remote_target: Some(false),
                    has_delete_remote_target: Some(false),
                    has_execute_remote_target: Some(false),
                    has_read_audit: Some(false),
                    has_manage_event_subscription: Some(false),
                }
            } else {
                UpdatePermission::default()
            };
            apply_permission_list_to_update(&mut update, permissions);

            crate::storage::postgres::updated_or_current(
                diesel::update(
                    permissions_table
                        .filter(collection_id.eq(collection_id_value))
                        .filter(group_id.eq(group_id_value)),
                )
                .set(&update)
                .get_result::<Permission>(conn)
                .await
                .optional(),
                async move || Ok(existing),
            )
            .await
            .map_err(ApiError::from)
        }
        None => {
            let new_entry = NewPermission {
                collection_id: collection_id_value,
                group_id: group_id_value,
                has_read_collection: permission_list.contains(&Permissions::ReadCollection),
                has_update_collection: permission_list.contains(&Permissions::UpdateCollection),
                has_delete_collection: permission_list.contains(&Permissions::DeleteCollection),
                has_delegate_collection: permission_list.contains(&Permissions::DelegateCollection),
                has_create_class: permission_list.contains(&Permissions::CreateClass),
                has_read_class: permission_list.contains(&Permissions::ReadClass),
                has_update_class: permission_list.contains(&Permissions::UpdateClass),
                has_delete_class: permission_list.contains(&Permissions::DeleteClass),
                has_create_object: permission_list.contains(&Permissions::CreateObject),
                has_read_object: permission_list.contains(&Permissions::ReadObject),
                has_update_object: permission_list.contains(&Permissions::UpdateObject),
                has_delete_object: permission_list.contains(&Permissions::DeleteObject),
                has_create_class_relation: permission_list
                    .contains(&Permissions::CreateClassRelation),
                has_read_class_relation: permission_list.contains(&Permissions::ReadClassRelation),
                has_update_class_relation: permission_list
                    .contains(&Permissions::UpdateClassRelation),
                has_delete_class_relation: permission_list
                    .contains(&Permissions::DeleteClassRelation),
                has_create_object_relation: permission_list
                    .contains(&Permissions::CreateObjectRelation),
                has_read_object_relation: permission_list
                    .contains(&Permissions::ReadObjectRelation),
                has_update_object_relation: permission_list
                    .contains(&Permissions::UpdateObjectRelation),
                has_delete_object_relation: permission_list
                    .contains(&Permissions::DeleteObjectRelation),
                has_read_template: permission_list.contains(&Permissions::ReadTemplate),
                has_create_template: permission_list.contains(&Permissions::CreateTemplate),
                has_update_template: permission_list.contains(&Permissions::UpdateTemplate),
                has_delete_template: permission_list.contains(&Permissions::DeleteTemplate),
                has_read_remote_target: permission_list.contains(&Permissions::ReadRemoteTarget),
                has_create_remote_target: permission_list
                    .contains(&Permissions::CreateRemoteTarget),
                has_update_remote_target: permission_list
                    .contains(&Permissions::UpdateRemoteTarget),
                has_delete_remote_target: permission_list
                    .contains(&Permissions::DeleteRemoteTarget),
                has_execute_remote_target: permission_list
                    .contains(&Permissions::ExecuteRemoteTarget),
                has_read_audit: permission_list.contains(&Permissions::ReadAudit),
                has_manage_event_subscription: permission_list
                    .contains(&Permissions::ManageEventSubscription),
            };

            diesel::insert_into(permissions_table)
                .values(&new_entry)
                .get_result::<Permission>(conn)
                .await
                .map_err(ApiError::from)
        }
    }
}

fn normalize_pair(left: i32, right: i32) -> (i32, i32) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn apply_permission_list_to_update(update: &mut UpdatePermission, permissions: &[Permissions]) {
    for permission in permissions {
        match permission {
            Permissions::ReadCollection => update.has_read_collection = Some(true),
            Permissions::UpdateCollection => update.has_update_collection = Some(true),
            Permissions::DeleteCollection => update.has_delete_collection = Some(true),
            Permissions::DelegateCollection => update.has_delegate_collection = Some(true),
            Permissions::CreateClass => update.has_create_class = Some(true),
            Permissions::ReadClass => update.has_read_class = Some(true),
            Permissions::UpdateClass => update.has_update_class = Some(true),
            Permissions::DeleteClass => update.has_delete_class = Some(true),
            Permissions::CreateObject => update.has_create_object = Some(true),
            Permissions::ReadObject => update.has_read_object = Some(true),
            Permissions::UpdateObject => update.has_update_object = Some(true),
            Permissions::DeleteObject => update.has_delete_object = Some(true),
            Permissions::CreateClassRelation => update.has_create_class_relation = Some(true),
            Permissions::ReadClassRelation => update.has_read_class_relation = Some(true),
            Permissions::UpdateClassRelation => update.has_update_class_relation = Some(true),
            Permissions::DeleteClassRelation => update.has_delete_class_relation = Some(true),
            Permissions::CreateObjectRelation => update.has_create_object_relation = Some(true),
            Permissions::ReadObjectRelation => update.has_read_object_relation = Some(true),
            Permissions::UpdateObjectRelation => update.has_update_object_relation = Some(true),
            Permissions::DeleteObjectRelation => update.has_delete_object_relation = Some(true),
            Permissions::ReadTemplate => update.has_read_template = Some(true),
            Permissions::CreateTemplate => update.has_create_template = Some(true),
            Permissions::UpdateTemplate => update.has_update_template = Some(true),
            Permissions::DeleteTemplate => update.has_delete_template = Some(true),
            Permissions::ReadRemoteTarget => update.has_read_remote_target = Some(true),
            Permissions::CreateRemoteTarget => update.has_create_remote_target = Some(true),
            Permissions::UpdateRemoteTarget => update.has_update_remote_target = Some(true),
            Permissions::DeleteRemoteTarget => update.has_delete_remote_target = Some(true),
            Permissions::ExecuteRemoteTarget => update.has_execute_remote_target = Some(true),
            Permissions::ReadAudit => update.has_read_audit = Some(true),
            Permissions::ManageEventSubscription => {
                update.has_manage_event_subscription = Some(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{GroupKey, LDAP_PROVIDER_KIND, NewGroup};
    use crate::storage::postgres::operations::identity::ensure_identity_scope;
    use crate::tests::TestScope;

    #[test]
    fn conditional_import_reports_a_missing_execution_target_as_stale() {
        let error = require_existing_import_target::<()>(
            None,
            Some(ImportWriteCondition::IfRevision {
                expected_revision: ResourceRevision::INITIAL,
            }),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ApiError::PreconditionFailed(message, _)
                if message == CONDITIONAL_IMPORT_TARGET_MISSING
        ));
    }

    #[actix_rt::test]
    async fn group_lookup_disambiguates_identity_scopes() {
        let scope = TestScope::new();
        let groupname = scope.scoped_name("shared_groupname");
        let local_group = NewGroup {
            identity_scope: None,
            groupname: groupname.clone(),
            description: Some("Local group".to_string()),
        }
        .save_without_events(&scope.pool)
        .await
        .unwrap();

        let external_scope_name = scope.scoped_name("directory");
        let external_scope =
            ensure_identity_scope(&scope.pool, &external_scope_name, LDAP_PROVIDER_KIND)
                .await
                .unwrap();
        let external_group = with_connection(&scope.pool, async |conn| {
            use crate::schema::groups;

            diesel::insert_into(groups::table)
                .values((
                    groups::identity_scope_id.eq(external_scope.id),
                    groups::groupname.eq(&groupname),
                    groups::description.eq("Directory group"),
                    groups::managed_by.eq(LDAP_PROVIDER_KIND),
                    groups::external_key.eq(scope.scoped_name("external_group_key")),
                ))
                .get_result::<Group>(conn)
                .await
        })
        .await
        .unwrap();

        let external_key = GroupKey {
            identity_scope: Some(external_scope_name),
            groupname: groupname.clone(),
        };
        let loaded_external = lookup_group_by_name(
            scope.pool.get_ref(),
            external_key.identity_scope_name(),
            &external_key.groupname,
        )
        .await
        .unwrap()
        .unwrap();
        let local_key = GroupKey {
            identity_scope: None,
            groupname,
        };
        let loaded_local = with_connection(&scope.pool, async |conn| {
            lookup_group_by_name_db(conn, local_key.identity_scope_name(), &local_key.groupname)
                .await
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(loaded_external.id, external_group.id);
        assert_eq!(loaded_local.id, local_group.id);
    }
}
