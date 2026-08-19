//! PostgreSQL implementation of backend-neutral import lookup and result I/O.

use diesel::Insertable;
use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::{
    StorageClassRecord, StorageCollection, StorageImportCollectionKey, StorageImportResult,
    StorageObject,
};
use serde_json::Value;

use crate::operations::class::ClassRow;
use crate::operations::collection::CollectionRow;
use crate::operations::object::ObjectRow;
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

pub use super::import_execution::{
    apply_import_best_effort, apply_import_strict, preflight_import,
};

#[derive(Insertable)]
#[diesel(table_name = crate::schema::import_task_results)]
struct NewImportResultRow {
    task_id: i32,
    item_ref: Option<String>,
    entity_kind: String,
    action: String,
    identifier: Option<String>,
    outcome: String,
    error: Option<String>,
    details: Option<Value>,
}

impl From<StorageImportResult> for NewImportResultRow {
    fn from(result: StorageImportResult) -> Self {
        let (task_id, item_ref, entity_kind, action, identifier, outcome, error, details) =
            result.into_parts();
        Self {
            task_id: task_id.id(),
            item_ref,
            entity_kind,
            action,
            identifier,
            outcome,
            error,
            details,
        }
    }
}

pub async fn root_collection(
    runtime: &PostgresRuntime,
) -> Result<StorageCollection, PostgresStorageError> {
    runtime
        .with_connection(async |connection| root_collection_on_connection(connection).await)
        .await
}

pub async fn collection_by_id(
    runtime: &PostgresRuntime,
    collection_id: i32,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    runtime
        .with_connection(async move |connection| {
            crate::schema::collections::table
                .filter(crate::schema::collections::id.eq(collection_id))
                .first::<CollectionRow>(connection)
                .await
                .optional()
        })
        .await?
        .map(CollectionRow::into_storage)
        .transpose()
}

pub async fn collection_by_key(
    runtime: &PostgresRuntime,
    key: &StorageImportCollectionKey,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    let key = key.clone();
    runtime
        .with_connection(async move |connection| {
            collection_by_key_on_connection(connection, key).await
        })
        .await
}

pub async fn collections_by_name(
    runtime: &PostgresRuntime,
    name: &str,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    validate_name(name, "collection name")?;
    let name = name.to_string();
    runtime
        .with_connection(async move |connection| {
            collections_by_name_on_connection(connection, &name).await
        })
        .await
}

pub async fn collection_child_by_name(
    runtime: &PostgresRuntime,
    parent_collection_id: i32,
    name: &str,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    validate_positive_id(parent_collection_id, "parent collection id")?;
    validate_name(name, "collection name")?;
    let name = name.to_string();
    runtime
        .with_connection(async move |connection| {
            collection_child_by_name_on_connection(connection, parent_collection_id, &name).await
        })
        .await
}

pub async fn class_by_name(
    runtime: &PostgresRuntime,
    collection_id: i32,
    name: &str,
) -> Result<Option<StorageClassRecord>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    validate_name(name, "class name")?;
    let name = name.to_string();
    runtime
        .with_connection(async move |connection| {
            class_by_name_on_connection(connection, collection_id, &name).await
        })
        .await
}

pub async fn classes_by_names(
    runtime: &PostgresRuntime,
    collection_id: i32,
    names: &[String],
) -> Result<Vec<StorageClassRecord>, PostgresStorageError> {
    validate_positive_id(collection_id, "collection id")?;
    validate_names(names, "class name")?;
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let names = names.to_vec();
    let rows = runtime
        .with_connection(async move |connection| {
            crate::schema::hubuumclass::table
                .filter(crate::schema::hubuumclass::collection_id.eq(collection_id))
                .filter(crate::schema::hubuumclass::name.eq_any(names))
                .order(crate::schema::hubuumclass::id.asc())
                .load::<ClassRow>(connection)
                .await
        })
        .await?;
    rows.into_iter().map(ClassRow::into_storage).collect()
}

pub async fn object_by_name(
    runtime: &PostgresRuntime,
    class_id: i32,
    name: &str,
) -> Result<Option<StorageObject>, PostgresStorageError> {
    validate_positive_id(class_id, "class id")?;
    validate_name(name, "object name")?;
    let name = name.to_string();
    runtime
        .with_connection(async move |connection| {
            object_by_name_on_connection(connection, class_id, &name).await
        })
        .await
}

pub async fn objects_by_names(
    runtime: &PostgresRuntime,
    class_id: i32,
    names: &[String],
) -> Result<Vec<StorageObject>, PostgresStorageError> {
    validate_positive_id(class_id, "class id")?;
    validate_names(names, "object name")?;
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let names = names.to_vec();
    runtime
        .with_connection(async move |connection| {
            crate::schema::hubuumobject::table
                .filter(crate::schema::hubuumobject::hubuum_class_id.eq(class_id))
                .filter(crate::schema::hubuumobject::name.eq_any(names))
                .order(crate::schema::hubuumobject::id.asc())
                .load::<ObjectRow>(connection)
                .await
        })
        .await
        .and_then(|rows| rows.into_iter().map(ObjectRow::into_storage).collect())
}

pub async fn class_relation_exists(
    runtime: &PostgresRuntime,
    left_class_id: i32,
    right_class_id: i32,
) -> Result<bool, PostgresStorageError> {
    let (from_class_id, to_class_id) = normalized_pair(left_class_id, right_class_id, "class")?;
    runtime
        .with_connection(async move |connection| {
            diesel::select(diesel::dsl::exists(
                crate::schema::hubuumclass_relation::table
                    .filter(
                        crate::schema::hubuumclass_relation::from_hubuum_class_id.eq(from_class_id),
                    )
                    .filter(
                        crate::schema::hubuumclass_relation::to_hubuum_class_id.eq(to_class_id),
                    ),
            ))
            .get_result::<bool>(connection)
            .await
        })
        .await
}

pub async fn object_relation_exists(
    runtime: &PostgresRuntime,
    left_object_id: i32,
    right_object_id: i32,
) -> Result<bool, PostgresStorageError> {
    let (from_object_id, to_object_id) =
        normalized_pair(left_object_id, right_object_id, "object")?;
    runtime
        .with_connection(async move |connection| {
            diesel::select(diesel::dsl::exists(
                crate::schema::hubuumobject_relation::table
                    .filter(
                        crate::schema::hubuumobject_relation::from_hubuum_object_id
                            .eq(from_object_id),
                    )
                    .filter(
                        crate::schema::hubuumobject_relation::to_hubuum_object_id.eq(to_object_id),
                    ),
            ))
            .get_result::<bool>(connection)
            .await
        })
        .await
}

pub async fn group_exists(
    runtime: &PostgresRuntime,
    identity_scope: &str,
    group_name: &str,
) -> Result<bool, PostgresStorageError> {
    validate_name(identity_scope, "identity scope")?;
    validate_name(group_name, "group name")?;
    let identity_scope = identity_scope.to_string();
    let group_name = group_name.to_string();
    runtime
        .with_connection(async move |connection| {
            diesel::select(diesel::dsl::exists(
                crate::schema::groups::table
                    .inner_join(crate::schema::identity_scopes::table)
                    .filter(crate::schema::groups::groupname.eq(group_name))
                    .filter(crate::schema::identity_scopes::name.eq(identity_scope)),
            ))
            .get_result::<bool>(connection)
            .await
        })
        .await
}

pub async fn record_results(
    runtime: &PostgresRuntime,
    results: Vec<StorageImportResult>,
) -> Result<(), PostgresStorageError> {
    if results.is_empty() {
        return Ok(());
    }
    let rows = results
        .into_iter()
        .map(NewImportResultRow::from)
        .collect::<Vec<_>>();
    runtime
        .with_connection(async move |connection| {
            diesel::insert_into(crate::schema::import_task_results::table)
                .values(rows)
                .execute(connection)
                .await
        })
        .await?;
    Ok(())
}

pub(crate) async fn root_collection_on_connection(
    connection: &mut PostgresConnection,
) -> Result<StorageCollection, PostgresStorageError> {
    crate::schema::collections::table
        .filter(crate::schema::collections::parent_collection_id.is_null())
        .first::<CollectionRow>(connection)
        .await?
        .into_storage()
}

pub(crate) async fn collection_by_key_on_connection(
    connection: &mut PostgresConnection,
    key: StorageImportCollectionKey,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    let parts = key.into_parts();
    validate_collection_key_parts(&parts.name, parts.path.as_deref())?;
    let Some(path) = parts.path else {
        return unique_collection_by_name_on_connection(connection, &parts.name).await;
    };
    if path.is_empty() {
        return root_collection_on_connection(connection).await.map(Some);
    }
    let mut parent = root_collection_on_connection(connection).await?;
    for segment in path {
        let Some(child) =
            collection_child_by_name_on_connection(connection, parent.id().id(), &segment).await?
        else {
            return Ok(None);
        };
        parent = child;
    }
    Ok(Some(parent))
}

pub(crate) async fn class_by_name_on_connection(
    connection: &mut PostgresConnection,
    collection_id: i32,
    name: &str,
) -> Result<Option<StorageClassRecord>, PostgresStorageError> {
    crate::schema::hubuumclass::table
        .filter(crate::schema::hubuumclass::collection_id.eq(collection_id))
        .filter(crate::schema::hubuumclass::name.eq(name))
        .first::<ClassRow>(connection)
        .await
        .optional()
        .map_err(PostgresStorageError::from)?
        .map(ClassRow::into_storage)
        .transpose()
}

pub(crate) async fn object_by_name_on_connection(
    connection: &mut PostgresConnection,
    class_id: i32,
    name: &str,
) -> Result<Option<StorageObject>, PostgresStorageError> {
    crate::schema::hubuumobject::table
        .filter(crate::schema::hubuumobject::hubuum_class_id.eq(class_id))
        .filter(crate::schema::hubuumobject::name.eq(name))
        .first::<ObjectRow>(connection)
        .await
        .optional()
        .map_err(PostgresStorageError::from)?
        .map(ObjectRow::into_storage)
        .transpose()
}

async fn unique_collection_by_name_on_connection(
    connection: &mut PostgresConnection,
    name: &str,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    let matches = collections_by_name_on_connection(connection, name).await?;
    match matches.as_slice() {
        [] => Ok(None),
        [collection] => Ok(Some(collection.clone())),
        _ => Err(PostgresStorageError::bad_request(format!(
            "Collection name '{name}' is ambiguous; use collection_key.path"
        ))),
    }
}

async fn collections_by_name_on_connection(
    connection: &mut PostgresConnection,
    name: &str,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    crate::schema::collections::table
        .filter(crate::schema::collections::name.eq(name))
        .order(crate::schema::collections::id.asc())
        .load::<CollectionRow>(connection)
        .await?
        .into_iter()
        .map(CollectionRow::into_storage)
        .collect()
}

async fn collection_child_by_name_on_connection(
    connection: &mut PostgresConnection,
    parent_collection_id: i32,
    name: &str,
) -> Result<Option<StorageCollection>, PostgresStorageError> {
    crate::schema::collections::table
        .filter(crate::schema::collections::parent_collection_id.eq(parent_collection_id))
        .filter(crate::schema::collections::name.eq(name))
        .first::<CollectionRow>(connection)
        .await
        .optional()?
        .map(CollectionRow::into_storage)
        .transpose()
}

fn normalized_pair(left: i32, right: i32, label: &str) -> Result<(i32, i32), PostgresStorageError> {
    validate_positive_id(left, &format!("left {label} id"))?;
    validate_positive_id(right, &format!("right {label} id"))?;
    if left == right {
        return Err(PostgresStorageError::bad_request(format!(
            "{label} relation endpoints must be distinct"
        )));
    }
    Ok(if left < right {
        (left, right)
    } else {
        (right, left)
    })
}

fn validate_positive_id(value: i32, label: &str) -> Result<(), PostgresStorageError> {
    if value <= 0 {
        Err(PostgresStorageError::bad_request(format!(
            "{label} must be greater than zero"
        )))
    } else {
        Ok(())
    }
}

fn validate_name(value: &str, label: &str) -> Result<(), PostgresStorageError> {
    if value.trim().is_empty() {
        Err(PostgresStorageError::bad_request(format!(
            "{label} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn validate_names(values: &[String], label: &str) -> Result<(), PostgresStorageError> {
    for value in values {
        validate_name(value, label)?;
    }
    Ok(())
}

fn validate_collection_key_parts(
    name: &str,
    path: Option<&[String]>,
) -> Result<(), PostgresStorageError> {
    validate_name(name, "collection name")?;
    let Some(path) = path else {
        return Ok(());
    };
    validate_names(path, "collection path segment")?;
    match path.last() {
        Some(last) if last == name => Ok(()),
        Some(_) => Err(PostgresStorageError::bad_request(format!(
            "collection_key.path must end with collection name '{name}'"
        ))),
        None if name == "root" => Ok(()),
        None => Err(PostgresStorageError::bad_request(
            "collection_key.path may be empty only for the root collection",
        )),
    }
}

#[cfg(test)]
mod tests {
    use hubuum_storage_core::{StorageImportCollectionKey, StorageImportCollectionKeyParts};

    use super::*;

    #[test]
    fn relation_lookups_reject_self_edges() {
        let error = normalized_pair(7, 7, "class").unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }

    #[test]
    fn collection_paths_must_end_with_the_collection_name() {
        let parts = StorageImportCollectionKey::from_parts(StorageImportCollectionKeyParts {
            name: "leaf".to_string(),
            path: Some(vec!["other".to_string()]),
        })
        .into_parts();

        let error = validate_collection_key_parts(&parts.name, parts.path.as_deref()).unwrap_err();

        assert_eq!(
            error.kind(),
            hubuum_storage_core::StorageErrorKind::InvalidInput
        );
    }
}
