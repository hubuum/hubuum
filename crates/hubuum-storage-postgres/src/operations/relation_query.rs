//! PostgreSQL relation listing and endpoint-visibility queries.

use diesel::dsl::not;
use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};
use diesel::{JoinOnDsl, SelectableHelper};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, QueryOptions};
use hubuum_storage_core::{
    AuthorizationPermission, ObjectRelationsTouchingIdsQuery, RelationIdsQuery, RelationListQuery,
    RelationPage, RelationTouchingQuery, StorageClassRelation, StorageObjectRelation,
    StorageVisibility,
};

use crate::cursor::{CursorSqlField, CursorSqlType};
use crate::operations::authorization::apply_permission_filter;
use crate::operations::relation::{ClassRelationRow, ObjectRelationRow};
use crate::{PostgresConnection, PostgresRuntime, PostgresStorageError};

const CLASS_RELATION_PERMISSION: AuthorizationPermission =
    AuthorizationPermission::ReadClassRelation;
const OBJECT_RELATION_PERMISSION: AuthorizationPermission =
    AuthorizationPermission::ReadObjectRelation;

/// List visible class relations with stable cursor paging and an optional count.
pub async fn list_class_relations(
    runtime: &PostgresRuntime,
    query: RelationListQuery,
) -> Result<RelationPage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let (from_name_ids, to_name_ids) =
                class_name_filter_ids(connection, &options, &visibility).await?;
            let build_query = || {
                build_class_relation_query(
                    &options,
                    &visibility,
                    &collection_ids,
                    None,
                    from_name_ids.as_deref(),
                    to_name_ids.as_deref(),
                )
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Class)?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List visible object relations with stable cursor paging and an optional count.
pub async fn list_object_relations(
    runtime: &PostgresRuntime,
    query: RelationListQuery,
) -> Result<RelationPage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (options, visibility) = query.into_parts();
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query =
                || build_object_relation_query(&options, &visibility, &collection_ids, None);
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Object)?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List class relations touching one class.
pub async fn list_class_relations_touching(
    runtime: &PostgresRuntime,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageClassRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (class_id, options, visibility) = query.into_parts();
    validate_positive_id(class_id, "class id")?;
    let permissions = required_permissions(&options, [CLASS_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || {
                build_class_relation_query(
                    &options,
                    &visibility,
                    &collection_ids,
                    Some(class_id),
                    None,
                    None,
                )
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Class)?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?
                .into_iter()
                .map(ClassRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// List object relations touching one object.
pub async fn list_object_relations_touching(
    runtime: &PostgresRuntime,
    query: RelationTouchingQuery,
) -> Result<RelationPage<StorageObjectRelation>, PostgresStorageError> {
    let include_total = query.options().include_total;
    let (object_id, options, visibility) = query.into_parts();
    validate_positive_id(object_id, "object id")?;
    let permissions = required_permissions(&options, [OBJECT_RELATION_PERMISSION])?;
    if !visibility.allows_permissions(&permissions) {
        return Ok(RelationPage::new(Vec::new(), include_total.then_some(0)));
    }

    runtime
        .with_read_only_snapshot(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &permissions).await?;
            let build_query = || {
                build_object_relation_query(&options, &visibility, &collection_ids, Some(object_id))
            };
            let total = if include_total {
                Some(build_query()?.count().get_result::<i64>(connection).await?)
            } else {
                None
            };
            let mut records = build_query()?;
            let fields = relation_cursor_fields(&options, RelationKind::Object)?;
            crate::apply_query_options_with_fields!(records, options, fields);
            let rows = records
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?
                .into_iter()
                .map(ObjectRelationRow::into_storage)
                .collect();
            Ok::<_, PostgresStorageError>(RelationPage::new(rows, total))
        })
        .await
}

/// Return visible class relations touching any supplied class id.
pub async fn class_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Touching).await
}

/// Return visible class relations whose endpoints are both supplied ids.
pub async fn class_relations_between_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    class_relations_for_ids(runtime, query, IdMatch::Between).await
}

/// Return visible object relations whose endpoints are both supplied ids.
pub async fn object_relations_between_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[OBJECT_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let rows = build_object_relation_query(&options, &visibility, &collection_ids, None)?
                .filter(crate::schema::hubuumobject_relation::from_hubuum_object_id.eq_any(&ids))
                .filter(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq_any(&ids))
                .order(crate::schema::hubuumobject_relation::id.asc())
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ObjectRelationRow::into_storage)
                    .collect(),
            )
        })
        .await
}

/// Return a bounded set of visible object relations touching supplied ids.
pub async fn object_relations_touching_ids(
    runtime: &PostgresRuntime,
    query: ObjectRelationsTouchingIdsQuery,
) -> Result<Vec<StorageObjectRelation>, PostgresStorageError> {
    let (ids, excluded_ids, max_results, visibility) = query.into_parts();
    if ids.is_empty()
        || max_results == 0
        || !visibility.allows_permissions(&[OBJECT_RELATION_PERMISSION])
    {
        return Ok(Vec::new());
    }
    let max_results = i64::try_from(max_results).map_err(|_| {
        PostgresStorageError::bad_request("Object-relation result limit is too large")
    })?;
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[OBJECT_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let mut records =
                build_object_relation_query(&options, &visibility, &collection_ids, None)?.filter(
                    crate::schema::hubuumobject_relation::from_hubuum_object_id
                        .eq_any(&ids)
                        .or(crate::schema::hubuumobject_relation::to_hubuum_object_id.eq_any(&ids)),
                );
            if !excluded_ids.is_empty() {
                records = records.filter(not(
                    crate::schema::hubuumobject_relation::id.eq_any(excluded_ids)
                ));
            }
            let rows = records
                .order(crate::schema::hubuumobject_relation::id.asc())
                .limit(max_results)
                .select(ObjectRelationRow::as_select())
                .load::<ObjectRelationRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ObjectRelationRow::into_storage)
                    .collect(),
            )
        })
        .await
}

#[derive(Clone, Copy)]
enum IdMatch {
    Touching,
    Between,
}

async fn class_relations_for_ids(
    runtime: &PostgresRuntime,
    query: RelationIdsQuery,
    mode: IdMatch,
) -> Result<Vec<StorageClassRelation>, PostgresStorageError> {
    let (ids, visibility) = query.into_parts();
    if ids.is_empty() || !visibility.allows_permissions(&[CLASS_RELATION_PERMISSION]) {
        return Ok(Vec::new());
    }
    runtime
        .with_connection(async move |connection| {
            let collection_ids =
                authorized_collection_ids(connection, &visibility, &[CLASS_RELATION_PERMISSION])
                    .await?;
            let options = empty_options();
            let records = build_class_relation_query(
                &options,
                &visibility,
                &collection_ids,
                None,
                None,
                None,
            )?;
            let records = match mode {
                IdMatch::Touching => records.filter(
                    crate::schema::hubuumclass_relation::from_hubuum_class_id
                        .eq_any(&ids)
                        .or(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq_any(&ids)),
                ),
                IdMatch::Between => records
                    .filter(crate::schema::hubuumclass_relation::from_hubuum_class_id.eq_any(&ids))
                    .filter(crate::schema::hubuumclass_relation::to_hubuum_class_id.eq_any(&ids)),
            };
            let rows = records
                .order(crate::schema::hubuumclass_relation::id.asc())
                .select(ClassRelationRow::as_select())
                .load::<ClassRelationRow>(connection)
                .await?;
            Ok::<_, PostgresStorageError>(
                rows.into_iter()
                    .map(ClassRelationRow::into_storage)
                    .collect(),
            )
        })
        .await
}

fn build_class_relation_query<'query>(
    options: &'query QueryOptions,
    visibility: &'query StorageVisibility,
    collection_ids: &'query [i32],
    touching_id: Option<i32>,
    from_name_ids: Option<&'query [i32]>,
    to_name_ids: Option<&'query [i32]>,
) -> Result<
    crate::schema::hubuumclass_relation::BoxedQuery<'query, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::{hubuumclass, hubuumclass_relation};

    let visible_class_ids = || {
        hubuumclass::table
            .select(hubuumclass::id)
            .filter(hubuumclass::collection_id.eq_any(collection_ids))
    };
    let mut records = hubuumclass_relation::table
        .filter(hubuumclass_relation::from_hubuum_class_id.eq_any(visible_class_ids()))
        .filter(hubuumclass_relation::to_hubuum_class_id.eq_any(visible_class_ids()))
        .into_boxed();
    if let Some(touching_id) = touching_id {
        records = records.filter(
            hubuumclass_relation::from_hubuum_class_id
                .eq(touching_id)
                .or(hubuumclass_relation::to_hubuum_class_id.eq(touching_id)),
        );
    }
    if let Some(scope) = visibility.resources() {
        let scoped_class_ids = || {
            hubuumclass::table.select(hubuumclass::id).filter(
                hubuumclass::collection_id
                    .eq_any(scope.collection_ids())
                    .or(hubuumclass::id.eq_any(scope.class_ids())),
            )
        };
        records = records
            .filter(hubuumclass_relation::from_hubuum_class_id.eq_any(scoped_class_ids()))
            .filter(hubuumclass_relation::to_hubuum_class_id.eq_any(scoped_class_ids()));
    }
    if let Some(ids) = from_name_ids {
        records = records.filter(hubuumclass_relation::from_hubuum_class_id.eq_any(ids));
    }
    if let Some(ids) = to_name_ids {
        records = records.filter(hubuumclass_relation::to_hubuum_class_id.eq_any(ids));
    }
    for parameter in &options.filters {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, hubuumclass_relation::id)
            }
            FilterField::ClassFrom => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumclass_relation::from_hubuum_class_id
            ),
            FilterField::ClassTo => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumclass_relation::to_hubuum_class_id
            ),
            FilterField::CreatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumclass_relation::created_at
            ),
            FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumclass_relation::updated_at
            ),
            FilterField::Revision => {
                crate::postgres_revision_filter!(records, parameter, hubuumclass_relation::revision)
            }
            FilterField::Permissions | FilterField::ClassFromName | FilterField::ClassToName => {}
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for class relations",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

fn build_object_relation_query<'query>(
    options: &'query QueryOptions,
    visibility: &'query StorageVisibility,
    collection_ids: &'query [i32],
    touching_id: Option<i32>,
) -> Result<
    crate::schema::hubuumobject_relation::BoxedQuery<'query, diesel::pg::Pg>,
    PostgresStorageError,
> {
    use crate::schema::{hubuumobject, hubuumobject_relation};

    let visible_object_ids = || {
        hubuumobject::table
            .select(hubuumobject::id)
            .filter(hubuumobject::collection_id.eq_any(collection_ids))
    };
    let mut records = hubuumobject_relation::table
        .filter(hubuumobject_relation::from_hubuum_object_id.eq_any(visible_object_ids()))
        .filter(hubuumobject_relation::to_hubuum_object_id.eq_any(visible_object_ids()))
        .into_boxed();
    if let Some(touching_id) = touching_id {
        records = records.filter(
            hubuumobject_relation::from_hubuum_object_id
                .eq(touching_id)
                .or(hubuumobject_relation::to_hubuum_object_id.eq(touching_id)),
        );
    }
    if let Some(scope) = visibility.resources() {
        let scoped_object_ids = || {
            hubuumobject::table.select(hubuumobject::id).filter(
                hubuumobject::collection_id
                    .eq_any(scope.collection_ids())
                    .or(hubuumobject::hubuum_class_id.eq_any(scope.class_ids()))
                    .or(hubuumobject::id.eq_any(scope.object_ids())),
            )
        };
        records = records
            .filter(hubuumobject_relation::from_hubuum_object_id.eq_any(scoped_object_ids()))
            .filter(hubuumobject_relation::to_hubuum_object_id.eq_any(scoped_object_ids()));
    }
    for parameter in &options.filters {
        match parameter.field {
            FilterField::Id => {
                crate::postgres_integer_filter!(records, parameter, hubuumobject_relation::id)
            }
            FilterField::ClassRelation => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::class_relation_id
            ),
            FilterField::ObjectFrom => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::from_hubuum_object_id
            ),
            FilterField::ObjectTo => crate::postgres_integer_filter!(
                records,
                parameter,
                hubuumobject_relation::to_hubuum_object_id
            ),
            FilterField::CreatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumobject_relation::created_at
            ),
            FilterField::UpdatedAt => crate::postgres_datetime_filter!(
                records,
                parameter,
                hubuumobject_relation::updated_at
            ),
            FilterField::Revision => crate::postgres_revision_filter!(
                records,
                parameter,
                hubuumobject_relation::revision
            ),
            FilterField::Permissions => {}
            _ => {
                return Err(PostgresStorageError::bad_request(format!(
                    "Field '{}' isn't searchable (or does not exist) for object relations",
                    parameter.field
                )));
            }
        }
    }
    Ok(records)
}

async fn authorized_collection_ids(
    connection: &mut PostgresConnection,
    visibility: &StorageVisibility,
    permissions: &[AuthorizationPermission],
) -> Result<Vec<i32>, PostgresStorageError> {
    use crate::schema::{
        collection_closure, collections, group_memberships, permissions as grants,
    };

    if visibility.is_admin() {
        return collections::table
            .select(collections::id)
            .load(connection)
            .await
            .map_err(PostgresStorageError::from);
    }
    let group_ids = group_memberships::table
        .filter(group_memberships::principal_id.eq(visibility.principal_id()))
        .select(group_memberships::group_id);
    let mut records = grants::table
        .filter(grants::group_id.eq_any(group_ids))
        .into_boxed();
    for permission in permissions.iter().copied() {
        apply_permission_filter!(records, permission, true);
    }
    records
        .inner_join(
            collection_closure::table
                .on(grants::collection_id.eq(collection_closure::ancestor_collection_id)),
        )
        .select(collection_closure::descendant_collection_id)
        .distinct()
        .load(connection)
        .await
        .map_err(PostgresStorageError::from)
}

async fn class_name_filter_ids(
    connection: &mut PostgresConnection,
    options: &QueryOptions,
    visibility: &StorageVisibility,
) -> Result<(Option<Vec<i32>>, Option<Vec<i32>>), PostgresStorageError> {
    let from = options
        .filters
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassFromName);
    let to = options
        .filters
        .iter()
        .find(|parameter| parameter.field == FilterField::ClassToName);
    if from.is_none() && to.is_none() {
        return Ok((None, None));
    }
    let class_permissions = [
        AuthorizationPermission::ReadCollection,
        AuthorizationPermission::ReadClass,
    ];
    if !visibility.allows_permissions(&class_permissions) {
        return Ok((from.map(|_| Vec::new()), to.map(|_| Vec::new())));
    }
    let collection_ids =
        authorized_collection_ids(connection, visibility, &class_permissions).await?;
    let mut load = async |parameter: &hubuum_query::ParsedQueryParam| {
        use crate::schema::hubuumclass;

        let mut classes = hubuumclass::table
            .filter(hubuumclass::collection_id.eq_any(&collection_ids))
            .into_boxed();
        crate::postgres_string_filter!(classes, parameter, hubuumclass::name);
        if let Some(scope) = visibility.resources() {
            classes = classes.filter(
                hubuumclass::collection_id
                    .eq_any(scope.collection_ids())
                    .or(hubuumclass::id.eq_any(scope.class_ids())),
            );
        }
        classes
            .select(hubuumclass::id)
            .load::<i32>(connection)
            .await
            .map_err(PostgresStorageError::from)
    };
    let from_ids = match from {
        Some(parameter) => Some(load(parameter).await?),
        None => None,
    };
    let to_ids = match to {
        Some(parameter) => Some(load(parameter).await?),
        None => None,
    };
    Ok((from_ids, to_ids))
}

fn required_permissions(
    options: &QueryOptions,
    baseline: impl IntoIterator<Item = AuthorizationPermission>,
) -> Result<Vec<AuthorizationPermission>, PostgresStorageError> {
    let mut permissions = baseline.into_iter().collect::<Vec<_>>();
    for parameter in &options.filters {
        if parameter.field == FilterField::Permissions {
            permissions.push(
                AuthorizationPermission::from_name(&parameter.value)
                    .map_err(|error| PostgresStorageError::bad_request(error.to_string()))?,
            );
        }
    }
    permissions.sort_unstable();
    permissions.dedup();
    Ok(permissions)
}

fn empty_options() -> QueryOptions {
    QueryOptions {
        filters: Vec::new(),
        sort: Vec::new(),
        limit: None,
        cursor: None,
        include_total: false,
    }
}

#[derive(Clone, Copy)]
enum RelationKind {
    Class,
    Object,
}

fn relation_cursor_fields(
    options: &QueryOptions,
    kind: RelationKind,
) -> Result<Vec<CursorSqlField>, PostgresStorageError> {
    options
        .sort
        .iter()
        .map(|sort| relation_cursor_field(&sort.field, kind))
        .collect()
}

fn relation_cursor_field(
    field: &FilterField,
    kind: RelationKind,
) -> Result<CursorSqlField, PostgresStorageError> {
    let (column, sql_type) = match (kind, field) {
        (RelationKind::Class, FilterField::Id) => {
            ("hubuumclass_relation.id", CursorSqlType::Integer)
        }
        (RelationKind::Object, FilterField::Id) => {
            ("hubuumobject_relation.id", CursorSqlType::Integer)
        }
        (RelationKind::Class, FilterField::ClassFrom) => (
            "hubuumclass_relation.from_hubuum_class_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Class, FilterField::ClassTo) => (
            "hubuumclass_relation.to_hubuum_class_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ClassRelation) => (
            "hubuumobject_relation.class_relation_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ObjectFrom) => (
            "hubuumobject_relation.from_hubuum_object_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Object, FilterField::ObjectTo) => (
            "hubuumobject_relation.to_hubuum_object_id",
            CursorSqlType::Integer,
        ),
        (RelationKind::Class, FilterField::CreatedAt) => {
            ("hubuumclass_relation.created_at", CursorSqlType::DateTime)
        }
        (RelationKind::Object, FilterField::CreatedAt) => {
            ("hubuumobject_relation.created_at", CursorSqlType::DateTime)
        }
        (RelationKind::Class, FilterField::UpdatedAt) => {
            ("hubuumclass_relation.updated_at", CursorSqlType::DateTime)
        }
        (RelationKind::Object, FilterField::UpdatedAt) => {
            ("hubuumobject_relation.updated_at", CursorSqlType::DateTime)
        }
        (RelationKind::Class, FilterField::Revision) => {
            ("hubuumclass_relation.revision", CursorSqlType::BigInt)
        }
        (RelationKind::Object, FilterField::Revision) => {
            ("hubuumobject_relation.revision", CursorSqlType::BigInt)
        }
        _ => {
            return Err(PostgresStorageError::bad_request(format!(
                "Field '{field}' is not orderable for relations"
            )));
        }
    };
    Ok(CursorSqlField {
        column,
        sql_type,
        nullable: false,
    })
}

fn validate_positive_id(id: i32, label: &str) -> Result<(), PostgresStorageError> {
    if id <= 0 {
        return Err(PostgresStorageError::bad_request(format!(
            "{label} must be greater than zero"
        )));
    }
    Ok(())
}
