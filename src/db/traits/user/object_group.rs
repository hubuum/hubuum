use std::collections::HashSet;

use diesel::dsl::{count, sql};
use diesel::sql_types::{BigInt, Bool, Jsonb};
use diesel_async::RunQueryDsl;
use hubuum_computed_fields::{EvaluationResult, MAX_PERSONAL_DEFINITIONS, MAX_SHARED_DEFINITIONS};

use super::{LoadPermittedCollections, UserCollectionAccessors};
use crate::db::prelude::*;
use crate::db::traits::authz::scope_allows;
use crate::db::traits::computed_field::{acquire_computed_class_shared_lock, evaluate_definitions};
use crate::db::traits::search::JsonPredicateExt;
use crate::db::{DbConnection, DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::computed_field::{
    COMPUTED_FIELD_VISIBILITY_PERSONAL, COMPUTED_FIELD_VISIBILITY_SHARED, ComputedFieldDefinition,
};
use crate::models::object::HubuumObject;
use crate::models::object_group::{
    ComputedFieldScope, DecodedObjectGroupCursor, ObjectGroupBackendParts,
    ObjectGroupBackendRequest, ObjectGroupDimension, ObjectGroupPage, ObjectGroupRow,
    ObjectGroupScalarField, ObjectGroupSort, ObjectGroupSpec,
};
use crate::models::search::{FilterField, QueryOptions, QueryParamsExt, SortParam};
use crate::models::{Permissions, UserID};
use crate::pagination::{
    SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, finalize_page,
    prepare_db_pagination,
};
use crate::permissions::{
    PermissionDecision, PermissionRequest, PrincipalRef, ResourceAttrs, ResourceKind, ResourceRef,
};
use crate::traits::BackendContext;
use crate::utilities::extensions::CustomStringExtensions;

#[cfg(not(feature = "integration-test-support"))]
const OBJECT_GROUP_CANDIDATE_BATCH_SIZE: usize = 500;
#[cfg(feature = "integration-test-support")]
const OBJECT_GROUP_CANDIDATE_BATCH_SIZE: usize = 2;

struct ObjectGroupExecution<'a> {
    pool: &'a DbPool,
    class_id: i32,
    query_options: &'a QueryOptions,
    spec: &'a ObjectGroupSpec,
    personal_owner_id: Option<i32>,
    required_permissions: Vec<Permissions>,
    token_scopes: Option<&'a [Permissions]>,
    decoded_cursor: Option<DecodedObjectGroupCursor>,
    effective_limit: usize,
}

#[derive(Debug, Clone)]
enum ObjectGroupBindValue {
    Json(serde_json::Value),
    BigInt(i64),
}

#[derive(Debug, Clone)]
struct ObjectGroupSqlSpec {
    sql: String,
    binds: Vec<ObjectGroupBindValue>,
}

impl ObjectGroupSqlSpec {
    fn indexed(self) -> Self {
        Self {
            sql: self.sql.replace_question_mark_with_indexed_n(),
            binds: self.binds,
        }
    }
}

macro_rules! bind_object_group_query {
    ($spec:expr) => {{
        let spec = $spec.indexed();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for bind in spec.binds {
            query = match bind {
                ObjectGroupBindValue::Json(value) => query.bind::<Jsonb, _>(value),
                ObjectGroupBindValue::BigInt(value) => query.bind::<BigInt, _>(value),
            };
        }
        query
    }};
}

macro_rules! apply_visible_object_filters {
    ($query:ident, $query_options:expr) => {{
        let query_params = $query_options.filters.clone();
        for param in query_params.json_datas(FilterField::JsonData)? {
            $query = $query.filter(param.as_json_predicate()?);
        }
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => crate::numeric_search!($query, param, operator, object_id),
                FilterField::Collections | FilterField::CollectionId => {
                    crate::numeric_search!($query, param, operator, object_collection_id)
                }
                FilterField::CreatedAt => {
                    crate::date_search!($query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    crate::date_search!($query, param, operator, object_updated_at)
                }
                FilterField::Name => crate::string_search!($query, param, operator, object_name),
                FilterField::Description => {
                    crate::string_search!($query, param, operator, object_description)
                }
                FilterField::Classes | FilterField::ClassId => {
                    crate::numeric_search!($query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData | FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }
    }};
}

macro_rules! visible_filtered_object_query {
    ($collection_ids:expr, $query_options:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .filter(object_collection_id.eq_any($collection_ids))
            .into_boxed();
        apply_visible_object_filters!(query, $query_options);
        query
    }};
}

macro_rules! visible_filtered_group_query {
    ($collection_ids:expr, $query_options:expr, $sort_key_sql:expr) => {{
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, created_at as object_created_at,
            description as object_description, hubuum_class_id, hubuumobject, id as object_id,
            name as object_name, updated_at as object_updated_at,
        };

        let mut query = hubuumobject
            .group_by(sql::<Jsonb>($sort_key_sql))
            .select((sql::<Jsonb>($sort_key_sql), sql::<BigInt>("COUNT(*)")))
            .into_boxed()
            .filter(object_collection_id.eq_any($collection_ids));
        apply_visible_object_filters!(query, $query_options);
        query
    }};
}

#[derive(diesel::QueryableByName)]
struct ObjectGroupDatabaseRow {
    #[diesel(sql_type = Jsonb)]
    dimensions: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    sort_key: serde_json::Value,
    #[diesel(sql_type = BigInt)]
    object_count: i64,
}

#[derive(diesel::QueryableByName)]
struct ObjectGroupCountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

pub trait ObjectGroupBackend: UserCollectionAccessors {
    async fn group_objects_from_backend<C>(
        &self,
        context: &C,
        request: ObjectGroupBackendRequest,
    ) -> Result<ObjectGroupPage, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let ObjectGroupBackendParts {
            class_id,
            query_options,
            spec,
            personal_owner_id,
            authorization,
        } = request.into_parts();
        let class_id = class_id.id();
        let personal_owner_id = personal_owner_id.map(UserID::id);
        let (required_permissions, token_scopes) = authorization.into_parts();
        let pool = context.db_pool();
        let effective_limit = effective_page_limit(&query_options)?;
        let decoded_cursor = query_options
            .cursor
            .as_deref()
            .map(|cursor| spec.decode_cursor(cursor))
            .transpose()?;
        tracing::debug!(
            message = "Grouping visible filtered objects",
            user_id = self.principal_id(),
            dimensions = ?spec
                .dimensions()
                .iter()
                .map(ObjectGroupDimension::canonical)
                .collect::<Vec<_>>()
        );
        let sql_visibility_pushdown = context
            .permission_backend()
            .is_none_or(|backend| backend.supports_sql_visibility_pushdown());
        let execution = ObjectGroupExecution {
            pool,
            class_id,
            query_options: &query_options,
            spec: &spec,
            personal_owner_id,
            required_permissions,
            token_scopes: token_scopes.as_deref(),
            decoded_cursor,
            effective_limit,
        };
        if sql_visibility_pushdown && !spec.has_computed_dimension() {
            return group_visible_filtered_objects_with_sql(self, execution).await;
        }
        group_visible_filtered_objects_in_batches(self, context, execution).await
    }
}

impl<T> ObjectGroupBackend for T where T: UserCollectionAccessors + ?Sized {}

async fn group_visible_filtered_objects_with_sql<U>(
    user: &U,
    execution: ObjectGroupExecution<'_>,
) -> Result<ObjectGroupPage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    let ObjectGroupExecution {
        pool,
        query_options,
        spec,
        mut required_permissions,
        token_scopes,
        decoded_cursor,
        effective_limit,
        ..
    } = execution;
    if !required_permissions.contains(&Permissions::ReadCollection) {
        required_permissions.push(Permissions::ReadCollection);
    }
    let is_admin = user.is_admin(pool).await?;
    let collection_ids = user
        .load_collections_with_permissions_with_admin_status(
            pool,
            &required_permissions,
            is_admin,
            token_scopes,
        )
        .await?
        .into_iter()
        .map(|collection| collection.id)
        .collect::<Vec<_>>();
    let sort_key_sql = direct_group_sort_key(spec);

    let total_count = if query_options.include_total {
        let query = visible_filtered_object_query!(&collection_ids, query_options);
        with_connection(pool, async |conn| {
            query
                .select(count(sql::<Jsonb>(&sort_key_sql)).aggregate_distinct())
                .get_result::<i64>(conn)
                .await
        })
        .await?
    } else {
        SKIPPED_TOTAL_COUNT
    };

    let mut query = visible_filtered_group_query!(&collection_ids, query_options, &sort_key_sql);
    if let Some(cursor) = decoded_cursor {
        query = query.having(sql::<Bool>(&inline_cursor_clause(
            spec.sort(),
            &cursor,
            &sort_key_sql,
        )?));
    }
    query = match spec.sort() {
        ObjectGroupSort::DimensionsAscending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC")))
        }
        ObjectGroupSort::DimensionsDescending => {
            query.order_by(sql::<Jsonb>(&format!("{sort_key_sql} DESC")))
        }
        ObjectGroupSort::ObjectCountAscending => query
            .order_by(sql::<BigInt>("COUNT(*) ASC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
        ObjectGroupSort::ObjectCountDescending => query
            .order_by(sql::<BigInt>("COUNT(*) DESC"))
            .then_order_by(sql::<Jsonb>(&format!("{sort_key_sql} ASC"))),
    };
    query = query.limit(
        i64::try_from(effective_limit.saturating_add(1)).map_err(|_| {
            ApiError::BadRequest("Object group page limit is too large".to_string())
        })?,
    );
    let database_rows = with_connection(pool, async |conn| {
        query.load::<(serde_json::Value, i64)>(conn).await
    })
    .await?
    .into_iter()
    .map(|(sort_key, object_count)| {
        Ok(ObjectGroupDatabaseRow {
            dimensions: dimensions_from_sort_key(spec, &sort_key)?,
            sort_key,
            object_count,
        })
    })
    .collect::<Result<Vec<_>, ApiError>>()?;
    finish_group_page(database_rows, total_count, effective_limit, spec)
}

async fn group_visible_filtered_objects_in_batches<U, C>(
    user: &U,
    context: &C,
    execution: ObjectGroupExecution<'_>,
) -> Result<ObjectGroupPage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
    C: BackendContext + ?Sized,
{
    let ObjectGroupExecution {
        pool,
        class_id,
        query_options,
        spec,
        personal_owner_id,
        required_permissions,
        token_scopes,
        decoded_cursor,
        effective_limit,
    } = execution;
    let permission_backend = context.permission_backend();
    let sql_visibility_pushdown =
        permission_backend.is_none_or(|backend| backend.supports_sql_visibility_pushdown());
    let mut visibility_permissions = required_permissions.clone();
    if sql_visibility_pushdown && !visibility_permissions.contains(&Permissions::ReadCollection) {
        visibility_permissions.push(Permissions::ReadCollection);
    }
    if !scope_allows(token_scopes, &visibility_permissions) {
        return empty_group_page(query_options);
    }

    let principal = if sql_visibility_pushdown {
        None
    } else {
        Some(PrincipalRef::load(pool, user).await?)
    };
    let collection_ids = if sql_visibility_pushdown {
        let is_admin = user.is_admin(pool).await?;
        Some(
            user.load_collections_with_permissions_with_admin_status(
                pool,
                &visibility_permissions,
                is_admin,
                token_scopes,
            )
            .await?
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>(),
        )
    } else {
        None
    };

    with_transaction(
        pool,
        async |connection| -> Result<ObjectGroupPage, ApiError> {
            let computed_definitions =
                load_computed_group_definitions(connection, class_id, spec, personal_owner_id)
                    .await?;
            create_group_accumulator(connection).await?;

            let mut chunk_options = count_query_options(query_options);
            chunk_options.sort = vec![SortParam {
                field: FilterField::Id,
                descending: false,
            }];
            chunk_options.limit = Some(OBJECT_GROUP_CANDIDATE_BATCH_SIZE);
            chunk_options.include_total = false;
            let mut object_cursor = None;

            loop {
                chunk_options.cursor.clone_from(&object_cursor);
                let database_options = prepare_db_pagination::<HubuumObject>(&chunk_options)?;
                let candidates = load_group_candidate_batch(
                    connection,
                    &database_options,
                    collection_ids.as_deref(),
                )
                .await?;
                let candidate_page = finalize_page(candidates, &chunk_options)?;
                let authorized = if let (Some(backend), Some(principal)) =
                    (permission_backend, principal.as_ref())
                {
                    authorize_object_snapshots(
                        backend,
                        principal,
                        candidate_page.items,
                        &required_permissions,
                    )
                    .await?
                } else {
                    candidate_page.items
                };
                validate_candidate_classes(&authorized, class_id)?;
                let grouped =
                    grouped_snapshot_rows(connection, authorized, spec, &computed_definitions)
                        .await?;
                merge_group_rows(connection, grouped).await?;

                object_cursor = candidate_page.next_cursor;
                if object_cursor.is_none() {
                    break;
                }
            }

            page_accumulated_groups(
                connection,
                query_options,
                spec,
                decoded_cursor,
                effective_limit,
            )
            .await
        },
    )
    .await
}

fn empty_group_page(query_options: &QueryOptions) -> Result<ObjectGroupPage, ApiError> {
    Ok(ObjectGroupPage::new(
        Vec::new(),
        if query_options.include_total {
            0
        } else {
            SKIPPED_TOTAL_COUNT
        },
        None,
    ))
}

async fn load_group_candidate_batch(
    connection: &mut DbConnection,
    query_options: &QueryOptions,
    collection_ids: Option<&[i32]>,
) -> Result<Vec<HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, created_at as object_created_at,
        description as object_description, hubuum_class_id, hubuumobject, id as object_id,
        name as object_name, updated_at as object_updated_at,
    };

    let mut query = hubuumobject.into_boxed();
    if let Some(collection_ids) = collection_ids {
        query = query.filter(object_collection_id.eq_any(collection_ids));
    }
    apply_visible_object_filters!(query, query_options);
    crate::apply_query_options!(query, query_options, HubuumObject);
    Ok(query
        .select(hubuumobject::all_columns())
        .distinct()
        .load::<HubuumObject>(connection)
        .await?)
}

async fn authorize_object_snapshots(
    backend: &dyn crate::permissions::PermissionBackend,
    principal: &PrincipalRef,
    candidates: Vec<HubuumObject>,
    required_permissions: &[Permissions],
) -> Result<Vec<HubuumObject>, ApiError> {
    let permissions_per_object = required_permissions.len();
    if permissions_per_object == 0 {
        return Err(ApiError::InternalServerError(
            "Object group authorization requires at least one permission".to_string(),
        ));
    }
    let requests = candidates
        .iter()
        .flat_map(|object| {
            let object_resource = ResourceRef {
                kind: ResourceKind::Object,
                id: object.id,
                attrs: ResourceAttrs {
                    collection_id: Some(object.collection_id),
                    class_id: Some(object.hubuum_class_id),
                    name: Some(object.name.clone()),
                    ..Default::default()
                },
            };
            required_permissions
                .iter()
                .copied()
                .map(move |permission| PermissionRequest {
                    resource: object_resource.normalized_for_permission(permission),
                    permissions: vec![permission],
                })
        })
        .collect::<Vec<_>>();
    let decisions = backend.authorize_many(principal, requests).await?;
    let expected_decisions = candidates
        .len()
        .checked_mul(permissions_per_object)
        .ok_or_else(|| {
            ApiError::InternalServerError(
                "Object group permission decision count overflowed".to_string(),
            )
        })?;
    if decisions.len() != expected_decisions {
        return Err(ApiError::InternalServerError(
            "Permission backend returned an unexpected number of object decisions".to_string(),
        ));
    }
    let allowed = decisions
        .chunks_exact(permissions_per_object)
        .map(|object_decisions| {
            object_decisions
                .iter()
                .all(|decision| *decision == PermissionDecision::Allow)
        })
        .collect::<Vec<_>>();
    Ok(candidates
        .into_iter()
        .zip(allowed)
        .filter_map(|(object, allowed)| allowed.then_some(object))
        .collect())
}

fn validate_candidate_classes(candidates: &[HubuumObject], class_id: i32) -> Result<(), ApiError> {
    let class_ids = candidates
        .iter()
        .map(|object| object.hubuum_class_id)
        .collect::<HashSet<_>>();
    if class_ids.len() > 1 {
        return Err(ApiError::InternalServerError(
            "Object group candidates must belong to exactly one class".to_string(),
        ));
    }
    if class_ids.iter().any(|candidate| *candidate != class_id) {
        return Err(ApiError::InternalServerError(
            "Object group candidates do not belong to the requested class".to_string(),
        ));
    }
    Ok(())
}

async fn grouped_snapshot_rows(
    connection: &mut DbConnection,
    candidates: Vec<HubuumObject>,
    spec: &ObjectGroupSpec,
    computed_definitions: &ComputedGroupDefinitions,
) -> Result<Vec<ObjectGroupDatabaseRow>, ApiError> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let (candidates, computed_payload) = if spec.has_computed_dimension() {
        computed_group_payload(candidates, spec, computed_definitions)?
    } else {
        (candidates, None)
    };
    let mut query = build_group_ctes(candidates, computed_payload, spec)?;
    query
        .sql
        .push_str("\nSELECT dimensions, sort_key, object_count FROM group_rows");
    Ok(bind_object_group_query!(query)
        .load::<ObjectGroupDatabaseRow>(connection)
        .await?)
}

async fn create_group_accumulator(connection: &mut DbConnection) -> Result<(), ApiError> {
    diesel::sql_query(
        "CREATE TEMP TABLE object_group_accumulator (
            sort_key jsonb NOT NULL,
            dimensions jsonb NOT NULL,
            object_count bigint NOT NULL CHECK (object_count > 0)
        ) ON COMMIT DROP",
    )
    .execute(connection)
    .await?;
    diesel::sql_query(
        "CREATE INDEX object_group_accumulator_sort_key_idx
            ON object_group_accumulator USING HASH (sort_key)",
    )
    .execute(connection)
    .await?;
    Ok(())
}

async fn merge_group_rows(
    connection: &mut DbConnection,
    groups: Vec<ObjectGroupDatabaseRow>,
) -> Result<(), ApiError> {
    if groups.is_empty() {
        return Ok(());
    }
    let payload = serde_json::Value::Array(
        groups
            .into_iter()
            .map(|row| {
                serde_json::json!({
                    "dimensions": row.dimensions,
                    "sort_key": row.sort_key,
                    "object_count": row.object_count,
                })
            })
            .collect(),
    );
    let update = ObjectGroupSqlSpec {
        sql: "UPDATE object_group_accumulator AS accumulated
SET object_count = accumulated.object_count + incoming.object_count
FROM jsonb_to_recordset(?::jsonb) AS incoming(
    dimensions jsonb,
    sort_key jsonb,
    object_count bigint
)
WHERE accumulated.sort_key = incoming.sort_key"
            .to_string(),
        binds: vec![ObjectGroupBindValue::Json(payload.clone())],
    };
    bind_object_group_query!(update).execute(connection).await?;

    let insert = ObjectGroupSqlSpec {
        sql: "INSERT INTO object_group_accumulator (
    dimensions,
    sort_key,
    object_count
)
SELECT incoming.dimensions, incoming.sort_key, incoming.object_count
FROM jsonb_to_recordset(?::jsonb) AS incoming(
    dimensions jsonb,
    sort_key jsonb,
    object_count bigint
)
WHERE NOT EXISTS (
    SELECT 1
    FROM object_group_accumulator AS accumulated
    WHERE accumulated.sort_key = incoming.sort_key
)"
        .to_string(),
        binds: vec![ObjectGroupBindValue::Json(payload)],
    };
    bind_object_group_query!(insert).execute(connection).await?;
    Ok(())
}

async fn page_accumulated_groups(
    connection: &mut DbConnection,
    query_options: &QueryOptions,
    spec: &ObjectGroupSpec,
    decoded_cursor: Option<DecodedObjectGroupCursor>,
    effective_limit: usize,
) -> Result<ObjectGroupPage, ApiError> {
    let total_count = if query_options.include_total {
        diesel::sql_query("SELECT COUNT(*) AS count FROM object_group_accumulator")
            .get_result::<ObjectGroupCountRow>(connection)
            .await?
            .count
    } else {
        SKIPPED_TOTAL_COUNT
    };
    let mut page_spec = ObjectGroupSqlSpec {
        sql: "SELECT dimensions, sort_key, object_count
FROM object_group_accumulator"
            .to_string(),
        binds: Vec::new(),
    };
    if let Some(cursor) = decoded_cursor {
        append_cursor_clause(&mut page_spec, spec.sort(), cursor);
    }
    page_spec.sql.push_str("\nORDER BY ");
    page_spec.sql.push_str(order_clause(spec.sort()));
    page_spec.sql.push_str("\nLIMIT ?");
    page_spec.binds.push(ObjectGroupBindValue::BigInt(
        i64::try_from(effective_limit.saturating_add(1)).map_err(|_| {
            ApiError::BadRequest("Object group page limit is too large".to_string())
        })?,
    ));
    let database_rows = bind_object_group_query!(page_spec)
        .load::<ObjectGroupDatabaseRow>(connection)
        .await?;
    finish_group_page(database_rows, total_count, effective_limit, spec)
}

fn finish_group_page(
    database_rows: Vec<ObjectGroupDatabaseRow>,
    total_count: i64,
    effective_limit: usize,
    spec: &ObjectGroupSpec,
) -> Result<ObjectGroupPage, ApiError> {
    let mut rows = database_rows
        .into_iter()
        .map(|row| ObjectGroupRow::from_database(row.dimensions, row.object_count, row.sort_key))
        .collect::<Result<Vec<_>, _>>()?;
    let has_more = rows.len() > effective_limit;
    if has_more {
        rows.truncate(effective_limit);
    }
    let next_cursor = if has_more {
        rows.last().map(|row| spec.encode_cursor(row)).transpose()?
    } else {
        None
    };
    Ok(ObjectGroupPage::new(rows, total_count, next_cursor))
}

#[derive(Default)]
struct ComputedGroupDefinitions {
    shared: Vec<ComputedFieldDefinition>,
    personal: Vec<ComputedFieldDefinition>,
}

async fn load_computed_group_definitions(
    connection: &mut DbConnection,
    class_id_value: i32,
    spec: &ObjectGroupSpec,
    personal_owner_id: Option<i32>,
) -> Result<ComputedGroupDefinitions, ApiError> {
    let selectors = spec
        .dimensions()
        .iter()
        .filter_map(ObjectGroupDimension::computed_selector)
        .collect::<Vec<_>>();
    if selectors.is_empty() {
        return Ok(ComputedGroupDefinitions::default());
    }

    let shared_keys = selectors
        .iter()
        .filter(|selector| selector.scope() == ComputedFieldScope::Shared)
        .map(|selector| selector.key().to_string())
        .collect::<Vec<_>>();
    let personal_keys = selectors
        .iter()
        .filter(|selector| selector.scope() == ComputedFieldScope::Personal)
        .map(|selector| selector.key().to_string())
        .collect::<Vec<_>>();

    if !shared_keys.is_empty() {
        acquire_computed_class_shared_lock(connection, class_id_value).await?;
    }
    use crate::schema::computed_field_definitions::dsl::{
        class_id, computed_field_definitions, id, key, owner_user_id, visibility,
    };
    let mut definitions = Vec::with_capacity(selectors.len());
    if !shared_keys.is_empty() {
        definitions.extend(
            computed_field_definitions
                .filter(class_id.eq(class_id_value))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_SHARED))
                .filter(key.eq_any(&shared_keys))
                .order(id.asc())
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(connection)
                .await?,
        );
    }
    if !personal_keys.is_empty() {
        let personal_owner_id = personal_owner_id.ok_or_else(|| {
            ApiError::InternalServerError(
                "Personal computed grouping requires an owner".to_string(),
            )
        })?;
        definitions.extend(
            computed_field_definitions
                .filter(class_id.eq(class_id_value))
                .filter(visibility.eq(COMPUTED_FIELD_VISIBILITY_PERSONAL))
                .filter(owner_user_id.eq(Some(personal_owner_id)))
                .filter(key.eq_any(&personal_keys))
                .order(id.asc())
                .select(ComputedFieldDefinition::as_select())
                .load::<ComputedFieldDefinition>(connection)
                .await?,
        );
    }

    let mut selected = ComputedGroupDefinitions::default();
    for selector in selectors {
        let definition = definitions
            .iter()
            .find(|definition| match selector.scope() {
                ComputedFieldScope::Shared => {
                    definition.visibility == COMPUTED_FIELD_VISIBILITY_SHARED
                        && definition.key == selector.key()
                }
                ComputedFieldScope::Personal => {
                    definition.visibility == COMPUTED_FIELD_VISIBILITY_PERSONAL
                        && definition.owner_user_id == personal_owner_id
                        && definition.key == selector.key()
                }
            });
        let Some(definition) = definition else {
            return Err(ApiError::BadRequest(format!(
                "Computed group dimension '{}' does not name an accessible field in class {class_id_value}",
                selector.canonical()
            )));
        };
        if !definition.enabled {
            return Err(ApiError::BadRequest(format!(
                "Computed group dimension '{}' is disabled",
                selector.canonical()
            )));
        }
        definition.evaluator_definition()?;
        let target = match selector.scope() {
            ComputedFieldScope::Shared => &mut selected.shared,
            ComputedFieldScope::Personal => &mut selected.personal,
        };
        if !target
            .iter()
            .any(|selected_definition| selected_definition.id == definition.id)
        {
            target.push(definition.clone());
        }
    }
    Ok(selected)
}

fn computed_group_payload(
    candidates: Vec<HubuumObject>,
    spec: &ObjectGroupSpec,
    definitions: &ComputedGroupDefinitions,
) -> Result<(Vec<HubuumObject>, Option<serde_json::Value>), ApiError> {
    let mut payload = serde_json::Map::new();
    for object in &candidates {
        let shared = if definitions.shared.is_empty() {
            None
        } else {
            Some(evaluate_definitions(
                &object.data,
                &definitions.shared,
                MAX_SHARED_DEFINITIONS,
                "shared_group",
            )?)
        };
        let personal = if definitions.personal.is_empty() {
            None
        } else {
            Some(evaluate_definitions(
                &object.data,
                &definitions.personal,
                MAX_PERSONAL_DEFINITIONS,
                "personal_group",
            )?)
        };
        let values = spec
            .dimensions()
            .iter()
            .map(|dimension| {
                computed_dimension_value(shared.as_ref(), personal.as_ref(), dimension)
            })
            .collect::<Vec<_>>();
        payload.insert(object.id.to_string(), serde_json::Value::Array(values));
    }
    Ok((candidates, Some(serde_json::Value::Object(payload))))
}

fn computed_dimension_value(
    shared: Option<&EvaluationResult>,
    personal: Option<&EvaluationResult>,
    dimension: &ObjectGroupDimension,
) -> serde_json::Value {
    let Some(selector) = dimension.computed_selector() else {
        return serde_json::Value::Null;
    };
    let (values, has_error) = match selector.scope() {
        ComputedFieldScope::Shared => match shared {
            Some(shared) => (&shared.values, shared.errors.contains_key(selector.key())),
            None => return serde_json::json!({"state": 3, "value": null}),
        },
        ComputedFieldScope::Personal => match personal {
            Some(personal) => (
                &personal.values,
                personal.errors.contains_key(selector.key()),
            ),
            None => return serde_json::json!({"state": 3, "value": null}),
        },
    };
    if has_error {
        return serde_json::json!({"state": 3, "value": null});
    }
    match values.get(selector.key()) {
        Some(serde_json::Value::Null) => serde_json::json!({"state": 1, "value": null}),
        Some(value) => serde_json::json!({"state": 0, "value": value}),
        None => serde_json::json!({"state": 3, "value": null}),
    }
}

fn build_group_ctes(
    candidates: Vec<HubuumObject>,
    computed_payload: Option<serde_json::Value>,
    spec: &ObjectGroupSpec,
) -> Result<ObjectGroupSqlSpec, ApiError> {
    let candidates = serde_json::to_value(candidates).map_err(|error| {
        ApiError::InternalServerError(format!(
            "Failed to serialize authorized object snapshots: {error}"
        ))
    })?;
    let mut binds = vec![ObjectGroupBindValue::Json(candidates)];
    let computed_input = if let Some(payload) = computed_payload {
        binds.push(ObjectGroupBindValue::Json(payload));
        ", ?::jsonb AS computed_values"
    } else {
        ""
    };
    let computed_column = if computed_input.is_empty() {
        ""
    } else {
        ", input.computed_values"
    };

    let expressions = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| dimension_sql(index, dimension))
        .collect::<Vec<_>>();
    let dimension_select = expressions
        .iter()
        .enumerate()
        .flat_map(|(index, (state, value))| {
            [
                format!("{state} AS d{index}_state"),
                format!("{value} AS d{index}_value"),
            ]
        })
        .collect::<Vec<_>>()
        .join(",\n        ");
    let group_columns = (0..expressions.len())
        .flat_map(|index| [format!("d{index}_state"), format!("d{index}_value")])
        .collect::<Vec<_>>()
        .join(", ");
    let response_dimensions = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| response_dimension_sql(index, dimension))
        .collect::<Vec<_>>()
        .join(",\n            ");
    let sort_dimensions = (0..expressions.len())
        .map(|index| format!("jsonb_build_array(d{index}_state, d{index}_value)"))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        r#"WITH query_input AS (
    SELECT ?::jsonb AS objects{computed_input}
),
visible_filtered_objects AS (
    SELECT object.*{computed_column}
    FROM query_input AS input
    CROSS JOIN LATERAL jsonb_populate_recordset(NULL::hubuumobject, input.objects) AS object
),
dimensioned_objects AS (
    SELECT
        {dimension_select}
    FROM visible_filtered_objects AS object
),
grouped_objects AS (
    SELECT
        {group_columns},
        COUNT(*) AS object_count
    FROM dimensioned_objects
    GROUP BY {group_columns}
),
group_rows AS (
    SELECT
        jsonb_build_array(
            {response_dimensions}
        ) AS dimensions,
        jsonb_build_array({sort_dimensions}) AS sort_key,
        object_count
    FROM grouped_objects
)"#
    );
    Ok(ObjectGroupSqlSpec { sql, binds })
}

fn dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> (String, String) {
    dimension_sql_for_source(index, dimension, "object")
}

fn dimension_sql_for_source(
    index: usize,
    dimension: &ObjectGroupDimension,
    source: &str,
) -> (String, String) {
    match dimension {
        ObjectGroupDimension::Scalar(field) => {
            let column = match field {
                ObjectGroupScalarField::Name => "name",
                ObjectGroupScalarField::Description => "description",
                ObjectGroupScalarField::CollectionId => "collection_id",
                ObjectGroupScalarField::CreatedAt => "created_at",
                ObjectGroupScalarField::UpdatedAt => "updated_at",
            };
            (
                "0::smallint".to_string(),
                format!("to_jsonb({source}.{column})"),
            )
        }
        ObjectGroupDimension::JsonData(path) => {
            let path = path
                .segments()
                .iter()
                .map(|segment| sql_string_literal(segment))
                .collect::<Vec<_>>()
                .join(", ");
            let value = format!("{source}.data #> ARRAY[{path}]::text[]");
            (
                format!(
                    "CASE WHEN {value} IS NULL THEN 2::smallint WHEN {value} = 'null'::jsonb THEN 1::smallint ELSE 0::smallint END"
                ),
                format!("COALESCE({value}, 'null'::jsonb)"),
            )
        }
        ObjectGroupDimension::Computed(_) => {
            let value = format!("{source}.computed_values -> {source}.id::text -> {index}");
            (
                format!("(({value}) ->> 'state')::smallint"),
                format!("COALESCE(({value}) -> 'value', 'null'::jsonb)"),
            )
        }
    }
}

fn direct_group_sort_key(spec: &ObjectGroupSpec) -> String {
    let expressions = spec
        .dimensions()
        .iter()
        .enumerate()
        .map(|(index, dimension)| dimension_sql_for_source(index, dimension, "hubuumobject"))
        .collect::<Vec<_>>();
    let sort_key = expressions
        .iter()
        .map(|(state, value)| format!("jsonb_build_array({state}, {value})"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("jsonb_build_array({sort_key})")
}

fn dimensions_from_sort_key(
    spec: &ObjectGroupSpec,
    sort_key: &serde_json::Value,
) -> Result<serde_json::Value, ApiError> {
    let values = sort_key.as_array().ok_or_else(|| {
        ApiError::InternalServerError(
            "Database returned a non-array object group sort key".to_string(),
        )
    })?;
    if values.len() != spec.dimensions().len() {
        return Err(ApiError::InternalServerError(
            "Database returned an object group sort key with the wrong dimension count".to_string(),
        ));
    }
    let dimensions = spec
        .dimensions()
        .iter()
        .zip(values)
        .map(|(dimension, item)| {
            let pair = item
                .as_array()
                .filter(|pair| pair.len() == 2)
                .ok_or_else(|| {
                    ApiError::InternalServerError(
                        "Database returned an invalid object group sort key".to_string(),
                    )
                })?;
            let state = pair[0].as_i64().ok_or_else(|| {
                ApiError::InternalServerError(
                    "Database returned an invalid object group value state".to_string(),
                )
            })?;
            let field = dimension.canonical();
            Ok(match state {
                0 => serde_json::json!({
                    "field": field,
                    "state": "value",
                    "value": pair[1].clone(),
                }),
                1 => serde_json::json!({"field": field, "state": "null"}),
                2 => serde_json::json!({"field": field, "state": "missing"}),
                3 => serde_json::json!({"field": field, "state": "unavailable"}),
                _ => {
                    return Err(ApiError::InternalServerError(
                        "Database returned an unknown object group value state".to_string(),
                    ));
                }
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(serde_json::Value::Array(dimensions))
}

fn response_dimension_sql(index: usize, dimension: &ObjectGroupDimension) -> String {
    let state = format!("d{index}_state");
    let value = format!("d{index}_value");
    response_dimension_expression(dimension, &state, &value)
}

fn response_dimension_expression(
    dimension: &ObjectGroupDimension,
    state: &str,
    value: &str,
) -> String {
    let field = sql_string_literal(&dimension.canonical());
    format!(
        "CASE {state} WHEN 0 THEN jsonb_build_object('field', {field}, 'state', 'value', 'value', {value}) WHEN 1 THEN jsonb_build_object('field', {field}, 'state', 'null') WHEN 2 THEN jsonb_build_object('field', {field}, 'state', 'missing') ELSE jsonb_build_object('field', {field}, 'state', 'unavailable') END"
    )
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn inline_cursor_clause(
    sort: ObjectGroupSort,
    cursor: &DecodedObjectGroupCursor,
    sort_key_sql: &str,
) -> Result<String, ApiError> {
    let sort_key = serde_json::to_string(&cursor.sort_key).map_err(|error| {
        ApiError::BadRequest(format!(
            "group cursor contains invalid ordering values: {error}"
        ))
    })?;
    let sort_key = format!("{}::jsonb", sql_string_literal(&sort_key));
    Ok(match sort {
        ObjectGroupSort::DimensionsAscending => format!("{sort_key_sql} > {sort_key}"),
        ObjectGroupSort::DimensionsDescending => format!("{sort_key_sql} < {sort_key}"),
        ObjectGroupSort::ObjectCountAscending => format!(
            "(COUNT(*) > {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
        ObjectGroupSort::ObjectCountDescending => format!(
            "(COUNT(*) < {count} OR (COUNT(*) = {count} AND {sort_key_sql} > {sort_key}))",
            count = cursor.object_count,
        ),
    })
}

fn append_cursor_clause(
    spec: &mut ObjectGroupSqlSpec,
    sort: ObjectGroupSort,
    cursor: crate::models::object_group::DecodedObjectGroupCursor,
) {
    spec.sql.push_str("\nWHERE ");
    match sort {
        ObjectGroupSort::DimensionsAscending => {
            spec.sql.push_str("sort_key > ?::jsonb");
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::DimensionsDescending => {
            spec.sql.push_str("sort_key < ?::jsonb");
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::ObjectCountAscending => {
            spec.sql
                .push_str("(object_count > ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
        ObjectGroupSort::ObjectCountDescending => {
            spec.sql
                .push_str("(object_count < ? OR (object_count = ? AND sort_key > ?::jsonb))");
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds
                .push(ObjectGroupBindValue::BigInt(cursor.object_count));
            spec.binds.push(ObjectGroupBindValue::Json(cursor.sort_key));
        }
    }
}

const fn order_clause(sort: ObjectGroupSort) -> &'static str {
    match sort {
        ObjectGroupSort::DimensionsAscending => "sort_key ASC",
        ObjectGroupSort::DimensionsDescending => "sort_key DESC",
        ObjectGroupSort::ObjectCountAscending => "object_count ASC, sort_key ASC",
        ObjectGroupSort::ObjectCountDescending => "object_count DESC, sort_key ASC",
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn json_dimension_sql_distinguishes_value_null_and_missing() {
        let dimension = ObjectGroupDimension::from_str("json_data.location,country").unwrap();
        let (state, value) = dimension_sql(0, &dimension);
        assert!(state.contains("IS NULL THEN 2"));
        assert!(state.contains("= 'null'::jsonb THEN 1"));
        assert!(value.contains("COALESCE"));
    }

    #[test]
    fn count_sort_always_uses_complete_dimension_tie_breaker() {
        assert_eq!(
            order_clause(ObjectGroupSort::ObjectCountDescending),
            "object_count DESC, sort_key ASC"
        );
    }
}
