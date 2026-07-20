mod accumulator;
mod authorization;
mod computed;
mod sql;

use super::{UserCollectionAccessors, UserPermissions};
use crate::db::traits::authz::scope_allows;
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::object::HubuumObject;
use crate::models::object_group::{
    DecodedObjectGroupCursor, ObjectGroupBackendParts, ObjectGroupBackendRequest,
    ObjectGroupDimension, ObjectGroupPage, ObjectGroupSpec,
};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{CollectionID, Permissions, UserID};
use crate::pagination::{
    SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, finalize_page,
    prepare_db_pagination,
};
use crate::permissions::PrincipalRef;
use crate::traits::BackendContext;

use self::accumulator::{
    ExternalGroupAccumulator, create_group_accumulator, merge_group_rows, page_accumulated_groups,
    page_external_groups,
};
use self::authorization::{ExternalObjectGroupAuthorizer, ObjectGroupPermissionResources};
use self::computed::{ComputedGroupDefinitions, load_computed_group_definitions};
use self::sql::{
    group_visible_filtered_objects_with_sql, grouped_snapshot_rows, load_group_candidate_batch,
};

#[cfg(not(feature = "integration-test-support"))]
const OBJECT_GROUP_CANDIDATE_BATCH_SIZE: usize = 500;
#[cfg(feature = "integration-test-support")]
const OBJECT_GROUP_CANDIDATE_BATCH_SIZE: usize = 2;

#[derive(Debug)]
struct ObjectGroupRouteTarget {
    class_id: i32,
    class_name: String,
    collection_id: i32,
}

struct ObjectGroupExecution<'a> {
    pool: &'a DbPool,
    target: ObjectGroupRouteTarget,
    query_options: &'a QueryOptions,
    spec: &'a ObjectGroupSpec,
    personal_owner_id: Option<i32>,
    required_permissions: Vec<Permissions>,
    token_scopes: Option<&'a [Permissions]>,
    decoded_cursor: Option<DecodedObjectGroupCursor>,
    effective_limit: usize,
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
            target,
            query_options,
            spec,
            personal_owner_id,
            authorization,
        } = request.into_parts();
        let (class_id, class_name, collection_id) = target.into_parts();
        let (required_permissions, token_scopes) = authorization.into_parts();
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

        let execution = ObjectGroupExecution {
            pool: context.db_pool(),
            target: ObjectGroupRouteTarget {
                class_id: class_id.id(),
                class_name,
                collection_id: collection_id.id(),
            },
            query_options: &query_options,
            spec: &spec,
            personal_owner_id: personal_owner_id.map(UserID::id),
            required_permissions,
            token_scopes: token_scopes.as_deref(),
            decoded_cursor,
            effective_limit,
        };

        let permission_backend = context.permission_backend();
        let sql_visibility_pushdown =
            permission_backend.is_none_or(|backend| backend.supports_sql_visibility_pushdown());
        if sql_visibility_pushdown {
            return group_objects_with_local_authorization(self, execution).await;
        }
        let backend = permission_backend.ok_or_else(|| {
            ApiError::InternalServerError(
                "External object grouping requires a permission backend".to_string(),
            )
        })?;
        group_visible_filtered_objects_with_external_batches(self, backend, execution).await
    }
}

impl<T> ObjectGroupBackend for T where T: UserCollectionAccessors + ?Sized {}

async fn group_objects_with_local_authorization<U>(
    user: &U,
    mut execution: ObjectGroupExecution<'_>,
) -> Result<ObjectGroupPage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    if !execution
        .required_permissions
        .contains(&Permissions::ReadCollection)
    {
        execution
            .required_permissions
            .push(Permissions::ReadCollection);
    }
    if !scope_allows(execution.token_scopes, &execution.required_permissions) {
        return empty_group_page(execution.query_options);
    }
    let collection = CollectionID::new(execution.target.collection_id)?;
    match user
        .can(
            execution.pool,
            execution.required_permissions.iter().copied(),
            [collection],
            execution.token_scopes,
        )
        .await
    {
        Ok(()) => {}
        Err(ApiError::Forbidden(_)) => return empty_group_page(execution.query_options),
        Err(error) => return Err(error),
    }

    if !execution.spec.has_computed_dimension() {
        return group_visible_filtered_objects_with_sql(execution).await;
    }
    group_visible_filtered_objects_with_local_batches(execution).await
}

async fn group_visible_filtered_objects_with_local_batches(
    execution: ObjectGroupExecution<'_>,
) -> Result<ObjectGroupPage, ApiError> {
    let ObjectGroupExecution {
        pool,
        target,
        query_options,
        spec,
        personal_owner_id,
        decoded_cursor,
        effective_limit,
        ..
    } = execution;
    with_transaction(
        pool,
        async |connection| -> Result<ObjectGroupPage, ApiError> {
            create_group_accumulator(connection).await?;
            let mut computed_definitions = None;
            let mut chunk_options = object_group_chunk_options(query_options);
            let mut object_cursor = None;

            loop {
                chunk_options.cursor.clone_from(&object_cursor);
                let database_options = prepare_db_pagination::<HubuumObject>(&chunk_options)?;
                let candidates =
                    load_group_candidate_batch(connection, &database_options, target.collection_id)
                        .await?;
                let candidate_page = finalize_page(candidates, &chunk_options)?;
                validate_candidate_target(&candidate_page.items, &target)?;
                if !candidate_page.items.is_empty() && computed_definitions.is_none() {
                    computed_definitions = Some(
                        load_computed_group_definitions(
                            connection,
                            target.class_id,
                            spec,
                            personal_owner_id,
                        )
                        .await?,
                    );
                }
                if let Some(definitions) = computed_definitions.as_ref() {
                    let grouped =
                        grouped_snapshot_rows(connection, candidate_page.items, spec, definitions)
                            .await?;
                    merge_group_rows(connection, grouped).await?;
                }

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

async fn group_visible_filtered_objects_with_external_batches<U>(
    user: &U,
    backend: &dyn crate::permissions::PermissionBackend,
    execution: ObjectGroupExecution<'_>,
) -> Result<ObjectGroupPage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    let ObjectGroupExecution {
        pool,
        target,
        query_options,
        spec,
        personal_owner_id,
        required_permissions,
        token_scopes,
        decoded_cursor,
        effective_limit,
    } = execution;
    if !scope_allows(token_scopes, &required_permissions) {
        return empty_group_page(query_options);
    }

    let principal = PrincipalRef::load(pool, user).await?;
    let resources = with_connection(pool, async |connection| {
        ObjectGroupPermissionResources::load(connection, &target).await
    })
    .await?;
    let authorizer =
        ExternalObjectGroupAuthorizer::new(backend, &principal, &required_permissions, &resources)?;
    let mut computed_definitions =
        (!spec.has_computed_dimension()).then(ComputedGroupDefinitions::default);
    let mut accumulator = ExternalGroupAccumulator::default();
    let mut chunk_options = object_group_chunk_options(query_options);
    let mut object_cursor = None;

    loop {
        chunk_options.cursor.clone_from(&object_cursor);
        let database_options = prepare_db_pagination::<HubuumObject>(&chunk_options)?;
        let candidates = with_connection(pool, async |connection| {
            load_group_candidate_batch(connection, &database_options, target.collection_id).await
        })
        .await?;
        let candidate_page = finalize_page(candidates, &chunk_options)?;
        validate_candidate_target(&candidate_page.items, &target)?;
        let authorized = authorizer.authorize(candidate_page.items).await?;

        if !authorized.is_empty() && computed_definitions.is_none() {
            computed_definitions = Some(
                with_transaction(pool, async |connection| {
                    load_computed_group_definitions(
                        connection,
                        target.class_id,
                        spec,
                        personal_owner_id,
                    )
                    .await
                })
                .await?,
            );
        }
        if let Some(definitions) = computed_definitions.as_ref() {
            let grouped = with_connection(pool, async |connection| {
                grouped_snapshot_rows(connection, authorized, spec, definitions).await
            })
            .await?;
            accumulator.add_rows(pool, grouped).await?;
        }

        object_cursor = candidate_page.next_cursor;
        if object_cursor.is_none() {
            break;
        }
    }

    let groups = accumulator.finish(pool).await?;
    if groups.is_empty() {
        return empty_group_page(query_options);
    }
    page_external_groups(
        pool,
        groups,
        query_options,
        spec,
        decoded_cursor,
        effective_limit,
    )
    .await
}

fn object_group_chunk_options(query_options: &QueryOptions) -> QueryOptions {
    let mut chunk_options = count_query_options(query_options);
    chunk_options.sort = vec![SortParam {
        field: FilterField::Id,
        descending: false,
    }];
    chunk_options.limit = Some(OBJECT_GROUP_CANDIDATE_BATCH_SIZE);
    chunk_options.include_total = false;
    chunk_options
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

fn validate_candidate_target(
    candidates: &[HubuumObject],
    target: &ObjectGroupRouteTarget,
) -> Result<(), ApiError> {
    if candidates.iter().any(|object| {
        object.hubuum_class_id != target.class_id || object.collection_id != target.collection_id
    }) {
        return Err(ApiError::InternalServerError(
            "Object group candidates do not belong to the requested class and collection"
                .to_string(),
        ));
    }
    Ok(())
}
