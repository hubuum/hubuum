mod accumulator;
mod authorization;
mod bounded_json;
mod candidate;
mod computed;
mod filters;
mod sql;

use super::{UserCollectionAccessors, UserPermissions};
use crate::db::traits::authz::scope_allows;
use crate::db::traits::computed_field::{ComputedQuerySnapshot, resolve_computed_query_fields};
use crate::db::{DbPool, with_connection, with_transaction};
use crate::errors::ApiError;
use crate::models::object::HubuumObject;
use crate::models::object_aggregate::{
    DecodedObjectAggregateCursor, ObjectAggregateBackendParts, ObjectAggregateBackendRequest,
    ObjectAggregateCursorBudget, ObjectAggregateDimension, ObjectAggregatePage,
    ObjectAggregateSpec,
};
use crate::models::search::{FilterField, QueryOptions, SortParam};
use crate::models::{CollectionID, Permissions, TokenScope, UserID};
use crate::pagination::{
    SKIPPED_TOTAL_COUNT, count_query_options, effective_page_limit, prepare_db_pagination,
};
use crate::permissions::PrincipalRef;
use crate::traits::BackendContext;

use self::accumulator::{
    ExternalAggregateAccumulator, create_aggregate_accumulator, merge_aggregate_rows,
    page_accumulated_aggregates, page_external_aggregates,
};
use self::authorization::{ExternalObjectAggregateAuthorizer, ObjectAggregatePermissionResources};
use self::candidate::{
    ObjectAggregateCandidate, ObjectAggregateCandidateQuery, load_aggregate_candidate_batch,
};
use self::computed::{ComputedAggregateDefinitions, load_computed_aggregate_definitions};
use self::sql::{
    SnapshotAggregatePlan, aggregate_snapshot_rows, aggregate_visible_filtered_objects_with_sql,
};

#[cfg(not(feature = "integration-test-support"))]
const OBJECT_AGGREGATE_CANDIDATE_BATCH_SIZE: usize = 500;
#[cfg(feature = "integration-test-support")]
const OBJECT_AGGREGATE_CANDIDATE_BATCH_SIZE: usize = 2;

#[derive(Debug)]
struct ObjectAggregateRouteTarget {
    class_id: i32,
    class_name: String,
    collection_id: i32,
}

struct ObjectAggregateExecution<'a> {
    pool: &'a DbPool,
    target: ObjectAggregateRouteTarget,
    paging: ObjectAggregatePaging,
    personal_owner_id: Option<i32>,
    required_permissions: Vec<Permissions>,
    token_scopes: Option<&'a TokenScope>,
}

struct ObjectAggregatePaging {
    query_options: QueryOptions,
    spec: ObjectAggregateSpec,
    decoded_cursor: Option<DecodedObjectAggregateCursor>,
    effective_limit: usize,
    cursor_budget: ObjectAggregateCursorBudget,
    computed_filter_snapshot: Option<ComputedQuerySnapshot>,
}

impl ObjectAggregatePaging {
    fn has_computed_filter(&self) -> bool {
        self.query_options
            .filters
            .iter()
            .any(|filter| filter.field.computed_query().is_some())
    }

    async fn resolve_computed_filters(
        &mut self,
        pool: &DbPool,
        class_id: i32,
        personal_owner_id: Option<i32>,
    ) -> Result<(), ApiError> {
        if !self.has_computed_filter() {
            return Ok(());
        }
        let mut no_sorts = [];
        let snapshot = resolve_computed_query_fields(
            pool,
            class_id,
            personal_owner_id,
            &mut self.query_options.filters,
            &mut no_sorts,
        )
        .await?;
        self.computed_filter_snapshot = Some(snapshot);
        Ok(())
    }
}

pub trait ObjectAggregateBackend: UserCollectionAccessors {
    async fn aggregate_objects_from_backend<C>(
        &self,
        context: &C,
        request: ObjectAggregateBackendRequest,
    ) -> Result<ObjectAggregatePage, ApiError>
    where
        C: BackendContext + ?Sized,
    {
        let ObjectAggregateBackendParts {
            target,
            query_options,
            spec,
            personal_owner_id,
            authorization,
            cursor_budget,
        } = request.into_parts();
        let (class_id, class_name, collection_id) = target.into_parts();
        let (required_permissions, token_scopes) = authorization.into_parts();
        let effective_limit = effective_page_limit(&query_options)?;
        let decoded_cursor = query_options
            .cursor
            .as_deref()
            .map(|cursor| spec.decode_cursor(cursor, cursor_budget))
            .transpose()?;
        tracing::debug!(
            message = "Grouping visible filtered objects",
            user_id = self.principal_id(),
            dimensions = ?spec
                .dimensions()
                .iter()
                .map(ObjectAggregateDimension::canonical)
                .collect::<Vec<_>>()
        );

        let execution = ObjectAggregateExecution {
            pool: context.db_pool(),
            target: ObjectAggregateRouteTarget {
                class_id: class_id.id(),
                class_name,
                collection_id: collection_id.id(),
            },
            paging: ObjectAggregatePaging {
                query_options,
                spec,
                decoded_cursor,
                effective_limit,
                cursor_budget,
                computed_filter_snapshot: None,
            },
            personal_owner_id: personal_owner_id.map(UserID::id),
            required_permissions,
            token_scopes: token_scopes.as_ref(),
        };

        let permission_backend = context.permission_backend();
        let sql_visibility_pushdown =
            permission_backend.is_none_or(|backend| backend.supports_sql_visibility_pushdown());
        if sql_visibility_pushdown {
            return aggregate_objects_with_local_authorization(self, execution).await;
        }
        let backend = permission_backend.ok_or_else(|| {
            ApiError::InternalServerError(
                "External object aggregation requires a permission backend".to_string(),
            )
        })?;
        aggregate_visible_filtered_objects_with_external_batches(self, backend, execution).await
    }
}

impl<T> ObjectAggregateBackend for T where T: UserCollectionAccessors + ?Sized {}

async fn aggregate_objects_with_local_authorization<U>(
    user: &U,
    mut execution: ObjectAggregateExecution<'_>,
) -> Result<ObjectAggregatePage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    if !scope_allows(execution.token_scopes, &execution.required_permissions) {
        return empty_aggregate_page(&execution.paging.query_options);
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
        Err(ApiError::Forbidden(_)) => {
            return empty_aggregate_page(&execution.paging.query_options);
        }
        Err(error) => return Err(error),
    }

    if execution.paging.has_computed_filter()
        && !local_computed_filter_has_visible_candidates(&execution).await?
    {
        return empty_aggregate_page(&execution.paging.query_options);
    }
    execution
        .paging
        .resolve_computed_filters(
            execution.pool,
            execution.target.class_id,
            execution.personal_owner_id,
        )
        .await?;
    if !execution.paging.spec.has_computed_field() {
        return aggregate_visible_filtered_objects_with_sql(execution).await;
    }
    aggregate_visible_filtered_objects_with_local_batches(execution).await
}

async fn local_computed_filter_has_visible_candidates(
    execution: &ObjectAggregateExecution<'_>,
) -> Result<bool, ApiError> {
    let mut candidate_options =
        object_aggregate_authorization_chunk_options(&execution.paging.query_options);
    candidate_options.limit = Some(1);
    let database_options = prepare_db_pagination::<HubuumObject>(&candidate_options)?;
    let candidate_query = ObjectAggregateCandidateQuery::new(
        &database_options,
        execution.target.collection_id,
        &execution.paging.spec,
    )
    .token_scope(execution.token_scopes);
    let candidates = with_connection(execution.pool, async |connection| {
        load_aggregate_candidate_batch(connection, candidate_query).await
    })
    .await?
    .into_page(&candidate_options)?;
    validate_candidate_target(&candidates.items, &execution.target)?;
    Ok(!candidates.items.is_empty())
}

async fn aggregate_visible_filtered_objects_with_local_batches(
    execution: ObjectAggregateExecution<'_>,
) -> Result<ObjectAggregatePage, ApiError> {
    let ObjectAggregateExecution {
        pool,
        target,
        paging,
        personal_owner_id,
        token_scopes,
        ..
    } = execution;
    with_transaction(
        pool,
        async |connection| -> Result<ObjectAggregatePage, ApiError> {
            create_aggregate_accumulator(connection).await?;
            let mut computed_definitions = None;
            let mut chunk_options = object_aggregate_chunk_options(&paging.query_options);
            let mut object_cursor = None;

            loop {
                chunk_options.cursor.clone_from(&object_cursor);
                let database_options = prepare_db_pagination::<HubuumObject>(&chunk_options)?;
                let candidate_query = ObjectAggregateCandidateQuery::new(
                    &database_options,
                    target.collection_id,
                    &paging.spec,
                )
                .token_scope(token_scopes);
                let candidate_query =
                    if let Some(snapshot) = paging.computed_filter_snapshot.as_ref() {
                        candidate_query.resolved_computed_filters(snapshot)
                    } else {
                        candidate_query
                    };
                let candidates =
                    load_aggregate_candidate_batch(connection, candidate_query).await?;
                let candidate_page = candidates.into_page(&chunk_options)?;
                validate_candidate_target(&candidate_page.items, &target)?;
                if !candidate_page.items.is_empty() && computed_definitions.is_none() {
                    computed_definitions = Some(
                        load_computed_aggregate_definitions(
                            connection,
                            target.class_id,
                            &paging.spec,
                            personal_owner_id,
                            paging.computed_filter_snapshot.as_ref(),
                        )
                        .await?,
                    );
                }
                if let Some(definitions) = computed_definitions.as_ref() {
                    let plan = SnapshotAggregatePlan::new(&paging.spec, definitions);
                    let grouped =
                        aggregate_snapshot_rows(connection, candidate_page.items, plan).await?;
                    merge_aggregate_rows(connection, grouped, &paging.spec).await?;
                }

                object_cursor = candidate_page.next_cursor;
                if object_cursor.is_none() {
                    break;
                }
            }

            page_accumulated_aggregates(connection, &paging).await
        },
    )
    .await
}

async fn aggregate_visible_filtered_objects_with_external_batches<U>(
    user: &U,
    backend: &dyn crate::permissions::PermissionBackend,
    execution: ObjectAggregateExecution<'_>,
) -> Result<ObjectAggregatePage, ApiError>
where
    U: UserCollectionAccessors + ?Sized,
{
    if !scope_allows(execution.token_scopes, &execution.required_permissions) {
        return empty_aggregate_page(&execution.paging.query_options);
    }

    let principal = PrincipalRef::load(execution.pool, user).await?;
    let resources = with_connection(execution.pool, async |connection| {
        ObjectAggregatePermissionResources::load(connection, &execution.target).await
    })
    .await?;
    {
        let authorizer = ExternalObjectAggregateAuthorizer::new(
            backend,
            &principal,
            &execution.required_permissions,
            &resources,
        )?;
        if !authorizer.authorize_invariants().await? {
            return empty_aggregate_page(&execution.paging.query_options);
        }
    }
    let ObjectAggregateExecution {
        pool,
        target,
        mut paging,
        personal_owner_id,
        required_permissions,
        token_scopes,
    } = execution;
    let authorizer = ExternalObjectAggregateAuthorizer::new(
        backend,
        &principal,
        &required_permissions,
        &resources,
    )?;
    let mut computed_definitions =
        (!paging.spec.has_computed_field()).then(ComputedAggregateDefinitions::default);
    let mut accumulator = ExternalAggregateAccumulator::default();
    let filters_computed_values = paging.has_computed_filter();
    let mut chunk_options = object_aggregate_authorization_chunk_options(&paging.query_options);
    let mut object_cursor = None;

    loop {
        chunk_options.cursor.clone_from(&object_cursor);
        let database_options = prepare_db_pagination::<HubuumObject>(&chunk_options)?;
        let candidate_query = ObjectAggregateCandidateQuery::new(
            &database_options,
            target.collection_id,
            &paging.spec,
        )
        .token_scope(token_scopes);
        let candidate_query = if filters_computed_values {
            candidate_query.include_computed_filter_data()
        } else {
            candidate_query
        };
        let candidates = with_connection(pool, async |connection| {
            load_aggregate_candidate_batch(connection, candidate_query).await
        })
        .await?;
        let candidate_page = candidates.into_page(&chunk_options)?;
        validate_candidate_target(&candidate_page.items, &target)?;
        let authorized = authorizer.authorize(candidate_page.items).await?;

        if !authorized.is_empty()
            && filters_computed_values
            && paging.computed_filter_snapshot.is_none()
        {
            paging
                .resolve_computed_filters(pool, target.class_id, personal_owner_id)
                .await?;
        }
        if !authorized.is_empty() && computed_definitions.is_none() {
            computed_definitions = Some(
                with_connection(pool, async |connection| {
                    load_computed_aggregate_definitions(
                        connection,
                        target.class_id,
                        &paging.spec,
                        personal_owner_id,
                        paging.computed_filter_snapshot.as_ref(),
                    )
                    .await
                })
                .await?,
            );
        }
        if let Some(definitions) = computed_definitions.as_ref() {
            let plan = SnapshotAggregatePlan::new(&paging.spec, definitions);
            let plan = if let Some(snapshot) = paging.computed_filter_snapshot.as_ref() {
                plan.computed_filters(&paging.query_options, snapshot)
            } else {
                plan
            };
            let grouped = with_connection(pool, async |connection| {
                aggregate_snapshot_rows(connection, authorized, plan).await
            })
            .await?;
            accumulator.add_rows(pool, grouped, &paging.spec).await?;
        }

        object_cursor = candidate_page.next_cursor;
        if object_cursor.is_none() {
            break;
        }
    }

    let groups = accumulator.finish(pool, &paging.spec).await?;
    if groups.is_empty() {
        return empty_aggregate_page(&paging.query_options);
    }
    page_external_aggregates(pool, groups, &paging).await
}

fn object_aggregate_chunk_options(query_options: &QueryOptions) -> QueryOptions {
    let mut chunk_options = count_query_options(query_options);
    chunk_options.sort = vec![SortParam {
        field: FilterField::Id,
        descending: false,
    }];
    chunk_options.limit = Some(OBJECT_AGGREGATE_CANDIDATE_BATCH_SIZE);
    chunk_options.include_total = false;
    chunk_options
}

fn object_aggregate_authorization_chunk_options(query_options: &QueryOptions) -> QueryOptions {
    let mut chunk_options = object_aggregate_chunk_options(query_options);
    chunk_options
        .filters
        .retain(|filter| filter.field.computed_query().is_none());
    chunk_options
}

fn empty_aggregate_page(query_options: &QueryOptions) -> Result<ObjectAggregatePage, ApiError> {
    Ok(ObjectAggregatePage::new(
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
    candidates: &[ObjectAggregateCandidate],
    target: &ObjectAggregateRouteTarget,
) -> Result<(), ApiError> {
    if candidates.iter().any(|object| {
        object.hubuum_class_id != target.class_id || object.collection_id != target.collection_id
    }) {
        return Err(ApiError::InternalServerError(
            "Object aggregate candidates do not belong to the requested class and collection"
                .to_string(),
        ));
    }
    Ok(())
}
