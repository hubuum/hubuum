mod accumulator;
mod authorization;
mod bounded_json;
mod candidate;
mod computed;
mod filters;
mod sql;

use hubuum_query::QueryOptions;
use hubuum_storage_core::{
    AuthorizationPermission, ObjectAggregateAuthorization, ObjectAggregateAuthorizer,
    ObjectAggregateStorageQuery, StorageObjectAggregateCursor, StorageObjectAggregatePage,
    StorageObjectAggregateSpec, StorageVisibility,
};

use crate::operations::computed_objects::query::{
    ComputedQuerySnapshot, resolve_computed_query_fields,
};
use crate::operations::visibility::authorized_collection_ids;
use crate::{PostgresRuntime, PostgresStorageError};

use self::accumulator::{
    ExternalAggregateAccumulator, create_aggregate_accumulator, merge_aggregate_rows,
    page_accumulated_aggregates, page_external_aggregates,
};
use self::authorization::DelegatedObjectAggregateAuthorization;
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
    runtime: &'a PostgresRuntime,
    target: ObjectAggregateRouteTarget,
    paging: ObjectAggregatePaging,
    personal_owner_id: Option<i32>,
    required_permissions: Vec<AuthorizationPermission>,
    visibility: StorageVisibility,
}

struct ObjectAggregatePaging {
    query_options: QueryOptions,
    spec: StorageObjectAggregateSpec,
    decoded_cursor: Option<StorageObjectAggregateCursor>,
    effective_limit: usize,
    cursor_max_encoded_bytes: usize,
    computed_filter_snapshot: Option<ComputedQuerySnapshot>,
}

impl ObjectAggregatePaging {
    fn has_computed_filter(&self) -> bool {
        self.query_options
            .filters()
            .iter()
            .any(|filter| filter.field.computed_query().is_some())
    }

    async fn resolve_computed_filters(
        &mut self,
        connection: &mut crate::PostgresConnection,
        class_id: i32,
        personal_owner_id: Option<i32>,
    ) -> Result<(), PostgresStorageError> {
        if !self.has_computed_filter() {
            return Ok(());
        }
        let mut no_sorts = Default::default();
        let snapshot = resolve_computed_query_fields(
            connection,
            class_id,
            personal_owner_id,
            self.query_options.filters_mut(),
            &mut no_sorts,
        )
        .await?;
        self.computed_filter_snapshot = Some(snapshot);
        Ok(())
    }
}

/// Aggregate visible objects without exposing PostgreSQL connections or query
/// construction to the application composition crate.
pub async fn aggregate_objects(
    runtime: &PostgresRuntime,
    query: ObjectAggregateStorageQuery,
    authorization: ObjectAggregateAuthorization<'_>,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    let target = query.target();
    let class_id = target.class_id().id();
    let class_name = target.class_name().to_string();
    let collection_id = target.collection_id().id();
    reject_unsupported_filters(query.options())?;

    let spec = query.spec().clone();
    let query_options = query.options().clone();
    let cursor_max_encoded_bytes = query.cursor_max_encoded_bytes();
    let effective_limit = query.page_limit();
    let decoded_cursor = query_options
        .cursor()
        .map(|cursor| cursor.as_str())
        .map(|cursor| spec.decode_cursor(cursor, cursor_max_encoded_bytes))
        .transpose()?;
    let required_permissions = query.required_permissions().to_vec();
    tracing::debug!(
        operation = "aggregate_objects",
        backend = "postgresql",
        dimension_count = spec.dimensions().len(),
        measure_count = spec.measures().len(),
        filter_count = query_options.filters().len(),
        include_total = query_options.include_total(),
        authorization = ?authorization,
        "grouping visible filtered objects"
    );
    let execution = ObjectAggregateExecution {
        runtime,
        target: ObjectAggregateRouteTarget {
            class_id,
            class_name,
            collection_id,
        },
        paging: ObjectAggregatePaging {
            query_options,
            spec,
            decoded_cursor,
            effective_limit,
            cursor_max_encoded_bytes,
            computed_filter_snapshot: None,
        },
        personal_owner_id: query.personal_owner_id().map(|id| id.id()),
        required_permissions,
        visibility: query.visibility().clone(),
    };

    match authorization {
        ObjectAggregateAuthorization::Storage => {
            aggregate_objects_with_local_authorization(execution).await
        }
        ObjectAggregateAuthorization::Delegated(authorizer) => {
            aggregate_visible_filtered_objects_with_external_batches(authorizer, execution).await
        }
    }
}

async fn aggregate_objects_with_local_authorization(
    execution: ObjectAggregateExecution<'_>,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    if !execution
        .visibility
        .allows_permissions(&execution.required_permissions)
    {
        return empty_aggregate_page(&execution.paging.query_options);
    }
    let runtime = execution.runtime;
    runtime
        .with_transaction(async move |connection| {
            let authorized = authorized_collection_ids(
                connection,
                &execution.visibility,
                &execution.required_permissions,
            )
            .await?;
            if !authorized.contains(&execution.target.collection_id) {
                tracing::debug!(
                    operation = "aggregate_objects",
                    backend = "postgresql",
                    authorization = "denied",
                    "object aggregate target is not visible"
                );
                return empty_aggregate_page(&execution.paging.query_options);
            }

            tracing::debug!(
                operation = "aggregate_objects",
                backend = "postgresql",
                authorization = "granted",
                "object aggregate target is visible"
            );
            let mut execution = execution;
            if execution.paging.has_computed_filter()
                && !local_computed_filter_has_visible_candidates(connection, &execution).await?
            {
                return empty_aggregate_page(&execution.paging.query_options);
            }
            execution
                .paging
                .resolve_computed_filters(
                    connection,
                    execution.target.class_id,
                    execution.personal_owner_id,
                )
                .await?;
            if !execution.paging.spec.has_computed_field() {
                return aggregate_visible_filtered_objects_with_sql(connection, execution).await;
            }
            aggregate_visible_filtered_objects_with_local_batches(connection, execution).await
        })
        .await
}

async fn local_computed_filter_has_visible_candidates(
    connection: &mut crate::PostgresConnection,
    execution: &ObjectAggregateExecution<'_>,
) -> Result<bool, PostgresStorageError> {
    let mut candidate_options =
        object_aggregate_authorization_chunk_options(&execution.paging.query_options);
    candidate_options.set_limit(Some(1));
    let database_options = candidate_execution_options(&candidate_options)?;
    let candidate_query = ObjectAggregateCandidateQuery::new(
        &database_options,
        execution.target.collection_id,
        &execution.paging.spec,
    )
    .resource_scope(execution.visibility.resources());
    let candidates = load_aggregate_candidate_batch(connection, candidate_query)
        .await?
        .into_page(&candidate_options)?;
    validate_candidate_target(&candidates.items, &execution.target)?;
    Ok(!candidates.items.is_empty())
}

async fn aggregate_visible_filtered_objects_with_local_batches(
    connection: &mut crate::PostgresConnection,
    execution: ObjectAggregateExecution<'_>,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    let ObjectAggregateExecution {
        runtime,
        target,
        paging,
        personal_owner_id,
        visibility,
        ..
    } = execution;
    create_aggregate_accumulator(connection).await?;
    let mut computed_definitions = None;
    let mut chunk_options = object_aggregate_chunk_options(&paging.query_options);
    let mut object_cursor = None;

    loop {
        chunk_options.set_validated_cursor(object_cursor.clone());
        let database_options = candidate_execution_options(&chunk_options)?;
        let candidate_query = ObjectAggregateCandidateQuery::new(
            &database_options,
            target.collection_id,
            &paging.spec,
        )
        .resource_scope(visibility.resources());
        let candidate_query = if let Some(snapshot) = paging.computed_filter_snapshot.as_ref() {
            candidate_query.resolved_computed_filters(snapshot)
        } else {
            candidate_query
        };
        let candidates = load_aggregate_candidate_batch(connection, candidate_query).await?;
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
            let plan = SnapshotAggregatePlan::new(runtime, &paging.spec, definitions);
            let grouped = aggregate_snapshot_rows(connection, candidate_page.items, plan).await?;
            merge_aggregate_rows(connection, grouped, &paging.spec).await?;
        }

        object_cursor = candidate_page
            .next_cursor
            .map(TryInto::try_into)
            .transpose()
            .map_err(|error: hubuum_query::QueryError| {
                PostgresStorageError::internal(error.to_string())
            })?;
        if object_cursor.is_none() {
            break;
        }
    }

    page_accumulated_aggregates(connection, &paging).await
}

async fn aggregate_visible_filtered_objects_with_external_batches(
    authorizer: &dyn ObjectAggregateAuthorizer,
    execution: ObjectAggregateExecution<'_>,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    if !execution
        .visibility
        .allows_permissions(&execution.required_permissions)
    {
        return empty_aggregate_page(&execution.paging.query_options);
    }

    let ObjectAggregateExecution {
        runtime,
        target,
        mut paging,
        personal_owner_id,
        visibility,
        required_permissions,
    } = execution;
    let authorizer = DelegatedObjectAggregateAuthorization::new(authorizer, required_permissions);

    // Candidate paging, computed-definition resolution, aggregation, and final
    // page construction must observe one native snapshot. The delegated policy
    // calls therefore run while this read-only transaction remains open. The
    // temporary accumulator keeps memory bounded and is the only state mutated
    // by the transaction.
    runtime
        .with_read_only_snapshot(async move |connection| {
            let authorization_target = authorizer
                .load_authorization_target(connection, &target)
                .await?;
            if !authorizer.authorize_target(authorization_target).await? {
                return empty_aggregate_page(&paging.query_options);
            }

            let mut computed_definitions =
                (!paging.spec.has_computed_field()).then(ComputedAggregateDefinitions::default);
            let mut accumulator = ExternalAggregateAccumulator::default();
            let filters_computed_values = paging.has_computed_filter();
            let mut chunk_options =
                object_aggregate_authorization_chunk_options(&paging.query_options);
            let mut object_cursor = None;
            loop {
                chunk_options.set_validated_cursor(object_cursor.clone());
                let database_options = candidate_execution_options(&chunk_options)?;
                let candidate_query = ObjectAggregateCandidateQuery::new(
                    &database_options,
                    target.collection_id,
                    &paging.spec,
                )
                .resource_scope(visibility.resources());
                let candidate_query = if filters_computed_values {
                    candidate_query.include_computed_filter_data()
                } else {
                    candidate_query
                };
                let candidates =
                    load_aggregate_candidate_batch(connection, candidate_query).await?;
                let candidate_page = candidates.into_page(&chunk_options)?;
                validate_candidate_target(&candidate_page.items, &target)?;
                let authorized = authorizer.authorize_objects(candidate_page.items).await?;

                if !authorized.is_empty()
                    && filters_computed_values
                    && paging.computed_filter_snapshot.is_none()
                {
                    paging
                        .resolve_computed_filters(connection, target.class_id, personal_owner_id)
                        .await?;
                }
                if !authorized.is_empty() && computed_definitions.is_none() {
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
                    let plan = SnapshotAggregatePlan::new(runtime, &paging.spec, definitions);
                    let plan = if let Some(snapshot) = paging.computed_filter_snapshot.as_ref() {
                        plan.computed_filters(&paging.query_options, snapshot)
                    } else {
                        plan
                    };
                    let grouped = aggregate_snapshot_rows(connection, authorized, plan).await?;
                    accumulator
                        .add_rows(connection, grouped, &paging.spec)
                        .await?;
                }

                object_cursor = candidate_page
                    .next_cursor
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|error: hubuum_query::QueryError| {
                        PostgresStorageError::internal(error.to_string())
                    })?;
                if object_cursor.is_none() {
                    break;
                }
            }

            let groups = accumulator.finish(connection, &paging.spec).await?;
            if groups.is_empty() {
                return empty_aggregate_page(&paging.query_options);
            }
            page_external_aggregates(connection, groups, &paging).await
        })
        .await
}

fn object_aggregate_chunk_options(query_options: &QueryOptions) -> QueryOptions {
    let mut chunk_options = query_options.clone();
    chunk_options.set_sort(
        candidate::candidate_sorts()
            .try_into()
            .expect("the fixed aggregate candidate sort must be valid"),
    );
    chunk_options.set_limit(Some(OBJECT_AGGREGATE_CANDIDATE_BATCH_SIZE));
    chunk_options.clear_cursor();
    chunk_options.set_include_total(false);
    chunk_options
}

fn candidate_execution_options(
    query_options: &QueryOptions,
) -> Result<QueryOptions, PostgresStorageError> {
    let limit = query_options.limit().ok_or_else(|| {
        PostgresStorageError::internal("aggregate candidate query is missing its limit")
    })?;
    if limit == 0 {
        return Err(PostgresStorageError::invalid_input(
            "aggregate candidate query limit must be positive",
        ));
    }
    let mut options = query_options.clone();
    options.set_limit(Some(limit.saturating_add(1)));
    Ok(options)
}

fn object_aggregate_authorization_chunk_options(query_options: &QueryOptions) -> QueryOptions {
    let mut chunk_options = object_aggregate_chunk_options(query_options);
    chunk_options
        .filters_mut()
        .try_retain(|filter| filter.field.computed_query().is_none())
        .expect("removing computed filters preserves related-filter invariants");
    chunk_options
}

fn empty_aggregate_page(
    query_options: &QueryOptions,
) -> Result<StorageObjectAggregatePage, PostgresStorageError> {
    Ok(StorageObjectAggregatePage::try_new(
        Vec::new(),
        query_options.include_total().then_some(0),
        None,
    )?)
}

fn validate_candidate_target(
    candidates: &[ObjectAggregateCandidate],
    target: &ObjectAggregateRouteTarget,
) -> Result<(), PostgresStorageError> {
    if candidates.iter().any(|object| {
        object.hubuum_class_id != target.class_id || object.collection_id != target.collection_id
    }) {
        return Err(PostgresStorageError::database(
            "Object aggregate candidates do not belong to the requested class and collection",
        ));
    }
    Ok(())
}

fn reject_unsupported_filters(options: &QueryOptions) -> Result<(), PostgresStorageError> {
    if let Some(field) = options
        .filters()
        .iter()
        .map(|filter| &filter.field)
        .find(|field| field.related_query().is_some())
    {
        return Err(PostgresStorageError::invalid_input(format!(
            "Field '{field}' isn't searchable (or does not exist) for object aggregates"
        )));
    }
    Ok(())
}
