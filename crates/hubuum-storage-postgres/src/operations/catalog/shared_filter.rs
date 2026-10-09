//! Share expensive JSON filtering between an exact count and a cursor page.

use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::prelude::QueryDsl;
use diesel::query_builder::{AstPass, Query, QueryFragment, QueryId};
use diesel::result::QueryResult;
use diesel::sql_types::{BigInt, Bool, Nullable};
use diesel_async::RunQueryDsl;
use hubuum_query::{FilterField, Operator, QueryOptions};
use hubuum_storage_core::{StorageObject, StoragePage};

use super::{object_cursor_field, object_cursor_fields};
use crate::cursor::{CursorTieBreaker, normalize_query_fields, order_sql_clause_for_field};
use crate::operations::object::ObjectRow;
use crate::schema::hubuumobject;
use crate::{PostgresConnection, PostgresStorageError};

pub(super) fn applies(options: &QueryOptions) -> bool {
    // ponytail: this is a workload heuristic, not a cost estimator. Multiple
    // JSON substring predicates benefited in the scan benchmarks; ordinary
    // indexed and single-predicate queries retain their independent plans.
    // Revisit with adapter-owned workload evidence when query hints land.
    options.include_total()
        && options.has_cursor()
        && options
            .filters()
            .iter()
            .filter(|filter| {
                filter.field == FilterField::JsonData
                    && matches!(
                        filter.operator.op_and_neg().0,
                        Operator::Contains | Operator::IContains
                    )
            })
            .take(2)
            .count()
            == 2
}

pub(super) async fn load_page(
    connection: &mut PostgresConnection,
    filtered: hubuumobject::BoxedQuery<'_, Pg>,
    options: &QueryOptions,
) -> Result<StoragePage<StorageObject>, PostgresStorageError> {
    tracing::debug!(
        operation = "list_objects",
        filter_count = options.filters().len(),
        sort_count = options.sort().len(),
        has_cursor = options.has_cursor(),
        include_total = true,
        shared_filters = true,
        "executing PostgreSQL catalog query"
    );
    let (options, fields) = normalize_query_fields(
        options,
        object_cursor_fields(options)?,
        CursorTieBreaker::new(
            FilterField::Id,
            false,
            object_cursor_field(&FilterField::Id)?,
        ),
    )?;
    let order = options
        .sort()
        .iter()
        .zip(&fields)
        .map(|(sort, field)| order_sql_clause_for_field(sort, field))
        .collect::<Vec<_>>()
        .join(", ");
    let mut page = hubuumobject::table
        .filter(sql::<Bool>(
            "hubuumobject.id IN (SELECT id FROM catalog_matches)",
        ))
        .into_boxed();
    crate::apply_query_options_with_fields!(
        page,
        options,
        fields,
        CursorTieBreaker::new(
            FilterField::Id,
            false,
            object_cursor_field(&FilterField::Id)?
        )
    );
    let rows = SharedObjectPage {
        matches: filtered.select(hubuumobject::id),
        page: page.select(hubuumobject::all_columns),
        order,
    }
    .load::<(Option<ObjectRow>, i64)>(connection)
    .await?;
    let total = rows
        .as_slice()
        .first()
        .map(|(_, total)| *total)
        .ok_or_else(|| {
            PostgresStorageError::internal("Shared object page did not return its total")
        })?;
    let objects = rows
        .into_iter()
        .filter_map(|(object, _)| object)
        .map(ObjectRow::into_storage)
        .collect::<Result<Vec<_>, _>>()?;
    crate::persisted_page(objects, Some(total))
}

struct SharedObjectPage<Matches, Page> {
    matches: Matches,
    page: Page,
    order: String,
}

impl<Matches, Page> Query for SharedObjectPage<Matches, Page> {
    type SqlType = (Nullable<hubuumobject::SqlType>, BigInt);
}

impl<Matches, Page> QueryId for SharedObjectPage<Matches, Page> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<Matches, Page> QueryFragment<Pg> for SharedObjectPage<Matches, Page>
where
    Matches: QueryFragment<Pg>,
    Page: QueryFragment<Pg>,
{
    fn walk_ast<'bind>(&'bind self, mut out: AstPass<'_, 'bind, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        // Keep only IDs in the shared result, not entire JSON documents. Count
        // before seeking the cursor, and retain the total when the page is empty.
        out.push_sql("WITH catalog_matches AS MATERIALIZED (");
        self.matches.walk_ast(out.reborrow())?;
        out.push_sql(
            ") SELECT hubuumobject.*, totals.total \
             FROM (SELECT COUNT(*) AS total FROM catalog_matches) totals LEFT JOIN (",
        );
        self.page.walk_ast(out.reborrow())?;
        out.push_sql(") AS hubuumobject ON TRUE ORDER BY ");
        out.push_sql(&self.order);
        Ok(())
    }
}
