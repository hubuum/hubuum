use crate::storage::postgres::operations::ClassRelation;
use crate::storage::postgres::prelude::*;

pub use crate::config::max_transitive_depth as max_transitive_depth_from_config;

use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, ParsedQueryParamExt, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationID, HubuumClassRelationTransitive,
    HubuumObject, HubuumObjectRelation, HubuumObjectRelationID, HubuumObjectTransitiveLink, User,
};
use crate::storage::postgres::operations::collection::user_can_on_any_from_backend;
use crate::storage::postgres::operations::object::HubuumObjectRow;
use crate::storage::postgres::operations::relation_rows::{
    HubuumClassRelationRow, HubuumClassRelationTransitiveRow, HubuumObjectRelationRow,
    HubuumObjectTransitiveLinkRow,
};
use crate::storage::postgres::with_connection;
use crate::{
    apply_query_options, bind_transitive_filter_params, date_search, numeric_search,
    revision_search, string_search, trace_query,
};

use crate::traits::{GroupAccessors, SelfAccessors};

use super::{ObjectRelationsFromUser, Relations, SelfRelations};

fn class_relations_from_rows(
    rows: Vec<HubuumClassRelationRow>,
) -> Result<Vec<HubuumClassRelation>, ApiError> {
    rows.into_iter().map(TryInto::try_into).collect()
}

impl<C1> SelfRelations<HubuumClass> for C1 where C1: SelfAccessors<HubuumClass> + Clone + Send + Sync
{}

#[derive(Debug, Clone, Default)]
pub struct TransitiveFilterParams {
    pub depth_op: Option<String>,
    pub depth_values: Option<Vec<i32>>,
    pub depth_negated: bool,
    pub path_op: Option<String>,
    pub path_values: Option<Vec<i32>>,
    pub path_negated: bool,
}

fn parse_depth_filter(param: &ParsedQueryParam) -> Result<(String, Vec<i32>, bool), ApiError> {
    use crate::models::search::{DataType, Operator};

    if !param.operator.is_applicable_to(DataType::NumericOrDate) {
        return Err(ApiError::OperatorMismatch(format!(
            "Operator '{:?}' is not applicable to field '{}'",
            param.operator, param.field
        )));
    }

    let values = param.value_as_integer()?;
    if values.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Searching on field '{}' requires a value",
            param.field
        )));
    }

    let (op, negated) = param.operator.op_and_neg();
    let op_name = match op {
        Operator::Equals => "equals",
        Operator::Gt => "gt",
        Operator::Gte => "gte",
        Operator::Lt => "lt",
        Operator::Lte => "lte",
        Operator::Between => {
            if values.len() != 2 {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator 'between' requires 2 values (min,max) for field '{}'",
                    param.field
                )));
            }
            "between"
        }
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                param.operator, param.field
            )));
        }
    };

    Ok((op_name.to_string(), values, negated))
}

fn parse_path_filter(param: &ParsedQueryParam) -> Result<(String, Vec<i32>, bool), ApiError> {
    use crate::models::search::{DataType, Operator};

    if !param.operator.is_applicable_to(DataType::Array) {
        return Err(ApiError::OperatorMismatch(format!(
            "Operator '{:?}' is not applicable to field '{}'",
            param.operator, param.field
        )));
    }

    let values = param.value_as_integer()?;
    if values.is_empty() {
        return Err(ApiError::BadRequest(format!(
            "Searching on field '{}' requires a value",
            param.field
        )));
    }

    let (op, negated) = param.operator.op_and_neg();
    let op_name = match op {
        Operator::Contains => "contains",
        Operator::Equals => "equals",
        _ => {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{:?}' not implemented for field '{}' (type: array)",
                param.operator, param.field
            )));
        }
    };

    Ok((op_name.to_string(), values, negated))
}

pub fn parse_transitive_filter_params(
    query_options: &QueryOptions,
) -> Result<TransitiveFilterParams, ApiError> {
    let mut params = TransitiveFilterParams::default();

    for param in query_options.filters() {
        match param.field {
            FilterField::Depth => {
                if params.depth_op.is_some() {
                    return Err(ApiError::BadRequest(
                        "Multiple depth filters are not supported for transitive class relations"
                            .to_string(),
                    ));
                }
                let (op_name, values, negated) = parse_depth_filter(param)?;
                params.depth_op = Some(op_name);
                params.depth_values = Some(values);
                params.depth_negated = negated;
            }
            FilterField::Path => {
                if params.path_op.is_some() {
                    return Err(ApiError::BadRequest(
                        "Multiple path filters are not supported for transitive class relations"
                            .to_string(),
                    ));
                }

                let (op_name, values, negated) = parse_path_filter(param)?;
                params.path_op = Some(op_name);
                params.path_values = Some(values);
                params.path_negated = negated;
            }
            FilterField::ClassFrom | FilterField::ClassTo => {
                // These are constrained by the caller in this trait module.
            }
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for transitive class relations",
                    param.field
                )));
            }
        }
    }

    Ok(params)
}

pub trait SelfRelationsBackend {
    async fn transitive_relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    async fn relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;

    async fn search_relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;
}

impl<T> SelfRelationsBackend for T
where
    T: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn transitive_relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use diesel::sql_query;
        use diesel::sql_types::Integer;

        let rows = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitiveRow>(
                sql_query(
                    "SELECT ancestor_class_id, descendant_class_id, depth, path
                     FROM get_bidirectionally_related_classes($1, ARRAY[]::INT[], $2)
                     WHERE ancestor_class_id = $1 OR descendant_class_id = $1
                     ORDER BY depth ASC, descendant_class_id ASC",
                )
                .bind::<Integer, _>(self.id())
                .bind::<Integer, _>(max_transitive_depth_from_config()),
                conn,
            )
            .await
        })
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let rows = with_connection(pool, async |conn| {
            hubuumclass_relation
                .or_filter(from_hubuum_class_id.eq(self.id()))
                .or_filter(to_hubuum_class_id.eq(self.id()))
                .load::<HubuumClassRelationRow>(conn)
                .await
        })
        .await?;
        class_relations_from_rows(rows)
    }

    async fn search_relations_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let query_params = query_options.filters().clone();
        let mut base_query = hubuumclass_relation.into_boxed();
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => {
                    numeric_search!(base_query, param, operator, id)
                }
                FilterField::ClassFrom => {
                    numeric_search!(base_query, param, operator, from_hubuum_class_id)
                }
                FilterField::ClassTo => {
                    numeric_search!(base_query, param, operator, to_hubuum_class_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, revision)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )));
                }
            }
        }

        apply_query_options!(base_query, query_options, HubuumClassRelationRow);

        trace_query!(base_query, "Searching relations");

        let rows = with_connection(pool, async |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelationRow>(conn)
                .await
        })
        .await?;
        class_relations_from_rows(rows)
    }
}

impl<C1, C2> Relations<C1, C2> for C1
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations_between(
        pool: &crate::storage::postgres::PostgresPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations(pool, from, to).await
    }
}

impl<C1, C2> ClassRelation<C1, C2> for C1
where
    C1: SelfAccessors<HubuumClass> + Relations<C1, C2> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations_to(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        other: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        <C1 as Relations<C1, C2>>::relations_between(pool, self, other).await
    }

    async fn relations_to_paginated(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        other: &C2,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations_paginated(pool, self, other, query_options).await
    }

    async fn direct_relation_to(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        other: &C2,
    ) -> Result<Option<HubuumClassRelation>, ApiError> {
        fetch_relations_direct(pool, self, other)
            .await
            .map(Some)
            .or(Ok(None))
    }
}

impl<C1, C2> Relations<C1, C2> for HubuumClassRelationTransitive
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations_between(
        pool: &crate::storage::postgres::PostgresPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations(pool, from, to).await
    }
}

async fn fetch_relations_direct<C1, C2>(
    pool: &crate::storage::postgres::PostgresPool,
    from: &C1,
    to: &C2,
) -> Result<HubuumClassRelation, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use crate::schema::hubuumclass_relation::dsl::*;
    use crate::storage::postgres::prelude::*;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    let row = with_connection(pool, async |conn| {
        diesel_async::RunQueryDsl::first::<HubuumClassRelationRow>(
            hubuumclass_relation
                .filter(from_hubuum_class_id.eq(from))
                .filter(to_hubuum_class_id.eq(to)),
            conn,
        )
        .await
    })
    .await?;
    row.try_into()
}

async fn fetch_relations<C1, C2>(
    pool: &crate::storage::postgres::PostgresPool,
    from: &C1,
    to: &C2,
) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use diesel::sql_query;
    use diesel::sql_types::Integer;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    let rows = with_connection(pool, async |conn| {
        diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitiveRow>(
            sql_query(
                "SELECT ancestor_class_id, descendant_class_id, depth, path
                 FROM get_bidirectionally_related_classes($1, ARRAY[]::INT[], $2)
                 WHERE ancestor_class_id = $3 AND descendant_class_id = $4
                 ORDER BY depth ASC, descendant_class_id ASC",
            )
            .bind::<Integer, _>(from)
            .bind::<Integer, _>(max_transitive_depth_from_config())
            .bind::<Integer, _>(from)
            .bind::<Integer, _>(to),
            conn,
        )
        .await
    })
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

async fn fetch_relations_paginated<C1, C2>(
    pool: &crate::storage::postgres::PostgresPool,
    from: &C1,
    to: &C2,
    query_options: &QueryOptions,
) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use crate::pagination::{cursor_filter_sql, normalized_sorts, order_sql_clause};
    use diesel::sql_query;
    use diesel::sql_types::Integer;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    let filter = parse_transitive_filter_params(query_options)?;
    let sorts = normalized_sorts::<HubuumClassRelationTransitiveRow>(query_options.sort())?;
    let mut raw_sql = String::from(
        "SELECT ancestor_class_id, descendant_class_id, depth, path
         FROM get_bidirectionally_related_classes(
             $1, ARRAY[]::INT[], $2, $3, $4, $5, $6, $7, $8
         )
         WHERE ancestor_class_id = $9 AND descendant_class_id = $10",
    );

    if let Some(cursor_sql) = cursor_filter_sql::<HubuumClassRelationTransitiveRow>(
        &sorts,
        query_options.cursor().map(|cursor| cursor.as_str()),
    )? {
        raw_sql.push_str("\n  AND ");
        raw_sql.push_str(&cursor_sql);
    }

    let order_by = sorts
        .iter()
        .map(order_sql_clause::<HubuumClassRelationTransitiveRow>)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    raw_sql.push_str(&format!("\nORDER BY {order_by}"));

    if let Some(limit) = query_options.limit() {
        raw_sql.push_str(&format!("\nLIMIT {limit}"));
    }

    let rows = with_connection(pool, async |conn| {
        let query = bind_transitive_filter_params!(
            sql_query(raw_sql.clone())
                .bind::<Integer, _>(from)
                .bind::<Integer, _>(max_transitive_depth_from_config()),
            filter
        );

        diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitiveRow>(
            query.bind::<Integer, _>(from).bind::<Integer, _>(to),
            conn,
        )
        .await
    })
    .await?;
    Ok(rows.into_iter().map(Into::into).collect())
}

impl<U> ObjectRelationsFromUser for U
where
    U: SelfAccessors<User> + GroupAccessors,
    for<'a> &'a U: GroupAccessors,
{
    async fn get_related_objects<O, C>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        source_object: &O,
        target_class: &C,
    ) -> Result<Vec<HubuumObjectTransitiveLink>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync,
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    {
        use crate::models::Permissions;
        use diesel::sql_query;
        use diesel::sql_types::{Array, Integer};

        // No token context on this internal traversal helper (production related-
        // object endpoints go through the scope-aware `objects_related_to_page`),
        // so this runs with full principal authority.
        let collections =
            user_can_on_any_from_backend(pool, self, Permissions::ReadObject, None).await?;
        let rows = with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectTransitiveLinkRow>(
                sql_query("SELECT * FROM get_transitively_linked_objects($1, $2, $3, $4)")
                    .bind::<Integer, _>(source_object.id())
                    .bind::<Integer, _>(target_class.id())
                    .bind::<Array<Integer>, _>(
                        collections.into_iter().map(|n| n.id()).collect::<Vec<_>>(),
                    )
                    .bind::<Integer, _>(max_transitive_depth_from_config()),
                conn,
            )
            .await
        })
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

pub trait ObjectRelationMembershipsBackend {
    async fn is_member_of_class_relation_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError>;

    async fn object_relation_from_backend<O, C>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class: &C,
        target_object: &O,
    ) -> Result<HubuumObjectRelation, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync;

    async fn related_objects_from_backend<C>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class: &C,
        query_params: &[ParsedQueryParam],
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync;
}

impl<T> ObjectRelationMembershipsBackend for T
where
    T: SelfAccessors<HubuumObject> + Clone + Send + Sync,
{
    async fn is_member_of_class_relation_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError> {
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        with_connection(pool, async |conn| {
            obj_rel::hubuumobject_relation
                .inner_join(class_rel::hubuumclass_relation)
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(self.id())
                        .or(obj_rel::to_hubuum_object_id.eq(self.id())),
                )
                .filter(class_rel::id.eq(class_relation.id))
                .select(obj_rel::id)
                .first::<i32>(conn)
                .await
                .optional()
        })
        .await
        .map(|result| result.is_some())
    }

    async fn object_relation_from_backend<O, C>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class: &C,
        target_object: &O,
    ) -> Result<HubuumObjectRelation, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync,
    {
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        let (from, to) = (self.id(), target_object.id());
        let (from, to) = if from > to { (to, from) } else { (from, to) };

        with_connection(pool, async |conn| {
            obj_rel::hubuumobject_relation
                .inner_join(class_rel::hubuumclass_relation)
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(from)
                        .and(obj_rel::to_hubuum_object_id.eq(to)),
                )
                .filter(
                    class_rel::from_hubuum_class_id
                        .eq(class.id())
                        .or(class_rel::to_hubuum_class_id.eq(class.id())),
                )
                .select(obj_rel::hubuumobject_relation::all_columns())
                .first::<HubuumObjectRelationRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }

    async fn related_objects_from_backend<C>(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
        class: &C,
        query_params: &[ParsedQueryParam],
    ) -> Result<Vec<HubuumObject>, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    {
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject::dsl as obj;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        let mut base_query = obj::hubuumobject.into_boxed();
        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, obj::created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, obj::updated_at)
                }
                FilterField::Revision => {
                    revision_search!(base_query, param, operator, obj::revision)
                }
                FilterField::Collections => {
                    numeric_search!(base_query, param, operator, obj::collection_id)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, obj::description)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, obj::name)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, async |conn| {
            base_query
                .inner_join(
                    obj_rel::hubuumobject_relation.on(obj::id
                        .eq(obj_rel::from_hubuum_object_id)
                        .or(obj::id.eq(obj_rel::to_hubuum_object_id))),
                )
                .inner_join(
                    class_rel::hubuumclass_relation
                        .on(obj_rel::class_relation_id.eq(class_rel::id)),
                )
                .filter(
                    obj_rel::from_hubuum_object_id
                        .eq(self.id())
                        .or(obj_rel::to_hubuum_object_id.eq(self.id())),
                )
                .filter(
                    class_rel::from_hubuum_class_id
                        .eq(class.id())
                        .or(class_rel::to_hubuum_class_id.eq(class.id())),
                )
                // Exclude self from results — we want the *other* objects
                .filter(obj::id.ne(self.id()))
                .select(obj::hubuumobject::all_columns())
                .distinct()
                .load::<HubuumObjectRow>(conn)
                .await
        })
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

pub trait LoadClassRelationRecord {
    async fn load_class_relation_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClassRelation, ApiError>;
}

impl LoadClassRelationRecord for HubuumClassRelationID {
    async fn load_class_relation_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        let row = with_connection(pool, async |conn| {
            hubuumclass_relation
                .filter(id.eq(self.id()))
                .first::<HubuumClassRelationRow>(conn)
                .await
        })
        .await?;
        row.try_into()
    }
}

pub trait LoadObjectRelationRecord {
    async fn load_object_relation_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObjectRelation, ApiError>;
}

impl LoadObjectRelationRecord for HubuumObjectRelationID {
    async fn load_object_relation_record(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, async |conn| {
            hubuumobject_relation
                .filter(id.eq(self.id()))
                .first::<HubuumObjectRelationRow>(conn)
                .await
        })
        .await
        .map(Into::into)
    }
}
