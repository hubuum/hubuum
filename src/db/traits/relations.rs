use crate::db::traits::ClassRelation;
use diesel::prelude::*;

pub use crate::config::max_transitive_depth as max_transitive_depth_from_config;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationID, HubuumClassRelationTransitive,
    HubuumObject, HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID,
    HubuumObjectTransitiveLink, NewHubuumClassRelation, NewHubuumObjectRelation, User,
    user_can_on_any,
};
use crate::{
    bind_transitive_filter_params, date_search, numeric_search, string_search, trace_query,
};

use crate::traits::{GroupAccessors, SelfAccessors};

use super::{ObjectRelationsFromUser, Relations, SelfRelations};

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

    for param in &query_options.filters {
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
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    async fn relations_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;

    #[allow(dead_code)]
    async fn search_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;
}

impl<T> SelfRelationsBackend for T
where
    T: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn transitive_relations_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use diesel::prelude::*;
        use diesel::sql_query;
        use diesel::sql_types::Integer;


        with_connection(pool, |conn| {
            sql_query(
                "SELECT ancestor_class_id, descendant_class_id, depth, path
                 FROM get_bidirectionally_related_classes($1, ARRAY[]::INT[], $2)
                 WHERE ancestor_class_id = $1 OR descendant_class_id = $1
                 ORDER BY depth ASC, descendant_class_id ASC",
            )
            .bind::<Integer, _>(self.id())
            .bind::<Integer, _>(max_transitive_depth_from_config())
            .load::<HubuumClassRelationTransitive>(conn)
        })
    }

    async fn relations_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        with_connection(pool, |conn| {
            hubuumclass_relation
                .or_filter(from_hubuum_class_id.eq(self.id()))
                .or_filter(to_hubuum_class_id.eq(self.id()))
                .load::<HubuumClassRelation>(conn)
        })
    }

    async fn search_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let query_params = query_options.filters.clone();
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
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
        })
    }
}

impl<C1, C2> Relations<C1, C2> for C1
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations_between(
        pool: &DbPool,
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
        pool: &DbPool,
        other: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        <C1 as Relations<C1, C2>>::relations_between(pool, self, other).await
    }

    async fn relations_to_paginated(
        &self,
        pool: &DbPool,
        other: &C2,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations_paginated(pool, self, other, query_options).await
    }

    async fn direct_relation_to(
        &self,
        pool: &DbPool,
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
        pool: &DbPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations(pool, from, to).await
    }
}

const TRANSITIVE_RELATIONS_PAGINATED_SQL: &str = concat!(
    "SELECT ancestor_class_id, descendant_class_id, depth, path",
    " FROM get_bidirectionally_related_classes(",
    "     $1, ARRAY[]::INT[], $2, $3, $4, $5, $6, $7, $8",
    " )",
    " WHERE ancestor_class_id = $9 AND descendant_class_id = $10",
    " ORDER BY depth ASC, descendant_class_id ASC"
);

async fn fetch_relations_direct<C1, C2>(
    pool: &DbPool,
    from: &C1,
    to: &C2,
) -> Result<HubuumClassRelation, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use crate::schema::hubuumclass_relation::dsl::*;
    use diesel::prelude::*;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    with_connection(pool, |conn| {
        hubuumclass_relation
            .filter(from_hubuum_class_id.eq(from))
            .filter(to_hubuum_class_id.eq(to))
            .first::<HubuumClassRelation>(conn)
    })
}

#[allow(dead_code)]
async fn fetch_relations<C1, C2>(
    pool: &DbPool,
    from: &C1,
    to: &C2,
) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use diesel::prelude::*;
    use diesel::sql_query;
    use diesel::sql_types::Integer;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    with_connection(pool, |conn| {
        sql_query(
            "SELECT ancestor_class_id, descendant_class_id, depth, path
             FROM get_bidirectionally_related_classes($1, ARRAY[]::INT[], $2)
             WHERE ancestor_class_id = $3 AND descendant_class_id = $4
             ORDER BY depth ASC, descendant_class_id ASC",
        )
        .bind::<Integer, _>(from)
        .bind::<Integer, _>(max_transitive_depth_from_config())
        .bind::<Integer, _>(from)
        .bind::<Integer, _>(to)
        .load::<HubuumClassRelationTransitive>(conn)
    })
}

#[allow(dead_code)]
async fn fetch_relations_paginated<C1, C2>(
    pool: &DbPool,
    from: &C1,
    to: &C2,
    query_options: &QueryOptions,
) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    use diesel::prelude::*;
    use diesel::sql_query;
    use diesel::sql_types::Integer;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    let filter = parse_transitive_filter_params(query_options)?;

    with_connection(pool, |conn| {
        let query = bind_transitive_filter_params!(
            sql_query(TRANSITIVE_RELATIONS_PAGINATED_SQL)
                .bind::<Integer, _>(from)
                .bind::<Integer, _>(max_transitive_depth_from_config()),
            filter
        );

        query
            .bind::<Integer, _>(from)
            .bind::<Integer, _>(to)
            .load::<HubuumClassRelationTransitive>(conn)
    })
}

impl<U> ObjectRelationsFromUser for U
where
    U: SelfAccessors<User> + GroupAccessors,
    for<'a> &'a U: GroupAccessors,
{
    async fn get_related_objects<O, C>(
        &self,
        pool: &DbPool,
        source_object: &O,
        target_class: &C,
    ) -> Result<Vec<HubuumObjectTransitiveLink>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync,
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    {
        use crate::models::Permissions;
        use diesel::RunQueryDsl;
        use diesel::sql_query;
        use diesel::sql_types::{Array, Integer};

        let namespaces = user_can_on_any(pool, self, Permissions::ReadObject).await?;
        with_connection(pool, |conn| {
            sql_query("SELECT * FROM get_transitively_linked_objects($1, $2, $3, $4)")
                .bind::<Integer, _>(source_object.id())
                .bind::<Integer, _>(target_class.id())
                .bind::<Array<Integer>, _>(
                    namespaces.into_iter().map(|n| n.id()).collect::<Vec<_>>(),
                )
                .bind::<Integer, _>(max_transitive_depth_from_config())
                .load::<HubuumObjectTransitiveLink>(conn)
        })
    }
}

pub trait ObjectRelationMembershipsBackend {
    async fn is_member_of_class_relation_from_backend(
        &self,
        pool: &DbPool,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError>;

    async fn object_relation_from_backend<O, C>(
        &self,
        pool: &DbPool,
        class: &C,
        target_object: &O,
    ) -> Result<HubuumObjectRelation, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync;

    async fn related_objects_from_backend<C>(
        &self,
        pool: &DbPool,
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
        pool: &DbPool,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError> {
        use crate::schema::hubuumclass_relation::dsl as class_rel;
        use crate::schema::hubuumobject_relation::dsl as obj_rel;

        with_connection(pool, |conn| {
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
                .optional()
        })
        .map(|result| result.is_some())
    }

    async fn object_relation_from_backend<O, C>(
        &self,
        pool: &DbPool,
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

        with_connection(pool, |conn| {
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
                .first::<HubuumObjectRelation>(conn)
        })
    }

    async fn related_objects_from_backend<C>(
        &self,
        pool: &DbPool,
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
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, obj::namespace_id)
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

        with_connection(pool, |conn| {
            base_query
                .inner_join(
                    obj_rel::hubuumobject_relation.on(
                        obj::id
                            .eq(obj_rel::from_hubuum_object_id)
                            .or(obj::id.eq(obj_rel::to_hubuum_object_id)),
                    ),
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
                .load::<HubuumObject>(conn)
        })
    }
}

pub trait LoadClassRelationRecord {
    async fn load_class_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumClassRelation, ApiError>;
}

impl LoadClassRelationRecord for HubuumClassRelationID {
    async fn load_class_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, |conn| {
            hubuumclass_relation
                .filter(id.eq(self.0))
                .first::<HubuumClassRelation>(conn)
        })
    }
}

pub trait DeleteClassRelationRecord {
    async fn delete_class_relation_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteClassRelationRecord for HubuumClassRelation {
    async fn delete_class_relation_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumclass_relation.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

impl DeleteClassRelationRecord for HubuumClassRelationID {
    async fn delete_class_relation_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumclass_relation.filter(id.eq(self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait SaveClassRelationRecord {
    async fn save_class_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumClassRelation, ApiError>;
}

impl SaveClassRelationRecord for NewHubuumClassRelation {
    async fn save_class_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        if self.from_hubuum_class_id == self.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_class_id and to_hubuum_class_id cannot be the same".to_string(),
            ));
        }

        with_connection(pool, |conn| {
            diesel::insert_into(hubuumclass_relation)
                .values(self)
                .get_result(conn)
        })
    }
}

pub trait LoadObjectRelationRecord {
    async fn load_object_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObjectRelation, ApiError>;
}

impl LoadObjectRelationRecord for HubuumObjectRelationID {
    async fn load_object_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, |conn| {
            hubuumobject_relation
                .filter(id.eq(self.0))
                .first::<HubuumObjectRelation>(conn)
        })
    }
}

pub trait DeleteObjectRelationRecord {
    async fn delete_object_relation_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteObjectRelationRecord for HubuumObjectRelation {
    async fn delete_object_relation_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumobject_relation.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

impl DeleteObjectRelationRecord for HubuumObjectRelationID {
    async fn delete_object_relation_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumobject_relation.filter(id.eq(self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait SaveObjectRelationRecord {
    async fn save_object_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObjectRelation, ApiError>;
}

impl SaveObjectRelationRecord for NewHubuumObjectRelation {
    async fn save_object_relation_record(
        &self,
        pool: &DbPool,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }

        let obj1 = match HubuumObjectID(self.from_hubuum_object_id)
            .instance(pool)
            .await
        {
            Ok(obj1) => obj1,
            Err(_) => {
                return Err(ApiError::NotFound(
                    "from_hubuum_object_id not found".to_string(),
                ));
            }
        };

        let obj2 = match HubuumObjectID(self.to_hubuum_object_id)
            .instance(pool)
            .await
        {
            Ok(obj2) => obj2,
            Err(_) => {
                return Err(ApiError::NotFound(
                    "to_hubuum_object_id not found".to_string(),
                ));
            }
        };

        if obj1.hubuum_class_id == obj2.hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id must not have the same class"
                    .to_string(),
            ));
        }

        with_connection(pool, |conn| {
            diesel::insert_into(hubuumobject_relation)
                .values(self)
                .get_result(conn)
        })
    }
}
