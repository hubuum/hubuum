use std::collections::HashMap;

use crate::storage::postgres::operations::ClassRelation;
use crate::storage::postgres::prelude::*;

pub use crate::config::max_transitive_depth as max_transitive_depth_from_config;

use crate::errors::ApiError;
use crate::events::{Action, EntityType, EventContext, NewEvent, emit_event};
use crate::models::search::{FilterField, ParsedQueryParam, ParsedQueryParamExt, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationID, HubuumClassRelationTransitive,
    HubuumObject, HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID,
    HubuumObjectTransitiveLink, NewHubuumClassRelation, NewHubuumObjectRelation,
    ObjectRelationCreateSelector, ObjectRelationCreateSelectorKind, ObjectRelationSelector,
    ObjectRelationSelectorKind, PreparedClassRelation, PreparedObjectRelation,
    ResolvedClassRelationTarget, ResolvedObjectRelationTarget, User, user_can_on_any,
};
use crate::permissions::{ResourceAttrs, ResourceKind, ResourceRef};
use crate::storage::postgres::{PostgresConnection, with_connection, with_transaction};
use crate::{
    apply_query_options, bind_transitive_filter_params, date_search, numeric_search,
    revision_search, string_search, trace_query,
};

use crate::traits::{GroupAccessors, SelfAccessors};

use super::{ObjectRelationsFromUser, Relations, SelfRelations};

/// Build Cedar authorization resources for class relations with one bulk
/// class lookup. Keeping this in the database layer avoids the two endpoint
/// queries per relation that `AuthzTarget::to_resource_ref` would otherwise
/// require on list paths.
pub async fn class_relation_authorization_resources(
    pool: &impl crate::storage::StorageContext,
    relations: &[HubuumClassRelation],
) -> Result<Vec<ResourceRef>, ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};

    let class_ids = relations
        .iter()
        .flat_map(|relation| [relation.from_hubuum_class_id, relation.to_hubuum_class_id])
        .collect::<Vec<_>>();
    let classes = with_connection(pool, async |conn| {
        hubuumclass
            .filter(id.eq_any(class_ids))
            .load::<HubuumClass>(conn)
            .await
    })
    .await?;
    let by_id = classes
        .into_iter()
        .map(|class| (class.id, class))
        .collect::<HashMap<_, _>>();

    relations
        .iter()
        .map(|relation| {
            let from = by_id.get(&relation.from_hubuum_class_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "class relation {} references missing class {}",
                    relation.id, relation.from_hubuum_class_id
                ))
            })?;
            let to = by_id.get(&relation.to_hubuum_class_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "class relation {} references missing class {}",
                    relation.id, relation.to_hubuum_class_id
                ))
            })?;
            Ok(ResourceRef {
                kind: ResourceKind::ClassRelation,
                id: relation.id,
                attrs: ResourceAttrs {
                    collection_id: (from.collection_id == to.collection_id)
                        .then_some(from.collection_id),
                    from_collection_id: Some(from.collection_id),
                    to_collection_id: Some(to.collection_id),
                    from_class_id: Some(from.id),
                    to_class_id: Some(to.id),
                    ..Default::default()
                },
            })
        })
        .collect()
}

/// Build Cedar authorization resources for object IDs with one bulk lookup.
pub async fn object_authorization_resources(
    pool: &impl crate::storage::StorageContext,
    object_ids: &[i32],
) -> Result<Vec<ResourceRef>, ApiError> {
    let by_id = load_objects_by_id(pool, object_ids).await?;
    object_ids
        .iter()
        .map(|object_id| {
            let object = by_id.get(object_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "authorization candidate references missing object {object_id}"
                ))
            })?;
            Ok(ResourceRef {
                kind: ResourceKind::Object,
                id: object.id,
                attrs: ResourceAttrs {
                    collection_id: Some(object.collection_id),
                    class_id: Some(object.hubuum_class_id),
                    name: Some(object.name.clone()),
                    ..Default::default()
                },
            })
        })
        .collect()
}

async fn load_objects_by_id(
    pool: &impl crate::storage::StorageContext,
    object_ids: &[i32],
) -> Result<HashMap<i32, HubuumObject>, ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    let objects = with_connection(pool, async |conn| {
        hubuumobject
            .filter(id.eq_any(object_ids))
            .load::<HubuumObject>(conn)
            .await
    })
    .await?;
    Ok(objects
        .into_iter()
        .map(|object| (object.id, object))
        .collect())
}

/// Build Cedar authorization resources for object relations with one bulk
/// object lookup, rather than issuing endpoint lookups for every row.
pub async fn object_relation_authorization_resources(
    pool: &impl crate::storage::StorageContext,
    relations: &[HubuumObjectRelation],
) -> Result<Vec<ResourceRef>, ApiError> {
    let object_ids = relations
        .iter()
        .flat_map(|relation| [relation.from_hubuum_object_id, relation.to_hubuum_object_id])
        .collect::<Vec<_>>();
    let by_id = load_objects_by_id(pool, &object_ids).await?;

    relations
        .iter()
        .map(|relation| {
            let from = by_id.get(&relation.from_hubuum_object_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "object relation {} references missing object {}",
                    relation.id, relation.from_hubuum_object_id
                ))
            })?;
            let to = by_id.get(&relation.to_hubuum_object_id).ok_or_else(|| {
                ApiError::InternalServerError(format!(
                    "object relation {} references missing object {}",
                    relation.id, relation.to_hubuum_object_id
                ))
            })?;
            Ok(ResourceRef {
                kind: ResourceKind::ObjectRelation,
                id: relation.id,
                attrs: ResourceAttrs {
                    collection_id: (from.collection_id == to.collection_id)
                        .then_some(from.collection_id),
                    from_collection_id: Some(from.collection_id),
                    to_collection_id: Some(to.collection_id),
                    from_class_id: Some(from.hubuum_class_id),
                    to_class_id: Some(to.hubuum_class_id),
                    from_object_id: Some(from.id),
                    to_object_id: Some(to.id),
                    class_relation_id: Some(relation.class_relation_id),
                    ..Default::default()
                },
            })
        })
        .collect()
}

fn class_relation_snapshot(relation: &HubuumClassRelation) -> serde_json::Value {
    serde_json::json!({
        "id": relation.id,
        "from_hubuum_class_id": relation.from_hubuum_class_id,
        "to_hubuum_class_id": relation.to_hubuum_class_id,
        "forward_template_alias": relation.forward_template_alias,
        "reverse_template_alias": relation.reverse_template_alias,
        "from_max_relations": relation.from_max_relations,
        "to_max_relations": relation.to_max_relations,
        "created_at": relation.created_at,
        "updated_at": relation.updated_at,
        "revision": relation.revision,
    })
}

fn object_relation_snapshot(relation: &HubuumObjectRelation) -> serde_json::Value {
    serde_json::json!({
        "id": relation.id,
        "from_hubuum_object_id": relation.from_hubuum_object_id,
        "to_hubuum_object_id": relation.to_hubuum_object_id,
        "class_relation_id": relation.class_relation_id,
        "created_at": relation.created_at,
        "updated_at": relation.updated_at,
        "revision": relation.revision,
    })
}

fn class_relation_metadata(from_class: &HubuumClass, to_class: &HubuumClass) -> serde_json::Value {
    serde_json::json!({
        "from_class_id": from_class.id,
        "to_class_id": to_class.id,
        "related_collection_ids": [from_class.collection_id, to_class.collection_id],
    })
}

fn object_relation_metadata(
    relation: &HubuumObjectRelation,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
) -> serde_json::Value {
    serde_json::json!({
        "class_relation_id": relation.class_relation_id,
        "from_object_id": from_object.id,
        "to_object_id": to_object.id,
        "from_class_id": from_object.hubuum_class_id,
        "to_class_id": to_object.hubuum_class_id,
        "related_collection_ids": [from_object.collection_id, to_object.collection_id],
    })
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
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError>;

    async fn relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;

    async fn search_relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError>;
}

impl<T> SelfRelationsBackend for T
where
    T: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn transitive_relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        use diesel::sql_query;
        use diesel::sql_types::Integer;

        with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitive>(
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
        .await
    }

    async fn relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        with_connection(pool, async |conn| {
            hubuumclass_relation
                .or_filter(from_hubuum_class_id.eq(self.id()))
                .or_filter(to_hubuum_class_id.eq(self.id()))
                .load::<HubuumClassRelation>(conn)
                .await
        })
        .await
    }

    async fn search_relations_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
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

        apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching relations");

        with_connection(pool, async |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
                .await
        })
        .await
    }
}

impl<C1, C2> Relations<C1, C2> for C1
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations_between(
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
        other: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        <C1 as Relations<C1, C2>>::relations_between(pool, self, other).await
    }

    async fn relations_to_paginated(
        &self,
        pool: &impl crate::storage::StorageContext,
        other: &C2,
        query_options: &QueryOptions,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations_paginated(pool, self, other, query_options).await
    }

    async fn direct_relation_to(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassRelationTransitive>, ApiError> {
        fetch_relations(pool, from, to).await
    }
}

async fn fetch_relations_direct<C1, C2>(
    pool: &impl crate::storage::StorageContext,
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

    with_connection(pool, async |conn| {
        diesel_async::RunQueryDsl::first::<HubuumClassRelation>(
            hubuumclass_relation
                .filter(from_hubuum_class_id.eq(from))
                .filter(to_hubuum_class_id.eq(to)),
            conn,
        )
        .await
    })
    .await
}

async fn fetch_relations<C1, C2>(
    pool: &impl crate::storage::StorageContext,
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

    with_connection(pool, async |conn| {
        diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitive>(
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
    .await
}

async fn fetch_relations_paginated<C1, C2>(
    pool: &impl crate::storage::StorageContext,
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
    let sorts = normalized_sorts::<HubuumClassRelationTransitive>(&query_options.sort)?;
    let mut raw_sql = String::from(
        "SELECT ancestor_class_id, descendant_class_id, depth, path
         FROM get_bidirectionally_related_classes(
             $1, ARRAY[]::INT[], $2, $3, $4, $5, $6, $7, $8
         )
         WHERE ancestor_class_id = $9 AND descendant_class_id = $10",
    );

    if let Some(cursor_sql) =
        cursor_filter_sql::<HubuumClassRelationTransitive>(&sorts, query_options.cursor.as_deref())?
    {
        raw_sql.push_str("\n  AND ");
        raw_sql.push_str(&cursor_sql);
    }

    let order_by = sorts
        .iter()
        .map(order_sql_clause::<HubuumClassRelationTransitive>)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    raw_sql.push_str(&format!("\nORDER BY {order_by}"));

    if let Some(limit) = query_options.limit {
        raw_sql.push_str(&format!("\nLIMIT {limit}"));
    }

    with_connection(pool, async |conn| {
        let query = bind_transitive_filter_params!(
            sql_query(raw_sql.clone())
                .bind::<Integer, _>(from)
                .bind::<Integer, _>(max_transitive_depth_from_config()),
            filter
        );

        diesel_async::RunQueryDsl::load::<HubuumClassRelationTransitive>(
            query.bind::<Integer, _>(from).bind::<Integer, _>(to),
            conn,
        )
        .await
    })
    .await
}

impl<U> ObjectRelationsFromUser for U
where
    U: SelfAccessors<User> + GroupAccessors,
    for<'a> &'a U: GroupAccessors,
{
    async fn get_related_objects<O, C>(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        let collections = user_can_on_any(pool, self, Permissions::ReadObject, None).await?;
        with_connection(pool, async |conn| {
            diesel_async::RunQueryDsl::load::<HubuumObjectTransitiveLink>(
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
        .await
    }
}

pub trait ObjectRelationMembershipsBackend {
    async fn is_member_of_class_relation_from_backend(
        &self,
        pool: &impl crate::storage::StorageContext,
        class_relation: &HubuumClassRelation,
    ) -> Result<bool, ApiError>;

    async fn object_relation_from_backend<O, C>(
        &self,
        pool: &impl crate::storage::StorageContext,
        class: &C,
        target_object: &O,
    ) -> Result<HubuumObjectRelation, ApiError>
    where
        C: SelfAccessors<HubuumClass> + Clone + Send + Sync,
        O: SelfAccessors<HubuumObject> + Clone + Send + Sync;

    async fn related_objects_from_backend<C>(
        &self,
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
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
        pool: &impl crate::storage::StorageContext,
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
                .first::<HubuumObjectRelation>(conn)
                .await
        })
        .await
    }

    async fn related_objects_from_backend<C>(
        &self,
        pool: &impl crate::storage::StorageContext,
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
                .load::<HubuumObject>(conn)
                .await
        })
        .await
    }
}

pub trait LoadClassRelationRecord {
    async fn load_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError>;
}

impl LoadClassRelationRecord for HubuumClassRelationID {
    async fn load_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, async |conn| {
            hubuumclass_relation
                .filter(id.eq(self.id()))
                .first::<HubuumClassRelation>(conn)
                .await
        })
        .await
    }
}

async fn load_class_relation_endpoint_records(
    conn: &mut PostgresConnection,
    from_class_id: i32,
    to_class_id: i32,
) -> Result<(HubuumClass, HubuumClass), ApiError> {
    use crate::schema::hubuumclass::dsl::{hubuumclass, id};

    let classes = hubuumclass
        .filter(id.eq_any([from_class_id, to_class_id]))
        .load::<HubuumClass>(conn)
        .await?;
    let from_class = classes
        .iter()
        .find(|class| class.id == from_class_id)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(format!("Class {from_class_id} was not found")))?;
    let to_class = classes
        .into_iter()
        .find(|class| class.id == to_class_id)
        .ok_or_else(|| ApiError::NotFound(format!("Class {to_class_id} was not found")))?;
    Ok((from_class, to_class))
}

pub(crate) trait PrepareClassRelationRecord {
    async fn prepare_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<PreparedClassRelation, ApiError>;
}

impl PrepareClassRelationRecord for NewHubuumClassRelation {
    async fn prepare_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<PreparedClassRelation, ApiError> {
        let command = self.clone().normalized()?;
        with_connection(pool, async |conn| {
            let (from_class, to_class) = load_class_relation_endpoint_records(
                conn,
                command.from_hubuum_class_id,
                command.to_hubuum_class_id,
            )
            .await?;
            PreparedClassRelation::new(command, from_class, to_class)
        })
        .await
    }
}

pub(crate) trait ResolveClassRelationTargetRecord {
    async fn resolve_class_relation_target_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResolvedClassRelationTarget, ApiError>;
}

async fn load_resolved_class_relation_target_on_connection(
    conn: &mut PostgresConnection,
    relation_id: i32,
) -> Result<ResolvedClassRelationTarget, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

    let relation = hubuumclass_relation
        .filter(id.eq(relation_id))
        .first::<HubuumClassRelation>(conn)
        .await?;
    let (from_class, to_class) = load_class_relation_endpoint_records(
        conn,
        relation.from_hubuum_class_id,
        relation.to_hubuum_class_id,
    )
    .await?;
    ResolvedClassRelationTarget::new(relation, from_class, to_class)
}

impl ResolveClassRelationTargetRecord for HubuumClassRelationID {
    async fn resolve_class_relation_target_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResolvedClassRelationTarget, ApiError> {
        with_connection(pool, async |conn| {
            load_resolved_class_relation_target_on_connection(conn, self.id()).await
        })
        .await
    }
}

pub trait DeleteClassRelationRecord {
    async fn delete_class_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError>;

    async fn delete_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_class_relation_record_without_events(pool).await
    }
}

impl DeleteClassRelationRecord for HubuumClassRelation {
    async fn delete_class_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumclass_relation.filter(id.eq(self.id)))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_class_relation_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::{hubuumclass, id as class_id};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumclass_relation
                .filter(id.eq(self.id))
                .for_update()
                .first::<HubuumClassRelation>(conn)
                .await?;
            let from_class = hubuumclass
                .filter(class_id.eq(relation.from_hubuum_class_id))
                .first::<HubuumClass>(conn)
                .await?;
            let to_class = hubuumclass
                .filter(class_id.eq(relation.to_hubuum_class_id))
                .first::<HubuumClass>(conn)
                .await?;

            diesel::delete(hubuumclass_relation.filter(id.eq(self.id)))
                .execute(conn)
                .await?;
            let event = NewEvent::new(
                EntityType::ClassRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Class relation {} -> {} deleted",
                    relation.from_hubuum_class_id, relation.to_hubuum_class_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(class_relation_snapshot(&relation))
            .with_metadata(class_relation_metadata(&from_class, &to_class));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

impl DeleteClassRelationRecord for HubuumClassRelationID {
    async fn delete_class_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumclass_relation.filter(id.eq(self.id())))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self.delete_class_relation_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::{hubuumclass, id as class_id};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumclass_relation
                .filter(id.eq(self.id()))
                .for_update()
                .first::<HubuumClassRelation>(conn)
                .await?;
            let from_class = hubuumclass
                .filter(class_id.eq(relation.from_hubuum_class_id))
                .first::<HubuumClass>(conn)
                .await?;
            let to_class = hubuumclass
                .filter(class_id.eq(relation.to_hubuum_class_id))
                .first::<HubuumClass>(conn)
                .await?;
            diesel::delete(hubuumclass_relation.filter(id.eq(self.id())))
                .execute(conn)
                .await?;

            let event = NewEvent::new(
                EntityType::ClassRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Class relation {} -> {} deleted",
                    relation.from_hubuum_class_id, relation.to_hubuum_class_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(class_relation_snapshot(&relation))
            .with_metadata(class_relation_metadata(&from_class, &to_class));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait SaveClassRelationRecord {
    async fn save_class_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError>;

    async fn save_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumClassRelation, ApiError> {
        let _ = context;
        self.save_class_relation_record_without_events(pool).await
    }
}

impl SaveClassRelationRecord for NewHubuumClassRelation {
    async fn save_class_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        let normalized = self.clone().normalized()?;

        with_connection(pool, async |conn| {
            diesel::insert_into(hubuumclass_relation)
                .values(&normalized)
                .get_result(conn)
                .await
        })
        .await
    }

    async fn save_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumClassRelation, ApiError> {
        let Some(context) = context else {
            return self.save_class_relation_record_without_events(pool).await;
        };

        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        let normalized = self.clone().normalized()?;

        with_transaction(
            pool,
            async |conn| -> Result<HubuumClassRelation, ApiError> {
                let relation = diesel::insert_into(hubuumclass_relation)
                    .values(&normalized)
                    .get_result::<HubuumClassRelation>(conn)
                    .await?;
                let from_class = hubuumclass
                    .filter(id.eq(relation.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                let to_class = hubuumclass
                    .filter(id.eq(relation.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)
                    .await?;
                let event = NewEvent::new(
                    EntityType::ClassRelation,
                    Action::Created,
                    context.actor_kind(),
                    format!(
                        "Class relation {} -> {} created",
                        relation.from_hubuum_class_id, relation.to_hubuum_class_id
                    ),
                )?
                .with_context(context)
                .with_entity_id(relation.id)
                .with_after(class_relation_snapshot(&relation))
                .with_metadata(class_relation_metadata(&from_class, &to_class));
                emit_event(conn, &event).await?;
                Ok(relation)
            },
        )
        .await
    }
}

pub(crate) trait CreatePreparedClassRelationRecord {
    async fn create_prepared_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClassRelation, ApiError>;
}

impl CreatePreparedClassRelationRecord for PreparedClassRelation {
    async fn create_prepared_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        with_transaction(
            pool,
            async |conn| -> Result<HubuumClassRelation, ApiError> {
                let from_class = hubuumclass
                    .filter(id.eq(self.command().from_hubuum_class_id))
                    .for_update()
                    .first::<HubuumClass>(conn)
                    .await?;
                let to_class = hubuumclass
                    .filter(id.eq(self.command().to_hubuum_class_id))
                    .for_update()
                    .first::<HubuumClass>(conn)
                    .await?;
                if &from_class != self.from_class() || &to_class != self.to_class() {
                    return Err(ApiError::NotFound(
                        "Class relation endpoints no longer match the prepared target".to_string(),
                    ));
                }

                let relation = diesel::insert_into(hubuumclass_relation)
                    .values(self.command())
                    .get_result::<HubuumClassRelation>(conn)
                    .await?;
                let event = NewEvent::new(
                    EntityType::ClassRelation,
                    Action::Created,
                    context.actor_kind(),
                    format!(
                        "Class relation {} -> {} created",
                        relation.from_hubuum_class_id, relation.to_hubuum_class_id
                    ),
                )?
                .with_context(context)
                .with_entity_id(relation.id)
                .with_after(class_relation_snapshot(&relation))
                .with_metadata(class_relation_metadata(&from_class, &to_class));
                emit_event(conn, &event).await?;
                Ok(relation)
            },
        )
        .await
    }
}

pub(crate) trait DeleteResolvedClassRelationRecord {
    async fn delete_resolved_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteResolvedClassRelationRecord for ResolvedClassRelationTarget {
    async fn delete_resolved_class_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as class_id};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumclass_relation
                .filter(id.eq(self.relation().id))
                .for_update()
                .first::<HubuumClassRelation>(conn)
                .await?;
            let from_class = hubuumclass
                .filter(class_id.eq(relation.from_hubuum_class_id))
                .for_update()
                .first::<HubuumClass>(conn)
                .await?;
            let to_class = hubuumclass
                .filter(class_id.eq(relation.to_hubuum_class_id))
                .for_update()
                .first::<HubuumClass>(conn)
                .await?;
            if &relation != self.relation()
                || &from_class != self.from_class()
                || &to_class != self.to_class()
            {
                return Err(ApiError::NotFound(
                    "Class relation no longer matches the resolved target".to_string(),
                ));
            }

            diesel::delete(hubuumclass_relation.filter(id.eq(relation.id)))
                .execute(conn)
                .await?;
            let event = NewEvent::new(
                EntityType::ClassRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Class relation {} -> {} deleted",
                    relation.from_hubuum_class_id, relation.to_hubuum_class_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(class_relation_snapshot(&relation))
            .with_metadata(class_relation_metadata(&from_class, &to_class));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

async fn load_object_relation_endpoint_records(
    conn: &mut PostgresConnection,
    from_object_id: i32,
    to_object_id: i32,
) -> Result<(HubuumObject, HubuumObject), ApiError> {
    use crate::schema::hubuumobject::dsl::{hubuumobject, id};

    let objects = hubuumobject
        .filter(id.eq_any([from_object_id, to_object_id]))
        .load::<HubuumObject>(conn)
        .await?;
    let from_object = objects
        .iter()
        .find(|object| object.id == from_object_id)
        .cloned()
        .ok_or_else(|| ApiError::NotFound(format!("Object {from_object_id} was not found")))?;
    let to_object = objects
        .into_iter()
        .find(|object| object.id == to_object_id)
        .ok_or_else(|| ApiError::NotFound(format!("Object {to_object_id} was not found")))?;
    Ok((from_object, to_object))
}

async fn load_direct_class_relation_target_on_connection(
    conn: &mut PostgresConnection,
    first_class_id: i32,
    second_class_id: i32,
) -> Result<ResolvedClassRelationTarget, ApiError> {
    use crate::schema::hubuumclass_relation::dsl::{
        from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
    };

    let lower_class_id = first_class_id.min(second_class_id);
    let higher_class_id = first_class_id.max(second_class_id);
    let relation = hubuumclass_relation
        .filter(from_hubuum_class_id.eq(lower_class_id))
        .filter(to_hubuum_class_id.eq(higher_class_id))
        .first::<HubuumClassRelation>(conn)
        .await
        .map_err(|error| match error {
            diesel::result::Error::NotFound => ApiError::NotFound(format!(
                "Class {first_class_id} is not related to class {second_class_id}"
            )),
            error => ApiError::from(error),
        })?;
    let (from_class, to_class) = load_class_relation_endpoint_records(
        conn,
        relation.from_hubuum_class_id,
        relation.to_hubuum_class_id,
    )
    .await?;
    ResolvedClassRelationTarget::new(relation, from_class, to_class)
}

fn order_object_relation_endpoints(
    command: &NewHubuumObjectRelation,
    first_object: HubuumObject,
    second_object: HubuumObject,
) -> Result<(HubuumObject, HubuumObject), ApiError> {
    if first_object.id == command.from_hubuum_object_id
        && second_object.id == command.to_hubuum_object_id
    {
        Ok((first_object, second_object))
    } else if second_object.id == command.from_hubuum_object_id
        && first_object.id == command.to_hubuum_object_id
    {
        Ok((second_object, first_object))
    } else {
        Err(ApiError::InternalServerError(
            "loaded object relation endpoints do not match the command".to_string(),
        ))
    }
}

pub(crate) trait PrepareObjectRelationRecord {
    async fn prepare_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<PreparedObjectRelation, ApiError>;
}

impl PrepareObjectRelationRecord for ObjectRelationCreateSelector {
    async fn prepare_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<PreparedObjectRelation, ApiError> {
        with_connection(pool, async |conn| match self.kind() {
            ObjectRelationCreateSelectorKind::Explicit(command) => {
                let command = command.clone().normalized()?;
                let (from_object, to_object) = load_object_relation_endpoint_records(
                    conn,
                    command.from_hubuum_object_id,
                    command.to_hubuum_object_id,
                )
                .await?;
                let class_relation = load_resolved_class_relation_target_on_connection(
                    conn,
                    command.class_relation_id,
                )
                .await?;
                PreparedObjectRelation::new(command, from_object, to_object, class_relation)
            }
            ObjectRelationCreateSelectorKind::Between { from, to } => {
                let (route_from_object, route_to_object) = load_object_relation_endpoint_records(
                    conn,
                    from.object_id().id(),
                    to.object_id().id(),
                )
                .await?;
                if route_from_object.hubuum_class_id != from.class_id().id()
                    || route_to_object.hubuum_class_id != to.class_id().id()
                {
                    return Err(ApiError::NotFound(
                        "Object was not found in the selected class".to_string(),
                    ));
                }
                let class_relation = load_direct_class_relation_target_on_connection(
                    conn,
                    from.class_id().id(),
                    to.class_id().id(),
                )
                .await?;
                let command = NewHubuumObjectRelation {
                    from_hubuum_object_id: route_from_object.id,
                    to_hubuum_object_id: route_to_object.id,
                    class_relation_id: class_relation.relation().id,
                }
                .normalized()?;
                let (from_object, to_object) =
                    order_object_relation_endpoints(&command, route_from_object, route_to_object)?;
                PreparedObjectRelation::new(command, from_object, to_object, class_relation)
            }
        })
        .await
    }
}

pub(crate) trait ResolveObjectRelationTargetRecord {
    async fn resolve_object_relation_target_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResolvedObjectRelationTarget, ApiError>;
}

impl ResolveObjectRelationTargetRecord for ObjectRelationSelector {
    async fn resolve_object_relation_target_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResolvedObjectRelationTarget, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{
            from_hubuum_object_id, hubuumobject_relation, id, to_hubuum_object_id,
        };

        with_connection(pool, async |conn| {
            let (relation, from_object, to_object) = match self.kind() {
                ObjectRelationSelectorKind::ById(relation_id) => {
                    let relation = hubuumobject_relation
                        .filter(id.eq(relation_id.id()))
                        .first::<HubuumObjectRelation>(conn)
                        .await?;
                    let (from_object, to_object) = load_object_relation_endpoint_records(
                        conn,
                        relation.from_hubuum_object_id,
                        relation.to_hubuum_object_id,
                    )
                    .await?;
                    (relation, from_object, to_object)
                }
                ObjectRelationSelectorKind::Between { from, to } => {
                    let (route_from_object, route_to_object) =
                        load_object_relation_endpoint_records(
                            conn,
                            from.object_id().id(),
                            to.object_id().id(),
                        )
                        .await?;
                    if route_from_object.hubuum_class_id != from.class_id().id()
                        || route_to_object.hubuum_class_id != to.class_id().id()
                    {
                        return Err(ApiError::NotFound(
                            "Object relation was not found for the selected classes".to_string(),
                        ));
                    }
                    let lower_object_id = from.object_id().id().min(to.object_id().id());
                    let higher_object_id = from.object_id().id().max(to.object_id().id());
                    let relation = hubuumobject_relation
                        .filter(from_hubuum_object_id.eq(lower_object_id))
                        .filter(to_hubuum_object_id.eq(higher_object_id))
                        .first::<HubuumObjectRelation>(conn)
                        .await?;
                    let command = NewHubuumObjectRelation {
                        from_hubuum_object_id: relation.from_hubuum_object_id,
                        to_hubuum_object_id: relation.to_hubuum_object_id,
                        class_relation_id: relation.class_relation_id,
                    };
                    let (from_object, to_object) = order_object_relation_endpoints(
                        &command,
                        route_from_object,
                        route_to_object,
                    )?;
                    (relation, from_object, to_object)
                }
            };
            let class_relation =
                load_resolved_class_relation_target_on_connection(conn, relation.class_relation_id)
                    .await?;
            ResolvedObjectRelationTarget::new(relation, from_object, to_object, class_relation)
        })
        .await
    }
}

fn object_relation_scope_matches(current: &HubuumObject, expected: &HubuumObject) -> bool {
    current.id == expected.id
        && current.collection_id == expected.collection_id
        && current.hubuum_class_id == expected.hubuum_class_id
}

pub(crate) trait CreatePreparedObjectRelationRecord {
    async fn create_prepared_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumObjectRelation, ApiError>;
}

impl CreatePreparedObjectRelationRecord for PreparedObjectRelation {
    async fn create_prepared_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{
            hubuumclass_relation, id as class_relation_id,
        };
        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        with_transaction(
            pool,
            async |conn| -> Result<HubuumObjectRelation, ApiError> {
                let (from_object, to_object) = load_object_relation_endpoint_records(
                    conn,
                    self.command().from_hubuum_object_id,
                    self.command().to_hubuum_object_id,
                )
                .await?;
                if !object_relation_scope_matches(&from_object, self.from_object())
                    || !object_relation_scope_matches(&to_object, self.to_object())
                {
                    return Err(ApiError::NotFound(
                        "Object relation endpoints no longer match the prepared target".to_string(),
                    ));
                }
                let class_relation = hubuumclass_relation
                    .filter(class_relation_id.eq(self.command().class_relation_id))
                    .for_share()
                    .first::<HubuumClassRelation>(conn)
                    .await?;
                if &class_relation != self.class_relation().relation() {
                    return Err(ApiError::NotFound(
                        "Class relation no longer matches the prepared object relation".to_string(),
                    ));
                }

                let relation = diesel::insert_into(hubuumobject_relation)
                    .values(self.command())
                    .get_result::<HubuumObjectRelation>(conn)
                    .await?;
                let event = NewEvent::new(
                    EntityType::ObjectRelation,
                    Action::Created,
                    context.actor_kind(),
                    format!(
                        "Object relation {} -> {} created",
                        relation.from_hubuum_object_id, relation.to_hubuum_object_id
                    ),
                )?
                .with_context(context)
                .with_entity_id(relation.id)
                .with_after(object_relation_snapshot(&relation))
                .with_metadata(object_relation_metadata(
                    &relation,
                    &from_object,
                    &to_object,
                ));
                emit_event(conn, &event).await?;
                Ok(relation)
            },
        )
        .await
    }
}

pub(crate) trait DeleteResolvedObjectRelationRecord {
    async fn delete_resolved_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError>;
}

impl DeleteResolvedObjectRelationRecord for ResolvedObjectRelationTarget {
    async fn delete_resolved_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: &EventContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumobject_relation
                .filter(id.eq(self.relation().id))
                .for_update()
                .first::<HubuumObjectRelation>(conn)
                .await?;
            let (from_object, to_object) = load_object_relation_endpoint_records(
                conn,
                relation.from_hubuum_object_id,
                relation.to_hubuum_object_id,
            )
            .await?;
            if &relation != self.relation()
                || !object_relation_scope_matches(&from_object, self.from_object())
                || !object_relation_scope_matches(&to_object, self.to_object())
            {
                return Err(ApiError::NotFound(
                    "Object relation no longer matches the resolved target".to_string(),
                ));
            }

            diesel::delete(hubuumobject_relation.filter(id.eq(relation.id)))
                .execute(conn)
                .await?;
            let event = NewEvent::new(
                EntityType::ObjectRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Object relation {} -> {} deleted",
                    relation.from_hubuum_object_id, relation.to_hubuum_object_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(object_relation_snapshot(&relation))
            .with_metadata(object_relation_metadata(
                &relation,
                &from_object,
                &to_object,
            ));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait LoadObjectRelationRecord {
    async fn load_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError>;
}

impl LoadObjectRelationRecord for HubuumObjectRelationID {
    async fn load_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, async |conn| {
            hubuumobject_relation
                .filter(id.eq(self.id()))
                .first::<HubuumObjectRelation>(conn)
                .await
        })
        .await
    }
}

pub trait DeleteObjectRelationRecord {
    async fn delete_object_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError>;

    async fn delete_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let _ = context;
        self.delete_object_relation_record_without_events(pool)
            .await
    }
}

impl DeleteObjectRelationRecord for HubuumObjectRelation {
    async fn delete_object_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumobject_relation.filter(id.eq(self.id)))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .delete_object_relation_record_without_events(pool)
                .await;
        };

        use crate::schema::hubuumobject::dsl::{hubuumobject, id as object_id};
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumobject_relation
                .filter(id.eq(self.id))
                .for_update()
                .first::<HubuumObjectRelation>(conn)
                .await?;
            let from_object = hubuumobject
                .filter(object_id.eq(relation.from_hubuum_object_id))
                .first::<HubuumObject>(conn)
                .await?;
            let to_object = hubuumobject
                .filter(object_id.eq(relation.to_hubuum_object_id))
                .first::<HubuumObject>(conn)
                .await?;
            diesel::delete(hubuumobject_relation.filter(id.eq(self.id)))
                .execute(conn)
                .await?;
            let event = NewEvent::new(
                EntityType::ObjectRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Object relation {} -> {} deleted",
                    relation.from_hubuum_object_id, relation.to_hubuum_object_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(object_relation_snapshot(&relation))
            .with_metadata(object_relation_metadata(
                &relation,
                &from_object,
                &to_object,
            ));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

impl DeleteObjectRelationRecord for HubuumObjectRelationID {
    async fn delete_object_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<(), ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_connection(pool, async |conn| {
            diesel::delete(hubuumobject_relation.filter(id.eq(self.id())))
                .execute(conn)
                .await
        })
        .await?;
        Ok(())
    }

    async fn delete_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<(), ApiError> {
        let Some(context) = context else {
            return self
                .delete_object_relation_record_without_events(pool)
                .await;
        };

        use crate::schema::hubuumobject::dsl::{hubuumobject, id as object_id};
        use crate::schema::hubuumobject_relation::dsl::{hubuumobject_relation, id};

        with_transaction(pool, async |conn| -> Result<(), ApiError> {
            let relation = hubuumobject_relation
                .filter(id.eq(self.id()))
                .for_update()
                .first::<HubuumObjectRelation>(conn)
                .await?;
            let from_object = hubuumobject
                .filter(object_id.eq(relation.from_hubuum_object_id))
                .first::<HubuumObject>(conn)
                .await?;
            let to_object = hubuumobject
                .filter(object_id.eq(relation.to_hubuum_object_id))
                .first::<HubuumObject>(conn)
                .await?;
            diesel::delete(hubuumobject_relation.filter(id.eq(self.id())))
                .execute(conn)
                .await?;
            let event = NewEvent::new(
                EntityType::ObjectRelation,
                Action::Deleted,
                context.actor_kind(),
                format!(
                    "Object relation {} -> {} deleted",
                    relation.from_hubuum_object_id, relation.to_hubuum_object_id
                ),
            )?
            .with_context(context)
            .with_entity_id(relation.id)
            .with_before(object_relation_snapshot(&relation))
            .with_metadata(object_relation_metadata(
                &relation,
                &from_object,
                &to_object,
            ));
            emit_event(conn, &event).await?;
            Ok(())
        })
        .await
    }
}

pub trait SaveObjectRelationRecord {
    async fn save_object_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError>;

    async fn save_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumObjectRelation, ApiError> {
        let _ = context;
        self.save_object_relation_record_without_events(pool).await
    }
}

impl SaveObjectRelationRecord for NewHubuumObjectRelation {
    async fn save_object_relation_record_without_events(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<HubuumObjectRelation, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }

        let obj1 = match HubuumObjectID::new(self.from_hubuum_object_id)?
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

        let obj2 = match HubuumObjectID::new(self.to_hubuum_object_id)?
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

        with_connection(pool, async |conn| {
            diesel::insert_into(hubuumobject_relation)
                .values(self)
                .get_result(conn)
                .await
        })
        .await
    }

    async fn save_object_relation_record(
        &self,
        pool: &impl crate::storage::StorageContext,
        context: Option<&EventContext>,
    ) -> Result<HubuumObjectRelation, ApiError> {
        let Some(context) = context else {
            return self.save_object_relation_record_without_events(pool).await;
        };

        use crate::schema::hubuumobject_relation::dsl::hubuumobject_relation;

        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }

        let obj1 = match HubuumObjectID::new(self.from_hubuum_object_id)?
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

        let obj2 = match HubuumObjectID::new(self.to_hubuum_object_id)?
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

        with_transaction(
            pool,
            async |conn| -> Result<HubuumObjectRelation, ApiError> {
                let relation = diesel::insert_into(hubuumobject_relation)
                    .values(self)
                    .get_result::<HubuumObjectRelation>(conn)
                    .await?;
                let event = NewEvent::new(
                    EntityType::ObjectRelation,
                    Action::Created,
                    context.actor_kind(),
                    format!(
                        "Object relation {} -> {} created",
                        relation.from_hubuum_object_id, relation.to_hubuum_object_id
                    ),
                )?
                .with_context(context)
                .with_entity_id(relation.id)
                .with_after(object_relation_snapshot(&relation))
                .with_metadata(object_relation_metadata(&relation, &obj1, &obj2));
                emit_event(conn, &event).await?;
                Ok(relation)
            },
        )
        .await
    }
}
