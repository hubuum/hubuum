use crate::db::traits::ClassRelation;
use diesel::prelude::*;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::search::{FilterField, ParsedQueryParam, QueryOptions};
use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationID, HubuumClassRelationTransitive,
    HubuumObject, HubuumObjectID, HubuumObjectRelation, HubuumObjectRelationID,
    HubuumObjectTransitiveLink, NewHubuumClassRelation, NewHubuumObjectRelation, User,
    user_can_on_any,
};
use crate::{date_search, numeric_search, string_search, trace_query};

use crate::traits::{GroupAccessors, SelfAccessors};

use super::{ObjectRelationsFromUser, Relations, SelfRelations};

impl<C1> SelfRelations<HubuumClass> for C1 where C1: SelfAccessors<HubuumClass> + Clone + Send + Sync
{}

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
        use crate::schema::hubuumclass_closure::dsl::*;

        with_connection(pool, |conn| {
            hubuumclass_closure
                .or_filter(ancestor_class_id.eq(self.id()))
                .or_filter(descendant_class_id.eq(self.id()))
                .then_order_by(depth.asc())
                .then_order_by(descendant_class_id.asc())
                .select((
                    ancestor_class_id.assume_not_null(),
                    descendant_class_id.assume_not_null(),
                    depth,
                    path,
                ))
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
    use crate::schema::hubuumclass_closure::dsl::*;
    use diesel::prelude::*;

    // Use the smallest ID as from and the largest as to. Also,
    // resolve the ID first as from and to may be different types
    // that implement SelfAccessors<HubuumClass>. This makes a direct
    // tuple swap problematic.
    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    with_connection(pool, |conn| {
        hubuumclass_closure
            .filter(ancestor_class_id.eq(from))
            .filter(descendant_class_id.eq(to))
            .select((
                ancestor_class_id.assume_not_null(),
                descendant_class_id.assume_not_null(),
                depth,
                path,
            ))
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
    use crate::schema::hubuumclass_closure::dsl::*;
    use crate::{array_search, numeric_search};
    use diesel::prelude::*;

    let (from, to) = (from.id(), to.id());
    let (from, to) = if from > to { (to, from) } else { (from, to) };

    let mut base_query = hubuumclass_closure
        .filter(ancestor_class_id.eq(from))
        .filter(descendant_class_id.eq(to))
        .into_boxed();

    for param in &query_options.filters {
        let operator = param.operator.clone();
        match param.field {
            FilterField::ClassFrom => {
                numeric_search!(base_query, param, operator, ancestor_class_id)
            }
            FilterField::ClassTo => {
                numeric_search!(base_query, param, operator, descendant_class_id)
            }
            FilterField::Depth => numeric_search!(base_query, param, operator, depth),
            FilterField::Path => array_search!(base_query, param, operator, path),
            _ => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{}' isn't searchable (or does not exist) for transitive class relations",
                    param.field
                )));
            }
        }
    }

    crate::apply_query_options!(base_query, query_options, HubuumClassRelationTransitive);

    with_connection(pool, |conn| {
        base_query
            .select((
                ancestor_class_id.assume_not_null(),
                descendant_class_id.assume_not_null(),
                depth,
                path,
            ))
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
            sql_query("SELECT * FROM get_transitively_linked_objects($1, $2, $3)")
                .bind::<Integer, _>(source_object.id())
                .bind::<Integer, _>(target_class.id())
                .bind::<Array<Integer>, _>(
                    namespaces.into_iter().map(|n| n.id()).collect::<Vec<_>>(),
                )
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
                    obj_rel::hubuumobject_relation.on(obj::id.eq(obj_rel::from_hubuum_object_id)),
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
