use crate::db::traits::ClassRelation;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{
    user_can_on_any, HubuumClass, HubuumClassRelationTransitive, HubuumObject,
    HubuumObjectTransitiveLink, User,
};

use crate::traits::{GroupAccessors, SelfAccessors};

use super::{ObjectRelationsFromUser, Relations, SelfRelations};

impl<C1> SelfRelations<HubuumClass> for C1 where C1: SelfAccessors<HubuumClass> + Clone + Send + Sync
{}

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
        use diesel::sql_query;
        use diesel::sql_types::{Array, Integer};
        use diesel::RunQueryDsl;

        let namespaces = user_can_on_any(pool, self.clone(), Permissions::ReadObject).await?;
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
