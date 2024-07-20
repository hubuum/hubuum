use crate::db::traits::ClassRelation;

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{HubuumClass, HubuumClassClosure};

use crate::traits::SelfAccessors;

use super::Relations;

impl<C1, C2> Relations<C1, C2> for C1
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations(
        pool: &DbPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassClosure>, ApiError> {
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
    ) -> Result<Vec<HubuumClassClosure>, ApiError> {
        <C1 as Relations<C1, C2>>::relations(pool, self, other).await
    }
}

impl<C1, C2> Relations<C1, C2> for HubuumClassClosure
where
    C1: SelfAccessors<HubuumClass> + Clone + Send + Sync,
    C2: SelfAccessors<HubuumClass> + Clone + Send + Sync,
{
    async fn relations(
        pool: &DbPool,
        from: &C1,
        to: &C2,
    ) -> Result<Vec<HubuumClassClosure>, ApiError> {
        fetch_relations(pool, from, to).await
    }
}

async fn fetch_relations<C1, C2>(
    pool: &DbPool,
    from: &C1,
    to: &C2,
) -> Result<Vec<HubuumClassClosure>, ApiError>
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
        Ok(hubuumclass_closure
            .filter(ancestor_class_id.eq(from))
            .filter(descendant_class_id.eq(to))
            .load::<HubuumClassClosure>(conn)
            .map_err(|e| ApiError::DatabaseError(e.to_string())))
    })?
}
