use super::*;
use crate::storage::postgres::operations::relations::LoadObjectRelationRecord;
use diesel_async::RunQueryDsl;
use std::collections::HashMap;

fn endpoint_collections(
    rows: Vec<(i32, CollectionRow)>,
    from_id: i32,
    to_id: i32,
    entity: &str,
) -> Result<(Collection, Collection), ApiError> {
    let mut by_endpoint = rows
        .into_iter()
        .map(|(id, collection)| (id, Collection::from(collection)))
        .collect::<HashMap<_, _>>();
    let from = by_endpoint.remove(&from_id).ok_or_else(|| {
        ApiError::NotFound(format!(
            "Could not find collection for {entity} endpoint {from_id}"
        ))
    })?;
    let to = if from_id == to_id {
        from.clone()
    } else {
        by_endpoint.remove(&to_id).ok_or_else(|| {
            ApiError::NotFound(format!(
                "Could not find collection for {entity} endpoint {to_id}"
            ))
        })?
    };
    Ok((from, to))
}

async fn class_endpoint_collections(
    pool: &crate::storage::postgres::PostgresPool,
    from_id: i32,
    to_id: i32,
) -> Result<(Collection, Collection), ApiError> {
    use crate::schema::collections::dsl::{collections, id as collection_id};
    use crate::schema::hubuumclass::dsl::{
        collection_id as class_collection_id, hubuumclass, id as class_id,
    };

    let rows = with_connection(pool, async |conn| {
        hubuumclass
            .filter(class_id.eq_any([from_id, to_id]))
            .inner_join(collections.on(collection_id.eq(class_collection_id)))
            .select((class_id, collections::all_columns()))
            .load::<(i32, CollectionRow)>(conn)
            .await
    })
    .await?;
    endpoint_collections(rows, from_id, to_id, "class relation")
}

async fn object_endpoint_collections(
    pool: &crate::storage::postgres::PostgresPool,
    from_id: i32,
    to_id: i32,
) -> Result<(Collection, Collection), ApiError> {
    use crate::schema::collections::dsl::{collections, id as collection_id};
    use crate::schema::hubuumobject::dsl::{
        collection_id as object_collection_id, hubuumobject, id as object_id,
    };

    let rows = with_connection(pool, async |conn| {
        hubuumobject
            .filter(object_id.eq_any([from_id, to_id]))
            .inner_join(collections.on(collection_id.eq(object_collection_id)))
            .select((object_id, collections::all_columns()))
            .load::<(i32, CollectionRow)>(conn)
            .await
    })
    .await?;
    endpoint_collections(rows, from_id, to_id, "object relation")
}

impl GetCollection<(Collection, Collection)> for HubuumClassRelation {
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(Collection, Collection), ApiError> {
        class_endpoint_collections(pool, self.from_hubuum_class_id, self.to_hubuum_class_id).await
    }
}

impl GetCollection<(Collection, Collection)> for NewHubuumClassRelation {
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(Collection, Collection), ApiError> {
        class_endpoint_collections(pool, self.from_hubuum_class_id, self.to_hubuum_class_id).await
    }
}

impl GetCollection<(Collection, Collection)> for HubuumObjectRelation {
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(Collection, Collection), ApiError> {
        object_endpoint_collections(pool, self.from_hubuum_object_id, self.to_hubuum_object_id)
            .await
    }
}

impl GetCollection<(Collection, Collection)> for NewHubuumObjectRelation {
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(Collection, Collection), ApiError> {
        object_endpoint_collections(pool, self.from_hubuum_object_id, self.to_hubuum_object_id)
            .await
    }
}

impl GetCollection<(Collection, Collection)> for HubuumObjectRelationID {
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<(Collection, Collection), ApiError> {
        let relation = self.load_object_relation_record(pool).await?;
        object_endpoint_collections(
            pool,
            relation.from_hubuum_object_id,
            relation.to_hubuum_object_id,
        )
        .await
    }
}

impl<S> GetCollection for S
where
    S: SelfAccessors<Collection> + Sync,
{
    async fn collection_from_backend(
        &self,
        pool: &crate::storage::postgres::PostgresPool,
    ) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        let collection = with_connection(pool, async |conn| {
            collections
                .filter(id.eq(self.id()))
                .first::<CollectionRow>(conn)
                .await
        })
        .await?
        .into();

        Ok(collection)
    }
}
