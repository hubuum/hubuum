use super::*;
impl GetCollection<(Collection, Collection)> for HubuumClassRelation {
    async fn collection_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Collection, Collection), ApiError> {
        use crate::schema::collections::dsl::{collections, id as collection_id};
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };

        let (from_id, to_id) = self.class_id(pool).await?;
        // Work with raw ids at the Diesel boundary.
        let (from_id, to_id) = (from_id.id(), to_id.id());

        let collection_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(collections.on(collection_id.eq(class_collection_id)))
                .select(collections::all_columns())
                .load::<Collection>(conn)
        })?;

        if from_id == to_id && collection_list.len() == 1 {
            trace!("Found same collection for class relation, returning same collection twice");
            return Ok((collection_list[0].clone(), collection_list[0].clone()));
        } else if collection_list.len() != 2 {
            debug!(
                "Could not find two collections for class relation: {} and {}, found {:?}",
                from_id, to_id, collection_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find collections ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((collection_list[0].clone(), collection_list[1].clone()))
    }
}

impl GetCollection<(Collection, Collection)> for NewHubuumClassRelation {
    async fn collection_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Collection, Collection), ApiError> {
        use crate::schema::collections::dsl::{collections, id as collection_id};
        use crate::schema::hubuumclass::dsl::{
            collection_id as class_collection_id, hubuumclass, id as class_id,
        };

        let (from_id, to_id) = self.class_id(pool).await?;
        // Work with raw ids at the Diesel boundary.
        let (from_id, to_id) = (from_id.id(), to_id.id());

        let collection_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(collections.on(collection_id.eq(class_collection_id)))
                .select(collections::all_columns())
                .load::<Collection>(conn)
        })?;

        if collection_list.len() == 1 {
            trace!("Found same collection for class relation, returning same collection twice");
            return Ok((collection_list[0].clone(), collection_list[0].clone()));
        } else if collection_list.len() != 2 {
            debug!(
                "Could not find two collections for class relation: {} and {}, found {:?}",
                from_id, to_id, collection_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find collections ({from_id} and {to_id}) for class relation",)
                    .to_string(),
            ));
        }
        Ok((collection_list[0].clone(), collection_list[1].clone()))
    }
}

impl GetCollection<(Collection, Collection)> for HubuumObjectRelation {
    async fn collection_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Collection, Collection), ApiError> {
        use crate::schema::collections::dsl::{collections, id as collection_id};
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id,
        };

        let (from_id, to_id) = self.object_id(pool).await?;
        // Work with raw ids at the Diesel boundary.
        let (from_id, to_id) = (from_id.id(), to_id.id());

        let collection_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(collections.on(collection_id.eq(object_collection_id)))
                .select(collections::all_columns())
                .load::<Collection>(conn)
        })?;

        if collection_list.len() == 1 {
            trace!("Found same collection for object relation, returning same collection twice");
            return Ok((collection_list[0].clone(), collection_list[0].clone()));
        } else if collection_list.len() != 2 {
            debug!(
                "Could not find two collections for object relation: {} and {}, found {:?}",
                from_id, to_id, collection_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find collections ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((collection_list[0].clone(), collection_list[1].clone()))
    }
}

impl GetCollection<(Collection, Collection)> for NewHubuumObjectRelation {
    async fn collection_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Collection, Collection), ApiError> {
        use crate::schema::collections::dsl::{collections, id as collection_id};
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id,
        };

        let (from_id, to_id) = self.object_id(pool).await?;
        // Work with raw ids at the Diesel boundary.
        let (from_id, to_id) = (from_id.id(), to_id.id());

        let collection_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(collections.on(collection_id.eq(object_collection_id)))
                .select(collections::all_columns())
                .load::<Collection>(conn)
        })?;

        if collection_list.len() == 1 {
            trace!("Found same collection for object relation, returning same collection twice");
            return Ok((collection_list[0].clone(), collection_list[0].clone()));
        } else if collection_list.len() != 2 {
            debug!(
                "Could not find two collections for object relation: {} and {}, found {:?}",
                from_id, to_id, collection_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find collections ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((collection_list[0].clone(), collection_list[1].clone()))
    }
}

impl GetCollection<(Collection, Collection)> for HubuumObjectRelationID {
    async fn collection_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Collection, Collection), ApiError> {
        use crate::schema::collections::dsl::{collections, id as collection_id};
        use crate::schema::hubuumobject::dsl::{
            collection_id as object_collection_id, hubuumobject, id as object_id,
        };

        let (from_id, to_id) = self.object_id(pool).await?;
        // Work with raw ids at the Diesel boundary.
        let (from_id, to_id) = (from_id.id(), to_id.id());

        let collection_list = with_connection(pool, |conn| {
            hubuumobject
                .filter(object_id.eq_any(&[from_id, to_id]))
                .inner_join(collections.on(collection_id.eq(object_collection_id)))
                .select(collections::all_columns())
                .load::<Collection>(conn)
        })?;

        if collection_list.len() == 1 {
            trace!("Found same collection for object relation, returning same collection twice");
            return Ok((collection_list[0].clone(), collection_list[0].clone()));
        } else if collection_list.len() != 2 {
            debug!(
                "Could not find two collections for object relation: {} and {}, found {:?}",
                from_id, to_id, collection_list
            );
            return Err(ApiError::NotFound(
                format!("Could not find collections ({from_id} and {to_id}) for object relation",)
                    .to_string(),
            ));
        }
        Ok((collection_list[0].clone(), collection_list[1].clone()))
    }
}

impl<S> GetCollection for S
where
    S: SelfAccessors<Collection>,
{
    async fn collection_from_backend(&self, pool: &DbPool) -> Result<Collection, ApiError> {
        use crate::schema::collections::dsl::{collections, id};

        let collection = with_connection(pool, |conn| {
            collections
                .filter(id.eq(self.id()))
                .first::<Collection>(conn)
        })?;

        Ok(collection)
    }
}
