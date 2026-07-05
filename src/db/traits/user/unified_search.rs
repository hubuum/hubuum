use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Text};

use crate::db::traits::user::LoadPermittedCollections;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::traits::ExpandCollectionFromMap;
use crate::models::traits::user::UserCollectionAccessors;
use crate::models::{
    Collection, HubuumClass, HubuumClassExpanded, HubuumObject, Permissions, UnifiedSearchSpec,
};

pub trait UnifiedSearchBackend: UserCollectionAccessors {
    async fn search_unified_collections_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<Collection>, ApiError> {
        use crate::schema::collections::dsl as ns;

        let collections =
            permitted_collections(self, pool, &[Permissions::ReadCollection], scopes).await?;
        if collections.is_empty() {
            return Ok(vec![]);
        }

        let collection_ids = collections
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        with_connection(pool, |conn| {
            ns::collections
                .filter(ns::id.eq_any(collection_ids))
                .filter(
                    ns::name
                        .ilike(pattern.clone())
                        .or(ns::description.ilike(pattern.clone())),
                )
                .load::<Collection>(conn)
        })
    }

    async fn search_unified_classes_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl as class_dsl;

        let collections = permitted_collections(
            self,
            pool,
            &[Permissions::ReadCollection, Permissions::ReadClass],
            scopes,
        )
        .await?;
        if collections.is_empty() {
            return Ok(vec![]);
        }

        let collection_map = collections
            .iter()
            .cloned()
            .map(|collection| (collection.id, collection))
            .collect::<std::collections::HashMap<_, _>>();
        let collection_ids = collection_map.keys().copied().collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        let classes = with_connection(pool, |conn| {
            let mut query = class_dsl::hubuumclass
                .filter(class_dsl::collection_id.eq_any(collection_ids))
                .into_boxed();

            if params.search_class_schema {
                query = query.filter(
                    class_dsl::name
                        .ilike(pattern.clone())
                        .or(class_dsl::description.ilike(pattern.clone()))
                        .or(sql::<Bool>(
                            "lower(coalesce(json_schema::text, '')) LIKE '%' || lower(",
                        )
                        .bind::<Text, _>(params.query.clone())
                        .sql(") || '%'")),
                );
            } else {
                query = query.filter(
                    class_dsl::name
                        .ilike(pattern.clone())
                        .or(class_dsl::description.ilike(pattern.clone())),
                );
            }

            query.load::<HubuumClass>(conn)
        })?;

        Ok(classes.expand_collection_from_map(&collection_map))
    }

    async fn search_unified_objects_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&[Permissions]>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl as object_dsl;

        let collections = permitted_collections(
            self,
            pool,
            &[Permissions::ReadCollection, Permissions::ReadObject],
            scopes,
        )
        .await?;
        if collections.is_empty() {
            return Ok(vec![]);
        }

        let collection_ids = collections
            .into_iter()
            .map(|collection| collection.id)
            .collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        with_connection(pool, |conn| {
            let mut query = object_dsl::hubuumobject
                .filter(object_dsl::collection_id.eq_any(collection_ids))
                .into_boxed();

            if params.search_object_data {
                query = query.filter(
                    object_dsl::name
                        .ilike(pattern.clone())
                        .or(object_dsl::description.ilike(pattern.clone()))
                        .or(
                            sql::<Bool>(
                                "jsonb_to_tsvector('simple', data, '[\"string\"]') @@ plainto_tsquery('simple', ",
                            )
                            .bind::<Text, _>(params.query.clone())
                            .sql(")"),
                        ),
                );
            } else {
                query = query.filter(
                    object_dsl::name
                        .ilike(pattern.clone())
                        .or(object_dsl::description.ilike(pattern.clone())),
                );
            }

            query.load::<HubuumObject>(conn)
        })
    }
}

async fn permitted_collections<T>(
    user: &T,
    pool: &DbPool,
    permissions: &[Permissions],
    scopes: Option<&[Permissions]>,
) -> Result<Vec<Collection>, ApiError>
where
    T: UserCollectionAccessors + ?Sized,
{
    let permissions = permissions.to_vec();
    user.load_collections_with_permissions(pool, &permissions, scopes)
        .await
}

impl<T: ?Sized> UnifiedSearchBackend for T where T: UserCollectionAccessors {}
