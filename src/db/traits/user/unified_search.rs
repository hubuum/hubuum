use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::sql_types::{Bool, Text};

use crate::db::traits::user::LoadPermittedNamespaces;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::traits::ExpandNamespaceFromMap;
use crate::models::traits::user::UserNamespaceAccessors;
use crate::models::{
    HubuumClass, HubuumClassExpanded, HubuumObject, Namespace, Permissions, UnifiedSearchSpec,
    User,
};
use crate::traits::SelfAccessors;

pub trait UnifiedSearchBackend: SelfAccessors<User> + UserNamespaceAccessors {
    async fn search_unified_namespaces_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::schema::namespaces::dsl as ns;

        let namespaces = permitted_namespaces(self, pool, &[Permissions::ReadCollection]).await?;
        if namespaces.is_empty() {
            return Ok(vec![]);
        }

        let namespace_ids = namespaces
            .into_iter()
            .map(|namespace| namespace.id)
            .collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        with_connection(pool, |conn| {
            ns::namespaces
                .filter(ns::id.eq_any(namespace_ids))
                .filter(
                    ns::name
                        .ilike(pattern.clone())
                        .or(ns::description.ilike(pattern.clone())),
                )
                .load::<Namespace>(conn)
        })
    }

    async fn search_unified_classes_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl as class_dsl;

        let namespaces = permitted_namespaces(
            self,
            pool,
            &[Permissions::ReadCollection, Permissions::ReadClass],
        )
        .await?;
        if namespaces.is_empty() {
            return Ok(vec![]);
        }

        let namespace_map = namespaces
            .iter()
            .cloned()
            .map(|namespace| (namespace.id, namespace))
            .collect::<std::collections::HashMap<_, _>>();
        let namespace_ids = namespace_map.keys().copied().collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        let classes = with_connection(pool, |conn| {
            let mut query = class_dsl::hubuumclass
                .filter(class_dsl::namespace_id.eq_any(namespace_ids))
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

        Ok(classes.expand_namespace_from_map(&namespace_map))
    }

    async fn search_unified_objects_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl as object_dsl;

        let namespaces = permitted_namespaces(
            self,
            pool,
            &[Permissions::ReadCollection, Permissions::ReadObject],
        )
        .await?;
        if namespaces.is_empty() {
            return Ok(vec![]);
        }

        let namespace_ids = namespaces
            .into_iter()
            .map(|namespace| namespace.id)
            .collect::<Vec<_>>();
        let pattern = format!("%{}%", params.query);

        with_connection(pool, |conn| {
            let mut query = object_dsl::hubuumobject
                .filter(object_dsl::namespace_id.eq_any(namespace_ids))
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

async fn permitted_namespaces<T>(
    user: &T,
    pool: &DbPool,
    permissions: &[Permissions],
) -> Result<Vec<Namespace>, ApiError>
where
    T: SelfAccessors<User> + UserNamespaceAccessors + ?Sized,
{
    let permissions = permissions.to_vec();
    user.load_namespaces_with_permissions(pool, &permissions)
        .await
}

impl<T: ?Sized> UnifiedSearchBackend for T where T: SelfAccessors<User> + UserNamespaceAccessors {}
