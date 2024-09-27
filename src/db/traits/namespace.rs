use diesel::prelude::*;
use tracing::{debug, trace};

use crate::db::traits::GetNamespace;
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::HubuumClassRelation;
use crate::models::Namespace;
use crate::models::NewHubuumClassRelation;
use crate::traits::ClassAccessors;
use crate::traits::SelfAccessors;

impl GetNamespace<(Namespace, Namespace)> for HubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if from_id == to_id && namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!(
                    "Could not find namespaces ({} and {}) for class relation",
                    from_id, to_id,
                )
                .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl GetNamespace<(Namespace, Namespace)> for NewHubuumClassRelation {
    async fn namespace_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        let (from_id, to_id) = self.class_id(pool).await?;

        let namespace_list = with_connection(pool, |conn| {
            hubuumclass
                .filter(class_id.eq_any(&[from_id, to_id]))
                .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        if namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                from_id, to_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!(
                    "Could not find namespaces ({} and {}) for class relation",
                    from_id, to_id,
                )
                .to_string(),
            ));
        }
        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }
}

impl<S> GetNamespace for S
where
    S: SelfAccessors<Namespace>,
{
    async fn namespace_from_backend(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        let namespace = with_connection(pool, |conn| {
            namespaces.filter(id.eq(self.id())).first::<Namespace>(conn)
        })?;

        Ok(namespace)
    }
}
