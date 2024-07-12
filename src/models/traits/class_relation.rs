use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use tracing::{debug, trace};

use serde::{Deserialize, Serialize};

use crate::db::DbPool;
use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

use crate::models::{
    HubuumClassRelation, HubuumClassRelationID, Namespace, NewHubuumClassRelation,
};
use crate::traits::{CanDelete, CanSave, NamespaceAccessors, SelfAccessors};

impl SelfAccessors<HubuumClassRelation> for HubuumClassRelationID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id};
        use diesel::prelude::*;

        let mut conn = pool.get()?;
        let class = hubuumclass_relation
            .filter(id.eq(self.0))
            .first::<HubuumClassRelation>(&mut conn)?;

        Ok(class)
    }
}
impl SelfAccessors<HubuumClassRelation> for HubuumClassRelation {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        Ok(self.clone())
    }
}

impl CanDelete for HubuumClassRelation {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass_relation::dsl::*;

        let mut conn = pool.get()?;
        diesel::delete(hubuumclass_relation.filter(id.eq(self.id))).execute(&mut conn)?;

        Ok(())
    }
}

impl CanSave for NewHubuumClassRelation {
    type Output = HubuumClassRelation;

    async fn save(&self, pool: &DbPool) -> Result<HubuumClassRelation, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::hubuumclass_relation;

        if self.from_hubuum_class_id == self.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_class_id and to_hubuum_class_id cannot be the same".to_string(),
            ));
        }

        let mut conn = pool.get()?;
        let result = diesel::insert_into(hubuumclass_relation)
            .values(self)
            .get_result(&mut conn)?;

        Ok(result)
    }
}

impl HubuumClassRelation {
    pub async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{from_hubuum_class_id, to_hubuum_class_id};
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};

        trace!("Getting namespaces for class relation");

        let mut conn = pool.get()?;
        let namespace_list = hubuumclass
            .filter(class_id.eq_any(&[self.from_hubuum_class_id, self.to_hubuum_class_id]))
            .inner_join(namespaces.on(namespace_id.eq(class_namespace_id)))
            .select(namespaces::all_columns())
            .load::<Namespace>(&mut conn)?;

        if self.from_hubuum_class_id == self.to_hubuum_class_id && namespace_list.len() == 1 {
            trace!("Found same namespace for class relation, returning same namespace twice");
            return Ok((namespace_list[0].clone(), namespace_list[0].clone()));
        } else if namespace_list.len() != 2 {
            debug!(
                "Could not find two namespaces for class relation: {} and {}, found {:?}",
                self.from_hubuum_class_id, self.to_hubuum_class_id, namespace_list
            );
            return Err(ApiError::NotFound(
                format!(
                    "Could not find namespaces ({} and {}) for class relation",
                    self.from_hubuum_class_id, self.to_hubuum_class_id,
                )
                .to_string(),
            ));
        }

        Ok((namespace_list[0].clone(), namespace_list[1].clone()))
    }

    pub async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let namespace_tuple = self.namespace(pool).await?;
        Ok((namespace_tuple.0.id, namespace_tuple.1.id))
    }
}

impl HubuumClassRelationID {
    pub async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        self.instance(pool).await?.namespace(pool).await
    }

    pub async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.namespace_id(pool).await
    }
}
