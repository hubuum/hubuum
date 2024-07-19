use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use tracing::{debug, trace};

use serde::{Deserialize, Serialize};

use crate::db::traits::GetNamespace;
use crate::db::DbPool;
use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

use crate::models::{
    HubuumClass, HubuumClassRelation, HubuumClassRelationID, Namespace, NewHubuumClassRelation,
};
use crate::traits::{CanDelete, CanSave, ClassAccessors, NamespaceAccessors, SelfAccessors};

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
        Ok(*self)
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

impl CanDelete for HubuumClassRelationID {
    async fn delete(&self, pool: &DbPool) -> Result<(), ApiError> {
        self.instance(pool).await?.delete(pool).await
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for NewHubuumClassRelation {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for HubuumClassRelation {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        use crate::db::traits::GetNamespace;
        self.namespace_from_backend(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        let (ns1, ns2) = self.namespace(pool).await?;
        Ok((ns1.id, ns2.id))
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelation {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}

impl NamespaceAccessors<(Namespace, Namespace), (i32, i32)> for HubuumClassRelationID {
    async fn namespace(&self, pool: &DbPool) -> Result<(Namespace, Namespace), ApiError> {
        self.instance(pool).await?.namespace(pool).await
    }

    async fn namespace_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.namespace_id(pool).await
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for HubuumClassRelationID {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, pool: &DbPool) -> Result<(i32, i32), ApiError> {
        self.instance(pool).await?.class_id(pool).await
    }
}

impl ClassAccessors<(HubuumClass, HubuumClass), (i32, i32)> for NewHubuumClassRelation {
    async fn class(&self, pool: &DbPool) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::db::traits::GetClass;
        self.class_from_backend(pool).await
    }

    async fn class_id(&self, _pool: &DbPool) -> Result<(i32, i32), ApiError> {
        Ok((self.from_hubuum_class_id, self.to_hubuum_class_id))
    }
}
