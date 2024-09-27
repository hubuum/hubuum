use diesel::prelude::*;

use crate::db::traits::GetClass;
use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumClassRelation, HubuumClassRelationID, NewHubuumClassRelation,
};

impl GetClass for HubuumClass {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(pool, |conn| {
            let class = hubuumclass
                .filter(id.eq(self.id))
                .first::<HubuumClass>(conn)?;
            Ok(class)
        })
    }
}

impl GetClass for HubuumClassID {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(pool, |conn| {
            let class = hubuumclass
                .filter(id.eq(self.0))
                .first::<HubuumClass>(conn)?;
            Ok(class)
        })
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(pool, |conn| {
            let from_class = hubuumclass
                .filter(id.eq(self.from_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            let to_class = hubuumclass
                .filter(id.eq(self.to_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            Ok((from_class, to_class))
        })
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelationID {
    async fn class_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id as rel_id};

        with_connection(pool, |conn| {
            let relation = hubuumclass_relation
                .filter(rel_id.eq(self.0))
                .first::<HubuumClassRelation>(conn)?;

            let from_class = hubuumclass
                .filter(hid.eq(relation.from_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            let to_class = hubuumclass
                .filter(hid.eq(relation.to_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            Ok((from_class, to_class))
        })
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for NewHubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};

        with_connection(pool, |conn| {
            let from_class = hubuumclass
                .filter(hid.eq(self.from_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            let to_class = hubuumclass
                .filter(hid.eq(self.to_hubuum_class_id))
                .first::<HubuumClass>(conn)?;
            Ok((from_class, to_class))
        })
    }
}
