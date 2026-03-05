use diesel::prelude::*;

use crate::db::traits::GetClass;
use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumClassRelation, HubuumClassRelationID, Namespace,
    NewHubuumClass, NewHubuumClassRelation, UpdateHubuumClass,
};

impl GetClass for HubuumClass {
    async fn class_from_backend(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};
        with_connection(pool, |conn| -> Result<HubuumClass, diesel::result::Error> {
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
        with_connection(pool, |conn| -> Result<HubuumClass, diesel::result::Error> {
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
        with_connection(
            pool,
            |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(id.eq(self.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)?;
                let to_class = hubuumclass
                    .filter(id.eq(self.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)?;
                Ok((from_class, to_class))
            },
        )
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for HubuumClassRelationID {
    async fn class_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};
        use crate::schema::hubuumclass_relation::dsl::{hubuumclass_relation, id as rel_id};

        with_connection(
            pool,
            |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
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
            },
        )
    }
}

impl GetClass<(HubuumClass, HubuumClass)> for NewHubuumClassRelation {
    async fn class_from_backend(
        &self,
        pool: &DbPool,
    ) -> Result<(HubuumClass, HubuumClass), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id as hid};

        with_connection(
            pool,
            |conn| -> Result<(HubuumClass, HubuumClass), diesel::result::Error> {
                let from_class = hubuumclass
                    .filter(hid.eq(self.from_hubuum_class_id))
                    .first::<HubuumClass>(conn)?;
                let to_class = hubuumclass
                    .filter(hid.eq(self.to_hubuum_class_id))
                    .first::<HubuumClass>(conn)?;
                Ok((from_class, to_class))
            },
        )
    }
}

pub trait LoadClassRecord {
    async fn load_class_record(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
}

impl LoadClassRecord for HubuumClass {
    async fn load_class_record(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

impl LoadClassRecord for HubuumClassID {
    async fn load_class_record(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        self.class_from_backend(pool).await
    }
}

pub trait CreateClassRecord {
    async fn create_class_record(&self, pool: &DbPool) -> Result<HubuumClass, ApiError>;
}

impl CreateClassRecord for NewHubuumClass {
    async fn create_class_record(&self, pool: &DbPool) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::hubuumclass;

        with_connection(pool, |conn| {
            diesel::insert_into(hubuumclass)
                .values(self)
                .get_result(conn)
        })
    }
}

pub trait UpdateClassRecord {
    async fn update_class_record(
        &self,
        pool: &DbPool,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError>;
}

impl UpdateClassRecord for UpdateHubuumClass {
    async fn update_class_record(
        &self,
        pool: &DbPool,
        class_id: i32,
    ) -> Result<HubuumClass, ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_connection(pool, |conn| {
            diesel::update(hubuumclass.filter(id.eq(class_id)))
                .set(self)
                .get_result(conn)
        })
    }
}

pub trait DeleteClassRecord {
    async fn delete_class_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteClassRecord for HubuumClass {
    async fn delete_class_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::hubuumclass::dsl::{hubuumclass, id};

        with_connection(pool, |conn| {
            diesel::delete(hubuumclass.filter(id.eq(self.id))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait ClassNamespaceLookup {
    async fn lookup_class_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError>;
}

impl ClassNamespaceLookup for HubuumClass {
    async fn lookup_class_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        use crate::schema::namespaces::dsl::{id, namespaces};

        with_connection(pool, |conn| {
            namespaces
                .filter(id.eq(self.namespace_id))
                .first::<Namespace>(conn)
        })
    }
}

impl ClassNamespaceLookup for HubuumClassID {
    async fn lookup_class_namespace(&self, pool: &DbPool) -> Result<Namespace, ApiError> {
        self.load_class_record(pool)
            .await?
            .lookup_class_namespace(pool)
            .await
    }
}

pub async fn total_class_count_from_backend(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    with_connection(pool, |conn| hubuumclass.count().get_result::<i64>(conn))
}
