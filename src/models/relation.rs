use diesel::prelude::*;
use std::{fmt, fmt::Display, slice};

use crate::db::DbPool;

use serde::{Deserialize, Serialize};

use crate::{errors::ApiError, schema::hubuumclass_relation, schema::hubuumobject_relation};

pub struct HubuumClassRelationID(pub i32);

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, Copy, PartialEq, Eq)]
#[diesel(table_name = hubuumclass_relation)]
pub struct HubuumClassRelation {
    pub id: i32,
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = hubuumclass_relation)]
pub struct NewHubuumClassRelation {
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::models::class::tests::create_class;
    use crate::models::traits::relation;
    use crate::models::{HubuumClass, Namespace};
    use crate::tests::{create_namespace, get_pool_and_config};
    use crate::traits::{
        CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors, SelfAccessors,
    };

    pub async fn create_namespace_and_classes(
        suffix: &str,
    ) -> (Namespace, HubuumClass, HubuumClass) {
        let (pool, _) = get_pool_and_config().await;

        let namespace = create_namespace(&pool, &format!("rel_test_{}", suffix))
            .await
            .unwrap();

        let class1 = create_class(&pool, &namespace, &format!("rel_class1_{}", suffix)).await;
        let class2 = create_class(&pool, &namespace, &format!("rel_class2_{}", suffix)).await;

        (namespace, class1, class2)
    }

    pub async fn verify_no_such_class_relation(pool: &DbPool, id: i32) {
        match HubuumClassRelationID(id).instance(pool).await {
            Ok(_) => panic!("Found a class relation that should not exist"),
            Err(ApiError::NotFound(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    pub async fn create_class_relation(
        pool: &DbPool,
        class1: HubuumClass,
        class2: HubuumClass,
    ) -> HubuumClassRelation {
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: class1.id,
            to_hubuum_class_id: class2.id,
        };

        let relation = relation.save(&pool).await.unwrap();

        assert_eq!(relation.from_hubuum_class_id, class1.id);
        assert_eq!(relation.to_hubuum_class_id, class2.id);

        let fetched_relation = HubuumClassRelationID(relation.id)
            .instance(&pool)
            .await
            .unwrap();

        assert_eq!(fetched_relation, relation);
        relation
    }

    #[actix_rt::test]
    async fn test_creating_class_relation() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("create_class").await;
        let relation = create_class_relation(&pool, class1, class2).await;
        namespace.delete(&pool).await.unwrap();
        verify_no_such_class_relation(&pool, relation.id).await;
    }

    #[actix_rt::test]
    async fn test_creating_class_relation_with_same_classes() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, _) = create_namespace_and_classes("same_classes").await;
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: class1.id,
            to_hubuum_class_id: class1.id,
        };

        match relation.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Should not be able to create a relation with the same classes"),
        }

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_class_relation_lowest_id_becomes_from() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("lowest_id").await;

        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: class2.id,
            to_hubuum_class_id: class1.id,
        }
        .save(&pool)
        .await
        .unwrap();

        // Check that the database actually swapped the order of the identifiers
        assert_eq!(relation.from_hubuum_class_id, class1.id);
        assert_eq!(relation.to_hubuum_class_id, class2.id);

        // Check that the original relation will give a conflict
        let old_relation = NewHubuumClassRelation {
            from_hubuum_class_id: class2.id,
            to_hubuum_class_id: class1.id,
        };
        match old_relation.save(&pool).await {
            Err(ApiError::Conflict(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Should not be able to create a relation with the same classes"),
        }

        namespace.delete(&pool).await.unwrap();

        verify_no_such_class_relation(&pool, relation.id).await;
    }
}
