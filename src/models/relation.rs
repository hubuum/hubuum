use diesel::prelude::*;
use diesel::sql_types::{Array, BigInt, Integer, Jsonb, Nullable, Text, Timestamp};

use std::{fmt, fmt::Display, slice};

use crate::db::DbPool;

use serde::{Deserialize, Serialize};

use crate::{
    errors::ApiError, schema::hubuumclass_closure, schema::hubuumclass_relation,
    schema::hubuumobject_relation,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubuumObjectRelationID(pub i32);

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, Copy, PartialEq, Eq)]
#[diesel(table_name = hubuumobject_relation)]
pub struct HubuumObjectRelation {
    pub id: i32,
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable)]
#[diesel(table_name = hubuumobject_relation)]
pub struct NewHubuumObjectRelation {
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
}

#[derive(
    Debug, Serialize, Deserialize, Queryable, QueryableByName, Selectable, Clone, PartialEq, Eq,
)]
#[diesel(table_name = hubuumclass_closure)]
pub struct HubuumClassClosure {
    #[diesel(sql_type = Integer)]
    pub ancestor_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub depth: i32,
    #[diesel(sql_type = Array<Nullable<Integer>>)]
    pub path: Vec<Option<i32>>,
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db::traits::ClassRelation;
    use crate::models::class::tests::create_class;
    use crate::models::object::tests::create_object;
    use crate::models::traits::class_relation;
    use crate::models::{HubuumClass, HubuumObject, Namespace};
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

    pub async fn verify_no_such_object_relation(pool: &DbPool, id: i32) {
        match HubuumObjectRelationID(id).instance(pool).await {
            Ok(_) => panic!("Found an object relation that should not exist"),
            Err(ApiError::NotFound(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    pub async fn create_class_relation(
        pool: &DbPool,
        class1: &HubuumClass,
        class2: &HubuumClass,
    ) -> HubuumClassRelation {
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: class1.id,
            to_hubuum_class_id: class2.id,
        };

        let relation = relation.save(&pool).await.unwrap();

        assert!(relation.from_hubuum_class_id < relation.to_hubuum_class_id);

        let correct_relation = if class1.id > class2.id {
            NewHubuumClassRelation {
                from_hubuum_class_id: class2.id,
                to_hubuum_class_id: class1.id,
            }
        } else {
            NewHubuumClassRelation {
                from_hubuum_class_id: class1.id,
                to_hubuum_class_id: class2.id,
            }
        };

        let fetched_relation = HubuumClassRelationID(relation.id)
            .instance(&pool)
            .await
            .unwrap();

        assert_eq!(fetched_relation.id, relation.id);
        assert_eq!(
            fetched_relation.from_hubuum_class_id,
            correct_relation.from_hubuum_class_id
        );
        assert_eq!(
            fetched_relation.to_hubuum_class_id,
            correct_relation.to_hubuum_class_id
        );
        relation
    }

    pub async fn create_object_relation(
        pool: &DbPool,
        object1: &HubuumObject,
        object2: &HubuumObject,
    ) -> HubuumObjectRelation {
        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
        };

        object_rel.save(&pool).await.unwrap()
    }

    #[actix_rt::test]
    async fn test_creating_class_relation() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("create_class").await;
        let relation = create_class_relation(&pool, &class1, &class2).await;
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
        let relation = create_class_relation(&pool, &class2, &class1).await;

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

    #[actix_rt::test]
    async fn test_deleting_class_relation() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("delete_class").await;
        let relation = create_class_relation(&pool, &class1, &class2).await;

        relation.delete(&pool).await.unwrap();
        verify_no_such_class_relation(&pool, relation.id).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_object_relation() {
        use crate::models::NewHubuumObject;
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("create_object").await;

        let nid = namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_create relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_create relation", json.clone())
            .await
            .unwrap();

        let _class_rel = create_class_relation(&pool, &class1, &class2).await;

        let class_relations: Vec<HubuumClassClosure> =
            class1.relations_to(&pool, &class2).await.unwrap();

        assert_eq!(class_relations.len(), 1);

        let object_rel = create_object_relation(&pool, &object1, &object2).await;

        assert_eq!(object_rel.from_hubuum_object_id, object1.id);
        assert_eq!(object_rel.to_hubuum_object_id, object2.id);

        let fetched_relation = HubuumObjectRelationID(object_rel.id)
            .instance(&pool)
            .await
            .unwrap();

        assert_eq!(fetched_relation, object_rel);
        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_object_relation_failure_class_mismatch() {
        use crate::db::traits::{ClassRelation, Relations};
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) =
            create_namespace_and_classes("create_object_class_mismatch").await;

        let nid = namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_fail relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_fail relation", json.clone())
            .await
            .unwrap();

        let class_relations: Vec<HubuumClassClosure> =
            HubuumClassClosure::relations(&pool, &class1, &class2)
                .await
                .unwrap();

        assert_eq!(
            class_relations.len(),
            0,
            "There should be no class relations between the two objects"
        );

        let class1_to_class2_relations = class1.relations_to(&pool, &class2).await.unwrap();
        let class2_to_class1_relations = class2.relations_to(&pool, &class1).await.unwrap();

        assert_eq!(class1_to_class2_relations.len(), 0);
        assert_eq!(class2_to_class1_relations.len(), 0);

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Creating a relation should fail when the classes of objects do not match the relation classes"),
        }

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object2.id,
            to_hubuum_object_id: object1.id,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!("Creating a relation should fail also when the order is flipped"),
        }

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {:?}", e),
            Ok(_) => panic!(
                "Should not be able to create object relations when class relation does not exist"
            ),
        }

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_deleting_object_relation() {
        let (pool, _) = get_pool_and_config().await;

        let (namespace, class1, class2) = create_namespace_and_classes("delete_object").await;

        let nid = namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_delete relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_delete relation", json.clone())
            .await
            .unwrap();

        // Create a class relation so that we can create an object relation
        let _class_rel = create_class_relation(&pool, &class1, &class2).await;
        let object_rel = create_object_relation(&pool, &object1, &object2).await;

        object_rel.delete(&pool).await.unwrap();
        verify_no_such_object_relation(&pool, object_rel.id).await;

        namespace.delete(&pool).await.unwrap();
    }
}
