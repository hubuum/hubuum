use diesel::prelude::*;
use diesel::sql_types::{Array, Bool, Integer, Jsonb, Nullable, Text, Timestamp};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::models::{HubuumClassWithPath, HubuumObjectWithPath};
use crate::{schema::hubuumclass_relation, schema::hubuumobject_relation};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HubuumClassRelationID(pub i32);

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, Copy, PartialEq, Eq, ToSchema)]
#[diesel(table_name = hubuumclass_relation)]
pub struct HubuumClassRelation {
    pub id: i32,
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable, ToSchema)]
#[schema(example = new_hubuum_class_relation_example)]
#[diesel(table_name = hubuumclass_relation)]
pub struct NewHubuumClassRelation {
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
}

/// To create new relations between classes from within a class
/// we only need the id of the class we want to relate to.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[schema(example = new_hubuum_class_relation_from_class_example)]
pub struct NewHubuumClassRelationFromClass {
    pub to_hubuum_class_id: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HubuumObjectRelationID(pub i32);

#[derive(Debug, Serialize, Deserialize, Queryable, Clone, Copy, PartialEq, Eq, ToSchema)]
#[diesel(table_name = hubuumobject_relation)]
pub struct HubuumObjectRelation {
    pub id: i32,
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
    pub class_relation_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable, ToSchema)]
#[schema(example = new_hubuum_object_relation_example)]
#[diesel(table_name = hubuumobject_relation)]
pub struct NewHubuumObjectRelation {
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
    pub class_relation_id: i32,
}

/// To create new relations between objects from within a
/// path where we already provide the class and object IDs
/// we only need the destination object ID.
#[derive(Debug, Serialize, Deserialize, Insertable, Clone, ToSchema)]
#[diesel(table_name = hubuumobject_relation)]
pub struct NewHubuumObjectRelationFromClassAndObject {
    pub to_hubuum_object_id: i32,
}

#[derive(Debug, Serialize, Deserialize, QueryableByName, Clone, PartialEq, Eq, ToSchema)]
pub struct HubuumClassRelationTransitive {
    #[diesel(sql_type = Integer)]
    pub ancestor_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub depth: i32,
    #[diesel(sql_type = Array<Nullable<Integer>>)]
    pub path: Vec<Option<i32>>,
}

#[derive(Debug, Serialize, Deserialize, QueryableByName, Clone)]
pub struct HubuumObjectTransitiveLink {
    #[diesel(sql_type = Integer)]
    target_object_id: i32,
    #[diesel(sql_type = Array<Integer>)]
    path: Vec<i32>,
}

#[derive(Debug, Queryable, QueryableByName, Serialize, Deserialize, Clone)]
pub struct ClassGraphRow {
    #[diesel(sql_type = Integer)]
    pub ancestor_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub depth: i32,
    #[diesel(sql_type = Array<Integer>)]
    pub path: Vec<i32>,
    #[diesel(sql_type = Text)]
    pub ancestor_name: String,
    #[diesel(sql_type = Text)]
    pub descendant_name: String,
    #[diesel(sql_type = Integer)]
    pub ancestor_namespace_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_namespace_id: i32,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub ancestor_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub descendant_json_schema: Option<serde_json::Value>,
    #[diesel(sql_type = Bool)]
    pub ancestor_validate_schema: bool,
    #[diesel(sql_type = Bool)]
    pub descendant_validate_schema: bool,
    #[diesel(sql_type = Text)]
    pub ancestor_description: String,
    #[diesel(sql_type = Text)]
    pub descendant_description: String,
    #[diesel(sql_type = Timestamp)]
    pub ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub descendant_updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Queryable, Serialize, Deserialize, Clone)]
pub struct ObjectGraphRow {
    pub ancestor_object_id: i32,
    pub descendant_object_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub ancestor_name: String,
    pub descendant_name: String,
    pub ancestor_namespace_id: i32,
    pub descendant_namespace_id: i32,
    pub ancestor_class_id: i32,
    pub descendant_class_id: i32,
    pub ancestor_description: String,
    pub descendant_description: String,
    pub ancestor_data: serde_json::Value,
    pub descendant_data: serde_json::Value,
    pub ancestor_created_at: chrono::NaiveDateTime,
    pub descendant_created_at: chrono::NaiveDateTime,
    pub ancestor_updated_at: chrono::NaiveDateTime,
    pub descendant_updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, QueryableByName, Serialize, Deserialize, Clone)]
pub struct RelatedObjectGraphRow {
    #[diesel(sql_type = Integer)]
    pub ancestor_object_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_object_id: i32,
    #[diesel(sql_type = Integer)]
    pub depth: i32,
    #[diesel(sql_type = Array<Integer>)]
    pub path: Vec<i32>,
    #[diesel(sql_type = Text)]
    pub ancestor_name: String,
    #[diesel(sql_type = Text)]
    pub descendant_name: String,
    #[diesel(sql_type = Integer)]
    pub ancestor_namespace_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_namespace_id: i32,
    #[diesel(sql_type = Integer)]
    pub ancestor_class_id: i32,
    #[diesel(sql_type = Integer)]
    pub descendant_class_id: i32,
    #[diesel(sql_type = Text)]
    pub ancestor_description: String,
    #[diesel(sql_type = Text)]
    pub descendant_description: String,
    #[diesel(sql_type = Jsonb)]
    pub ancestor_data: serde_json::Value,
    #[diesel(sql_type = Jsonb)]
    pub descendant_data: serde_json::Value,
    #[diesel(sql_type = Timestamp)]
    pub ancestor_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub descendant_created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub ancestor_updated_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub descendant_updated_at: chrono::NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct RelatedObjectGraph {
    pub objects: Vec<HubuumObjectWithPath>,
    pub relations: Vec<HubuumObjectRelation>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct RelatedClassGraph {
    pub classes: Vec<HubuumClassWithPath>,
    pub relations: Vec<HubuumClassRelation>,
}

#[allow(dead_code)]
fn new_hubuum_class_relation_example() -> NewHubuumClassRelation {
    NewHubuumClassRelation {
        from_hubuum_class_id: 1,
        to_hubuum_class_id: 2,
    }
}

#[allow(dead_code)]
fn new_hubuum_class_relation_from_class_example() -> NewHubuumClassRelationFromClass {
    NewHubuumClassRelationFromClass {
        to_hubuum_class_id: 2,
    }
}

#[allow(dead_code)]
fn new_hubuum_object_relation_example() -> NewHubuumObjectRelation {
    NewHubuumObjectRelation {
        from_hubuum_object_id: 10,
        to_hubuum_object_id: 20,
        class_relation_id: 3,
    }
}

#[cfg(test)]
pub mod tests {
    use rstest::rstest;

    use super::*;
    use crate::db::DbPool;
    use crate::db::traits::ClassRelation;
    use crate::errors::ApiError;
    use crate::models::class::tests::create_class;
    use crate::models::object::tests::create_object;
    use crate::models::{HubuumClass, HubuumObject};
    use crate::tests::{TestContext, TestScope, test_context};
    use crate::traits::{CanDelete, CanSave, SelfAccessors};

    pub async fn create_namespace_and_classes(
        suffix: &str,
    ) -> (crate::tests::NamespaceFixture, HubuumClass, HubuumClass) {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let namespace = scope.namespace_fixture(&format!("rel_test_{suffix}")).await;

        let class1 =
            create_class(&pool, &namespace.namespace, &format!("rel_class1_{suffix}")).await;
        let class2 =
            create_class(&pool, &namespace.namespace, &format!("rel_class2_{suffix}")).await;

        (namespace, class1, class2)
    }

    pub async fn verify_no_such_class_relation(pool: &DbPool, id: i32) {
        match HubuumClassRelationID(id).instance(pool).await {
            Ok(_) => panic!("Found a class relation that should not exist"),
            Err(ApiError::NotFound(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    pub async fn verify_no_such_object_relation(pool: &DbPool, id: i32) {
        match HubuumObjectRelationID(id).instance(pool).await {
            Ok(_) => panic!("Found an object relation that should not exist"),
            Err(ApiError::NotFound(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
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

        let relation = relation.save(pool).await.unwrap();

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
            .instance(pool)
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
        class1: &HubuumClassRelation,
        object1: &HubuumObject,
        object2: &HubuumObject,
    ) -> HubuumObjectRelation {
        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
            class_relation_id: class1.id,
        };

        object_rel.save(pool).await.unwrap()
    }

    #[actix_rt::test]
    async fn test_creating_class_relation() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) = create_namespace_and_classes("create_class").await;
        let relation = create_class_relation(&pool, &class1, &class2).await;
        namespace.cleanup().await.unwrap();
        verify_no_such_class_relation(&pool, relation.id).await;
    }

    #[actix_rt::test]
    async fn test_creating_class_relation_with_same_classes() {
        let pool = TestScope::new().pool;

        let (namespace, class1, _) = create_namespace_and_classes("same_classes").await;
        let relation = NewHubuumClassRelation {
            from_hubuum_class_id: class1.id,
            to_hubuum_class_id: class1.id,
        };

        match relation.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!("Should not be able to create a relation with the same classes"),
        }

        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_class_relation_lowest_id_becomes_from() {
        let pool = TestScope::new().pool;

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
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!("Should not be able to create a relation with the same classes"),
        }

        namespace.cleanup().await.unwrap();

        verify_no_such_class_relation(&pool, relation.id).await;
    }

    #[actix_rt::test]
    async fn test_deleting_class_relation() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) = create_namespace_and_classes("delete_class").await;
        let relation = create_class_relation(&pool, &class1, &class2).await;

        relation.delete(&pool).await.unwrap();
        verify_no_such_class_relation(&pool, relation.id).await;

        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_object_relation() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) = create_namespace_and_classes("create_object").await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_create relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_create relation", json.clone())
            .await
            .unwrap();

        let class_rel = create_class_relation(&pool, &class1, &class2).await;

        let class_relations: Vec<HubuumClassRelationTransitive> =
            class1.relations_to(&pool, &class2).await.unwrap();

        assert_eq!(class_relations.len(), 1);

        let object_rel = create_object_relation(&pool, &class_rel, &object1, &object2).await;

        assert_eq!(object_rel.from_hubuum_object_id, object1.id);
        assert_eq!(object_rel.to_hubuum_object_id, object2.id);

        let fetched_relation = HubuumObjectRelationID(object_rel.id)
            .instance(&pool)
            .await
            .unwrap();

        assert_eq!(fetched_relation, object_rel);
        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_object_relation_reverse_duplicate_conflicts() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) =
            create_namespace_and_classes("create_object_reverse").await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(
            &pool,
            class1.id,
            nid,
            "o1_create reverse relation",
            json.clone(),
        )
        .await
        .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_create reverse relation", json)
            .await
            .unwrap();

        let class_rel = create_class_relation(&pool, &class1, &class2).await;
        let object_rel = create_object_relation(&pool, &class_rel, &object1, &object2).await;

        let reverse_relation = NewHubuumObjectRelation {
            from_hubuum_object_id: object2.id,
            to_hubuum_object_id: object1.id,
            class_relation_id: class_rel.id,
        };

        match reverse_relation.save(&pool).await {
            Err(ApiError::Conflict(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!("Should not be able to create an inverse duplicate object relation"),
        }

        let fetched_relation = HubuumObjectRelationID(object_rel.id)
            .instance(&pool)
            .await
            .unwrap();

        assert_eq!(fetched_relation, object_rel);
        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_creating_object_relation_failure_class_mismatch() {
        use crate::db::traits::ClassRelation;
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) =
            create_namespace_and_classes("create_object_class_mismatch").await;

        let class3 = create_class(
            &pool,
            &namespace.namespace,
            "class3_create_object_class_mismatch",
        )
        .await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_fail relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_fail relation", json.clone())
            .await
            .unwrap();

        let class_relation_13 = create_class_relation(&pool, &class1, &class3).await;

        let class1_to_class2_relations = class1.relations_to(&pool, &class2).await.unwrap();
        let class2_to_class1_relations = class2.relations_to(&pool, &class1).await.unwrap();

        assert_eq!(class1_to_class2_relations.len(), 0);
        assert_eq!(class2_to_class1_relations.len(), 0);

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
            class_relation_id: class_relation_13.id,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!(
                "Creating a relation should fail when the classes of objects do not match the relation classes"
            ),
        }

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object2.id,
            to_hubuum_object_id: object1.id,
            class_relation_id: class_relation_13.id,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::BadRequest(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!("Creating a relation should fail also when the order is flipped"),
        }

        let object_rel = NewHubuumObjectRelation {
            from_hubuum_object_id: object1.id,
            to_hubuum_object_id: object2.id,
            class_relation_id: 999999999,
        };

        match object_rel.save(&pool).await {
            Err(ApiError::NotFound(_)) => {}
            Err(e) => panic!("Unexpected error: {e:?}"),
            Ok(_) => panic!(
                "Should not be able to create object relations when class relation does not exist"
            ),
        }

        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_deleting_object_relation() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) = create_namespace_and_classes("delete_object").await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_delete relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_delete relation", json.clone())
            .await
            .unwrap();

        // Create a class relation so that we can create an object relation
        let class_rel = create_class_relation(&pool, &class1, &class2).await;
        let object_rel = create_object_relation(&pool, &class_rel, &object1, &object2).await;

        object_rel.delete(&pool).await.unwrap();
        verify_no_such_object_relation(&pool, object_rel.id).await;

        namespace.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_deleting_class_relation_cascade() {
        let pool = TestScope::new().pool;

        let (namespace, class1, class2) =
            create_namespace_and_classes("delete_object_cascade").await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(&pool, class1.id, nid, "o1_delete relation", json.clone())
            .await
            .unwrap();
        let object2 = create_object(&pool, class2.id, nid, "o2_delete relation", json.clone())
            .await
            .unwrap();

        // Create a class relation so that we can create an object relation
        let class_rel = create_class_relation(&pool, &class1, &class2).await;
        let object_rel = create_object_relation(&pool, &class_rel, &object1, &object2).await;

        class_rel.delete(&pool).await.unwrap();
        verify_no_such_object_relation(&pool, object_rel.id).await;

        namespace.cleanup().await.unwrap();
    }

    #[rstest]
    #[actix_rt::test]
    async fn test_finding_object_relations(#[future(awt)] test_context: TestContext) {
        use crate::db::traits::ObjectRelationsFromUser;
        let context = test_context;
        let pool = &context.pool;

        let (namespace, class1, class2) =
            create_namespace_and_classes("find_object_relations").await;

        let nid = namespace.namespace.id;
        let json = serde_json::json!({"test": "data"});
        let object1 = create_object(
            pool,
            class1.id,
            nid,
            "o1_find_object relation",
            json.clone(),
        )
        .await
        .unwrap();
        let object2 = create_object(
            pool,
            class2.id,
            nid,
            "o2_find_object relation",
            json.clone(),
        )
        .await
        .unwrap();

        let class_rel = create_class_relation(pool, &class1, &class2).await;
        let object_rel = create_object_relation(pool, &class_rel, &object1, &object2).await;

        let rels = context
            .admin_user
            .get_related_objects(pool, &object1, &class2)
            .await
            .unwrap();

        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].target_object_id, object2.id);
        assert_eq!(rels[0].path, vec![object1.id, object2.id]);

        class_rel.delete(pool).await.unwrap();
        verify_no_such_object_relation(pool, object_rel.id).await;

        namespace.cleanup().await.unwrap();
    }

    /// Test that transitive object traversal works bidirectionally.
    /// Creates a chain: classA ↔ classB ↔ classC with objects oA, oB, oC
    /// linked oA-oB and oB-oC. Verifies that traversal from oC finds oA
    /// (i.e. walks "backwards" through the normalized from < to relations).
    #[rstest]
    #[actix_rt::test]
    async fn test_bidirectional_transitive_object_traversal(
        #[future(awt)] test_context: TestContext,
    ) {
        use crate::db::traits::ObjectRelationsFromUser;
        let context = test_context;
        let pool = &context.pool;

        let scope = TestScope::new();
        let ns = scope.namespace_fixture("bidir_obj_traversal").await;
        let nid = ns.namespace.id;

        let class_a = create_class(pool, &ns.namespace, "bidir_class_a").await;
        let class_b = create_class(pool, &ns.namespace, "bidir_class_b").await;
        let class_c = create_class(pool, &ns.namespace, "bidir_class_c").await;

        let rel_ab = create_class_relation(pool, &class_a, &class_b).await;
        let rel_bc = create_class_relation(pool, &class_b, &class_c).await;

        let json = serde_json::json!({});
        let obj_a = create_object(pool, class_a.id, nid, "bidir_oA", json.clone())
            .await
            .unwrap();
        let obj_b = create_object(pool, class_b.id, nid, "bidir_oB", json.clone())
            .await
            .unwrap();
        let obj_c = create_object(pool, class_c.id, nid, "bidir_oC", json.clone())
            .await
            .unwrap();

        create_object_relation(pool, &rel_ab, &obj_a, &obj_b).await;
        create_object_relation(pool, &rel_bc, &obj_b, &obj_c).await;

        // Forward: from oA, find objects of classC
        let forward = context
            .admin_user
            .get_related_objects(pool, &obj_a, &class_c)
            .await
            .unwrap();
        assert_eq!(forward.len(), 1, "Forward traversal A→C should find 1 object");
        assert_eq!(forward[0].target_object_id, obj_c.id);

        // Backward: from oC, find objects of classA — this is the key bidirectional test
        let backward = context
            .admin_user
            .get_related_objects(pool, &obj_c, &class_a)
            .await
            .unwrap();
        assert_eq!(
            backward.len(),
            1,
            "Backward traversal C→A should find 1 object"
        );
        assert_eq!(backward[0].target_object_id, obj_a.id);

        ns.cleanup().await.unwrap();
    }

    /// Test that transitive class relations are found bidirectionally.
    /// A ↔ B ↔ C: verify C sees a transitive relation to A.
    #[actix_rt::test]
    async fn test_bidirectional_transitive_class_relations() {
        use crate::db::traits::SelfRelations;
        let pool = TestScope::new().pool;

        let scope = TestScope::new();
        let ns = scope.namespace_fixture("bidir_class_trans").await;

        let class_a = create_class(&pool, &ns.namespace, "bidir_trans_a").await;
        let class_b = create_class(&pool, &ns.namespace, "bidir_trans_b").await;
        let class_c = create_class(&pool, &ns.namespace, "bidir_trans_c").await;

        create_class_relation(&pool, &class_a, &class_b).await;
        create_class_relation(&pool, &class_b, &class_c).await;

        // From A, should see transitive relations to B (depth 1) and C (depth 2)
        let from_a = class_a.transitive_relations(&pool).await.unwrap();
        let from_a_ids: Vec<i32> = from_a.iter().map(|r| r.descendant_class_id).collect();
        assert!(
            from_a_ids.contains(&class_b.id),
            "A should see B transitively"
        );
        assert!(
            from_a_ids.contains(&class_c.id),
            "A should see C transitively"
        );

        // From C, should see transitive relations to B (depth 1) and A (depth 2)
        let from_c = class_c.transitive_relations(&pool).await.unwrap();
        let from_c_ids: Vec<i32> = from_c.iter().map(|r| r.descendant_class_id).collect();
        assert!(
            from_c_ids.contains(&class_b.id),
            "C should see B transitively"
        );
        assert!(
            from_c_ids.contains(&class_a.id),
            "C should see A transitively"
        );

        ns.cleanup().await.unwrap();
    }

    /// Deleting one class relation in a chain should only clean up object relations
    /// that depended on it, leaving other object relations intact.
    /// Chain: A ↔ B ↔ C, with oA-oB and oB-oC.
    /// Delete B↔C → oB-oC should be removed, oA-oB should survive.
    #[actix_rt::test]
    async fn test_cleanup_scoped_to_deleted_class_relation() {
        let pool = TestScope::new().pool;

        let scope = TestScope::new();
        let ns = scope.namespace_fixture("cleanup_scoped").await;
        let nid = ns.namespace.id;

        let class_a = create_class(&pool, &ns.namespace, "cleanup_a").await;
        let class_b = create_class(&pool, &ns.namespace, "cleanup_b").await;
        let class_c = create_class(&pool, &ns.namespace, "cleanup_c").await;

        let rel_ab = create_class_relation(&pool, &class_a, &class_b).await;
        let rel_bc = create_class_relation(&pool, &class_b, &class_c).await;

        let json = serde_json::json!({});
        let obj_a = create_object(&pool, class_a.id, nid, "cleanup_oA", json.clone())
            .await
            .unwrap();
        let obj_b = create_object(&pool, class_b.id, nid, "cleanup_oB", json.clone())
            .await
            .unwrap();
        let obj_c = create_object(&pool, class_c.id, nid, "cleanup_oC", json.clone())
            .await
            .unwrap();

        let obj_rel_ab = create_object_relation(&pool, &rel_ab, &obj_a, &obj_b).await;
        let obj_rel_bc = create_object_relation(&pool, &rel_bc, &obj_b, &obj_c).await;

        // Delete class relation B↔C
        rel_bc.delete(&pool).await.unwrap();

        // oB-oC should be cleaned up
        verify_no_such_object_relation(&pool, obj_rel_bc.id).await;

        // oA-oB should survive — its class relation (A↔B) still exists
        let surviving = HubuumObjectRelationID(obj_rel_ab.id)
            .instance(&pool)
            .await;
        assert!(
            surviving.is_ok(),
            "Object relation A-B should survive when only B-C class relation is deleted"
        );

        ns.cleanup().await.unwrap();
    }

    /// When there's an alternative path (triangle: A↔B, A↔C, B↔C),
    /// deleting one class relation edge should NOT clean up object relations
    /// if the classes remain reachable via the other path.
    #[actix_rt::test]
    async fn test_cleanup_preserves_object_relations_with_alternative_path() {
        let pool = TestScope::new().pool;

        let scope = TestScope::new();
        let ns = scope.namespace_fixture("cleanup_alt_path").await;
        let nid = ns.namespace.id;

        let class_a = create_class(&pool, &ns.namespace, "altpath_a").await;
        let class_b = create_class(&pool, &ns.namespace, "altpath_b").await;
        let class_c = create_class(&pool, &ns.namespace, "altpath_c").await;

        // Triangle: A↔B, B↔C, A↔C
        let rel_ab = create_class_relation(&pool, &class_a, &class_b).await;
        let rel_bc = create_class_relation(&pool, &class_b, &class_c).await;
        let _rel_ac = create_class_relation(&pool, &class_a, &class_c).await;

        let json = serde_json::json!({});
        let obj_a = create_object(&pool, class_a.id, nid, "altpath_oA", json.clone())
            .await
            .unwrap();
        let obj_b = create_object(&pool, class_b.id, nid, "altpath_oB", json.clone())
            .await
            .unwrap();
        let obj_c = create_object(&pool, class_c.id, nid, "altpath_oC", json.clone())
            .await
            .unwrap();

        let obj_rel_ab = create_object_relation(&pool, &rel_ab, &obj_a, &obj_b).await;
        let obj_rel_bc = create_object_relation(&pool, &rel_bc, &obj_b, &obj_c).await;

        // Delete B↔C class relation. But A↔C still exists, so classes B and C
        // are still transitively reachable (B→A→C). However, the object relation
        // oB-oC was created under rel_bc which is now deleted — so it should be
        // cleaned up via CASCADE (class_relation_id FK), regardless of transitive
        // reachability.
        rel_bc.delete(&pool).await.unwrap();

        // oA-oB should survive — its class relation A↔B still exists
        let surviving_ab = HubuumObjectRelationID(obj_rel_ab.id)
            .instance(&pool)
            .await;
        assert!(
            surviving_ab.is_ok(),
            "Object relation A-B should survive"
        );

        // oB-oC was created under rel_bc which is now deleted — FK CASCADE removes it
        verify_no_such_object_relation(&pool, obj_rel_bc.id).await;

        ns.cleanup().await.unwrap();
    }
}
