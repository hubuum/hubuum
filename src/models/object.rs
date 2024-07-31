use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text, Timestamp};
use serde::{Deserialize, Serialize};

use crate::db::DbPool;
use crate::errors::ApiError;
use crate::schema::hubuumobject;

#[derive(Serialize, Deserialize, Queryable, Clone, PartialEq, Debug, QueryableByName)]
#[diesel(table_name = hubuumobject)]
pub struct HubuumObject {
    #[diesel(sql_type = Integer)]
    pub id: i32,
    #[diesel(sql_type = Text)]
    pub name: String,
    #[diesel(sql_type = Integer)]
    pub namespace_id: i32,
    #[diesel(sql_type = Integer)]
    pub hubuum_class_id: i32,
    #[diesel(sql_type = Jsonb)]
    pub data: serde_json::Value,
    #[diesel(sql_type = Text)]
    pub description: String,

    #[diesel(sql_type = Timestamp)]
    pub created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Deserialize, Clone, Insertable)]
#[diesel(table_name = hubuumobject)]
pub struct NewHubuumObject {
    pub name: String,
    pub namespace_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
}
#[derive(Serialize, Deserialize, Clone, AsChangeset)]
#[diesel(table_name = hubuumobject)]
pub struct UpdateHubuumObject {
    pub name: Option<String>,
    pub namespace_id: Option<i32>,
    pub hubuum_class_id: Option<i32>,
    pub data: Option<serde_json::Value>,
    pub description: Option<String>,
}

// For retruning the IDs in raw sql queries, which is used
// to search in jsonb fields
#[derive(QueryableByName, Debug)]
#[diesel(table_name = hubuumobject)]
pub struct ObjectIDResult {
    #[diesel(column_name = id)]
    pub id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HubuumObjectID(pub i32);

// For objects per class.
#[derive(QueryableByName, Debug, Serialize, Deserialize)]
pub struct ObjectsByClass {
    #[diesel(sql_type = Integer)]
    pub hubuum_class_id: i32,
    #[diesel(sql_type = BigInt)]
    pub count: i64,
}

pub async fn total_object_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumobject::dsl::*;

    let mut conn = pool.get()?;
    let count = hubuumobject.count().get_result::<i64>(&mut conn)?;

    Ok(count)
}

pub async fn objects_per_class_count(pool: &DbPool) -> Result<Vec<ObjectsByClass>, ApiError> {
    let mut conn = pool.get()?;

    let raw_query =
        "SELECT hubuum_class_id, COUNT(*) as count FROM hubuumobject GROUP BY hubuum_class_id";
    let results = sql_query(raw_query).load::<ObjectsByClass>(&mut conn)?;

    Ok(results)
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::models::class::tests::{create_class, verify_no_such_class};
    use crate::tests::{create_namespace, get_pool_and_config};

    use crate::models::class::HubuumClass;
    use crate::models::namespace::Namespace;
    use crate::traits::{CanDelete, CanSave, SelfAccessors};

    #[allow(dead_code)]
    async fn setup_test_objects(
        pool: &DbPool,
        namespace: &Namespace,
        class: &HubuumClass,
    ) -> Vec<HubuumObject> {
        let simple_data = serde_json::json!({"key": "value"});
        let nested_data = serde_json::json!({"key": "value", "nested": {"key": "nested_value"}});
        let list_data = serde_json::json!({"key": "value", "list": [1, 2, 3]});

        let nid = namespace.id;
        let hid = class.id;

        let test_objects = vec![
            ("Object 1", hid, nid, simple_data.clone()),
            ("Object 2", hid, nid, simple_data.clone()),
            ("Object 3", hid, nid, simple_data.clone()),
            ("Object 4", hid, nid, nested_data.clone()),
            ("Object 5", hid, nid, nested_data.clone()),
            ("Object 6", hid, nid, list_data.clone()),
        ];

        let mut ret_vec = Vec::new();

        for (name, hid, nid, object_data) in test_objects {
            ret_vec.push(
                create_object(pool, hid, nid, name, object_data)
                    .await
                    .unwrap(),
            );
        }
        ret_vec
    }

    pub async fn verify_no_such_object(pool: &DbPool, object_id: i32) {
        use crate::schema::hubuumobject::dsl::*;

        let mut conn = pool.get().unwrap();
        let result = hubuumobject
            .filter(id.eq(object_id))
            .first::<HubuumObject>(&mut conn);

        match result {
            Ok(_) => panic!("Object {} should not exist", object_id),
            Err(diesel::result::Error::NotFound) => (),
            Err(e) => panic!("Error: {}", e),
        }
    }

    pub async fn create_object(
        pool: &DbPool,
        hubuum_class_id: i32,
        namespace_id: i32,
        object_name: &str,
        object_data: serde_json::Value,
    ) -> Result<HubuumObject, ApiError> {
        let object = NewHubuumObject {
            name: object_name.to_string(),
            namespace_id,
            hubuum_class_id,
            data: object_data,
            description: "Test object".to_string(),
        };
        object.save(pool).await
    }

    pub async fn get_object(pool: &DbPool, object_id: i32) -> HubuumObject {
        let object = HubuumObjectID(object_id);
        let object = object.instance(pool).await.unwrap();
        object
    }

    #[actix_rt::test]
    async fn test_creating_object_manual_delete() {
        let (pool, _) = get_pool_and_config().await;
        let namespace = create_namespace(&pool, "object_manual_test").await.unwrap();
        let class = create_class(&pool, &namespace, "test creating object").await;

        let obj_name = "test manual object creation";

        let object_data = serde_json::json!({"test": "data"});

        let object = create_object(&pool, class.id, namespace.id, obj_name, object_data.clone())
            .await
            .unwrap();
        assert_eq!(object.name, obj_name);

        let fetched_object = get_object(&pool, object.id).await;
        assert_eq!(fetched_object.name, obj_name);
        assert_eq!(fetched_object, object);
        assert_eq!(fetched_object.data, object_data);

        fetched_object.delete(&pool).await.unwrap();
        verify_no_such_object(&pool, object.id).await;

        class.delete(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;

        namespace.delete(&pool).await.unwrap();
    }
}
