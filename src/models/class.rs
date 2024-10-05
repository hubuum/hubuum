use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::schema::hubuumclass;

#[derive(Serialize, Deserialize, Queryable, Clone, PartialEq, Debug)]
#[diesel(table_name = hubuumclass )]
pub struct HubuumClass {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

// For retruning the IDs in raw sql queries, which is used
// to search in jsonb fields
#[derive(QueryableByName, Debug)]
#[diesel(table_name = hubuumclass)]
pub struct ClassIdResult {
    #[diesel(column_name = id)]
    pub id: i32,
}

#[derive(Serialize, Deserialize, Insertable, Clone, Debug)]
#[diesel(table_name = hubuumclass)]
pub struct NewHubuumClass {
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: String,
}

#[derive(Serialize, Deserialize, AsChangeset, Clone, Debug)]
#[diesel(table_name = hubuumclass)]
pub struct UpdateHubuumClass {
    pub name: Option<String>,
    pub namespace_id: Option<i32>,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HubuumClassWithPath {
    pub id: i32,
    pub name: String,
    pub namespace_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub path: Vec<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HubuumClassID(pub i32);

pub async fn total_class_count(pool: &DbPool) -> Result<i64, ApiError> {
    use crate::schema::hubuumclass::dsl::*;

    let count = with_connection(pool, |conn| hubuumclass.count().get_result::<i64>(conn))?;

    Ok(count)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::models::class::HubuumClass;
    use crate::models::namespace::Namespace;
    use crate::tests::{create_namespace, get_pool_and_config};
    use crate::traits::{CanDelete, CanSave, CanUpdate, ClassAccessors, NamespaceAccessors};

    pub async fn verify_no_such_class(pool: &DbPool, id: i32) {
        match HubuumClassID(id).class(pool).await {
            Ok(_) => panic!("Class should not exist"),
            Err(e) => match e {
                ApiError::NotFound(_) => {}
                _ => panic!("Unexpected error: {:?}", e),
            },
        }
    }

    pub async fn get_class(id: i32, pool: &DbPool) -> HubuumClass {
        HubuumClassID(id).class(pool).await.unwrap()
    }

    pub async fn create_class(
        pool: &DbPool,
        namespace: &Namespace,
        class_name: &str,
    ) -> HubuumClass {
        let class = NewHubuumClass {
            name: class_name.to_string(),
            namespace_id: namespace.id,
            json_schema: None,
            validate_schema: None,
            description: "test".to_string(),
        };

        class.save(pool).await.unwrap()
    }

    #[actix_rt::test]
    async fn test_creating_class_and_cascade_delete() {
        let (pool, _) = get_pool_and_config().await;

        let namespace = create_namespace(&pool, "test").await.unwrap();
        //        let admin_group = ensure_admin_group(&pool).await;

        let class_name = "test_creating_class";
        let class = create_class(&pool, &namespace, class_name).await;

        assert_eq!(class.namespace_id(&pool).await.unwrap(), namespace.id);
        assert_eq!(class.name, class_name);
        assert_eq!(class.description, "test");
        assert_eq!(class.json_schema, None);

        let fetched_class = get_class(class.id, &pool).await;

        assert_eq!(fetched_class, class);

        // Deleting the namespace should cascade away the class
        namespace.delete(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;
    }

    #[actix_rt::test]
    async fn test_updating_class_and_deleting_it() {
        let (pool, _) = get_pool_and_config().await;
        let namespace = create_namespace(&pool, "updating_class").await.unwrap();
        let class = create_class(&pool, &namespace, "test_updating_class").await;

        let update = UpdateHubuumClass {
            name: Some("test update 2".to_string()),
            namespace_id: None,
            json_schema: None,
            validate_schema: None,
            description: None,
        };

        let updated_class = update.update(&pool, class.id).await.unwrap();

        assert_eq!(updated_class.id, class.id);
        assert_eq!(updated_class.name, "test update 2");
        assert_eq!(updated_class.namespace_id, class.namespace_id);
        assert_eq!(updated_class.json_schema, class.json_schema);
        assert_eq!(updated_class.validate_schema, class.validate_schema);
        assert_eq!(updated_class.description, class.description);

        updated_class.delete(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;

        namespace.delete(&pool).await.unwrap();
    }

    #[actix_rt::test]
    async fn test_saving_after_changing_class() {
        let (pool, _) = get_pool_and_config().await;
        let namespace = create_namespace(&pool, "test_saving_after_changing_class")
            .await
            .unwrap();
        let mut class = create_class(&pool, &namespace, "test saving").await;

        class.description = "new description".to_string();
        class.save(&pool).await.unwrap();

        let fetched_class = get_class(class.id, &pool).await;

        assert_eq!(fetched_class.description, "new description");

        namespace.delete(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;
    }
}
