use crate::db::prelude::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::class::total_class_count_from_backend;
use crate::errors::ApiError;
use crate::permissions::{AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef};
use crate::schema::hubuumclass;
use crate::traits::{BackendContext, SelfAccessors};

#[derive(Serialize, Deserialize, Queryable, QueryableByName, Clone, PartialEq, Debug, ToSchema)]
#[diesel(table_name = hubuumclass )]
pub struct HubuumClass {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
}

#[derive(Serialize, Deserialize, Insertable, Clone, Debug, ToSchema)]
#[schema(example = new_hubuum_class_example)]
#[diesel(table_name = hubuumclass)]
pub struct NewHubuumClass {
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: String,
}

#[derive(Serialize, Deserialize, AsChangeset, Clone, Debug, ToSchema)]
#[schema(example = update_hubuum_class_example)]
#[diesel(table_name = hubuumclass)]
pub struct UpdateHubuumClass {
    pub name: Option<String>,
    pub collection_id: Option<i32>,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: Option<String>,
}

impl UpdateHubuumClass {
    pub(crate) fn has_changes(&self, current: &HubuumClass) -> bool {
        self.name
            .as_ref()
            .is_some_and(|value| value != &current.name)
            || self
                .collection_id
                .is_some_and(|value| value != current.collection_id)
            || self
                .json_schema
                .as_ref()
                .is_some_and(|value| Some(value) != current.json_schema.as_ref())
            || self
                .validate_schema
                .is_some_and(|value| value != current.validate_schema)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
    }
}

impl HubuumClass {
    /// Enforce the collection boundary shared by class-scoped domain records.
    pub(crate) fn ensure_in_collection(
        &self,
        target_collection_id: i32,
        entity_kind: &str,
    ) -> Result<(), ApiError> {
        if self.collection_id != target_collection_id {
            return Err(ApiError::BadRequest(format!(
                "{entity_kind} class {} belongs to collection {}, not target collection {}",
                self.id, self.collection_id, target_collection_id
            )));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct HubuumClassWithPath {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub path: Vec<i32>,
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`HubuumClass`].
    pub struct HubuumClassID;
    noun = "class id";
}

/// A normalized set of class ids: deduplicated, sorted ascending, and guaranteed positive.
///
/// Construct via [`ClassIdSet::new`]; the inner vec stays private so the "sorted, deduped,
/// positive" invariant holds for every consumer — including callers that `binary_search` the
/// set and rely on the ordering. Bulk class-keyed backend lookups hang off this type (see
/// `crate::db::traits::class`).
#[derive(Debug, Clone)]
pub(crate) struct ClassIdSet(Vec<i32>);

impl ClassIdSet {
    /// Normalize an iterator of class ids into a set, rejecting non-positive ids.
    pub(crate) fn new(ids: impl IntoIterator<Item = i32>) -> Result<Self, ApiError> {
        let mut ids = ids.into_iter().collect::<Vec<_>>();
        if ids.iter().any(|class_id| *class_id <= 0) {
            return Err(ApiError::BadRequest(
                "class ids must be greater than 0".to_string(),
            ));
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(Self(ids))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The normalized ids, sorted ascending and deduplicated.
    pub(crate) fn as_slice(&self) -> &[i32] {
        &self.0
    }
}

pub async fn total_class_count<C>(backend: &C) -> Result<i64, ApiError>
where
    C: BackendContext + ?Sized,
{
    total_class_count_from_backend(backend.db_pool()).await
}

fn new_hubuum_class_example() -> NewHubuumClass {
    NewHubuumClass {
        name: "server".to_string(),
        collection_id: 1,
        json_schema: None,
        validate_schema: Some(false),
        description: "Server inventory class".to_string(),
    }
}

fn update_hubuum_class_example() -> UpdateHubuumClass {
    UpdateHubuumClass {
        name: Some("server".to_string()),
        collection_id: Some(1),
        json_schema: None,
        validate_schema: Some(true),
        description: Some("Validated server inventory class".to_string()),
    }
}

#[derive(serde::Serialize, diesel::Queryable, Clone, Debug, ToSchema)]
#[diesel(table_name = crate::schema::hubuumclass_history)]
pub struct HubuumClassHistory {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(HubuumClassHistory, "hubuumclass_history");

#[async_trait]
impl AuthzTarget for HubuumClass {
    async fn to_resource_ref(&self, _pool: &DbPool) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef {
            kind: ResourceKind::Class,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: Some(self.collection_id),
                name: Some(self.name.clone()),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for HubuumClassID {
    async fn to_resource_ref(&self, pool: &DbPool) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db::DbPool;
    use crate::models::class::HubuumClass;
    use crate::models::collection::Collection;
    use crate::tests::TestScope;
    use crate::traits::{CanDelete, CanSave, CanUpdate, ClassAccessors, CollectionAccessors};

    pub async fn verify_no_such_class(pool: &DbPool, id: i32) {
        match HubuumClassID(id).class(pool).await {
            Ok(_) => panic!("Class should not exist"),
            Err(e) => match e {
                ApiError::NotFound(_) => {}
                _ => panic!("Unexpected error: {e:?}"),
            },
        }
    }

    pub async fn get_class(id: i32, pool: &DbPool) -> HubuumClass {
        HubuumClassID(id).class(pool).await.unwrap()
    }

    pub async fn create_class(
        pool: &DbPool,
        collection: &Collection,
        class_name: &str,
    ) -> HubuumClass {
        let class = NewHubuumClass {
            name: class_name.to_string(),
            collection_id: collection.id,
            json_schema: None,
            validate_schema: None,
            description: "test".to_string(),
        };

        class.save_without_events(pool).await.unwrap()
    }

    #[actix_rt::test]
    async fn test_creating_class_and_cascade_delete() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();

        let collection = scope.collection_fixture("test").await;
        //        let admin_group = ensure_admin_group(&pool).await;

        let class_name = "test_creating_class";
        let class = create_class(&pool, &collection.collection, class_name).await;

        assert_eq!(
            class.collection_id(&pool).await.unwrap().id(),
            collection.collection.id
        );
        assert_eq!(class.name, class_name);
        assert_eq!(class.description, "test");
        assert_eq!(class.json_schema, None);

        let fetched_class = get_class(class.id, &pool).await;

        assert_eq!(fetched_class, class);

        // Deleting the collection should cascade away the class
        collection.cleanup().await.unwrap();
        verify_no_such_class(&pool, class.id).await;
    }

    #[actix_rt::test]
    async fn test_updating_class_and_deleting_it() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let collection = scope.collection_fixture("updating_class").await;
        let class = create_class(&pool, &collection.collection, "test_updating_class").await;

        let update = UpdateHubuumClass {
            name: Some("test update 2".to_string()),
            collection_id: None,
            json_schema: None,
            validate_schema: None,
            description: None,
        };

        let updated_class = update.update_without_events(&pool, class.id).await.unwrap();

        assert_eq!(updated_class.id, class.id);
        assert_eq!(updated_class.name, "test update 2");
        assert_eq!(updated_class.collection_id, class.collection_id);
        assert_eq!(updated_class.json_schema, class.json_schema);
        assert_eq!(updated_class.validate_schema, class.validate_schema);
        assert_eq!(updated_class.description, class.description);

        updated_class.delete_without_events(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;

        collection.cleanup().await.unwrap();
    }

    #[actix_rt::test]
    async fn test_saving_after_changing_class() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let collection = scope
            .collection_fixture("test_saving_after_changing_class")
            .await;
        let mut class = create_class(&pool, &collection.collection, "test saving").await;

        class.description = "new description".to_string();
        class.save_without_events(&pool).await.unwrap();

        let fetched_class = get_class(class.id, &pool).await;

        assert_eq!(fetched_class.description, "new description");

        collection.cleanup().await.unwrap();
        verify_no_such_class(&pool, class.id).await;
    }
}
