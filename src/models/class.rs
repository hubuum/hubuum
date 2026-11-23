use hubuum_storage_core::StorageClass;

use crate::storage::{StorageClassSelector, StorageResolvedClass};
use async_trait::async_trait;
use hubuum_domain::CollectionId;
use hubuum_storage_core::StorageClassCreate;
use hubuum_storage_core::StorageClassSchemaPolicy;
use hubuum_storage_core::StorageClassUpdate;
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;

use crate::errors::ApiError;
use crate::models::ResourceRevision;
use crate::permissions::{AuthzTarget, ResourceRef};
use crate::traits::SelfAccessors;

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, ToSchema)]
pub struct HubuumClass {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: bool,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

impl From<StorageClass> for HubuumClass {
    fn from(row: StorageClass) -> Self {
        let (
            id,
            name,
            collection_id,
            json_schema,
            validate_schema,
            description,
            created_at,
            updated_at,
            revision,
        ) = row.into_parts();
        Self {
            id: id.id(),
            name,
            collection_id: collection_id.id(),
            json_schema,
            validate_schema,
            description,
            created_at: created_at.naive_utc(),
            updated_at: updated_at.naive_utc(),
            revision,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[schema(example = new_hubuum_class_example)]
pub struct NewHubuumClass {
    pub name: String,
    pub collection_id: i32,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: String,
}

impl TryFrom<NewHubuumClass> for StorageClassCreate {
    type Error = ApiError;

    fn try_from(command: NewHubuumClass) -> Result<Self, Self::Error> {
        let schema_policy = StorageClassSchemaPolicy::try_from_parts(
            command.json_schema,
            command.validate_schema.unwrap_or(false),
        )
        .map_err(|error| ApiError::from(error.into_request_error()))?;
        Ok(StorageClassCreate::builder(
            command.name,
            CollectionId::new(command.collection_id)?,
            command.description,
        )
        .schema_policy(schema_policy)
        .build())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[schema(example = update_hubuum_class_example)]
pub struct UpdateHubuumClass {
    pub name: Option<String>,
    pub collection_id: Option<i32>,
    pub json_schema: Option<serde_json::Value>,
    pub validate_schema: Option<bool>,
    pub description: Option<String>,
}

impl TryFrom<UpdateHubuumClass> for StorageClassUpdate {
    type Error = ApiError;

    fn try_from(update: UpdateHubuumClass) -> Result<Self, Self::Error> {
        Ok(StorageClassUpdate::builder()
            .name(update.name)
            .collection_id(update.collection_id.map(CollectionId::new).transpose()?)
            .json_schema(update.json_schema)
            .validate_schema(update.validate_schema)
            .description(update.description)
            .build())
    }
}

impl UpdateHubuumClass {
    /// Validate the schema state that would result from applying this update to `current`.
    #[cfg(test)]
    pub(crate) fn validate_schema_update(&self, current: &HubuumClass) -> Result<(), ApiError> {
        if self.json_schema.is_none() && self.validate_schema.is_none() {
            return Ok(());
        }

        let schema = self.json_schema.as_ref().or(current.json_schema.as_ref());
        if let Some(schema) = schema {
            crate::utilities::json_schema::validate_json_schema(schema)?;
            if self.validate_schema.unwrap_or(current.validate_schema) {
                crate::utilities::json_schema::compile_json_schema(schema)?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
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
    pub(crate) fn authorization_resource(&self) -> ResourceRef {
        ResourceRef::class(self.id, self.collection_id, Some(self.name.clone()))
    }

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
    pub revision: ResourceRevision,
    pub path: Vec<i32>,
}

pub use hubuum_domain::ClassId as HubuumClassID;

/// Explicit route-selected address for a class.
///
/// The route chooses the constructor, so [`Self::by_name`] never interprets a numeric-looking
/// class name as an ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassSelector(StorageClassSelector);

impl ClassSelector {
    pub fn by_id(class_id: HubuumClassID) -> Self {
        Self(StorageClassSelector::Id(class_id))
    }

    pub fn by_name(class_name: impl Into<String>) -> Self {
        Self(StorageClassSelector::Name(class_name.into()))
    }
}

impl From<ClassSelector> for StorageClassSelector {
    fn from(selector: ClassSelector) -> Self {
        selector.0
    }
}

/// A class resolved from one explicit selector and safe to carry from authorization into a
/// selector-aware mutation.
#[derive(Clone)]
pub struct ResolvedClassTarget {
    storage: StorageResolvedClass,
    class: HubuumClass,
}

impl From<StorageResolvedClass> for ResolvedClassTarget {
    /// Preserve the validated aggregate; private views serve domain and HTTP callers.
    fn from(storage: StorageResolvedClass) -> Self {
        Self {
            class: HubuumClass::from(storage.class().clone()),
            storage,
        }
    }
}

impl ResolvedClassTarget {
    pub(crate) fn as_storage(&self) -> &StorageResolvedClass {
        &self.storage
    }

    pub fn class(&self) -> &HubuumClass {
        &self.class
    }
}

/// A normalized set of class ids: deduplicated, sorted ascending, and guaranteed positive.
///
/// Construct via [`ClassIdSet::new`]; the inner vec stays private so the "sorted, deduped,
/// positive" invariant holds for every consumer — including callers that `binary_search` the
/// set and rely on the ordering. Storage contracts can accept this type without trusting callers
/// to normalize class identifiers themselves.
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

#[derive(serde::Serialize, Clone, Debug, ToSchema)]
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
    pub actor_kind: Option<String>,
    pub initiator_user_id: Option<i32>,
    pub task_id: Option<i32>,
    pub revision: ResourceRevision,
}

impl crate::traits::CursorPaginated for HubuumClassHistory {
    fn supports_sort(field: &crate::models::search::FilterField) -> bool {
        matches!(
            field,
            crate::models::search::FilterField::HistoryId
                | crate::models::search::FilterField::Revision
        )
    }

    fn cursor_value(
        &self,
        field: &crate::models::search::FilterField,
    ) -> Result<crate::traits::CursorValue, ApiError> {
        Ok(match field {
            crate::models::search::FilterField::HistoryId => {
                crate::traits::CursorValue::Integer(self.history_id)
            }
            crate::models::search::FilterField::Revision => {
                crate::traits::CursorValue::Integer(self.revision.get())
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "Field '{other}' is not orderable for history"
                )));
            }
        })
    }

    fn default_sort() -> Vec<crate::models::search::SortParam> {
        vec![crate::models::search::SortParam {
            field: crate::models::search::FilterField::HistoryId,
            descending: true,
        }]
    }

    fn tie_breaker_sort() -> Vec<crate::models::search::SortParam> {
        Self::default_sort()
    }
}

#[async_trait]
impl AuthzTarget for HubuumClass {
    async fn to_resource_ref(
        &self,
        _pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        Ok(self.authorization_resource())
    }
}

#[async_trait]
impl AuthzTarget for HubuumClassID {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

impl fmt::Debug for ResolvedClassTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedClassTarget")
            .field("selector", self.storage.selector())
            .field("class", &self.class)
            .finish()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::models::class::HubuumClass;
    use crate::models::collection::Collection;
    use crate::tests::TestScope;
    use crate::traits::{CanDelete, CanSave, CanUpdate, ClassAccessors, CollectionAccessors};

    pub async fn verify_no_such_class(pool: &impl crate::storage::StorageContext, id: i32) {
        match HubuumClassID::new(id).unwrap().class(pool).await {
            Ok(_) => panic!("Class should not exist"),
            Err(e) => match e {
                ApiError::NotFound(_) => {}
                _ => panic!("Unexpected error: {e:?}"),
            },
        }
    }

    pub async fn get_class(id: i32, pool: &impl crate::storage::StorageContext) -> HubuumClass {
        HubuumClassID::new(id).unwrap().class(pool).await.unwrap()
    }

    pub async fn create_class(
        pool: &impl crate::storage::StorageContext,
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

        let updated_class = update
            .update_without_events(&pool, HubuumClassID::new(class.id).unwrap())
            .await
            .unwrap();

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
