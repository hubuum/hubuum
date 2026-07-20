use crate::db::prelude::*;
use async_trait::async_trait;
use diesel::sql_types::{BigInt, Integer, Jsonb, Text, Timestamp};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::db::DbPool;
use crate::db::traits::object::{
    objects_per_class_count_from_backend, total_object_count_from_backend,
};
#[cfg(test)]
use crate::db::with_connection;
use crate::errors::ApiError;
use crate::models::class::{HubuumClass, HubuumClassID};
use crate::models::computed_field::HubuumObjectComputedResponse;
use crate::permissions::{AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef};
use crate::schema::hubuumobject;
use crate::traits::{BackendContext, SelfAccessors};

#[derive(Serialize, Deserialize, Queryable, Clone, PartialEq, Debug, QueryableByName, ToSchema)]
#[diesel(table_name = hubuumobject)]
pub struct HubuumObject {
    #[diesel(sql_type = Integer)]
    pub id: i32,
    #[diesel(sql_type = Text)]
    pub name: String,
    #[diesel(sql_type = Integer)]
    pub collection_id: i32,
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

#[derive(Serialize, Deserialize, Clone, Insertable, ToSchema)]
#[schema(example = new_hubuum_object_example)]
#[diesel(table_name = hubuumobject)]
pub struct NewHubuumObject {
    pub name: String,
    pub collection_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
}

impl NewHubuumObject {
    pub(crate) fn validate_for_class(&self, class: &HubuumClass) -> Result<(), ApiError> {
        if self.hubuum_class_id != class.id {
            return Err(ApiError::BadRequest(format!(
                "Object hubuum_class_id {} does not match path class_id {}",
                self.hubuum_class_id, class.id
            )));
        }
        if self.collection_id != class.collection_id {
            return Err(ApiError::BadRequest(format!(
                "Object collection_id {} does not match class collection_id {}",
                self.collection_id, class.collection_id
            )));
        }
        if class.validate_schema
            && let Some(ref schema) = class.json_schema
        {
            crate::utilities::json_schema::validate_json_value(schema, &self.data)?;
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize, Clone, AsChangeset, ToSchema)]
#[schema(example = update_hubuum_object_example)]
#[diesel(table_name = hubuumobject)]
pub struct UpdateHubuumObject {
    pub name: Option<String>,
    pub collection_id: Option<i32>,
    pub hubuum_class_id: Option<i32>,
    pub data: Option<serde_json::Value>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
enum ComputedInputPresence {
    #[default]
    Absent,
    Present,
}

impl<'de> Deserialize<'de> for ComputedInputPresence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let _ = serde_json::Value::deserialize(deserializer)?;
        Ok(Self::Present)
    }
}

#[derive(Deserialize, ToSchema)]
pub struct NewHubuumObjectRequest {
    pub name: String,
    pub collection_id: Option<i32>,
    pub hubuum_class_id: Option<i32>,
    pub data: serde_json::Value,
    pub description: String,
    #[serde(default)]
    #[schema(ignore)]
    computed: ComputedInputPresence,
}

impl NewHubuumObjectRequest {
    pub fn into_domain_for_class(self, class: &HubuumClass) -> Result<NewHubuumObject, ApiError> {
        if matches!(self.computed, ComputedInputPresence::Present) {
            return Err(ApiError::BadRequest(
                "computed is response-only and cannot be supplied when creating an object"
                    .to_string(),
            ));
        }
        if let Some(class_id) = self.hubuum_class_id
            && class_id != class.id
        {
            return Err(ApiError::BadRequest(format!(
                "Object hubuum_class_id {class_id} does not match path class_id {}",
                class.id
            )));
        }
        if let Some(collection_id) = self.collection_id
            && collection_id != class.collection_id
        {
            return Err(ApiError::BadRequest(format!(
                "Object collection_id {collection_id} does not match class collection_id {}",
                class.collection_id
            )));
        }
        Ok(NewHubuumObject {
            name: self.name,
            collection_id: class.collection_id,
            hubuum_class_id: class.id,
            data: self.data,
            description: self.description,
        })
    }
}

#[derive(Deserialize, ToSchema)]
pub struct UpdateHubuumObjectRequest {
    pub name: Option<String>,
    pub collection_id: Option<i32>,
    pub hubuum_class_id: Option<i32>,
    pub data: Option<serde_json::Value>,
    pub description: Option<String>,
    #[serde(default)]
    #[schema(ignore)]
    computed: ComputedInputPresence,
}

impl UpdateHubuumObjectRequest {
    pub fn into_domain(self) -> Result<UpdateHubuumObject, ApiError> {
        if matches!(self.computed, ComputedInputPresence::Present) {
            return Err(ApiError::BadRequest(
                "computed is response-only and cannot be supplied when updating an object"
                    .to_string(),
            ));
        }
        Ok(UpdateHubuumObject {
            name: self.name,
            collection_id: self.collection_id,
            hubuum_class_id: self.hubuum_class_id,
            data: self.data,
            description: self.description,
        })
    }
}

impl UpdateHubuumObject {
    pub(crate) fn validate_for_class(
        &self,
        current: &HubuumObject,
        class: &HubuumClass,
    ) -> Result<(), ApiError> {
        let merged = current.merge_update(self);
        if merged.hubuum_class_id != class.id {
            return Err(ApiError::BadRequest(format!(
                "Object hubuum_class_id {} does not match class {}",
                merged.hubuum_class_id, class.id
            )));
        }
        if merged.collection_id != class.collection_id {
            return Err(ApiError::BadRequest(format!(
                "Object collection_id {} does not match class collection_id {}",
                merged.collection_id, class.collection_id
            )));
        }
        if class.validate_schema
            && let Some(schema) = class.json_schema.as_ref()
        {
            crate::utilities::json_schema::validate_json_value(schema, &merged.data)?;
        }
        Ok(())
    }

    pub(crate) fn has_changes(&self, current: &HubuumObject) -> bool {
        self.name
            .as_ref()
            .is_some_and(|value| value != &current.name)
            || self
                .collection_id
                .is_some_and(|value| value != current.collection_id)
            || self
                .hubuum_class_id
                .is_some_and(|value| value != current.hubuum_class_id)
            || self
                .data
                .as_ref()
                .is_some_and(|value| value != &current.data)
            || self
                .description
                .as_ref()
                .is_some_and(|value| value != &current.description)
    }
}

crate::int_id_newtype! {
    /// Identifier wrapper for a [`HubuumObject`].
    pub struct HubuumObjectID;
    noun = "object id";
}

/// Explicit route-selected address for an object.
///
/// Callers choose the constructor from the route shape. In particular, [`Self::by_name`] never
/// attempts to parse numeric-looking names as IDs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectSelector(ObjectSelectorKind);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObjectSelectorKind {
    ById {
        class_id: HubuumClassID,
        object_id: HubuumObjectID,
    },
    ByName {
        class_name: String,
        object_name: String,
    },
}

impl ObjectSelector {
    pub fn by_id(class_id: HubuumClassID, object_id: HubuumObjectID) -> Self {
        Self(ObjectSelectorKind::ById {
            class_id,
            object_id,
        })
    }

    pub fn by_name(class_name: impl Into<String>, object_name: impl Into<String>) -> Self {
        Self(ObjectSelectorKind::ByName {
            class_name: class_name.into(),
            object_name: object_name.into(),
        })
    }

    pub(crate) fn kind(&self) -> &ObjectSelectorKind {
        &self.0
    }
}

/// An object resolved from one explicit selector and safe to pass through authorization to a
/// transactional mutation.
#[derive(Clone, Debug)]
pub struct ResolvedObjectTarget {
    selector: ObjectSelector,
    class: HubuumClass,
    object: HubuumObject,
}

impl ResolvedObjectTarget {
    pub(crate) fn new(selector: ObjectSelector, class: HubuumClass, object: HubuumObject) -> Self {
        Self {
            selector,
            class,
            object,
        }
    }

    pub fn object(&self) -> &HubuumObject {
        &self.object
    }

    pub fn class(&self) -> &HubuumClass {
        &self.class
    }

    pub(crate) fn selector(&self) -> &ObjectSelector {
        &self.selector
    }
}

// For objects per class.
#[derive(QueryableByName, Debug, Serialize, Deserialize, ToSchema)]
pub struct ObjectsByClass {
    #[diesel(sql_type = Integer)]
    pub hubuum_class_id: i32,
    #[diesel(sql_type = BigInt)]
    pub count: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct HubuumObjectWithPath {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub path: Vec<i32>,
}

/// The two typed representations returned by object read endpoints.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(untagged)]
pub enum HubuumObjectReadResponse {
    Raw(HubuumObject),
    Computed(HubuumObjectComputedResponse),
}

impl From<HubuumObject> for HubuumObjectReadResponse {
    fn from(object: HubuumObject) -> Self {
        Self::Raw(object)
    }
}

impl From<HubuumObjectComputedResponse> for HubuumObjectReadResponse {
    fn from(object: HubuumObjectComputedResponse) -> Self {
        Self::Computed(object)
    }
}

pub async fn total_object_count<C>(backend: &C) -> Result<i64, ApiError>
where
    C: BackendContext + ?Sized,
{
    total_object_count_from_backend(backend.db_pool()).await
}

pub async fn objects_per_class_count<C>(backend: &C) -> Result<Vec<ObjectsByClass>, ApiError>
where
    C: BackendContext + ?Sized,
{
    objects_per_class_count_from_backend(backend.db_pool()).await
}

fn new_hubuum_object_example() -> NewHubuumObject {
    NewHubuumObject {
        name: "srv-01".to_string(),
        collection_id: 1,
        hubuum_class_id: 2,
        data: serde_json::json!({"hostname": "srv-01", "ip": "10.0.0.10"}),
        description: "Primary application server".to_string(),
    }
}

fn update_hubuum_object_example() -> UpdateHubuumObject {
    UpdateHubuumObject {
        name: Some("srv-01".to_string()),
        collection_id: None,
        hubuum_class_id: None,
        data: Some(serde_json::json!({"hostname": "srv-01", "status": "active"})),
        description: Some("Primary application server (updated)".to_string()),
    }
}

#[derive(serde::Serialize, diesel::Queryable, Clone, Debug, ToSchema)]
#[diesel(table_name = crate::schema::hubuumobject_history)]
pub struct HubuumObjectHistory {
    pub id: i32,
    pub name: String,
    pub collection_id: i32,
    pub hubuum_class_id: i32,
    pub data: serde_json::Value,
    pub description: String,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub op: String,
    pub valid_from: chrono::DateTime<chrono::Utc>,
    pub valid_to: Option<chrono::DateTime<chrono::Utc>>,
    pub actor_id: Option<i32>,
    pub history_id: i64,
}

crate::impl_history_pagination!(HubuumObjectHistory, "hubuumobject_history");

#[async_trait]
impl AuthzTarget for HubuumObject {
    async fn to_resource_ref(&self, _pool: &DbPool) -> Result<ResourceRef, ApiError> {
        Ok(ResourceRef {
            kind: ResourceKind::Object,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: Some(self.collection_id),
                class_id: Some(self.hubuum_class_id),
                name: Some(self.name.clone()),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for HubuumObjectID {
    async fn to_resource_ref(&self, pool: &DbPool) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::db::DbPool;
    use crate::models::class::tests::{create_class, verify_no_such_class};
    use crate::tests::TestScope;
    use crate::traits::{CanDelete, CanSave, SelfAccessors};

    fn request_path_class() -> HubuumClass {
        let now = chrono::Local::now().naive_local();
        HubuumClass {
            id: 17,
            name: "servers".to_string(),
            collection_id: 23,
            json_schema: None,
            validate_schema: false,
            description: String::new(),
            created_at: now,
            updated_at: now,
        }
    }

    #[rstest::rstest]
    #[case::omitted(None, None)]
    #[case::matching(Some(23), Some(17))]
    fn contextual_object_request_infers_path_ids(
        #[case] collection_id: Option<i32>,
        #[case] hubuum_class_id: Option<i32>,
    ) {
        let request: NewHubuumObjectRequest = serde_json::from_value(serde_json::json!({
            "name": "web-01",
            "collection_id": collection_id,
            "hubuum_class_id": hubuum_class_id,
            "data": {},
            "description": ""
        }))
        .unwrap();

        let object = request
            .into_domain_for_class(&request_path_class())
            .unwrap();

        assert_eq!(object.collection_id, 23);
        assert_eq!(object.hubuum_class_id, 17);
    }

    #[rstest::rstest]
    #[case::collection(Some(99), Some(17))]
    #[case::class(Some(23), Some(99))]
    fn contextual_object_request_rejects_conflicting_path_ids(
        #[case] collection_id: Option<i32>,
        #[case] hubuum_class_id: Option<i32>,
    ) {
        let request: NewHubuumObjectRequest = serde_json::from_value(serde_json::json!({
            "name": "web-01",
            "collection_id": collection_id,
            "hubuum_class_id": hubuum_class_id,
            "data": {},
            "description": ""
        }))
        .unwrap();

        let Err(error) = request.into_domain_for_class(&request_path_class()) else {
            panic!("conflicting path IDs must be rejected");
        };

        assert!(matches!(error, ApiError::BadRequest(_)));
    }

    pub async fn verify_no_such_object(pool: &DbPool, object_id: i32) {
        use crate::schema::hubuumobject::dsl::*;

        let result = with_connection(pool, async |conn| {
            hubuumobject
                .filter(id.eq(object_id))
                .first::<HubuumObject>(conn)
                .await
        })
        .await;

        match result {
            Ok(_) => panic!("Object {object_id} should not exist"),
            Err(ApiError::NotFound(_)) => (),
            Err(e) => panic!("Error: {e}"),
        }
    }

    pub async fn create_object(
        pool: &DbPool,
        hubuum_class_id: i32,
        collection_id: i32,
        object_name: &str,
        object_data: serde_json::Value,
    ) -> Result<HubuumObject, ApiError> {
        let object = NewHubuumObject {
            name: object_name.to_string(),
            collection_id,
            hubuum_class_id,
            data: object_data,
            description: "Test object".to_string(),
        };
        object.save_without_events(pool).await
    }

    pub async fn get_object(pool: &DbPool, object_id: i32) -> HubuumObject {
        let object = HubuumObjectID(object_id);
        object.instance(pool).await.unwrap()
    }

    #[actix_rt::test]
    async fn test_creating_object_manual_delete() {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let collection = scope.collection_fixture("object_manual_test").await;
        let class = create_class(&pool, &collection.collection, "test creating object").await;

        let obj_name = "test manual object creation";

        let object_data = serde_json::json!({"test": "data"});

        let object = create_object(
            &pool,
            class.id,
            collection.collection.id,
            obj_name,
            object_data.clone(),
        )
        .await
        .unwrap();
        assert_eq!(object.name, obj_name);

        let fetched_object = get_object(&pool, object.id).await;
        assert_eq!(fetched_object.name, obj_name);
        assert_eq!(fetched_object, object);
        assert_eq!(fetched_object.data, object_data);

        fetched_object.delete_without_events(&pool).await.unwrap();
        verify_no_such_object(&pool, object.id).await;

        class.delete_without_events(&pool).await.unwrap();
        verify_no_such_class(&pool, class.id).await;

        collection.cleanup().await.unwrap();
    }
}
