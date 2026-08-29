use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use utoipa::openapi::schema::{Schema, Type};
use utoipa::openapi::{KnownFormat, ObjectBuilder, RefOr, SchemaFormat};

use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumClassWithPath, HubuumObject, HubuumObjectID,
    HubuumObjectWithPath, ResourceRevision,
};
use crate::permissions::{AuthzTarget, ResourceAttrs, ResourceKind, ResourceRef};
use crate::traits::SelfAccessors;
use crate::utilities::aliases::normalize_template_alias;

pub use hubuum_domain::ClassRelationId as HubuumClassRelationID;

/// Maximum number of object relations allowed for one object on one side of a
/// class relation.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct ObjectRelationLimit(i32);

impl ObjectRelationLimit {
    /// Create a positive object-relation limit.
    pub fn new(value: i32) -> Result<Self, ApiError> {
        if value <= 0 {
            return Err(ApiError::BadRequest(format!(
                "Invalid object relation limit '{value}': must be a positive integer"
            )));
        }
        Ok(Self(value))
    }

    /// Return the underlying positive limit.
    pub fn value(self) -> i32 {
        self.0
    }
}

impl<'de> Deserialize<'de> for ObjectRelationLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

impl utoipa::PartialSchema for ObjectRelationLimit {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::Integer)
            .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
            .minimum(Some(1))
            .description(Some(
                "Maximum number of object relations allowed for one object on one side of a class relation.",
            ))
            .into()
    }
}

impl ToSchema for ObjectRelationLimit {}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct HubuumClassRelation {
    pub id: i32,
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
    pub forward_template_alias: Option<String>,
    pub reverse_template_alias: Option<String>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    /// Maximum relations allowed for each object in `from_hubuum_class_id`.
    /// `None` means unlimited.
    pub from_max_relations: Option<ObjectRelationLimit>,
    /// Maximum relations allowed for each object in `to_hubuum_class_id`.
    /// `None` means unlimited.
    pub to_max_relations: Option<ObjectRelationLimit>,
    pub revision: ResourceRevision,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = new_hubuum_class_relation_example)]
pub struct NewHubuumClassRelation {
    pub from_hubuum_class_id: i32,
    pub to_hubuum_class_id: i32,
    pub forward_template_alias: Option<String>,
    pub reverse_template_alias: Option<String>,
    /// Maximum relations allowed for each object in `from_hubuum_class_id`.
    /// Omit or set to `null` for unlimited.
    pub from_max_relations: Option<ObjectRelationLimit>,
    /// Maximum relations allowed for each object in `to_hubuum_class_id`.
    /// Omit or set to `null` for unlimited.
    pub to_max_relations: Option<ObjectRelationLimit>,
}

/// To create new relations between classes from within a class
/// we only need the id of the class we want to relate to.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = new_hubuum_class_relation_from_class_example)]
pub struct NewHubuumClassRelationFromClass {
    pub to_hubuum_class_id: i32,
    pub forward_template_alias: Option<String>,
    pub reverse_template_alias: Option<String>,
    /// Maximum relations allowed for each object in the class from the URL.
    /// Omit or set to `null` for unlimited.
    pub from_max_relations: Option<ObjectRelationLimit>,
    /// Maximum relations allowed for each object in `to_hubuum_class_id`.
    /// Omit or set to `null` for unlimited.
    pub to_max_relations: Option<ObjectRelationLimit>,
}

impl NewHubuumClassRelation {
    /// Validate and normalize a class relation before persistence.
    ///
    /// Class IDs are stored in ascending order. Directional aliases and limits
    /// move with their corresponding class when the supplied order is reversed.
    pub(crate) fn normalized(mut self) -> Result<Self, ApiError> {
        if self.from_hubuum_class_id == self.to_hubuum_class_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_class_id and to_hubuum_class_id cannot be the same".to_string(),
            ));
        }

        self.forward_template_alias = self
            .forward_template_alias
            .as_deref()
            .map(normalize_template_alias)
            .transpose()?;
        self.reverse_template_alias = self
            .reverse_template_alias
            .as_deref()
            .map(normalize_template_alias)
            .transpose()?;

        if self.from_hubuum_class_id > self.to_hubuum_class_id {
            std::mem::swap(&mut self.from_hubuum_class_id, &mut self.to_hubuum_class_id);
            std::mem::swap(
                &mut self.forward_template_alias,
                &mut self.reverse_template_alias,
            );
            std::mem::swap(&mut self.from_max_relations, &mut self.to_max_relations);
        }

        Ok(self)
    }
}

impl NewHubuumClassRelationFromClass {
    /// Complete a class-scoped relation request with the class from the route.
    pub(crate) fn into_relation(
        self,
        from_hubuum_class_id: HubuumClassID,
    ) -> NewHubuumClassRelation {
        NewHubuumClassRelation {
            from_hubuum_class_id: from_hubuum_class_id.id(),
            to_hubuum_class_id: self.to_hubuum_class_id,
            forward_template_alias: self.forward_template_alias,
            reverse_template_alias: self.reverse_template_alias,
            from_max_relations: self.from_max_relations,
            to_max_relations: self.to_max_relations,
        }
    }
}

fn class_relation_authorization_resource(
    relation_id: i32,
    from_class: &HubuumClass,
    to_class: &HubuumClass,
) -> ResourceRef {
    ResourceRef {
        kind: ResourceKind::ClassRelation,
        id: relation_id,
        attrs: ResourceAttrs {
            collection_id: (from_class.collection_id == to_class.collection_id)
                .then_some(from_class.collection_id),
            from_collection_id: Some(from_class.collection_id),
            to_collection_id: Some(to_class.collection_id),
            from_class_id: Some(from_class.id),
            to_class_id: Some(to_class.id),
            ..Default::default()
        },
    }
}

/// A normalized prospective class relation together with both endpoint classes.
///
/// Carrying the endpoints keeps authorization independent of the persistence
/// adapter and lets creation recheck the exact aggregate that was authorized.
#[derive(Clone, Debug)]
pub struct PreparedClassRelation {
    command: NewHubuumClassRelation,
    from_class: HubuumClass,
    to_class: HubuumClass,
}

impl PreparedClassRelation {
    pub(crate) fn new(
        command: NewHubuumClassRelation,
        from_class: HubuumClass,
        to_class: HubuumClass,
    ) -> Result<Self, ApiError> {
        let command = command.normalized()?;
        if command.from_hubuum_class_id != from_class.id
            || command.to_hubuum_class_id != to_class.id
        {
            return Err(ApiError::InternalServerError(
                "prepared class relation endpoints do not match its normalized command".to_string(),
            ));
        }
        Ok(Self {
            command,
            from_class,
            to_class,
        })
    }

    pub(crate) fn command(&self) -> &NewHubuumClassRelation {
        &self.command
    }

    pub fn from_class(&self) -> &HubuumClass {
        &self.from_class
    }

    pub fn to_class(&self) -> &HubuumClass {
        &self.to_class
    }

    pub(crate) fn authorization_resource(&self) -> ResourceRef {
        class_relation_authorization_resource(0, &self.from_class, &self.to_class)
    }
}

/// A persisted class relation resolved with both endpoint classes.
#[derive(Clone, Debug)]
pub struct ResolvedClassRelationTarget {
    relation: HubuumClassRelation,
    from_class: HubuumClass,
    to_class: HubuumClass,
}

impl ResolvedClassRelationTarget {
    pub(crate) fn new(
        relation: HubuumClassRelation,
        from_class: HubuumClass,
        to_class: HubuumClass,
    ) -> Result<Self, ApiError> {
        if relation.from_hubuum_class_id != from_class.id
            || relation.to_hubuum_class_id != to_class.id
        {
            return Err(ApiError::InternalServerError(format!(
                "class relation {} endpoints do not match the resolved classes",
                relation.id
            )));
        }
        Ok(Self {
            relation,
            from_class,
            to_class,
        })
    }

    pub fn relation(&self) -> &HubuumClassRelation {
        &self.relation
    }

    pub fn from_class(&self) -> &HubuumClass {
        &self.from_class
    }

    pub fn to_class(&self) -> &HubuumClass {
        &self.to_class
    }

    pub fn contains_class(&self, class_id: HubuumClassID) -> bool {
        self.from_class.id == class_id.id() || self.to_class.id == class_id.id()
    }

    pub(crate) fn authorization_resource(&self) -> ResourceRef {
        class_relation_authorization_resource(self.relation.id, &self.from_class, &self.to_class)
    }
}

pub use hubuum_domain::ObjectRelationId as HubuumObjectRelationID;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, ToSchema)]
pub struct HubuumObjectRelation {
    pub id: i32,
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
    pub class_relation_id: i32,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub revision: ResourceRevision,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(example = new_hubuum_object_relation_example)]
pub struct NewHubuumObjectRelation {
    pub from_hubuum_object_id: i32,
    pub to_hubuum_object_id: i32,
    pub class_relation_id: i32,
}

impl NewHubuumObjectRelation {
    /// Validate and normalize an object relation before persistence.
    pub(crate) fn normalized(mut self) -> Result<Self, ApiError> {
        if self.from_hubuum_object_id == self.to_hubuum_object_id {
            return Err(ApiError::BadRequest(
                "from_hubuum_object_id and to_hubuum_object_id cannot be the same".to_string(),
            ));
        }
        if self.from_hubuum_object_id > self.to_hubuum_object_id {
            std::mem::swap(
                &mut self.from_hubuum_object_id,
                &mut self.to_hubuum_object_id,
            );
        }
        Ok(self)
    }
}

/// One typed endpoint of an object relation route.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectRelationEndpoint {
    class_id: HubuumClassID,
    object_id: HubuumObjectID,
}

impl ObjectRelationEndpoint {
    pub fn new(class_id: HubuumClassID, object_id: HubuumObjectID) -> Self {
        Self {
            class_id,
            object_id,
        }
    }

    pub fn class_id(self) -> HubuumClassID {
        self.class_id
    }

    pub fn object_id(self) -> HubuumObjectID {
        self.object_id
    }
}

/// Explicit address for a persisted object relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectRelationSelector(ObjectRelationSelectorKind);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ObjectRelationSelectorKind {
    ById(HubuumObjectRelationID),
    Between {
        from: ObjectRelationEndpoint,
        to: ObjectRelationEndpoint,
    },
}

impl ObjectRelationSelector {
    pub fn by_id(id: HubuumObjectRelationID) -> Self {
        Self(ObjectRelationSelectorKind::ById(id))
    }

    pub fn between(from: ObjectRelationEndpoint, to: ObjectRelationEndpoint) -> Self {
        Self(ObjectRelationSelectorKind::Between { from, to })
    }

    pub(crate) fn kind(&self) -> &ObjectRelationSelectorKind {
        &self.0
    }
}

/// Explicit source for preparing a prospective object relation.
#[derive(Clone, Debug)]
pub struct ObjectRelationCreateSelector(ObjectRelationCreateSelectorKind);

#[derive(Clone, Debug)]
pub(crate) enum ObjectRelationCreateSelectorKind {
    Explicit(NewHubuumObjectRelation),
    Between {
        from: ObjectRelationEndpoint,
        to: ObjectRelationEndpoint,
    },
}

impl ObjectRelationCreateSelector {
    pub fn explicit(command: NewHubuumObjectRelation) -> Self {
        Self(ObjectRelationCreateSelectorKind::Explicit(command))
    }

    pub fn between(from: ObjectRelationEndpoint, to: ObjectRelationEndpoint) -> Self {
        Self(ObjectRelationCreateSelectorKind::Between { from, to })
    }

    pub(crate) fn kind(&self) -> &ObjectRelationCreateSelectorKind {
        &self.0
    }
}

fn object_relation_authorization_resource(
    relation_id: i32,
    class_relation_id: i32,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
) -> ResourceRef {
    ResourceRef {
        kind: ResourceKind::ObjectRelation,
        id: relation_id,
        attrs: ResourceAttrs {
            collection_id: (from_object.collection_id == to_object.collection_id)
                .then_some(from_object.collection_id),
            from_collection_id: Some(from_object.collection_id),
            to_collection_id: Some(to_object.collection_id),
            from_class_id: Some(from_object.hubuum_class_id),
            to_class_id: Some(to_object.hubuum_class_id),
            from_object_id: Some(from_object.id),
            to_object_id: Some(to_object.id),
            class_relation_id: Some(class_relation_id),
            ..Default::default()
        },
    }
}

fn validate_object_relation_membership(
    command: &NewHubuumObjectRelation,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
    class_relation: &ResolvedClassRelationTarget,
) -> Result<(), ApiError> {
    if command.from_hubuum_object_id != from_object.id
        || command.to_hubuum_object_id != to_object.id
        || command.class_relation_id != class_relation.relation().id
    {
        return Err(ApiError::InternalServerError(
            "object relation aggregate does not match its command".to_string(),
        ));
    }
    if from_object.hubuum_class_id == to_object.hubuum_class_id {
        return Err(ApiError::BadRequest(
            "from_hubuum_object_id and to_hubuum_object_id must not have the same class"
                .to_string(),
        ));
    }
    let matches_class_relation = (from_object.hubuum_class_id == class_relation.from_class().id
        && to_object.hubuum_class_id == class_relation.to_class().id)
        || (from_object.hubuum_class_id == class_relation.to_class().id
            && to_object.hubuum_class_id == class_relation.from_class().id);
    if !matches_class_relation {
        return Err(ApiError::BadRequest(
            "objects do not match the specified class relation".to_string(),
        ));
    }
    Ok(())
}

/// A prospective object relation with both objects and its class relation.
#[derive(Clone, Debug)]
pub struct PreparedObjectRelation {
    command: NewHubuumObjectRelation,
    from_object: HubuumObject,
    to_object: HubuumObject,
    class_relation: ResolvedClassRelationTarget,
}

impl PreparedObjectRelation {
    pub(crate) fn new(
        command: NewHubuumObjectRelation,
        from_object: HubuumObject,
        to_object: HubuumObject,
        class_relation: ResolvedClassRelationTarget,
    ) -> Result<Self, ApiError> {
        let command = command.normalized()?;
        validate_object_relation_membership(&command, &from_object, &to_object, &class_relation)?;
        Ok(Self {
            command,
            from_object,
            to_object,
            class_relation,
        })
    }

    pub(crate) fn command(&self) -> &NewHubuumObjectRelation {
        &self.command
    }

    pub fn from_object(&self) -> &HubuumObject {
        &self.from_object
    }

    pub fn to_object(&self) -> &HubuumObject {
        &self.to_object
    }

    pub fn class_relation(&self) -> &ResolvedClassRelationTarget {
        &self.class_relation
    }

    pub(crate) fn authorization_resource(&self) -> ResourceRef {
        object_relation_authorization_resource(
            0,
            self.class_relation.relation().id,
            &self.from_object,
            &self.to_object,
        )
    }
}

/// A persisted object relation resolved with both objects and its class relation.
#[derive(Clone, Debug)]
pub struct ResolvedObjectRelationTarget {
    relation: HubuumObjectRelation,
    from_object: HubuumObject,
    to_object: HubuumObject,
    class_relation: ResolvedClassRelationTarget,
}

impl ResolvedObjectRelationTarget {
    pub(crate) fn new(
        relation: HubuumObjectRelation,
        from_object: HubuumObject,
        to_object: HubuumObject,
        class_relation: ResolvedClassRelationTarget,
    ) -> Result<Self, ApiError> {
        validate_object_relation_membership(
            &NewHubuumObjectRelation {
                from_hubuum_object_id: relation.from_hubuum_object_id,
                to_hubuum_object_id: relation.to_hubuum_object_id,
                class_relation_id: relation.class_relation_id,
            },
            &from_object,
            &to_object,
            &class_relation,
        )?;
        Ok(Self {
            relation,
            from_object,
            to_object,
            class_relation,
        })
    }

    pub fn relation(&self) -> &HubuumObjectRelation {
        &self.relation
    }

    pub fn from_object(&self) -> &HubuumObject {
        &self.from_object
    }

    pub fn to_object(&self) -> &HubuumObject {
        &self.to_object
    }

    pub fn class_relation(&self) -> &ResolvedClassRelationTarget {
        &self.class_relation
    }

    pub(crate) fn authorization_resource(&self) -> ResourceRef {
        object_relation_authorization_resource(
            self.relation.id,
            self.relation.class_relation_id,
            &self.from_object,
            &self.to_object,
        )
    }
}

/// To create new relations between objects from within a
/// path where we already provide the class and object IDs
/// we only need the destination object ID.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct NewHubuumObjectRelationFromClassAndObject {
    pub to_hubuum_object_id: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, ToSchema)]
pub struct HubuumClassRelationTransitive {
    pub ancestor_class_id: i32,
    pub descendant_class_id: i32,
    pub depth: i32,
    pub path: Vec<Option<i32>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClassGraphRow {
    pub ancestor_class_id: i32,
    pub descendant_class_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub ancestor_name: String,
    pub descendant_name: String,
    pub ancestor_collection_id: i32,
    pub descendant_collection_id: i32,
    pub ancestor_json_schema: Option<serde_json::Value>,
    pub descendant_json_schema: Option<serde_json::Value>,
    pub ancestor_validate_schema: bool,
    pub descendant_validate_schema: bool,
    pub ancestor_description: String,
    pub descendant_description: String,
    pub ancestor_created_at: chrono::NaiveDateTime,
    pub descendant_created_at: chrono::NaiveDateTime,
    pub ancestor_updated_at: chrono::NaiveDateTime,
    pub descendant_updated_at: chrono::NaiveDateTime,
    pub ancestor_revision: ResourceRevision,
    pub descendant_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectGraphRow {
    pub ancestor_object_id: i32,
    pub descendant_object_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub ancestor_name: String,
    pub descendant_name: String,
    pub ancestor_collection_id: i32,
    pub descendant_collection_id: i32,
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
    pub ancestor_revision: ResourceRevision,
    pub descendant_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelatedObjectGraphRow {
    pub ancestor_object_id: i32,
    pub descendant_object_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub ancestor_name: String,
    pub descendant_name: String,
    pub ancestor_collection_id: i32,
    pub descendant_collection_id: i32,
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
    pub ancestor_revision: ResourceRevision,
    pub descendant_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelatedObjectIncludeRow {
    pub root_object_id: i32,
    pub ancestor_object_id: i32,
    pub descendant_object_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub ancestor_name: String,
    pub descendant_name: String,
    pub ancestor_collection_id: i32,
    pub descendant_collection_id: i32,
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
    pub ancestor_revision: ResourceRevision,
    pub descendant_revision: ResourceRevision,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelatedObjectForRootRow {
    pub root_object_id: i32,
    pub descendant_object_id: i32,
    pub depth: i32,
    pub path: Vec<i32>,
    pub descendant_name: String,
    pub descendant_collection_id: i32,
    pub descendant_class_id: i32,
    pub descendant_description: String,
    pub descendant_data: serde_json::Value,
    pub descendant_created_at: chrono::NaiveDateTime,
    pub descendant_updated_at: chrono::NaiveDateTime,
    pub descendant_revision: ResourceRevision,
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

fn new_hubuum_class_relation_example() -> NewHubuumClassRelation {
    NewHubuumClassRelation {
        from_hubuum_class_id: 1,
        to_hubuum_class_id: 2,
        forward_template_alias: Some("rooms".to_string()),
        reverse_template_alias: Some("hosts".to_string()),
        from_max_relations: Some(ObjectRelationLimit::new(1).expect("valid example limit")),
        to_max_relations: None,
    }
}

fn new_hubuum_class_relation_from_class_example() -> NewHubuumClassRelationFromClass {
    NewHubuumClassRelationFromClass {
        to_hubuum_class_id: 2,
        forward_template_alias: Some("rooms".to_string()),
        reverse_template_alias: Some("hosts".to_string()),
        from_max_relations: Some(ObjectRelationLimit::new(1).expect("valid example limit")),
        to_max_relations: None,
    }
}

fn new_hubuum_object_relation_example() -> NewHubuumObjectRelation {
    NewHubuumObjectRelation {
        from_hubuum_object_id: 10,
        to_hubuum_object_id: 20,
        class_relation_id: 3,
    }
}

#[async_trait]
impl AuthzTarget for HubuumClassRelation {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        let from_class = HubuumClassID::new(self.from_hubuum_class_id)?
            .instance(pool)
            .await?;
        let to_class = HubuumClassID::new(self.to_hubuum_class_id)?
            .instance(pool)
            .await?;
        let same_collection = from_class.collection_id == to_class.collection_id;

        Ok(ResourceRef {
            kind: ResourceKind::ClassRelation,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: same_collection.then_some(from_class.collection_id),
                from_collection_id: Some(from_class.collection_id),
                to_collection_id: Some(to_class.collection_id),
                from_class_id: Some(self.from_hubuum_class_id),
                to_class_id: Some(self.to_hubuum_class_id),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for NewHubuumClassRelation {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        let from_class = HubuumClassID::new(self.from_hubuum_class_id)?
            .instance(pool)
            .await?;
        let to_class = HubuumClassID::new(self.to_hubuum_class_id)?
            .instance(pool)
            .await?;
        Ok(ResourceRef {
            kind: ResourceKind::ClassRelation,
            id: 0,
            attrs: ResourceAttrs {
                collection_id: (from_class.collection_id == to_class.collection_id)
                    .then_some(from_class.collection_id),
                from_collection_id: Some(from_class.collection_id),
                to_collection_id: Some(to_class.collection_id),
                from_class_id: Some(from_class.id),
                to_class_id: Some(to_class.id),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for HubuumClassRelationID {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[async_trait]
impl AuthzTarget for HubuumObjectRelation {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        let from_object = HubuumObjectID::new(self.from_hubuum_object_id)?
            .instance(pool)
            .await?;
        let to_object = HubuumObjectID::new(self.to_hubuum_object_id)?
            .instance(pool)
            .await?;
        let same_collection = from_object.collection_id == to_object.collection_id;

        Ok(ResourceRef {
            kind: ResourceKind::ObjectRelation,
            id: self.id,
            attrs: ResourceAttrs {
                collection_id: same_collection.then_some(from_object.collection_id),
                from_collection_id: Some(from_object.collection_id),
                to_collection_id: Some(to_object.collection_id),
                from_class_id: Some(from_object.hubuum_class_id),
                to_class_id: Some(to_object.hubuum_class_id),
                from_object_id: Some(self.from_hubuum_object_id),
                to_object_id: Some(self.to_hubuum_object_id),
                class_relation_id: Some(self.class_relation_id),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for NewHubuumObjectRelation {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        let from_object = HubuumObjectID::new(self.from_hubuum_object_id)?
            .instance(pool)
            .await?;
        let to_object = HubuumObjectID::new(self.to_hubuum_object_id)?
            .instance(pool)
            .await?;
        Ok(ResourceRef {
            kind: ResourceKind::ObjectRelation,
            id: 0,
            attrs: ResourceAttrs {
                collection_id: (from_object.collection_id == to_object.collection_id)
                    .then_some(from_object.collection_id),
                from_collection_id: Some(from_object.collection_id),
                to_collection_id: Some(to_object.collection_id),
                from_class_id: Some(from_object.hubuum_class_id),
                to_class_id: Some(to_object.hubuum_class_id),
                from_object_id: Some(from_object.id),
                to_object_id: Some(to_object.id),
                class_relation_id: Some(self.class_relation_id),
                ..Default::default()
            },
        })
    }
}

#[async_trait]
impl AuthzTarget for HubuumObjectRelationID {
    async fn to_resource_ref(
        &self,
        pool: &impl crate::storage::StorageContext,
    ) -> Result<ResourceRef, ApiError> {
        self.instance(pool).await?.to_resource_ref(pool).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_relation_normalization_keeps_directional_settings_with_their_classes() {
        let normalized = NewHubuumClassRelation {
            from_hubuum_class_id: 20,
            to_hubuum_class_id: 10,
            forward_template_alias: Some("Jack Room".to_string()),
            reverse_template_alias: Some("Room Jacks".to_string()),
            from_max_relations: Some(ObjectRelationLimit::new(1).unwrap()),
            to_max_relations: Some(ObjectRelationLimit::new(2).unwrap()),
        }
        .normalized()
        .expect("class relation should normalize");

        assert_eq!(normalized.from_hubuum_class_id, 10);
        assert_eq!(normalized.to_hubuum_class_id, 20);
        assert_eq!(
            normalized.forward_template_alias.as_deref(),
            Some("room_jacks")
        );
        assert_eq!(
            normalized.reverse_template_alias.as_deref(),
            Some("jack_room")
        );
        assert_eq!(
            normalized.from_max_relations,
            Some(ObjectRelationLimit::new(2).unwrap())
        );
        assert_eq!(
            normalized.to_max_relations,
            Some(ObjectRelationLimit::new(1).unwrap())
        );
    }
}
