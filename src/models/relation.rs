use crate::permissions::ClassResourceEndpoint;
use crate::permissions::ObjectResourceEndpoint;
use crate::services::storage_boundary::{
    class_record_from_storage, class_relation_create_from_storage, class_relation_from_storage,
    object_from_storage, object_relation_create_from_storage, object_relation_from_storage,
    resolved_class_relation_from_storage,
};
#[cfg(test)]
use crate::services::storage_boundary::{
    class_record_to_storage, class_relation_create_to_storage, class_relation_to_storage,
    object_relation_create_to_storage, object_relation_to_storage, object_to_storage,
    resolved_class_relation_to_storage,
};
use crate::storage::{
    StoragePreparedClassRelation, StoragePreparedObjectRelation, StorageResolvedClassRelation,
    StorageResolvedObjectRelation,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;
use utoipa::openapi::schema::{Schema, Type};
use utoipa::openapi::{KnownFormat, ObjectBuilder, RefOr, SchemaFormat};

use crate::errors::ApiError;
use crate::models::{
    HubuumClass, HubuumClassID, HubuumClassWithPath, HubuumObject, HubuumObjectID,
    HubuumObjectWithPath, ResourceRevision,
};
use crate::permissions::{AuthzTarget, ResourceRef};
use crate::traits::SelfAccessors;
#[cfg(test)]
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
    #[cfg(test)]
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
    relation_id: Option<i32>,
    from_class: &HubuumClass,
    to_class: &HubuumClass,
) -> ResourceRef {
    ResourceRef::class_relation(
        relation_id,
        ClassResourceEndpoint::new(from_class.collection_id, from_class.id),
        ClassResourceEndpoint::new(to_class.collection_id, to_class.id),
    )
}

/// A normalized prospective class relation together with both endpoint classes.
///
/// Carrying the endpoints keeps authorization independent of the persistence
/// adapter and lets creation recheck the exact aggregate that was authorized.
#[derive(Clone)]
pub struct PreparedClassRelation {
    storage: StoragePreparedClassRelation,
    command: NewHubuumClassRelation,
    from_class: HubuumClass,
    to_class: HubuumClass,
}

impl PreparedClassRelation {
    /// Preserve the validated aggregate; private views serve domain and HTTP callers.
    pub(crate) fn from_storage(storage: StoragePreparedClassRelation) -> Result<Self, ApiError> {
        Ok(Self {
            command: class_relation_create_from_storage(storage.command())?,
            from_class: class_record_from_storage(storage.from_class().clone())?,
            to_class: class_record_from_storage(storage.to_class().clone())?,
            storage,
        })
    }

    pub(crate) fn as_storage(&self) -> &StoragePreparedClassRelation {
        &self.storage
    }

    #[cfg(test)]
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
        let storage = StoragePreparedClassRelation::try_new(
            class_relation_create_to_storage(command.clone())?,
            class_record_to_storage(from_class.clone())?,
            class_record_to_storage(to_class.clone())?,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Ok(Self {
            storage,
            command,
            from_class,
            to_class,
        })
    }

    #[cfg(test)]
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
        class_relation_authorization_resource(None, &self.from_class, &self.to_class)
    }
}

/// A persisted class relation resolved with both endpoint classes.
#[derive(Clone)]
pub struct ResolvedClassRelationTarget {
    storage: StorageResolvedClassRelation,
    relation: HubuumClassRelation,
    from_class: HubuumClass,
    to_class: HubuumClass,
}

impl ResolvedClassRelationTarget {
    /// Preserve the validated aggregate; private views serve domain and HTTP callers.
    pub(crate) fn from_storage(storage: StorageResolvedClassRelation) -> Result<Self, ApiError> {
        Ok(Self {
            relation: class_relation_from_storage(storage.relation().clone())?,
            from_class: class_record_from_storage(storage.from_class().clone())?,
            to_class: class_record_from_storage(storage.to_class().clone())?,
            storage,
        })
    }

    pub(crate) fn as_storage(&self) -> &StorageResolvedClassRelation {
        &self.storage
    }

    #[cfg(test)]
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
        let storage = StorageResolvedClassRelation::try_new(
            class_relation_to_storage(relation.clone())?,
            class_record_to_storage(from_class.clone())?,
            class_record_to_storage(to_class.clone())?,
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Ok(Self {
            storage,
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
        class_relation_authorization_resource(
            Some(self.relation.id),
            &self.from_class,
            &self.to_class,
        )
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
    #[cfg(test)]
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
    relation_id: Option<i32>,
    class_relation_id: i32,
    from_object: &HubuumObject,
    to_object: &HubuumObject,
) -> ResourceRef {
    ResourceRef::object_relation(
        relation_id,
        ObjectResourceEndpoint::new(
            from_object.collection_id,
            from_object.hubuum_class_id,
            from_object.id,
        ),
        ObjectResourceEndpoint::new(
            to_object.collection_id,
            to_object.hubuum_class_id,
            to_object.id,
        ),
        class_relation_id,
    )
}

#[cfg(test)]
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
#[derive(Clone)]
pub struct PreparedObjectRelation {
    storage: StoragePreparedObjectRelation,
    command: NewHubuumObjectRelation,
    from_object: HubuumObject,
    to_object: HubuumObject,
    class_relation: ResolvedClassRelationTarget,
}

impl PreparedObjectRelation {
    /// Preserve the validated aggregate; private views serve domain and HTTP callers.
    pub(crate) fn from_storage(storage: StoragePreparedObjectRelation) -> Result<Self, ApiError> {
        Ok(Self {
            command: object_relation_create_from_storage(*storage.command()),
            from_object: object_from_storage(storage.from_object().clone())?,
            to_object: object_from_storage(storage.to_object().clone())?,
            class_relation: resolved_class_relation_from_storage(storage.class_relation().clone())?,
            storage,
        })
    }

    pub(crate) fn as_storage(&self) -> &StoragePreparedObjectRelation {
        &self.storage
    }

    #[cfg(test)]
    pub(crate) fn new(
        command: NewHubuumObjectRelation,
        from_object: HubuumObject,
        to_object: HubuumObject,
        class_relation: ResolvedClassRelationTarget,
    ) -> Result<Self, ApiError> {
        let command = command.normalized()?;
        validate_object_relation_membership(&command, &from_object, &to_object, &class_relation)?;
        let storage = StoragePreparedObjectRelation::try_new(
            object_relation_create_to_storage(command.clone())?,
            object_to_storage(from_object.clone())?,
            object_to_storage(to_object.clone())?,
            resolved_class_relation_to_storage(&class_relation).clone(),
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Ok(Self {
            storage,
            command,
            from_object,
            to_object,
            class_relation,
        })
    }

    #[cfg(test)]
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
            None,
            self.class_relation.relation().id,
            &self.from_object,
            &self.to_object,
        )
    }
}

/// A persisted object relation resolved with both objects and its class relation.
#[derive(Clone)]
pub struct ResolvedObjectRelationTarget {
    storage: StorageResolvedObjectRelation,
    relation: HubuumObjectRelation,
    from_object: HubuumObject,
    to_object: HubuumObject,
    class_relation: ResolvedClassRelationTarget,
}

impl ResolvedObjectRelationTarget {
    /// Preserve the validated aggregate; private views serve domain and HTTP callers.
    pub(crate) fn from_storage(storage: StorageResolvedObjectRelation) -> Result<Self, ApiError> {
        Ok(Self {
            relation: object_relation_from_storage(storage.relation().clone())?,
            from_object: object_from_storage(storage.from_object().clone())?,
            to_object: object_from_storage(storage.to_object().clone())?,
            class_relation: resolved_class_relation_from_storage(storage.class_relation().clone())?,
            storage,
        })
    }

    pub(crate) fn as_storage(&self) -> &StorageResolvedObjectRelation {
        &self.storage
    }

    #[cfg(test)]
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
        let storage = StorageResolvedObjectRelation::try_new(
            object_relation_to_storage(relation)?,
            object_to_storage(from_object.clone())?,
            object_to_storage(to_object.clone())?,
            resolved_class_relation_to_storage(&class_relation).clone(),
        )
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        Ok(Self {
            storage,
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
            Some(self.relation.id),
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

        Ok(ResourceRef::class_relation(
            Some(self.id),
            ClassResourceEndpoint::new(from_class.collection_id, self.from_hubuum_class_id),
            ClassResourceEndpoint::new(to_class.collection_id, self.to_hubuum_class_id),
        ))
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
        Ok(ResourceRef::class_relation(
            None,
            ClassResourceEndpoint::new(from_class.collection_id, from_class.id),
            ClassResourceEndpoint::new(to_class.collection_id, to_class.id),
        ))
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

        Ok(ResourceRef::object_relation(
            Some(self.id),
            ObjectResourceEndpoint::new(
                from_object.collection_id,
                from_object.hubuum_class_id,
                self.from_hubuum_object_id,
            ),
            ObjectResourceEndpoint::new(
                to_object.collection_id,
                to_object.hubuum_class_id,
                self.to_hubuum_object_id,
            ),
            self.class_relation_id,
        ))
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
        Ok(ResourceRef::object_relation(
            None,
            ObjectResourceEndpoint::new(
                from_object.collection_id,
                from_object.hubuum_class_id,
                from_object.id,
            ),
            ObjectResourceEndpoint::new(
                to_object.collection_id,
                to_object.hubuum_class_id,
                to_object.id,
            ),
            self.class_relation_id,
        ))
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

impl fmt::Debug for PreparedClassRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedClassRelation")
            .field("command", &self.command)
            .field("from_class", &self.from_class)
            .field("to_class", &self.to_class)
            .finish()
    }
}

impl fmt::Debug for ResolvedClassRelationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedClassRelationTarget")
            .field("relation", &self.relation)
            .field("from_class", &self.from_class)
            .field("to_class", &self.to_class)
            .finish()
    }
}

impl fmt::Debug for PreparedObjectRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedObjectRelation")
            .field("command", &self.command)
            .field("from_object", &self.from_object)
            .field("to_object", &self.to_object)
            .field("class_relation", &self.class_relation)
            .finish()
    }
}

impl fmt::Debug for ResolvedObjectRelationTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedObjectRelationTarget")
            .field("relation", &self.relation)
            .field("from_object", &self.from_object)
            .field("to_object", &self.to_object)
            .field("class_relation", &self.class_relation)
            .finish()
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
