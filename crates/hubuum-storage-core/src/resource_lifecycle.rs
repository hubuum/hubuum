use async_trait::async_trait;
use hubuum_domain::{
    BoundedJsonPatch, ClassId, CollectionId, GroupId, JsonPatchErrorKind, ObjectId,
    ResourceRevision,
};
use hubuum_events_core::EventContext;
use serde_json::Value;

use chrono::{DateTime, Utc};

use crate::{
    MutationOutcome, StorageCollection, StorageError, StorageObject, StorageRecordMetadata,
};

/// Flat class record used by point and lifecycle operations.
///
/// Catalog projections use `StorageClass`, which also embeds the collection.
/// Keeping this record flat prevents lifecycle writes from requiring an
/// otherwise unnecessary collection lookup.
#[derive(Clone, PartialEq)]
pub struct StorageClassRecord {
    id: ClassId,
    name: String,
    collection_id: CollectionId,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    revision: ResourceRevision,
}

impl StorageClassRecord {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        collection_id: CollectionId,
        description: impl Into<String>,
    ) -> StorageClassRecordBuilder {
        StorageClassRecordBuilder {
            metadata,
            name: name.into(),
            collection_id,
            description: description.into(),
            json_schema: None,
            validate_schema: false,
        }
    }

    #[must_use]
    pub const fn id(&self) -> ClassId {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn json_schema(&self) -> Option<&Value> {
        self.json_schema.as_ref()
    }

    #[must_use]
    pub const fn validates_schema(&self) -> bool {
        self.validate_schema
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> ResourceRevision {
        self.revision
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        ClassId,
        String,
        CollectionId,
        Option<Value>,
        bool,
        String,
        DateTime<Utc>,
        DateTime<Utc>,
        ResourceRevision,
    ) {
        (
            self.id,
            self.name,
            self.collection_id,
            self.json_schema,
            self.validate_schema,
            self.description,
            self.created_at,
            self.updated_at,
            self.revision,
        )
    }
}

pub struct StorageClassRecordBuilder {
    metadata: StorageRecordMetadata,
    name: String,
    collection_id: CollectionId,
    description: String,
    json_schema: Option<Value>,
    validate_schema: bool,
}

impl StorageClassRecordBuilder {
    #[must_use]
    pub fn json_schema(mut self, json_schema: Option<Value>) -> Self {
        self.json_schema = json_schema;
        self
    }

    #[must_use]
    pub const fn validate_schema(mut self, validate_schema: bool) -> Self {
        self.validate_schema = validate_schema;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageClassRecord {
        StorageClassRecord {
            id: ClassId::from(self.metadata.id()),
            name: self.name,
            collection_id: self.collection_id,
            json_schema: self.json_schema,
            validate_schema: self.validate_schema,
            description: self.description,
            created_at: self.metadata.created_at(),
            updated_at: self.metadata.updated_at(),
            revision: self.metadata.revision(),
        }
    }
}

/// Data required to create a collection and its initial owner grant atomically.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageCollectionCreate {
    name: String,
    description: String,
    owner_group_id: GroupId,
    parent_collection_id: Option<CollectionId>,
}

impl StorageCollectionCreate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        owner_group_id: GroupId,
        parent_collection_id: Option<CollectionId>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            owner_group_id,
            parent_collection_id,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }

    #[must_use]
    pub const fn owner_group_id(&self) -> GroupId {
        self.owner_group_id
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<CollectionId> {
        self.parent_collection_id
    }
}

/// Partial update to one collection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageCollectionUpdate {
    name: Option<String>,
    description: Option<String>,
}

impl StorageCollectionUpdate {
    #[must_use]
    pub fn new(name: Option<String>, description: Option<String>) -> Self {
        Self { name, description }
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

/// Explicit route-selected address for a class.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageClassSelector {
    Id(ClassId),
    Name(String),
}

/// Data required to create a class.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageClassCreate {
    name: String,
    collection_id: CollectionId,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
}

impl StorageClassCreate {
    #[must_use]
    pub fn builder(
        name: impl Into<String>,
        collection_id: CollectionId,
        description: impl Into<String>,
    ) -> StorageClassCreateBuilder {
        StorageClassCreateBuilder {
            command: Self {
                name: name.into(),
                collection_id,
                json_schema: None,
                validate_schema: false,
                description: description.into(),
            },
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn json_schema(&self) -> Option<&Value> {
        self.json_schema.as_ref()
    }

    #[must_use]
    pub const fn validates_schema(&self) -> bool {
        self.validate_schema
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
}

pub struct StorageClassCreateBuilder {
    command: StorageClassCreate,
}

impl StorageClassCreateBuilder {
    #[must_use]
    pub fn json_schema(mut self, json_schema: Option<Value>) -> Self {
        self.command.json_schema = json_schema;
        self
    }

    #[must_use]
    pub const fn validate_schema(mut self, validate_schema: bool) -> Self {
        self.command.validate_schema = validate_schema;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageClassCreate {
        self.command
    }
}

/// Partial update to one class.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageClassUpdate {
    name: Option<String>,
    collection_id: Option<CollectionId>,
    json_schema: Option<Value>,
    validate_schema: Option<bool>,
    description: Option<String>,
}

impl StorageClassUpdate {
    #[must_use]
    pub fn builder() -> StorageClassUpdateBuilder {
        StorageClassUpdateBuilder {
            update: Self::default(),
        }
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }

    #[must_use]
    pub const fn json_schema(&self) -> Option<&Value> {
        self.json_schema.as_ref()
    }

    #[must_use]
    pub const fn validate_schema(&self) -> Option<bool> {
        self.validate_schema
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

pub struct StorageClassUpdateBuilder {
    update: StorageClassUpdate,
}

impl StorageClassUpdateBuilder {
    #[must_use]
    pub fn name(mut self, name: Option<String>) -> Self {
        self.update.name = name;
        self
    }

    #[must_use]
    pub const fn collection_id(mut self, collection_id: Option<CollectionId>) -> Self {
        self.update.collection_id = collection_id;
        self
    }

    #[must_use]
    pub fn json_schema(mut self, json_schema: Option<Value>) -> Self {
        self.update.json_schema = json_schema;
        self
    }

    #[must_use]
    pub const fn validate_schema(mut self, validate_schema: Option<bool>) -> Self {
        self.update.validate_schema = validate_schema;
        self
    }

    #[must_use]
    pub fn description(mut self, description: Option<String>) -> Self {
        self.update.description = description;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageClassUpdate {
        self.update
    }
}

/// A class resolved from one explicit selector.
#[derive(Clone, PartialEq)]
pub struct StorageResolvedClass {
    selector: StorageClassSelector,
    class: StorageClassRecord,
}

impl StorageResolvedClass {
    #[must_use]
    pub fn new(selector: StorageClassSelector, class: StorageClassRecord) -> Self {
        Self { selector, class }
    }

    #[must_use]
    pub const fn selector(&self) -> &StorageClassSelector {
        &self.selector
    }

    #[must_use]
    pub const fn class(&self) -> &StorageClassRecord {
        &self.class
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageClassSelector, StorageClassRecord) {
        (self.selector, self.class)
    }
}

/// Explicit route-selected address for an object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageObjectSelector {
    Ids {
        class_id: ClassId,
        object_id: ObjectId,
    },
    Names {
        class_name: String,
        object_name: String,
    },
}

/// Data required to create an object.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageObjectCreate {
    name: String,
    collection_id: CollectionId,
    class_id: ClassId,
    data: Value,
    description: String,
}

impl StorageObjectCreate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        collection_id: CollectionId,
        class_id: ClassId,
        data: Value,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            collection_id,
            class_id,
            data,
            description: description.into(),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> CollectionId {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn data(&self) -> &Value {
        &self.data
    }

    #[must_use]
    pub fn description(&self) -> &str {
        &self.description
    }
}

/// Partial update to one object.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageObjectUpdate {
    name: Option<String>,
    collection_id: Option<CollectionId>,
    class_id: Option<ClassId>,
    data: Option<Value>,
    description: Option<String>,
}

impl StorageObjectUpdate {
    #[must_use]
    pub fn builder() -> StorageObjectUpdateBuilder {
        StorageObjectUpdateBuilder {
            update: Self::default(),
        }
    }

    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub const fn collection_id(&self) -> Option<CollectionId> {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> Option<ClassId> {
        self.class_id
    }

    #[must_use]
    pub const fn data(&self) -> Option<&Value> {
        self.data.as_ref()
    }

    #[must_use]
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }
}

pub struct StorageObjectUpdateBuilder {
    update: StorageObjectUpdate,
}

impl StorageObjectUpdateBuilder {
    #[must_use]
    pub fn name(mut self, name: Option<String>) -> Self {
        self.update.name = name;
        self
    }

    #[must_use]
    pub const fn collection_id(mut self, collection_id: Option<CollectionId>) -> Self {
        self.update.collection_id = collection_id;
        self
    }

    #[must_use]
    pub const fn class_id(mut self, class_id: Option<ClassId>) -> Self {
        self.update.class_id = class_id;
        self
    }

    #[must_use]
    pub fn data(mut self, data: Option<Value>) -> Self {
        self.update.data = data;
        self
    }

    #[must_use]
    pub fn description(mut self, description: Option<String>) -> Self {
        self.update.description = description;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageObjectUpdate {
        self.update
    }
}

/// A validated JSON Patch document for an object's data field.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageObjectDataPatch {
    patch: BoundedJsonPatch,
}

impl StorageObjectDataPatch {
    #[must_use]
    pub const fn new(patch: BoundedJsonPatch) -> Self {
        Self { patch }
    }

    /// Apply the complete bounded patch with backend-independent semantics.
    pub fn apply(&self, document: &Value) -> Result<Value, StorageError> {
        self.patch.apply(document).map_err(|error| {
            let (kind, message) = error.into_parts();
            match kind {
                JsonPatchErrorKind::BadRequest => StorageError::invalid_input(message),
                JsonPatchErrorKind::Conflict => StorageError::conflict(message),
                JsonPatchErrorKind::PayloadTooLarge => StorageError::input_too_large(message),
            }
        })
    }
}

/// An object and its class resolved from one explicit selector.
#[derive(Clone, PartialEq)]
pub struct StorageResolvedObject {
    selector: StorageObjectSelector,
    class: StorageClassRecord,
    object: StorageObject,
}

impl StorageResolvedObject {
    #[must_use]
    pub fn new(
        selector: StorageObjectSelector,
        class: StorageClassRecord,
        object: StorageObject,
    ) -> Self {
        Self {
            selector,
            class,
            object,
        }
    }

    #[must_use]
    pub const fn selector(&self) -> &StorageObjectSelector {
        &self.selector
    }

    #[must_use]
    pub const fn class(&self) -> &StorageClassRecord {
        &self.class
    }

    #[must_use]
    pub const fn object(&self) -> &StorageObject {
        &self.object
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageObjectSelector, StorageClassRecord, StorageObject) {
        (self.selector, self.class, self.object)
    }
}

/// Complete collection lifecycle required from a selectable backend.
///
/// Every mutation is audited. Restore and import operations use the separate
/// [`crate::ImportStorage`] and [`crate::RestoreStorage`] contracts and never weaken this interface.
#[async_trait]
pub trait CollectionStorage: Send + Sync {
    async fn get_collection(&self, id: CollectionId) -> Result<StorageCollection, StorageError>;

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError>;

    async fn update_collection(
        &self,
        id: CollectionId,
        changes: StorageCollectionUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError>;

    async fn delete_collection(
        &self,
        id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError>;

    async fn list_collection_children(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError>;

    async fn list_collection_ancestors(
        &self,
        id: CollectionId,
    ) -> Result<Vec<StorageCollection>, StorageError>;

    async fn move_collection(
        &self,
        id: CollectionId,
        new_parent_id: CollectionId,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageCollection>, StorageError>;
}

/// Complete class lifecycle required from a selectable backend.
///
/// Every mutation is audited. Restore and import operations use the separate
/// [`crate::ImportStorage`] and [`crate::RestoreStorage`] contracts and never weaken this interface.
#[async_trait]
pub trait ClassStorage: Send + Sync {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError>;

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError>;

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageClassRecord>, StorageError>;

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError>;

    /// Resolve class names in one backend operation.
    ///
    /// Implementations must return one row for every distinct requested ID or
    /// fail rather than silently returning a partial mapping.
    async fn resolve_class_names(
        &self,
        class_ids: Vec<ClassId>,
    ) -> Result<Vec<(ClassId, String)>, StorageError>;
}

/// Complete object lifecycle required from a selectable backend.
///
/// Every mutation is audited. Restore and import operations use the separate
/// [`crate::ImportStorage`] and [`crate::RestoreStorage`] contracts and never weaken this interface.
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// Load one object and its class by object ID.
    async fn get_object(&self, object_id: ObjectId) -> Result<StorageResolvedObject, StorageError>;

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError>;

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError>;

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError>;

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<MutationOutcome<StorageObject>, StorageError>;

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: &EventContext,
    ) -> Result<MutationOutcome<()>, StorageError>;

    /// Validate one stored object against its referenced class and collection.
    async fn validate_object(&self, object: StorageObject) -> Result<(), StorageError>;

    /// Validate an object-create command without writing it.
    async fn validate_object_create(
        &self,
        command: StorageObjectCreate,
    ) -> Result<(), StorageError>;

    /// Validate an object update against the current stored record.
    async fn validate_object_update(
        &self,
        object_id: ObjectId,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError>;
}
