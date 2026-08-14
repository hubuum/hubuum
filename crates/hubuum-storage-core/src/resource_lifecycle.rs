use async_trait::async_trait;
use hubuum_events_core::EventContext;
use serde_json::Value;

use chrono::NaiveDateTime;

use crate::{StorageCollection, StorageError, StorageObject, StorageRecordMetadata};

/// Flat class record used by point and lifecycle operations.
///
/// Catalog projections use `StorageClass`, which also embeds the collection.
/// Keeping this record flat prevents lifecycle writes from requiring an
/// otherwise unnecessary collection lookup.
#[derive(Clone, PartialEq)]
pub struct StorageClassRecord {
    id: i32,
    name: String,
    collection_id: i32,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    revision: i64,
}

impl StorageClassRecord {
    #[must_use]
    pub fn builder(
        metadata: StorageRecordMetadata,
        name: impl Into<String>,
        collection_id: i32,
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
    pub const fn id(&self) -> i32 {
        self.id
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn collection_id(&self) -> i32 {
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
    pub const fn created_at(&self) -> NaiveDateTime {
        self.created_at
    }

    #[must_use]
    pub const fn updated_at(&self) -> NaiveDateTime {
        self.updated_at
    }

    #[must_use]
    pub const fn revision(&self) -> i64 {
        self.revision
    }

    #[must_use]
    #[allow(clippy::type_complexity)]
    pub fn into_parts(
        self,
    ) -> (
        i32,
        String,
        i32,
        Option<Value>,
        bool,
        String,
        NaiveDateTime,
        NaiveDateTime,
        i64,
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
    collection_id: i32,
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
            id: self.metadata.id(),
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
    owner_group_id: i32,
    parent_collection_id: Option<i32>,
}

impl StorageCollectionCreate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        owner_group_id: i32,
        parent_collection_id: Option<i32>,
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
    pub const fn owner_group_id(&self) -> i32 {
        self.owner_group_id
    }

    #[must_use]
    pub const fn parent_collection_id(&self) -> Option<i32> {
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
    Id(i32),
    Name(String),
}

/// Data required to create a class.
#[derive(Clone, Debug, PartialEq)]
pub struct StorageClassCreate {
    name: String,
    collection_id: i32,
    json_schema: Option<Value>,
    validate_schema: bool,
    description: String,
}

impl StorageClassCreate {
    #[must_use]
    pub fn builder(
        name: impl Into<String>,
        collection_id: i32,
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
    pub const fn collection_id(&self) -> i32 {
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
    collection_id: Option<i32>,
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
    pub const fn collection_id(&self) -> Option<i32> {
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
    pub const fn collection_id(mut self, collection_id: Option<i32>) -> Self {
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
        class_id: i32,
        object_id: i32,
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
    collection_id: i32,
    class_id: i32,
    data: Value,
    description: String,
}

impl StorageObjectCreate {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        collection_id: i32,
        class_id: i32,
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
    pub const fn collection_id(&self) -> i32 {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> i32 {
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
    collection_id: Option<i32>,
    class_id: Option<i32>,
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
    pub const fn collection_id(&self) -> Option<i32> {
        self.collection_id
    }

    #[must_use]
    pub const fn class_id(&self) -> Option<i32> {
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
    pub const fn collection_id(mut self, collection_id: Option<i32>) -> Self {
        self.update.collection_id = collection_id;
        self
    }

    #[must_use]
    pub const fn class_id(mut self, class_id: Option<i32>) -> Self {
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
    document: Value,
}

impl StorageObjectDataPatch {
    #[must_use]
    pub const fn new(document: Value) -> Self {
        Self { document }
    }

    #[must_use]
    pub const fn document(&self) -> &Value {
        &self.document
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
#[async_trait]
pub trait CollectionStore: Send + Sync {
    async fn get_collection(&self, id: i32) -> Result<StorageCollection, StorageError>;

    async fn create_collection(
        &self,
        command: StorageCollectionCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError>;

    async fn update_collection(
        &self,
        id: i32,
        changes: StorageCollectionUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError>;

    async fn delete_collection(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn collection_children(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError>;

    async fn collection_ancestors(&self, id: i32) -> Result<Vec<StorageCollection>, StorageError>;

    async fn move_collection(
        &self,
        id: i32,
        new_parent_id: i32,
        context: Option<&EventContext>,
    ) -> Result<StorageCollection, StorageError>;
}

/// Complete class lifecycle required from a selectable backend.
#[async_trait]
pub trait ClassStore: Send + Sync {
    async fn resolve_class(
        &self,
        selector: StorageClassSelector,
    ) -> Result<StorageResolvedClass, StorageError>;

    async fn create_class(
        &self,
        command: StorageClassCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError>;

    async fn update_class(
        &self,
        target: &StorageResolvedClass,
        changes: StorageClassUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRecord, StorageError>;

    async fn delete_class(
        &self,
        target: &StorageResolvedClass,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    /// Resolve class names in one backend operation.
    ///
    /// Implementations must return one row for every distinct requested ID or
    /// fail rather than silently returning a partial mapping.
    async fn class_names(&self, class_ids: Vec<i32>) -> Result<Vec<(i32, String)>, StorageError>;
}

/// Complete object lifecycle required from a selectable backend.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Load one object and its class by object ID.
    async fn get_object(&self, object_id: i32) -> Result<StorageResolvedObject, StorageError>;

    async fn resolve_object(
        &self,
        selector: StorageObjectSelector,
    ) -> Result<StorageResolvedObject, StorageError>;

    async fn create_object(
        &self,
        class: &StorageResolvedClass,
        command: StorageObjectCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError>;

    async fn update_object(
        &self,
        target: &StorageResolvedObject,
        changes: StorageObjectUpdate,
        context: Option<&EventContext>,
    ) -> Result<StorageObject, StorageError>;

    async fn patch_object_data(
        &self,
        target: &StorageResolvedObject,
        patch: StorageObjectDataPatch,
        context: &EventContext,
    ) -> Result<StorageObject, StorageError>;

    async fn delete_object(
        &self,
        target: &StorageResolvedObject,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

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
        object_id: i32,
        changes: StorageObjectUpdate,
    ) -> Result<(), StorageError>;
}
