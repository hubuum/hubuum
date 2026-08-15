use async_trait::async_trait;
use hubuum_events_core::EventContext;

use crate::{
    StorageClassRecord, StorageClassRelation, StorageError, StorageObject, StorageObjectRelation,
};

/// Data required to create one class relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageClassRelationCreate {
    from_class_id: i32,
    to_class_id: i32,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl StorageClassRelationCreate {
    #[must_use]
    pub fn builder(from_class_id: i32, to_class_id: i32) -> StorageClassRelationCreateBuilder {
        StorageClassRelationCreateBuilder {
            command: Self {
                from_class_id,
                to_class_id,
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
            },
        }
    }

    #[must_use]
    pub const fn from_class_id(&self) -> i32 {
        self.from_class_id
    }

    #[must_use]
    pub const fn to_class_id(&self) -> i32 {
        self.to_class_id
    }

    #[must_use]
    pub fn forward_template_alias(&self) -> Option<&str> {
        self.forward_template_alias.as_deref()
    }

    #[must_use]
    pub fn reverse_template_alias(&self) -> Option<&str> {
        self.reverse_template_alias.as_deref()
    }

    #[must_use]
    pub const fn from_max_relations(&self) -> Option<i32> {
        self.from_max_relations
    }

    #[must_use]
    pub const fn to_max_relations(&self) -> Option<i32> {
        self.to_max_relations
    }
}

pub struct StorageClassRelationCreateBuilder {
    command: StorageClassRelationCreate,
}

impl StorageClassRelationCreateBuilder {
    #[must_use]
    pub fn template_aliases(mut self, forward: Option<String>, reverse: Option<String>) -> Self {
        self.command.forward_template_alias = forward;
        self.command.reverse_template_alias = reverse;
        self
    }

    #[must_use]
    pub const fn relation_limits(mut self, from: Option<i32>, to: Option<i32>) -> Self {
        self.command.from_max_relations = from;
        self.command.to_max_relations = to;
        self
    }

    #[must_use]
    pub fn build(self) -> StorageClassRelationCreate {
        self.command
    }
}

/// Prospective class relation with both resolved endpoint classes.
#[derive(Clone, PartialEq)]
pub struct StoragePreparedClassRelation {
    command: StorageClassRelationCreate,
    from_class: StorageClassRecord,
    to_class: StorageClassRecord,
}

impl StoragePreparedClassRelation {
    #[must_use]
    pub fn new(
        command: StorageClassRelationCreate,
        from_class: StorageClassRecord,
        to_class: StorageClassRecord,
    ) -> Self {
        Self {
            command,
            from_class,
            to_class,
        }
    }

    #[must_use]
    pub const fn command(&self) -> &StorageClassRelationCreate {
        &self.command
    }

    #[must_use]
    pub const fn from_class(&self) -> &StorageClassRecord {
        &self.from_class
    }

    #[must_use]
    pub const fn to_class(&self) -> &StorageClassRecord {
        &self.to_class
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageClassRelationCreate,
        StorageClassRecord,
        StorageClassRecord,
    ) {
        (self.command, self.from_class, self.to_class)
    }
}

/// Persisted class relation with both resolved endpoint classes.
#[derive(Clone, PartialEq)]
pub struct StorageResolvedClassRelation {
    relation: StorageClassRelation,
    from_class: StorageClassRecord,
    to_class: StorageClassRecord,
}

impl StorageResolvedClassRelation {
    #[must_use]
    pub fn new(
        relation: StorageClassRelation,
        from_class: StorageClassRecord,
        to_class: StorageClassRecord,
    ) -> Self {
        Self {
            relation,
            from_class,
            to_class,
        }
    }

    #[must_use]
    pub const fn relation(&self) -> &StorageClassRelation {
        &self.relation
    }

    #[must_use]
    pub const fn from_class(&self) -> &StorageClassRecord {
        &self.from_class
    }

    #[must_use]
    pub const fn to_class(&self) -> &StorageClassRecord {
        &self.to_class
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageClassRelation, StorageClassRecord, StorageClassRecord) {
        (self.relation, self.from_class, self.to_class)
    }
}

/// One class/object pair used by relation route selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectRelationEndpoint {
    class_id: i32,
    object_id: i32,
}

impl StorageObjectRelationEndpoint {
    #[must_use]
    pub const fn new(class_id: i32, object_id: i32) -> Self {
        Self {
            class_id,
            object_id,
        }
    }

    #[must_use]
    pub const fn class_id(self) -> i32 {
        self.class_id
    }

    #[must_use]
    pub const fn object_id(self) -> i32 {
        self.object_id
    }
}

/// Data required to create one object relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectRelationCreate {
    from_object_id: i32,
    to_object_id: i32,
    class_relation_id: i32,
}

impl StorageObjectRelationCreate {
    #[must_use]
    pub const fn new(from_object_id: i32, to_object_id: i32, class_relation_id: i32) -> Self {
        Self {
            from_object_id,
            to_object_id,
            class_relation_id,
        }
    }

    #[must_use]
    pub const fn from_object_id(self) -> i32 {
        self.from_object_id
    }

    #[must_use]
    pub const fn to_object_id(self) -> i32 {
        self.to_object_id
    }

    #[must_use]
    pub const fn class_relation_id(self) -> i32 {
        self.class_relation_id
    }
}

/// Explicit source used to prepare a prospective object relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageObjectRelationCreateSelector {
    Explicit(StorageObjectRelationCreate),
    Between {
        from: StorageObjectRelationEndpoint,
        to: StorageObjectRelationEndpoint,
    },
}

/// Explicit address for a persisted object relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageObjectRelationSelector {
    Id(i32),
    Between {
        from: StorageObjectRelationEndpoint,
        to: StorageObjectRelationEndpoint,
    },
}

/// Prospective object relation with its complete authorization aggregate.
#[derive(Clone, PartialEq)]
pub struct StoragePreparedObjectRelation {
    command: StorageObjectRelationCreate,
    from_object: StorageObject,
    to_object: StorageObject,
    class_relation: StorageResolvedClassRelation,
}

impl StoragePreparedObjectRelation {
    #[must_use]
    pub fn new(
        command: StorageObjectRelationCreate,
        from_object: StorageObject,
        to_object: StorageObject,
        class_relation: StorageResolvedClassRelation,
    ) -> Self {
        Self {
            command,
            from_object,
            to_object,
            class_relation,
        }
    }

    #[must_use]
    pub const fn command(&self) -> &StorageObjectRelationCreate {
        &self.command
    }

    #[must_use]
    pub const fn from_object(&self) -> &StorageObject {
        &self.from_object
    }

    #[must_use]
    pub const fn to_object(&self) -> &StorageObject {
        &self.to_object
    }

    #[must_use]
    pub const fn class_relation(&self) -> &StorageResolvedClassRelation {
        &self.class_relation
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageObjectRelationCreate,
        StorageObject,
        StorageObject,
        StorageResolvedClassRelation,
    ) {
        (
            self.command,
            self.from_object,
            self.to_object,
            self.class_relation,
        )
    }
}

/// Persisted object relation with its complete authorization aggregate.
#[derive(Clone, PartialEq)]
pub struct StorageResolvedObjectRelation {
    relation: StorageObjectRelation,
    from_object: StorageObject,
    to_object: StorageObject,
    class_relation: StorageResolvedClassRelation,
}

impl StorageResolvedObjectRelation {
    #[must_use]
    pub fn new(
        relation: StorageObjectRelation,
        from_object: StorageObject,
        to_object: StorageObject,
        class_relation: StorageResolvedClassRelation,
    ) -> Self {
        Self {
            relation,
            from_object,
            to_object,
            class_relation,
        }
    }

    #[must_use]
    pub const fn relation(&self) -> &StorageObjectRelation {
        &self.relation
    }

    #[must_use]
    pub const fn from_object(&self) -> &StorageObject {
        &self.from_object
    }

    #[must_use]
    pub const fn to_object(&self) -> &StorageObject {
        &self.to_object
    }

    #[must_use]
    pub const fn class_relation(&self) -> &StorageResolvedClassRelation {
        &self.class_relation
    }

    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StorageObjectRelation,
        StorageObject,
        StorageObject,
        StorageResolvedClassRelation,
    ) {
        (
            self.relation,
            self.from_object,
            self.to_object,
            self.class_relation,
        )
    }
}

/// Complete class-relation lifecycle required from a selectable backend.
///
/// An event context makes a mutation audited. `None` is an adapter-facing seam
/// for dedicated migrations, restores, imports, and fixtures; normal
/// application mutations use an audited service or [`crate::TransactionalStorage`].
#[async_trait]
pub trait ClassRelationStore: Send + Sync {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError>;

    async fn resolve_class_relation(
        &self,
        id: i32,
    ) -> Result<StorageResolvedClassRelation, StorageError>;

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedClassRelation, StorageError>;

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn create_class_relation_from_command(
        &self,
        command: StorageClassRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageClassRelation, StorageError>;

    async fn delete_class_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}

/// Complete object-relation lifecycle required from a selectable backend.
///
/// An event context makes a mutation audited. `None` is an adapter-facing seam
/// for dedicated migrations, restores, imports, and fixtures; normal
/// application mutations use an audited service or [`crate::TransactionalStorage`].
#[async_trait]
pub trait ObjectRelationStore: Send + Sync {
    async fn prepare_object_relation(
        &self,
        selector: StorageObjectRelationCreateSelector,
    ) -> Result<StoragePreparedObjectRelation, StorageError>;

    async fn resolve_object_relation(
        &self,
        selector: StorageObjectRelationSelector,
    ) -> Result<StorageResolvedObjectRelation, StorageError>;

    async fn create_object_relation(
        &self,
        prepared: &StoragePreparedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<StorageResolvedObjectRelation, StorageError>;

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;

    async fn create_object_relation_from_command(
        &self,
        command: StorageObjectRelationCreate,
        context: Option<&EventContext>,
    ) -> Result<StorageObjectRelation, StorageError>;

    async fn delete_object_relation_by_id(
        &self,
        id: i32,
        context: Option<&EventContext>,
    ) -> Result<(), StorageError>;
}
