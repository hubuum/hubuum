use async_trait::async_trait;
use hubuum_domain::{
    ClassId, ClassRelationId, ObjectId, ObjectRelationId, RelationLabel, normalize_template_alias,
};
use hubuum_events_core::EventContext;

use crate::{
    StorageClass, StorageClassRelation, StorageError, StorageMutationOutcome, StorageObject,
    StorageObjectRelation, StorageValidationError,
};

/// Data required to create one class relation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageClassRelationCreate {
    from_class_id: ClassId,
    to_class_id: ClassId,
    forward_label: Option<RelationLabel>,
    reverse_label: Option<RelationLabel>,
    forward_template_alias: Option<String>,
    reverse_template_alias: Option<String>,
    from_max_relations: Option<i32>,
    to_max_relations: Option<i32>,
}

impl StorageClassRelationCreate {
    #[must_use]
    pub fn builder(
        from_class_id: ClassId,
        to_class_id: ClassId,
    ) -> StorageClassRelationCreateBuilder {
        StorageClassRelationCreateBuilder {
            command: Self {
                from_class_id,
                to_class_id,
                forward_label: None,
                reverse_label: None,
                forward_template_alias: None,
                reverse_template_alias: None,
                from_max_relations: None,
                to_max_relations: None,
            },
        }
    }

    pub fn normalized(mut self) -> Result<Self, StorageValidationError> {
        if self.from_class_id == self.to_class_id {
            return Err(StorageValidationError::invalid(
                "class relation endpoints must be distinct",
            ));
        }
        self.forward_template_alias = self
            .forward_template_alias
            .as_deref()
            .map(normalize_template_alias)
            .transpose()
            .map_err(|error| StorageValidationError::invalid(error.into_message()))?;
        self.reverse_template_alias = self
            .reverse_template_alias
            .as_deref()
            .map(normalize_template_alias)
            .transpose()
            .map_err(|error| StorageValidationError::invalid(error.into_message()))?;
        if self.from_class_id > self.to_class_id {
            std::mem::swap(&mut self.from_class_id, &mut self.to_class_id);
            std::mem::swap(&mut self.forward_label, &mut self.reverse_label);
            std::mem::swap(
                &mut self.forward_template_alias,
                &mut self.reverse_template_alias,
            );
            std::mem::swap(&mut self.from_max_relations, &mut self.to_max_relations);
        }
        Ok(self)
    }

    #[must_use]
    pub const fn from_class_id(&self) -> ClassId {
        self.from_class_id
    }

    #[must_use]
    pub const fn to_class_id(&self) -> ClassId {
        self.to_class_id
    }

    #[must_use]
    pub fn forward_label(&self) -> Option<&RelationLabel> {
        self.forward_label.as_ref()
    }

    #[must_use]
    pub fn reverse_label(&self) -> Option<&RelationLabel> {
        self.reverse_label.as_ref()
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
    pub fn labels(
        mut self,
        forward: Option<RelationLabel>,
        reverse: Option<RelationLabel>,
    ) -> Self {
        self.command.forward_label = forward;
        self.command.reverse_label = reverse;
        self
    }

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
    from_class: StorageClass,
    to_class: StorageClass,
}

impl StoragePreparedClassRelation {
    pub fn try_new(
        command: StorageClassRelationCreate,
        from_class: StorageClass,
        to_class: StorageClass,
    ) -> Result<Self, StorageValidationError> {
        if command.from_class_id() >= command.to_class_id()
            || command.from_class_id() != from_class.id()
            || command.to_class_id() != to_class.id()
            || command.from_max_relations().is_some_and(|value| value <= 0)
            || command.to_max_relations().is_some_and(|value| value <= 0)
        {
            return Err(StorageValidationError::invalid(
                "prepared class relation command and endpoints are inconsistent",
            ));
        }
        Ok(Self {
            command,
            from_class,
            to_class,
        })
    }

    #[must_use]
    pub const fn command(&self) -> &StorageClassRelationCreate {
        &self.command
    }

    #[must_use]
    pub const fn from_class(&self) -> &StorageClass {
        &self.from_class
    }

    #[must_use]
    pub const fn to_class(&self) -> &StorageClass {
        &self.to_class
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageClassRelationCreate, StorageClass, StorageClass) {
        (self.command, self.from_class, self.to_class)
    }
}

/// Persisted class relation with both resolved endpoint classes.
#[derive(Clone, PartialEq)]
pub struct StorageResolvedClassRelation {
    relation: StorageClassRelation,
    from_class: StorageClass,
    to_class: StorageClass,
}

impl StorageResolvedClassRelation {
    pub fn try_new(
        relation: StorageClassRelation,
        from_class: StorageClass,
        to_class: StorageClass,
    ) -> Result<Self, StorageValidationError> {
        if relation.from_class_id() != from_class.id() || relation.to_class_id() != to_class.id() {
            return Err(StorageValidationError::invalid(
                "resolved class relation endpoints must match its relation",
            ));
        }
        Ok(Self {
            relation,
            from_class,
            to_class,
        })
    }

    #[must_use]
    pub const fn relation(&self) -> &StorageClassRelation {
        &self.relation
    }

    #[must_use]
    pub const fn from_class(&self) -> &StorageClass {
        &self.from_class
    }

    #[must_use]
    pub const fn to_class(&self) -> &StorageClass {
        &self.to_class
    }

    #[must_use]
    pub fn into_parts(self) -> (StorageClassRelation, StorageClass, StorageClass) {
        (self.relation, self.from_class, self.to_class)
    }
}

/// One class/object pair used by relation route selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectRelationEndpoint {
    class_id: ClassId,
    object_id: ObjectId,
}

impl StorageObjectRelationEndpoint {
    #[must_use]
    pub const fn new(class_id: ClassId, object_id: ObjectId) -> Self {
        Self {
            class_id,
            object_id,
        }
    }

    #[must_use]
    pub const fn class_id(self) -> ClassId {
        self.class_id
    }

    #[must_use]
    pub const fn object_id(self) -> ObjectId {
        self.object_id
    }
}

/// Data required to create one object relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StorageObjectRelationCreate {
    from_object_id: ObjectId,
    to_object_id: ObjectId,
    class_relation_id: ClassRelationId,
}

impl StorageObjectRelationCreate {
    #[must_use]
    pub const fn new(
        from_object_id: ObjectId,
        to_object_id: ObjectId,
        class_relation_id: ClassRelationId,
    ) -> Self {
        Self {
            from_object_id,
            to_object_id,
            class_relation_id,
        }
    }

    #[must_use]
    pub const fn from_object_id(self) -> ObjectId {
        self.from_object_id
    }

    #[must_use]
    pub const fn to_object_id(self) -> ObjectId {
        self.to_object_id
    }

    #[must_use]
    pub const fn class_relation_id(self) -> ClassRelationId {
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
    Id(ObjectRelationId),
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
    pub fn try_new(
        command: StorageObjectRelationCreate,
        from_object: StorageObject,
        to_object: StorageObject,
        class_relation: StorageResolvedClassRelation,
    ) -> Result<Self, StorageValidationError> {
        let relation = class_relation.relation();
        let classes_match = (from_object.class_id() == relation.from_class_id()
            && to_object.class_id() == relation.to_class_id())
            || (from_object.class_id() == relation.to_class_id()
                && to_object.class_id() == relation.from_class_id());
        if command.from_object_id() >= command.to_object_id()
            || command.from_object_id() != from_object.id()
            || command.to_object_id() != to_object.id()
            || command.class_relation_id() != ClassRelationId::from(relation.metadata().id())
            || !classes_match
        {
            return Err(StorageValidationError::invalid(
                "prepared object relation aggregate has inconsistent endpoint ids",
            ));
        }
        Ok(Self {
            command,
            from_object,
            to_object,
            class_relation,
        })
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
    pub fn try_new(
        relation: StorageObjectRelation,
        from_object: StorageObject,
        to_object: StorageObject,
        class_relation: StorageResolvedClassRelation,
    ) -> Result<Self, StorageValidationError> {
        let resolved_class_relation = class_relation.relation();
        let classes_match = (from_object.class_id() == resolved_class_relation.from_class_id()
            && to_object.class_id() == resolved_class_relation.to_class_id())
            || (from_object.class_id() == resolved_class_relation.to_class_id()
                && to_object.class_id() == resolved_class_relation.from_class_id());
        if relation.from_object_id() >= relation.to_object_id()
            || relation.from_object_id() != from_object.id()
            || relation.to_object_id() != to_object.id()
            || relation.class_relation_id()
                != ClassRelationId::from(resolved_class_relation.metadata().id())
            || !classes_match
        {
            return Err(StorageValidationError::invalid(
                "resolved object relation aggregate has inconsistent endpoint ids",
            ));
        }
        Ok(Self {
            relation,
            from_object,
            to_object,
            class_relation,
        })
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
/// Every mutation is audited. Restore and import operations use the separate
/// [`crate::ImportStorage`] and [`crate::RestoreStorage`] contracts and never weaken this interface.
#[async_trait]
pub trait ClassRelationStorage: Send + Sync {
    async fn prepare_class_relation(
        &self,
        command: StorageClassRelationCreate,
    ) -> Result<StoragePreparedClassRelation, StorageError>;

    async fn resolve_class_relation(
        &self,
        id: ClassRelationId,
    ) -> Result<StorageResolvedClassRelation, StorageError>;

    async fn create_class_relation(
        &self,
        prepared: &StoragePreparedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError>;

    async fn update_class_relation_labels(
        &self,
        target: &StorageResolvedClassRelation,
        labels: StorageClassRelationLabelsUpdate,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedClassRelation>, StorageError>;

    async fn delete_class_relation(
        &self,
        target: &StorageResolvedClassRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

/// Complete object-relation lifecycle required from a selectable backend.
///
/// Every mutation is audited. Restore and import operations use the separate
/// [`crate::ImportStorage`] and [`crate::RestoreStorage`] contracts and never weaken this interface.
#[async_trait]
pub trait ObjectRelationStorage: Send + Sync {
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
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<StorageResolvedObjectRelation>, StorageError>;

    async fn delete_object_relation(
        &self,
        target: &StorageResolvedObjectRelation,
        context: &EventContext,
    ) -> Result<StorageMutationOutcome<()>, StorageError>;
}

/// Partial display-label update. `None` preserves a direction; `Some(None)` clears it.
#[derive(Clone, Debug, Default)]
pub struct StorageClassRelationLabelsUpdate {
    forward: Option<Option<RelationLabel>>,
    reverse: Option<Option<RelationLabel>>,
}

impl StorageClassRelationLabelsUpdate {
    #[must_use]
    pub fn new(
        forward: Option<Option<RelationLabel>>,
        reverse: Option<Option<RelationLabel>>,
    ) -> Self {
        Self { forward, reverse }
    }

    #[must_use]
    pub fn apply(&self, relation: StorageClassRelation) -> StorageClassRelation {
        let forward = self
            .forward
            .clone()
            .unwrap_or_else(|| relation.forward_label().cloned());
        let reverse = self
            .reverse
            .clone()
            .unwrap_or_else(|| relation.reverse_label().cloned());
        relation.with_labels(forward, reverse)
    }
}
