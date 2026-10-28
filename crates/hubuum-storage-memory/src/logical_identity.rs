//! Native identity assignment belongs to writes, never projections or capture.

use super::*;
use hubuum_storage_core::{
    LogicalIdentityStorage, StorageCapturedIdentity, StorageIdentityEvidence,
    StorageIdentityHistoryKind as History, StorageIdentityResourceKind as Family,
    StorageIdentitySubject, StorageIdentityTarget as Target, TaskExplicitTarget,
};
use serde_json::Value;

type References = BTreeMap<String, Target>;

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub(super) enum Association {
    Membership(PrincipalId, GroupId),
    Permission(AuthorizationGrantId),
    ServiceAccount(PrincipalId),
    SinkGrant(EventSinkId, CollectionId),
    TaskOutput(TaskId),
    ImportResult(ImportTaskResultId),
    Delivery(EventDeliveryId),
}

#[derive(Clone)]
struct Entry {
    evidence: StorageIdentityEvidence,
    revision: Option<ResourceRevision>,
    references: References,
}

#[derive(Clone)]
pub(super) struct MemoryLogicalIdentities {
    entries: BTreeMap<Target, Entry>,
    associations: BTreeMap<Association, Entry>,
    versions: BTreeMap<(ResourceIdentity, ResourceRevision), VersionIdentity>,
    unresolved_cohort: LegacyIdentityCohort,
}

impl Default for MemoryLogicalIdentities {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            associations: BTreeMap::new(),
            versions: BTreeMap::new(),
            unresolved_cohort: LegacyIdentityCohort::generate(),
        }
    }
}

fn resource(family: Family, id: impl Into<ResourceId>) -> Target {
    Target::Resource {
        kind: family,
        id: id.into(),
    }
}

fn references<const N: usize>(entries: [(&'static str, Option<Target>); N]) -> References {
    entries
        .into_iter()
        .filter_map(|(path, target)| target.map(|target| (path.to_owned(), target)))
        .collect()
}

pub(super) trait IdentityRecord {
    fn target(&self) -> Target;
    fn revision(&self) -> Option<ResourceRevision>;
    fn references(&self) -> References;
}

macro_rules! resource_record {
    ($type:ty, $family:ident, $value:ident, $id:expr, $revision:expr, $references:expr) => {
        impl IdentityRecord for $type {
            fn target(&self) -> Target {
                let $value = self;
                resource(Family::$family, $id)
            }
            fn revision(&self) -> Option<ResourceRevision> {
                let $value = self;
                Some($revision)
            }
            fn references(&self) -> References {
                let $value = self;
                $references
            }
        }
    };
}

resource_record!(
    StorageCollection,
    Collection,
    value,
    value.id(),
    value.revision(),
    references([(
        "/parent_collection_id",
        value
            .parent_collection_id()
            .map(|id| resource(Family::Collection, id))
    )])
);
resource_record!(
    StorageClass,
    Class,
    value,
    value.id(),
    value.revision(),
    references([(
        "/collection_id",
        Some(resource(Family::Collection, value.collection_id()))
    )])
);
resource_record!(
    StorageObject,
    Object,
    value,
    value.id(),
    value.revision(),
    references([
        (
            "/collection_id",
            Some(resource(Family::Collection, value.collection_id()))
        ),
        ("/class_id", Some(resource(Family::Class, value.class_id()))),
    ])
);
resource_record!(
    StorageClassRelation,
    ClassRelation,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    references([
        (
            "/from_class_id",
            Some(resource(Family::Class, value.from_class_id()))
        ),
        (
            "/to_class_id",
            Some(resource(Family::Class, value.to_class_id()))
        ),
    ])
);
resource_record!(
    StorageObjectRelation,
    ObjectRelation,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    references([
        (
            "/from_object_id",
            Some(resource(Family::Object, value.from_object_id()))
        ),
        (
            "/to_object_id",
            Some(resource(Family::Object, value.to_object_id()))
        ),
        (
            "/class_relation_id",
            Some(resource(Family::ClassRelation, value.class_relation_id()))
        ),
    ])
);
resource_record!(
    StorageIdentityGroup,
    Group,
    value,
    value.id(),
    value.revision(),
    references([(
        "/identity_scope_id",
        Some(resource(Family::IdentityScope, value.identity_scope_id()))
    )])
);
resource_record!(
    StorageExportTemplate,
    ExportTemplate,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    {
        let mut dependencies = references([
            (
                "/collection_id",
                Some(resource(Family::Collection, value.collection_id())),
            ),
            (
                "/class_id",
                value.class_id().map(|id| resource(Family::Class, id)),
            ),
        ]);
        if let Some(includes) = value
            .include()
            .and_then(|include| include.get("related_objects"))
            .and_then(Value::as_object)
        {
            for (alias, include) in includes {
                let alias = alias.replace('~', "~0").replace('/', "~1");
                for (field, family) in [
                    ("class_id", Family::Class),
                    ("class_relation_id", Family::ClassRelation),
                ] {
                    if let Some(id) = include
                        .get(field)
                        .and_then(Value::as_i64)
                        .and_then(|id| i32::try_from(id).ok())
                        .and_then(|id| ResourceId::new(id).ok())
                    {
                        dependencies.insert(
                            format!("/include/related_objects/{alias}/{field}"),
                            resource(family, id),
                        );
                    }
                }
            }
        }
        dependencies
    }
);
resource_record!(
    StorageRemoteTarget,
    RemoteTarget,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    references([
        (
            "/collection_id",
            Some(resource(Family::Collection, value.collection_id()))
        ),
        (
            "/class_id",
            value.class_id().map(|id| resource(Family::Class, id))
        ),
    ])
);
resource_record!(
    StorageEventSink,
    EventSink,
    value,
    value.id(),
    value.revision(),
    references([(
        "/collection_id",
        value
            .collection_id()
            .map(|id| resource(Family::Collection, id))
    )])
);
resource_record!(
    StorageEventSubscription,
    EventSubscription,
    value,
    value.id(),
    value.revision(),
    references([
        (
            "/collection_id",
            value
                .scope()
                .collection_id()
                .map(|id| resource(Family::Collection, id))
        ),
        (
            "/sink_id",
            Some(resource(Family::EventSink, value.sink_id()))
        ),
    ])
);
resource_record!(
    StorageQueryUsageDeclaration,
    QueryUsageDeclaration,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    references([
        ("/class_id", Some(resource(Family::Class, value.class_id()))),
        ("/created_by", value.created_by().map(Target::Principal)),
        ("/updated_by", value.updated_by().map(Target::Principal)),
    ])
);
resource_record!(
    StorageComputedFieldDefinition,
    ComputedDefinition,
    value,
    value.metadata().id(),
    value.metadata().revision(),
    references([
        ("/class_id", Some(resource(Family::Class, value.class_id()))),
        ("/created_by", value.created_by().map(Target::Principal)),
        ("/updated_by", value.updated_by().map(Target::Principal)),
        (
            "/owner_principal_id",
            match value.visibility() {
                StorageComputedFieldVisibility::Shared => None,
                StorageComputedFieldVisibility::Personal { owner_id } =>
                    Some(Target::Principal(owner_id)),
            }
        ),
    ])
);

impl IdentityRecord for StorageIdentityScope {
    fn target(&self) -> Target {
        resource(Family::IdentityScope, self.id())
    }
    fn revision(&self) -> Option<ResourceRevision> {
        Some(self.revision())
    }
    fn references(&self) -> References {
        BTreeMap::new()
    }
}

impl IdentityRecord for StoragePrincipal {
    fn target(&self) -> Target {
        Target::Principal(self.id())
    }
    fn revision(&self) -> Option<ResourceRevision> {
        Some(self.revision())
    }
    fn references(&self) -> References {
        references([(
            "/identity_scope_id",
            Some(resource(Family::IdentityScope, self.identity_scope_id())),
        )])
    }
}

impl IdentityRecord for MemoryTaskRecord {
    fn target(&self) -> Target {
        Target::Task(self.id)
    }
    fn revision(&self) -> Option<ResourceRevision> {
        None
    }
    fn references(&self) -> References {
        let mut refs = references([
            ("/submitted_by", self.submitted_by.map(Target::Principal)),
            (
                "/initiator_principal_id",
                self.initiator_principal_id.map(Target::Principal),
            ),
            ("/deleted_by", self.deleted_by.map(Target::Principal)),
            (
                "/cancel_requested_by",
                self.control
                    .cancellation()
                    .and_then(StorageTaskCancellation::requested_by)
                    .map(Target::Principal),
            ),
        ]);
        if let Some(metadata) = self.discovery_metadata() {
            let (class, schema, target, template, remote) = match metadata.details() {
                TaskMetadataDetails::SchemaValidation {
                    class_id,
                    schema_revision,
                    ..
                } => (*class_id, *schema_revision, None, None, None),
                TaskMetadataDetails::SchemaProfiling { class_id, .. }
                | TaskMetadataDetails::Reindex { class_id, .. } => {
                    (*class_id, None, None, None, None)
                }
                TaskMetadataDetails::Export {
                    target,
                    template_id,
                    ..
                } => (None, None, *target, *template_id, None),
                TaskMetadataDetails::RemoteCall {
                    target,
                    remote_target_id,
                } => (None, None, *target, None, *remote_target_id),
                TaskMetadataDetails::Import { .. } | TaskMetadataDetails::Backup { .. } => {
                    (None, None, None, None, None)
                }
            };
            refs.extend(references([
                (
                    "/discovery_metadata/data/class_id",
                    class.map(|id| resource(Family::Class, id)),
                ),
                (
                    "/discovery_metadata/data/template_id",
                    template.map(|id| resource(Family::ExportTemplate, id)),
                ),
                (
                    "/discovery_metadata/data/remote_target_id",
                    remote.map(|id| resource(Family::RemoteTarget, id)),
                ),
                (
                    "/discovery_metadata/data/schema_revision",
                    class
                        .zip(schema)
                        .map(|(class_id, revision)| Target::SchemaRevision { class_id, revision }),
                ),
            ]));
            if let Some(target) = target {
                let dependencies = match target {
                    TaskExplicitTarget::Collection { collection_id } => references([(
                        "/discovery_metadata/data/target/collection_id",
                        Some(resource(Family::Collection, collection_id)),
                    )]),
                    TaskExplicitTarget::Class { class_id } => references([(
                        "/discovery_metadata/data/target/class_id",
                        Some(resource(Family::Class, class_id)),
                    )]),
                    TaskExplicitTarget::Object {
                        class_id,
                        object_id,
                    } => references([
                        (
                            "/discovery_metadata/data/target/class_id",
                            class_id.map(|id| resource(Family::Class, id)),
                        ),
                        (
                            "/discovery_metadata/data/target/object_id",
                            Some(resource(Family::Object, object_id)),
                        ),
                    ]),
                    TaskExplicitTarget::ClassRelation { relation_id } => references([(
                        "/discovery_metadata/data/target/relation_id",
                        Some(resource(Family::ClassRelation, relation_id)),
                    )]),
                    TaskExplicitTarget::ObjectRelation { relation_id } => references([(
                        "/discovery_metadata/data/target/relation_id",
                        Some(resource(Family::ObjectRelation, relation_id)),
                    )]),
                };
                refs.extend(dependencies);
            }
        }
        refs
    }
}

impl IdentityRecord for MemoryHistoryValue {
    fn target(&self) -> Target {
        self.record().target()
    }
    fn revision(&self) -> Option<ResourceRevision> {
        self.record().revision()
    }
    fn references(&self) -> References {
        self.record().references()
    }
}

impl MemoryHistoryValue {
    fn identity_kind(&self) -> History {
        match self {
            Self::Collection(_) => History::Collection,
            Self::Class(_) => History::Class,
            Self::Object(_) => History::Object,
            Self::ExportTemplate(_) => History::ExportTemplate,
            Self::RemoteTarget(_) => History::RemoteTarget,
        }
    }
    fn record(&self) -> &dyn IdentityRecord {
        match self {
            Self::Collection(value) => value,
            Self::Class(value) => value,
            Self::Object(value) => value,
            Self::ExportTemplate(value) => value,
            Self::RemoteTarget(value) => value,
        }
    }
}

impl MemoryLogicalIdentities {
    pub(super) fn restore_requester(
        &self,
        principal: Option<PrincipalId>,
    ) -> StorageIdentityEvidence {
        StorageIdentityEvidence::try_new(
            None,
            principal
                .map(|id| {
                    (
                        "/requested_by".into(),
                        self.reference(Target::Principal(id)),
                    )
                })
                .into_iter()
                .collect(),
        )
        .expect("one restore requester")
    }

    fn retain_version(
        &mut self,
        evidence: &StorageIdentityEvidence,
        revision: Option<ResourceRevision>,
    ) {
        if let Some((record, revision)) = evidence
            .subject()
            .and_then(StorageIdentitySubject::record_identity)
            .zip(revision)
        {
            self.versions
                .insert((record.resource(), revision), record.version());
        }
    }

    pub(super) fn task_outcome(
        &self,
        task: TaskId,
        target: Option<RemoteTargetId>,
        subject: Option<(StorageRemoteTargetSubjectType, ResourceId)>,
    ) -> StorageIdentityEvidence {
        let mut references =
            BTreeMap::from([("/task_id".into(), self.reference(Target::Task(task)))]);
        if let Some(target) = target {
            references.insert(
                "/target_id".into(),
                self.reference(resource(Family::RemoteTarget, target)),
            );
        }
        if let Some((kind, id)) = subject {
            let family = match kind {
                StorageRemoteTargetSubjectType::Collection => Family::Collection,
                StorageRemoteTargetSubjectType::Class => Family::Class,
                StorageRemoteTargetSubjectType::Object => Family::Object,
                StorageRemoteTargetSubjectType::ClassRelation => Family::ClassRelation,
                StorageRemoteTargetSubjectType::ObjectRelation => Family::ObjectRelation,
            };
            references.insert("/subject_id".into(), self.reference(resource(family, id)));
        }
        StorageIdentityEvidence::try_new(None, references).expect("bounded outcome dependencies")
    }

    fn event_metadata(
        &self,
        envelope: &EventEnvelope,
    ) -> BTreeMap<String, StorageCapturedIdentity> {
        let metadata = envelope.metadata();
        let id = |field: &str| {
            metadata
                .get(field)
                .and_then(Value::as_i64)
                .and_then(|id| i32::try_from(id).ok())
                .and_then(|id| ResourceId::new(id).ok())
        };
        let mut captured = BTreeMap::new();
        for (field, family) in [
            ("parent_collection_id", Family::Collection),
            ("collection_id", Family::Collection),
            ("class_id", Family::Class),
            ("from_class_id", Family::Class),
            ("to_class_id", Family::Class),
            ("object_id", Family::Object),
            ("from_object_id", Family::Object),
            ("to_object_id", Family::Object),
            ("class_relation_id", Family::ClassRelation),
            ("identity_scope_id", Family::IdentityScope),
            ("source_scope_id", Family::IdentityScope),
            ("group_id", Family::Group),
            ("owner_group_id", Family::Group),
            ("sink_id", Family::EventSink),
            ("subscription_id", Family::EventSubscription),
            ("target_id", Family::RemoteTarget),
        ] {
            if let Some(id) = id(field) {
                captured.insert(
                    format!("/metadata/{field}"),
                    self.reference(resource(family, id)),
                );
            }
        }
        for field in [
            "principal_id",
            "actor_principal_id",
            "initiator_principal_id",
            "submitted_by",
            "created_by",
            "updated_by",
            "deleted_by",
            "cancel_requested_by",
            "requested_by",
            "owner_principal_id",
        ] {
            if let Some(id) = id(field) {
                captured.insert(
                    format!("/metadata/{field}"),
                    self.reference(Target::Principal(id.into())),
                );
            }
        }
        for field in ["task_id", "dependent_rebuild_task_id"] {
            if let Some(id) = id(field) {
                captured.insert(
                    format!("/metadata/{field}"),
                    self.reference(Target::Task(id.into())),
                );
            }
        }
        if let Some(collections) = metadata
            .get("related_collection_ids")
            .and_then(Value::as_array)
        {
            for (index, id) in collections.iter().enumerate() {
                if let Some(id) = id
                    .as_i64()
                    .and_then(|id| i32::try_from(id).ok())
                    .and_then(|id| ResourceId::new(id).ok())
                {
                    captured.insert(
                        format!("/metadata/related_collection_ids/{index}"),
                        self.reference(resource(Family::Collection, id)),
                    );
                }
            }
        }
        for field in ["schema", "before_schema"] {
            if let Some(schema) = metadata
                .get(field)
                .and_then(|value| serde_json::from_value::<SchemaReference>(value.clone()).ok())
            {
                captured.insert(
                    format!("/metadata/{field}/class_id"),
                    self.reference(resource(Family::Class, schema.class_id())),
                );
                captured.insert(
                    format!("/metadata/{field}/revision"),
                    self.reference(Target::SchemaRevision {
                        class_id: schema.class_id(),
                        revision: schema.revision(),
                    }),
                );
            }
        }
        for field in ["schema_revision", "before_schema_revision"] {
            if let Some((class_id, revision)) = id("class_id")
                .or_else(|| {
                    envelope
                        .entity_id()
                        .and_then(|id| ResourceId::new(id.get()).ok())
                })
                .zip(
                    metadata
                        .get(field)
                        .and_then(Value::as_i64)
                        .and_then(|value| SchemaRevision::new(value).ok()),
                )
            {
                captured.insert(
                    format!("/metadata/{field}"),
                    self.reference(Target::SchemaRevision {
                        class_id: class_id.into(),
                        revision,
                    }),
                );
            }
        }
        if let Some((object, revision)) = envelope
            .entity_id()
            .and_then(|id| ResourceId::new(id.get()).ok())
            .zip(
                metadata
                    .get("object_revision")
                    .and_then(Value::as_i64)
                    .and_then(|value| ResourceRevision::new(value).ok()),
            )
        {
            let target = resource(Family::Object, object);
            let version = match self.reference(target) {
                StorageCapturedIdentity::Resource { reference, .. } => reference
                    .known_identity()
                    .and_then(|identity| self.versions.get(&(identity, revision)).copied()),
                _ => None,
            };
            captured.insert(
                "/metadata/object_revision".into(),
                StorageCapturedIdentity::Version {
                    reference: match version {
                        Some(identity) => IdentityReference::Known { identity },
                        None => IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(self.unresolved_cohort, object)
                                .at_revision(revision),
                        },
                    },
                },
            );
        }
        captured
    }

    pub(super) fn event(
        &self,
        envelope: &EventEnvelope,
        before: Option<ResourceRevision>,
        after: Option<ResourceRevision>,
    ) -> StorageIdentityEvidence {
        let mut references = references([
            (
                "/actor_principal_id",
                envelope.actor_user_id().map(Target::Principal),
            ),
            (
                "/initiator_principal_id",
                envelope
                    .provenance()
                    .initiator
                    .as_ref()
                    .map(|principal| Target::Principal(principal.principal_id)),
            ),
            ("/task_id", envelope.provenance().task_id.map(Target::Task)),
            (
                "/collection_id",
                envelope
                    .collection_id()
                    .map(|id| resource(Family::Collection, id)),
            ),
        ])
        .into_iter()
        .map(|(path, target)| (path.to_owned(), self.reference(target)))
        .collect::<BTreeMap<_, _>>();
        if let Some(id) = envelope.entity_id() {
            let id = ResourceId::new(id.get()).expect("positive event entity");
            let target = match envelope.entity_type() {
                EntityType::Collection => Some(resource(Family::Collection, id)),
                EntityType::Class | EntityType::ClassSchema => Some(resource(Family::Class, id)),
                EntityType::Object | EntityType::ObjectValidation => {
                    Some(resource(Family::Object, id))
                }
                EntityType::ClassRelation => Some(resource(Family::ClassRelation, id)),
                EntityType::ObjectRelation => Some(resource(Family::ObjectRelation, id)),
                EntityType::User | EntityType::ServiceAccount => Some(Target::Principal(id.into())),
                EntityType::Group => Some(resource(Family::Group, id)),
                EntityType::ExportTemplate => Some(resource(Family::ExportTemplate, id)),
                EntityType::RemoteTarget => Some(resource(Family::RemoteTarget, id)),
                EntityType::Task => Some(Target::Task(id.into())),
                EntityType::EventSink => Some(resource(Family::EventSink, id)),
                EntityType::EventSubscription => Some(resource(Family::EventSubscription, id)),
                EntityType::ComputedFieldDefinition => {
                    Some(resource(Family::ComputedDefinition, id))
                }
                EntityType::QueryUsageDeclaration => {
                    Some(resource(Family::QueryUsageDeclaration, id))
                }
                _ => None,
            };
            if let Some(target) = target {
                let captured = self.reference(target);
                if let StorageCapturedIdentity::Resource { reference, .. } = captured {
                    for (path, revision) in
                        [("/before_revision", before), ("/after_revision", after)]
                    {
                        if let Some(revision) = revision {
                            let version = reference.known_identity().and_then(|identity| {
                                self.versions.get(&(identity, revision)).copied()
                            });
                            let reference = match version {
                                Some(identity) => IdentityReference::Known { identity },
                                None => IdentityReference::Unresolved {
                                    legacy: UnresolvedIdentity::new(self.unresolved_cohort, id)
                                        .at_revision(revision),
                                },
                            };
                            references.insert(
                                path.into(),
                                StorageCapturedIdentity::Version { reference },
                            );
                        }
                    }
                }
                references.insert("/entity_id".into(), captured);
            }
        }
        references.extend(self.event_metadata(envelope));
        StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Event {
                identity: envelope.event_id().into(),
            }),
            references,
        )
        .expect("bounded event identity dependencies")
    }

    pub(super) fn schema_evidence(
        &self,
        object: ObjectId,
        revision: ResourceRevision,
        schema: &StorageSchemaRevision,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        let entry = self
            .entries
            .get(&resource(Family::Object, object))
            .filter(|entry| entry.revision == Some(revision))
            .ok_or_else(|| {
                StorageError::conflict("Schema evidence requires the inspected object version")
            })?;
        let Some(StorageIdentitySubject::Resource { identity, version }) = entry.evidence.subject()
        else {
            unreachable!("stored object identity")
        };
        let Some(StorageIdentitySubject::Resource {
            identity: class,
            version: schema_version,
        }) = schema
            .logical_identity()
            .and_then(StorageIdentityEvidence::subject)
        else {
            return Err(StorageError::backend_failure(
                "Schema revision has no identity evidence",
            ));
        };
        StorageIdentityEvidence::try_new(
            None,
            BTreeMap::from([
                (
                    "/object_id".into(),
                    StorageCapturedIdentity::Resource {
                        family: Family::Object,
                        reference: identity,
                    },
                ),
                (
                    "/object_revision".into(),
                    StorageCapturedIdentity::Version {
                        reference: IdentityReference::Known { identity: version },
                    },
                ),
                (
                    "/class_id".into(),
                    StorageCapturedIdentity::Resource {
                        family: Family::Class,
                        reference: class,
                    },
                ),
                (
                    "/schema_revision".into(),
                    StorageCapturedIdentity::Version {
                        reference: IdentityReference::Known {
                            identity: schema_version,
                        },
                    },
                ),
            ]),
        )
        .map_err(invalid_contract_value)
    }

    fn capture_association(&mut self, key: Association, dependencies: References) {
        let previous = self.associations.get(&key);
        let mut references = previous
            .map(|entry| entry.evidence.references().clone())
            .unwrap_or_default();
        for (path, &target) in &dependencies {
            if previous.is_none_or(|entry| entry.references.get(path) != Some(&target)) {
                references.insert(path.to_owned(), self.reference(target));
            }
        }
        let subject = match key {
            Association::ServiceAccount(id) => self
                .entries
                .get(&Target::Principal(id))
                .and_then(|entry| entry.evidence.subject()),
            _ => None,
        };
        let evidence = StorageIdentityEvidence::try_new(subject, references)
            .expect("bounded association references");
        self.associations.insert(
            key,
            Entry {
                evidence,
                references: dependencies,
                revision: None,
            },
        );
    }

    pub(super) fn association(
        &self,
        key: Association,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        self.associations
            .get(&key)
            .map(|entry| entry.evidence.clone())
            .ok_or_else(|| {
                StorageError::backend_failure("Stored association is missing its identity evidence")
            })
    }

    pub(super) fn restore_association(
        &mut self,
        key: Association,
        evidence: StorageIdentityEvidence,
    ) {
        let entry = self.associations.entry(key).or_insert_with(|| Entry {
            evidence: evidence.clone(),
            references: BTreeMap::new(),
            revision: None,
        });
        entry.evidence = evidence;
    }

    pub(super) fn membership_source(
        &self,
        principal: PrincipalId,
        group: GroupId,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        let mut references = self
            .association(Association::Membership(principal, group))?
            .references()
            .clone();
        let group = self.record(resource(Family::Group, group))?;
        let scope = group
            .references()
            .get("/identity_scope_id")
            .copied()
            .ok_or_else(|| {
                StorageError::backend_failure("Group is missing its captured identity scope")
            })?;
        references.insert("/source_scope_id".into(), scope);
        StorageIdentityEvidence::try_new(None, references).map_err(invalid_contract_value)
    }

    pub(super) fn restore_record(
        &mut self,
        value: &impl IdentityRecord,
        evidence: StorageIdentityEvidence,
    ) {
        self.retain_version(&evidence, value.revision());
        self.entries.insert(
            value.target(),
            Entry {
                evidence,
                revision: value.revision(),
                references: value.references(),
            },
        );
    }

    pub(super) fn record(&self, target: Target) -> Result<StorageIdentityEvidence, StorageError> {
        self.entries
            .get(&target)
            .map(|entry| entry.evidence.clone())
            .ok_or_else(|| StorageError::backend_failure("Stored record is missing its identity"))
    }

    pub(super) fn fork(&self, targets: impl IntoIterator<Item = Target>) -> Self {
        let mut entries = BTreeMap::new();
        let mut pending = targets.into_iter().collect::<Vec<_>>();
        while let Some(target) = pending.pop() {
            if entries.contains_key(&target) {
                continue;
            }
            if let Target::Resource {
                kind: Family::Class,
                id,
            } = target
            {
                pending.extend(self.entries.keys().filter(|key| matches!(key, Target::SchemaRevision { class_id, .. } if class_id.id() == id.id())).copied());
            }
            if let Some(entry) = self.entries.get(&target) {
                pending.extend(entry.references.values().copied());
                entries.insert(target, entry.clone());
            }
        }
        Self {
            versions: self
                .versions
                .iter()
                .filter(|((identity, _), _)| {
                    entries.values().any(|entry| {
                        entry
                            .evidence
                            .subject()
                            .and_then(StorageIdentitySubject::record_identity)
                            .is_some_and(|record| record.resource() == *identity)
                    })
                })
                .map(|(key, value)| (*key, *value))
                .collect(),
            entries,
            associations: BTreeMap::new(),
            unresolved_cohort: self.unresolved_cohort,
        }
    }

    pub(super) fn merge(&mut self, other: Self) {
        self.versions.extend(other.versions);
        self.entries.extend(other.entries);
        self.associations.extend(other.associations);
    }

    pub(super) fn class_version(
        &self,
        class_id: ClassId,
        mut references: BTreeMap<String, StorageCapturedIdentity>,
    ) -> StorageIdentityEvidence {
        let owner = self.reference(resource(Family::Class, class_id));
        let StorageCapturedIdentity::Resource {
            reference: identity,
            ..
        } = owner
        else {
            unreachable!("class reference")
        };
        references.insert("/class_id".into(), owner);
        StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Resource {
                identity,
                version: VersionIdentity::generate(),
            }),
            references,
        )
        .expect("bounded class version dependencies")
    }

    pub(super) fn schema_revision(
        &mut self,
        revision: StorageSchemaRevision,
    ) -> StorageSchemaRevision {
        let revision = if revision.logical_identity().is_some() {
            revision
        } else {
            let references = revision
                .created_by()
                .map(|id| ("/created_by".into(), self.reference(Target::Principal(id))))
                .into_iter()
                .collect();
            let evidence = self.class_version(revision.reference().class_id(), references);
            revision.with_logical_identity(evidence)
        };
        let target = Target::SchemaRevision {
            class_id: revision.reference().class_id(),
            revision: revision.reference().revision(),
        };
        self.entries.insert(
            target,
            Entry {
                evidence: revision
                    .logical_identity()
                    .expect("assigned schema identity")
                    .clone(),
                revision: None,
                references: references([
                    (
                        "/class_id",
                        Some(resource(Family::Class, revision.reference().class_id())),
                    ),
                    ("/created_by", revision.created_by().map(Target::Principal)),
                ]),
            },
        );
        revision
    }

    pub(super) fn computed_history(
        &self,
        class_id: ClassId,
        definitions: &[StorageHistoricalComputedField],
    ) -> StorageIdentityEvidence {
        let mut references = BTreeMap::new();
        for (index, definition) in definitions.iter().enumerate() {
            let target = resource(Family::ComputedDefinition, definition.id());
            references.insert(format!("/definitions/{index}/id"), self.reference(target));
            let entry = self
                .entries
                .get(&target)
                .expect("stored computed definition has an identity");
            let Some(StorageIdentitySubject::Resource { version, .. }) = entry.evidence.subject()
            else {
                unreachable!("computed definition resource identity")
            };
            references.insert(
                format!("/definitions/{index}/revision"),
                StorageCapturedIdentity::Version {
                    reference: IdentityReference::Known { identity: version },
                },
            );
            references.insert(
                format!("/definitions/{index}/class_id"),
                entry.evidence.references()["/class_id"],
            );
        }
        self.class_version(class_id, references)
    }

    pub(super) fn schema_history(
        &self,
        revision: &StorageSchemaRevision,
        context: &EventContext,
    ) -> StorageIdentityEvidence {
        let source = revision
            .logical_identity()
            .expect("stored schema revision has an identity");
        let Some(StorageIdentitySubject::Resource { identity, version }) = source.subject() else {
            unreachable!("schema revision resource identity")
        };
        let mut references = source
            .references()
            .iter()
            .map(|(path, identity)| (format!("/snapshot{path}"), *identity))
            .collect::<BTreeMap<_, _>>();
        references.insert(
            "/snapshot/revision".into(),
            StorageCapturedIdentity::Version {
                reference: IdentityReference::Known { identity: version },
            },
        );
        references.insert(
            "/class_id".into(),
            StorageCapturedIdentity::Resource {
                family: Family::Class,
                reference: identity,
            },
        );
        for (path, target) in [
            (
                "/actor_principal_id",
                context.actor_user_id().map(Target::Principal),
            ),
            ("/task_id", context.task_id().map(Target::Task)),
        ] {
            if let Some(target) = target {
                references.insert(path.to_owned(), self.reference(target));
            }
        }
        StorageIdentityEvidence::try_new(
            Some(StorageIdentitySubject::Resource {
                identity,
                version: VersionIdentity::generate(),
            }),
            references,
        )
        .expect("bounded schema history dependencies")
    }

    pub(super) fn history(
        &mut self,
        value: &impl IdentityRecord,
        operation: StorageHistoryOperation,
        context: Option<&EventContext>,
    ) -> StorageIdentityEvidence {
        self.capture(value);
        let current = &self.entries[&value.target()].evidence;
        let subject = match current.subject().expect("record has an identity") {
            StorageIdentitySubject::Resource { identity, version } => {
                StorageIdentitySubject::Resource {
                    identity,
                    version: if operation == StorageHistoryOperation::Delete {
                        VersionIdentity::generate()
                    } else {
                        version
                    },
                }
            }
            _ => unreachable!("temporal history represents a resource"),
        };
        let mut references = current.references().clone();
        if let Some(context) = context {
            for (path, target) in [
                (
                    "/actor_principal_id",
                    context.actor_user_id().map(Target::Principal),
                ),
                (
                    "/initiator_principal_id",
                    context.initiator_user_id().map(Target::Principal),
                ),
                ("/task_id", context.task_id().map(Target::Task)),
            ] {
                if let Some(target) = target {
                    references.insert(path.to_owned(), self.reference(target));
                }
            }
        }
        StorageIdentityEvidence::try_new(Some(subject), references)
            .expect("bounded temporal references")
    }
    fn reference(&self, target: Target) -> StorageCapturedIdentity {
        let subject = self
            .entries
            .get(&target)
            .and_then(|entry| entry.evidence.subject());
        match target {
            Target::Resource { kind, id } => StorageCapturedIdentity::Resource {
                family: kind,
                reference: match subject {
                    Some(StorageIdentitySubject::Resource { identity, .. }) => identity,
                    _ => IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(self.unresolved_cohort, id),
                    },
                },
            },
            Target::Principal(id) => StorageCapturedIdentity::Principal {
                reference: match subject {
                    Some(StorageIdentitySubject::Principal { identity }) => identity,
                    _ => IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            self.unresolved_cohort,
                            ResourceId::from(id),
                        ),
                    },
                },
            },
            Target::Task(id) => StorageCapturedIdentity::Task {
                reference: match subject {
                    Some(StorageIdentitySubject::Task { identity }) => identity,
                    _ => IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            self.unresolved_cohort,
                            ResourceId::from(id),
                        ),
                    },
                },
            },
            Target::SchemaRevision { class_id, revision } => StorageCapturedIdentity::Version {
                reference: match subject {
                    Some(StorageIdentitySubject::Resource { version, .. }) => {
                        IdentityReference::Known { identity: version }
                    }
                    _ => IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            self.unresolved_cohort,
                            ResourceId::from(class_id),
                        )
                        .at_revision(
                            ResourceRevision::new(revision.get())
                                .expect("positive schema revision"),
                        ),
                    },
                },
            },
            Target::History { .. } => {
                unreachable!("resource records supply owning-resource references")
            }
        }
    }

    pub(super) fn capture(&mut self, value: &impl IdentityRecord) {
        let target = value.target();
        let previous = self.entries.get(&target);
        let revision = value.revision();
        let subject = match previous.and_then(|entry| entry.evidence.subject()) {
            Some(StorageIdentitySubject::Resource { identity, version }) => {
                StorageIdentitySubject::Resource {
                    identity,
                    version: if previous.is_some_and(|entry| entry.revision == revision) {
                        version
                    } else {
                        VersionIdentity::generate()
                    },
                }
            }
            Some(subject) => subject,
            None => match target {
                Target::Resource { .. } => StorageIdentitySubject::Resource {
                    identity: IdentityReference::Known {
                        identity: ResourceIdentity::generate(),
                    },
                    version: VersionIdentity::generate(),
                },
                Target::Principal(_) => StorageIdentitySubject::Principal {
                    identity: IdentityReference::Known {
                        identity: PrincipalIdentity::generate(),
                    },
                },
                Target::Task(_) => StorageIdentitySubject::Task {
                    identity: IdentityReference::Known {
                        identity: TaskIdentity::generate(),
                    },
                },
                _ => unreachable!("owning record target"),
            },
        };
        let dependencies = value.references();
        let mut references: BTreeMap<_, _> = dependencies
            .iter()
            .map(|(path, &target)| {
                let captured = previous
                    .filter(|entry| entry.references.get(path) == Some(&target))
                    .and_then(|entry| entry.evidence.references().get(path))
                    .copied()
                    .unwrap_or_else(|| self.reference(target));
                (path.to_owned(), captured)
            })
            .collect();
        if let Some(previous) = previous {
            for (path, reference) in previous.evidence.references() {
                if matches!(
                    reference,
                    StorageCapturedIdentity::Principal { .. }
                        | StorageCapturedIdentity::Task { .. }
                ) {
                    references.entry(path.clone()).or_insert(*reference);
                }
            }
        }
        let evidence = StorageIdentityEvidence::try_new(Some(subject), references)
            .expect("bounded, statically named references from validated records");
        self.retain_version(&evidence, revision);
        self.entries.insert(
            target,
            Entry {
                evidence,
                revision,
                references: dependencies,
            },
        );
    }
}

impl MemoryState {
    pub(super) fn store_membership(
        &mut self,
        key: (i32, i32),
        value: StoragePrincipalGroup,
    ) -> Option<StoragePrincipalGroup> {
        if !self.memberships.contains_key(&key) {
            self.logical_identities
                .associations
                .remove(&Association::Membership(
                    value.principal_id(),
                    value.group_id(),
                ));
        }
        self.logical_identities.capture_association(
            Association::Membership(value.principal_id(), value.group_id()),
            references([
                (
                    "/principal_id",
                    Some(Target::Principal(value.principal_id())),
                ),
                ("/group_id", Some(resource(Family::Group, value.group_id()))),
            ]),
        );
        self.memberships.insert(key, value)
    }

    pub(super) fn store_authorization_grant(
        &mut self,
        key: (i32, i32),
        value: StorageAuthorizationGrant,
    ) -> Option<StorageAuthorizationGrant> {
        if !self.authorization_grants.contains_key(&key) {
            self.logical_identities
                .associations
                .remove(&Association::Permission(value.id()));
        }
        self.logical_identities.capture_association(
            Association::Permission(value.id()),
            references([
                (
                    "/collection_id",
                    Some(resource(Family::Collection, value.collection_id())),
                ),
                ("/group_id", Some(resource(Family::Group, value.group_id()))),
            ]),
        );
        self.authorization_grants.insert(key, value)
    }

    pub(super) fn store_service_account(
        &mut self,
        id: i32,
        value: StorageServiceAccount,
    ) -> Option<StorageServiceAccount> {
        if !self.service_accounts.contains_key(&id) {
            self.logical_identities
                .associations
                .remove(&Association::ServiceAccount(value.id().into()));
        }
        self.logical_identities.capture_association(
            Association::ServiceAccount(value.id().into()),
            references([
                (
                    "/owner_group_id",
                    Some(resource(Family::Group, value.owner_group_id())),
                ),
                ("/created_by", value.created_by().map(Target::Principal)),
            ]),
        );
        self.service_accounts.insert(id, value)
    }

    pub(super) fn store_sink_grant(&mut self, sink: EventSinkId, collection: CollectionId) -> bool {
        if self
            .event_sink_grants
            .contains(&(sink.id(), collection.id()))
        {
            return false;
        }
        self.logical_identities
            .associations
            .remove(&Association::SinkGrant(sink, collection));
        self.logical_identities.capture_association(
            Association::SinkGrant(sink, collection),
            references([
                ("/sink_id", Some(resource(Family::EventSink, sink))),
                (
                    "/collection_id",
                    Some(resource(Family::Collection, collection)),
                ),
            ]),
        );
        self.event_sink_grants.insert((sink.id(), collection.id()))
    }
}

macro_rules! store_record {
    ($method:ident, $field:ident, $type:ty) => {
        impl MemoryState {
            pub(super) fn $method(&mut self, id: i32, value: $type) -> Option<$type> {
                if !self.$field.contains_key(&id) {
                    self.logical_identities.entries.remove(&value.target());
                }
                self.logical_identities.capture(&value);
                self.$field.insert(id, value)
            }
        }
    };
}

store_record!(store_collection, collections, StorageCollection);
store_record!(store_class, classes, StorageClass);
store_record!(store_object, objects, StorageObject);
store_record!(store_class_relation, class_relations, StorageClassRelation);
store_record!(
    store_object_relation,
    object_relations,
    StorageObjectRelation
);
store_record!(store_identity_scope, identity_scopes, StorageIdentityScope);
store_record!(store_group, groups, StorageIdentityGroup);
store_record!(store_principal, principals, StoragePrincipal);
store_record!(store_task, tasks, MemoryTaskRecord);
store_record!(
    store_export_template,
    export_templates,
    StorageExportTemplate
);
store_record!(store_remote_target, remote_targets, StorageRemoteTarget);
store_record!(store_event_sink, event_sinks, StorageEventSink);
store_record!(
    store_event_subscription,
    event_subscriptions,
    StorageEventSubscription
);
store_record!(
    store_computed_field,
    computed_fields,
    StorageComputedFieldDefinition
);
impl MemoryState {
    pub(super) fn store_query_usage(
        &mut self,
        value: StorageQueryUsageDeclaration,
    ) -> StorageQueryUsageDeclaration {
        let id = value.metadata().id().id();
        let target = value.target();
        if !self.query_usage.contains_key(&id) {
            self.logical_identities.entries.remove(&target);
        }
        self.logical_identities.capture(&value);
        let value =
            value.with_logical_identity(self.logical_identities.entries[&target].evidence.clone());
        self.query_usage.insert(id, value.clone());
        value
    }
}

#[async_trait]
impl LogicalIdentityStorage for MemoryStorage {
    async fn resolve_logical_identity(
        &self,
        target: Target,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        let state = self.state.read().await;
        let missing = || StorageError::not_found("Logical identity target was not found");
        match target {
            Target::History { kind, id } => {
                if let Some(entry) = state
                    .history
                    .iter()
                    .find(|entry| entry.id == id && entry.value.identity_kind() == kind)
                {
                    return Ok(entry.logical_identity.clone());
                }
                if kind == History::ComputedDefinitions {
                    return state
                        .computed_history
                        .iter()
                        .find(|record| record.history_entry_id() == id)
                        .and_then(|record| record.logical_identity())
                        .cloned()
                        .ok_or_else(missing);
                }
                let (rows, key) = match kind {
                    History::ClassSchema => (Some(&state.schema_history), "id"),
                    History::ClassRelation => (
                        state
                            .relation_history
                            .get(&StorageBackupHistorySection::ClassRelationHistory),
                        "history_entry_id",
                    ),
                    History::ObjectRelation => (
                        state
                            .relation_history
                            .get(&StorageBackupHistorySection::ObjectRelationHistory),
                        "history_entry_id",
                    ),
                    _ => (None, "history_entry_id"),
                };
                let value = rows
                    .and_then(|rows| {
                        rows.iter().find(|row| {
                            row.get(key).and_then(serde_json::Value::as_i64) == Some(id.id())
                        })
                    })
                    .and_then(|row| row.get("logical_identity"))
                    .ok_or_else(missing)?;
                return serde_json::from_value(value.clone())
                    .map_err(|error| StorageError::backend_failure(error.to_string()));
            }
            Target::SchemaRevision { class_id, revision } => {
                return state
                    .schema_revisions
                    .get(&(class_id.id(), revision.get()))
                    .and_then(StorageSchemaRevision::logical_identity)
                    .cloned()
                    .ok_or_else(missing);
            }
            Target::Resource { kind, id } => {
                let exists = match kind {
                    Family::Collection => state.collections.contains_key(&id.id()),
                    Family::Class => state.classes.contains_key(&id.id()),
                    Family::Object => state.objects.contains_key(&id.id()),
                    Family::ClassRelation => state.class_relations.contains_key(&id.id()),
                    Family::ObjectRelation => state.object_relations.contains_key(&id.id()),
                    Family::ExportTemplate => state.export_templates.contains_key(&id.id()),
                    Family::RemoteTarget => state.remote_targets.contains_key(&id.id()),
                    Family::IdentityScope => state.identity_scopes.contains_key(&id.id()),
                    Family::Group => state.groups.contains_key(&id.id()),
                    Family::ComputedDefinition => state.computed_fields.contains_key(&id.id()),
                    Family::EventSink => state.event_sinks.contains_key(&id.id()),
                    Family::EventSubscription => state.event_subscriptions.contains_key(&id.id()),
                    Family::QueryUsageDeclaration => state.query_usage.contains_key(&id.id()),
                };
                if !exists {
                    return Err(missing());
                }
            }
            Target::Principal(id) if !state.principals.contains_key(&id.id()) => {
                return Err(missing());
            }
            Target::Task(id) if !state.tasks.contains_key(&id.id()) => return Err(missing()),
            _ => {}
        }
        state
            .logical_identities
            .entries
            .get(&target)
            .map(|entry| entry.evidence.clone())
            .ok_or_else(|| StorageError::not_found("Logical identity target was not found"))
    }
}

impl MemoryState {
    pub(super) fn store_import_result(&mut self, result: StorageImportTaskResult) {
        self.logical_identities.capture_association(
            Association::ImportResult(result.id()),
            references([("/task_id", Some(Target::Task(result.task_id())))]),
        );
        self.import_task_results
            .entry(result.task_id().id())
            .or_default()
            .push(result);
    }

    pub(super) fn capture_task_output(&mut self, task_id: TaskId) {
        self.logical_identities.capture_association(
            Association::TaskOutput(task_id),
            references([("/task_id", Some(Target::Task(task_id)))]),
        );
    }

    pub(super) fn store_event_delivery(
        &mut self,
        id: i64,
        delivery: StorageEventDelivery,
    ) -> Option<StorageEventDelivery> {
        if !self.event_deliveries.contains_key(&id) {
            let event = self
                .events
                .iter()
                .find(|event| event.as_parts().0.id() == delivery.event_id());
            let reference = match event {
                Some(event) => IdentityReference::Known {
                    identity: event.as_parts().0.event_id().into(),
                },
                None => IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(
                        self.logical_identities.unresolved_cohort,
                        LegacyRecordId::new(delivery.event_id().get())
                            .expect("positive event sequence"),
                    ),
                },
            };
            let evidence = StorageIdentityEvidence::try_new(
                None,
                BTreeMap::from([
                    (
                        "/event_id".into(),
                        StorageCapturedIdentity::Event { reference },
                    ),
                    (
                        "/subscription_id".into(),
                        self.logical_identities.reference(resource(
                            Family::EventSubscription,
                            delivery.subscription_id(),
                        )),
                    ),
                ]),
            )
            .expect("bounded delivery dependencies");
            self.logical_identities
                .restore_association(Association::Delivery(delivery.id()), evidence);
        }
        self.event_deliveries.insert(id, delivery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(future: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future);
    }

    #[test]
    fn history_closure_keeps_its_version_identity() {
        run(async {
            let storage = MemoryStorage::new();
            let target = Target::History {
                kind: History::Collection,
                id: HistoryRecordId::new(1).unwrap(),
            };
            let before = storage.resolve_logical_identity(target).await.unwrap();
            storage
                .update_collection(
                    CollectionId::new(1).unwrap(),
                    StorageCollectionUpdate::new(None, Some("changed".into())),
                    &EventContext::system(),
                )
                .await
                .unwrap()
                .into_value();
            let after = storage.resolve_logical_identity(target).await.unwrap();
            assert_eq!(before, after);
            let current = storage
                .resolve_logical_identity(resource(
                    Family::Collection,
                    CollectionId::new(1).unwrap(),
                ))
                .await
                .unwrap();
            assert_ne!(
                before
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .version(),
                current
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .version()
            );
        });
    }

    #[test]
    fn reused_integer_cannot_inherit_a_deleted_class_identity() {
        run(async {
            let storage = MemoryStorage::new();
            let create =
                || StorageClassCreate::builder("class", CollectionId::new(1).unwrap(), "").build();
            let first = storage
                .create_class(create(), &EventContext::system())
                .await
                .unwrap()
                .into_value();
            let target = resource(Family::Class, first.id());
            let before = storage.resolve_logical_identity(target).await.unwrap();
            let resolved = storage
                .resolve_class(StorageClassSelector::Id(first.id()))
                .await
                .unwrap();
            storage
                .delete_class(&resolved, &EventContext::system())
                .await
                .unwrap()
                .into_value();
            assert!(storage.resolve_logical_identity(target).await.is_err());
            storage.state.write().await.next_class_id = first.id().id();
            storage
                .create_class(create(), &EventContext::system())
                .await
                .unwrap()
                .into_value();
            let after = storage.resolve_logical_identity(target).await.unwrap();
            assert_ne!(
                before
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource(),
                after
                    .subject()
                    .unwrap()
                    .record_identity()
                    .unwrap()
                    .resource()
            );
        });
    }
}
