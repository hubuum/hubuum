//! Identity validation and quarantined legacy upgrade, before adapter replay.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use hubuum_domain::{
    IdentityReference, LegacyIdentityCohort, LegacyRecordId, PrincipalIdentity, ResourceIdentity,
    ResourceRevision, TaskIdentity, UnresolvedIdentity, VersionIdentity,
};
use hubuum_events_core::EventId;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{
    StorageBackupHistorySection as History, StorageBackupRow, StorageBackupStateSection as State,
};
use crate::{
    StorageCapturedIdentity, StorageError, StorageIdentityEvidence,
    StorageIdentityResourceKind as Family, StorageIdentitySubject,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum IdentityFamily {
    Resource(Family),
    Principal,
    PrincipalExtension,
    Task,
    Event,
    ClassVersion,
    Association,
}

pub(super) fn state_family(section: State) -> Option<IdentityFamily> {
    use IdentityFamily as Kind;
    Some(match section {
        State::Collections => Kind::Resource(Family::Collection),
        State::Classes => Kind::Resource(Family::Class),
        State::Objects => Kind::Resource(Family::Object),
        State::ClassRelations => Kind::Resource(Family::ClassRelation),
        State::ObjectRelations => Kind::Resource(Family::ObjectRelation),
        State::ExportTemplates => Kind::Resource(Family::ExportTemplate),
        State::RemoteTargets => Kind::Resource(Family::RemoteTarget),
        State::IdentityScopes => Kind::Resource(Family::IdentityScope),
        State::Groups => Kind::Resource(Family::Group),
        State::ComputedFieldDefinitions => Kind::Resource(Family::ComputedDefinition),
        State::EventSinks => Kind::Resource(Family::EventSink),
        State::EventSubscriptions => Kind::Resource(Family::EventSubscription),
        State::QueryUsageDeclarations => Kind::Resource(Family::QueryUsageDeclaration),
        State::Principals => Kind::Principal,
        State::Users | State::ServiceAccounts => Kind::PrincipalExtension,
        State::ClassSchemaRevisions => Kind::ClassVersion,
        State::GroupMemberships
        | State::GroupMembershipSources
        | State::CollectionPermissionGrants
        | State::EventSinkCollectionGrants
        | State::ObjectSchemaEvidence => Kind::Association,
        State::CollectionAuthorization | State::CollectionHierarchy | State::ClassSchemaState => {
            return None;
        }
    })
}

pub(super) fn history_family(section: History) -> Option<IdentityFamily> {
    use IdentityFamily as Kind;
    Some(match section {
        History::ExportArtifacts => return None,
        History::CollectionHistory => Kind::Resource(Family::Collection),
        History::ClassHistory => Kind::Resource(Family::Class),
        History::ObjectHistory => Kind::Resource(Family::Object),
        History::ClassRelationHistory => Kind::Resource(Family::ClassRelation),
        History::ObjectRelationHistory => Kind::Resource(Family::ObjectRelation),
        History::ExportTemplateHistory => Kind::Resource(Family::ExportTemplate),
        History::RemoteTargetHistory => Kind::Resource(Family::RemoteTarget),
        History::ClassSchemaHistory | History::ComputedFieldHistory => Kind::ClassVersion,
        History::TerminalTasks => Kind::Task,
        History::AuditEvents => Kind::Event,
        History::ImportResults
        | History::ExportOutputs
        | History::RemoteCallResults
        | History::TerminalEventDeliveries => Kind::Association,
    })
}

fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::invalid_input(message)
}

fn legacy_id(row: &StorageBackupRow, field: &str) -> Result<LegacyRecordId, StorageError> {
    row.get(field)
        .and_then(Value::as_i64)
        .ok_or_else(|| invalid(format!("Identity upgrade requires a positive {field}")))
        .and_then(|id| LegacyRecordId::new(id).map_err(|error| invalid(error.to_string())))
}

/// Stable only inside one explicitly persisted restore cohort. Length-delimited
/// input and domain separation prevent different families or key shapes aliasing.
pub(super) fn derived_uuid(cohort: LegacyIdentityCohort, family: &str, key: &Value) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"hubuum.legacy-identity.v1\0");
    digest.update(cohort.as_bytes());
    digest.update((family.len() as u64).to_be_bytes());
    digest.update(family.as_bytes());
    digest.update(serde_json::to_vec(key).expect("JSON values serialize"));
    let mut bytes: [u8; 16] = digest.finalize()[..16].try_into().expect("digest prefix");
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    bytes
}

fn family_name(family: Family) -> String {
    serde_json::to_value(family)
        .expect("closed family serializes")
        .as_str()
        .expect("family string")
        .to_owned()
}

/// One legacy incarnation requires both its integer and creation timestamp.
/// Missing evidence remains unresolved; it is never replaced by a current-row join.
pub(super) fn legacy_subject(
    family: IdentityFamily,
    section: &str,
    row: &StorageBackupRow,
    cohort: LegacyIdentityCohort,
) -> Result<Option<StorageIdentitySubject>, StorageError> {
    let version_key = json!([
        row.get("history_entry_id"),
        row.get("id"),
        row.get("class_id"),
        row.get("revision")
    ]);
    let version = VersionIdentity::from_bytes(derived_uuid(cohort, section, &version_key))
        .expect("derived RFC UUID");
    let subject = match family {
        IdentityFamily::Resource(_) | IdentityFamily::Principal | IdentityFamily::Task => {
            let id = legacy_id(row, "id")?;
            let created_at = row
                .get("created_at")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Legacy incarnation requires its creation timestamp"))?;
            let name = match family {
                IdentityFamily::Resource(kind) => family_name(kind),
                IdentityFamily::Principal => "principal".into(),
                IdentityFamily::Task => "task".into(),
                _ => unreachable!(),
            };
            let bytes = derived_uuid(cohort, &name, &json!([id, created_at]));
            match family {
                IdentityFamily::Resource(_) => StorageIdentitySubject::Resource {
                    identity: IdentityReference::Known {
                        identity: ResourceIdentity::from_bytes(bytes).expect("derived RFC UUID"),
                    },
                    version,
                },
                IdentityFamily::Principal => StorageIdentitySubject::Principal {
                    identity: IdentityReference::Known {
                        identity: PrincipalIdentity::from_bytes(bytes).expect("derived RFC UUID"),
                    },
                },
                IdentityFamily::Task => StorageIdentitySubject::Task {
                    identity: IdentityReference::Known {
                        identity: TaskIdentity::from_bytes(bytes).expect("derived RFC UUID"),
                    },
                },
                _ => unreachable!(),
            }
        }
        IdentityFamily::PrincipalExtension => StorageIdentitySubject::Principal {
            identity: IdentityReference::Unresolved {
                legacy: UnresolvedIdentity::new(cohort, legacy_id(row, "id")?),
            },
        },
        IdentityFamily::ClassVersion => StorageIdentitySubject::Resource {
            identity: IdentityReference::Unresolved {
                legacy: UnresolvedIdentity::new(cohort, legacy_id(row, "class_id")?),
            },
            version,
        },
        IdentityFamily::Event => StorageIdentitySubject::Event {
            identity: serde_json::from_value(
                row.get("event_id")
                    .cloned()
                    .ok_or_else(|| invalid("Event UUID is missing"))?,
            )
            .map_err(|error| invalid(format!("Invalid event UUID: {error}")))?,
        },
        IdentityFamily::Association => return Ok(None),
    };
    Ok(Some(subject))
}

/// Native backups must already contain identities. Legacy upgrade assigns an
/// isolated cohort and retains unresolved references rather than guessing links
/// to current resources, archives or deletion receipts.
#[derive(Clone, Copy, Debug)]
pub enum StorageIdentityUpgrade {
    Native,
    Legacy(LegacyIdentityCohort),
}

pub(super) fn upgrade_schema_snapshot(
    row: &mut StorageBackupRow,
    cohort: LegacyIdentityCohort,
) -> Result<(), StorageError> {
    let Some(snapshot) = row
        .get("snapshot")
        .filter(|value| value.get("class_id").is_some() && value.get("revision").is_some())
    else {
        return Ok(());
    };
    let mut snapshot = StorageBackupRow::try_from_value(snapshot.clone())
        .map_err(|error| error.into_request_error())?;
    // A legacy envelope cannot smuggle native correspondence through its
    // embedded schema snapshot. Assign it in the same isolated cohort as the
    // standalone revision, even if an unexpected identity field was supplied.
    let evidence = StorageIdentityEvidence::try_new(
        legacy_subject(
            IdentityFamily::ClassVersion,
            State::ClassSchemaRevisions.as_str(),
            &snapshot,
            cohort,
        )?,
        legacy_references(&snapshot, cohort)?,
    )
    .map_err(|error| error.into_request_error())?;
    snapshot.0.insert(
        "logical_identity".into(),
        serde_json::to_value(evidence).map_err(|error| invalid(error.to_string()))?,
    );
    row.0.insert("snapshot".into(), snapshot.into_value());
    Ok(())
}

/// Identity-validated rows prepared in quarantine. This proof is retained until
/// the adapter enters its fenced restore transaction.
#[derive(Clone, Debug)]
pub struct StorageIdentityReplay {
    pub(super) replay: super::StorageBackupReplay,
    pub(super) legacy_cohort: Option<LegacyIdentityCohort>,
    pub(super) unresolved_references: usize,
}

impl StorageIdentityReplay {
    pub fn with_schema_limits(
        mut self,
        limits: hubuum_domain::JsonSchemaLimits,
    ) -> Result<Self, StorageError> {
        self.replay = self.replay.with_schema_limits(limits)?;
        Ok(self)
    }

    pub fn at_restore_boundary(
        self,
        context: crate::StorageRestoreIdentityContext,
    ) -> Result<Self, StorageError> {
        let mut prepared = self
            .replay
            .identity_restore_boundary(context.cohort(), context.confirmed_at())?;
        prepared.legacy_cohort = self.legacy_cohort;
        Ok(prepared)
    }
    #[must_use]
    pub const fn replay(&self) -> &super::StorageBackupReplay {
        &self.replay
    }

    #[must_use]
    pub const fn legacy_cohort(&self) -> Option<LegacyIdentityCohort> {
        self.legacy_cohort
    }

    #[must_use]
    pub const fn unresolved_references(&self) -> usize {
        self.unresolved_references
    }

    #[must_use]
    pub fn into_replay(self) -> super::StorageBackupReplay {
        self.replay
    }
}

const RESOURCE_FIELDS: &[(&str, Family)] = &[
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
];
const PRINCIPAL_FIELDS: &[&str] = &[
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
];

fn resource_family(kind: &str) -> Option<Family> {
    Some(match kind {
        "collection" => Family::Collection,
        "class" | "class_schema" => Family::Class,
        "object" | "object_validation" => Family::Object,
        "class_relation" => Family::ClassRelation,
        "object_relation" => Family::ObjectRelation,
        "group" => Family::Group,
        "export_template" => Family::ExportTemplate,
        "remote_target" => Family::RemoteTarget,
        "event_sink" => Family::EventSink,
        "event_subscription" => Family::EventSubscription,
        "computed_field_definition" => Family::ComputedDefinition,
        "query_usage_declaration" => Family::QueryUsageDeclaration,
        _ => return None,
    })
}

pub(super) fn legacy_references(
    row: &StorageBackupRow,
    cohort: LegacyIdentityCohort,
) -> Result<BTreeMap<String, StorageCapturedIdentity>, StorageError> {
    let mut result = BTreeMap::new();
    if row.get("entity_type").is_some()
        && let Some(metadata) = row.get("metadata").and_then(Value::as_object)
    {
        // Only the event contract's scalar references are inspected. In
        // particular, metadata.data and before/after user payloads stay opaque.
        let fields = metadata
            .iter()
            .filter(|(field, value)| {
                value.is_number()
                    && (RESOURCE_FIELDS
                        .iter()
                        .any(|(name, _)| name == &field.as_str())
                        || PRINCIPAL_FIELDS.contains(&field.as_str())
                        || field.as_str() == "task_id")
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let typed = StorageBackupRow::try_from_value(Value::Object(fields))
            .map_err(|error| error.into_request_error())?;
        for (path, reference) in legacy_references(&typed, cohort)? {
            result.insert(format!("/metadata{path}"), reference);
        }
        if let Some(collections) = metadata
            .get("related_collection_ids")
            .and_then(Value::as_array)
        {
            for (index, id) in collections.iter().enumerate() {
                let Some(id) = id.as_i64() else {
                    continue;
                };
                result.insert(
                    format!("/metadata/related_collection_ids/{index}"),
                    StorageCapturedIdentity::Resource {
                        family: Family::Collection,
                        reference: IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(
                                cohort,
                                LegacyRecordId::new(id)
                                    .map_err(|error| invalid(error.to_string()))?,
                            ),
                        },
                    },
                );
            }
        }
        for field in ["schema", "before_schema"] {
            if let Some(schema) = metadata
                .get(field)
                .filter(|value| value.get("class_id").is_some())
            {
                let schema = StorageBackupRow::try_from_value(schema.clone())
                    .map_err(|error| error.into_request_error())?;
                result.insert(
                    format!("/metadata/{field}/revision"),
                    unresolved_version(&schema, "class_id", "revision", cohort)?,
                );
                for (path, reference) in legacy_references(&schema, cohort)? {
                    result.insert(format!("/metadata/{field}{path}"), reference);
                }
            }
        }
        for field in ["schema_revision", "before_schema_revision"] {
            if metadata.get(field).is_some_and(Value::is_number) {
                let owner = StorageBackupRow::try_from_value(json!({"class_id": metadata.get("class_id").or(row.get("entity_id")), "revision": metadata[field]}))
                    .map_err(|error| error.into_request_error())?;
                result.insert(
                    format!("/metadata/{field}"),
                    unresolved_version(&owner, "class_id", "revision", cohort)?,
                );
            }
        }
        if let Some(id) = metadata
            .get("dependent_rebuild_task_id")
            .and_then(Value::as_i64)
        {
            result.insert(
                "/metadata/dependent_rebuild_task_id".into(),
                StorageCapturedIdentity::Task {
                    reference: IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            cohort,
                            LegacyRecordId::new(id).map_err(|error| invalid(error.to_string()))?,
                        ),
                    },
                },
            );
        }
        if metadata
            .get("object_revision")
            .is_some_and(Value::is_number)
        {
            let owner = StorageBackupRow::try_from_value(json!({"object_id": row.get("entity_id"), "object_revision": metadata["object_revision"]}))
                .map_err(|error| error.into_request_error())?;
            result.insert(
                "/metadata/object_revision".into(),
                unresolved_version(&owner, "object_id", "object_revision", cohort)?,
            );
        }
    }
    for (field, kind) in [("entity_id", "entity_type"), ("subject_id", "subject_type")] {
        if row.get(field).is_none_or(Value::is_null) {
            continue;
        }
        let legacy = UnresolvedIdentity::new(cohort, legacy_id(row, field)?);
        let reference = match row.get(kind).and_then(Value::as_str) {
            Some("user" | "service_account") => Some(StorageCapturedIdentity::Principal {
                reference: IdentityReference::Unresolved { legacy },
            }),
            Some("task") => Some(StorageCapturedIdentity::Task {
                reference: IdentityReference::Unresolved { legacy },
            }),
            Some(kind) => resource_family(kind).map(|family| StorageCapturedIdentity::Resource {
                family,
                reference: IdentityReference::Unresolved { legacy },
            }),
            None => None,
        };
        if let Some(reference) = reference {
            result.insert(format!("/{field}"), reference);
            if field == "entity_id" && matches!(reference, StorageCapturedIdentity::Resource { .. })
            {
                for version in ["before_revision", "after_revision"] {
                    if row.get(version).is_some_and(|value| !value.is_null()) {
                        result.insert(
                            format!("/{version}"),
                            unresolved_version(row, field, version, cohort)?,
                        );
                    }
                }
            }
        }
    }
    if row.get("event_id").is_some_and(Value::is_number) {
        result.insert(
            "/event_id".into(),
            StorageCapturedIdentity::Event {
                reference: IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(cohort, legacy_id(row, "event_id")?),
                },
            },
        );
    }
    if row.get("entity_type").and_then(Value::as_str) == Some("restore")
        && let Some(id) = row
            .get("metadata")
            .and_then(|value| value.pointer("/initiated_by/principal_id"))
            .and_then(Value::as_i64)
    {
        result.insert(
            "/metadata/initiated_by/principal_id".into(),
            StorageCapturedIdentity::Principal {
                reference: IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(
                        cohort,
                        LegacyRecordId::new(id).map_err(|error| invalid(error.to_string()))?,
                    ),
                },
            },
        );
    }
    for &(field, family) in RESOURCE_FIELDS {
        if row.get(field).is_none_or(Value::is_null) {
            continue;
        }
        result.insert(
            format!("/{field}"),
            StorageCapturedIdentity::Resource {
                family,
                reference: IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(cohort, legacy_id(row, field)?),
                },
            },
        );
    }
    for &field in PRINCIPAL_FIELDS {
        if row.get(field).is_none_or(Value::is_null) {
            continue;
        }
        result.insert(
            format!("/{field}"),
            StorageCapturedIdentity::Principal {
                reference: IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(cohort, legacy_id(row, field)?),
                },
            },
        );
    }
    if row.get("task_id").is_some_and(|value| !value.is_null()) {
        result.insert(
            "/task_id".into(),
            StorageCapturedIdentity::Task {
                reference: IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(cohort, legacy_id(row, "task_id")?),
                },
            },
        );
    }
    for (field, owner) in [
        ("schema_revision", "class_id"),
        ("object_revision", "object_id"),
    ] {
        if row.get(field).is_some_and(|value| !value.is_null()) {
            result.insert(
                format!("/{field}"),
                unresolved_version(row, owner, field, cohort)?,
            );
        }
    }
    if let Some(includes) = row
        .get("include")
        .and_then(|value| value.get("related_objects"))
        .and_then(Value::as_object)
    {
        for (alias, include) in includes {
            let alias = alias.replace('~', "~0").replace('/', "~1");
            let include = StorageBackupRow::try_from_value(include.clone())
                .map_err(|error| error.into_request_error())?;
            for (field, family) in [
                ("class_id", Family::Class),
                ("class_relation_id", Family::ClassRelation),
            ] {
                if include.get(field).is_none_or(Value::is_null) {
                    continue;
                }
                result.insert(
                    format!("/include/related_objects/{alias}/{field}"),
                    StorageCapturedIdentity::Resource {
                        family,
                        reference: IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(cohort, legacy_id(&include, field)?),
                        },
                    },
                );
            }
        }
    }
    if let Some(definitions) = row.get("definitions").and_then(Value::as_array) {
        for (index, definition) in definitions.iter().enumerate() {
            let definition = StorageBackupRow::try_from_value(definition.clone())
                .map_err(|error| error.into_request_error())?;
            result.insert(
                format!("/definitions/{index}/id"),
                StorageCapturedIdentity::Resource {
                    family: Family::ComputedDefinition,
                    reference: IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(cohort, legacy_id(&definition, "id")?),
                    },
                },
            );
            result.insert(
                format!("/definitions/{index}/revision"),
                unresolved_version(&definition, "id", "revision", cohort)?,
            );
            result.insert(
                format!("/definitions/{index}/class_id"),
                StorageCapturedIdentity::Resource {
                    family: Family::Class,
                    reference: IdentityReference::Unresolved {
                        legacy: UnresolvedIdentity::new(
                            cohort,
                            legacy_id(&definition, "class_id")?,
                        ),
                    },
                },
            );
        }
    }
    if let Some(snapshot) = row.get("snapshot").filter(|value| value.is_object()) {
        let snapshot = StorageBackupRow::try_from_value(snapshot.clone())
            .map_err(|error| error.into_request_error())?;
        if snapshot.get("class_id").is_some() && snapshot.get("revision").is_some() {
            result.insert(
                "/snapshot/revision".into(),
                unresolved_version(&snapshot, "class_id", "revision", cohort)?,
            );
        }
        for (path, reference) in legacy_references(&snapshot, cohort)? {
            result.insert(format!("/snapshot{path}"), reference);
        }
    }
    if let Some(metadata) = row
        .get("discovery_metadata")
        .and_then(|value| value.get("data"))
        .filter(|value| value.is_object())
    {
        let metadata = StorageBackupRow::try_from_value(metadata.clone())
            .map_err(|error| error.into_request_error())?;
        for (path, reference) in legacy_references(&metadata, cohort)? {
            result.insert(format!("/discovery_metadata/data{path}"), reference);
        }
        for (field, family) in [
            ("template_id", Family::ExportTemplate),
            ("remote_target_id", Family::RemoteTarget),
        ] {
            if metadata.get(field).is_some_and(|value| !value.is_null()) {
                result.insert(
                    format!("/discovery_metadata/data/{field}"),
                    StorageCapturedIdentity::Resource {
                        family,
                        reference: IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(cohort, legacy_id(&metadata, field)?),
                        },
                    },
                );
            }
        }
        if let Some(target) = metadata.get("target").filter(|value| value.is_object()) {
            let target = StorageBackupRow::try_from_value(target.clone())
                .map_err(|error| error.into_request_error())?;
            for (path, reference) in legacy_references(&target, cohort)? {
                result.insert(format!("/discovery_metadata/data/target{path}"), reference);
            }
            if let Some(family) =
                target
                    .get("type")
                    .and_then(Value::as_str)
                    .and_then(|kind| match kind {
                        "class_relation" => Some(Family::ClassRelation),
                        "object_relation" => Some(Family::ObjectRelation),
                        _ => None,
                    })
            {
                result.insert(
                    "/discovery_metadata/data/target/relation_id".into(),
                    StorageCapturedIdentity::Resource {
                        family,
                        reference: IdentityReference::Unresolved {
                            legacy: UnresolvedIdentity::new(
                                cohort,
                                legacy_id(&target, "relation_id")?,
                            ),
                        },
                    },
                );
            }
        }
    }
    Ok(result)
}

fn unresolved_version(
    row: &StorageBackupRow,
    owner: &str,
    revision: &str,
    cohort: LegacyIdentityCohort,
) -> Result<StorageCapturedIdentity, StorageError> {
    let revision = row
        .get(revision)
        .and_then(Value::as_i64)
        .ok_or_else(|| invalid("Missing identity dependency revision"))?;
    let revision = ResourceRevision::new(revision).map_err(|error| invalid(error.to_string()))?;
    Ok(StorageCapturedIdentity::Version {
        reference: IdentityReference::Unresolved {
            legacy: UnresolvedIdentity::new(cohort, legacy_id(row, owner)?).at_revision(revision),
        },
    })
}

/// Validates native identity evidence without turning absence into new UUIDs.
/// Registry keys are retained identity values, never today's integer lookup keys.
#[derive(Default)]
pub(super) struct IdentityValidation {
    resources: BTreeSet<ResourceIdentity>,
    resource_owners: BTreeMap<ResourceIdentity, (Family, LegacyRecordId)>,
    live_resources: BTreeMap<(Family, LegacyRecordId), IdentityReference<ResourceIdentity>>,
    live_dependencies: Vec<(Family, LegacyRecordId, ResourceIdentity)>,
    principals: BTreeMap<LegacyRecordId, IdentityReference<PrincipalIdentity>>,
    principal_identities: BTreeSet<PrincipalIdentity>,
    tasks: BTreeSet<TaskIdentity>,
    versions: BTreeSet<VersionIdentity>,
    incarnations: BTreeMap<(Family, LegacyRecordId, String), IdentityReference<ResourceIdentity>>,
    live_versions: BTreeMap<ResourceIdentity, VersionIdentity>,
    live_version_owners: BTreeMap<VersionIdentity, IdentityReference<ResourceIdentity>>,
    events: HashSet<EventId>,
}

impl IdentityValidation {
    pub(super) fn finish(&self) -> Result<(), StorageError> {
        for &(family, id, captured) in &self.live_dependencies {
            if let Some(IdentityReference::Known { identity }) =
                self.live_resources.get(&(family, id))
                && *identity != captured
            {
                return Err(invalid(
                    "Live resource dependency disagrees with its referenced incarnation",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn row(
        &mut self,
        family: IdentityFamily,
        row: &StorageBackupRow,
        historical: bool,
    ) -> Result<StorageIdentityEvidence, StorageError> {
        let evidence: StorageIdentityEvidence = serde_json::from_value(
            row.get("logical_identity")
                .cloned()
                .ok_or_else(|| invalid("Native backup is missing logical identity evidence"))?,
        )
        .map_err(|error| invalid(format!("Invalid logical identity evidence: {error}")))?;
        match (family, evidence.subject()) {
            (
                IdentityFamily::Resource(kind),
                Some(StorageIdentitySubject::Resource { identity, version }),
            ) => {
                let created_at = row
                    .get("created_at")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("Resource identity requires its creation timestamp"))?;
                let key = (kind, legacy_id(row, "id")?, created_at.to_owned());
                if !historical {
                    self.live_resources
                        .insert((kind, legacy_id(row, "id")?), identity);
                }
                if let IdentityReference::Known { identity } = identity {
                    let owner = (kind, legacy_id(row, "id")?);
                    if self
                        .resource_owners
                        .insert(identity, owner)
                        .is_some_and(|previous| previous != owner)
                    {
                        return Err(invalid(
                            "Resource identity is shared by different resource owners",
                        ));
                    }
                }
                if let Some(previous) = self.incarnations.insert(key, identity)
                    && previous != identity
                {
                    return Err(invalid(
                        "Versions of one resource incarnation disagree on its identity",
                    ));
                }
                if historical {
                    if row.get("valid_to").is_some_and(Value::is_null)
                        && row.get("op").and_then(Value::as_str) != Some("D")
                        && self
                            .live_resources
                            .get(&(kind, legacy_id(row, "id")?))
                            .is_some_and(|live| *live != identity)
                    {
                        return Err(invalid(
                            "Live resource and open historical version disagree on resource identity",
                        ));
                    }
                    if let Some(owner) = self.live_version_owners.get(&version) {
                        if *owner != identity {
                            return Err(invalid(
                                "Version identity belongs to a different resource",
                            ));
                        }
                        if row.get("valid_to").is_none_or(|value| !value.is_null())
                            || row.get("op").and_then(Value::as_str) == Some("D")
                        {
                            return Err(invalid(
                                "Live version identity is shared with a closed historical version",
                            ));
                        }
                    }
                    if !self.versions.insert(version) {
                        return Err(invalid("Duplicate historical version identity"));
                    }
                    if row.get("valid_to").is_some_and(Value::is_null)
                        && let IdentityReference::Known { identity } = identity
                        && self
                            .live_versions
                            .get(&identity)
                            .is_some_and(|live| *live != version)
                    {
                        return Err(invalid(
                            "Live resource and open historical version disagree on version identity",
                        ));
                    }
                } else if let IdentityReference::Known { identity } = identity
                    && !self.resources.insert(identity)
                {
                    return Err(invalid("Duplicate live resource identity"));
                }
                if !historical {
                    if self.live_version_owners.insert(version, identity).is_some()
                        || self.versions.contains(&version)
                    {
                        return Err(invalid("Duplicate live version identity"));
                    }
                    if let IdentityReference::Known { identity } = identity {
                        self.live_versions.insert(identity, version);
                    }
                }
            }
            (
                IdentityFamily::ClassVersion,
                Some(StorageIdentitySubject::Resource { version, .. }),
            ) => {
                if !self.versions.insert(version) || self.live_version_owners.contains_key(&version)
                {
                    return Err(invalid("Duplicate schema/definition version identity"));
                }
            }
            (IdentityFamily::Principal, Some(StorageIdentitySubject::Principal { identity })) => {
                self.principals.insert(legacy_id(row, "id")?, identity);
                if let IdentityReference::Known { identity } = identity
                    && !self.principal_identities.insert(identity)
                {
                    return Err(invalid("Duplicate principal identity"));
                }
            }
            (
                IdentityFamily::PrincipalExtension,
                Some(StorageIdentitySubject::Principal { identity }),
            ) => {
                if self.principals.get(&legacy_id(row, "id")?) != Some(&identity) {
                    return Err(invalid(
                        "Principal extension identity does not match its owning principal",
                    ));
                }
            }
            (IdentityFamily::Task, Some(StorageIdentitySubject::Task { identity })) => {
                if let IdentityReference::Known { identity } = identity
                    && !self.tasks.insert(identity)
                {
                    return Err(invalid("Duplicate task identity"));
                }
            }
            (IdentityFamily::Event, Some(StorageIdentitySubject::Event { identity })) => {
                let represented: EventId = serde_json::from_value(
                    row.get("event_id")
                        .cloned()
                        .ok_or_else(|| invalid("Missing event UUID"))?,
                )
                .map_err(|error| invalid(error.to_string()))?;
                if represented != identity || !self.events.insert(identity) {
                    return Err(invalid("Duplicate or mismatched event identity"));
                }
            }
            (IdentityFamily::Association, None) => {}
            _ => {
                return Err(invalid(
                    "Logical identity subject does not match its backup section",
                ));
            }
        }
        if !historical {
            for &(field, family) in RESOURCE_FIELDS {
                if row.get(field).is_none_or(Value::is_null) {
                    continue;
                }
                if let Some(StorageCapturedIdentity::Resource {
                    reference: IdentityReference::Known { identity },
                    ..
                }) = evidence.references().get(&format!("/{field}"))
                {
                    self.live_dependencies
                        .push((family, legacy_id(row, field)?, *identity));
                }
            }
        }
        // Only the path and reference kind are inspected here. This fixed local
        // placeholder is never retained or exposed as correspondence evidence.
        let mut bytes = [0; 16];
        bytes[6] = 0x80;
        bytes[8] = 0x80;
        let shape_cohort = LegacyIdentityCohort::from_bytes(bytes).expect("RFC UUID placeholder");
        for (path, expected) in legacy_references(row, shape_cohort)? {
            let actual = evidence.references().get(&path).ok_or_else(|| {
                invalid(format!("Missing captured identity dependency at {path}"))
            })?;
            let matches = match (expected, *actual) {
                (
                    StorageCapturedIdentity::Resource { family: left, .. },
                    StorageCapturedIdentity::Resource { family: right, .. },
                ) => left == right,
                (
                    StorageCapturedIdentity::Principal { .. },
                    StorageCapturedIdentity::Principal { .. },
                )
                | (StorageCapturedIdentity::Task { .. }, StorageCapturedIdentity::Task { .. })
                | (
                    StorageCapturedIdentity::Version { .. },
                    StorageCapturedIdentity::Version { .. },
                )
                | (StorageCapturedIdentity::Event { .. }, StorageCapturedIdentity::Event { .. }) => {
                    true
                }
                _ => false,
            };
            if !matches {
                return Err(invalid(format!(
                    "Captured identity dependency has the wrong kind at {path}"
                )));
            }
        }
        Ok(evidence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::open(true)]
    #[case::closed(false)]
    fn live_version_identity_can_only_be_shared_with_its_open_history(#[case] open: bool) {
        let mut row = StorageBackupRow::try_from_value(json!({
            "id": 42, "created_at": "2026-01-01T00:00:00Z", "revision": 2,
            "logical_identity": {
                "subject": {"kind": "resource", "identity": {"state": "known", "identity": ResourceIdentity::generate()}, "version": VersionIdentity::generate()},
                "references": {},
            },
        })).unwrap();
        let mut validation = IdentityValidation::default();
        let family = IdentityFamily::Resource(Family::Collection);
        validation.row(family, &row, false).unwrap();
        row.0.insert(
            "valid_to".into(),
            if open {
                Value::Null
            } else {
                json!("2026-01-02T00:00:00Z")
            },
        );
        assert_eq!(validation.row(family, &row, true).is_ok(), open);
    }

    #[test]
    fn legacy_schema_snapshot_cannot_preserve_injected_native_correspondence() {
        let cohort = LegacyIdentityCohort::generate();
        let native = ResourceIdentity::generate();
        let mut row = StorageBackupRow::try_from_value(json!({
            "snapshot": {"class_id": 7, "revision": 3,
                "logical_identity": {"subject": {"kind": "resource", "identity": {"state": "known", "identity": native}, "version": VersionIdentity::generate()}, "references": {}}
            }
        })).unwrap();
        upgrade_schema_snapshot(&mut row, cohort).unwrap();
        let evidence: StorageIdentityEvidence =
            serde_json::from_value(row.get("snapshot").unwrap()["logical_identity"].clone())
                .unwrap();
        assert!(matches!(
            evidence.subject(),
            Some(StorageIdentitySubject::Resource {
                identity: IdentityReference::Unresolved { .. },
                ..
            })
        ));
    }

    #[rstest]
    #[case::class("class_id", Family::Class)]
    #[case::relation("class_relation_id", Family::ClassRelation)]
    fn template_selector_paths_capture_only_defined_dependencies(
        #[case] field: &str,
        #[case] family: Family,
    ) {
        let row = StorageBackupRow::try_from_value(json!({
            "include": {"related_objects": {"a/b~c": {field: 7}}},
            "data": {"class_id": 99},
        }))
        .unwrap();
        let references = legacy_references(&row, LegacyIdentityCohort::generate()).unwrap();
        assert_eq!(references.len(), 1);
        assert!(
            matches!(references[&format!("/include/related_objects/a~1b~0c/{field}")],
            StorageCapturedIdentity::Resource { family: actual, reference: IdentityReference::Unresolved { .. } } if actual == family)
        );
    }

    fn resource(history_id: i64, revision: i64) -> StorageBackupRow {
        StorageBackupRow::try_from_value(json!({
            "id": 42, "created_at": "2026-01-01T00:00:00Z", "revision": revision,
            "history_entry_id": history_id, "collection_id": 7,
        }))
        .unwrap()
    }

    #[test]
    fn legacy_versions_share_one_incarnation_and_have_distinct_versions() {
        let cohort = LegacyIdentityCohort::generate();
        let identities = (1..=10)
            .map(|revision| {
                legacy_subject(
                    IdentityFamily::Resource(Family::Object),
                    "object_history",
                    &resource(revision, revision),
                    cohort,
                )
                .unwrap()
                .unwrap()
                .record_identity()
                .unwrap()
            })
            .collect::<Vec<_>>();
        let resources = identities
            .iter()
            .map(|identity| identity.resource())
            .collect::<BTreeSet<_>>();
        let versions = identities
            .iter()
            .map(|identity| identity.version())
            .collect::<BTreeSet<_>>();
        assert_eq!((resources.len(), versions.len()), (1, 10));
    }

    #[rstest]
    #[case::retry(true)]
    #[case::different_import(false)]
    fn retry_reuses_assignments_only_within_its_restore_cohort(#[case] same_cohort: bool) {
        let first = LegacyIdentityCohort::generate();
        let second = if same_cohort {
            first
        } else {
            LegacyIdentityCohort::generate()
        };
        let subject = |cohort| {
            legacy_subject(
                IdentityFamily::Resource(Family::Object),
                "object_history",
                &resource(1, 1),
                cohort,
            )
            .unwrap()
        };
        assert_eq!(subject(first) == subject(second), same_cohort);
    }

    #[test]
    fn native_backup_cannot_omit_a_historical_dependency() {
        let cohort = LegacyIdentityCohort::generate();
        let mut row = resource(1, 1);
        row.0.insert("logical_identity".into(), json!({
            "subject": legacy_subject(IdentityFamily::Resource(Family::Object), "object_history", &row, cohort).unwrap(),
            "references": {},
        }));
        let error = IdentityValidation::default()
            .row(IdentityFamily::Resource(Family::Object), &row, true)
            .unwrap_err();
        assert!(error.to_string().contains("/collection_id"));
    }
}
