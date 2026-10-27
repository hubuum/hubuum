pub(crate) mod access;
pub(crate) mod computed;
pub(crate) mod navigation;

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use hubuum_storage_core::StorageIdentityEvidence;

mod name;
mod relations;
use hubuum_domain::{ClassId, CollectionId, ObjectId, PrincipalIdentity, ResourceId};
pub use name::{class_objects_as_of, object_history_by_name};

use crate::errors::ApiError;
use crate::events::Provenance;
use crate::models::search::QueryOptions;
use crate::models::{
    CollectionHistory, ExportTemplateHistory, HubuumClassHistory, HubuumObjectHistory,
    RemoteTargetHistory, ResourceRevision,
};
use crate::pagination::SKIPPED_TOTAL_COUNT;
use crate::storage::{
    HistoryStorage, StorageClassHistoryRecord, StorageCollectionHistoryRecord, StorageContext,
    StorageExportTemplateHistoryRecord, StorageHistoryAsOfQuery, StorageHistoryCollectionScope,
    StorageHistoryListQuery, StorageHistoryMetadata, StorageHistoryOperation,
    StorageObjectHistoryAsOfQuery, StorageObjectHistoryRecord, StorageRemoteTargetHistoryRecord,
    storage_handle,
};

/// Collection visibility applied before history rows are counted or paged.
#[derive(Clone, Copy)]
pub enum HistoryCollectionFilter<'a> {
    All,
    Visible(&'a [i32]),
}

fn history_total(total: Option<i64>, options: &QueryOptions) -> Result<i64, ApiError> {
    if !options.include_total() {
        return Ok(SKIPPED_TOTAL_COUNT);
    }
    total.ok_or_else(|| {
        ApiError::InternalServerError(
            "Storage history query omitted its required exact total".to_string(),
        )
    })
}

impl TryFrom<HistoryCollectionFilter<'_>> for StorageHistoryCollectionScope {
    type Error = ApiError;

    fn try_from(value: HistoryCollectionFilter<'_>) -> Result<Self, Self::Error> {
        match value {
            HistoryCollectionFilter::All => Ok(Self::All),
            HistoryCollectionFilter::Visible(collection_ids) => Ok(Self::Visible(
                collection_ids
                    .iter()
                    .copied()
                    .map(CollectionId::new)
                    .collect::<Result<Vec<_>, _>>()?,
            )),
        }
    }
}

struct AppHistoryMetadata {
    operation: String,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    actor_id: Option<i32>,
    history_id: i64,
    actor_kind: Option<String>,
    initiator_principal_id: Option<i32>,
    task_id: Option<i32>,
    revision: ResourceRevision,
    logical_identity: Option<StorageIdentityEvidence>,
}

impl From<StorageHistoryMetadata> for AppHistoryMetadata {
    fn from(value: StorageHistoryMetadata) -> Self {
        let parts = value.into_parts();
        // Preserve the established HTTP history representation while the
        // storage contract uses semantic operation values.
        let operation = match parts.operation() {
            StorageHistoryOperation::Create => "I",
            StorageHistoryOperation::Update => "U",
            StorageHistoryOperation::Delete => "D",
        }
        .to_string();
        Self {
            operation,
            valid_from: parts.valid_from(),
            valid_to: parts.valid_to(),
            actor_id: parts.actor_id().map(|id| id.id()),
            history_id: parts.history_entry_id().id(),
            actor_kind: parts.actor_kind().map(str::to_owned),
            initiator_principal_id: parts.initiator_principal_id().map(|id| id.id()),
            task_id: parts.task_id().map(|id| id.id()),
            revision: parts.revision(),
            logical_identity: parts.logical_identity().cloned(),
        }
    }
}

fn collection_from_storage(
    row: StorageCollectionHistoryRecord,
) -> Result<CollectionHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (id, name, description, created_at, updated_at, parent_collection_id, _) =
        record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(CollectionHistory {
        id: id.id(),
        name,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        parent_collection_id: parent_collection_id.map(|id| id.id()),
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        logical_identity: metadata.logical_identity,
        revision: metadata.revision,
    })
}

fn class_from_storage(row: StorageClassHistoryRecord) -> Result<HubuumClassHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (
        id,
        name,
        collection_id,
        json_schema,
        validate_schema,
        description,
        created_at,
        updated_at,
        _,
    ) = record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(HubuumClassHistory {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        json_schema,
        validate_schema,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        logical_identity: metadata.logical_identity,
        revision: metadata.revision,
    })
}

fn object_from_storage(row: StorageObjectHistoryRecord) -> Result<HubuumObjectHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at, _) =
        record.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(HubuumObjectHistory {
        id: id.id(),
        name,
        collection_id: collection_id.id(),
        hubuum_class_id: hubuum_class_id.id(),
        data,
        description,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        logical_identity: metadata.logical_identity,
        revision: metadata.revision,
    })
}

fn export_template_from_storage(
    row: StorageExportTemplateHistoryRecord,
) -> Result<ExportTemplateHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (record_metadata, collection_id, name, definition) = record.into_parts();
    let definition = definition.into_parts();
    let (id, created_at, updated_at, _) = record_metadata.into_parts();
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(ExportTemplateHistory {
        id: id.id(),
        collection_id: collection_id.id(),
        name,
        description: definition.description().to_string(),
        content_type: definition.content_type().to_string(),
        template: definition.template().to_string(),
        kind: definition.kind().to_string(),
        scope_kind: definition.scope_kind().map(str::to_string),
        class_id: definition.class_id().map(|id| id.id()),
        default_query: definition.default_query().map(str::to_string),
        include: definition.include().cloned(),
        relation_context: definition.relation_context().cloned(),
        default_missing_data_policy: definition.default_missing_data_policy().map(str::to_string),
        default_limits: definition.default_limits().cloned(),
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        logical_identity: metadata.logical_identity,
        revision: metadata.revision,
    })
}

fn remote_target_from_storage(
    row: StorageRemoteTargetHistoryRecord,
) -> Result<RemoteTargetHistory, ApiError> {
    let (record, metadata) = row.into_parts();
    let (record_metadata, collection_id, name, definition) = record.into_parts();
    let (description, transport, policy) = definition.into_parts();
    let transport = transport.into_parts();
    let method = transport.method();
    let url_template = transport.url_template().to_owned();
    let headers_template = transport.headers_template().clone();
    let body_template = transport.body_template().map(str::to_owned);
    let auth_config = transport.auth_config().clone();
    let timeout_ms = transport.timeout_ms();
    let (class_id, allowed_subject_types, enabled) = policy.into_parts();
    let (id, created_at, updated_at, _) = record_metadata.into_parts();
    let allowed_subject_types = serde_json::to_value(
        allowed_subject_types
            .into_iter()
            .map(|subject_type| subject_type.as_str())
            .collect::<Vec<_>>(),
    )?;
    let metadata = AppHistoryMetadata::from(metadata);
    Ok(RemoteTargetHistory {
        id: id.id(),
        collection_id: collection_id.id(),
        class_id: class_id.map(|id| id.id()),
        name,
        description,
        method: method.as_str().to_string(),
        url_template,
        headers_template,
        body_template,
        auth_config,
        allowed_subject_types,
        timeout_ms,
        enabled,
        created_at: created_at.naive_utc(),
        updated_at: updated_at.naive_utc(),
        op: metadata.operation,
        valid_from: metadata.valid_from,
        valid_to: metadata.valid_to,
        actor_id: metadata.actor_id,
        history_id: metadata.history_id,
        actor_kind: metadata.actor_kind,
        initiator_user_id: metadata.initiator_principal_id,
        task_id: metadata.task_id,
        logical_identity: metadata.logical_identity,
        revision: metadata.revision,
    })
}

/// Names indexed by the captured principal UUID, never the reusable REST id.
#[derive(Default)]
pub(crate) struct CapturedPrincipalNames(BTreeMap<PrincipalIdentity, String>);

impl CapturedPrincipalNames {
    pub(crate) fn enrich(
        &self,
        evidence: Option<&StorageIdentityEvidence>,
        mut provenance: Provenance,
    ) -> Provenance {
        for (path, principal) in [
            ("/actor_principal_id", &mut provenance.actor.principal),
            ("/initiator_principal_id", &mut provenance.initiator),
        ] {
            if let Some(principal) = principal {
                principal.name = evidence
                    .and_then(|evidence| evidence.principal_reference(path))
                    .and_then(|identity| self.0.get(&identity))
                    .cloned();
            }
        }
        provenance
    }
}

pub(crate) async fn resolve_principal_names(
    backend: &impl StorageContext,
    principal_ids: Vec<PrincipalIdentity>,
) -> Result<CapturedPrincipalNames, ApiError> {
    Ok(CapturedPrincipalNames(
        storage_handle(backend)
            .resolve_history_principal_names(principal_ids)
            .await?
            .into_iter()
            .map(|row| row.into_parts())
            .collect(),
    ))
}

macro_rules! history_service {
    ($list:ident, $as_of:ident, $storage_list:ident, $storage_as_of:ident, $from:ident, $ty:ty) => {
        pub async fn $list(
            entity_id: i32,
            backend: &impl StorageContext,
            query_options: &QueryOptions,
            collection_filter: HistoryCollectionFilter<'_>,
        ) -> Result<(Vec<$ty>, i64), ApiError> {
            let (rows, total_count) = storage_handle(backend)
                .$storage_list(StorageHistoryListQuery::new(
                    ResourceId::new(entity_id)?,
                    query_options.clone(),
                    collection_filter.try_into()?,
                ))
                .await?
                .into_parts();
            Ok((
                rows.into_iter().map($from).collect::<Result<_, _>>()?,
                history_total(total_count, query_options)?,
            ))
        }

        pub async fn $as_of(
            entity_id: i32,
            at: DateTime<Utc>,
            backend: &impl StorageContext,
        ) -> Result<Option<$ty>, ApiError> {
            storage_handle(backend)
                .$storage_as_of(StorageHistoryAsOfQuery::new(
                    ResourceId::new(entity_id)?,
                    at,
                ))
                .await?
                .map($from)
                .transpose()
        }
    };
}

history_service!(
    collection_history_paginated_with_total_count,
    collection_as_of,
    list_collection_history,
    get_collection_history_as_of,
    collection_from_storage,
    CollectionHistory
);
pub async fn class_as_of(
    class_id: i32,
    at: DateTime<Utc>,
    backend: &impl StorageContext,
) -> Result<Option<HubuumClassHistory>, ApiError> {
    storage_handle(backend)
        .get_class_history_as_of(StorageHistoryAsOfQuery::new(ResourceId::new(class_id)?, at))
        .await?
        .map(class_from_storage)
        .transpose()
}

history_service!(
    export_template_history_paginated_with_total_count,
    export_template_as_of,
    list_export_template_history,
    get_export_template_history_as_of,
    export_template_from_storage,
    ExportTemplateHistory
);
history_service!(
    remote_target_history_paginated_with_total_count,
    remote_target_as_of,
    list_remote_target_history,
    get_remote_target_history_as_of,
    remote_target_from_storage,
    RemoteTargetHistory
);

pub async fn object_as_of(
    object_id: i32,
    class_id: i32,
    at: DateTime<Utc>,
    backend: &impl StorageContext,
) -> Result<Option<HubuumObjectHistory>, ApiError> {
    storage_handle(backend)
        .get_object_history_as_of(StorageObjectHistoryAsOfQuery::new(
            ObjectId::new(object_id)?,
            ClassId::new(class_id)?,
            at,
        ))
        .await?
        .map(object_from_storage)
        .transpose()
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    use crate::events::StoredProvenance;
    use hubuum_domain::{
        IdentityReference, LegacyIdentityCohort, LegacyRecordId, UnresolvedIdentity,
    };
    use hubuum_storage_core::{StorageCapturedIdentity, StorageIdentityEvidence};
    use rstest::rstest;
    use std::collections::BTreeMap;

    #[rstest]
    #[case::same_principal(true)]
    #[case::reused_integer(false)]
    fn names_require_the_captured_principal_identity(#[case] same_identity: bool) {
        let captured = PrincipalIdentity::generate();
        let current = if same_identity {
            captured
        } else {
            PrincipalIdentity::generate()
        };
        let names = CapturedPrincipalNames(BTreeMap::from([(current, "current-name".into())]));
        let evidence = StorageIdentityEvidence::try_new(
            None,
            BTreeMap::from([(
                "/actor_principal_id".into(),
                StorageCapturedIdentity::Principal {
                    reference: IdentityReference::Known { identity: captured },
                },
            )]),
        )
        .unwrap();
        let provenance = StoredProvenance::from_actor_kind(Some("user"))
            .with_actor_user_id(Some(42))
            .without_names();
        assert_eq!(
            names
                .enrich(Some(&evidence), provenance)
                .actor
                .principal
                .unwrap()
                .name
                .as_deref(),
            same_identity.then_some("current-name")
        );
    }

    #[rstest]
    #[case::distinct_identities(true, true)]
    #[case::unresolved_actor(false, true)]
    #[case::unresolved_initiator(true, false)]
    fn shared_integer_does_not_mix_actor_and_initiator_names(
        #[case] actor_known: bool,
        #[case] initiator_known: bool,
    ) {
        let actor = PrincipalIdentity::generate();
        let initiator = PrincipalIdentity::generate();
        let names = CapturedPrincipalNames(BTreeMap::from([
            (actor, "actor".into()),
            (initiator, "initiator".into()),
        ]));
        let reference = |identity, known| StorageCapturedIdentity::Principal {
            reference: if known {
                IdentityReference::Known { identity }
            } else {
                IdentityReference::Unresolved {
                    legacy: UnresolvedIdentity::new(
                        LegacyIdentityCohort::generate(),
                        LegacyRecordId::new(42).unwrap(),
                    ),
                }
            },
        };
        let evidence = StorageIdentityEvidence::try_new(
            None,
            BTreeMap::from([
                ("/actor_principal_id".into(), reference(actor, actor_known)),
                (
                    "/initiator_principal_id".into(),
                    reference(initiator, initiator_known),
                ),
            ]),
        )
        .unwrap();
        let provenance = StoredProvenance::from_actor_kind(Some("user"))
            .with_actor_user_id(Some(42))
            .with_initiator_user_id(Some(42))
            .without_names();
        let provenance = names.enrich(Some(&evidence), provenance);
        assert_eq!(
            (
                provenance.actor.principal.unwrap().name,
                provenance.initiator.unwrap().name
            ),
            (
                actor_known.then(|| "actor".to_owned()),
                initiator_known.then(|| "initiator".to_owned())
            ),
        );
    }
}
