use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::sql_types::{
    BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use futures_util::TryStreamExt;
use hubuum_storage_core::*;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::PostgresStorage;
use crate::{PostgresConnection, PostgresStorageError};

fn history_scan_select(family: StorageIdentityHistoryKind) -> String {
    let (table, columns) = match family {
        StorageIdentityHistoryKind::Collection => (
            "collections_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::Class => (
            "hubuumclass_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::Object => (
            "hubuumobject_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::ClassRelation => (
            "hubuumclass_relation_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::ObjectRelation => (
            "hubuumobject_relation_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::ExportTemplate => (
            "export_templates_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::RemoteTarget => (
            "remote_targets_history",
            "CASE WHEN op='D' THEN valid_from ELSE valid_to END AS anchor, op='D' AS deleted",
        ),
        StorageIdentityHistoryKind::ComputedDefinitions => (
            "computed_field_history",
            "valid_to AS anchor, FALSE AS deleted",
        ),
        StorageIdentityHistoryKind::ClassSchema => (
            "class_schema_history",
            "occurred_at AS anchor, FALSE AS deleted",
        ),
    };
    // Identifiers and projections come only from the closed family vocabulary.
    let interval = if family == StorageIdentityHistoryKind::ClassSchema {
        "occurred_at AS valid_from, NULL::TIMESTAMPTZ AS valid_to"
    } else {
        "valid_from, valid_to"
    };
    format!("SELECT logical_identity, {columns}, {interval} FROM public.{table}")
}

#[derive(QueryableByName)]
struct MaterialRow {
    #[diesel(sql_type = Jsonb)]
    material: Value,
}
#[derive(QueryableByName)]
struct CommittedRow {
    #[diesel(sql_type = Bool)]
    committed: bool,
}
#[derive(QueryableByName)]
struct PublicationGenerationRow {
    #[diesel(sql_type = BigInt)]
    generation: i64,
}
#[derive(QueryableByName)]
struct HistoryRow {
    #[diesel(sql_type = Timestamptz)]
    valid_from: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    valid_to: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    logical_identity: Option<Value>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    anchor: Option<DateTime<Utc>>,
    #[diesel(sql_type = Bool)]
    deleted: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capture {
    observed_at: DateTime<Utc>,
    control: HistoryLifecycleControl,
    incarnation: Uuid,
    deletion_revision: HistoryDeletionRevision,
}
/// Caller holds deletion authority. Pin application/history authority before
/// any task/control or exact-row locks. Failure leaves exposure quarantined;
/// this path never removes a current or protected-open payload.
pub(super) async fn require_history_exposure_safe_on(
    connection: &mut PostgresConnection,
) -> Result<HistoryLifecycleControl, PostgresStorageError> {
    sql_query("SELECT id FROM hubuum_control.application_control WHERE id=1 FOR UPDATE")
        .execute(connection)
        .await?;
    let material = sql_query(
        "SELECT material FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR SHARE",
    )
    .get_result::<MaterialRow>(connection)
    .await?
    .material;
    let control: HistoryLifecycleControl = serde_json::from_value(material).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("history exposure authority", error)
    })?;
    let undrained = sql_query(
        "SELECT EXISTS(SELECT 1 FROM hubuum_control.history_publication_owners) AS committed",
    )
    .get_result::<CommittedRow>(connection)
    .await?
    .committed;
    if undrained {
        return Err(PostgresStorageError::unavailable(
            "History publication drivers or producers lack positive retirement; keep quarantine",
        ));
    }
    let journal = control.authority().expiry();
    if journal.intents().is_empty() {
        let projected = sql_query(
            "SELECT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members) AS committed",
        )
        .get_result::<CommittedRow>(connection)
        .await?
        .committed;
        if projected {
            return Err(PostgresStorageError::unavailable(
                "Retained history projection lacks matching authority; keep quarantine",
            ));
        }
        return Ok(control);
    }
    #[derive(QueryableByName)]
    struct Setting {
        #[diesel(sql_type=Text)]
        value: String,
    }
    let previous = sql_query("SELECT current_setting('statement_timeout') AS value")
        .get_result::<Setting>(connection)
        .await?
        .value;
    // Reduce an existing timeout; never expand a caller's tighter bound.
    sql_query("SELECT set_config('statement_timeout',CASE WHEN $1::INTERVAL=INTERVAL '0' THEN $2::TEXT ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM $1::INTERVAL)*1000,$2)))::BIGINT::TEXT END,true)")
        .bind::<Text,_>(&previous).bind::<BigInt,_>(i64::from(HistoryRecoveryExposureBudget::MAX_STATEMENT_MILLISECONDS))
        .execute(connection).await?;
    // Reconcile the derived read projection before exposure. The existing
    // owner-only function restores missing original membership and refuses
    // conflicting/ahead facts; it never rewrites authority or its clocks.
    sql_query("SELECT public.hubuum_history_expiry_sync()")
        .execute(connection)
        .await?;
    #[derive(QueryableByName)]
    struct Identity {
        #[diesel(sql_type=Nullable<Jsonb>)]
        logical_identity: Option<Value>,
        #[diesel(sql_type=BigInt)]
        encoded_bytes: i64,
    }
    use StorageIdentityHistoryKind as Family;
    // Complete current/open inventory for the nine supported history families.
    // Tombstone D records are closed deletion events, not protected live data.
    const SOURCES: &[(Family, &str, &str)] = &[
        (Family::Collection, "collections", "TRUE"),
        (
            Family::Collection,
            "collections_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::Class, "hubuumclass", "TRUE"),
        (
            Family::Class,
            "hubuumclass_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::Object, "hubuumobject", "TRUE"),
        (
            Family::Object,
            "hubuumobject_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::ClassRelation, "hubuumclass_relation", "TRUE"),
        (
            Family::ClassRelation,
            "hubuumclass_relation_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::ObjectRelation, "hubuumobject_relation", "TRUE"),
        (
            Family::ObjectRelation,
            "hubuumobject_relation_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::ExportTemplate, "export_templates", "TRUE"),
        (
            Family::ExportTemplate,
            "export_templates_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (Family::RemoteTarget, "remote_targets", "TRUE"),
        (
            Family::RemoteTarget,
            "remote_targets_history",
            "valid_to IS NULL AND op<>'D'",
        ),
        (
            Family::ComputedDefinitions,
            "computed_field_history",
            "valid_to IS NULL",
        ),
        (Family::ClassSchema, "class_schema_revisions", "TRUE"),
    ];
    let mut budget = HistoryRecoveryExposureBudget::default();
    for &(family, table, filter) in SOURCES {
        if !journal.intents().iter().any(|intent| {
            intent
                .manifest()
                .members()
                .iter()
                .any(|member| member.family() == family)
        }) {
            continue;
        }
        // Do not transfer an oversized identity into Rust just to reject it.
        // All dynamic SQL identifiers and filters are closed constants above.
        let query = format!(
            "SELECT CASE WHEN SUM(encoded_bytes) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)<=$1 THEN logical_identity END AS logical_identity, encoded_bytes FROM (SELECT logical_identity,coalesce(octet_length(logical_identity::TEXT),0)::BIGINT AS encoded_bytes FROM public.{table} WHERE {filter} LIMIT $2) bounded"
        );
        let rows = sql_query(query)
            .bind::<BigInt, _>(budget.remaining_bytes() as i64)
            .bind::<BigInt, _>(i64::from(budget.remaining_rows()) + 1)
            .load_stream::<Identity>(connection)
            .await?;
        futures_util::pin_mut!(rows);
        while let Some(row) = rows.try_next().await? {
            let bytes = usize::try_from(row.encoded_bytes).map_err(|_| {
                PostgresStorageError::unavailable(
                    "Invalid exposure identity length; keep quarantine",
                )
            })?;
            budget
                .observe(bytes)
                .map_err(|error| PostgresStorageError::unavailable(error.to_string()))?;
            let evidence = row
                .logical_identity
                .map(serde_json::from_value::<StorageIdentityEvidence>)
                .transpose()
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value(
                        "current exposure identity",
                        error,
                    )
                })?;
            if journal.excludes_evidence(family, evidence.as_ref()) {
                return Err(PostgresStorageError::unavailable(
                    "Current or protected-open copy carries expired or unresolved history ancestry; keep quarantine for explicit reconciliation",
                ));
            }
        }
    }
    sql_query("SELECT set_config('statement_timeout',$1,true)")
        .bind::<Text, _>(previous)
        .execute(connection)
        .await?;
    Ok(control)
}

pub(crate) async fn capture_on(
    connection: &mut PostgresConnection,
) -> Result<HistoryLifecycleSnapshot, PostgresStorageError> {
    let row = sql_query("SELECT public.hubuum_history_lifecycle_capture() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    let capture: Capture = serde_json::from_value(row.material).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("history lifecycle control", error)
    })?;
    HistoryLifecycleSnapshot::new(
        capture.control,
        capture.incarnation,
        capture.deletion_revision,
        capture.observed_at,
    )
    .map_err(Into::into)
}

async fn admit_publication_on(
    connection: &mut PostgresConnection,
    kind: HistoryPublicationKind,
    application: StorageExecutionPermit,
    scope: &StorageExecutionScope,
) -> Result<HistoryPublicationOwner, PostgresStorageError> {
    let snapshot = capture_on(connection).await?;
    let generation =
        sql_query("SELECT public.hubuum_history_publication_generation() AS generation")
            .get_result::<PublicationGenerationRow>(connection)
            .await?
            .generation;
    let generation =
        HistoryPublicationGeneration::new(u64::try_from(generation).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("history publication generation", error)
        })?)?;
    let epoch = HistoryReadEpoch::for_publication(kind, application, &snapshot, generation)?
        .bind_scope(scope)?;
    let owner = HistoryPublicationOwner::generate(kind, epoch);
    let context = owner.context();
    let epoch = context.epoch();
    let runtime = epoch
        .application()
        .owner()
        .expect("validated registered epoch");
    let admitted = sql_query(
        "SELECT public.hubuum_history_publication_admit($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) AS committed",
    )
    .bind::<SqlUuid, _>(context.id())
    .bind::<SqlUuid, _>(context.ownership_token())
    .bind::<diesel::sql_types::Text, _>(kind.as_str())
    .bind::<SqlUuid, _>(runtime.id())
    .bind::<SqlUuid, _>(runtime.ownership_token())
    .bind::<SqlUuid, _>(epoch.history().incarnation())
    .bind::<BigInt, _>(epoch.history().deletion_revision().value() as i64)
    .bind::<BigInt, _>(epoch.history().authority_revision().value() as i64)
    .bind::<BigInt, _>(generation.value() as i64)
    .bind::<Nullable<SqlUuid>, _>(scope.api_request_override().map(StorageApiRequestPermit::owner))
    .get_result::<CommittedRow>(connection)
    .await?
    .committed;
    if !admitted {
        return Err(PostgresStorageError::unavailable(
            "Publication owner capacity, protocol or epoch prevents admission",
        ));
    }
    Ok::<_, PostgresStorageError>(owner)
}

async fn admit_producer_on(
    connection: &mut PostgresConnection,
    context: HistoryReadContext,
) -> Result<HistoryProducerOwner, PostgresStorageError> {
    let producer = HistoryProducerOwner::generate(context.clone());
    let epoch = context.epoch();
    let runtime = epoch
        .application()
        .owner()
        .expect("registered publication epoch");
    let committed = sql_query(
        "SELECT public.hubuum_history_producer_admit($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) AS committed",
    )
    .bind::<SqlUuid, _>(producer.context().id())
    .bind::<SqlUuid, _>(producer.context().ownership_token())
    .bind::<SqlUuid, _>(context.id())
    .bind::<SqlUuid, _>(context.ownership_token())
    .bind::<SqlUuid, _>(runtime.id())
    .bind::<SqlUuid, _>(runtime.ownership_token())
    .bind::<SqlUuid, _>(epoch.history().incarnation())
    .bind::<BigInt, _>(epoch.history().deletion_revision().value() as i64)
    .bind::<BigInt, _>(epoch.history().authority_revision().value() as i64)
    .bind::<BigInt, _>(epoch.generation().value() as i64)
    .get_result::<CommittedRow>(connection)
    .await?
    .committed;
    if !committed {
        return Err(PostgresStorageError::unavailable(
            "Producer slot is unavailable or fenced",
        ));
    }
    Ok::<_, PostgresStorageError>(producer)
}

#[async_trait]
impl HistoryLifecycleStorage for PostgresStorage {
    async fn admit_history_archive_execution(
        &self,
    ) -> Result<HistoryArchiveExecutionOwner, StorageError> {
        let scope = self.current_execution_scope();
        let application = scope
            .application_access_override()
            .cloned()
            .ok_or_else(|| {
                StorageError::unavailable("Archive requires original application admission")
            })?;
        if application.owner().is_none() {
            return Err(StorageError::unavailable(
                "Archive requires registered runtime ownership",
            ));
        }
        self.runtime()
            .with_history_publication_admission(async move |connection| {
                let parent = admit_publication_on(
                    connection,
                    HistoryPublicationKind::ArchiveRead,
                    application,
                    &scope,
                )
                .await?;
                let child = admit_producer_on(connection, parent.context()).await?;
                HistoryArchiveExecutionOwner::from_admitted(parent, child, scope)
                    .map_err(PostgresStorageError::from)
            })
            .await
            .map_err(Into::into)
    }
    async fn admit_history_publication(
        &self,
        kind: HistoryPublicationKind,
    ) -> Result<HistoryPublicationOwner, StorageError> {
        let scope = self.current_execution_scope();
        if scope.history_read_override().is_some() {
            return Err(StorageError::conflict(
                "Nested work must inherit its original history publication owner",
            ));
        }
        let application = scope
            .application_access_override()
            .cloned()
            .ok_or_else(|| {
                StorageError::unavailable("Publication requires captured application admission")
            })?;
        if application.owner().is_none() {
            return Err(StorageError::unavailable(
                "Publication requires registered runtime ownership",
            ));
        }
        self.runtime()
            .with_history_publication_admission(async move |connection| {
                admit_publication_on(connection, kind, application, &scope).await
            })
            .await
            .map_err(Into::into)
    }
    async fn settle_history_publication(
        &self,
        settlement: HistoryPublicationSettlement,
    ) -> Result<(), StorageError> {
        crate::runtime::ensure_no_storage_callback()?;
        self.runtime()
            .with_control_transaction(async move |connection| {
                let context = settlement.context();
                let settled = sql_query(
                    "SELECT public.hubuum_history_publication_settle($1,$2) AS committed",
                )
                .bind::<SqlUuid, _>(context.id())
                .bind::<SqlUuid, _>(context.ownership_token())
                .get_result::<CommittedRow>(connection)
                .await?
                .committed;
                if !settled {
                    return Err(PostgresStorageError::conflict(
                        "Publication settlement owner is missing or mismatched",
                    ));
                }
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(Into::into)
    }
    async fn admit_history_producer(
        &self,
        context: HistoryReadContext,
    ) -> Result<HistoryProducerOwner, StorageError> {
        if self.current_execution_scope().history_read_override() != Some(&context) {
            return Err(StorageError::unavailable(
                "Producer must inherit its original publication scope",
            ));
        }
        self.runtime()
            .with_history_producer_admission(async move |connection| {
                admit_producer_on(connection, context).await
            })
            .await
            .map_err(Into::into)
    }
    async fn settle_history_producer(
        &self,
        settlement: &HistoryProducerSettlement,
    ) -> Result<(), StorageError> {
        crate::runtime::ensure_no_storage_callback()?;
        self.runtime()
            .with_control_transaction(async |connection| {
                let context = settlement.context();
                let committed = sql_query(
                    "SELECT public.hubuum_history_producer_settle($1,$2,$3) AS committed",
                )
                .bind::<SqlUuid, _>(context.parent().id())
                .bind::<SqlUuid, _>(context.id())
                .bind::<SqlUuid, _>(context.ownership_token())
                .get_result::<CommittedRow>(connection)
                .await?
                .committed;
                if !committed {
                    return Err(PostgresStorageError::conflict(
                        "Producer completion is missing or mismatched",
                    ));
                }
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(Into::into)
    }
    async fn register_history_runtime(
        &self,
        isolation: Option<RuntimeIsolationDomain>,
    ) -> Result<RuntimeRegistration, StorageError> {
        let registration = RuntimeRegistration::generate_history_aware();
        self.runtime()
            .with_control_transaction(async move |connection| {
                let result = sql_query(
                    "SELECT public.hubuum_register_history_runtime($1,$2,$3) AS committed",
                )
                .bind::<SqlUuid, _>(registration.id())
                .bind::<SqlUuid, _>(registration.ownership_token())
                .bind::<Nullable<Jsonb>, _>(
                    isolation
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|_| {
                            PostgresStorageError::invalid_input("Cannot encode runtime isolation")
                        })?,
                )
                .get_result::<CommittedRow>(connection)
                .await?;
                if !result.committed {
                    return Err(PostgresStorageError::unavailable(
                        "Application quarantine or runtime capacity prevents history-aware startup",
                    ));
                }
                let authority =
                    sql_query("SELECT public.hubuum_deletion_control_read() AS material")
                        .get_result::<MaterialRow>(connection)
                        .await?;
                serde_json::from_value::<DeletionControlState>(authority.material).map_err(
                    |error| {
                        PostgresStorageError::invalid_persisted_value("deletion authority", error)
                    },
                )?;
                capture_on(connection).await?;
                Ok::<_, PostgresStorageError>(registration)
            })
            .await
            .map_err(Into::into)
    }
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        self.runtime()
            .with_connection(async move |connection| {
                let family = scan.key().family();
                let order = if family == StorageIdentityHistoryKind::ClassSchema {
                    "id"
                } else {
                    "history_id"
                };
                let query = format!("{} ORDER BY {order} LIMIT $1", history_scan_select(family));
                let rows = sql_query(query)
                    .bind::<Integer, _>(scan.limit() as i32)
                    .load_stream::<HistoryRow>(connection)
                    .await?;
                futures_util::pin_mut!(rows);
                let mut candidates = Vec::new();
                let mut scanned = 0;
                let mut unresolved = 0;
                let mut input_bytes = 0;
                let mut exhausted = false;
                while let Some(row) = rows.try_next().await? {
                    let Some(value) = row.logical_identity else {
                        scanned += 1;
                        unresolved += 1;
                        continue;
                    };
                    input_bytes += serde_json::to_vec(&value)
                        .map_err(|_| {
                            PostgresStorageError::database("Cannot size historical identity")
                        })?
                        .len();
                    if input_bytes > HistoryLifecycleScan::MAX_INPUT_BYTES {
                        exhausted = true;
                        break;
                    }
                    scanned += 1;
                    let evidence: StorageIdentityEvidence =
                        serde_json::from_value(value).map_err(|error| {
                            PostgresStorageError::invalid_persisted_value(
                                "historical identity",
                                error,
                            )
                        })?;
                    let anchor = row.anchor.map(|at| {
                        if family == StorageIdentityHistoryKind::ClassSchema {
                            HistoryRetentionAnchor::SchemaEvent(at)
                        } else if row.deleted {
                            HistoryRetentionAnchor::Deleted(at)
                        } else {
                            HistoryRetentionAnchor::Closed(at)
                        }
                    });
                    match HistoryExpiryCandidate::from_evidence(family, &evidence, anchor, false)? {
                        Some(candidate) if scan.includes(&candidate) => candidates
                            .push(candidate.with_native_interval(row.valid_from, row.valid_to)?),
                        Some(_) => {}
                        None => unresolved += 1,
                    }
                }
                HistoryLifecycleScanResult::new(
                    candidates,
                    scanned,
                    !exhausted && scanned < scan.limit(),
                    unresolved,
                )
                .map_err(PostgresStorageError::from)
            })
            .await
            .map_err(Into::into)
    }

    async fn recover_history_lifecycle(
        &self,
        recovery: HistoryLifecycleRecovery,
    ) -> Result<(), StorageError> {
        self.runtime()
            .with_control_transaction(async move |connection| {
                sql_query("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE")
                    .execute(connection)
                    .await?;
                let exposed =
                    sql_query("SELECT public.hubuum_deletion_exposure_allowed() AS committed")
                        .get_result::<CommittedRow>(connection)
                        .await?
                        .committed;
                if exposed {
                    return Err(PostgresStorageError::conflict(
                        "History authority recovery requires quarantine",
                    ));
                }
                let current = capture_on(connection).await?;
                if current.control() != recovery.expected() || current.fence() != recovery.fence() {
                    return Err(PostgresStorageError::conflict(
                        "History authority changed during recovery",
                    ));
                }
                let material = serde_json::to_value(recovery.recovered()).map_err(|_| {
                    PostgresStorageError::invalid_input("Cannot encode recovered history authority")
                })?;
                sql_query(
                    "UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1",
                )
                .bind::<Jsonb, _>(material)
                .execute(connection)
                .await?;
                sql_query("SELECT public.hubuum_history_expiry_sync()")
                    .execute(connection)
                    .await?;
                for event in recovery
                    .recovered()
                    .authority()
                    .audit_events_since(&HistoryLifecycleAuthority::empty())?
                {
                    crate::operations::event_record::append_retained_lifecycle_event(
                        connection, &event,
                    )
                    .await?;
                }
                Ok::<_, PostgresStorageError>(())
            })
            .await
            .map_err(Into::into)
    }
    async fn capture_history_lifecycle(&self) -> Result<HistoryLifecycleSnapshot, StorageError> {
        self.runtime()
            .with_control_connection(async |connection| capture_on(connection).await)
            .await
            .map_err(Into::into)
    }
    async fn commit_history_lifecycle(
        &self,
        change: HistoryLifecycleControlChange,
    ) -> Result<HistoryLifecycleSnapshot, StorageError> {
        let fallback = StorageExecutionPermit::new(
            None,
            ApplicationFence::new(
                change.fence().incarnation(),
                change.fence().deletion_revision().value() as i64,
            )
            .map_err(PostgresStorageError::from)?,
        );
        self.runtime()
            .with_history_control_transaction(fallback, async move |connection| {
                for scope in change.scopes_to_validate() {
                    if !change.expected().retains_scope(scope) {
                        let (current, history, identity) = match scope {
                            HistoryPolicyScope::Deployment => continue,
                            HistoryPolicyScope::Collection { identity } => ("collections", "collections_history", identity),
                            HistoryPolicyScope::Class { identity } => ("hubuumclass", "hubuumclass_history", identity),
                        };
                        let query = format!("SELECT EXISTS(SELECT 1 FROM public.{current} WHERE (logical_identity #>> '{{subject,identity,identity}}')::uuid=$1 UNION ALL SELECT 1 FROM public.{history} WHERE (logical_identity #>> '{{subject,identity,identity}}')::uuid=$1) AS committed");
                        let exists = sql_query(query).bind::<SqlUuid,_>(Uuid::from_bytes(*identity.as_bytes())).get_result::<CommittedRow>(connection).await?.committed;
                        if !exists { return Err(PostgresStorageError::not_found("History policy scope was not found")); }
                    }
                }
                let expected = serde_json::to_value(change.expected()).map_err(|_| {
                    PostgresStorageError::invalid_input(
                        "Cannot encode history lifecycle predecessor",
                    )
                })?;
                let replacement = serde_json::to_value(change.next()).map_err(|_| {
                    PostgresStorageError::invalid_input(
                        "Cannot encode history lifecycle transition",
                    )
                })?;
                let committed = sql_query(
                    "SELECT public.hubuum_history_lifecycle_commit($1,$2,$3,$4) AS committed",
                )
                .bind::<Jsonb, _>(expected)
                .bind::<Jsonb, _>(replacement)
                .bind::<SqlUuid, _>(change.fence().incarnation())
                .bind::<BigInt, _>(change.fence().deletion_revision().value() as i64)
                .get_result::<CommittedRow>(connection)
                .await?
                .committed;
                if !committed {
                    return Err(PostgresStorageError::conflict(
                        "History policy authority or application incarnation changed",
                    ));
                }
                for event in change.next().audit_events_since(change.expected())? {
                    crate::operations::event_record::append_retained_lifecycle_event(
                        connection, &event,
                    )
                    .await?;
                }
                capture_on(connection).await
            })
            .await
            .map_err(Into::into)
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod scan_tests {
    use diesel::sql_types::Text;
    use diesel_async::SimpleAsyncConnection;

    use super::*;
    use crate::{test_support::integration_test_pool, with_transaction};

    #[derive(QueryableByName)]
    struct Created {
        #[diesel(sql_type = Integer)]
        id: i32,
    }

    #[rstest::rstest]
    #[case::current_collision("current", true)]
    #[case::protected_open_collision("open", true)]
    #[case::clean_descendant("clean", false)]
    #[case::unknown_ancestry("unknown", true)]
    #[case::unretired_publication("publication", true)]
    #[case::missing_projection("missing_projection", false)]
    #[case::conflicting_projection("conflicting_projection", true)]
    #[tokio::test]
    async fn restored_descendant_exposure_gate_preserves_copies_and_quarantine(
        #[case] mode: &str,
        #[case] blocked: bool,
    ) {
        use crate::test_support::{
            database_role_tests_enabled, integration_test_database_roles,
            integration_test_migration_pool,
        };
        #[derive(QueryableByName, Debug, PartialEq)]
        struct Version {
            #[diesel(sql_type=Jsonb)]
            logical_identity: Value,
            #[diesel(sql_type=Timestamptz)]
            valid_from: DateTime<Utc>,
            #[diesel(sql_type=Nullable<Timestamptz>)]
            valid_to: Option<DateTime<Utc>>,
            #[diesel(sql_type=BigInt)]
            input_bytes: i64,
        }
        #[derive(QueryableByName)]
        struct Evidence {
            #[diesel(sql_type=Jsonb)]
            logical_identity: Value,
        }
        let derived = || {
            let mut bytes = *hubuum_domain::VersionIdentity::generate().as_bytes();
            bytes[6] = (bytes[6] & 0x0f) | 0x80;
            hubuum_domain::VersionIdentity::from_bytes(bytes).unwrap()
        };
        let pool = integration_test_migration_pool(1);
        with_transaction(&pool, async |connection| {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role',$1,true)")
                    .bind::<Text,_>(integration_test_database_roles().owner().as_str()).execute(connection).await?;
            }
            connection.batch_execute("SAVEPOINT history_exposure_fixture").await?;
            // Same authority-first order as real reapplication. The fixture is
            // rollback-only and neither publishes an intent nor runs erasure.
            connection.batch_execute("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
                SELECT id FROM hubuum_control.application_control WHERE id=1 FOR UPDATE;
                SELECT id FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR UPDATE").await?;
            // Other parallel archive fixtures can retain publication owners
            // between transactions. Hide those obligations only inside this
            // rollback-only fixture, after excluding new productive admission.
            // Otherwise the earlier undrained-owner gate masks the particular
            // restore condition this case is intended to exercise. These rows
            // are restored with history_exposure_fixture; no retirement or
            // cleanup authority is issued by this test.
            connection.batch_execute("SELECT id FROM hubuum_control.history_publication_owners ORDER BY id FOR UPDATE;
                DELETE FROM hubuum_control.history_archive_publication_claims;
                DELETE FROM hubuum_control.history_publication_producers;
                DELETE FROM hubuum_control.history_publication_owners").await?;
            let created = sql_query("INSERT INTO public.collections(name,description,parent_collection_id)
                SELECT $1,'exposure original',id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id")
                .bind::<Text,_>(format!("history_exposure_{}",Uuid::new_v4().simple())).get_result::<Created>(connection).await?;
            sql_query("UPDATE public.collections SET description='exposure current' WHERE id=$1")
                .bind::<Integer,_>(created.id).execute(connection).await?;
            let version_query = "SELECT logical_identity,valid_from,valid_to,octet_length(to_jsonb(h)::TEXT)::BIGINT AS input_bytes FROM public.collections_history h WHERE id=$1 AND op='I'";
            let closed = sql_query(version_query).bind::<Integer,_>(created.id).get_result::<Version>(connection).await?;
            let until = closed.valid_to.unwrap();
            let captured = capture_on(connection).await?;
            let empty = HistoryLifecycleSnapshot::new(HistoryLifecycleControl::empty(),captured.fence().incarnation(),captured.fence().deletion_revision(),closed.valid_from-chrono::Duration::seconds(1))?;
            let key = HistoryPolicyKey::new(StorageIdentityHistoryKind::Collection,HistoryPolicyScope::Deployment)?;
            let principal = hubuum_domain::PrincipalIdentity::generate();
            let policy = HistoryPolicyChange::new(key,HistoryPolicyRevision::new(0)?,HistoryPolicyBinding::Expire {
                trigger: HistoryExpiryTrigger::Age { age_seconds: HistoryRetentionSeconds::new(0)?,deleted_age_seconds:None },
                minimum_age_seconds:HistoryRetentionSeconds::new(0)?,
            },HistoryPolicyApplication::Prospective)?;
            let applied = empty.control().apply(HistoryPolicyApplyRequest::new(policy,None),principal,empty.fence(),empty.observed_at())
                .map_err(|error| PostgresStorageError::invalid_input(format!("Invalid exposure fixture policy: {error:?}")))?;
            let selected = HistoryLifecycleSnapshot::new(applied.next().clone(),empty.fence().incarnation(),empty.fence().deletion_revision(),captured.observed_at())?;
            let source: StorageIdentityEvidence = serde_json::from_value(closed.logical_identity.clone()).unwrap();
            let candidate = HistoryExpiryCandidate::from_evidence(StorageIdentityHistoryKind::Collection,&source,Some(HistoryRetentionAnchor::Closed(until)),false)?.unwrap()
                .with_native_interval(closed.valid_from,Some(until))?;
            let mut selection = HistoryExpirySelection::new(Uuid::new_v4(),HistoryExpiryOrigin::Manual {principal},HistoryLifecycleScan::new(key,1)?,&selected)?;
            assert!(matches!(selection.consider(candidate,HistoryExpiryInputBytes::new(closed.input_bytes as u64)?,&[])?,HistoryExpirySelectionOutcome::Selected));
            let retained = selected.control().admit_expiry(selection.finish()?,selected.fence())?;
            sql_query("UPDATE hubuum_control.history_lifecycle_control SET material=$1 WHERE id=1")
                .bind::<Jsonb,_>(serde_json::to_value(retained.next()).unwrap()).execute(connection).await?;
            sql_query("SELECT public.hubuum_history_expiry_sync()").execute(connection).await?;
            let retained_intent=retained.next().authority().expiry().intents().first().unwrap();
            if mode=="missing_projection" {
                sql_query("DELETE FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1")
                    .bind::<SqlUuid,_>(retained_intent.identity()).execute(connection).await?;
            }
            if mode=="conflicting_projection" {
                sql_query("UPDATE hubuum_control.history_expiry_members SET valid_to=valid_to+INTERVAL '1 microsecond' WHERE intent_uuid=$1")
                    .bind::<SqlUuid,_>(retained_intent.identity()).execute(connection).await?;
            }
            sql_query("SELECT public.hubuum_rotate_application_for_restore()").execute(connection).await?;
            let live = sql_query("SELECT logical_identity FROM public.collections WHERE id=$1")
                .bind::<Integer,_>(created.id).get_result::<Evidence>(connection).await?;
            let live: StorageIdentityEvidence = serde_json::from_value(live.logical_identity).unwrap();
            let clean = live.at_restore_baseline(derived())?;
            let collision = source.at_restore_baseline(derived())?;
            let current = match mode {
                "current"=>collision.clone(),
                "unknown"=>StorageIdentityEvidence::try_new(clean.subject(),Default::default())?,
                _=>clean.clone(),
            };
            let open = if mode=="open" {collision} else {clean};
            if mode=="publication" {
                // Uncertain pre-restore ownership stays an obligation even
                // without a live registry row. Absence is not quiescence.
                sql_query("INSERT INTO hubuum_control.history_publication_owners(id,ownership_token,runtime_id,incarnation,deletion_revision,history_revision,generation,kind) VALUES($1,$2,$3,$4,$5,$6,0,'backup')")
                    .bind::<SqlUuid,_>(Uuid::new_v4()).bind::<SqlUuid,_>(Uuid::new_v4()).bind::<SqlUuid,_>(Uuid::new_v4())
                    .bind::<SqlUuid,_>(captured.fence().incarnation()).bind::<BigInt,_>(captured.fence().deletion_revision().value() as i64)
                    .bind::<BigInt,_>(retained.next().authority().revision().value() as i64).execute(connection).await?;
            }
            // Use the real trusted restore identity branch; no source ancestry
            // or original event/closure clock is rewritten by the exposure gate.
            connection.batch_execute("SELECT set_config('hubuum.restore_history','on',true),set_config('hubuum.restore_identities','on',true)").await?;
            sql_query("UPDATE public.collections SET logical_identity=$2 WHERE id=$1")
                .bind::<Integer,_>(created.id).bind::<Jsonb,_>(serde_json::to_value(&current).unwrap()).execute(connection).await?;
            sql_query("UPDATE public.collections_history SET logical_identity=$2 WHERE id=$1 AND valid_to IS NULL")
                .bind::<Integer,_>(created.id).bind::<Jsonb,_>(serde_json::to_value(&open).unwrap()).execute(connection).await?;
            connection.batch_execute("SELECT set_config('hubuum.restore_history','off',true),set_config('hubuum.restore_identities','off',true); SAVEPOINT exposure_gate_probe").await?;
            let result = require_history_exposure_safe_on(connection).await;
            if mode=="missing_projection" && result.is_ok() {
                let member=retained_intent.manifest().members().first().unwrap();
                let restored=sql_query("SELECT EXISTS(SELECT 1 FROM hubuum_control.history_expiry_members WHERE intent_uuid=$1 AND family='collection' AND resource_uuid=$2 AND version_uuid=$3 AND valid_from=$4 AND valid_to=$5) AS committed")
                    .bind::<SqlUuid,_>(retained_intent.identity())
                    .bind::<SqlUuid,_>(Uuid::from_bytes(*member.target().resource().as_bytes()))
                    .bind::<SqlUuid,_>(Uuid::from_bytes(*member.target().version().as_bytes()))
                    .bind::<Timestamptz,_>(closed.valid_from).bind::<Timestamptz,_>(until)
                    .get_result::<CommittedRow>(connection).await?.committed;
                assert!(restored,"Missing projection must preserve original UUID membership and interval clocks");
            }
            // Restore local timeout settings even for a rejected fixture probe.
            connection.batch_execute("ROLLBACK TO SAVEPOINT exposure_gate_probe").await?;
            assert_eq!(result.is_err(),blocked);
            if let Err(error)=result {
                let expected=if mode=="conflicting_projection" {StorageErrorKind::Backend} else {StorageErrorKind::Unavailable};
                assert_eq!(error.kind(),expected);
            }
            assert!(!sql_query("SELECT public.hubuum_deletion_exposure_allowed() AS committed").get_result::<CommittedRow>(connection).await?.committed);
            assert_eq!(sql_query(version_query).bind::<Integer,_>(created.id).get_result::<Version>(connection).await?,closed);
            let unchanged = sql_query("SELECT logical_identity FROM public.collections WHERE id=$1")
                .bind::<Integer,_>(created.id).get_result::<Evidence>(connection).await?;
            assert_eq!(unchanged.logical_identity,serde_json::to_value(current).unwrap());
            assert_eq!(capture_on(connection).await?.control(),retained.next());
            connection.batch_execute("ROLLBACK TO SAVEPOINT history_exposure_fixture").await?;
            Ok::<_,PostgresStorageError>(())
        }).await.unwrap();
    }

    #[tokio::test]
    async fn history_scan_uses_original_native_tombstone_clock() {
        let pool = integration_test_pool(1);
        with_transaction(&pool, async |connection| {
            connection
                .batch_execute("SAVEPOINT lifecycle_scan_fixture")
                .await?;
            let created = sql_query(
                "INSERT INTO public.collections(name, description, parent_collection_id)
                 SELECT $1, 'lifecycle tombstone', id FROM public.collections
                 WHERE parent_collection_id IS NULL RETURNING id",
            )
            .bind::<Text, _>(format!("history_scan_{}", Uuid::new_v4().simple()))
            .get_result::<Created>(connection)
            .await?;
            sql_query("DELETE FROM public.collections WHERE id=$1")
                .bind::<Integer, _>(created.id)
                .execute(connection)
                .await?;
            let query = format!(
                "{} WHERE id=$1 AND op='D'",
                history_scan_select(StorageIdentityHistoryKind::Collection)
            );
            let tombstone = sql_query(query)
                .bind::<Integer, _>(created.id)
                .get_result::<HistoryRow>(connection)
                .await?;
            #[derive(QueryableByName)]
            struct Clock {
                #[diesel(sql_type = Timestamptz)]
                at: DateTime<Utc>,
            }
            let original = sql_query(
                "SELECT valid_from AS at FROM public.collections_history WHERE id=$1 AND op='D'",
            )
            .bind::<Integer, _>(created.id)
            .get_result::<Clock>(connection)
            .await?;
            assert!(
                tombstone.deleted,
                "Native D operations are deletion anchors"
            );
            assert_eq!(tombstone.anchor, Some(original.at));
            assert_eq!(tombstone.valid_from, original.at);
            connection
                .batch_execute("ROLLBACK TO SAVEPOINT lifecycle_scan_fixture")
                .await?;
            Ok::<_, PostgresStorageError>(())
        })
        .await
        .unwrap();
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;
