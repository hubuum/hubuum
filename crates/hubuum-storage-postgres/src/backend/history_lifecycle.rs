mod expiry;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use diesel::dsl::{case_when, exists, sum};
use diesel::sql_types::{
    BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
};
use diesel::{
    BoolExpressionMethods, ExpressionMethods, IntoSql, NullableExpressionMethods,
    PgAnyJsonExpressionMethods, QueryDsl, Queryable, QueryableByName, WindowExpressionMethods,
    sql_query,
};
use diesel_async::RunQueryDsl;
use hubuum_storage_core::*;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use super::PostgresStorage;
use crate::operations::history::resource_identity_path;
use crate::{PostgresConnection, PostgresStorageError};

#[diesel::declare_sql_function]
extern "SQL" {
    fn octet_length(value: Nullable<Text>) -> Nullable<Integer>;

    #[sql_name = "public.hubuum_history_expiry_dependency"]
    fn history_expiry_dependency(family: Text, identity: Nullable<Jsonb>) -> Bool;
}

// Keep table/column selection typed. Every family orders by its unique native
// history key; the ordered SUM therefore has an unambiguous current-row bound.
// PostgreSQL suppresses the over-budget suffix before transferring JSON to Rust.
macro_rules! history_scan_query {
    ($family:expr, $table:ident, $position:ident, $from:expr, $until:expr, $anchor:expr, $deleted:expr) => {{
        let within_budget = || {
            let bytes = case_when(
                $table::logical_identity.is_null(),
                0_i32.into_sql::<Integer>(),
            )
            .otherwise(
                octet_length($table::logical_identity.cast::<Nullable<Text>>()).assume_not_null(),
            );
            sum(bytes)
                .window_order($table::$position)
                .assume_not_null()
                .le(HistoryLifecycleScan::MAX_INPUT_BYTES as i64)
        };
        $table::table.order($table::$position).select((
            $from,
            $until,
            case_when(within_budget(), $table::logical_identity).otherwise(None::<Value>),
            $anchor,
            $deleted,
            within_budget(),
            case_when(
                within_budget(),
                history_expiry_dependency($family, $table::logical_identity),
            )
            .otherwise(true),
        ))
    }};
    ($family:expr, $table:ident) => {
        history_scan_query!(
            $family,
            $table,
            history_id,
            $table::valid_from,
            $table::valid_to,
            case_when($table::op.eq("D"), $table::valid_from.nullable())
                .otherwise($table::valid_to),
            $table::op.eq("D")
        )
    };
}

async fn history_scan_rows(
    connection: &mut PostgresConnection,
    scan: &HistoryLifecycleScan,
) -> Result<Vec<HistoryRow>, PostgresStorageError> {
    use crate::schema::{
        class_schema_history, collections_history, computed_field_history,
        export_templates_history, hubuumclass_history, hubuumclass_relation_history,
        hubuumobject_history, hubuumobject_relation_history, remote_targets_history,
    };
    let family = serde_json::to_value(scan.key().family()).map_err(|_| {
        PostgresStorageError::invalid_input("Cannot encode history inspection family")
    })?;
    let family = family
        .as_str()
        .ok_or_else(|| PostgresStorageError::invalid_input("Invalid history inspection family"))?;
    macro_rules! load {
        ($($query:tt)*) => {
            history_scan_query!(family, $($query)*).limit(i64::from(scan.limit()))
                .load::<HistoryRow>(connection).await?
        };
    }
    Ok(match scan.key().family() {
        StorageIdentityHistoryKind::Collection => load!(collections_history),
        StorageIdentityHistoryKind::Class => load!(hubuumclass_history),
        StorageIdentityHistoryKind::Object => load!(hubuumobject_history),
        StorageIdentityHistoryKind::ClassRelation => load!(hubuumclass_relation_history),
        StorageIdentityHistoryKind::ObjectRelation => load!(hubuumobject_relation_history),
        StorageIdentityHistoryKind::ExportTemplate => load!(export_templates_history),
        StorageIdentityHistoryKind::RemoteTarget => load!(remote_targets_history),
        StorageIdentityHistoryKind::ComputedDefinitions => load!(
            computed_field_history,
            history_id,
            computed_field_history::valid_from,
            computed_field_history::valid_to,
            computed_field_history::valid_to,
            false.into_sql::<Bool>()
        ),
        StorageIdentityHistoryKind::ClassSchema => load!(
            class_schema_history,
            id,
            class_schema_history::occurred_at,
            None::<DateTime<Utc>>.into_sql::<Nullable<Timestamptz>>(),
            class_schema_history::occurred_at.nullable(),
            false.into_sql::<Bool>()
        ),
    })
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
#[derive(Queryable)]
struct HistoryRow {
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    logical_identity: Option<Value>,
    anchor: Option<DateTime<Utc>>,
    deleted: bool,
    within_budget: bool,
    dependency: bool,
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
    use crate::schema::{
        class_schema_revisions, collections, collections_history, computed_field_history,
        export_templates, export_templates_history, hubuumclass, hubuumclass_history,
        hubuumclass_relation, hubuumclass_relation_history, hubuumobject, hubuumobject_history,
        hubuumobject_relation, hubuumobject_relation_history, remote_targets,
        remote_targets_history,
    };
    use StorageIdentityHistoryKind as Family;
    let mut budget = HistoryRecoveryExposureBudget::default();
    macro_rules! observe {
        ($family:ident, $table:ident, $order:expr, $filter:expr) => {
            if journal.intents().iter().any(|intent| {
                intent
                    .manifest()
                    .members()
                    .iter()
                    .any(|member| member.family() == Family::$family)
            }) {
                let bytes = || {
                    case_when(
                        $table::logical_identity.is_null(),
                        0_i32.into_sql::<Integer>(),
                    )
                    .otherwise(
                        octet_length($table::logical_identity.cast::<Nullable<Text>>())
                            .assume_not_null(),
                    )
                };
                let within_budget = sum(bytes())
                    .window_order($order)
                    .assume_not_null()
                    .le(budget.remaining_bytes() as i64);
                let rows = $table::table
                    .filter($filter)
                    .order($order)
                    .limit(i64::from(budget.remaining_rows()) + 1)
                    .select((
                        case_when(within_budget, $table::logical_identity).otherwise(None::<Value>),
                        bytes(),
                    ))
                    .load::<(Option<Value>, i32)>(connection)
                    .await?;
                observe_exposure_identities(journal, &mut budget, Family::$family, rows)?;
            }
        };
        ($family:ident, $current:ident, $history:ident) => {
            observe!($family, $current, $current::id, true.into_sql::<Bool>());
            observe!(
                $family,
                $history,
                $history::history_id,
                $history::valid_to.is_null().and($history::op.ne("D"))
            );
        };
    }
    // Complete current/open inventory for all nine families. Tombstones remain
    // closed deletion events. Each query transfers at most the remaining budget.
    observe!(Collection, collections, collections_history);
    observe!(Class, hubuumclass, hubuumclass_history);
    observe!(Object, hubuumobject, hubuumobject_history);
    observe!(
        ClassRelation,
        hubuumclass_relation,
        hubuumclass_relation_history
    );
    observe!(
        ObjectRelation,
        hubuumobject_relation,
        hubuumobject_relation_history
    );
    observe!(ExportTemplate, export_templates, export_templates_history);
    observe!(RemoteTarget, remote_targets, remote_targets_history);
    observe!(
        ComputedDefinitions,
        computed_field_history,
        computed_field_history::history_id,
        computed_field_history::valid_to.is_null()
    );
    observe!(
        ClassSchema,
        class_schema_revisions,
        (
            class_schema_revisions::class_id,
            class_schema_revisions::revision
        ),
        true.into_sql::<Bool>()
    );
    sql_query("SELECT set_config('statement_timeout',$1,true)")
        .bind::<Text, _>(previous)
        .execute(connection)
        .await?;
    Ok(control)
}

fn observe_exposure_identities(
    journal: &HistoryExpiryJournal,
    budget: &mut HistoryRecoveryExposureBudget,
    family: StorageIdentityHistoryKind,
    rows: Vec<(Option<Value>, i32)>,
) -> Result<(), PostgresStorageError> {
    for (identity, bytes) in rows {
        let bytes = usize::try_from(bytes).map_err(|_| {
            PostgresStorageError::unavailable("Invalid exposure identity length; keep quarantine")
        })?;
        budget
            .observe(bytes)
            .map_err(|error| PostgresStorageError::unavailable(error.to_string()))?;
        let evidence = identity
            .map(serde_json::from_value::<StorageIdentityEvidence>)
            .transpose()
            .map_err(|error| {
                PostgresStorageError::invalid_persisted_value("current exposure identity", error)
            })?;
        if journal.excludes_evidence(family, evidence.as_ref()) {
            return Err(PostgresStorageError::unavailable(
                "Current or protected-open copy carries expired or unresolved history ancestry; keep quarantine for explicit reconciliation",
            ));
        }
    }
    Ok(())
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
    async fn admit_history_expiry(
        &self,
        request: HistoryExpiryAdmissionRequest,
    ) -> Result<HistoryExpiryAdmissionResult, StorageError> {
        expiry::admit(self, request).await
    }
    async fn scan_history_lifecycle(
        &self,
        scan: HistoryLifecycleScan,
    ) -> Result<HistoryLifecycleScanResult, StorageError> {
        self.runtime()
            .with_connection(async move |connection| {
                let family = scan.key().family();
                let rows = history_scan_rows(connection, &scan).await?;
                let mut candidates = Vec::new();
                let mut scanned = 0;
                let mut unresolved = 0;
                let mut exhausted = false;
                for row in rows {
                    if !row.within_budget {
                        exhausted = true;
                        break;
                    }
                    let Some(value) = row.logical_identity else {
                        scanned += 1;
                        unresolved += 1;
                        continue;
                    };
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
                    match HistoryExpiryCandidate::from_evidence(
                        family,
                        &evidence,
                        anchor,
                        row.dependency,
                    )? {
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
                        macro_rules! known_scope {
                            ($current:ident, $history:ident, $identity:expr) => {{
                                use crate::schema::{$current, $history};
                                let identity = $identity.to_string();
                                let current = $current::table.filter(
                                    $current::logical_identity
                                        .retrieve_by_path_as_text(resource_identity_path())
                                        .eq(&identity),
                                );
                                let history = $history::table.filter(
                                    $history::logical_identity
                                        .retrieve_by_path_as_text(resource_identity_path())
                                        .eq(&identity),
                                );
                                diesel::select(exists(current).or(exists(history)))
                                    .get_result::<bool>(connection)
                                    .await?
                            }};
                        }
                        let exists = match scope {
                            HistoryPolicyScope::Deployment => continue,
                            HistoryPolicyScope::Collection { identity } => {
                                known_scope!(collections, collections_history, identity)
                            }
                            HistoryPolicyScope::Class { identity } => {
                                known_scope!(hubuumclass, hubuumclass_history, identity)
                            }
                        };
                        if !exists {
                            return Err(PostgresStorageError::not_found(
                                "History policy scope was not found",
                            ));
                        }
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
    #[case::retained(true)]
    #[case::released(false)]
    #[tokio::test]
    async fn inspection_preserves_native_schema_revision_dependency(#[case] retained: bool) {
        let pool = integration_test_pool(1);
        with_transaction(&pool, async |connection| {
            connection.batch_execute("SAVEPOINT schema_dependency_fixture").await?;
            let class = sql_query("INSERT INTO public.hubuumclass(name,description,collection_id) SELECT $1,'',id FROM public.collections WHERE parent_collection_id IS NULL RETURNING id")
                .bind::<Text, _>(format!("schema_dependency_{}", Uuid::new_v4().simple()))
                .get_result::<Created>(connection).await?;
            if !retained {
                sql_query("DELETE FROM public.hubuumclass WHERE id=$1").bind::<Integer, _>(class.id).execute(connection).await?;
            }
            use crate::schema::class_schema_history;
            let row = history_scan_query!(
                "class_schema", class_schema_history, id, class_schema_history::occurred_at,
                None::<DateTime<Utc>>.into_sql::<Nullable<Timestamptz>>(),
                class_schema_history::occurred_at.nullable(), false.into_sql::<Bool>()
            ).filter(class_schema_history::class_id.eq(class.id))
                .first::<HistoryRow>(connection).await?;
            let evidence: StorageIdentityEvidence = serde_json::from_value(row.logical_identity.unwrap()).unwrap();
            let candidate = HistoryExpiryCandidate::from_evidence(
                StorageIdentityHistoryKind::ClassSchema, &evidence,
                Some(HistoryRetentionAnchor::SchemaEvent(row.valid_from)), row.dependency,
            )?.unwrap();
            let decision = HistoryLifecycleAuthority::empty().inspect(&candidate, Utc::now(), &[])?;
            assert_eq!(decision.blocker(), Some(if retained {
                HistoryExpiryBlocker::RetainedDependency
            } else { HistoryExpiryBlocker::ExpiryDisabled }));
            connection.batch_execute("ROLLBACK TO SAVEPOINT schema_dependency_fixture").await?;
            Ok::<_, PostgresStorageError>(())
        }).await.unwrap();
    }

    #[rstest::rstest]
    #[case::single_oversized([1_048_576, 1, 1], 3, &[false, false, false])]
    #[case::cumulative_overflow([524_288, 524_288, 1], 3, &[true, false, false])]
    #[case::exact_byte_limit([1_048_574, 0, 0], 3, &[true, false, false])]
    #[case::missing_identity([-1, 1_048_576, 0], 3, &[true, false, false])]
    #[case::row_limit([1, 1, 1_048_576], 2, &[true, true])]
    #[tokio::test]
    async fn history_scan_suppresses_over_budget_identity_suffix(
        #[case] widths: [i32; 3],
        #[case] row_limit: i32,
        #[case] expected: &[bool],
    ) {
        // Exercise the actual typed projection against a connection-local
        // fixture; no persistent domain objects survive this transaction.
        diesel::table! {
            history_scan_fixture (history_id) {
                history_id -> BigInt,
                logical_identity -> Nullable<Jsonb>,
                valid_from -> Timestamptz,
                valid_to -> Nullable<Timestamptz>,
                op -> Text,
            }
        }
        let pool = integration_test_pool(1);
        let rows = with_transaction(&pool, async |connection| {
            connection
                .batch_execute(
                    "CREATE TEMPORARY TABLE history_scan_fixture (
                history_id BIGINT PRIMARY KEY, logical_identity JSONB,
                valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ, op TEXT NOT NULL
            ) ON COMMIT DROP",
                )
                .await?;
            for (index, width) in widths.into_iter().enumerate() {
                diesel::insert_into(history_scan_fixture::table)
                    .values((
                        history_scan_fixture::history_id.eq(index as i64 + 1),
                        history_scan_fixture::logical_identity
                            .eq((width >= 0).then(|| Value::String("x".repeat(width as usize)))),
                        history_scan_fixture::valid_from
                            .eq(DateTime::from_timestamp(0, 0).unwrap()),
                        history_scan_fixture::valid_to.eq(DateTime::from_timestamp(1, 0)),
                        history_scan_fixture::op.eq("U"),
                    ))
                    .execute(connection)
                    .await?;
            }
            history_scan_query!("collection", history_scan_fixture)
                .limit(i64::from(row_limit))
                .load::<HistoryRow>(connection)
                .await
                .map_err(PostgresStorageError::from)
        })
        .await
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.within_budget).collect::<Vec<_>>(),
            expected
        );
        for (row, width) in rows.iter().zip(widths) {
            assert_eq!(
                row.logical_identity.is_some(),
                row.within_budget && width >= 0
            );
        }
        let transferred: usize = rows
            .iter()
            .filter_map(|row| row.logical_identity.as_ref())
            .map(|identity| serde_json::to_vec(identity).unwrap().len())
            .sum();
        assert!(transferred <= HistoryLifecycleScan::MAX_INPUT_BYTES);
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
            IsolatedRestoreTarget, RESTORE_TEST_LOCK, database_role_tests_enabled,
            integration_test_database_roles, integration_test_database_url,
            integration_test_restore_migration_database_url,
        };
        use crate::{PostgresPoolSettings, build_postgres_pool};
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
        let _guard = RESTORE_TEST_LOCK.lock().await;
        let database_url = integration_test_restore_migration_database_url();
        IsolatedRestoreTarget::new(&database_url, &integration_test_database_url()).unwrap();
        let settings = PostgresPoolSettings::builder(database_url)
            .max_size(1)
            .statement_timeout_ms(0)
            .acquire_timeout_ms(5_000)
            .build()
            .unwrap();
        let pool = build_postgres_pool(&settings).unwrap();
        with_transaction(&pool, async |connection| {
            if database_role_tests_enabled() {
                sql_query("SELECT set_config('role',$1,true)")
                    .bind::<Text,_>(integration_test_database_roles().owner().as_str()).execute(connection).await?;
            }
            connection.batch_execute("SAVEPOINT history_exposure_fixture").await?;
            // Isolated rollback-only fixtures need an empty publication fleet.
            // Ordinary archive fixtures in the primary database may retain real
            // owners across transactions, so never wait for their global drain.
            connection.batch_execute("SELECT id FROM hubuum_control.deletion_authority WHERE id=1 FOR UPDATE;
                SELECT id FROM hubuum_control.application_control WHERE id=1 FOR UPDATE;
                SELECT id FROM hubuum_control.history_lifecycle_control WHERE id=1 FOR UPDATE").await?;
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
            use crate::schema::collections_history;
            let tombstone = history_scan_query!("collection", collections_history)
                .filter(collections_history::id.eq(created.id))
                .filter(collections_history::op.eq("D"))
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
            let evidence: StorageIdentityEvidence =
                serde_json::from_value(tombstone.logical_identity.unwrap()).unwrap();
            let candidate = HistoryExpiryCandidate::from_evidence(
                StorageIdentityHistoryKind::Collection,
                &evidence,
                Some(HistoryRetentionAnchor::Deleted(original.at)),
                tombstone.dependency,
            )?
            .unwrap()
            .with_native_interval(tombstone.valid_from, tombstone.valid_to)?;
            assert_eq!(candidate.read_span().unwrap().valid_to(), Some(original.at));
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
