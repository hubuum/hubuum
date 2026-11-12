//! Bounded native admission and exact protected removal.
use super::*;
use diesel::OptionalExtension;

#[derive(QueryableByName)]
struct SourceRow {
    #[diesel(sql_type = BigInt)]
    position: i64,
    #[diesel(sql_type = Timestamptz)]
    valid_from: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    valid_to: Option<DateTime<Utc>>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    logical_identity: Option<Value>,
    #[diesel(sql_type = Bool)]
    deleted: bool,
    #[diesel(sql_type = BigInt)]
    input_bytes: i64,
    #[diesel(sql_type = Bool)]
    within_budget: bool,
    #[diesel(sql_type = Bool)]
    row_within_budget: bool,
    #[diesel(sql_type = Bool)]
    visible: bool,
    #[diesel(sql_type = Bool)]
    dependency: bool,
    #[diesel(sql_type = Nullable<Jsonb>)]
    coordinates: Option<Value>,
}

pub(super) async fn admit(
    storage: &PostgresStorage,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpiryAdmissionResult, StorageError> {
    let scope = current_scope(storage)?;
    storage
        .runtime()
        .with_history_control_transaction(scope, async move |connection| {
            admit_on(connection, request).await
        })
        .await
        .map_err(Into::into)
}

pub(crate) async fn admit_on(
    connection: &mut PostgresConnection,
    request: HistoryExpiryAdmissionRequest,
) -> Result<HistoryExpiryAdmissionResult, PostgresStorageError> {
    // Authority UPDATE is held before API/task validation or native rows.
    // The locking fleet read also serializes first admission with startup.
    let allowed = sql_query("SELECT public.hubuum_history_producers_fenced($1,$2) AS committed")
        .bind::<SqlUuid, _>(request.fence().incarnation())
        .bind::<BigInt, _>(request.fence().deletion_revision().value() as i64)
        .get_result::<CommittedRow>(connection)
        .await?
        .committed;
    if !allowed {
        return Err(PostgresStorageError::unavailable(
            "A registered producer lacks online history fencing",
        ));
    }
    let snapshot = capture_on(connection).await?;
    let mut selection = request.selection(&snapshot).map_err(|_| {
        PostgresStorageError::conflict("Expiry authority changed or batch already admitted")
    })?;
    // Bound native work as well as transferred metadata. Never widen an
    // inherited statement timeout. The setting is transaction-local.
    sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
        .execute(connection).await?;
    let family = request.scan().key().family();
    let (table, position) = source(family);
    let upper = sql_query(format!(
        "SELECT coalesce(max({position}),0)::bigint AS position FROM public.{table}"
    ))
    .get_result::<SourcePosition>(connection)
    .await?
    .position;
    let mut window = request.scan_window(u64::try_from(upper).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("native history scan boundary", error)
    })?)?;
    let rows = rows(connection, request.scan(), &window).await?;
    let source_exhausted = rows.len() < request.scan().limit() as usize;
    let mut stopped = false;
    for row in rows {
        window.observe(u64::try_from(row.position).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("native history scan position", error)
        })?)?;
        // Charge every returned source row, including suppressed metadata.
        if stopped {
            continue;
        }
        if !row.within_budget {
            // Defer a row that can fit a fresh page. An individually
            // oversized row cannot pin every later page to this prefix.
            if !row.row_within_budget {
                window.advance_oversized()?;
            }
            stopped = true;
            continue;
        }
        if !row.visible
            || (family != StorageIdentityHistoryKind::ClassSchema
                && row.valid_to.is_none()
                && !row.deleted)
        {
            window.advance();
            continue;
        }
        let Some(raw) = row.logical_identity else {
            window.advance();
            continue;
        };
        let evidence: StorageIdentityEvidence = serde_json::from_value(raw).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("expiry source identity", error)
        })?;
        if evidence.has_unknown_restore_source() {
            window.advance();
            continue;
        }
        let anchor = match family {
            StorageIdentityHistoryKind::ClassSchema => {
                HistoryRetentionAnchor::SchemaEvent(row.valid_from)
            }
            _ if row.deleted => HistoryRetentionAnchor::Deleted(row.valid_from),
            _ => HistoryRetentionAnchor::Closed(row.valid_to.ok_or_else(|| {
                PostgresStorageError::invalid_input("Expiry source must be closed")
            })?),
        };
        let Some(mut candidate) =
            HistoryExpiryCandidate::from_evidence(family, &evidence, Some(anchor), row.dependency)?
        else {
            window.advance();
            continue;
        };
        if !request.scan().includes(&candidate) {
            window.advance();
            continue;
        }
        candidate = candidate.with_native_interval(row.valid_from, row.valid_to)?;
        if family == StorageIdentityHistoryKind::Object {
            let Some(mut coordinates) = row
                .coordinates
                .filter(|_| !candidate.attributions().is_empty())
            else {
                window.advance();
                continue;
            };
            coordinates["target"] = serde_json::to_value(candidate.target()).map_err(|_| {
                PostgresStorageError::invalid_input("Cannot encode expiry identity")
            })?;
            coordinates["attribution"] = serde_json::to_value(candidate.attributions().first())
                .map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry scope"))?;
            coordinates["read_span"] =
                serde_json::to_value(candidate.read_span()).map_err(|_| {
                    PostgresStorageError::invalid_input("Cannot encode expiry interval")
                })?;
            coordinates["kind"] = Value::String(
                if row.deleted {
                    "tombstone"
                } else {
                    "closed_value"
                }
                .into(),
            );
            coordinates["source_versions"] = serde_json::to_value(
                evidence
                    .restore_source_versions()
                    .iter()
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry ancestry"))?;
            let facts: HistoryObjectAuthorizationFacts = serde_json::from_value(coordinates)
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value(
                        "expiry object authorization",
                        error,
                    )
                })?;
            candidate = candidate.with_object_authorization(facts)?;
        }
        if matches!(
            selection.consider(
                candidate,
                HistoryExpiryInputBytes::new(row.input_bytes as u64)?,
                &[]
            )?,
            HistoryExpirySelectionOutcome::BudgetReached
        ) {
            stopped = true;
        } else {
            window.advance();
        }
    }
    let exhausted = source_exhausted && !stopped;
    if selection.is_empty() {
        return window.finish(None, exhausted).map_err(Into::into);
    }
    let manifest = selection.finish()?;
    let change = snapshot
        .control()
        .admit_expiry(manifest.clone(), snapshot.fence())?;
    let result = window.finish(Some(manifest), exhausted)?;
    let committed =
        sql_query("SELECT public.hubuum_history_expiry_admit($1,$2,$3,$4) AS committed")
            .bind::<Jsonb, _>(serde_json::to_value(change.expected()).map_err(|_| {
                PostgresStorageError::invalid_input("Cannot encode expiry predecessor")
            })?)
            .bind::<Jsonb, _>(
                serde_json::to_value(change.next()).map_err(|_| {
                    PostgresStorageError::invalid_input("Cannot encode expiry intent")
                })?,
            )
            .bind::<SqlUuid, _>(request.fence().incarnation())
            .bind::<BigInt, _>(request.fence().deletion_revision().value() as i64)
            .get_result::<CommittedRow>(connection)
            .await?
            .committed;
    if !committed {
        return Err(PostgresStorageError::conflict(
            "Native expiry admission changed",
        ));
    }
    for event in change.next().audit_events_since(change.expected())? {
        crate::operations::event_record::append_retained_lifecycle_event(connection, &event)
            .await?;
    }
    Ok::<_, PostgresStorageError>(result)
}

pub(crate) async fn remove_on(
    connection: &mut PostgresConnection,
    checkpoint: &HistoryExpiryTaskCheckpoint,
    protection: &HistoryExposureProtection,
) -> Result<(HistoryExpiryTaskCheckpoint, u32), PostgresStorageError> {
    sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
        .execute(connection).await?;
    let snapshot = capture_on(connection).await?;
    let intent = checkpoint.protected_intent(&snapshot, protection)?;
    if intent.is_complete() {
        return checkpoint
            .complete_batch(&snapshot, protection)
            .map(|next| (next, intent.removals().len() as u32))
            .map_err(Into::into);
    }
    let member = intent
        .manifest()
        .members()
        .iter()
        .find(|member| {
            !intent
                .removals()
                .iter()
                .any(|receipt| receipt.target() == member.target())
        })
        .expect("incomplete intent has pending membership");
    remove_member_on(
        connection,
        member,
        checkpoint.batch(),
        &snapshot,
        protection,
        false,
    )
    .await?;
    Ok((checkpoint.clone(), intent.removals().len() as u32 + 1))
}

pub(super) async fn reapply_on(
    connection: &mut PostgresConnection,
    request: &HistoryExpiryReapplication,
    snapshot: &HistoryLifecycleSnapshot,
) -> Result<bool, PostgresStorageError> {
    remove_member_on(
        connection,
        request.member(),
        request.batch(),
        snapshot,
        request.protection(),
        request.has_receipt(),
    )
    .await
}

async fn remove_member_on(
    connection: &mut PostgresConnection,
    member: &HistoryExpiryMember,
    batch: Uuid,
    snapshot: &HistoryLifecycleSnapshot,
    protection: &HistoryExposureProtection,
    may_be_absent: bool,
) -> Result<bool, PostgresStorageError> {
    sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
        .execute(connection).await?;
    let family = member.family();
    let (table, _) = source(family);
    let (from, until, deleted) = if family == StorageIdentityHistoryKind::ClassSchema {
        ("occurred_at", "NULL::timestamptz", "FALSE")
    } else if family == StorageIdentityHistoryKind::ComputedDefinitions {
        ("valid_from", "valid_to", "FALSE")
    } else {
        ("valid_from", "valid_to", "op='D'")
    };
    #[derive(QueryableByName)]
    struct NativeCopy {
        #[diesel(sql_type = Timestamptz)]
        valid_from: DateTime<Utc>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        valid_to: Option<DateTime<Utc>>,
        #[diesel(sql_type = Nullable<Jsonb>)]
        logical_identity: Option<Value>,
        #[diesel(sql_type = Bool)]
        within_budget: bool,
        #[diesel(sql_type = Bool)]
        deleted: bool,
        #[diesel(sql_type = Bool)]
        dependency: bool,
    }
    // Runtime history access stays read-only. The authority UPDATE excludes
    // native writers; the SECURITY DEFINER removal function acquires the exact
    // row lock and rechecks its interval before deleting in this transaction.
    let row = sql_query(format!(
        "SELECT {from} AS valid_from,{until} AS valid_to,CASE WHEN octet_length(logical_identity::TEXT)<=1048576 THEN logical_identity END AS logical_identity,octet_length(to_jsonb(h.*)::TEXT)<=16777216 AS within_budget,{deleted} AS deleted,public.hubuum_history_expiry_dependency($3,logical_identity) AS dependency FROM public.{table} h WHERE logical_identity #>> '{{subject,identity,identity}}'=$1 AND logical_identity #>> '{{subject,version}}'=$2"
    ))
        .bind::<Text, _>(member.target().resource().to_string())
        .bind::<Text, _>(member.target().version().to_string())
        .bind::<Text, _>(serde_json::to_value(family).map_err(|error| PostgresStorageError::invalid_persisted_value("expiry family", error))?.as_str().expect("family string"))
        .get_result::<NativeCopy>(connection).await.optional()?;
    let Some(row) = row else {
        if may_be_absent {
            return Ok(false);
        }
        return Err(PostgresStorageError::conflict(
            "Absent history copy has no verified first receipt",
        ));
    };
    if !row.within_budget || row.logical_identity.is_none() {
        return Err(PostgresStorageError::conflict(
            "History removal exceeds the bounded input budget",
        ));
    }
    let evidence: StorageIdentityEvidence =
        serde_json::from_value(row.logical_identity.expect("bounded identity")).map_err(
            |error| PostgresStorageError::invalid_persisted_value("native removal identity", error),
        )?;
    let anchor =
        if family == StorageIdentityHistoryKind::ClassSchema {
            HistoryRetentionAnchor::SchemaEvent(row.valid_from)
        } else if row.deleted {
            HistoryRetentionAnchor::Deleted(row.valid_from)
        } else {
            HistoryRetentionAnchor::Closed(row.valid_to.ok_or_else(|| {
                PostgresStorageError::conflict("History copy is no longer closed")
            })?)
        };
    let candidate =
        HistoryExpiryCandidate::from_evidence(family, &evidence, Some(anchor), row.dependency)?
            .ok_or_else(|| {
                PostgresStorageError::conflict("History removal identity is unresolved")
            })?
            .with_native_interval(row.valid_from, row.valid_to)?;
    member.validate_removal(
        &candidate,
        snapshot.control().authority(),
        snapshot.observed_at(),
    )?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Outcome {
        affected_rows: u64,
        removed_at: DateTime<Utc>,
        control: HistoryLifecycleControl,
    }
    let material =
        sql_query("SELECT public.hubuum_history_expiry_remove($1,$2,$3,$4,$5,$6,$7) AS material")
            .bind::<Jsonb, _>(serde_json::to_value(snapshot.control()).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("removal predecessor", error)
            })?)
            .bind::<SqlUuid, _>(batch)
            .bind::<SqlUuid, _>(Uuid::from_bytes(*member.target().resource().as_bytes()))
            .bind::<SqlUuid, _>(Uuid::from_bytes(*member.target().version().as_bytes()))
            .bind::<Text, _>(protection.checkpoint().as_str())
            .bind::<SqlUuid, _>(snapshot.fence().incarnation())
            .bind::<BigInt, _>(snapshot.fence().deletion_revision().value() as i64)
            .get_result::<MaterialRow>(connection)
            .await?
            .material;
    let outcome: Outcome = serde_json::from_value(material).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("native removal outcome", error)
    })?;
    let deletion = HistoryExpiryExactDeletion::from_affected_rows(
        member.target(),
        protection.checkpoint().clone(),
        outcome.removed_at,
        outcome.affected_rows,
    )?;
    let change = snapshot
        .control()
        .record_expiry_removal(batch, deletion, snapshot.fence())?;
    if change.next() != &outcome.control {
        return Err(PostgresStorageError::conflict(
            "Native removal receipt differs from exact outcome",
        ));
    }
    for event in change.next().audit_events_since(change.expected())? {
        crate::operations::event_record::append_retained_lifecycle_event(connection, &event)
            .await?;
    }
    Ok(true)
}

pub(crate) async fn require_reapplied_on(
    connection: &mut PostgresConnection,
    control: &HistoryLifecycleControl,
) -> Result<(), PostgresStorageError> {
    if control
        .authority()
        .expiry()
        .intents()
        .iter()
        .any(|intent| !intent.is_complete())
    {
        return Err(PostgresStorageError::unavailable(
            "History expiry lacks a verified first receipt; keep quarantine",
        ));
    }
    sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
        .execute(connection).await?;
    let families: std::collections::BTreeSet<_> = control
        .authority()
        .expiry()
        .intents()
        .iter()
        .flat_map(|intent| {
            intent
                .manifest()
                .members()
                .iter()
                .map(HistoryExpiryMember::family)
        })
        .collect();
    for family in families {
        let (table, _) = source(family);
        let family_name = serde_json::to_value(family).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("expiry family", error)
        })?;
        if sql_query(format!("SELECT EXISTS(SELECT 1 FROM public.{table} h WHERE NOT public.hubuum_history_version_visible($1,h.logical_identity)) AS committed"))
            .bind::<Text,_>(family_name.as_str().expect("family string"))
            .get_result::<CommittedRow>(connection).await?.committed {
            return Err(PostgresStorageError::unavailable("Restored excluded history remains; keep quarantine"));
        }
    }
    Ok(())
}

pub(super) fn current_scope(
    storage: &PostgresStorage,
) -> Result<StorageExecutionPermit, StorageError> {
    let scope = storage.current_execution_scope();
    if scope.storage_callback_active() || scope.history_read_override().is_some() {
        return Err(StorageError::unavailable(
            "Expiry selection requires an independent control scope",
        ));
    }
    scope
        .application_access_override()
        .filter(|permit| {
            permit
                .owner()
                .is_some_and(|owner| owner.history_protocol().supports_online())
        })
        .cloned()
        .ok_or_else(|| {
            StorageError::unavailable(
                "Expiry selection requires original history-aware application admission",
            )
        })
}

#[derive(QueryableByName)]
struct SourcePosition {
    #[diesel(sql_type = BigInt)]
    position: i64,
}

fn source(family: StorageIdentityHistoryKind) -> (&'static str, &'static str) {
    use StorageIdentityHistoryKind as F;
    let table = match family {
        F::Collection => "collections_history",
        F::Class => "hubuumclass_history",
        F::Object => "hubuumobject_history",
        F::ClassRelation => "hubuumclass_relation_history",
        F::ObjectRelation => "hubuumobject_relation_history",
        F::ExportTemplate => "export_templates_history",
        F::RemoteTarget => "remote_targets_history",
        F::ComputedDefinitions => "computed_field_history",
        F::ClassSchema => "class_schema_history",
    };
    (
        table,
        if family == F::ClassSchema {
            "id"
        } else {
            "history_id"
        },
    )
}

async fn rows(
    connection: &mut PostgresConnection,
    scan: &HistoryLifecycleScan,
    window: &HistoryExpiryScanWindow<'_>,
) -> Result<Vec<SourceRow>, PostgresStorageError> {
    use StorageIdentityHistoryKind as F;
    // Closed adapter-owned identifiers; every caller value stays a parameter.
    // Bound source rows before closure, scope, identity or visibility filtering.
    let family = scan.key().family();
    let (table, position) = source(family);
    let (from, until) = if family == F::ClassSchema {
        ("occurred_at", "NULL::timestamptz")
    } else {
        ("valid_from", "valid_to")
    };
    let deleted = if matches!(family, F::ClassSchema | F::ComputedDefinitions) {
        "FALSE"
    } else {
        "op='D'"
    };
    let coordinates = if family == F::Object {
        "jsonb_build_object('object_id',id,'name',name,'collection_id',collection_id,'class_id',hubuum_class_id,'revision',revision)"
    } else {
        "NULL::jsonb"
    };
    let row_after = |after: &str| {
        format!(
            "SELECT {position}::bigint AS position, {from} AS valid_from, {until} AS valid_to, logical_identity, {deleted} AS deleted, octet_length(to_jsonb(h.*)::text)::bigint AS input_bytes, {coordinates} AS coordinates FROM public.{table} h WHERE {position}>{after} AND {position}<=$3 ORDER BY {position} LIMIT 1"
        )
    };
    let measure = |query: String| {
        format!(
            "SELECT native.*, octet_length(coalesce(logical_identity::text,''))::bigint+octet_length(coalesce(coordinates::text,''))::bigint AS metadata_bytes FROM ({query}) native"
        )
    };
    let first = measure(row_after("$2"));
    let next = measure(row_after("previous.position"));
    // Measure one row beyond the byte prefix to decide whether to defer or
    // skip it. Do not encode an entire row-count window after an oversized row.
    let query = format!(
        "WITH RECURSIVE bounded AS (SELECT native.*,1::bigint AS scanned,input_bytes AS total_input,metadata_bytes AS total_metadata FROM ({first}) native UNION ALL SELECT native.*,previous.scanned+1,previous.total_input+native.input_bytes,previous.total_metadata+native.metadata_bytes FROM bounded previous CROSS JOIN LATERAL ({next}) native WHERE previous.scanned<$1 AND previous.total_input<=16777216 AND previous.total_metadata<=1048576), measured AS (SELECT *, total_input<=16777216 AND total_metadata<=1048576 AS within_budget,input_bytes<=16777216 AND metadata_bytes<=1048576 AS row_within_budget FROM bounded) SELECT position,valid_from,valid_to,CASE WHEN within_budget THEN logical_identity END AS logical_identity,deleted,input_bytes,within_budget,row_within_budget,CASE WHEN within_budget THEN public.hubuum_history_version_visible($4,logical_identity) ELSE FALSE END AS visible,CASE WHEN within_budget THEN public.hubuum_history_expiry_dependency($4,logical_identity) ELSE TRUE END AS dependency,CASE WHEN within_budget THEN coordinates END AS coordinates FROM measured ORDER BY position"
    );
    sql_query(query)
        .bind::<BigInt, _>(i64::from(scan.limit()))
        .bind::<BigInt, _>(window.after() as i64)
        .bind::<BigInt, _>(window.through() as i64)
        .bind::<Text, _>(
            serde_json::to_value(family)
                .map_err(|_| PostgresStorageError::invalid_input("Invalid expiry family"))?
                .as_str()
                .ok_or_else(|| PostgresStorageError::invalid_input("Invalid expiry family"))?,
        )
        .load(connection)
        .await
        .map_err(Into::into)
}
