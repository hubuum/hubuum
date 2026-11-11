//! Native selection and intent admission only. No payload removal occurs here.
use super::*;

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
                window.advance();
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
