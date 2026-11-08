//! Native selection and intent admission only. No payload removal occurs here.
use super::*;

#[derive(QueryableByName)]
struct SourceRow {
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
    dependency: bool,
    #[diesel(sql_type = Nullable<Jsonb>)]
    coordinates: Option<Value>,
}

pub(super) async fn admit(
    storage: &PostgresStorage,
    request: HistoryExpiryAdmissionRequest,
) -> Result<Option<HistoryExpiryManifest>, StorageError> {
    let scope = current_scope(storage)?;
    storage.runtime().with_history_control_transaction(scope, async move |connection| {
        // Authority UPDATE is held before API/task validation or native rows.
        // The locking fleet read also serializes first admission with startup.
        let allowed = sql_query("SELECT public.hubuum_history_producers_fenced($1,$2) AS committed")
            .bind::<SqlUuid,_>(request.fence().incarnation())
            .bind::<BigInt,_>(request.fence().deletion_revision().value() as i64)
            .get_result::<CommittedRow>(connection).await?.committed;
        if !allowed {
            return Err(PostgresStorageError::unavailable("A registered producer lacks online history fencing"));
        }
        let snapshot = capture_on(connection).await?;
        let mut selection = request.selection(&snapshot).map_err(|_| PostgresStorageError::conflict("Expiry authority changed or batch already admitted"))?;
        // Bound native work as well as transferred metadata. Never widen an
        // inherited statement timeout. The setting is transaction-local.
        sql_query("SELECT set_config('statement_timeout', CASE WHEN current_setting('statement_timeout')::INTERVAL=INTERVAL '0' THEN '5000' ELSE GREATEST(1,FLOOR(LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,5000)))::BIGINT::TEXT END,true)")
            .execute(connection).await?;
        let family = request.scan().key().family();
        for row in rows(connection, request.scan()).await? {
            if !row.within_budget { break; }
            let Some(raw) = row.logical_identity else { continue; };
            let evidence: StorageIdentityEvidence = serde_json::from_value(raw)
                .map_err(|error| PostgresStorageError::invalid_persisted_value("expiry source identity", error))?;
            if evidence.has_unknown_restore_source() { continue; }
            let anchor = match family {
                StorageIdentityHistoryKind::ClassSchema => HistoryRetentionAnchor::SchemaEvent(row.valid_from),
                _ if row.deleted => HistoryRetentionAnchor::Deleted(row.valid_from),
                _ => HistoryRetentionAnchor::Closed(row.valid_to.ok_or_else(|| PostgresStorageError::invalid_input("Expiry source must be closed"))?),
            };
            let Some(mut candidate) = HistoryExpiryCandidate::from_evidence(family, &evidence, Some(anchor), row.dependency)? else { continue; };
            if !request.scan().includes(&candidate) { continue; }
            candidate = candidate.with_native_interval(row.valid_from, row.valid_to)?;
            if family == StorageIdentityHistoryKind::Object {
                let Some(mut coordinates) = row.coordinates else { continue; };
                coordinates["target"] = serde_json::to_value(candidate.target()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry identity"))?;
                coordinates["attribution"] = serde_json::to_value(candidate.attributions().first()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry scope"))?;
                coordinates["read_span"] = serde_json::to_value(candidate.read_span()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry interval"))?;
                coordinates["kind"] = Value::String(if row.deleted { "tombstone" } else { "closed_value" }.into());
                coordinates["source_versions"] = serde_json::to_value(evidence.restore_source_versions().iter().collect::<Vec<_>>()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry ancestry"))?;
                let facts: HistoryObjectAuthorizationFacts = serde_json::from_value(coordinates).map_err(|error| PostgresStorageError::invalid_persisted_value("expiry object authorization",error))?;
                candidate = candidate.with_object_authorization(facts)?;
            }
            if matches!(selection.consider(candidate, HistoryExpiryInputBytes::new(row.input_bytes as u64)?, &[])?, HistoryExpirySelectionOutcome::BudgetReached) { break; }
        }
        if selection.is_empty() { return Ok(None); }
        let manifest = selection.finish()?;
        let change = snapshot.control().admit_expiry(manifest.clone(), snapshot.fence())?;
        let committed = sql_query("SELECT public.hubuum_history_expiry_admit($1,$2,$3,$4) AS committed")
            .bind::<Jsonb,_>(serde_json::to_value(change.expected()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry predecessor"))?)
            .bind::<Jsonb,_>(serde_json::to_value(change.next()).map_err(|_| PostgresStorageError::invalid_input("Cannot encode expiry intent"))?)
            .bind::<SqlUuid,_>(request.fence().incarnation())
            .bind::<BigInt,_>(request.fence().deletion_revision().value() as i64)
            .get_result::<CommittedRow>(connection).await?.committed;
        if !committed { return Err(PostgresStorageError::conflict("Native expiry admission changed")); }
        for event in change.next().audit_events_since(change.expected())? {
            crate::operations::event_record::append_retained_lifecycle_event(connection, &event).await?;
        }
        Ok::<_,PostgresStorageError>(Some(manifest))
    }).await.map_err(Into::into)
}

fn current_scope(storage: &PostgresStorage) -> Result<StorageExecutionPermit, StorageError> {
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

async fn rows(
    connection: &mut PostgresConnection,
    scan: &HistoryLifecycleScan,
) -> Result<Vec<SourceRow>, PostgresStorageError> {
    use StorageIdentityHistoryKind as F;
    // These identifiers and projections are a closed adapter-owned vocabulary;
    // all caller values remain parameters. No historical payload is transferred.
    let (table, position, from, until, closed, deleted, coordinates) = match scan.key().family() {
        F::Collection => (
            "collections_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::Class => (
            "hubuumclass_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::Object => (
            "hubuumobject_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "jsonb_build_object('object_id',id,'name',name,'collection_id',collection_id,'class_id',hubuum_class_id,'revision',revision)",
        ),
        F::ClassRelation => (
            "hubuumclass_relation_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::ObjectRelation => (
            "hubuumobject_relation_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::ExportTemplate => (
            "export_templates_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::RemoteTarget => (
            "remote_targets_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL OR op='D'",
            "op='D'",
            "NULL::jsonb",
        ),
        F::ComputedDefinitions => (
            "computed_field_history",
            "history_id",
            "valid_from",
            "valid_to",
            "valid_to IS NOT NULL",
            "FALSE",
            "NULL::jsonb",
        ),
        F::ClassSchema => (
            "class_schema_history",
            "id",
            "occurred_at",
            "NULL::timestamptz",
            "TRUE",
            "FALSE",
            "NULL::jsonb",
        ),
    };
    let (scope, identity) = match scan.key().scope() {
        HistoryPolicyScope::Deployment => ("deployment", None),
        HistoryPolicyScope::Collection { identity } => {
            ("collection", Some(Uuid::from_bytes(*identity.as_bytes())))
        }
        HistoryPolicyScope::Class { identity } => {
            ("class", Some(Uuid::from_bytes(*identity.as_bytes())))
        }
    };
    let query = format!(
        "WITH source AS MATERIALIZED (SELECT {position} AS position, {from} AS valid_from, {until} AS valid_to, logical_identity, {deleted} AS deleted, octet_length(to_jsonb(h.*)::text)::bigint AS input_bytes, {coordinates} AS coordinates FROM public.{table} h WHERE ({closed}) AND public.hubuum_history_version_visible($4,logical_identity) AND ($2='deployment' OR CASE WHEN $2='collection' AND $4='collection' THEN logical_identity #>> '{{subject,identity,identity}}'=$3::text ELSE logical_identity #>> ARRAY['references','/'||$2||'_id','reference','identity']=$3::text OR logical_identity #>> ARRAY['references','/lifecycle/from/'||$2,'reference','identity']=$3::text OR logical_identity #>> ARRAY['references','/lifecycle/to/'||$2,'reference','identity']=$3::text END) ORDER BY {position} LIMIT $1), bounded AS (SELECT *, sum(input_bytes) OVER (ORDER BY position)<=16777216 AND sum(octet_length(coalesce(logical_identity::text,''))+octet_length(coalesce(coordinates::text,''))) OVER (ORDER BY position)<=1048576 AS within_budget FROM source) SELECT valid_from,valid_to,CASE WHEN within_budget THEN logical_identity END AS logical_identity,deleted,input_bytes,within_budget,CASE WHEN within_budget THEN public.hubuum_history_expiry_dependency($4,logical_identity) ELSE TRUE END AS dependency,CASE WHEN within_budget THEN coordinates END AS coordinates FROM bounded ORDER BY position"
    );
    sql_query(query)
        .bind::<BigInt, _>(i64::from(scan.limit()))
        .bind::<Text, _>(scope)
        .bind::<Nullable<SqlUuid>, _>(identity)
        .bind::<Text, _>(
            serde_json::to_value(scan.key().family())
                .map_err(|_| PostgresStorageError::invalid_input("Invalid expiry family"))?
                .as_str()
                .ok_or_else(|| PostgresStorageError::invalid_input("Invalid expiry family"))?,
        )
        .load(connection)
        .await
        .map_err(Into::into)
}
