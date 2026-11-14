use super::*;
use futures_util::TryStreamExt;

pub(super) async fn plan_on(
    connection: &mut PostgresConnection,
    request: HistoryExpiryAdmissionRequest,
    snapshot: HistoryLifecycleSnapshot,
) -> Result<HistoryExpirySizePlan, PostgresStorageError> {
    request.selection(&snapshot)?;
    let family = request.scan().key().family();
    let scan = request.scan().clone();
    let mut census = HistorySizeCensus::for_family(snapshot, family)?;
    super::super::accounting::fold_on(connection, family, &mut census).await?;
    let mut selection = HistoryExpirySizeSelection::new(request, census.finish_family()?)?;
    sql_query("SELECT set_config('statement_timeout',GREATEST(1,LEAST(EXTRACT(EPOCH FROM current_setting('statement_timeout')::INTERVAL)*1000,$1))::bigint::text,true)")
        .bind::<BigInt, _>(selection.remaining_time()?.as_millis().max(1) as i64).execute(connection).await?;
    let (table, position) = source(family);
    let (from, until) = if family == StorageIdentityHistoryKind::ClassSchema {
        ("occurred_at", "NULL::timestamptz")
    } else {
        ("valid_from", "valid_to")
    };
    let deleted = if matches!(
        family,
        StorageIdentityHistoryKind::ClassSchema | StorageIdentityHistoryKind::ComputedDefinitions
    ) {
        "FALSE"
    } else {
        "op='D'"
    };
    let coordinates = if family == StorageIdentityHistoryKind::Object {
        "jsonb_build_object('object_id',id,'name',name,'collection_id',collection_id,'class_id',hubuum_class_id,'revision',revision)"
    } else {
        "NULL::jsonb"
    };
    let query = format!(
        "WITH native AS (SELECT {position}::bigint AS position,{from} AS valid_from,{until} AS valid_to,logical_identity,{deleted} AS deleted,octet_length(to_jsonb(h.*)::text)::bigint AS input_bytes,{coordinates} AS coordinates FROM public.{table} h ORDER BY {position} LIMIT 100001) SELECT position,valid_from,valid_to,CASE WHEN octet_length(logical_identity::text)<=1048576 THEN logical_identity END AS logical_identity,deleted,input_bytes,TRUE AS within_budget,TRUE AS row_within_budget,public.hubuum_history_version_visible($1,logical_identity) AS visible,public.hubuum_history_expiry_dependency($1,logical_identity) AS dependency,CASE WHEN octet_length(coordinates::text)<=1048576 THEN coordinates END AS coordinates,(coalesce(octet_length(logical_identity::text),0)::bigint+coalesce(octet_length(coordinates::text),0)::bigint) AS metadata_bytes FROM native ORDER BY position"
    );
    let family_name = serde_json::to_value(family)
        .map_err(|_| PostgresStorageError::invalid_input("Invalid size selection family"))?;
    {
        let mut rows = sql_query(query)
            .bind::<Text, _>(family_name.as_str().expect("family string"))
            .load_stream::<SourceRow>(connection)
            .await?;
        while let Some(mut row) = rows.try_next().await? {
            let candidate = decode_candidate(family, &mut row, &scan)?.ok_or_else(|| {
                PostgresStorageError::unavailable("Size selection has unresolved native identity")
            })?;
            let visible = row.visible
                && !(family == StorageIdentityHistoryKind::Object
                    && candidate.anchor().is_some()
                    && candidate.object_authorization().is_none()
                    && row.input_bytes <= i64::from(HistoryExpiryInputBytes::MAX_BATCH_BYTES));
            selection.observe(
                candidate,
                u64::try_from(row.input_bytes)
                    .map_err(|_| PostgresStorageError::unavailable("Invalid native size input"))?,
                u64::try_from(row.metadata_bytes).map_err(|_| {
                    PostgresStorageError::unavailable("Invalid native size metadata")
                })?,
                visible,
            )?;
        }
    }
    selection.finish().map_err(Into::into)
}
