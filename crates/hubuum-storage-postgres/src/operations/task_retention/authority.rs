//! Administrative replacement and consistent capture of protected task state.

use super::*;

#[derive(Deserialize)]
struct Capture {
    task_control: TaskControlState,
    deletion: DeletionControlState,
    application: Value,
    system_identifier: String,
    wal_file: String,
    lsn: String,
    task_reapplication: Option<TaskControlReapplication>,
}

pub(crate) async fn state_on(
    connection: &mut PostgresConnection,
) -> Result<TaskControlState, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_control_read() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    decode(row.material)
}

pub(crate) async fn capture_on(
    connection: &mut PostgresConnection,
) -> Result<ControlCheckpointCapture, PostgresStorageError> {
    let row = diesel::sql_query("SELECT public.hubuum_task_control_capture() AS material")
        .get_result::<MaterialRow>(connection)
        .await?;
    let capture: Capture = decode(row.material)?;
    let application = crate::backend::deletion_control::decode_application(capture.application)?;
    let mut deletion = DeletionControlCapture::new(
        capture.deletion,
        DeletionRecoveryPosition::postgresql(
            capture.system_identifier,
            capture.wal_file,
            capture.lsn,
        )?,
    );
    if let Some(receipt) = application
        .reapplication()
        .filter(|receipt| receipt.fence() == application.fence())
    {
        deletion = deletion.with_reapplication(receipt.clone())?;
    }
    let mut combined = ControlCheckpointCapture::new(deletion, capture.task_control)?;
    if let Some(receipt) = capture.task_reapplication {
        combined = combined.with_task_reapplication(receipt)?;
    }
    Ok(combined)
}

pub(crate) async fn lock_expected_on(
    connection: &mut PostgresConnection,
    expected: &TaskControlState,
) -> Result<(), PostgresStorageError> {
    lock_on(connection).await?;
    if state_on(connection).await? != *expected {
        return Err(PostgresStorageError::conflict(
            "Task retention authority changed; recapture before retrying",
        ));
    }
    Ok(())
}

/// Privileged administration/recovery only. No runtime SECURITY DEFINER wrapper
/// permits wholesale replacement of protected receipt or deletion material.
pub(crate) async fn store_state_on(
    connection: &mut PostgresConnection,
    state: &TaskControlState,
) -> Result<(), PostgresStorageError> {
    state.validate()?;
    let value = serde_json::to_value(state)
        .map_err(|_| PostgresStorageError::internal("Cannot encode task control"))?;
    let receipt_bytes = state.receipts().iter().try_fold(0usize, |sum, receipt| {
        receipt.encoded_bytes().map(|bytes| sum + bytes)
    })?;
    let retention_bytes=canonical(&serde_json::json!({"policies":state.policies(),"holds":state.holds(),"purge_batches":state.purge_batches()}))?.len();
    diesel::sql_query("DELETE FROM hubuum_control.task_purge_targets")
        .execute(connection)
        .await?;
    diesel::sql_query("DELETE FROM hubuum_control.task_receipts")
        .execute(connection)
        .await?;
    diesel::sql_query("DELETE FROM hubuum_control.task_retention_policies")
        .execute(connection)
        .await?;
    diesel::sql_query("DELETE FROM hubuum_control.task_retention_holds")
        .execute(connection)
        .await?;
    diesel::sql_query("DELETE FROM hubuum_control.task_purge_batches")
        .execute(connection)
        .await?;
    // Canonical strings supply exact encoded bytes without JSONB whitespace.
    let records = state
        .receipts()
        .iter()
        .map(canonical)
        .collect::<Result<Vec<_>, _>>()?;
    diesel::sql_query("INSERT INTO hubuum_control.task_receipts(task_identity,principal_identity,protected_key,generation,material,encoded_bytes,expires_at) SELECT (r #>> '{ticket,receipt,task}')::uuid,(r #>> '{ticket,receipt,submission,principal}')::uuid,r #>> '{ticket,receipt,submission,protected_key}',(r #>> '{ticket,receipt,generation}')::uuid,r,octet_length(encoded),CASE WHEN r #>> '{outcome,state}'='may_execute' THEN NULL ELSE COALESCE(r #>> '{outcome,finished_at}',r #>> '{outcome,settled_at}')::timestamptz+make_interval(secs=>(r->>'window_secs')::double precision) END FROM unnest($1::text[]) encoded CROSS JOIN LATERAL (SELECT encoded::jsonb r) record")
        .bind::<diesel::sql_types::Array<Text>,_>(records).execute(connection).await?;
    let policies = state
        .policies()
        .values()
        .map(canonical)
        .collect::<Result<Vec<_>, _>>()?;
    diesel::sql_query("INSERT INTO hubuum_control.task_retention_policies(revision,material,encoded_bytes) SELECT (encoded::jsonb->>'revision')::bigint,encoded::jsonb,octet_length(encoded) FROM unnest($1::text[]) encoded")
        .bind::<diesel::sql_types::Array<Text>,_>(policies).execute(connection).await?;
    let holds = state
        .holds()
        .values()
        .map(canonical)
        .collect::<Result<Vec<_>, _>>()?;
    diesel::sql_query("INSERT INTO hubuum_control.task_retention_holds(id,task_identity,material,encoded_bytes) SELECT (encoded::jsonb->>'id')::uuid,(encoded::jsonb->>'task')::uuid,encoded::jsonb,octet_length(encoded) FROM unnest($1::text[]) encoded")
        .bind::<diesel::sql_types::Array<Text>,_>(holds).execute(connection).await?;
    let batches = state
        .purge_batches()
        .values()
        .map(canonical)
        .collect::<Result<Vec<_>, _>>()?;
    diesel::sql_query("INSERT INTO hubuum_control.task_purge_batches(id,material,encoded_bytes) SELECT (encoded::jsonb #>> '{ticket,manifest,id}')::uuid,encoded::jsonb,octet_length(encoded) FROM unnest($1::text[]) encoded")
        .bind::<diesel::sql_types::Array<Text>,_>(batches).execute(connection).await?;
    diesel::sql_query("INSERT INTO hubuum_control.task_purge_targets(task_identity,scope,batch_id) SELECT task::uuid,b.material #>> '{ticket,manifest,scope}',b.id FROM hubuum_control.task_purge_batches b CROSS JOIN LATERAL jsonb_array_elements_text(b.material #> '{ticket,manifest,tasks}') task")
        .execute(connection).await?;
    diesel::sql_query("UPDATE hubuum_control.task_retention_control SET revision=$1,activation=$2,receipt_expiry_floor=$3,current_policy_revision=$4,receipt_count=$5,receipt_bytes=$6,retention_bytes=$7,hold_release_revision=$8 WHERE id=1")
        .bind::<BigInt,_>(state.revision().value()).bind::<Jsonb,_>(&value["activation"])
        .bind::<diesel::sql_types::Timestamptz,_>(state.receipt_expiry_floor())
        .bind::<BigInt,_>(state.policy().revision().value()).bind::<Integer,_>(state.receipts().len() as i32)
        .bind::<BigInt,_>(receipt_bytes as i64).bind::<BigInt,_>(retention_bytes as i64).bind::<BigInt,_>(state.hold_release_revision().value()).execute(connection).await?;
    Ok(())
}
