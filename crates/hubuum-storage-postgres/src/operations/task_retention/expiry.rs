use super::*;
use diesel::sql_types::Timestamptz;

#[derive(QueryableByName)]
struct RevisionRow {
    #[diesel(sql_type = BigInt)]
    revision: i64,
}

pub async fn select_expired_task_receipts(
    runtime: &PostgresRuntime,
    request: ReceiptExpirySelection,
) -> Result<Option<ReceiptExpiryBatch>, PostgresStorageError> {
    runtime
        .with_control_transaction(async move |connection| {
            let header = lock_on(connection).await?;
            let rows =
                diesel::sql_query("SELECT public.hubuum_task_receipts_expired($1) AS material")
                    .bind::<Integer, _>(request.limit() as i32)
                    .load::<MaterialRow>(connection)
                    .await?;
            let mut receipts = rows
                .into_iter()
                .map(|row| decode::<TaskReceipt>(row.material))
                .collect::<Result<Vec<_>, _>>()?;
            if receipts.is_empty() {
                return Ok::<_, PostgresStorageError>(None);
            }
            receipts.sort_by(|a, b| a.reference().submission().cmp(b.reference().submission()));
            let floor = super::super::task_execution::database_now(connection)
                .await?
                .and_utc();
            Ok(Some(ReceiptExpiryBatch::new(
                header.revision,
                floor,
                receipts
                    .into_iter()
                    .map(|receipt| receipt.reference().clone())
                    .collect(),
            )?))
        })
        .await
}

pub async fn expire_task_receipts(
    runtime: &PostgresRuntime,
    request: ProtectedReceiptExpiry,
) -> Result<TaskControlRevision, PostgresStorageError> {
    runtime
        .with_control_transaction(async move |connection| {
            let batch = request.batch();
            let checkpoint = request.checkpoint();
            let targets = serde_json::to_value(batch.receipts()).map_err(|_| {
                PostgresStorageError::internal("Cannot encode receipt expiry batch")
            })?;
            let revision = diesel::sql_query(
                "SELECT public.hubuum_task_receipts_expire($1,$2,$3,$4,$5,$6) AS revision",
            )
            .bind::<BigInt, _>(batch.expected_revision().value())
            .bind::<Timestamptz, _>(batch.floor())
            .bind::<Jsonb, _>(targets)
            .bind::<BigInt, _>(checkpoint.task_revision().value())
            .bind::<SqlUuid, _>(checkpoint.lineage())
            .bind::<SqlUuid, _>(checkpoint.epoch())
            .get_result::<RevisionRow>(connection)
            .await?
            .revision;
            Ok::<_, PostgresStorageError>(TaskControlRevision::new(revision)?)
        })
        .await
}
