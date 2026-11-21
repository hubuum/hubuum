//! Short native catalog/capture transactions; external segment I/O is service-owned.
use super::*;
use crate::PostgresConnection;
use chrono::{DateTime, Utc};
use diesel::sql_types::{
    BigInt, Binary, Bool, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
};
use diesel::{QueryableByName, sql_query};
use diesel_async::RunQueryDsl;
use hubuum_domain::{PrincipalIdentity, ResourceIdentity};
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

#[derive(QueryableByName)]
struct ObservedMaterial {
    #[diesel(sql_type=Nullable<Text>)]
    material: Option<String>,
    #[diesel(sql_type=Timestamptz)]
    observed_at: DateTime<Utc>,
}
#[derive(QueryableByName)]
struct Material {
    #[diesel(sql_type=Nullable<Text>)]
    material: Option<String>,
}
#[derive(QueryableByName)]
struct Allowed {
    #[diesel(sql_type=Bool)]
    allowed: bool,
}
#[derive(QueryableByName)]
struct Clock {
    #[diesel(sql_type=Timestamptz)]
    recorded_at: DateTime<Utc>,
}
#[derive(QueryableByName)]
struct Hot {
    #[diesel(sql_type=Jsonb)]
    authorization: Value,
    #[diesel(sql_type=Jsonb)]
    material: Value,
    #[diesel(sql_type=Bool)]
    known_empty: bool,
    #[diesel(sql_type=Bool)]
    live: bool,
}

macro_rules! fenced {
    ($query:expr, $permit:expr) => {
        $query
            .bind::<SqlUuid, _>(
                $permit
                    .owner()
                    .ok_or_else(|| {
                        PostgresStorageError::unavailable(
                            "Archive requires a registered Legacy owner",
                        )
                    })?
                    .id(),
            )
            .bind::<SqlUuid, _>($permit.owner().expect("checked owner").ownership_token())
            .bind::<SqlUuid, _>($permit.fence().incarnation())
            .bind::<BigInt, _>($permit.fence().revision())
    };
}
fn decode<T: DeserializeOwned>(encoded: &str) -> Result<T, PostgresStorageError> {
    serde_json::from_str(encoded)
        .map_err(|e| PostgresStorageError::invalid_persisted_value("history archive", e))
}
fn json(value: &impl serde::Serialize, max: usize) -> Result<Value, PostgresStorageError> {
    let bytes = history_archive_encoded_bytes(value, max)?;
    serde_json::from_slice(&bytes).map_err(|e| {
        PostgresStorageError::invalid_persisted_value("history archive serialization", e)
    })
}
async fn catalog_on(
    connection: &mut PostgresConnection,
    resource: ResourceIdentity,
    locked: bool,
    permit: &StorageExecutionPermit,
) -> Result<HistoryArchiveCatalog, PostgresStorageError> {
    let row = fenced!(
        sql_query(
            "SELECT public.hubuum_history_archive_catalog_read($1,$2,$3,$4,$5,$6)::text AS material"
        )
        .bind::<SqlUuid, _>(Uuid::from_bytes(*resource.as_bytes()))
        .bind::<Bool, _>(locked),
        permit
    )
    .get_result::<Material>(connection)
    .await?;
    let catalog = row
        .material
        .as_deref()
        .map(decode)
        .transpose()?
        .unwrap_or_else(|| HistoryArchiveCatalog::empty(resource));
    catalog.validate()?;
    if catalog.resource() != resource {
        return Err(PostgresStorageError::database("Catalog resource mismatch"));
    }
    Ok(catalog)
}
async fn hot_on(
    connection: &mut PostgresConnection,
    selection: &HistoryArchiveSelection,
) -> Result<HistoryArchiveInput, PostgresStorageError> {
    if selection.family() != StorageBackupHistorySection::CollectionHistory {
        return Ok(HistoryArchiveInput::Unavailable);
    }
    let visible =
        sql_query("SELECT public.hubuum_history_archive_resource_available($1) AS allowed")
            .bind::<SqlUuid, _>(Uuid::from_bytes(*selection.resource().as_bytes()))
            .get_result::<Allowed>(connection)
            .await?
            .allowed;
    if !visible {
        return Ok(HistoryArchiveInput::Unavailable);
    }
    let (version, at) = match selection.point() {
        HistoryArchivePoint::Exact { version } => (
            Some(Uuid::from_bytes(*version.as_bytes()).to_string()),
            None,
        ),
        HistoryArchivePoint::AsOf { at } => (None, Some(*at)),
    };
    // One statement snapshot. Limit both candidate count and returned text before
    // allocation in Rust; do not materialize an unbounded history page.
    let rows = sql_query("WITH candidates AS (SELECT h.* FROM public.collections_history h WHERE logical_identity #>> '{subject,identity,identity}'=$1 AND (($2::text IS NOT NULL AND logical_identity #>> '{subject,version}'=$2) OR ($2::text IS NULL AND valid_from<=$3 AND (valid_to IS NULL OR $3<valid_to))) ORDER BY valid_from DESC,history_id DESC LIMIT 2), encoded AS (SELECT CASE WHEN octet_length(name)+octet_length(description)>4194304 THEN NULL ELSE jsonb_build_object('identity',jsonb_build_object('resource',logical_identity #> '{subject,identity,identity}','version',logical_identity #> '{subject,version}'),'valid_from',valid_from,'valid_to',valid_to,'payload',jsonb_build_object('id',id,'name',name,'description',description,'created_at',created_at AT TIME ZONE 'UTC','updated_at',updated_at AT TIME ZONE 'UTC','parent_collection_id',parent_collection_id,'revision',revision,'logical_identity',logical_identity)) END AS row,id,name,logical_identity FROM candidates) SELECT COALESCE((SELECT jsonb_agg(CASE WHEN row IS NULL OR octet_length(row::text)>4194304 THEN '__oversized__' ELSE row::text END) FROM encoded),'[]'::jsonb) AS material,COALESCE((SELECT jsonb_agg(jsonb_build_object('collection',id,'name',name,'identity',jsonb_build_object('resource',logical_identity #> '{subject,identity,identity}','version',logical_identity #> '{subject,version}'))) FROM encoded),'[]'::jsonb) AS authorization,EXISTS(SELECT 1 FROM public.collections WHERE logical_identity #>> '{subject,identity,identity}'=$1 AND $3 < created_at AT TIME ZONE 'UTC') AS known_empty,EXISTS(SELECT 1 FROM public.collections WHERE logical_identity #>> '{subject,identity,identity}'=$1) AS live")
        .bind::<Text,_>(Uuid::from_bytes(*selection.resource().as_bytes()).to_string())
        .bind::<Nullable<Text>,_>(version).bind::<Nullable<Timestamptz>,_>(at).get_result::<Hot>(connection).await?;
    let encoded: Vec<String> = serde_json::from_value(rows.material)
        .map_err(|e| PostgresStorageError::invalid_persisted_value("history capture", e))?;
    if encoded.len() > 1 {
        return Err(PostgresStorageError::database(
            "Ambiguous collection history intervals",
        ));
    }
    if !rows.live {
        return Ok(HistoryArchiveInput::Unavailable);
    }
    let Some(bytes) = encoded.as_slice().first() else {
        return Ok(if rows.known_empty {
            HistoryArchiveInput::CompleteEmpty
        } else {
            HistoryArchiveInput::Unavailable
        });
    };
    if bytes == "__oversized__" || bytes.len() > HISTORY_ARCHIVE_ROW_BYTES {
        let mut facts: Vec<HistoryArchiveAuthorization> =
            serde_json::from_value(rows.authorization).map_err(|error| {
                PostgresStorageError::invalid_persisted_value("history authorization", error)
            })?;
        return Ok(HistoryArchiveInput::BudgetLimited(facts.pop().ok_or_else(
            || PostgresStorageError::database("Overflow authorization missing"),
        )?));
    }
    Ok(HistoryArchiveInput::Available(decode::<HistoryArchiveRow>(
        bytes,
    )?))
}
async fn capture_on(
    connection: &mut PostgresConnection,
    id: Uuid,
    principal: PrincipalIdentity,
    locked: bool,
    permit: &StorageExecutionPermit,
) -> Result<Option<HistoryArchiveCapture>, PostgresStorageError> {
    let material = fenced!(sql_query("SELECT public.hubuum_history_archive_capture_read($1,$2,$3,$4,$5,$6,$7)::text AS material,clock_timestamp() AS observed_at")
        .bind::<SqlUuid,_>(id).bind::<SqlUuid,_>(Uuid::from_bytes(*principal.as_bytes())).bind::<Bool,_>(locked),permit).get_result::<ObservedMaterial>(connection).await?;
    let capture: Option<HistoryArchiveCapture> =
        material.material.as_deref().map(decode).transpose()?;
    if let Some(capture) = &capture {
        capture.validate_access(principal, permit)?;
    }
    Ok(capture.map(|capture| capture.observe_expiry(material.observed_at)))
}
async fn save_on(
    connection: &mut PostgresConnection,
    capture: &HistoryArchiveCapture,
    expected_work: Option<i64>,
    permit: &StorageExecutionPermit,
) -> Result<bool, PostgresStorageError> {
    let saved = fenced!(
        sql_query(
            "SELECT public.hubuum_history_archive_capture_save($1,$2,$3,$4,$5,$6) AS allowed"
        )
        .bind::<Jsonb, _>(json(capture, HISTORY_ARCHIVE_CAPTURE_BYTES)?)
        .bind::<Nullable<BigInt>, _>(expected_work),
        permit
    )
    .get_result::<Allowed>(connection)
    .await?;
    Ok(saved.allowed)
}
// Returns metadata only inside the transaction; a usable affine claim is
// constructed by the adapter AFTER with_history_archive_claim_transaction commits.
async fn publication_claim_on(
    connection: &mut PostgresConnection,
    request: HistoryArchivePublicationRequest,
    permit: &HistoryArchiveExecutionPermit,
) -> Result<HistoryArchivePublicationRequest, PostgresStorageError> {
    let manifest = request.manifest();
    let capture = capture_on(
        connection,
        manifest.capture(),
        manifest.principal(),
        true,
        permit.application(),
    )
    .await?
    .ok_or_else(|| PostgresStorageError::unavailable("Original archive capture is unavailable"))?;
    let now = sql_query("SELECT clock_timestamp() AS recorded_at")
        .get_result::<Clock>(connection)
        .await?
        .recorded_at;
    request.validate_capture(&capture, permit, now)?;
    let producer = permit.producer();
    let saved = sql_query("SELECT public.hubuum_history_archive_publication_claim($1,$2,$3,$4,$5,$6,$7,$8,$9) AS allowed")
                .bind::<SqlUuid,_>(producer.parent().id())
                .bind::<SqlUuid,_>(producer.parent().ownership_token())
                .bind::<SqlUuid,_>(producer.id())
                .bind::<SqlUuid,_>(producer.ownership_token())
                .bind::<SqlUuid,_>(permit.application().owner().expect("validated original owner").id())
                .bind::<SqlUuid,_>(permit.application().owner().expect("validated original owner").ownership_token())
                    .bind::<Binary, _>(history_archive_encoded_bytes(manifest, HISTORY_ARCHIVE_PUBLICATION_MANIFEST_BYTES)?)
    .bind::<Binary, _>(request.intent().encoded())
                .bind::<Jsonb,_>(json(&capture,HISTORY_ARCHIVE_CAPTURE_BYTES)?)
                .get_result::<Allowed>(connection).await?;
    if !saved.allowed {
        return Err(PostgresStorageError::conflict(
            "Publication claim is unavailable or already assigned",
        ));
    }
    Ok::<_, PostgresStorageError>(request)
}

impl PostgresStorage {
    fn require_archive_owner(
        &self,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<(), StorageError> {
        permit.validate_scope(&self.current_execution_scope())?;
        if permit.application().owner().is_none()
            || self
                .runtime()
                .runtime_owner()
                .is_some_and(|bound| Some(bound) != permit.application().owner())
        {
            return Err(StorageError::unavailable(
                "Archive requires its original registered Legacy owner",
            ));
        }
        Ok(())
    }
}
#[async_trait]
impl HistoryArchiveStorage for PostgresStorage {
    async fn claim_history_archive_publication(
        &self,
        request: HistoryArchivePublicationRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchivePublicationClaim, StorageError> {
        self.require_archive_owner(permit)?;
        let request = self
            .runtime()
            .with_history_archive_claim_transaction(permit, async move |connection| {
                publication_claim_on(connection, request, permit).await
            })
            .await
            .map_err(StorageError::from)?;
        // No usable claim escapes the transaction before its commit succeeds.
        HistoryArchivePublicationClaim::from_committed(request, permit)
    }

    async fn history_archive_catalog(
        &self,
        resource: ResourceIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        self.require_archive_owner(permit)?;
        self.runtime()
            .with_history_archive_connection(permit, async |connection| {
                catalog_on(connection, resource, false, permit.application()).await
            })
            .await
            .map_err(StorageError::from)
    }
    async fn publish_history_archive(
        &self,
        expected: HistoryArchiveCatalogGeneration,
        descriptors: Vec<HistoryArchiveDescriptor>,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCatalog, StorageError> {
        self.require_archive_owner(permit)?;
        self.runtime().with_history_archive_transaction(permit, async move |connection| {
        let resource = descriptors
            .as_slice()
            .first()
            .ok_or_else(|| StorageError::invalid_input("Empty archive publication"))?
            .identity()
            .resource();
            let current=catalog_on(connection,resource,true,permit.application()).await?;
            let next=current.publish(expected,descriptors.clone())?;
            let mut scanned=history_archive_encoded_bytes(&descriptors,HISTORY_ARCHIVE_BATCH_BYTES)?.len();
            for descriptor in &descriptors {
                let selection=HistoryArchiveSelection::new(StorageBackupHistorySection::CollectionHistory,resource,HistoryArchivePoint::Exact{version:descriptor.identity().version()});
                let input=hot_on(connection,&selection).await?;
                let HistoryArchiveInput::Available(hot)=input else { return Err(PostgresStorageError::conflict("Publication requires the exact retained primary version")); };
                scanned+=history_archive_encoded_bytes(&hot,HISTORY_ARCHIVE_ROW_BYTES)?.len();
                if scanned>HISTORY_ARCHIVE_BATCH_BYTES { return Err(StorageError::input_too_large("Publication input exceeds batch budget").into()); }
                if !descriptor.matches_row(&hot)? { return Err(PostgresStorageError::conflict("Catalog content differs from retained primary version")); }
            }
            let saved=fenced!(sql_query("SELECT public.hubuum_history_archive_catalog_publish($1,$2,$3,$4,$5,$6,$7) AS allowed")
                .bind::<SqlUuid,_>(Uuid::from_bytes(*resource.as_bytes())).bind::<BigInt,_>(expected.value()).bind::<Jsonb,_>(json(&next,HISTORY_ARCHIVE_CATALOG_BYTES)?),permit.application()).get_result::<Allowed>(connection).await?;
            if !saved.allowed { return Err(PostgresStorageError::conflict("Catalog generation changed")); }
            Ok::<_,PostgresStorageError>(next)
        }).await.map_err(StorageError::from)
    }
    async fn capture_history_archive(
        &self,
        request: HistoryArchiveCaptureRequest,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        self.require_archive_owner(permit)?;
        let (capture, saved) = self
            .runtime()
            .with_history_archive_transaction(permit, async move |connection| {
                let catalog = catalog_on(
                    connection,
                    request.selection().resource(),
                    true,
                    permit.application(),
                )
                .await?;
                let input = hot_on(connection, request.selection()).await?;
                let now = sql_query("SELECT clock_timestamp() AS recorded_at")
                    .get_result::<Clock>(connection)
                    .await?
                    .recorded_at;
                let capture = HistoryArchiveCapture::capture(
                    request,
                    &catalog,
                    permit.application(),
                    input,
                    now,
                )?
                .bind_execution(permit)?;
                let saved = save_on(connection, &capture, None, permit.application()).await?;
                // Capacity rejection must commit the bounded expired-pin cleanup
                // so later admissions can reclaim the next batch.
                Ok::<_, PostgresStorageError>((capture, saved))
            })
            .await
            .map_err(StorageError::from)?;
        if saved {
            Ok(capture)
        } else {
            Err(capture.staging_budget_error())
        }
    }
    async fn history_archive_capture(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<Option<HistoryArchiveCapture>, StorageError> {
        self.require_archive_owner(permit)?;
        self.runtime()
            .with_history_archive_connection(permit, async |connection| {
                let capture =
                    capture_on(connection, id, principal, false, permit.application()).await?;
                if let Some(capture) = &capture {
                    capture.validate_execution(permit)?;
                }
                Ok::<_, PostgresStorageError>(capture)
            })
            .await
            .map_err(StorageError::from)
    }
    async fn reserve_history_archive_read(
        &self,
        id: Uuid,
        principal: PrincipalIdentity,
        bytes: u64,
        permit: &HistoryArchiveExecutionPermit,
    ) -> Result<HistoryArchiveCapture, StorageError> {
        self.require_archive_owner(permit)?;
        self.runtime()
            .with_history_archive_transaction(permit, async |connection| {
                let mut capture = capture_on(connection, id, principal, true, permit.application())
                    .await?
                    .ok_or_else(|| {
                        PostgresStorageError::not_found("Archive capture unavailable")
                    })?;
                capture.validate_execution(permit)?;
                let expected = capture.reserved_read_bytes() as i64;
                capture.reserve(bytes, Utc::now())?;
                if !save_on(connection, &capture, Some(expected), permit.application()).await? {
                    return Err(PostgresStorageError::from(capture.staging_budget_error()));
                }
                Ok::<_, PostgresStorageError>(capture)
            })
            .await
            .map_err(StorageError::from)
    }
}

#[cfg(all(test, feature = "integration-test-support"))]
mod tests;
