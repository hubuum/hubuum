//! Row/metadata exhaustion alongside the real HTTP/worker and signed replay paths.
use super::*;
use diesel::sql_types::{Array, Integer};
use hubuum_storage_postgres::with_transaction;
use serde::Serialize;

#[derive(Debug, PartialEq, Eq, QueryableByName, Serialize)]
struct Fingerprint {
    #[diesel(sql_type = BigInt)]
    rows: i64,
    #[diesel(sql_type = BigInt)]
    closed: i64,
    #[diesel(sql_type = Text)]
    versions: String,
    #[diesel(sql_type = BigInt)]
    metadata_bytes: i64,
    #[diesel(sql_type = BigInt)]
    largest_identity_bytes: i64,
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(QueryableByName)]
struct Population {
    #[diesel(sql_type = BigInt)]
    source_rows: i64,
    #[diesel(sql_type = BigInt)]
    exposure_rows: i64,
    #[diesel(sql_type = BigInt)]
    exposure_bytes: i64,
}

pub(super) struct Census {
    family: StorageIdentityHistoryKind,
    resource: ResourceIdentity,
    fingerprint: Fingerprint,
    before: HistoryLifecycleSnapshot,
    population: Population,
    requires_fallback: bool,
}

fn tables(family: StorageIdentityHistoryKind) -> (&'static str, &'static str, &'static str) {
    match family {
        StorageIdentityHistoryKind::Collection => {
            ("collections", "collections_history", "parent_collection_id")
        }
        StorageIdentityHistoryKind::ExportTemplate => (
            "export_templates",
            "export_templates_history",
            "collection_id",
        ),
        _ => unreachable!("census fixture family"),
    }
}

async fn fingerprint(
    pool: &PostgresPool,
    resource: ResourceIdentity,
    family: StorageIdentityHistoryKind,
) -> Fingerprint {
    let (_, history, parent) = tables(family);
    with_connection(pool, async |connection| {
        diesel::sql_query(format!("SELECT count(*) AS rows,count(*) FILTER (WHERE valid_to IS NOT NULL) AS closed,md5(string_agg(logical_identity #>> '{{subject,version}}',',' ORDER BY logical_identity #>> '{{subject,version}}')) AS versions,sum(octet_length(logical_identity::text))::bigint AS metadata_bytes,max(octet_length(logical_identity::text))::bigint AS largest_identity_bytes FROM public.{history} WHERE {parent}=(SELECT id FROM public.collections WHERE logical_identity #>> '{{subject,identity,identity}}'=$1)"))
            .bind::<Text,_>(resource.to_string()).get_result::<Fingerprint>(connection).await
    }).await.unwrap()
}

pub(super) async fn prepare(
    fixture: &RestoreContractFixture,
    pool: &PostgresPool,
    family: StorageIdentityHistoryKind,
    shape: &str,
) -> Census {
    let inventory = matches!(shape, "inventory_rows" | "inventory_metadata");
    let collection = fixture
        .create_collection(&format!("census_held_{}", Uuid::new_v4()), "initial")
        .await
        .unwrap();
    let storage = fixture.storage();
    let resource = storage
        .resolve_logical_identity(StorageIdentityTarget::Resource {
            kind: StorageIdentityResourceKind::Collection,
            id: collection.id().into(),
        })
        .await
        .unwrap()
        .subject()
        .and_then(StorageIdentitySubject::record_identity)
        .unwrap()
        .resource();
    // Provision genuine closed versions through the production history trigger,
    // without forging identities, intervals, revisions or restore-only writes.
    // Provisioning uses only fixture constants and a validated integer parent.
    if family == StorageIdentityHistoryKind::ExportTemplate {
        if inventory {
            metadata::inventory_filler(pool, collection.id().id()).await;
        } else {
            metadata::filler(pool, collection.id().id()).await;
        }
    } else {
        let (children, updates) = if inventory { (1201, 1) } else { (400, 251) };
        with_transaction(pool, async |connection| {
        diesel::sql_query("INSERT INTO public.collections(name,description,parent_collection_id) SELECT $1||n::text,'initial',$2 FROM generate_series(1,$3) n")
            .bind::<Text,_>(format!("census_{}_", Uuid::new_v4().simple()))
            .bind::<Integer,_>(collection.id().id()).bind::<Integer,_>(children).execute(connection).await?;
        diesel::sql_query(format!("DO $fixture$ BEGIN FOR n IN 1..{updates} LOOP UPDATE public.collections SET description=n::text WHERE parent_collection_id={}; END LOOP; END $fixture$", collection.id().id()))
            .execute(connection).await
    }).await.unwrap();
    }
    let snapshot = storage.capture_history_lifecycle().await.unwrap();
    let hold = HistoryRetentionHold::new(
        Uuid::new_v4(),
        HistoryPolicyKey::new(
            family,
            HistoryPolicyScope::Collection { identity: resource },
        )
        .unwrap(),
        snapshot.observed_at(),
        None,
        PrincipalIdentity::generate(),
    )
    .unwrap();
    let before = storage
        .commit_history_lifecycle(
            snapshot
                .control()
                .create_hold(hold, snapshot.fence())
                .unwrap(),
        )
        .await
        .unwrap();
    let fingerprint = fingerprint(pool, resource, family).await;
    if family == StorageIdentityHistoryKind::Collection {
        assert_eq!(
            (fingerprint.rows, fingerprint.closed),
            if inventory {
                (2402, 1201)
            } else {
                (100800, 100400)
            }
        );
    } else {
        assert_eq!(
            (fingerprint.rows, fingerprint.closed),
            if inventory { (32, 16) } else { (514, 513) }
        );
        if !inventory {
            assert!(fingerprint.metadata_bytes > HistorySizeCensus::MAX_METADATA_BYTES as i64);
        }
        assert!(fingerprint.largest_identity_bytes <= 1024 * 1024);
    }
    let (current, history, _) = tables(family);
    let population = with_connection(pool, async |connection| {
        diesel::sql_query(format!("SELECT (SELECT count(*) FROM public.{history}) AS source_rows,count(*) AS exposure_rows,sum(octet_length(logical_identity::text))::bigint AS exposure_bytes FROM (SELECT logical_identity FROM public.{current} UNION ALL SELECT logical_identity FROM public.{history} WHERE valid_to IS NULL AND op<>'D') inventory"))
            .get_result::<Population>(connection)
            .await
    })
    .await
    .unwrap();
    assert_eq!(
        population.exposure_rows > i64::from(HistoryRecoveryExposureBudget::MAX_ROWS),
        shape == "inventory_rows"
    );
    assert_eq!(
        population.exposure_bytes > HistoryRecoveryExposureBudget::MAX_BYTES as i64,
        shape == "inventory_metadata"
    );
    Census {
        family,
        resource,
        fingerprint,
        before,
        population,
        requires_fallback: !inventory,
    }
}

impl Census {
    pub(super) async fn verify(
        &self,
        pool: &PostgresPool,
        tasks: &[TaskId],
        after: &HistoryLifecycleSnapshot,
    ) -> Value {
        let warnings = with_connection(pool, async |connection| {
            diesel::sql_query("SELECT count(DISTINCT entity_id) AS count FROM public.events WHERE entity_type='task' AND entity_id=ANY($1) AND metadata #>> '{data,size_accounting_unavailable}'='true'")
                .bind::<Array<Integer>,_>(tasks.iter().map(|id| id.id()).collect::<Vec<_>>()).get_result::<Count>(connection).await
        }).await.unwrap().count;
        assert_eq!(
            usize::try_from(warnings).unwrap(),
            if self.requires_fallback {
                tasks.len()
            } else {
                0
            },
            "Unavailable accounting warnings must match the measured shape"
        );
        self.verify_restored(pool, after).await;
        json!({"source_rows":self.population.source_rows,"filler":self.fingerprint,
            "exposure_rows":self.population.exposure_rows,"exposure_bytes":self.population.exposure_bytes,
            "fallback_tasks":warnings,"expected_tasks":tasks.len(),"age_only":true,
            "pressure_unchanged":true,"holds_preserved":true,"filler_preserved":true,
            "restored_filler_preserved":true})
    }

    pub(super) async fn verify_restored(
        &self,
        pool: &PostgresPool,
        after: &HistoryLifecycleSnapshot,
    ) {
        assert_eq!(
            fingerprint(pool, self.resource, self.family).await,
            self.fingerprint,
            "Unselected filler versions must survive workers and signed restore unchanged"
        );
        assert_eq!(
            after.control().authority().size_transitions(),
            self.before.control().authority().size_transitions()
        );
        assert_eq!(
            after
                .control()
                .authority()
                .current_holds()
                .collect::<Vec<_>>(),
            self.before
                .control()
                .authority()
                .current_holds()
                .collect::<Vec<_>>()
        );
    }
}
