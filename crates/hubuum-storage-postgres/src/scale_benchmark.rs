use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use argon2::{Argon2, password_hash::PasswordHasher};
use async_trait::async_trait;
use diesel::QueryableByName;
use diesel::sql_types::{BigInt, Double, Integer, Text};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hubuum_scale_core::{
    BackendIdentity, BackendPreparation, BackendResourceReport, BenchmarkPrincipal, ClassPlan,
    ClassRelationPlan, DatasetManifest, DatasetRegion, Error, LoadReport, Result,
    ScaleBenchmarkBackend, ScaleProfile, class_relation_plan, invalid_data,
};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

use crate::{
    PostgresPool, PostgresPoolSettings, build_postgres_pool, with_connection, with_transaction,
};

const BENCHMARK_PASSWORD: &str = "hubuum-scale-benchmark-disposable-password";

#[derive(Debug)]
struct ReachabilityPlan {
    ancestor: u64,
    descendant: u64,
    path: Vec<u64>,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    value: i64,
}

#[derive(QueryableByName)]
struct DistributionRow {
    #[diesel(sql_type = BigInt)]
    maximum: i64,
    #[diesel(sql_type = BigInt)]
    median: i64,
}

#[derive(QueryableByName)]
struct RatioRow {
    #[diesel(sql_type = Double)]
    value: f64,
}

#[derive(QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    value: String,
}

#[derive(QueryableByName)]
struct CollectionIdRow {
    #[diesel(sql_type = Integer)]
    collection_id: i32,
}

#[derive(QueryableByName)]
struct DatabaseResourceRow {
    #[diesel(sql_type = BigInt)]
    database_bytes: i64,
    #[diesel(sql_type = BigInt)]
    table_bytes: i64,
    #[diesel(sql_type = BigInt)]
    index_bytes: i64,
}

#[derive(Clone)]
pub struct PostgresScaleBackend {
    database_url: String,
    pool: PostgresPool,
}

pub struct PostgresResourceBaseline {
    wal_position: i64,
    cpu_seconds: Option<f64>,
    resident_bytes: Option<u64>,
}

impl PostgresScaleBackend {
    pub fn connect(database_url: &str, max_size: u32) -> Result<Self> {
        Ok(Self {
            database_url: database_url.to_string(),
            pool: benchmark_pool(database_url, max_size)?,
        })
    }
}

fn benchmark_pool(database_url: &str, max_size: u32) -> Result<PostgresPool> {
    let settings = PostgresPoolSettings::builder(database_url)
        .max_size(max_size)
        .statement_timeout_ms(0)
        .acquire_timeout_ms(60_000)
        .build()?;
    Ok(build_postgres_pool(&settings)?)
}

async fn load_dataset_with_pool(profile: &ScaleProfile, pool: &PostgresPool) -> Result<LoadReport> {
    profile.validate()?;
    let generation_started = Instant::now();
    let manifest = profile.manifest()?;
    let class_plans = profile.class_plan();
    let relation_plans = class_relation_plan(profile)?;
    let reachability = reachability_plan(&relation_plans);
    let generation_ms = elapsed_ms(generation_started);
    let password_hash = Argon2::default()
        .hash_password(BENCHMARK_PASSWORD.as_bytes())
        .map_err(|error| invalid_data(format!("failed to hash benchmark credential: {error}")))?
        .to_string();
    ensure_fresh_database(pool).await?;

    let loading_started = Instant::now();
    let profile_name = profile.name;
    let profile_seed = profile.seed;
    let transaction_profile = profile.clone();
    with_transaction(
        pool,
        async move |connection| -> std::result::Result<_, diesel::result::Error> {
            connection
                .batch_execute(
                    "SET LOCAL session_replication_role = replica;\n\
                     SET LOCAL hubuum.restore_revisions = 'on';\n\
                     SET LOCAL hubuum.restore_events = 'on';\n\
                     SET LOCAL statement_timeout = 0;",
                )
                .await?;
            load_identities(connection, &transaction_profile, &password_hash).await?;
            load_collections(connection, &transaction_profile).await?;
            load_class_plan(connection, &class_plans).await?;
            load_classes_and_objects(connection, &transaction_profile).await?;
            load_class_relations(connection, &relation_plans, &reachability).await?;
            load_object_relations(connection, &transaction_profile).await?;
            load_authorization(connection, &transaction_profile).await?;
            load_history_and_operations(connection, &transaction_profile).await?;
            reset_sequences(connection).await?;
            Ok(())
        },
    )
    .await
    .map_err(|error| invalid_data(format!("scale dataset load failed: {error}")))?;

    with_connection(
        pool,
        async |connection| -> std::result::Result<_, diesel::result::Error> {
            connection
                .batch_execute(
                    "ANALYZE collections; ANALYZE hubuumclass; ANALYZE hubuumobject; \
                     ANALYZE hubuumclass_relation; ANALYZE hubuumobject_relation; \
                     ANALYZE permissions; ANALYZE events; ANALYZE event_deliveries; \
                     ANALYZE tasks;",
                )
                .await
        },
    )
    .await
    .map_err(|error| invalid_data(format!("scale dataset analyze failed: {error}")))?;

    verify_loaded_dataset(pool, profile, &manifest).await?;
    Ok(LoadReport {
        backend: "postgres".to_string(),
        profile: profile_name,
        seed: profile_seed,
        generation_ms,
        loading_ms: elapsed_ms(loading_started),
        manifest,
    })
}

async fn ensure_fresh_database(pool: &PostgresPool) -> Result<()> {
    let value = scalar(
        pool,
        "SELECT (SELECT count(*) FROM hubuumclass) + \
         (SELECT count(*) FROM hubuumobject) + \
         (SELECT count(*) FROM principals) AS value",
    )
    .await?;
    let collections = scalar(pool, "SELECT count(*) AS value FROM collections").await?;
    if value != 0 || collections != 1 {
        return Err(invalid_data(format!(
            "scale loader requires a freshly migrated database (domain rows={value}, collections={collections})"
        )));
    }
    Ok(())
}

async fn load_identities(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
    password_hash: &str,
) -> std::result::Result<(), diesel::result::Error> {
    let principal_count = profile.totals.principals;
    let group_count = profile.totals.groups;
    connection
        .batch_execute(&format!(
            "INSERT INTO principals (id, kind, name, identity_scope_id, settings, revision)\n\
         SELECT n, 'human',\n\
           CASE n WHEN 1 THEN 'scale-admin' WHEN 2 THEN 'scale-tenant'\n\
             WHEN 3 THEN 'scale-sparse' ELSE format('scale-principal-%s', n) END,\n\
           1, jsonb_build_object('benchmark', true), 1\n\
         FROM generate_series(1, {principal_count}) AS n"
        ))
        .await?;
    diesel::sql_query(format!(
        "INSERT INTO users (id, kind, password, proper_name, email)\n\
         SELECT n, 'human', $1, format('Scale principal %s', n), NULL\n\
         FROM generate_series(1, {principal_count}) AS n"
    ))
    .bind::<Text, _>(password_hash)
    .execute(connection)
    .await?;
    connection.batch_execute(&format!(
        "INSERT INTO groups (id, groupname, description, identity_scope_id, managed_by, revision)\n\
         SELECT n, CASE n WHEN 1 THEN 'admin' WHEN 2 THEN 'scale-tenant'\n\
           WHEN 3 THEN 'scale-sparse' ELSE format('scale-group-%s', n) END,\n\
           'Deterministic scale benchmark group', 1, 'local', 1\n\
         FROM generate_series(1, {group_count}) AS n"
    ))
    .await?;
    Ok(())
}

async fn load_collections(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
) -> std::result::Result<(), diesel::result::Error> {
    let collections = profile.totals.collections;
    connection
        .batch_execute(&format!(
            "INSERT INTO collections (id, name, description, parent_collection_id, revision)\n\
         SELECT n, format('scale-collection-%06s', n),\n\
           'Deterministic scale benchmark collection',\n\
           CASE WHEN n = 2 THEN 1 ELSE 1 + ((n - 2) / 8) END, 1\n\
         FROM generate_series(2, {collections}) AS n"
        ))
        .await?;
    connection
        .batch_execute(
            "TRUNCATE collection_closure;\n\
             WITH RECURSIVE closure(ancestor_collection_id, descendant_collection_id, depth) AS (\n\
               SELECT id, id, 0 FROM collections\n\
               UNION ALL\n\
               SELECT parent.parent_collection_id, closure.descendant_collection_id, closure.depth + 1\n\
               FROM closure\n\
               JOIN collections parent ON parent.id = closure.ancestor_collection_id\n\
               WHERE parent.parent_collection_id IS NOT NULL\n\
             )\n\
             INSERT INTO collection_closure\n\
             SELECT ancestor_collection_id, descendant_collection_id, depth FROM closure;\n\
             INSERT INTO collection_authorization_state (collection_id, revision)\n\
             SELECT id, 1 FROM collections ON CONFLICT (collection_id) DO NOTHING;",
        )
        .await?;
    Ok(())
}

async fn load_class_plan(
    connection: &mut diesel_async::AsyncPgConnection,
    plans: &[ClassPlan],
) -> std::result::Result<(), diesel::result::Error> {
    connection
        .batch_execute(
            "CREATE TEMP TABLE scale_class_plan (\n\
               id INTEGER PRIMARY KEY, collection_id INTEGER NOT NULL, region TEXT NOT NULL,\n\
               object_count BIGINT NOT NULL, first_object_id BIGINT NULL\n\
             ) ON COMMIT DROP;",
        )
        .await?;
    for chunk in plans.chunks(1_000) {
        let values = chunk
            .iter()
            .map(|plan| {
                let region = match plan.region {
                    DatasetRegion::ObjectHeavy => "object_heavy",
                    DatasetRegion::ClassHeavy => "class_heavy",
                    DatasetRegion::Balanced => "balanced",
                };
                let first_object = plan
                    .first_object_id
                    .map_or_else(|| "NULL".to_string(), |value| value.to_string());
                format!(
                    "({}, {}, '{}', {}, {})",
                    plan.id, plan.collection_id, region, plan.object_count, first_object
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        connection
            .batch_execute(&format!("INSERT INTO scale_class_plan VALUES {values};"))
            .await?;
    }
    Ok(())
}

async fn load_classes_and_objects(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
) -> std::result::Result<(), diesel::result::Error> {
    connection
        .batch_execute(&format!(
            "INSERT INTO hubuumclass (\n\
           id, name, collection_id, json_schema, validate_schema, description,\n\
           created_at, updated_at, revision\n\
         )\n\
         SELECT id, format('scale-class-%06s', id), collection_id,\n\
           CASE WHEN id % 20 = 0 THEN '{{\"type\":\"object\"}}'::jsonb ELSE NULL END,\n\
           false, format('Scale %s class', region),\n\
           timestamp '2026-01-01 00:00:00', timestamp '2026-01-01 00:00:00', 1\n\
         FROM scale_class_plan ORDER BY id;\n\
         WITH generated AS (\n\
           SELECT row_number() OVER (ORDER BY plan.id, ordinal)::INTEGER AS object_id,\n\
             plan.id AS class_id, plan.collection_id, plan.region, ordinal\n\
           FROM scale_class_plan plan\n\
           CROSS JOIN LATERAL generate_series(1, plan.object_count) AS ordinal\n\
         )\n\
         INSERT INTO hubuumobject (\n\
           id, name, collection_id, hubuum_class_id, data, description,\n\
           created_at, updated_at, revision\n\
         )\n\
         SELECT object_id, format('scale-object-%010s', object_id), collection_id, class_id,\n\
           jsonb_build_object(\n\
             'benchmark', true, 'region', region, 'ordinal', ordinal,\n\
             'selectivity_bucket', (ordinal + {seed}) % 100,\n\
             'timestamp_bucket', (ordinal + {seed}) % 365,\n\
             'payload', repeat('x', CASE WHEN ordinal % 1000 = 0 THEN 8192\n\
               WHEN ordinal % 100 = 0 THEN 2048 WHEN ordinal % 20 = 0 THEN 512 ELSE 64 END)\n\
           ), 'Deterministic scale benchmark object',\n\
           timestamp '2026-01-01 00:00:00', timestamp '2026-01-01 00:00:00', 1\n\
         FROM generated ORDER BY object_id;",
            seed = profile.seed
        ))
        .await?;
    Ok(())
}

fn reachability_plan(relations: &[ClassRelationPlan]) -> Vec<ReachabilityPlan> {
    let mut adjacency = BTreeMap::<u64, Vec<u64>>::new();
    for relation in relations {
        adjacency
            .entry(relation.from_class_id)
            .or_default()
            .push(relation.to_class_id);
        adjacency
            .entry(relation.to_class_id)
            .or_default()
            .push(relation.from_class_id);
    }
    for neighbors in adjacency.values_mut() {
        neighbors.sort_unstable();
        neighbors.dedup();
    }
    let mut result = Vec::new();
    for start in adjacency.keys().copied() {
        let mut paths = BTreeMap::<u64, Vec<u64>>::from([(start, vec![start])]);
        let mut queue = VecDeque::from([start]);
        while let Some(node) = queue.pop_front() {
            let prefix = paths[&node].clone();
            for neighbor in adjacency.get(&node).into_iter().flatten().copied() {
                if paths.contains_key(&neighbor) {
                    continue;
                }
                let mut path = prefix.clone();
                path.push(neighbor);
                paths.insert(neighbor, path);
                queue.push_back(neighbor);
            }
        }
        for (end, path) in paths {
            if start < end {
                result.push(ReachabilityPlan {
                    ancestor: start,
                    descendant: end,
                    path,
                });
            }
        }
    }
    result
}

async fn load_class_relations(
    connection: &mut diesel_async::AsyncPgConnection,
    relations: &[ClassRelationPlan],
    reachability: &[ReachabilityPlan],
) -> std::result::Result<(), diesel::result::Error> {
    connection
        .batch_execute(
            "CREATE TEMP TABLE scale_relation_plan (\n\
               id INTEGER PRIMARY KEY, from_class_id INTEGER NOT NULL, to_class_id INTEGER NOT NULL,\n\
               region TEXT NOT NULL\n\
             ) ON COMMIT DROP;",
        )
        .await?;
    for chunk in relations.chunks(1_000) {
        let values = chunk
            .iter()
            .map(|relation| {
                let region = match relation.region {
                    DatasetRegion::ObjectHeavy => "object_heavy",
                    DatasetRegion::ClassHeavy => "class_heavy",
                    DatasetRegion::Balanced => "balanced",
                };
                format!(
                    "({}, {}, {}, '{}')",
                    relation.id, relation.from_class_id, relation.to_class_id, region
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        connection
            .batch_execute(&format!("INSERT INTO scale_relation_plan VALUES {values};"))
            .await?;
    }
    connection
        .batch_execute(
            "INSERT INTO hubuumclass_relation (\n\
               id, from_hubuum_class_id, to_hubuum_class_id,\n\
               forward_template_alias, reverse_template_alias, revision\n\
             )\n\
             SELECT id, from_class_id, to_class_id,\n\
               format('forward_%s', id), format('reverse_%s', id), 1\n\
             FROM scale_relation_plan ORDER BY id;",
        )
        .await?;
    for chunk in reachability.chunks(1_000) {
        let values = chunk
            .iter()
            .map(|entry| {
                let path = entry
                    .path
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "({}, {}, {}, ARRAY[{}]::INTEGER[])",
                    entry.ancestor,
                    entry.descendant,
                    entry.path.len() - 1,
                    path
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        connection
            .batch_execute(&format!(
                "INSERT INTO hubuumclass_reachability \
                 (ancestor_class_id, descendant_class_id, depth, path) VALUES {values};"
            ))
            .await?;
    }
    Ok(())
}

async fn load_object_relations(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
) -> std::result::Result<(), diesel::result::Error> {
    let object_heavy = &profile.regions.object_heavy;
    let hub_degree = profile.invariants.minimum_hub_object_degree;
    let class_plan = profile.class_plan();
    let first_class = class_plan.as_slice().first().expect("hot class plan");
    let second_class = &class_plan[1];
    let first_start = first_class.first_object_id.expect("hot class object start");
    let second_start = second_class
        .first_object_id
        .expect("secondary class object start");
    connection
        .batch_execute(&format!(
            "WITH generated AS (SELECT n FROM generate_series(1, {relation_count}) AS n)\n\
         INSERT INTO hubuumobject_relation (\n\
           id, from_hubuum_object_id, to_hubuum_object_id, class_relation_id, revision\n\
         )\n\
         SELECT n,\n\
           CASE WHEN n <= {hub_degree} THEN {first_start}\n\
             ELSE {first_start} + 1 + ((n - {hub_degree} - 1) % {from_non_hub}) END,\n\
           CASE WHEN n <= {hub_degree} THEN {second_start} + n - 1\n\
             ELSE {second_start} + ((((n - {hub_degree} - 1) / {from_non_hub})\n\
               + ((n - {hub_degree} - 1) % {from_non_hub}) * 17) % {to_count}) END,\n\
           1, 1 FROM generated;",
            relation_count = object_heavy.object_relations,
            from_non_hub = first_class.object_count - 1,
            to_count = second_class.object_count,
        ))
        .await?;

    let mut next_id = object_heavy.object_relations + 1;
    let object_class_start = object_heavy.classes + 1;
    let class_heavy_relation_start = object_heavy.class_relations + 1;
    load_spread_object_relations(
        connection,
        "class_heavy",
        profile.regions.class_heavy.object_relations,
        next_id,
        class_heavy_relation_start,
        profile.regions.class_heavy.class_relations,
        object_class_start,
    )
    .await?;
    next_id += profile.regions.class_heavy.object_relations;
    let balanced_class_start = object_class_start + profile.regions.class_heavy.classes;
    let balanced_relation_start =
        class_heavy_relation_start + profile.regions.class_heavy.class_relations;
    load_spread_object_relations(
        connection,
        "balanced",
        profile.regions.balanced.object_relations,
        next_id,
        balanced_relation_start,
        profile.regions.balanced.class_relations,
        balanced_class_start,
    )
    .await?;
    Ok(())
}

async fn load_spread_object_relations(
    connection: &mut diesel_async::AsyncPgConnection,
    region: &str,
    count: u64,
    first_id: u64,
    first_relation_id: u64,
    relation_count: u64,
    _first_class_id: u64,
) -> std::result::Result<(), diesel::result::Error> {
    diesel::sql_query(format!(
        "WITH eligible AS (\n\
           SELECT row_number() OVER (ORDER BY relation.id) AS slot, relation.id AS relation_id,\n\
             source.first_object_id AS source_start, source.object_count AS source_count,\n\
             target.first_object_id AS target_start, target.object_count AS target_count\n\
           FROM scale_relation_plan relation\n\
           JOIN scale_class_plan source ON source.id = relation.from_class_id\n\
           JOIN scale_class_plan target ON target.id = relation.to_class_id\n\
           WHERE relation.region = '{region}' AND source.object_count > 0 AND target.object_count > 0\n\
             AND relation.id BETWEEN {first_relation_id} AND {last_relation_id}\n\
         ), eligible_count AS (SELECT count(*) AS count FROM eligible), generated AS (\n\
           SELECT n, 1 + ((n - 1) % eligible_count.count) AS slot,\n\
             ((n - 1) / eligible_count.count) AS round\n\
           FROM generate_series(1, {count}) AS n CROSS JOIN eligible_count\n\
         )\n\
         INSERT INTO hubuumobject_relation (\n\
           id, from_hubuum_object_id, to_hubuum_object_id, class_relation_id, revision\n\
         )\n\
         SELECT {first_id} + generated.n - 1,\n\
           eligible.source_start + (generated.round % eligible.source_count),\n\
           eligible.target_start + (((generated.round / eligible.source_count)\n\
             + (generated.round % eligible.source_count) * 17 + eligible.slot) % eligible.target_count),\n\
           eligible.relation_id, 1\n\
         FROM generated JOIN eligible USING (slot);",
        last_relation_id = first_relation_id + relation_count - 1,
    ))
    .execute(connection)
    .await?;
    Ok(())
}

async fn load_authorization(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
) -> std::result::Result<(), diesel::result::Error> {
    let memberships = profile.totals.memberships;
    let principals = profile.totals.principals;
    let groups = profile.totals.groups;
    let seed = profile.seed;
    connection.batch_execute(&format!(
        "WITH candidates AS (\n\
           SELECT 0 AS priority, 1 AS principal_id, 1 AS group_id\n\
           UNION ALL SELECT 0, 2, 2 UNION ALL SELECT 0, 3, 3\n\
           UNION ALL\n\
           SELECT 1, principal_id, group_id\n\
           FROM generate_series(4, {principals}) AS principal_id\n\
           CROSS JOIN generate_series(1, {groups}) AS group_id\n\
         ), deduped AS (\n\
           SELECT DISTINCT ON (principal_id, group_id) priority, principal_id, group_id\n\
           FROM candidates ORDER BY principal_id, group_id, priority\n\
         ), chosen AS (\n\
           SELECT principal_id, group_id FROM deduped\n\
           ORDER BY priority, ((principal_id::BIGINT * 1103515245 + group_id * 12345 + {seed}) % 2147483647)\n\
           LIMIT {memberships}\n\
         )\n\
         INSERT INTO group_memberships (principal_id, group_id, revision)\n\
         SELECT principal_id, group_id, 1 FROM chosen;\n\
         INSERT INTO group_membership_sources (principal_id, group_id, source, source_scope_id, source_key)\n\
         SELECT principal_id, group_id, 'app', 1, 'scale-benchmark' FROM group_memberships;"
    ))
    .await?;

    let grants = profile.totals.permission_grants;
    let collections = profile.totals.collections;
    diesel::sql_query(format!(
        "WITH candidates AS (\n\
           SELECT 0 AS priority, id AS collection_id, 2 AS group_id\n\
             FROM collections WHERE (id - 1) % 4 = 0\n\
           UNION ALL\n\
           SELECT 0, sparse.id, 3 FROM (\n\
             SELECT leaf.id FROM collections leaf\n\
             WHERE NOT EXISTS (\n\
               SELECT 1 FROM collections child WHERE child.parent_collection_id = leaf.id\n\
             )\n\
             ORDER BY ((leaf.id::BIGINT * 48271 + {seed}) % 2147483647)\n\
             LIMIT {sparse_collections}\n\
           ) sparse\n\
           UNION ALL\n\
           SELECT 1, collection_id, group_id\n\
             FROM generate_series(1, {collections}) AS collection_id\n\
             CROSS JOIN generate_series(4, {groups}) AS group_id\n\
         ), deduped AS (\n\
           SELECT DISTINCT ON (collection_id, group_id) priority, collection_id, group_id\n\
           FROM candidates ORDER BY collection_id, group_id, priority\n\
         ), chosen AS (\n\
           SELECT collection_id, group_id FROM deduped\n\
           ORDER BY priority, ((collection_id::BIGINT * 48271 + group_id * 69621 + {seed}) % 2147483647)\n\
           LIMIT {grants}\n\
         )\n\
         INSERT INTO permissions (\n\
           collection_id, group_id, has_read_collection, has_update_collection,\n\
           has_delete_collection, has_delegate_collection, has_create_class, has_read_class,\n\
           has_update_class, has_delete_class, has_create_object, has_read_object,\n\
           has_update_object, has_delete_object, has_create_class_relation,\n\
           has_read_class_relation, has_update_class_relation, has_delete_class_relation,\n\
           has_create_object_relation, has_read_object_relation, has_update_object_relation,\n\
           has_delete_object_relation, has_read_template, has_create_template,\n\
           has_update_template, has_delete_template, has_read_remote_target,\n\
           has_create_remote_target, has_update_remote_target, has_delete_remote_target,\n\
           has_execute_remote_target, has_read_audit, has_manage_event_subscription\n\
         )\n\
         SELECT collection_id, group_id, true, false, false, false, false, true,\n\
           false, false, false, true, false, false, false, true, false, false,\n\
           false, true, false, false, true, false, false, false, true, false,\n\
           false, false, false, true, false FROM chosen;",
        sparse_collections = (collections / 100).max(1),
    ))
    .execute(connection)
    .await?;
    Ok(())
}

async fn load_history_and_operations(
    connection: &mut diesel_async::AsyncPgConnection,
    profile: &ScaleProfile,
) -> std::result::Result<(), diesel::result::Error> {
    let overlays = &profile.overlays;
    diesel::sql_query(format!(
        "WITH selected AS (\n\
           SELECT object.*, CASE\n\
             WHEN object.id <= {heavy_resources} THEN {heavy_revisions}\n\
             WHEN object.id <= {heavy_resources} + {moderate_resources} THEN {moderate_revisions}\n\
             ELSE {typical_revisions} END AS revision_count\n\
           FROM hubuumobject object WHERE object.id <= {history_resources}\n\
         )\n\
         INSERT INTO hubuumobject_history (\n\
           id, name, collection_id, hubuum_class_id, data, description, created_at, updated_at,\n\
           revision, op, valid_from, valid_to, actor_id, actor_kind, initiator_user_id, task_id, history_id\n\
         )\n\
         SELECT selected.id, selected.name, selected.collection_id, selected.hubuum_class_id,\n\
           selected.data || jsonb_build_object('historical_revision', historical.revision), selected.description,\n\
           timestamp '2026-01-01 00:00:00', timestamp '2026-01-01 00:00:00' + historical.revision * interval '1 second',\n\
           historical.revision, CASE WHEN historical.revision = 1 THEN 'I' ELSE 'U' END,\n\
           timestamptz '2026-01-01 00:00:00+00' + historical.revision * interval '1 second',\n\
           CASE WHEN historical.revision = selected.revision_count THEN NULL\n\
             ELSE timestamptz '2026-01-01 00:00:00+00' + (historical.revision + 1) * interval '1 second' END,\n\
           1, 'user', 1, NULL, nextval('hubuumobject_history_seq')\n\
         FROM selected CROSS JOIN LATERAL\n\
           generate_series(1, selected.revision_count) AS historical(revision);",
        heavy_resources = overlays.history_heavy_resources,
        heavy_revisions = overlays.history_heavy_revisions,
        moderate_resources = overlays.history_moderate_resources,
        moderate_revisions = overlays.history_moderate_revisions,
        typical_revisions = overlays.history_typical_revisions,
        history_resources = overlays.history_resources,
    ))
    .execute(connection)
    .await?;
    diesel::sql_query(format!(
        "UPDATE hubuumobject object SET\n\
           revision = CASE WHEN object.id <= {heavy_resources} THEN {heavy_revisions}\n\
             WHEN object.id <= {heavy_resources} + {moderate_resources} THEN {moderate_revisions}\n\
             ELSE {typical_revisions} END,\n\
           data = object.data || jsonb_build_object('historical_revision',\n\
             CASE WHEN object.id <= {heavy_resources} THEN {heavy_revisions}\n\
               WHEN object.id <= {heavy_resources} + {moderate_resources} THEN {moderate_revisions}\n\
               ELSE {typical_revisions} END),\n\
           updated_at = timestamp '2026-01-01 00:00:00' +\n\
             (CASE WHEN object.id <= {heavy_resources} THEN {heavy_revisions}\n\
               WHEN object.id <= {heavy_resources} + {moderate_resources} THEN {moderate_revisions}\n\
               ELSE {typical_revisions} END) * interval '1 second'\n\
         WHERE object.id <= {history_resources};",
        heavy_resources = overlays.history_heavy_resources,
        heavy_revisions = overlays.history_heavy_revisions,
        moderate_resources = overlays.history_moderate_resources,
        moderate_revisions = overlays.history_moderate_revisions,
        typical_revisions = overlays.history_typical_revisions,
        history_resources = overlays.history_resources,
    ))
    .execute(connection)
    .await?;

    connection.batch_execute(&format!(
        "INSERT INTO computed_field_definitions (\n\
           id, class_id, visibility, owner_user_id, key, label, description, operation,\n\
           result_type, enabled, revision, semantics_version, created_by, updated_by\n\
         )\n\
         SELECT n, plan.id, 'shared', NULL, 'scale_value', 'Scale value',\n\
           'Deterministic benchmark computed field',\n\
           '{{\"type\":\"first_non_null\",\"paths\":[\"/ordinal\"]}}'::jsonb,\n\
           'integer', true, 1, 1, 1, 1\n\
         FROM generate_series(1, {computed_classes}) AS n\n\
         JOIN LATERAL (SELECT id FROM scale_class_plan WHERE region = 'balanced'\n\
           ORDER BY id OFFSET n - 1 LIMIT 1) plan ON true;\n\
         INSERT INTO class_computation_state (class_id, evaluation_revision, rebuild_status)\n\
         SELECT class_id, 1, CASE WHEN id % 10 = 0 THEN 'rebuilding' ELSE 'ready' END\n\
         FROM computed_field_definitions;\n\
         INSERT INTO object_computed_data (\n\
           object_id, class_id, evaluation_revision, source_data_sha256, values, errors\n\
         )\n\
         SELECT object.id, object.hubuum_class_id, 1, repeat('0', 64),\n\
           jsonb_build_object('shared', jsonb_build_object('scale_value', object.data->'ordinal')),\n\
           '{{}}'::jsonb\n\
         FROM hubuumobject object\n\
         JOIN computed_field_definitions field ON field.class_id = object.hubuum_class_id;",
        computed_classes = overlays.computed_classes,
    ))
    .await?;

    connection.batch_execute(&format!(
        "INSERT INTO export_templates (\n\
           id, collection_id, name, description, content_type, template, kind, scope_kind,\n\
           class_id, default_query, include, relation_context, default_missing_data_policy,\n\
           default_limits, revision\n\
         )\n\
         SELECT n, 1 + ((n - 1) % {collections}), format('scale-template-%s', n),\n\
           'Scale benchmark template', 'text/plain', '{{{{ object.name | default(\"\") }}}}',\n\
           'export', 'objects_in_class', 1 + ((n - 1) % {classes}), NULL, NULL, NULL, 'omit',\n\
           '{{\"max_objects\":1000}}'::jsonb, 1\n\
         FROM generate_series(1, {templates}) AS n;\n\
         INSERT INTO remote_targets (\n\
           id, collection_id, class_id, name, description, method, url_template,\n\
           headers_template, body_template, auth_config, allowed_subject_types, timeout_ms, enabled, revision\n\
         )\n\
         SELECT n, 1 + ((n - 1) % {collections}), 1 + ((n - 1) % {classes}),\n\
           format('scale-target-%s', n),\n\
           'Disabled scale benchmark target', 'post', 'https://disabled.invalid/scale',\n\
           '{{}}'::jsonb, NULL, '{{}}'::jsonb, '[\"object\"]'::jsonb, 1000, false, 1\n\
         FROM generate_series(1, {remote_targets}) AS n;\n\
         INSERT INTO event_sinks (id, name, kind, config, enabled, revision)\n\
         SELECT n, format('scale-sink-%s', n), 'webhook',\n\
           '{{\"url\":\"https://disabled.invalid/events\"}}'::jsonb, false, 1\n\
         FROM generate_series(1, {event_sinks}) AS n;\n\
         INSERT INTO event_subscriptions (\n\
           id, collection_id, sink_id, name, description, entity_types, actions, filter, routing, enabled, revision\n\
         )\n\
         SELECT n, 1 + ((n - 1) % {collections}), 1 + ((n - 1) % {event_sinks}),\n\
           format('scale-subscription-%s', n), 'Disabled scale benchmark subscription',\n\
           '[\"object\"]'::jsonb, '[\"created\",\"updated\"]'::jsonb, '{{}}'::jsonb, '{{}}'::jsonb, false, 1\n\
         FROM generate_series(1, {event_subscriptions}) AS n;",
        collections = profile.totals.collections,
        classes = profile.totals.classes,
        templates = overlays.templates,
        remote_targets = overlays.remote_targets,
        event_sinks = overlays.event_sinks,
        event_subscriptions = overlays.event_subscriptions,
    ))
    .await?;

    connection.batch_execute(&format!(
        "INSERT INTO tasks (\n\
           id, kind, status, submitted_by, request_payload, summary, total_items, processed_items,\n\
           success_items, failed_items, started_at, finished_at, created_at, updated_at, initiator_user_id\n\
         )\n\
         SELECT n, CASE n % 4 WHEN 0 THEN 'import' WHEN 1 THEN 'export'\n\
           WHEN 2 THEN 'reindex' ELSE 'remote_call' END,\n\
           CASE n % 4 WHEN 0 THEN 'succeeded' WHEN 1 THEN 'failed'\n\
           WHEN 2 THEN 'partially_succeeded' ELSE 'cancelled' END,\n\
           1 + ((n - 1) % {principals}), '{{}}'::jsonb, 'Scale benchmark terminal task',\n\
           10, 10, CASE WHEN n % 4 = 1 THEN 9 ELSE 10 END, CASE WHEN n % 4 = 1 THEN 1 ELSE 0 END,\n\
           timestamp '2026-01-01' + n * interval '1 second',\n\
           timestamp '2026-01-01' + (n + 1) * interval '1 second',\n\
           timestamp '2026-01-01' + n * interval '1 second',\n\
           timestamp '2026-01-01' + (n + 1) * interval '1 second', 1\n\
         FROM generate_series(1, {tasks}) AS n;\n\
         INSERT INTO events (\n\
           id, event_id, occurred_at, entity_type, entity_id, entity_name, collection_id, action,\n\
           actor_user_id, actor_kind, summary, metadata, schema_version, dispatched_at,\n\
           initiator_user_id, before_revision, after_revision\n\
         )\n\
         SELECT n, md5(format('scale-event-%s-{seed}', n))::uuid,\n\
           timestamp '2026-01-01' + n * interval '1 second',\n\
           CASE n % 3 WHEN 0 THEN 'object' WHEN 1 THEN 'class' ELSE 'collection' END,\n\
           1 + ((n - 1) % {objects}), format('scale-event-resource-%s', n),\n\
           1 + ((n - 1) % {collections}),\n\
           CASE n % 3 WHEN 0 THEN 'created' WHEN 1 THEN 'updated' ELSE 'deleted' END,\n\
           1 + ((n - 1) % {principals}), 'user', 'Sanitized scale benchmark audit event',\n\
           jsonb_build_object('benchmark', true, 'bucket', n % 100), 1,\n\
           timestamp '2026-01-01' + (n + 1) * interval '1 second', 1,\n\
           CASE WHEN n % 3 IN (1, 2) THEN 1 ELSE NULL END,\n\
           CASE n % 3 WHEN 0 THEN 1 WHEN 1 THEN 2 ELSE NULL END\n\
         FROM generate_series(1, {events}) AS n;",
        principals = profile.totals.principals,
        tasks = overlays.terminal_tasks,
        events = overlays.audit_events,
        seed = profile.seed,
        objects = profile.totals.objects,
        collections = profile.totals.collections,
    ))
    .await?;
    connection
        .batch_execute(&format!(
            "INSERT INTO event_deliveries (
               id, event_id, subscription_id, status, attempts, next_attempt_at,
               last_error, locked_until, claim_token, created_at, updated_at
             )
             SELECT n, 1 + ((n - 1) % {events}),
               1 + ((n - 1) % {subscriptions}),
               CASE n % 3 WHEN 0 THEN 'succeeded' WHEN 1 THEN 'failed' ELSE 'dead' END,
               1 + (n % 5), timestamp '2026-01-02',
               CASE WHEN n % 3 = 0 THEN NULL ELSE 'sanitized benchmark delivery outcome' END,
               NULL, NULL, timestamp '2026-01-01' + n * interval '1 second',
               timestamp '2026-01-01' + (n + 1) * interval '1 second'
             FROM generate_series(1, {deliveries}) AS n;",
            events = overlays.audit_events,
            subscriptions = overlays.event_subscriptions,
            deliveries = overlays.event_deliveries,
        ))
        .await?;
    Ok(())
}

async fn reset_sequences(
    connection: &mut diesel_async::AsyncPgConnection,
) -> std::result::Result<(), diesel::result::Error> {
    connection
        .batch_execute(
            "SELECT setval(pg_get_serial_sequence('principals', 'id'), (SELECT max(id) FROM principals), true);\n\
             SELECT setval(pg_get_serial_sequence('groups', 'id'), (SELECT max(id) FROM groups), true);\n\
             SELECT setval(pg_get_serial_sequence('collections', 'id'), (SELECT max(id) FROM collections), true);\n\
             SELECT setval(pg_get_serial_sequence('hubuumclass', 'id'), (SELECT max(id) FROM hubuumclass), true);\n\
             SELECT setval(pg_get_serial_sequence('hubuumobject', 'id'), (SELECT max(id) FROM hubuumobject), true);\n\
             SELECT setval(pg_get_serial_sequence('hubuumclass_relation', 'id'), (SELECT max(id) FROM hubuumclass_relation), true);\n\
             SELECT setval(pg_get_serial_sequence('hubuumobject_relation', 'id'), (SELECT max(id) FROM hubuumobject_relation), true);\n\
             SELECT setval(pg_get_serial_sequence('permissions', 'id'), (SELECT max(id) FROM permissions), true);\n\
             SELECT setval(pg_get_serial_sequence('computed_field_definitions', 'id'), (SELECT max(id) FROM computed_field_definitions), true);\n\
             SELECT setval(pg_get_serial_sequence('export_templates', 'id'), (SELECT max(id) FROM export_templates), true);\n\
             SELECT setval(pg_get_serial_sequence('remote_targets', 'id'), (SELECT max(id) FROM remote_targets), true);\n\
             SELECT setval(pg_get_serial_sequence('event_sinks', 'id'), (SELECT max(id) FROM event_sinks), true);\n\
             SELECT setval(pg_get_serial_sequence('event_subscriptions', 'id'), (SELECT max(id) FROM event_subscriptions), true);\n\
             SELECT setval(pg_get_serial_sequence('event_deliveries', 'id'), (SELECT max(id) FROM event_deliveries), true);\n\
             SELECT setval(pg_get_serial_sequence('tasks', 'id'), (SELECT max(id) FROM tasks), true);\n\
             SELECT setval(pg_get_serial_sequence('events', 'id'), (SELECT max(id) FROM events), true);",
        )
        .await
}

async fn verify_loaded_dataset(
    pool: &PostgresPool,
    profile: &ScaleProfile,
    manifest: &DatasetManifest,
) -> Result<()> {
    manifest.validate(profile)?;
    for (label, query, expected) in [
        (
            "collections",
            "SELECT count(*) AS value FROM collections",
            profile.totals.collections,
        ),
        (
            "classes",
            "SELECT count(*) AS value FROM hubuumclass",
            profile.totals.classes,
        ),
        (
            "objects",
            "SELECT count(*) AS value FROM hubuumobject",
            profile.totals.objects,
        ),
        (
            "class relations",
            "SELECT count(*) AS value FROM hubuumclass_relation",
            profile.totals.class_relations,
        ),
        (
            "object relations",
            "SELECT count(*) AS value FROM hubuumobject_relation",
            profile.totals.object_relations,
        ),
        (
            "principals",
            "SELECT count(*) AS value FROM principals",
            profile.totals.principals,
        ),
        (
            "groups",
            "SELECT count(*) AS value FROM groups",
            profile.totals.groups,
        ),
        (
            "memberships",
            "SELECT count(*) AS value FROM group_memberships",
            profile.totals.memberships,
        ),
        (
            "permission grants",
            "SELECT count(*) AS value FROM permissions",
            profile.totals.permission_grants,
        ),
        (
            "computed field definitions",
            "SELECT count(*) AS value FROM computed_field_definitions",
            profile.overlays.computed_classes,
        ),
        (
            "export templates",
            "SELECT count(*) AS value FROM export_templates",
            profile.overlays.templates,
        ),
        (
            "remote targets",
            "SELECT count(*) AS value FROM remote_targets",
            profile.overlays.remote_targets,
        ),
        (
            "event sinks",
            "SELECT count(*) AS value FROM event_sinks",
            profile.overlays.event_sinks,
        ),
        (
            "event subscriptions",
            "SELECT count(*) AS value FROM event_subscriptions",
            profile.overlays.event_subscriptions,
        ),
        (
            "event deliveries",
            "SELECT count(*) AS value FROM event_deliveries",
            profile.overlays.event_deliveries,
        ),
        (
            "terminal tasks",
            "SELECT count(*) AS value FROM tasks",
            profile.overlays.terminal_tasks,
        ),
        (
            "audit events",
            "SELECT count(*) AS value FROM events",
            profile.overlays.audit_events,
        ),
    ] {
        let actual = scalar(pool, query).await? as u64;
        if actual != expected {
            return Err(invalid_data(format!(
                "loaded {label} count is {actual}, expected {expected}"
            )));
        }
    }

    let objects = distribution(
        pool,
        "WITH counts AS (\n\
           SELECT class.id, count(object.id)::BIGINT AS value\n\
           FROM hubuumclass class LEFT JOIN hubuumobject object ON object.hubuum_class_id = class.id\n\
           GROUP BY class.id\n\
         ) SELECT max(value)::BIGINT AS maximum,\n\
           percentile_disc(0.5) WITHIN GROUP (ORDER BY value)::BIGINT AS median FROM counts",
    )
    .await?;
    if objects.maximum as u64 != manifest.objects_per_class.maximum
        || objects.median as u64 != manifest.objects_per_class.median
    {
        return Err(invalid_data(format!(
            "loaded object distribution drifted (max={}, median={})",
            objects.maximum, objects.median
        )));
    }
    let max_degree = scalar(
        pool,
        "WITH degree AS (\n\
           SELECT object_id, count(*)::BIGINT AS value FROM (\n\
             SELECT from_hubuum_object_id AS object_id FROM hubuumobject_relation\n\
             UNION ALL SELECT to_hubuum_object_id FROM hubuumobject_relation\n\
           ) endpoints GROUP BY object_id\n\
         ) SELECT coalesce(max(value), 0)::BIGINT AS value FROM degree",
    )
    .await? as u64;
    if max_degree < profile.invariants.minimum_hub_object_degree {
        return Err(invalid_data(format!(
            "loaded hub degree {max_degree} is below required {}",
            profile.invariants.minimum_hub_object_degree
        )));
    }
    let concentrated = scalar(
        pool,
        "SELECT coalesce(max(value), 0)::BIGINT AS value FROM (\n\
           SELECT count(*)::BIGINT AS value FROM hubuumobject_relation GROUP BY class_relation_id\n\
         ) counts",
    )
    .await? as u64;
    if concentrated < profile.invariants.minimum_concentrated_relation_count {
        return Err(invalid_data(
            "loaded relation concentration invariant failed",
        ));
    }
    for (name, shape) in &manifest.relation_shapes {
        let actual = scalar(
            pool,
            &format!(
                "SELECT count(*)::BIGINT AS value FROM hubuumobject_relation WHERE class_relation_id = {}",
                shape.class_relation_id
            ),
        )
        .await? as u64;
        if actual != shape.edge_count {
            return Err(invalid_data(format!(
                "loaded relation shape '{name}' has {actual} edges, expected {}",
                shape.edge_count
            )));
        }
    }
    let history_max = scalar(
        pool,
        "SELECT coalesce(max(value), 0)::BIGINT AS value FROM (\n\
           SELECT count(*)::BIGINT AS value FROM hubuumobject_history GROUP BY id\n\
         ) counts",
    )
    .await? as u64;
    if history_max < profile.invariants.minimum_heavy_history_revisions {
        return Err(invalid_data("loaded history-heavy invariant failed"));
    }
    let sparse_visibility = ratio(
        pool,
        "SELECT 100.0 * count(DISTINCT object.id)::DOUBLE PRECISION /\n\
           (SELECT count(*) FROM hubuumobject)::DOUBLE PRECISION AS value\n\
         FROM hubuumobject object JOIN permissions permission USING (collection_id)\n\
         WHERE permission.group_id = 3 AND permission.has_read_object",
    )
    .await?;
    if sparse_visibility > profile.invariants.maximum_sparse_visibility_percent + 0.01 {
        return Err(invalid_data(format!(
            "loaded sparse visibility {sparse_visibility:.3}% exceeds the profile limit"
        )));
    }
    Ok(())
}

#[async_trait]
impl ScaleBenchmarkBackend for PostgresScaleBackend {
    type ResourceBaseline = PostgresResourceBaseline;

    fn name(&self) -> &'static str {
        "postgres"
    }

    fn server_environment(&self) -> BTreeMap<String, String> {
        BTreeMap::from([("HUBUUM_DATABASE_URL".to_string(), self.database_url.clone())])
    }

    fn benchmark_principals(&self) -> Vec<BenchmarkPrincipal> {
        [
            ("admin", "scale-admin"),
            ("tenant", "scale-tenant"),
            ("sparse", "scale-sparse"),
        ]
        .into_iter()
        .map(|(role, username)| {
            BenchmarkPrincipal::new(role, username, BENCHMARK_PASSWORD)
                .expect("static PostgreSQL benchmark principal must be valid")
        })
        .collect()
    }

    async fn load_dataset(&self, profile: &ScaleProfile) -> Result<LoadReport> {
        load_dataset_with_pool(profile, &self.pool).await
    }

    async fn verify_dataset(
        &self,
        profile: &ScaleProfile,
        manifest: &DatasetManifest,
    ) -> Result<()> {
        verify_loaded_dataset(&self.pool, profile, manifest).await
    }

    async fn prepare_measurement(&self) -> Result<BackendPreparation> {
        let database_fresh = scalar(&self.pool, "SELECT count(*) AS value FROM tokens")
            .await
            .map_err(|error| operation_error("checking whether the database is fresh", error))?
            == 0;
        let version = text_scalar(
            &self.pool,
            "SELECT current_setting('server_version') AS value",
        )
        .await
        .map_err(|error| operation_error("reading the PostgreSQL version", error))?;
        let settings = load_database_settings(&self.pool)
            .await
            .map_err(|error| operation_error("reading PostgreSQL settings", error))?;
        let sparse_collection_ids = load_sparse_collection_ids(&self.pool)
            .await
            .map_err(|error| operation_error("reading sparse collection identifiers", error))?;
        Ok(BackendPreparation {
            identity: BackendIdentity {
                name: self.name().to_string(),
                version,
                settings,
            },
            database_fresh,
            sparse_collection_ids,
        })
    }

    async fn mark_computed_ready(&self) -> Result<()> {
        with_connection(
            &self.pool,
            async |connection| -> std::result::Result<_, diesel::result::Error> {
                diesel::sql_query(
                    "UPDATE class_computation_state SET rebuild_status = 'ready'\n\
                     WHERE rebuild_status = 'rebuilding'",
                )
                .execute(connection)
                .await
                .map(|_| ())
            },
        )
        .await
        .map_err(storage_error)
    }

    async fn begin_resource_measurement(&self) -> Result<Self::ResourceBaseline> {
        let (cpu_seconds, resident_bytes) = postgres_process_resources();
        Ok(PostgresResourceBaseline {
            wal_position: current_wal(&self.pool).await?,
            cpu_seconds,
            resident_bytes,
        })
    }

    async fn finish_resource_measurement(
        &self,
        baseline: Self::ResourceBaseline,
    ) -> Result<BackendResourceReport> {
        let database = with_connection(
            &self.pool,
            async |connection| -> std::result::Result<_, diesel::result::Error> {
                diesel::sql_query(
                    "SELECT pg_database_size(current_database())::BIGINT AS database_bytes,\n\
                       coalesce(sum(pg_relation_size(oid)), 0)::BIGINT AS table_bytes,\n\
                       coalesce(sum(pg_indexes_size(oid)), 0)::BIGINT AS index_bytes\n\
                     FROM pg_class\n\
                     WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p')",
                )
                .get_result::<DatabaseResourceRow>(connection)
                .await
            },
        )
        .await
        .map_err(storage_error)?;
        let wal_end = current_wal(&self.pool).await?;
        let (cpu_after, resident_after) = postgres_process_resources();
        Ok(BackendResourceReport {
            cpu_seconds: cpu_after
                .zip(baseline.cpu_seconds)
                .map(|(after, before)| (after - before).max(0.0)),
            peak_resident_bytes: resident_after
                .into_iter()
                .chain(baseline.resident_bytes)
                .max(),
            storage_bytes: database.database_bytes.max(0) as u64,
            data_bytes: Some(database.table_bytes.max(0) as u64),
            index_bytes: Some(database.index_bytes.max(0) as u64),
            write_ahead_bytes: Some(wal_end.saturating_sub(baseline.wal_position) as u64),
            metrics: BTreeMap::new(),
        })
    }
}

async fn load_sparse_collection_ids(
    pool: &PostgresPool,
) -> Result<std::collections::BTreeSet<i64>> {
    let rows = with_connection(
        pool,
        async |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(
                "SELECT collection_id FROM permissions\n\
                 WHERE group_id = 3 AND has_read_object\n\
                 ORDER BY collection_id",
            )
            .get_results::<CollectionIdRow>(connection)
            .await
        },
    )
    .await
    .map_err(storage_error)?;
    Ok(rows
        .into_iter()
        .map(|row| i64::from(row.collection_id))
        .collect())
}

async fn load_database_settings(pool: &PostgresPool) -> Result<BTreeMap<String, String>> {
    let rows = with_connection(
        pool,
        async |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(
                "SELECT name || '=' || setting || coalesce(unit, '') AS value FROM pg_settings\n\
                 WHERE name IN (\n\
                   'max_connections', 'shared_buffers', 'work_mem', 'maintenance_work_mem',\n\
                   'effective_cache_size', 'random_page_cost', 'max_parallel_workers_per_gather'\n\
                 ) ORDER BY name",
            )
            .load::<TextRow>(connection)
            .await
        },
    )
    .await
    .map_err(storage_error)?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            row.value
                .split_once('=')
                .map(|(name, value)| (name.to_string(), value.to_string()))
        })
        .collect())
}

async fn current_wal(pool: &PostgresPool) -> Result<i64> {
    scalar(
        pool,
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::BIGINT AS value",
    )
    .await
}

async fn text_scalar(pool: &PostgresPool, query: &str) -> Result<String> {
    with_connection(
        pool,
        async move |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(query)
                .get_result::<TextRow>(connection)
                .await
                .map(|row| row.value)
        },
    )
    .await
    .map_err(storage_error)
}

fn postgres_process_resources() -> (Option<f64>, Option<u64>) {
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );
    let mut found = false;
    let mut cpu_millis = 0_u64;
    let mut resident_bytes = 0_u64;
    for process in system.processes().values() {
        if process.name().to_string_lossy().contains("postgres") {
            found = true;
            cpu_millis = cpu_millis.saturating_add(process.accumulated_cpu_time());
            resident_bytes = resident_bytes.saturating_add(process.memory());
        }
    }
    if found {
        (Some(cpu_millis as f64 / 1_000.0), Some(resident_bytes))
    } else {
        (None, None)
    }
}

async fn scalar(pool: &PostgresPool, query: &str) -> Result<i64> {
    with_connection(
        pool,
        async move |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(query)
                .get_result::<CountRow>(connection)
                .await
                .map(|row| row.value)
        },
    )
    .await
    .map_err(storage_error)
}

async fn distribution(pool: &PostgresPool, query: &str) -> Result<DistributionRow> {
    with_connection(
        pool,
        async move |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(query).get_result(connection).await
        },
    )
    .await
    .map_err(storage_error)
}

async fn ratio(pool: &PostgresPool, query: &str) -> Result<f64> {
    with_connection(
        pool,
        async move |connection| -> std::result::Result<_, diesel::result::Error> {
            diesel::sql_query(query)
                .get_result::<RatioRow>(connection)
                .await
                .map(|row| row.value)
        },
    )
    .await
    .map_err(storage_error)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn storage_error(error: impl std::fmt::Display) -> Error {
    invalid_data(format!("scale benchmark storage operation failed: {error}"))
}

fn operation_error(operation: &str, error: impl std::fmt::Display) -> Error {
    invalid_data(format!(
        "PostgreSQL scale benchmark failed while {operation}: {error}"
    ))
}
