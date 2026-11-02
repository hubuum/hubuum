use diesel::{
    QueryableByName, sql_query,
    sql_types::{Array, BigInt, Integer, Nullable, Text, Timestamp, Timestamptz},
};
use hubuum_domain::{ClassRelationId, ObjectId};
use hubuum_query::CursorValue;
use hubuum_storage_core::{
    StorageClassRelation, StorageHistoricalRelation, StorageHistoryEndpointSnapshot,
    StorageHistoryResource, StorageObjectRelation, StorageRelationHistoryEntry,
    StorageRelationHistoryQuery, StorageRelationHistoryRecord, StorageRelationHistoryResource,
    StorageRelationHistorySelection,
};

use super::*;

#[cfg(test)]
mod tests;

#[derive(QueryableByName)]
struct MetadataRow {
    #[diesel(sql_type = Text)]
    op: String,
    #[diesel(sql_type = Timestamptz)]
    valid_from: DateTime<Utc>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    valid_to: Option<DateTime<Utc>>,
    #[diesel(sql_type = BigInt)]
    history_id: i64,
    #[diesel(sql_type = BigInt)]
    revision: PostgresRevision,
    #[diesel(sql_type = Nullable<Integer>)]
    actor_id: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    actor_kind: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    initiator_user_id: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    task_id: Option<i32>,
}

impl MetadataRow {
    fn project(self) -> Result<StorageHistoryMetadata, PostgresStorageError> {
        Ok(project_history_metadata(
            history_operation(&self.op)?,
            self.valid_from,
            self.valid_to,
            HistoryRecordId::new(self.history_id)?,
            self.revision.into_domain(),
        )?
        .actor(
            self.actor_id.map(PrincipalId::new).transpose()?,
            self.actor_kind,
        )
        .initiator_principal_id(self.initiator_user_id.map(PrincipalId::new).transpose()?)
        .task_id(self.task_id.map(TaskId::new).transpose()?))
    }
}

#[derive(QueryableByName)]
struct RelationRow {
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Integer)]
    from_id: i32,
    #[diesel(sql_type = Integer)]
    to_id: i32,
    #[diesel(sql_type = Nullable<Integer>)]
    class_relation_id: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    forward_template_alias: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    forward_label: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    reverse_label: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    reverse_template_alias: Option<String>,
    #[diesel(sql_type = Nullable<Integer>)]
    from_max_relations: Option<i32>,
    #[diesel(sql_type = Nullable<Integer>)]
    to_max_relations: Option<i32>,
    #[diesel(sql_type = Timestamp)]
    created_at: NaiveDateTime,
    #[diesel(sql_type = Timestamp)]
    updated_at: NaiveDateTime,
    #[diesel(embed)]
    metadata: MetadataRow,
}

impl RelationRow {
    fn project(self) -> Result<StorageRelationHistoryRecord, PostgresStorageError> {
        let metadata = record_metadata(
            self.id,
            self.created_at,
            self.updated_at,
            self.metadata.revision,
        )?;
        let relation = if let Some(id) = self.class_relation_id {
            StorageHistoricalRelation::Object(
                StorageObjectRelation::try_new(
                    metadata,
                    ObjectId::new(self.from_id)?,
                    ObjectId::new(self.to_id)?,
                    ClassRelationId::new(id)?,
                )
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value(
                        "object relationship history",
                        error,
                    )
                })?,
            )
        } else {
            let forward = crate::operations::relation::parse_label(self.forward_label)?;
            let reverse = crate::operations::relation::parse_label(self.reverse_label)?;
            StorageHistoricalRelation::Class(
                StorageClassRelation::try_new(
                    metadata,
                    ClassId::new(self.from_id)?,
                    ClassId::new(self.to_id)?,
                )
                .and_then(|row| {
                    row.with_labels(forward, reverse).try_with_template_aliases(
                        self.forward_template_alias,
                        self.reverse_template_alias,
                    )
                })
                .and_then(|row| {
                    row.try_with_relation_limits(self.from_max_relations, self.to_max_relations)
                })
                .map_err(|error| {
                    PostgresStorageError::invalid_persisted_value(
                        "class relationship history",
                        error,
                    )
                })?,
            )
        };
        StorageRelationHistoryRecord::try_new(relation, self.metadata.project()?).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("relationship history", error)
        })
    }
}

#[derive(QueryableByName)]
struct EndpointRow {
    #[diesel(sql_type = BigInt)]
    ordinal: i64,
    #[diesel(sql_type = Integer)]
    id: i32,
    #[diesel(sql_type = Nullable<Integer>)]
    class_id: Option<i32>,
    #[diesel(sql_type = Integer)]
    collection_id: i32,
    #[diesel(sql_type = Text)]
    name: String,
    #[diesel(embed)]
    metadata: MetadataRow,
}

impl EndpointRow {
    fn project(self) -> Result<(usize, StorageHistoryEndpointSnapshot), PostgresStorageError> {
        let resource = match self.class_id {
            Some(class_id) => StorageHistoryResource::Object {
                class_id: ClassId::new(class_id)?,
                object_id: ObjectId::new(self.id)?,
            },
            None => StorageHistoryResource::Class(ClassId::new(self.id)?),
        };
        let ordinal = usize::try_from(self.ordinal - 1).map_err(|error| {
            PostgresStorageError::invalid_persisted_value("endpoint ordinal", error)
        })?;
        let snapshot = StorageHistoryEndpointSnapshot::try_new(
            resource,
            CollectionId::new(self.collection_id)?,
            self.name,
            self.metadata.project()?,
        )
        .map_err(|error| {
            PostgresStorageError::invalid_persisted_value("historical endpoint", error)
        })?;
        Ok((ordinal, snapshot))
    }
}

struct RelationTables {
    edges: &'static str,
    endpoints: &'static str,
    from: &'static str,
    to: &'static str,
    edge_projection: &'static str,
    class_projection: &'static str,
}

impl RelationTables {
    fn for_resource(resource: StorageRelationHistoryResource) -> (Self, i32) {
        match resource {
            StorageRelationHistoryResource::Class(id) => (
                Self {
                    edges: "hubuumclass_relation_history",
                    endpoints: "hubuumclass_history",
                    from: "from_hubuum_class_id",
                    to: "to_hubuum_class_id",
                    edge_projection: "NULL::int4 AS class_relation_id, forward_label, reverse_label, forward_template_alias, reverse_template_alias, from_max_relations, to_max_relations",
                    class_projection: "NULL::int4 AS class_id",
                },
                id.id(),
            ),
            StorageRelationHistoryResource::Object(id) => (
                Self {
                    edges: "hubuumobject_relation_history",
                    endpoints: "hubuumobject_history",
                    from: "from_hubuum_object_id",
                    to: "to_hubuum_object_id",
                    edge_projection: "class_relation_id, NULL::text AS forward_label, NULL::text AS reverse_label, NULL::text AS forward_template_alias, NULL::text AS reverse_template_alias, NULL::int4 AS from_max_relations, NULL::int4 AS to_max_relations",
                    class_projection: "hubuum_class_id AS class_id",
                },
                id.id(),
            ),
        }
    }

    fn page_sql(&self, descending: bool, has_cursor: bool) -> String {
        let order = if descending { "DESC" } else { "ASC" };
        let comparison = match (descending, has_cursor) {
            (true, true) => "<",
            (false, true) => ">",
            (true, false) => "<=",
            (false, false) => ">=",
        };
        let lower_comparison = if descending { ">=" } else { comparison };
        let upper_comparison = if descending { comparison } else { "<=" };
        let lower = if descending {
            "0"
        } else {
            "COALESCE($3::bigint, 0)"
        };
        let upper = if descending {
            format!("COALESCE($3::bigint, {})", i64::MAX)
        } else {
            i64::MAX.to_string()
        };
        // Canonical distinct endpoints make these branches disjoint. Each branch
        // seeks its endpoint/history-id index and contributes at most one page.
        // Two tuple bounds keep the endpoint in the ordering key. A scalar
        // endpoint equality lets PostgreSQL choose the global history-ID index
        // and filter arbitrarily many rows belonging to other endpoints.
        let branch = |endpoint| {
            format!(
            "(SELECT id, {from} AS from_id, {to} AS to_id, {projection}, created_at, updated_at, {metadata}
             FROM {table} WHERE {validity}
             AND ({endpoint}, history_id) {lower_comparison} ($1, {lower})
             AND ({endpoint}, history_id) {upper_comparison} ($1, {upper})
             ORDER BY {endpoint} {order}, history_id {order} LIMIT $4)",
            from=self.from, to=self.to, projection=self.edge_projection, metadata=METADATA,
            table=self.edges, validity=VALIDITY,
        )
        };
        format!(
            "{} UNION ALL {} ORDER BY history_id {order} LIMIT $4",
            branch(self.from),
            branch(self.to)
        )
    }

    fn endpoint_sql(&self) -> String {
        // Seek the latest version first, including tombstones. Checking validity
        // afterward preserves gaps without scanning thousands of older versions.
        format!(
            "SELECT requested.ordinal, endpoint.* FROM unnest($1::int4[], $2::timestamptz[]) WITH ORDINALITY AS requested(id, at, ordinal)
             CROSS JOIN LATERAL (SELECT id, {class}, collection_id, name, {metadata} FROM {table}
               WHERE id = requested.id AND valid_from <= requested.at
               ORDER BY valid_from DESC, history_id DESC LIMIT 1) endpoint
             WHERE endpoint.op <> 'D' AND (endpoint.valid_to IS NULL OR requested.at < endpoint.valid_to)
             ORDER BY requested.ordinal",
            class=self.class_projection, metadata=METADATA, table=self.endpoints,
        )
    }
}

const METADATA: &str = "op, valid_from, valid_to, history_id, revision, actor_id, actor_kind, initiator_user_id, task_id";
const VALIDITY: &str = "($2::timestamptz IS NULL OR (op <> 'D' AND tstzrange(valid_from, valid_to, '[)') @> $2::timestamptz))";

fn timestamp_floor(at: DateTime<Utc>) -> DateTime<Utc> {
    at - chrono::Duration::nanoseconds(i64::from(at.timestamp_subsec_nanos() % 1_000))
}

pub(crate) async fn list_relation_history(
    runtime: &PostgresRuntime,
    query: StorageRelationHistoryQuery,
) -> Result<StoragePage<StorageRelationHistoryEntry>, PostgresStorageError> {
    let options = query.options();
    let (tables, root) = RelationTables::for_resource(query.resource());
    let at = match query.selection() {
        StorageRelationHistorySelection::Changes => None,
        StorageRelationHistorySelection::At(at) => Some(timestamp_floor(at)),
    };
    let boundary = options
        .cursor_values(options.sort())
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?
        .map(|values| match values[0] {
            CursorValue::Integer(id) => id,
            _ => unreachable!("validated query"),
        });
    let limit = i64::try_from(options.limit().expect("validated limit"))
        .map_err(|error| PostgresStorageError::invalid_input(error.to_string()))?;
    runtime.with_read_only_snapshot(async |connection| {
        #[derive(QueryableByName)]
        struct Count { #[diesel(sql_type = BigInt)] count: i64 }
        let total = if options.include_total() {
            Some(sql_query(format!("SELECT count(*) AS count FROM {} WHERE ({} = $1 OR {} = $1) AND {VALIDITY}", tables.edges, tables.from, tables.to))
                .bind::<Integer, _>(root).bind::<Nullable<Timestamptz>, _>(at)
                .get_result::<Count>(connection).await?.count)
        } else { None };
        let records = sql_query(tables.page_sql(options.sort()[0].descending, boundary.is_some()))
            .bind::<Integer, _>(root).bind::<Nullable<Timestamptz>, _>(at)
            .bind::<Nullable<BigInt>, _>(boundary).bind::<BigInt, _>(limit)
            .load::<RelationRow>(connection).await?.into_iter().map(RelationRow::project).collect::<Result<Vec<_>, _>>()?;
        let mut ids = Vec::with_capacity(2 * records.len());
        let mut times = Vec::with_capacity(2 * records.len());
        for record in &records {
            for endpoint in record.relation().endpoints() {
                ids.push(match endpoint { StorageRelationHistoryResource::Class(id) => id.id(), StorageRelationHistoryResource::Object(id) => id.id() });
                times.push(at.unwrap_or_else(|| record.metadata().valid_from()));
            }
        }
        let mut endpoints = vec![None; ids.len()];
        if !ids.is_empty() {
            for row in sql_query(tables.endpoint_sql()).bind::<Array<Integer>, _>(ids).bind::<Array<Timestamptz>, _>(times).load::<EndpointRow>(connection).await? {
                let (ordinal, snapshot) = row.project()?;
                let slot = endpoints.get_mut(ordinal).ok_or_else(|| PostgresStorageError::internal("Unexpected endpoint ordinal"))?;
                *slot = Some(snapshot);
            }
        }
        let rows = records.into_iter().enumerate().map(|(index, record)| {
            StorageRelationHistoryEntry::try_new(record, query.selection(), endpoints[2 * index].take(), endpoints[2 * index + 1].take())
                .map_err(|error| PostgresStorageError::invalid_persisted_value("historical relationship context", error))
        }).collect::<Result<Vec<_>, _>>()?;
        crate::persisted_page(rows, total)
    }).await
}
