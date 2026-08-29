use std::collections::HashMap;

use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Bool, Integer, Text};
use diesel::{ExpressionMethods, QueryDsl, Queryable, QueryableByName};
use diesel_async::RunQueryDsl;
use hubuum_domain::{ClassId, CollectionId};
use hubuum_storage_core::{
    StorageAuthorizationPermission, StorageClassWithCollection, StorageCollection, StorageObject,
    StorageResourceScope, StorageUnifiedSearchQuery,
};

use crate::revision::record_metadata;
use crate::{PostgresRevision, PostgresRuntime, PostgresStorageError};

const COLLECTION_SEARCH_SQL: &str = r#"
    SELECT c.id, c.name, c.description, c.created_at, c.updated_at,
           c.parent_collection_id, c.revision
    FROM collections c
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN lower(c.name) = lower($1) THEN 0
            WHEN lower(c.name) LIKE lower($1) || '%' THEN 1
            WHEN lower(c.name) LIKE '%' || lower($1) || '%' THEN 2
            WHEN lower(c.description) LIKE '%' || lower($1) || '%' THEN 3
            ELSE 4
        END AS search_rank
    ) ranked
    WHERE (c.name ILIKE '%' || $1 || '%' OR c.description ILIKE '%' || $1 || '%')
      AND ($2 OR EXISTS (
          SELECT 1
          FROM permissions p
          JOIN group_memberships gm ON gm.group_id = p.group_id
          JOIN collection_closure cc ON cc.ancestor_collection_id = p.collection_id
          WHERE gm.principal_id = $3
            AND cc.descendant_collection_id = c.id
            AND p.has_read_collection
      ))
      AND ($4 OR c.id = ANY($5))
      AND ($6 OR (ranked.search_rank, lower(c.name), c.id) > ($7, $8, $9))
    ORDER BY ranked.search_rank, lower(c.name), c.id
    LIMIT $10
"#;

const CLASS_SEARCH_SQL: &str = r#"
    SELECT c.id, c.name, c.collection_id, c.json_schema, c.validate_schema,
           c.description, c.created_at, c.updated_at, c.revision
    FROM hubuumclass c
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN lower(c.name) = lower($1) THEN 0
            WHEN lower(c.name) LIKE lower($1) || '%' THEN 1
            WHEN lower(c.name) LIKE '%' || lower($1) || '%' THEN 2
            WHEN lower(c.description) LIKE '%' || lower($1) || '%' THEN 3
            WHEN $2 AND lower(coalesce(c.json_schema::text, ''))
                LIKE '%' || lower($1) || '%' THEN 4
            ELSE 5
        END AS search_rank
    ) ranked
    WHERE (
          c.name ILIKE '%' || $1 || '%'
          OR c.description ILIKE '%' || $1 || '%'
          OR ($2 AND lower(coalesce(c.json_schema::text, '')) LIKE '%' || lower($1) || '%')
      )
      AND ($3 OR EXISTS (
          SELECT 1
          FROM permissions p
          JOIN group_memberships gm ON gm.group_id = p.group_id
          JOIN collection_closure cc ON cc.ancestor_collection_id = p.collection_id
          WHERE gm.principal_id = $4
            AND cc.descendant_collection_id = c.collection_id
            AND p.has_read_collection
            AND p.has_read_class
      ))
      AND ($5 OR c.collection_id = ANY($6) OR c.id = ANY($7))
      AND ($8 OR (ranked.search_rank, lower(c.name), c.id) > ($9, $10, $11))
    ORDER BY ranked.search_rank, lower(c.name), c.id
    LIMIT $12
"#;

const OBJECT_SEARCH_SQL: &str = r#"
    SELECT o.id, o.name, o.collection_id, o.hubuum_class_id, o.data,
           o.description, o.created_at, o.updated_at, o.revision
    FROM hubuumobject o
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN lower(o.name) = lower($1) THEN 0
            WHEN lower(o.name) LIKE lower($1) || '%' THEN 1
            WHEN lower(o.name) LIKE '%' || lower($1) || '%' THEN 2
            WHEN lower(o.description) LIKE '%' || lower($1) || '%' THEN 3
            WHEN $2 AND jsonb_to_tsvector('simple', o.data, '["string"]')
                @@ plainto_tsquery('simple', $1) THEN 4
            ELSE 5
        END AS search_rank
    ) ranked
    WHERE (
          o.name ILIKE '%' || $1 || '%'
          OR o.description ILIKE '%' || $1 || '%'
          OR ($2 AND jsonb_to_tsvector('simple', o.data, '["string"]')
              @@ plainto_tsquery('simple', $1))
      )
      AND ($3 OR EXISTS (
          SELECT 1
          FROM permissions p
          JOIN group_memberships gm ON gm.group_id = p.group_id
          JOIN collection_closure cc ON cc.ancestor_collection_id = p.collection_id
          WHERE gm.principal_id = $4
            AND cc.descendant_collection_id = o.collection_id
            AND p.has_read_collection
            AND p.has_read_object
      ))
      AND ($5 OR o.collection_id = ANY($6) OR o.hubuum_class_id = ANY($7) OR o.id = ANY($8))
      AND ($9 OR (ranked.search_rank, lower(o.name), o.id) > ($10, $11, $12))
    ORDER BY ranked.search_rank, lower(o.name), o.id
    LIMIT $13
"#;

#[derive(Queryable, QueryableByName)]
#[diesel(table_name = crate::schema::collections)]
struct CollectionRow {
    id: i32,
    name: String,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    parent_collection_id: Option<i32>,
    revision: PostgresRevision,
}

impl TryFrom<CollectionRow> for StorageCollection {
    type Error = PostgresStorageError;

    fn try_from(row: CollectionRow) -> Result<Self, Self::Error> {
        crate::validate_persisted(
            "unified-search collection",
            Self::try_new(
                record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
                row.name,
                row.description,
                row.parent_collection_id
                    .map(CollectionId::new)
                    .transpose()?,
            ),
        )
    }
}

#[derive(QueryableByName)]
#[diesel(table_name = crate::schema::hubuumclass)]
struct ClassRow {
    id: i32,
    name: String,
    collection_id: i32,
    json_schema: Option<serde_json::Value>,
    validate_schema: bool,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: PostgresRevision,
}

impl ClassRow {
    fn into_storage(
        self,
        collections: &HashMap<i32, StorageCollection>,
    ) -> Result<StorageClassWithCollection, PostgresStorageError> {
        let collection = collections
            .get(&self.collection_id)
            .cloned()
            .ok_or_else(|| {
                PostgresStorageError::database(format!(
                    "class {} references missing collection {}",
                    self.id, self.collection_id
                ))
            })?;
        Ok(StorageClassWithCollection::builder(
            record_metadata(self.id, self.created_at, self.updated_at, self.revision)?,
            self.name,
            collection,
            self.description,
        )
        .json_schema(self.json_schema)
        .validate_schema(self.validate_schema)
        .build())
    }
}

#[derive(QueryableByName)]
#[diesel(table_name = crate::schema::hubuumobject)]
struct ObjectRow {
    id: i32,
    name: String,
    collection_id: i32,
    hubuum_class_id: i32,
    data: serde_json::Value,
    description: String,
    created_at: chrono::NaiveDateTime,
    updated_at: chrono::NaiveDateTime,
    revision: PostgresRevision,
}

impl TryFrom<ObjectRow> for StorageObject {
    type Error = PostgresStorageError;

    fn try_from(row: ObjectRow) -> Result<Self, Self::Error> {
        Ok(Self::new(
            record_metadata(row.id, row.created_at, row.updated_at, row.revision)?,
            row.name,
            CollectionId::new(row.collection_id)?,
            ClassId::new(row.hubuum_class_id)?,
            row.data,
            row.description,
        ))
    }
}

struct CursorBinds {
    absent: bool,
    rank: i32,
    name: String,
    id: i32,
}

impl CursorBinds {
    fn from_query(query: &StorageUnifiedSearchQuery) -> Self {
        match query.search_cursor() {
            Some(cursor) => Self {
                absent: false,
                rank: cursor.rank(),
                name: cursor.normalized_name().to_string(),
                id: cursor.id().id(),
            },
            None => Self {
                absent: true,
                rank: 0,
                name: String::new(),
                id: 0,
            },
        }
    }
}

struct ResourceBinds {
    unrestricted: bool,
    collection_ids: Vec<i32>,
    class_ids: Vec<i32>,
    object_ids: Vec<i32>,
}

impl ResourceBinds {
    fn from_scope(scope: Option<&StorageResourceScope>) -> Self {
        Self {
            unrestricted: scope.is_none(),
            collection_ids: scope
                .map(StorageResourceScope::collection_ids)
                .unwrap_or_default()
                .iter()
                .map(|id| id.id())
                .collect(),
            class_ids: scope
                .map(StorageResourceScope::class_ids)
                .unwrap_or_default()
                .iter()
                .map(|id| id.id())
                .collect(),
            object_ids: scope
                .map(StorageResourceScope::object_ids)
                .unwrap_or_default()
                .iter()
                .map(|id| id.id())
                .collect(),
        }
    }
}

fn bounded_limit(limit: usize) -> i64 {
    i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX)
}

pub async fn search_collections(
    runtime: &PostgresRuntime,
    query: StorageUnifiedSearchQuery,
) -> Result<Vec<StorageCollection>, PostgresStorageError> {
    if !query
        .visibility()
        .allows_permissions(&[StorageAuthorizationPermission::ReadCollection])
    {
        return Ok(Vec::new());
    }
    let resources = ResourceBinds::from_scope(query.visibility().resources());
    let cursor = CursorBinds::from_query(&query);
    let principal_id = query.visibility().principal_id();
    let is_admin = query.visibility().is_admin();
    let limit = bounded_limit(query.limit());
    let search_term = query.search_term().to_string();
    runtime
        .with_connection(async move |connection| {
            let rows = sql_query(COLLECTION_SEARCH_SQL)
                .bind::<Text, _>(search_term)
                .bind::<Bool, _>(is_admin)
                .bind::<Integer, _>(principal_id.id())
                .bind::<Bool, _>(resources.unrestricted)
                .bind::<Array<Integer>, _>(resources.collection_ids)
                .bind::<Bool, _>(cursor.absent)
                .bind::<Integer, _>(cursor.rank)
                .bind::<Text, _>(cursor.name)
                .bind::<Integer, _>(cursor.id)
                .bind::<BigInt, _>(limit)
                .load::<CollectionRow>(connection)
                .await?;
            rows.into_iter().map(TryInto::try_into).collect()
        })
        .await
}

pub async fn search_classes(
    runtime: &PostgresRuntime,
    query: StorageUnifiedSearchQuery,
) -> Result<Vec<StorageClassWithCollection>, PostgresStorageError> {
    if !query.visibility().allows_permissions(&[
        StorageAuthorizationPermission::ReadCollection,
        StorageAuthorizationPermission::ReadClass,
    ]) {
        return Ok(Vec::new());
    }
    let resources = ResourceBinds::from_scope(query.visibility().resources());
    let cursor = CursorBinds::from_query(&query);
    let principal_id = query.visibility().principal_id();
    let is_admin = query.visibility().is_admin();
    let limit = bounded_limit(query.limit());
    let search_extended_document = query.searches_extended_document();
    let search_term = query.search_term().to_string();
    runtime
        .with_read_only_snapshot(async move |connection| {
            let rows = sql_query(CLASS_SEARCH_SQL)
                .bind::<Text, _>(search_term)
                .bind::<Bool, _>(search_extended_document)
                .bind::<Bool, _>(is_admin)
                .bind::<Integer, _>(principal_id.id())
                .bind::<Bool, _>(resources.unrestricted)
                .bind::<Array<Integer>, _>(resources.collection_ids)
                .bind::<Array<Integer>, _>(resources.class_ids)
                .bind::<Bool, _>(cursor.absent)
                .bind::<Integer, _>(cursor.rank)
                .bind::<Text, _>(cursor.name)
                .bind::<Integer, _>(cursor.id)
                .bind::<BigInt, _>(limit)
                .load::<ClassRow>(connection)
                .await?;
            if rows.is_empty() {
                return Ok(Vec::new());
            }
            let collection_ids = rows.iter().map(|row| row.collection_id).collect::<Vec<_>>();
            let collections = {
                use crate::schema::collections::dsl::{collections, id};

                collections
                    .filter(id.eq_any(collection_ids))
                    .load::<CollectionRow>(connection)
                    .await?
                    .into_iter()
                    .map(|row| {
                        let row_id = row.id;
                        Ok((row_id, StorageCollection::try_from(row)?))
                    })
                    .collect::<Result<HashMap<_, _>, PostgresStorageError>>()?
            };
            rows.into_iter()
                .map(|row| row.into_storage(&collections))
                .collect()
        })
        .await
}

pub async fn search_objects(
    runtime: &PostgresRuntime,
    query: StorageUnifiedSearchQuery,
) -> Result<Vec<StorageObject>, PostgresStorageError> {
    if !query.visibility().allows_permissions(&[
        StorageAuthorizationPermission::ReadCollection,
        StorageAuthorizationPermission::ReadObject,
    ]) {
        return Ok(Vec::new());
    }
    let resources = ResourceBinds::from_scope(query.visibility().resources());
    let cursor = CursorBinds::from_query(&query);
    let principal_id = query.visibility().principal_id();
    let is_admin = query.visibility().is_admin();
    let limit = bounded_limit(query.limit());
    let search_extended_document = query.searches_extended_document();
    let search_term = query.search_term().to_string();
    runtime
        .with_connection(async move |connection| {
            let rows = sql_query(OBJECT_SEARCH_SQL)
                .bind::<Text, _>(search_term)
                .bind::<Bool, _>(search_extended_document)
                .bind::<Bool, _>(is_admin)
                .bind::<Integer, _>(principal_id.id())
                .bind::<Bool, _>(resources.unrestricted)
                .bind::<Array<Integer>, _>(resources.collection_ids)
                .bind::<Array<Integer>, _>(resources.class_ids)
                .bind::<Array<Integer>, _>(resources.object_ids)
                .bind::<Bool, _>(cursor.absent)
                .bind::<Integer, _>(cursor.rank)
                .bind::<Text, _>(cursor.name)
                .bind::<Integer, _>(cursor.id)
                .bind::<BigInt, _>(limit)
                .load::<ObjectRow>(connection)
                .await?;
            rows.into_iter().map(TryInto::try_into).collect()
        })
        .await
}
