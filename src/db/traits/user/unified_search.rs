use crate::db::prelude::*;
use crate::models::token_scope::TokenScope;
use diesel::sql_query;
use diesel::sql_types::{Array, BigInt, Bool, Integer, Text};

use crate::db::traits::authz::{AuthzSubject, scope_allows};
use crate::db::{DbPool, with_connection_async};
use crate::errors::ApiError;
use crate::models::traits::ExpandCollectionFromMap;
use crate::models::traits::user::UserCollectionAccessors;
use crate::models::{
    Collection, HubuumClass, HubuumClassExpanded, HubuumObject, Permissions,
    UnifiedSearchCursorToken, UnifiedSearchSpec,
};

const COLLECTION_SEARCH_SQL: &str = r#"
    SELECT c.id, c.name, c.description, c.created_at, c.updated_at,
           c.parent_collection_id
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
           c.description, c.created_at, c.updated_at
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
           o.description, o.created_at, o.updated_at
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

fn bounded_limit(limit: usize) -> i64 {
    i64::try_from(limit.saturating_add(1)).unwrap_or(i64::MAX)
}

fn cursor_values(cursor: Option<&UnifiedSearchCursorToken>) -> (bool, i32, String, i32) {
    match cursor {
        Some(cursor) => (false, cursor.rank, cursor.name.clone(), cursor.id),
        None => (true, 0, String::new(), 0),
    }
}

pub trait UnifiedSearchBackend: UserCollectionAccessors {
    async fn search_unified_collections_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<Collection>, ApiError> {
        let is_unscoped_admin = AuthzSubject::is_admin(self, pool).await? && scopes.is_none();
        self.search_unified_collections_from_backend_with_admin_status(
            pool,
            params,
            scopes,
            is_unscoped_admin,
        )
        .await
    }

    async fn search_unified_collections_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
        is_unscoped_admin: bool,
    ) -> Result<Vec<Collection>, ApiError> {
        if !scope_allows(scopes, &[Permissions::ReadCollection]) {
            return Ok(Vec::new());
        }

        let principal_id = self.principal_id();
        let query = params.query.clone();
        let resource_unscoped = scopes.and_then(TokenScope::collection_ids).is_none();
        let scoped_collection_ids = scopes
            .and_then(TokenScope::collection_ids)
            .unwrap_or_default()
            .to_vec();
        let (no_cursor, cursor_rank, cursor_name, cursor_id) =
            cursor_values(params.collection_cursor.as_ref());
        let limit = bounded_limit(params.limit_per_kind);

        with_connection_async(pool.clone(), async move |conn| {
            sql_query(COLLECTION_SEARCH_SQL)
                .bind::<Text, _>(query)
                .bind::<Bool, _>(is_unscoped_admin)
                .bind::<Integer, _>(principal_id)
                .bind::<Bool, _>(resource_unscoped)
                .bind::<Array<Integer>, _>(scoped_collection_ids)
                .bind::<Bool, _>(no_cursor)
                .bind::<Integer, _>(cursor_rank)
                .bind::<Text, _>(cursor_name)
                .bind::<Integer, _>(cursor_id)
                .bind::<BigInt, _>(limit)
                .load::<Collection>(conn)
                .await
        })
        .await
    }

    async fn search_unified_classes_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let is_unscoped_admin = AuthzSubject::is_admin(self, pool).await? && scopes.is_none();
        self.search_unified_classes_from_backend_with_admin_status(
            pool,
            params,
            scopes,
            is_unscoped_admin,
        )
        .await
    }

    async fn search_unified_classes_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
        is_unscoped_admin: bool,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        if !scope_allows(
            scopes,
            &[Permissions::ReadCollection, Permissions::ReadClass],
        ) {
            return Ok(Vec::new());
        }

        let principal_id = self.principal_id();
        let query = params.query.clone();
        let search_schema = params.search_class_schema;
        let resource_unscoped = scopes.and_then(TokenScope::collection_ids).is_none();
        let scoped_collection_ids = scopes
            .and_then(TokenScope::collection_ids)
            .unwrap_or_default()
            .to_vec();
        let scoped_class_ids = scopes
            .and_then(TokenScope::class_ids)
            .unwrap_or_default()
            .to_vec();
        let (no_cursor, cursor_rank, cursor_name, cursor_id) =
            cursor_values(params.class_cursor.as_ref());
        let limit = bounded_limit(params.limit_per_kind);

        let classes = with_connection_async(pool.clone(), async move |conn| {
            sql_query(CLASS_SEARCH_SQL)
                .bind::<Text, _>(query)
                .bind::<Bool, _>(search_schema)
                .bind::<Bool, _>(is_unscoped_admin)
                .bind::<Integer, _>(principal_id)
                .bind::<Bool, _>(resource_unscoped)
                .bind::<Array<Integer>, _>(scoped_collection_ids)
                .bind::<Array<Integer>, _>(scoped_class_ids)
                .bind::<Bool, _>(no_cursor)
                .bind::<Integer, _>(cursor_rank)
                .bind::<Text, _>(cursor_name)
                .bind::<Integer, _>(cursor_id)
                .bind::<BigInt, _>(limit)
                .load::<HubuumClass>(conn)
                .await
        })
        .await?;

        if classes.is_empty() {
            return Ok(Vec::new());
        }

        let collection_ids = classes
            .iter()
            .map(|class| class.collection_id)
            .collect::<Vec<_>>();
        let collections = with_connection_async(pool.clone(), async move |conn| {
            use crate::schema::collections::dsl::{collections, id};
            collections
                .filter(id.eq_any(collection_ids))
                .load::<Collection>(conn)
                .await
        })
        .await?;
        let collection_map = collections
            .into_iter()
            .map(|collection| (collection.id, collection))
            .collect::<std::collections::HashMap<_, _>>();

        Ok(classes.expand_collection_from_map(&collection_map))
    }

    async fn search_unified_objects_from_backend(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let is_unscoped_admin = AuthzSubject::is_admin(self, pool).await? && scopes.is_none();
        self.search_unified_objects_from_backend_with_admin_status(
            pool,
            params,
            scopes,
            is_unscoped_admin,
        )
        .await
    }

    async fn search_unified_objects_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        params: &UnifiedSearchSpec,
        scopes: Option<&TokenScope>,
        is_unscoped_admin: bool,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        if !scope_allows(
            scopes,
            &[Permissions::ReadCollection, Permissions::ReadObject],
        ) {
            return Ok(Vec::new());
        }

        let principal_id = self.principal_id();
        let query = params.query.clone();
        let search_data = params.search_object_data;
        let resource_unscoped = scopes.and_then(TokenScope::collection_ids).is_none();
        let scoped_collection_ids = scopes
            .and_then(TokenScope::collection_ids)
            .unwrap_or_default()
            .to_vec();
        let scoped_class_ids = scopes
            .and_then(TokenScope::class_ids)
            .unwrap_or_default()
            .to_vec();
        let scoped_object_ids = scopes
            .and_then(TokenScope::object_ids)
            .unwrap_or_default()
            .to_vec();
        let (no_cursor, cursor_rank, cursor_name, cursor_id) =
            cursor_values(params.object_cursor.as_ref());
        let limit = bounded_limit(params.limit_per_kind);

        with_connection_async(pool.clone(), async move |conn| {
            sql_query(OBJECT_SEARCH_SQL)
                .bind::<Text, _>(query)
                .bind::<Bool, _>(search_data)
                .bind::<Bool, _>(is_unscoped_admin)
                .bind::<Integer, _>(principal_id)
                .bind::<Bool, _>(resource_unscoped)
                .bind::<Array<Integer>, _>(scoped_collection_ids)
                .bind::<Array<Integer>, _>(scoped_class_ids)
                .bind::<Array<Integer>, _>(scoped_object_ids)
                .bind::<Bool, _>(no_cursor)
                .bind::<Integer, _>(cursor_rank)
                .bind::<Text, _>(cursor_name)
                .bind::<Integer, _>(cursor_id)
                .bind::<BigInt, _>(limit)
                .load::<HubuumObject>(conn)
                .await
        })
        .await
    }
}

impl<T: ?Sized> UnifiedSearchBackend for T where T: UserCollectionAccessors {}
