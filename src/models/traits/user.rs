use argon2::password_hash::rand_core::le;
use diesel::dsl::Filter;
use diesel::query_builder;
use diesel::sql_types::Integer;
use diesel::{pg::Pg, ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table};

use std::iter::IntoIterator;

use futures::future::try_join_all;
use tracing::debug;

use crate::api::v1::handlers::namespaces;
use crate::models::search::{FilterField, ParsedQueryParam, QueryParamsExt, SearchOperator};
use crate::models::traits::ExpandNamespaceFromMap;
use crate::models::{
    class, group, permissions, ClassClosureView, Group, HubuumClass, HubuumClassExpanded,
    HubuumClassRelation, HubuumObject, HubuumObjectRelation, Namespace, ObjectClosureView,
    Permission, Permissions, User, UserID,
};

use crate::schema::hubuumclass::namespace_id;
use crate::schema::{hubuumclass, hubuumobject};
use crate::traits::{ClassAccessors, NamespaceAccessors, SelfAccessors};

use crate::db::{with_connection, DbPool};
use crate::errors::ApiError;
use crate::utilities::extensions::CustomStringExtensions;

use crate::trace_query;

pub trait Search: SelfAccessors<User> + GroupAccessors + UserNamespaceAccessors {
    async fn search_namespaces(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};
        use crate::schema::permissions::dsl::{
            group_id, namespace_id as permissions_nid, permissions,
        };

        debug!(
            message = "Searching namespaces",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadCollection]);

        let group_id_subquery = self.group_ids_subquery();

        let mut base_query = namespaces
            .filter(
                namespace_id.eq_any(
                    permissions
                        .filter(group_id.eq_any(group_id_subquery))
                        .select(permissions_nid),
                ),
            )
            .into_boxed();

        for param in query_params {
            use crate::models::search::{DataType, SearchOperator};
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::namespaces::dsl::description
                ),
                FilterField::Permissions => {} // Handled above
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for namespaces",
                        param.field
                    )))
                }
            }
        }

        let result = with_connection(pool, |conn| {
            base_query
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        Ok(result)
    }

    async fn search_classes(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as hubuum_class_id, namespace_id as hubuum_classes_nid,
        };
        use crate::schema::permissions::dsl::*;

        debug!(
            message = "Searching classes",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        // Get all namespace IDs that the user has read permissions on, and if we have a list of selected namespaces, filter on those.
        let namespaces = self.namespaces(pool, &permissions_list).await?;
        let namespace_ids: Vec<i32> = namespaces.iter().map(|n| n.id).collect();

        debug!(
            message = "Searching classes",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumclass
            .filter(hubuum_classes_nid.eq_any(namespace_ids))
            .into_boxed();

        let json_schema_queries = query_params.json_schemas()?;
        if !json_schema_queries.is_empty() {
            debug!(
                message = "Searching classes",
                stage = "JSON Schema",
                user_id = self.id(),
                query_params = ?json_schema_queries
            );

            let json_schema_integers = self.json_schema_subquery(pool, json_schema_queries)?;

            if json_schema_integers.is_empty() {
                debug!(
                    message = "Searching classes",
                    stage = "JSON Schema",
                    user_id = self.id(),
                    result = "No class IDs found, returning empty result"
                );
                return Ok(vec![]);
            }

            debug!(
                message = "Searching classes",
                stage = "JSON Schema",
                user_id = self.id(),
                result = "Found class IDs",
                class_ids = ?json_schema_integers
            );

            base_query = base_query.filter(hubuum_class_id.eq_any(json_schema_integers));
        }

        for param in query_params {
            use crate::models::search::{DataType, SearchOperator};
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::id
                ),
                FilterField::Namespaces => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::namespace_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::description
                ),
                FilterField::ValidateSchema => boolean_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass::dsl::validate_schema
                ),
                FilterField::JsonSchema => {}  // Handled above
                FilterField::Permissions => {} // Handled above
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching classes");

        let result = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<HubuumClass>(conn)
        })?;

        // Map namespace IDs to namespaces for easy lookup
        let namespace_map: std::collections::HashMap<i32, Namespace> =
            namespaces.into_iter().map(|n| (n.id, n)).collect();

        let expanded_result: Vec<HubuumClassExpanded> =
            result.expand_namespace_from_map(&namespace_map);

        Ok(expanded_result)
    }

    async fn search_objects(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::hubuumobject::dsl::{
            hubuum_class_id, hubuumobject, id as hubuum_object_id,
            namespace_id as hubuum_object_nid,
        };
        use crate::schema::permissions::dsl::*;

        debug!(
            message = "Searching objects",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        // Get all namespace IDs that the user has read permissions on, and if we have a list of selected namespaces, filter on those.
        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permission_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching objects",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumobject
            .filter(hubuum_object_nid.eq_any(namespace_ids))
            .into_boxed();

        let json_data_queries = query_params.json_datas(FilterField::JsonData)?;
        if !json_data_queries.is_empty() {
            debug!(
                message = "Searching objects",
                stage = "JSON Data",
                user_id = self.id(),
                query_params = ?json_data_queries
            );

            let json_data_integers = self.json_data_subquery(pool, json_data_queries)?;

            if json_data_integers.is_empty() {
                debug!(
                    message = "Searching objects",
                    stage = "JSON Data",
                    user_id = self.id(),
                    result = "No object IDs found, returning empty result"
                );
                return Ok(vec![]);
            }

            debug!(
                message = "Searching objects",
                stage = "JSON Data",
                user_id = self.id(),
                result = "Found object IDs",
                class_ids = ?json_data_integers
            );

            base_query = base_query.filter(hubuum_object_id.eq_any(json_data_integers));
        }

        for param in query_params {
            use crate::models::search::{DataType, SearchOperator};
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::id
                ),
                FilterField::Namespaces => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::namespace_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::updated_at
                ),
                FilterField::Name => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::name
                ),
                FilterField::Description => string_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::description
                ),
                FilterField::Classes => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::hubuum_class_id
                ),
                FilterField::JsonData => {}    // Handled above
                FilterField::Permissions => {} // Handled above
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching objects");

        let result = with_connection(pool, |conn| {
            base_query
                .select(hubuumobject::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<HubuumObject>(conn)
        })?;

        Ok(result)
    }

    async fn search_class_relations(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        // Valid search fields:
        // From hubuumclass_relation:
        // - id (int)
        // - from_class_id (int)
        // - to_class_id (int)
        // - created_at (date)
        // - updated_at (date)
        // From permissions:
        // - A permission field for both from and to class IDs

        // Flow:
        // 1. Get all namespace IDs that the user has ReadClassRelations and any other required permissions on
        // 2. Filter the hubuumclass_relation table on the namespace IDs

        use crate::models::PermissionFilter;
        use crate::schema::hubuumclass;
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, id as hubuum_class_relation_id,
            to_hubuum_class_id,
        };
        use crate::schema::permissions::dsl::*;
        use diesel::alias;
        use std::collections::HashSet;

        debug!(
            message = "Searching class relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        // Permissions vector must contain ReadClassRelation
        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        // Get all namespace IDs that the user has ReadClassRelations and other requested permissions on.
        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching class relations",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumclass_relation.into_boxed();

        base_query = base_query
            .filter(
                from_hubuum_class_id.eq_any(
                    crate::schema::hubuumclass::dsl::hubuumclass
                        .select(crate::schema::hubuumclass::id)
                        .filter(crate::schema::hubuumclass::namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_class_id.eq_any(
                    crate::schema::hubuumclass::dsl::hubuumclass
                        .select(crate::schema::hubuumclass::id)
                        .filter(crate::schema::hubuumclass::namespace_id.eq_any(&namespace_ids)),
                ),
            );

        for param in query_params {
            use crate::models::search::{DataType, SearchOperator};
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::id
                ),
                FilterField::ClassFrom => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::from_hubuum_class_id
                ),
                FilterField::ClassTo => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::to_hubuum_class_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumclass_relation::dsl::updated_at
                ),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching class relations");

        let result = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<HubuumClassRelation>(conn)
        })?;

        Ok(result)
    }

    async fn search_objects_related_to<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<ObjectClosureView>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        use crate::schema::object_closure_view::dsl as obj;
        use diesel::prelude::*;

        debug!(
            message = "Searching object relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        // Permissions vector must contain ReadClassRelation
        let mut permissions_list = query_params.permissions()?;
        permissions_list
            .ensure_contains(&[Permissions::ReadObject, Permissions::ReadObjectRelation]);

        // Get all namespace IDs that the user has ReadClassRelations and other requested permissions on.
        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        // If the namespace_ids is empty, we can return early.
        if namespace_ids.is_empty() {
            debug!(
                message = "Searching object relations",
                stage = "Namespace IDs",
                user_id = self.id(),
                result = "No namespace IDs found, returning empty result"
            );
            return Ok(vec![]);
        } else {
            debug!(
                message = "Searching object relations",
                stage = "Namespace IDs",
                user_id = self.id(),
                result = "Found namespace IDs",
                namespace_ids = ?namespace_ids
            );
        }

        // First we need to ensure we have the correct permissions on both of the objects in question.
        let mut base_query = obj::object_closure_view.into_boxed();
        base_query = base_query
            .filter(obj::ancestor_object_id.eq(object.id()))
            .filter(obj::ancestor_namespace_id.eq_any(&namespace_ids))
            .filter(obj::descendant_namespace_id.eq_any(&namespace_ids));

        for param in &query_params {
            use crate::models::search::{DataType, SearchOperator};
            use crate::{
                array_search, boolean_search, date_search, json_search, numeric_search,
                string_search,
            };
            let operator = param.operator.clone();
            match &param.field {
                FilterField::ObjectFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_object_id)
                }
                FilterField::ObjectTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_object_id)
                }
                FilterField::ClassFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_class_id)
                }
                FilterField::ClassTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_class_id)
                }
                FilterField::NamespacesFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_namespace_id)
                }
                FilterField::NamespacesTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_namespace_id)
                }
                FilterField::NameFrom => {
                    string_search!(base_query, param, operator, obj::ancestor_name)
                }
                FilterField::NameTo => {
                    string_search!(base_query, param, operator, obj::descendant_name)
                }
                FilterField::DescriptionFrom => {
                    string_search!(base_query, param, operator, obj::ancestor_description)
                }
                FilterField::DescriptionTo => {
                    string_search!(base_query, param, operator, obj::descendant_description)
                }
                FilterField::CreatedAtFrom => {
                    date_search!(base_query, param, operator, obj::ancestor_created_at)
                }
                FilterField::CreatedAtTo => {
                    date_search!(base_query, param, operator, obj::descendant_created_at)
                }
                FilterField::UpdatedAtFrom => {
                    date_search!(base_query, param, operator, obj::ancestor_updated_at)
                }
                FilterField::UpdatedAtTo => {
                    date_search!(base_query, param, operator, obj::descendant_updated_at)
                }
                FilterField::JsonDataFrom => {
                    json_search!(
                        base_query,
                        query_params,
                        FilterField::JsonDataFrom,
                        obj::ancestor_object_id,
                        self,
                        pool
                    )
                }
                FilterField::JsonDataTo => {
                    json_search!(
                        base_query,
                        query_params,
                        FilterField::JsonDataTo,
                        obj::descendant_object_id,
                        self,
                        pool
                    )
                }
                FilterField::Depth => {
                    numeric_search!(base_query, param, operator, obj::depth)
                }
                FilterField::Path => {
                    array_search!(base_query, param, operator, obj::path)
                }
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for object relations",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching object relations");

        let result = with_connection(pool, |conn| {
            base_query
                .select(obj::object_closure_view::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<ObjectClosureView>(conn)
        })?;

        Ok(result)
    }
}

pub trait GroupAccessors: SelfAccessors<User> {
    /// Return all groups that the user is a member of.
    #[allow(async_fn_in_trait)]
    async fn groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

        let group_list = with_connection(pool, |conn| {
            user_groups
                .inner_join(groups.on(id.eq(group_id)))
                .filter(user_id.eq(self.id()))
                .select(groups::all_columns())
                .load::<Group>(conn)
        })?;

        Ok(group_list)
    }

    /*
      async fn group_ids(&self, pool: &DbPool) -> Result<Vec<i32>, ApiError> {
          use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

          let mut conn = pool.get()?;
          let group_list = user_groups
              .filter(user_id.eq(self.id()))
              .select(group_id)
              .load::<i32>(&mut conn)?;

          Ok(group_list)
      }
    */

    /// Generate a subquery to get all group IDs for a user.
    ///
    /// Note that this does not execute the query, it only creates it.
    ///
    /// ## Example
    ///
    /// Check if a user has a specific class permission to a given namespace ID
    ///
    /// ```ignore
    /// let group_id_subquery = user_id.group_ids_subquery();
    ///
    /// let base_query = classpermissions
    /// .into_boxed()
    /// .filter(namespace_id.eq(self.namespace_id))
    /// .filter(group_id.eq_any(group_id_subquery));
    ///
    /// let result = PermissionFilter::filter(permission, base_query)
    /// .first::<ClassPermission>(&mut conn)
    /// .optional()?;
    /// ```
    ///
    fn group_ids_subquery<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>
    {
        use crate::schema::user_groups::dsl::*;
        user_groups
            .filter(user_id.eq(self.id()))
            .select(group_id)
            .into_boxed()
    }

    fn json_schema_subquery(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        use crate::models::class::ClassIdResult;
        use crate::models::search::{Operator, SQLValue};

        if json_schema_query_params.is_empty() {
            return Err(ApiError::BadRequest(
                "No json_schema query parameters provided".to_string(),
            ));
        }

        let raw_sql_prefix = "select id from hubuumclass where";
        let mut raw_sql_clauses: Vec<String> = vec![];
        let mut bind_varaibles: Vec<SQLValue> = vec![];

        for param in json_schema_query_params {
            let clause = param.as_json_sql()?;
            debug!(message = "JSON Schema subquery", stage = "Clause", clause = ?clause);
            raw_sql_clauses.push(clause.sql);
            bind_varaibles.extend(clause.bind_variables);
        }

        let raw_sql = format!("{} {}", raw_sql_prefix, raw_sql_clauses.join(" and "))
            .replace_question_mark_with_indexed_n();

        debug!(message = "JSON Schema subquery", stage = "Complete", raw_sql = ?raw_sql, bind_variables = ?bind_varaibles);

        let mut query = diesel::sql_query(raw_sql).into_boxed();

        for bind_var in bind_varaibles {
            match bind_var {
                SQLValue::Integer(i) => query = query.bind::<diesel::sql_types::Integer, _>(i),
                SQLValue::String(s) => query = query.bind::<diesel::sql_types::Text, _>(s),
                SQLValue::Boolean(b) => query = query.bind::<diesel::sql_types::Bool, _>(b),
                SQLValue::Float(f) => query = query.bind::<diesel::sql_types::Float8, _>(f),
                SQLValue::Date(d) => query = query.bind::<diesel::sql_types::Timestamp, _>(d),
            }
        }

        trace_query!(query, "JSONB Schema subquery");

        let result_ids = with_connection(pool, |conn| query.get_results::<ClassIdResult>(conn))?;
        let ids: Vec<i32> = result_ids
            .into_iter()
            .map(|r: ClassIdResult| r.id)
            .collect();

        Ok(ids)
    }

    // Umm, async? Also, the name implies we return a subquery, but we return the Vec<i32> of the executed query.
    fn json_data_subquery(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        use crate::models::object::ObjectIDResult;
        use crate::models::search::{Operator, SQLValue};

        if json_schema_query_params.is_empty() {
            return Err(ApiError::BadRequest(
                "No json_data query parameters provided".to_string(),
            ));
        }

        let raw_sql_prefix = "select id from hubuumobject where";
        let mut raw_sql_clauses: Vec<String> = vec![];
        let mut bind_varaibles: Vec<SQLValue> = vec![];

        for param in json_schema_query_params {
            let clause = param.as_json_sql()?;
            debug!(message = "JSON Data subquery", stage = "Clause", clause = ?clause);
            raw_sql_clauses.push(clause.sql);
            bind_varaibles.extend(clause.bind_variables);
        }

        let raw_sql = format!("{} {}", raw_sql_prefix, raw_sql_clauses.join(" and "))
            .replace_question_mark_with_indexed_n();

        debug!(message = "JSON Data subquery", stage = "Complete", raw_sql = ?raw_sql, bind_variables = ?bind_varaibles);

        let mut query = diesel::sql_query(raw_sql).into_boxed();

        for bind_var in bind_varaibles {
            match bind_var {
                SQLValue::Integer(i) => query = query.bind::<diesel::sql_types::Integer, _>(i),
                SQLValue::String(s) => query = query.bind::<diesel::sql_types::Text, _>(s),
                SQLValue::Boolean(b) => query = query.bind::<diesel::sql_types::Bool, _>(b),
                SQLValue::Float(f) => query = query.bind::<diesel::sql_types::Float8, _>(f),
                SQLValue::Date(d) => query = query.bind::<diesel::sql_types::Timestamp, _>(d),
            }
        }

        trace_query!(query, "JSONB Data subquery");

        let result_ids = with_connection(pool, |conn| query.get_results::<ObjectIDResult>(conn))?;
        let ids: Vec<i32> = result_ids
            .into_iter()
            .map(|r: ObjectIDResult| r.id)
            .collect();

        Ok(ids)
    }
}

pub trait UserNamespaceAccessors: SelfAccessors<User> + GroupAccessors {
    /// Return all namespaces that the user has NamespacePermissions::ReadCollection on.
    #[allow(dead_code)] // Lazy-used in tests.
    async fn namespaces_read(&self, pool: &DbPool) -> Result<Vec<Namespace>, ApiError> {
        self.namespaces(pool, &[Permissions::ReadCollection]).await
    }

    /// Return all namespaces that the user has the given permissions on.
    async fn namespaces<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>,
    {
        use crate::models::PermissionFilter;
        use crate::schema::namespaces::dsl::{id as namespaces_table_id, namespaces};
        use crate::schema::permissions::dsl::{group_id, namespace_id, permissions};

        let groups_id_subquery = self.group_ids_subquery();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in permissions_list {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        let result = with_connection(pool, |conn| {
            base_query
                .inner_join(namespaces.on(namespace_id.eq(namespaces_table_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })?;

        Ok(result)
    }
}

pub trait UserClassAccessors: Search {
    async fn classes_read(&self, pool: &DbPool) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        self.search_classes(
            pool,
            vec![ParsedQueryParam::new(
                &FilterField::Permissions.to_string(),
                None,
                "ReadClass",
            )?],
        )
        .await
    }

    async fn classes_read_within_namespaces<N: NamespaceAccessors>(
        &self,
        pool: &DbPool,
        namespaces: Vec<N>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let futures: Vec<_> = namespaces
            .into_iter()
            .map(|n| {
                let pool_ref = &pool;
                async move { n.namespace_id(pool_ref).await }
            })
            .collect();
        let namespace_ids: Vec<i32> = try_join_all(futures).await?;

        let mut queries = vec![ParsedQueryParam::new(
            &FilterField::Permissions.to_string(),
            None,
            "ReadClass",
        )?];
        for nid in namespace_ids {
            queries.push(ParsedQueryParam::new(
                &FilterField::Namespaces.to_string(),
                None,
                &nid.to_string(),
            )?);
        }

        self.search_classes(pool, queries).await
    }

    async fn classes_within_namespaces_with_permissions<N: NamespaceAccessors>(
        &self,
        pool: &DbPool,
        namespaces: Vec<N>,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let futures: Vec<_> = namespaces
            .into_iter()
            .map(|n| {
                let pool_ref = &pool;
                async move { n.namespace_id(pool_ref).await }
            })
            .collect();
        let namespace_ids: Vec<i32> = try_join_all(futures).await?;

        let mut queries = vec![];
        for nid in namespace_ids {
            queries.push(ParsedQueryParam::new(
                &FilterField::Namespaces.to_string(),
                None,
                &nid.to_string(),
            )?);
        }

        for perm in permissions_list {
            queries.push(ParsedQueryParam::new(
                &FilterField::Namespaces.to_string(),
                None,
                &perm.to_string(),
            )?);
        }

        self.search_classes(pool, queries).await
    }

    async fn classes_with_permissions(
        &self,
        pool: &DbPool,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let mut queries = vec![];

        for perm in permissions_list {
            queries.push(ParsedQueryParam::new(
                &FilterField::Namespaces.to_string(),
                None,
                &perm.to_string(),
            )?);
        }

        self.search_classes(pool, queries).await
    }

    async fn classes(&self, pool: &DbPool) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        self.search_classes(pool, vec![]).await
    }
}

pub trait ObjectAccessors: UserClassAccessors + UserNamespaceAccessors {
    async fn objects_in_class_read<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_id: C,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        self.objects_in_classes_read(pool, vec![class_id]).await
    }

    async fn objects_in_classes_read<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_ids: Vec<C>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        self.objects(pool, class_ids, vec![Permissions::ReadClass])
            .await
    }

    async fn objects<C: UserClassAccessors>(
        &self,
        pool: &DbPool,
        class_ids: Vec<C>,
        permissions_list: Vec<Permissions>,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::models::PermissionFilter;
        use crate::schema::hubuumobject::dsl::{
            hubuum_class_id, hubuumobject, namespace_id as hubuumobject_nid,
        };
        use crate::schema::permissions::dsl::*;

        let group_id_subquery = self.group_ids_subquery();

        let namespace_ids: Vec<i32> = self
            .namespaces_read(pool)
            .await?
            .iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = permissions
            .into_boxed()
            .filter(namespace_id.eq_any(namespace_ids.clone()))
            .filter(group_id.eq_any(group_id_subquery));

        for perm in permissions_list {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        let mut joined_query =
            base_query.inner_join(hubuumobject.on(hubuumobject_nid.eq_any(namespace_ids)));

        if !class_ids.is_empty() {
            let valid_class_ids = class_ids.iter().map(|c| c.id()).collect::<Vec<i32>>();
            joined_query = joined_query.filter(hubuum_class_id.eq_any(valid_class_ids));
        }

        let result = with_connection(pool, |conn| {
            joined_query
                .select(hubuumobject::all_columns())
                .load::<HubuumObject>(conn)
        })?;

        Ok(result)
    }
}

impl UserNamespaceAccessors for User {}
impl UserNamespaceAccessors for UserID {}

impl UserClassAccessors for User {}
impl UserClassAccessors for UserID {}

impl GroupAccessors for User {}
impl GroupAccessors for UserID {}

impl GroupAccessors for &User {}
impl GroupAccessors for &UserID {}

impl Search for User {}
impl Search for UserID {}

impl SelfAccessors<User> for User {
    fn id(&self) -> i32 {
        self.id
    }

    async fn instance(&self, _pool: &DbPool) -> Result<User, ApiError> {
        Ok(self.clone())
    }
}

impl SelfAccessors<User> for UserID {
    fn id(&self) -> i32 {
        self.0
    }

    async fn instance(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;
        Ok(with_connection(pool, |conn| {
            users.filter(id.eq(self.0)).first::<User>(conn)
        })?)
    }
}

impl<'a> SelfAccessors<User> for &'a User {
    fn id(&self) -> i32 {
        (*self).id()
    }
    async fn instance(&self, pool: &DbPool) -> Result<User, ApiError> {
        (*self).instance(pool).await
    }
}

impl<'a> SelfAccessors<User> for &'a UserID {
    fn id(&self) -> i32 {
        (*self).id()
    }
    async fn instance(&self, pool: &DbPool) -> Result<User, ApiError> {
        (*self).instance(pool).await
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::models::{GroupID, NewHubuumClass, Permissions, PermissionsList};
    use crate::tests::{
        create_test_group, create_test_user, ensure_admin_group, ensure_admin_user,
        setup_pool_and_tokens,
    };
    use crate::traits::PermissionController;
    use crate::traits::{CanDelete, CanSave};
    use crate::{assert_contains, assert_not_contains};

    #[actix_rt::test]
    async fn test_user_permissions_namespace_and_class_listing() {
        use crate::models::namespace::NewNamespace;

        let (pool, _, _) = setup_pool_and_tokens().await;
        let test_user_1 = create_test_user(&pool).await;
        let test_group_1 = create_test_group(&pool).await;
        let test_user_2 = create_test_user(&pool).await;
        let test_group_2 = create_test_group(&pool).await;

        test_group_1.add_member(&pool, &test_user_1).await.unwrap();
        test_group_2.add_member(&pool, &test_user_2).await.unwrap();

        let ns = NewNamespace {
            name: "test_user_namespace_listing".to_string(),
            description: "Test namespace".to_string(),
        }
        .save_and_grant_all_to(&pool, GroupID(test_group_1.id))
        .await
        .unwrap();

        let class = NewHubuumClass {
            name: "test_user_namespace_listing".to_string(),
            description: "Test class".to_string(),
            json_schema: None,
            validate_schema: None,
            namespace_id: ns.id,
        }
        .save(&pool)
        .await
        .unwrap();

        class
            .grant(
                &pool,
                test_group_1.id,
                PermissionsList::new([
                    Permissions::ReadClass,
                    Permissions::UpdateClass,
                    Permissions::DeleteClass,
                    Permissions::CreateObject,
                ]),
            )
            .await
            .unwrap();

        let nslist = test_user_1.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_not_contains!(&nslist, &ns);

        let classlist = test_user_1.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_not_contains!(&classlist, &class);

        ns.grant_one(&pool, test_group_2.id, Permissions::ReadCollection)
            .await
            .unwrap();

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        let classlist = test_user_1.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .grant_one(&pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_contains!(&classlist, &class);

        class
            .revoke_one(&pool, test_group_2.id, Permissions::ReadClass)
            .await
            .unwrap();

        let classlist = test_user_2.classes_read(&pool).await.unwrap();
        assert_not_contains!(&classlist, &class);

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_contains!(&nslist, &ns);

        ns.revoke_all(&pool, test_group_2.id).await.unwrap();

        let nslist = test_user_2.namespaces_read(&pool).await.unwrap();
        assert_not_contains!(&nslist, &ns);

        test_user_1.delete(&pool).await.unwrap();
        test_user_2.delete(&pool).await.unwrap();
        test_group_1.delete(&pool).await.unwrap();
        test_group_2.delete(&pool).await.unwrap();
        ns.delete(&pool).await.unwrap();
    }
}
