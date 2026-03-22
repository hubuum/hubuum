use super::*;
use crate::models::search::SQLValue;
use crate::traits::{CursorPaginated, CursorSqlMapping};
use crate::utilities::extensions::CustomStringExtensions;

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

#[derive(Debug, Clone)]
struct RawSqlQuerySpec {
    sql: String,
    bind_variables: Vec<SQLValue>,
}

impl RawSqlQuerySpec {
    fn into_count_query(self, alias: &str) -> Self {
        Self {
            sql: format!("SELECT COUNT(*) AS count FROM ({}) AS {alias}", self.sql),
            bind_variables: self.bind_variables,
        }
    }

    fn into_indexed_sql(self) -> Self {
        Self {
            sql: self.sql.replace_question_mark_with_indexed_n(),
            bind_variables: self.bind_variables,
        }
    }
}

macro_rules! bind_raw_sql_query {
    ($spec:expr) => {{
        let spec = $spec.into_indexed_sql();
        let mut query = diesel::sql_query(spec.sql).into_boxed();
        for bind_var in spec.bind_variables {
            query = match bind_var {
                SQLValue::Integer(i) => query.bind::<diesel::sql_types::Integer, _>(i),
                SQLValue::String(s) => query.bind::<diesel::sql_types::Text, _>(s),
                SQLValue::Boolean(b) => query.bind::<diesel::sql_types::Bool, _>(b),
                SQLValue::Float(f) => query.bind::<diesel::sql_types::Float8, _>(f),
                SQLValue::Date(d) => query.bind::<diesel::sql_types::Timestamp, _>(d),
            };
        }
        query
    }};
}

pub trait UserSearchBackend: SelfAccessors<User> + UserNamespaceAccessors {
    async fn search_namespaces_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Namespace>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_namespaces_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn count_namespaces_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_namespaces_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn search_namespaces_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::schema::namespaces::dsl::{
            created_at as namespace_created_at, description as namespace_description,
            id as namespace_id, name as namespace_name, namespaces,
            updated_at as namespace_updated_at,
        };
        use crate::schema::permissions::dsl::{
            group_id, namespace_id as permissions_nid, permissions,
        };
        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching namespaces",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadCollection]);

        let mut base_query = if is_admin {
            namespaces.into_boxed()
        } else {
            let group_id_subquery = self.group_ids_subquery_from_backend();

            namespaces
                .filter(
                    namespace_id.eq_any(
                        permissions
                            .filter(group_id.eq_any(group_id_subquery))
                            .select(permissions_nid),
                    ),
                )
                .into_boxed()
        };

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, namespace_id),
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, namespace_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, namespace_updated_at)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, namespace_name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, namespace_description)
                }
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for namespaces",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Namespace);

        with_connection(pool, |conn| {
            base_query
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })
    }

    async fn count_namespaces_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<i64, ApiError> {
        use crate::schema::namespaces::dsl::{
            created_at as namespace_created_at, description as namespace_description,
            id as namespace_id, name as namespace_name, namespaces,
            updated_at as namespace_updated_at,
        };
        use crate::schema::permissions::dsl::{
            group_id, namespace_id as permissions_nid, permissions,
        };

        let query_params = query_options.filters.clone();
        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadCollection]);

        let mut base_query = if is_admin {
            namespaces.into_boxed()
        } else {
            let group_id_subquery = self.group_ids_subquery_from_backend();

            namespaces
                .filter(
                    namespace_id.eq_any(
                        permissions
                            .filter(group_id.eq_any(group_id_subquery))
                            .select(permissions_nid),
                    ),
                )
                .into_boxed()
        };

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, namespace_id),
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, namespace_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, namespace_updated_at)
                }
                FilterField::Name => {
                    string_search!(base_query, param, operator, namespace_name)
                }
                FilterField::Description => {
                    string_search!(base_query, param, operator, namespace_description)
                }
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for namespaces",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| base_query.count().get_result::<i64>(conn))
    }

    async fn search_classes_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_classes_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn count_classes_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_classes_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn search_classes_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            created_at as class_created_at, description as class_description, hubuumclass,
            id as class_id, name as class_name, namespace_id as class_namespace_id,
            updated_at as class_updated_at, validate_schema as class_validate_schema,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching classes",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        let namespaces = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
            .await?;
        let namespace_ids: Vec<i32> = namespaces.iter().map(|n| n.id).collect();

        debug!(
            message = "Searching classes",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let mut base_query = hubuumclass
            .filter(class_namespace_id.eq_any(namespace_ids))
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

            base_query = base_query.filter(class_id.eq_any(json_schema_integers));
        }

        for param in query_params {
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, class_id),
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, class_namespace_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, class_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, class_updated_at)
                }
                FilterField::Name => string_search!(base_query, param, operator, class_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, class_description)
                }
                FilterField::ValidateSchema => {
                    boolean_search!(base_query, param, operator, class_validate_schema)
                }
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassExpanded);

        trace_query!(base_query, "Searching classes");

        let result = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass::all_columns())
                .distinct()
                .load::<HubuumClass>(conn)
        })?;

        let namespace_map: std::collections::HashMap<i32, Namespace> =
            namespaces.into_iter().map(|n| (n.id, n)).collect();

        Ok(result.expand_namespace_from_map(&namespace_map))
    }

    async fn count_classes_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<i64, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            created_at as class_created_at, description as class_description, hubuumclass,
            id as class_id, name as class_name, namespace_id as class_namespace_id,
            updated_at as class_updated_at, validate_schema as class_validate_schema,
        };

        let query_params = query_options.filters.clone();

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadCollection]);

        let namespaces = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
            .await?;
        let namespace_ids: Vec<i32> = namespaces.iter().map(|n| n.id).collect();

        let mut base_query = hubuumclass
            .filter(class_namespace_id.eq_any(namespace_ids))
            .into_boxed();

        let json_schema_queries = query_params.json_schemas()?;
        if !json_schema_queries.is_empty() {
            let json_schema_integers = self.json_schema_subquery(pool, json_schema_queries)?;
            if json_schema_integers.is_empty() {
                return Ok(0);
            }
            base_query = base_query.filter(class_id.eq_any(json_schema_integers));
        }

        for param in query_params {
            use crate::{boolean_search, date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, class_id),
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, class_namespace_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, class_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, class_updated_at)
                }
                FilterField::Name => string_search!(base_query, param, operator, class_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, class_description)
                }
                FilterField::ValidateSchema => {
                    boolean_search!(base_query, param, operator, class_validate_schema)
                }
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| {
            base_query
                .select(class_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })
    }

    async fn search_objects_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_objects_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn count_objects_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.count_objects_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn search_objects_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            created_at as object_created_at, description as object_description, hubuum_class_id,
            hubuumobject, id as object_id, name as object_name,
            namespace_id as object_namespace_id, updated_at as object_updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching objects",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permission_list, is_admin)
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
            .filter(object_namespace_id.eq_any(namespace_ids))
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

            base_query = base_query.filter(object_id.eq_any(json_data_integers));
        }

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, object_id),
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, object_namespace_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, object_updated_at)
                }
                FilterField::Name => string_search!(base_query, param, operator, object_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, object_description)
                }
                FilterField::Classes => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::ClassId => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumObject);

        trace_query!(base_query, "Searching objects");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumobject::all_columns())
                .distinct()
                .load::<HubuumObject>(conn)
        })
    }

    async fn count_objects_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<i64, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            created_at as object_created_at, description as object_description, hubuum_class_id,
            hubuumobject, id as object_id, name as object_name,
            namespace_id as object_namespace_id, updated_at as object_updated_at,
        };

        let query_params = query_options.filters.clone();

        let mut permission_list = query_params.permissions()?;
        permission_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadCollection]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permission_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let mut base_query = hubuumobject
            .filter(object_namespace_id.eq_any(namespace_ids))
            .into_boxed();

        let json_data_queries = query_params.json_datas(FilterField::JsonData)?;
        if !json_data_queries.is_empty() {
            let json_data_integers = self.json_data_subquery(pool, json_data_queries)?;
            if json_data_integers.is_empty() {
                return Ok(0);
            }
            base_query = base_query.filter(object_id.eq_any(json_data_integers));
        }

        for param in query_params {
            use crate::{date_search, numeric_search, string_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, object_id),
                FilterField::Namespaces => {
                    numeric_search!(base_query, param, operator, object_namespace_id)
                }
                FilterField::CreatedAt => {
                    date_search!(base_query, param, operator, object_created_at)
                }
                FilterField::UpdatedAt => {
                    date_search!(base_query, param, operator, object_updated_at)
                }
                FilterField::Name => string_search!(base_query, param, operator, object_name),
                FilterField::Description => {
                    string_search!(base_query, param, operator, object_description)
                }
                FilterField::Classes => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::ClassId => {
                    numeric_search!(base_query, param, operator, hubuum_class_id)
                }
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| {
            base_query
                .select(object_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })
    }

    async fn search_class_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_class_relations_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn class_relations_page_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.class_relations_page_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn search_class_relations_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let (items, _) = self
            .class_relations_page_from_backend_with_admin_status(pool, query_options, is_admin)
            .await?;
        Ok(items)
    }

    async fn class_relations_page_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            created_at as class_relation_created_at, from_hubuum_class_id, hubuumclass_relation,
            id as class_relation_id, to_hubuum_class_id, updated_at as class_relation_updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching class relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut query_params = query_params;
        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
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

        for param in &[FilterField::ClassFromName, FilterField::ClassToName] {
            if let Some(class_param) = query_params.iter().find(|p| &p.field == param) {
                let qparam = ParsedQueryParam {
                    field: FilterField::Name,
                    operator: class_param.operator.clone(),
                    value: class_param.value.clone(),
                };
                let query_options = QueryOptions {
                    filters: vec![qparam],
                    sort: vec![],
                    limit: None,
                    cursor: None,
                };
                let classes = self
                    .search_classes_from_backend_with_admin_status(pool, query_options, is_admin)
                    .await?;
                let class_ids: Vec<i32> = classes.iter().map(|c| c.id).collect();

                if class_ids.is_empty() {
                    debug!(
                        message = "Searching class relations with class names",
                        stage = "Class IDs",
                        user_id = self.id(),
                        result = "No class IDs found, returning empty result"
                    );
                    return Ok((vec![], 0));
                }

                debug!(
                    message = "Searching class relations with class names",
                    stage = "Class IDs",
                    user_id = self.id(),
                    result = "Found class IDs",
                    class_ids = ?class_ids
                );

                let field = match param {
                    FilterField::ClassFromName => FilterField::ClassFrom,
                    FilterField::ClassToName => FilterField::ClassTo,
                    _ => unreachable!(),
                };

                query_params.push(ParsedQueryParam {
                    field,
                    operator: SearchOperator::Equals { is_negated: false },
                    value: class_ids
                        .iter()
                        .map(|item| item.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                });
            }
        }

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumclass_relation
                .filter(
                    from_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .filter(
                    to_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .into_boxed();

            for param in &query_params {
                use crate::{date_search, numeric_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ClassFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_class_id)
                    }
                    FilterField::ClassTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_class_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, class_relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, class_relation_updated_at)
                    }
                    FilterField::ClassFromName => {}
                    FilterField::ClassToName => {}
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for class relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = with_connection(pool, |conn| {
            base_query
                .select(class_relation_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching class relations");

        let items = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
        })?;

        Ok((items, total_count))
    }

    async fn class_relations_touching_page_from_backend<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.class_relations_touching_page_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
        )
        .await
    }

    async fn class_relations_touching_page_from_backend_with_admin_status<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<HubuumClassRelation>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            created_at as relation_created_at, from_hubuum_class_id, hubuumclass_relation,
            id as relation_id, to_hubuum_class_id, updated_at as relation_updated_at,
        };
        use diesel::BoolExpressionMethods;

        let query_params = query_options.filters.clone();

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadClassRelation]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumclass_relation
                .filter(
                    from_hubuum_class_id
                        .eq(class.id())
                        .or(to_hubuum_class_id.eq(class.id())),
                )
                .filter(
                    from_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .filter(
                    to_hubuum_class_id.eq_any(
                        hubuumclass
                            .select(class_id)
                            .filter(class_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .into_boxed();

            for param in &query_params {
                use crate::{date_search, numeric_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_class_id)
                    }
                    FilterField::ClassTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_class_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for class relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = with_connection(pool, |conn| {
            base_query
                .select(relation_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(
            base_query,
            "Searching direct class relations touching class"
        );

        let items = with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
        })?;

        Ok((items, total_count))
    }

    async fn search_class_relations_between_ids_from_backend(
        &self,
        pool: &DbPool,
        class_ids: &[i32],
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_class_relations_between_ids_from_backend_with_admin_status(
            pool, class_ids, is_admin,
        )
        .await
    }

    async fn search_class_relations_between_ids_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        class_ids: &[i32],
        is_admin: bool,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as class_id, namespace_id as class_namespace_id,
        };
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, id as relation_id, to_hubuum_class_id,
        };

        if class_ids.is_empty() {
            return Ok(vec![]);
        }

        let permission_list = [Permissions::ReadClassRelation];
        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permission_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        let base_query = hubuumclass_relation
            .filter(from_hubuum_class_id.eq_any(class_ids))
            .filter(to_hubuum_class_id.eq_any(class_ids))
            .filter(
                from_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_class_id.eq_any(
                    hubuumclass
                        .select(class_id)
                        .filter(class_namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .order(relation_id.asc());

        trace_query!(base_query, "Searching class relations among class IDs");

        with_connection(pool, |conn| base_query.load::<HubuumClassRelation>(conn))
    }

    async fn search_classes_related_to_from_backend<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
    ) -> Result<Vec<ClassGraphRow>, ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.search_classes_related_to_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
        )
        .await
    }

    async fn classes_related_to_page_from_backend<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
    ) -> Result<(Vec<ClassGraphRow>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.classes_related_to_page_from_backend_with_admin_status(
            pool,
            class,
            query_options,
            is_admin,
        )
        .await
    }

    async fn search_classes_related_to_from_backend_with_admin_status<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<ClassGraphRow>, ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let Some(base_spec) =
            build_related_classes_query_spec(self, pool, class, query_options.clone(), is_admin)
                .await?
        else {
            return Ok(vec![]);
        };
        let spec = apply_raw_sql_pagination::<ClassGraphRow>(base_spec, &query_options)?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching related classes",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching related classes");

        with_connection(pool, |conn| query.get_results::<ClassGraphRow>(conn))
    }

    async fn classes_related_to_page_from_backend_with_admin_status<K>(
        &self,
        pool: &DbPool,
        class: K,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<ClassGraphRow>, i64), ApiError>
    where
        K: SelfAccessors<HubuumClass>,
    {
        let Some(base_spec) =
            build_related_classes_query_spec(self, pool, class, query_options.clone(), is_admin)
                .await?
        else {
            return Ok((vec![], 0));
        };
        let total_count_spec = base_spec.clone().into_count_query("related_classes_count");
        let spec = apply_raw_sql_pagination::<ClassGraphRow>(base_spec, &query_options)?;

        let total_count = with_connection(pool, |conn| {
            bind_raw_sql_query!(total_count_spec)
                .get_result::<CountRow>(conn)
                .map(|row| row.count)
        })?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching related classes",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching related classes");
        let items = with_connection(pool, |conn| query.get_results::<ClassGraphRow>(conn))?;

        Ok((items, total_count))
    }

    async fn search_object_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_object_relations_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn object_relations_page_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.object_relations_page_from_backend_with_admin_status(pool, query_options, is_admin)
            .await
    }

    async fn search_object_relations_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let (items, _) = self
            .object_relations_page_from_backend_with_admin_status(pool, query_options, is_admin)
            .await?;
        Ok(items)
    }

    async fn object_relations_page_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id, namespace_id as object_namespace_id,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            class_relation_id, created_at as relation_created_at, from_hubuum_object_id,
            hubuumobject_relation, id as relation_id, to_hubuum_object_id,
            updated_at as relation_updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching object relations",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadObjectRelation]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching object relations",
            stage = "Namespace IDs",
            user_id = self.id(),
            namespace_ids = ?namespace_ids
        );

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumobject_relation
                .filter(
                    from_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id)
                            .filter(object_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .filter(
                    to_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id)
                            .filter(object_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .into_boxed();

            for param in &query_params {
                use crate::{date_search, numeric_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassRelation => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ObjectFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_object_id)
                    }
                    FilterField::ObjectTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_object_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for object relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = with_connection(pool, |conn| {
            base_query
                .select(relation_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(base_query, "Searching object relations");

        let items = with_connection(pool, |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
        })?;

        Ok((items, total_count))
    }

    async fn object_relations_touching_page_from_backend<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject>,
    {
        let is_admin = self.is_admin(pool).await?;
        self.object_relations_touching_page_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
        )
        .await
    }

    async fn object_relations_touching_page_from_backend_with_admin_status<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<HubuumObjectRelation>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject>,
    {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id_column, namespace_id as object_namespace_id,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            class_relation_id, created_at as relation_created_at, from_hubuum_object_id,
            hubuumobject_relation, id as relation_id, to_hubuum_object_id,
            updated_at as relation_updated_at,
        };
        use diesel::BoolExpressionMethods;

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching direct object relations touching object",
            stage = "Starting",
            user_id = self.id(),
            object_id = object.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list.ensure_contains(&[Permissions::ReadObjectRelation]);

        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching direct object relations touching object",
            stage = "Namespace IDs",
            user_id = self.id(),
            object_id = object.id(),
            namespace_ids = ?namespace_ids
        );

        let build_query = || -> Result<_, ApiError> {
            let mut base_query = hubuumobject_relation
                .filter(
                    from_hubuum_object_id
                        .eq(object.id())
                        .or(to_hubuum_object_id.eq(object.id())),
                )
                .filter(
                    from_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id_column)
                            .filter(object_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .filter(
                    to_hubuum_object_id.eq_any(
                        hubuumobject
                            .select(object_id_column)
                            .filter(object_namespace_id.eq_any(&namespace_ids)),
                    ),
                )
                .into_boxed();

            for param in &query_params {
                use crate::{date_search, numeric_search};
                let operator = param.operator.clone();
                match param.field {
                    FilterField::Id => numeric_search!(base_query, param, operator, relation_id),
                    FilterField::ClassRelation => {
                        numeric_search!(base_query, param, operator, class_relation_id)
                    }
                    FilterField::ObjectFrom => {
                        numeric_search!(base_query, param, operator, from_hubuum_object_id)
                    }
                    FilterField::ObjectTo => {
                        numeric_search!(base_query, param, operator, to_hubuum_object_id)
                    }
                    FilterField::CreatedAt => {
                        date_search!(base_query, param, operator, relation_created_at)
                    }
                    FilterField::UpdatedAt => {
                        date_search!(base_query, param, operator, relation_updated_at)
                    }
                    _ => {
                        return Err(ApiError::BadRequest(format!(
                            "Field '{}' isn't searchable (or does not exist) for object relations",
                            param.field
                        )));
                    }
                }
            }

            Ok(base_query)
        };

        let base_query = build_query()?;
        let total_count = with_connection(pool, |conn| {
            base_query
                .select(relation_id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })?;

        let mut base_query = build_query()?;
        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(
            base_query,
            "Searching direct object relations touching object"
        );

        let items = with_connection(pool, |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
        })?;

        Ok((items, total_count))
    }

    async fn search_object_relations_between_ids_from_backend(
        &self,
        pool: &DbPool,
        object_ids: &[i32],
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        let is_admin = self.is_admin(pool).await?;
        self.search_object_relations_between_ids_from_backend_with_admin_status(
            pool, object_ids, is_admin,
        )
        .await
    }

    async fn search_object_relations_between_ids_from_backend_with_admin_status(
        &self,
        pool: &DbPool,
        object_ids: &[i32],
        is_admin: bool,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as object_id_column, namespace_id as object_namespace_id,
        };
        use crate::schema::hubuumobject_relation::dsl::{
            from_hubuum_object_id, hubuumobject_relation, id, to_hubuum_object_id,
        };

        if object_ids.is_empty() {
            return Ok(vec![]);
        }

        let permission_list = [Permissions::ReadObjectRelation];
        let namespace_ids: Vec<i32> = self
            .load_namespaces_with_permissions_with_admin_status(pool, &permission_list, is_admin)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        debug!(
            message = "Searching object relations between visible object IDs",
            user_id = self.id(),
            object_ids = ?object_ids,
            namespace_ids = ?namespace_ids
        );

        let base_query = hubuumobject_relation
            .filter(from_hubuum_object_id.eq_any(object_ids))
            .filter(to_hubuum_object_id.eq_any(object_ids))
            .filter(
                from_hubuum_object_id.eq_any(
                    hubuumobject
                        .select(object_id_column)
                        .filter(object_namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_object_id.eq_any(
                    hubuumobject
                        .select(object_id_column)
                        .filter(object_namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .order(id.asc());

        trace_query!(base_query, "Searching object relations among object IDs");

        with_connection(pool, |conn| base_query.load::<HubuumObjectRelation>(conn))
    }

    async fn search_objects_related_to_from_backend<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let is_admin = self.is_admin(pool).await?;
        self.search_objects_related_to_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
        )
        .await
    }

    async fn objects_related_to_page_from_backend<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<(Vec<RelatedObjectGraphRow>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let is_admin = self.is_admin(pool).await?;
        self.objects_related_to_page_from_backend_with_admin_status(
            pool,
            object,
            query_options,
            is_admin,
        )
        .await
    }

    async fn search_objects_related_to_from_backend_with_admin_status<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<Vec<RelatedObjectGraphRow>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let Some(base_spec) =
            build_related_objects_query_spec(self, pool, object, query_options.clone(), is_admin)
                .await?
        else {
            return Ok(vec![]);
        };
        let spec = apply_raw_sql_pagination::<RelatedObjectGraphRow>(base_spec, &query_options)?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching source-relative related objects",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching source-relative related objects");

        with_connection(pool, |conn| query.get_results::<RelatedObjectGraphRow>(conn))
    }

    async fn objects_related_to_page_from_backend_with_admin_status<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
        is_admin: bool,
    ) -> Result<(Vec<RelatedObjectGraphRow>, i64), ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        let Some(base_spec) =
            build_related_objects_query_spec(self, pool, object, query_options.clone(), is_admin)
                .await?
        else {
            return Ok((vec![], 0));
        };
        let total_count_spec = base_spec.clone().into_count_query("related_objects_count");
        let spec = apply_raw_sql_pagination::<RelatedObjectGraphRow>(base_spec, &query_options)?;

        let total_count = with_connection(pool, |conn| {
            bind_raw_sql_query!(total_count_spec)
                .get_result::<CountRow>(conn)
                .map(|row| row.count)
        })?;

        let query = bind_raw_sql_query!(spec.clone());
        debug!(
            message = "Searching source-relative related objects",
            raw_sql = %spec.sql,
            bind_variables = ?spec.bind_variables
        );
        trace_query!(query, "Searching source-relative related objects");
        let items = with_connection(pool, |conn| {
            query.get_results::<RelatedObjectGraphRow>(conn)
        })?;

        Ok((items, total_count))
    }
}

fn apply_raw_sql_pagination<T>(
    mut spec: RawSqlQuerySpec,
    query_options: &QueryOptions,
) -> Result<RawSqlQuerySpec, ApiError>
where
    T: CursorPaginated + CursorSqlMapping,
{
    use crate::pagination::{cursor_filter_sql, normalized_sorts, order_sql_clause};

    let sorts = normalized_sorts::<T>(&query_options.sort)?;
    let mut where_clauses = Vec::new();
    if let Some(cursor_sql) = cursor_filter_sql::<T>(&sorts, query_options.cursor.as_deref())? {
        where_clauses.push(cursor_sql);
    }

    if !where_clauses.is_empty() {
        if spec.sql.contains("\nWHERE ") {
            spec.sql.push_str("\n  AND ");
        } else {
            spec.sql.push_str("\nWHERE ");
        }
        spec.sql.push_str(&where_clauses.join("\n  AND "));
    }

    let order_by = sorts
        .iter()
        .map(order_sql_clause::<T>)
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    spec.sql.push_str(&format!("\nORDER BY {order_by}"));

    if let Some(limit) = query_options.limit {
        spec.sql.push_str(&format!("\nLIMIT {limit}"));
    }

    Ok(spec)
}

async fn build_related_classes_query_spec<U, K>(
    user: &U,
    pool: &DbPool,
    class: K,
    query_options: QueryOptions,
    is_admin: bool,
) -> Result<Option<RawSqlQuerySpec>, ApiError>
where
    U: SelfAccessors<User> + UserNamespaceAccessors + ?Sized,
    K: SelfAccessors<HubuumClass>,
{
    let query_params = query_options.filters.clone();

    let mut permissions_list = query_params.permissions()?;
    permissions_list.ensure_contains(&[Permissions::ReadClass, Permissions::ReadClassRelation]);

    let namespace_ids: Vec<i32> = user
        .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
        .await?
        .into_iter()
        .map(|n| n.id)
        .collect();

    if namespace_ids.is_empty() {
        return Ok(None);
    }

    let mut bind_variables = Vec::<SQLValue>::new();
    bind_variables.push(SQLValue::Integer(class.id()));
    let related_depth_upper_bound = related_depth_upper_bound(&query_params)?;
    let namespace_array_sql = sql_integer_array(&namespace_ids, &mut bind_variables);
    let mut raw_sql = if let Some(max_depth) = related_depth_upper_bound {
        bind_variables.push(SQLValue::Integer(max_depth));
        format!(
            "SELECT * FROM get_bidirectionally_related_classes(?, {namespace_array_sql}, ?) AS related_classes"
        )
    } else {
        format!(
            "SELECT * FROM get_bidirectionally_related_classes(?, {namespace_array_sql}, NULL) AS related_classes"
        )
    };

    let mut where_clauses = Vec::new();
    for param in &query_params {
        let clause = build_related_classes_clause(param, &mut bind_variables)?;
        if let Some(clause) = clause {
            where_clauses.push(clause);
        }
    }

    if !where_clauses.is_empty() {
        raw_sql.push_str("\nWHERE ");
        raw_sql.push_str(&where_clauses.join("\n  AND "));
    }

    Ok(Some(RawSqlQuerySpec {
        sql: raw_sql,
        bind_variables,
    }))
}

async fn build_related_objects_query_spec<U, O>(
    user: &U,
    pool: &DbPool,
    object: O,
    query_options: QueryOptions,
    is_admin: bool,
) -> Result<Option<RawSqlQuerySpec>, ApiError>
where
    U: SelfAccessors<User> + UserNamespaceAccessors + ?Sized,
    O: SelfAccessors<HubuumObject> + ClassAccessors,
{
    let query_params = query_options.filters.clone();

    debug!(
        message = "Searching objects related to object",
        stage = "Starting",
        user_id = user.id(),
        object_id = object.id(),
        query_params = ?query_params
    );

    let mut permissions_list = query_params.permissions()?;
    permissions_list.ensure_contains(&[Permissions::ReadObject, Permissions::ReadObjectRelation]);

    let namespace_ids: Vec<i32> = user
        .load_namespaces_with_permissions_with_admin_status(pool, &permissions_list, is_admin)
        .await?
        .into_iter()
        .map(|n| n.id)
        .collect();

    if namespace_ids.is_empty() {
        debug!(
            message = "Searching object relations related to object",
            stage = "Namespace IDs",
            user_id = user.id(),
            result = "No namespace IDs found, returning empty result"
        );
        return Ok(None);
    }

    debug!(
        message = "Searching object relations related to object",
        stage = "Namespace IDs",
        user_id = user.id(),
        result = "Found namespace IDs",
        namespace_ids = ?namespace_ids
    );

    let mut bind_variables = Vec::<SQLValue>::new();
    bind_variables.push(SQLValue::Integer(object.id()));
    let related_depth_upper_bound = related_depth_upper_bound(&query_params)?;
    let namespace_array_sql = sql_integer_array(&namespace_ids, &mut bind_variables);
    let mut raw_sql = if let Some(max_depth) = related_depth_upper_bound {
        bind_variables.push(SQLValue::Integer(max_depth));
        format!(
            "SELECT * FROM get_bidirectionally_related_objects(?, {namespace_array_sql}, ?) AS related_objects"
        )
    } else {
        format!(
            "SELECT * FROM get_bidirectionally_related_objects(?, {namespace_array_sql}, NULL) AS related_objects"
        )
    };

    let mut where_clauses = Vec::new();
    for param in &query_params {
        let clause = build_related_objects_clause(user, pool, param, &mut bind_variables)?;
        if let Some(clause) = clause {
            where_clauses.push(clause);
        }
    }

    if !where_clauses.is_empty() {
        raw_sql.push_str("\nWHERE ");
        raw_sql.push_str(&where_clauses.join("\n  AND "));
    }

    Ok(Some(RawSqlQuerySpec {
        sql: raw_sql,
        bind_variables,
    }))
}

fn sql_integer_array(values: &[i32], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::Integer(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::integer[]")
}

fn related_depth_upper_bound(
    filters: &[crate::models::search::ParsedQueryParam],
) -> Result<Option<i32>, ApiError> {
    use crate::models::search::SearchOperator;

    let mut upper_bound: Option<i32> = None;

    for filter in filters {
        if filter.field != FilterField::Depth {
            continue;
        }

        let values = filter.value_as_integer()?;
        if values.is_empty() {
            continue;
        }

        let min = *values
            .iter()
            .min()
            .ok_or_else(|| ApiError::BadRequest("Depth filter requires a value".to_string()))?;
        let max = *values
            .iter()
            .max()
            .ok_or_else(|| ApiError::BadRequest("Depth filter requires a value".to_string()))?;

        let candidate = match &filter.operator {
            SearchOperator::Equals { is_negated: false } => Some(max),
            SearchOperator::Lt { is_negated: false } => Some(min.saturating_sub(1)),
            SearchOperator::Lte { is_negated: false } => Some(min),
            SearchOperator::Between { is_negated: false } => Some(max),
            _ => None,
        };

        if let Some(candidate) = candidate {
            upper_bound = Some(match upper_bound {
                Some(current) => current.min(candidate),
                None => candidate,
            });
        }
    }

    Ok(upper_bound)
}

fn sql_date_array(values: &[chrono::NaiveDateTime], bind_variables: &mut Vec<SQLValue>) -> String {
    let placeholders = values
        .iter()
        .map(|value| {
            bind_variables.push(SQLValue::Date(*value));
            "?"
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("ARRAY[{placeholders}]::timestamp[]")
}

fn related_classes_column(field: &FilterField) -> Option<&'static str> {
    match field {
        FilterField::Id | FilterField::ClassTo | FilterField::ClassId | FilterField::Classes => {
            Some("related_classes.descendant_class_id")
        }
        FilterField::ClassFrom => Some("related_classes.ancestor_class_id"),
        FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
            Some("related_classes.descendant_namespace_id")
        }
        FilterField::NamespacesFrom => Some("related_classes.ancestor_namespace_id"),
        FilterField::Name | FilterField::NameTo => Some("related_classes.descendant_name"),
        FilterField::NameFrom => Some("related_classes.ancestor_name"),
        FilterField::Description | FilterField::DescriptionTo => {
            Some("related_classes.descendant_description")
        }
        FilterField::DescriptionFrom => Some("related_classes.ancestor_description"),
        FilterField::CreatedAt | FilterField::CreatedAtTo => {
            Some("related_classes.descendant_created_at")
        }
        FilterField::CreatedAtFrom => Some("related_classes.ancestor_created_at"),
        FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
            Some("related_classes.descendant_updated_at")
        }
        FilterField::UpdatedAtFrom => Some("related_classes.ancestor_updated_at"),
        FilterField::Depth => Some("related_classes.depth"),
        FilterField::Path => Some("related_classes.path"),
        _ => None,
    }
}

fn build_related_classes_clause(
    param: &ParsedQueryParam,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<Option<String>, ApiError> {
    use crate::models::search::{DataType, Operator};

    if param.field == FilterField::Permissions {
        return Ok(None);
    }

    let column = related_classes_column(&param.field).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Field '{}' isn't searchable (or does not exist) for related classes",
            param.field
        ))
    })?;

    let (op, negated) = param.operator.op_and_neg();
    let wrap = |sql: String| {
        if negated { format!("NOT ({sql})") } else { sql }
    };

    let clause = match param.field {
        FilterField::Id
        | FilterField::ClassFrom
        | FilterField::ClassTo
        | FilterField::ClassId
        | FilterField::Classes
        | FilterField::Namespaces
        | FilterField::NamespaceId
        | FilterField::NamespacesFrom
        | FilterField::NamespacesTo
        | FilterField::Depth => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_integer_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Integer(values[0]));
                    bind_variables.push(SQLValue::Integer(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Name
        | FilterField::NameFrom
        | FilterField::NameTo
        | FilterField::Description
        | FilterField::DescriptionFrom
        | FilterField::DescriptionTo => {
            if !param.operator.is_applicable_to(DataType::String) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let value = param.value.clone();
            match op {
                Operator::Equals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} = ?"))
                }
                Operator::IEquals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Contains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IContains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::StartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IStartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::EndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IEndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Like => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::Regex => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ~ ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: string)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::CreatedAt
        | FilterField::CreatedAtFrom
        | FilterField::CreatedAtTo
        | FilterField::UpdatedAt
        | FilterField::UpdatedAtFrom
        | FilterField::UpdatedAtTo => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_date()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_date_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Date(values[0]));
                    bind_variables.push(SQLValue::Date(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: date)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Path => {
            if !param.operator.is_applicable_to(DataType::Array) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            let array_sql = sql_integer_array(&values, bind_variables);
            match op {
                Operator::Contains => wrap(format!("{column} @> {array_sql}")),
                Operator::Equals => wrap(format!("{column} = {array_sql}")),
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: array)",
                        param.operator, param.field
                    )));
                }
            }
        }
        _ => {
            return Err(ApiError::BadRequest(format!(
                "Field '{}' isn't searchable (or does not exist) for related classes",
                param.field
            )));
        }
    };

    Ok(Some(clause))
}

fn related_objects_column(field: &FilterField) -> Option<&'static str> {
    match field {
        FilterField::ObjectFrom => Some("related_objects.ancestor_object_id"),
        FilterField::Id | FilterField::ObjectTo => Some("related_objects.descendant_object_id"),
        FilterField::ClassFrom => Some("related_objects.ancestor_class_id"),
        FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
            Some("related_objects.descendant_class_id")
        }
        FilterField::NamespacesFrom => Some("related_objects.ancestor_namespace_id"),
        FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
            Some("related_objects.descendant_namespace_id")
        }
        FilterField::NameFrom => Some("related_objects.ancestor_name"),
        FilterField::Name | FilterField::NameTo => Some("related_objects.descendant_name"),
        FilterField::DescriptionFrom => Some("related_objects.ancestor_description"),
        FilterField::Description | FilterField::DescriptionTo => {
            Some("related_objects.descendant_description")
        }
        FilterField::CreatedAtFrom => Some("related_objects.ancestor_created_at"),
        FilterField::CreatedAt | FilterField::CreatedAtTo => {
            Some("related_objects.descendant_created_at")
        }
        FilterField::UpdatedAtFrom => Some("related_objects.ancestor_updated_at"),
        FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
            Some("related_objects.descendant_updated_at")
        }
        FilterField::Depth => Some("related_objects.depth"),
        FilterField::Path => Some("related_objects.path"),
        _ => None,
    }
}

fn build_related_objects_clause<U: QueryJsonDataIds + ?Sized>(
    user: &U,
    pool: &DbPool,
    param: &ParsedQueryParam,
    bind_variables: &mut Vec<SQLValue>,
) -> Result<Option<String>, ApiError> {
    use crate::models::search::{DataType, Operator};

    if param.field == FilterField::Permissions {
        return Ok(None);
    }

    if param.field == FilterField::JsonDataFrom || param.field == FilterField::JsonDataTo {
        let object_ids = user.query_object_ids_for_json_data(pool, vec![param])?;
        if object_ids.is_empty() {
            return Ok(Some("FALSE".to_string()));
        }

        let column = if param.field == FilterField::JsonDataFrom {
            "related_objects.ancestor_object_id"
        } else {
            "related_objects.descendant_object_id"
        };

        let array_sql = sql_integer_array(&object_ids, bind_variables);
        return Ok(Some(format!("{column} = ANY({array_sql})")));
    }

    let column = related_objects_column(&param.field).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "Field '{}' isn't searchable (or does not exist) for object relations",
            param.field
        ))
    })?;

    let (op, negated) = param.operator.op_and_neg();
    let wrap = |sql: String| {
        if negated { format!("NOT ({sql})") } else { sql }
    };

    let clause = match param.field {
        FilterField::ObjectFrom
        | FilterField::Id
        | FilterField::ObjectTo
        | FilterField::ClassFrom
        | FilterField::ClassId
        | FilterField::Classes
        | FilterField::ClassTo
        | FilterField::Namespaces
        | FilterField::NamespaceId
        | FilterField::NamespacesFrom
        | FilterField::NamespacesTo
        | FilterField::Depth => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    if values.len() > 50 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'equals' is limited to 50 values, got {} (use between?)",
                            values.len()
                        )));
                    }
                    let array_sql = sql_integer_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Integer(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Integer(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Integer(values[0]));
                    bind_variables.push(SQLValue::Integer(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Name
        | FilterField::NameFrom
        | FilterField::NameTo
        | FilterField::Description
        | FilterField::DescriptionFrom
        | FilterField::DescriptionTo => {
            if !param.operator.is_applicable_to(DataType::String) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let value = param.value.clone();
            if value.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            match op {
                Operator::Equals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} = ?"))
                }
                Operator::IEquals => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Contains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IContains => {
                    bind_variables.push(SQLValue::String(format!("%{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::StartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IStartsWith => {
                    bind_variables.push(SQLValue::String(format!("{value}%")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::EndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::IEndsWith => {
                    bind_variables.push(SQLValue::String(format!("%{value}")));
                    wrap(format!("{column} ILIKE ?"))
                }
                Operator::Like => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} LIKE ?"))
                }
                Operator::Regex => {
                    bind_variables.push(SQLValue::String(value));
                    wrap(format!("{column} ~ ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: string)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::CreatedAt
        | FilterField::CreatedAtFrom
        | FilterField::CreatedAtTo
        | FilterField::UpdatedAt
        | FilterField::UpdatedAtFrom
        | FilterField::UpdatedAtTo => {
            if !param.operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_date()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }

            let max = *values.iter().max().unwrap();
            let min = *values.iter().min().unwrap();

            match op {
                Operator::Equals => {
                    let array_sql = sql_date_array(&values, bind_variables);
                    wrap(format!("{column} = ANY({array_sql})"))
                }
                Operator::Gt => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} > ?"))
                }
                Operator::Gte => {
                    bind_variables.push(SQLValue::Date(max));
                    wrap(format!("{column} >= ?"))
                }
                Operator::Lt => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} < ?"))
                }
                Operator::Lte => {
                    bind_variables.push(SQLValue::Date(min));
                    wrap(format!("{column} <= ?"))
                }
                Operator::Between => {
                    if values.len() != 2 {
                        return Err(ApiError::OperatorMismatch(format!(
                            "Operator 'between' requires 2 values (min,max) for field '{}'",
                            param.field
                        )));
                    }
                    bind_variables.push(SQLValue::Date(values[0]));
                    bind_variables.push(SQLValue::Date(values[1]));
                    wrap(format!("{column} BETWEEN ? AND ?"))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: date)",
                        param.operator, param.field
                    )));
                }
            }
        }
        FilterField::Path => {
            if !param.operator.is_applicable_to(DataType::Array) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    param.operator, param.field
                )));
            }

            let values = param.value_as_integer()?;
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    param.field
                )));
            }
            let array_sql = sql_integer_array(&values, bind_variables);
            match op {
                Operator::Contains => wrap(format!("{column} @> {array_sql}")),
                Operator::Equals => wrap(format!("{column} = {array_sql}")),
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: array)",
                        param.operator, param.field
                    )));
                }
            }
        }
        _ => {
            return Err(ApiError::BadRequest(format!(
                "Field '{}' isn't searchable (or does not exist) for object relations",
                param.field
            )));
        }
    };

    Ok(Some(clause))
}

impl<T: ?Sized> UserSearchBackend for T where T: SelfAccessors<User> + UserNamespaceAccessors {}

impl User {
    pub async fn search_users(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::users::dsl::{created_at, email, id, updated_at, username, users};

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = users.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, username),
                FilterField::Username => string_search!(base_query, param, operator, username),
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, User);

        trace_query!(base_query, "Searching users");

        let result = with_connection(pool, |conn| {
            base_query
                .select(users::all_columns())
                .distinct() // TODO: Is it the joins that makes this required?
                .load::<User>(conn)
        })?;

        Ok(result)
    }

    pub async fn count_users(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::users::dsl::{created_at, email, id, updated_at, username, users};

        let query_params = query_options.filters.clone();
        let mut base_query = users.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, username),
                FilterField::Username => string_search!(base_query, param, operator, username),
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| {
            base_query
                .select(id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })
    }

    pub async fn search_groups(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::{
            created_at, description, groupname, groups, id, updated_at,
        };

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching groups",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut base_query = groups.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Groupname => string_search!(base_query, param, operator, groupname),
                FilterField::Description => {
                    string_search!(base_query, param, operator, description)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )));
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, Group);

        trace_query!(base_query, "Searching groups");

        let result = with_connection(pool, |conn| {
            base_query
                .select(groups::all_columns())
                .distinct()
                .load::<Group>(conn)
        })?;

        Ok(result)
    }

    pub async fn count_groups(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<i64, ApiError> {
        use crate::schema::groups::dsl::{
            created_at, description, groupname, groups, id, updated_at,
        };

        let query_params = query_options.filters.clone();
        let mut base_query = groups.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name => string_search!(base_query, param, operator, groupname),
                FilterField::Groupname => string_search!(base_query, param, operator, groupname),
                FilterField::Description => {
                    string_search!(base_query, param, operator, description)
                }
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for groups",
                        param.field
                    )));
                }
            }
        }

        with_connection(pool, |conn| {
            base_query
                .select(id)
                .distinct()
                .count()
                .get_result::<i64>(conn)
        })
    }
}
