use super::*;
pub trait LoadUserGroups: SelfAccessors<User> {
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError>;
}

impl<T: ?Sized> LoadUserGroups for T
where
    T: SelfAccessors<User>,
{
    async fn load_user_groups(&self, pool: &DbPool) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};

        with_connection(pool, |conn| {
            user_groups
                .inner_join(groups.on(id.eq(group_id)))
                .filter(user_id.eq(self.id()))
                .select(groups::all_columns())
                .load::<Group>(conn)
        })
    }
}

pub trait LoadUserGroupsPaginated: SelfAccessors<User> {
    async fn load_user_groups_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<Group>, ApiError>;
}

impl<T: ?Sized> LoadUserGroupsPaginated for T
where
    T: SelfAccessors<User>,
{
    async fn load_user_groups_paginated(
        &self,
        pool: &DbPool,
        query_options: &QueryOptions,
    ) -> Result<Vec<Group>, ApiError> {
        use crate::schema::groups::dsl::*;
        use crate::schema::user_groups::dsl::{group_id, user_groups, user_id};
        use crate::{date_search, numeric_search, string_search};

        let mut base_query = user_groups
            .inner_join(groups.on(id.eq(group_id)))
            .filter(user_id.eq(self.id()))
            .select(groups::all_columns())
            .into_boxed();

        for param in &query_options.filters {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Name | FilterField::Groupname => {
                    string_search!(base_query, param, operator, groupname)
                }
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

        with_connection(pool, |conn| base_query.load::<Group>(conn))
    }
}

pub trait GroupIdsSubqueryBackend: SelfAccessors<User> {
    fn group_ids_subquery_from_backend<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>;
}

impl<T: ?Sized> GroupIdsSubqueryBackend for T
where
    T: SelfAccessors<User>,
{
    fn group_ids_subquery_from_backend<'a>(
        &self,
    ) -> crate::schema::user_groups::BoxedQuery<'a, diesel::pg::Pg, diesel::sql_types::Integer>
    {
        use crate::schema::user_groups::dsl::*;

        user_groups
            .filter(user_id.eq(self.id()))
            .select(group_id)
            .into_boxed()
    }
}

pub trait QueryJsonSchemaIds: SelfAccessors<User> {
    fn query_class_ids_for_json_schema(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError>;
}

impl<T: ?Sized> QueryJsonSchemaIds for T
where
    T: SelfAccessors<User>,
{
    fn query_class_ids_for_json_schema(
        &self,
        pool: &DbPool,
        json_schema_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        use crate::models::class::ClassIdResult;
        use crate::models::search::SQLValue;

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
        Ok(result_ids.into_iter().map(|result| result.id).collect())
    }
}

pub trait QueryJsonDataIds: SelfAccessors<User> {
    fn query_object_ids_for_json_data(
        &self,
        pool: &DbPool,
        json_data_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError>;
}

impl<T: ?Sized> QueryJsonDataIds for T
where
    T: SelfAccessors<User>,
{
    fn query_object_ids_for_json_data(
        &self,
        pool: &DbPool,
        json_data_query_params: Vec<&ParsedQueryParam>,
    ) -> Result<Vec<i32>, ApiError> {
        use crate::models::object::ObjectIDResult;
        use crate::models::search::SQLValue;

        if json_data_query_params.is_empty() {
            return Err(ApiError::BadRequest(
                "No json_data query parameters provided".to_string(),
            ));
        }

        let raw_sql_prefix = "select id from hubuumobject where";
        let mut raw_sql_clauses: Vec<String> = vec![];
        let mut bind_varaibles: Vec<SQLValue> = vec![];

        for param in json_data_query_params {
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
        Ok(result_ids.into_iter().map(|result| result.id).collect())
    }
}

pub trait LoadPermittedNamespaces: SelfAccessors<User> + GroupAccessors {
    async fn load_namespaces_with_permissions<'a, I>(
        &self,
        pool: &DbPool,
        permissions_list: &'a I,
    ) -> Result<Vec<Namespace>, ApiError>
    where
        &'a I: IntoIterator<Item = &'a Permissions>;
}

impl<T: ?Sized> LoadPermittedNamespaces for T
where
    T: SelfAccessors<User> + GroupAccessors,
{
    async fn load_namespaces_with_permissions<'a, I>(
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

        let groups_id_subquery = self.group_ids_subquery_from_backend();

        let mut base_query = permissions
            .into_boxed()
            .filter(group_id.eq_any(groups_id_subquery));

        for perm in permissions_list {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        with_connection(pool, |conn| {
            base_query
                .inner_join(namespaces.on(namespace_id.eq(namespaces_table_id)))
                .select(namespaces::all_columns())
                .load::<Namespace>(conn)
        })
    }
}
