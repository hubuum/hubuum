use diesel::{ExpressionMethods, JoinOnDsl, QueryDsl, RunQueryDsl, Table};
use std::iter::IntoIterator;

use tracing::{debug, trace};

use crate::models::search::{
    FilterField, ParsedQueryParam, QueryOptions, QueryParamsExt, SearchOperator,
};
use crate::models::traits::ExpandNamespaceFromMap;
use crate::models::traits::user::UserNamespaceAccessors;
use crate::models::{
    Group, HubuumClass, HubuumClassExpanded, HubuumClassRelation, HubuumObject,
    HubuumObjectRelation, Namespace, NewUser, ObjectClosureView, Permissions, Token,
    UpdateUser, User, UserID, UserToken,
};
use crate::traits::{ClassAccessors, GroupAccessors, NamespaceAccessors, SelfAccessors};
use crate::utilities::auth::hash_password;
use crate::utilities::extensions::CustomStringExtensions;

use crate::db::{DbPool, with_connection};
use crate::errors::ApiError;

use crate::{date_search, numeric_search, string_search, trace_query};

impl User {
    pub async fn get_by_username(pool: &DbPool, username_arg: &str) -> Result<User, ApiError> {
        use crate::schema::users::dsl::*;

        with_connection(pool, |conn| {
            users.filter(username.eq(username_arg)).first::<User>(conn)
        })
    }

    /// Set a new password for a user
    ///
    /// The password will be hashed before storing it in the database, so the input should be the
    /// desired plaintext password.
    pub async fn set_password(&self, pool: &DbPool, new_password: &str) -> Result<(), ApiError> {
        use crate::schema::users::dsl::*;
        debug!(
            message = "Setting new password",
            id = self.id(),
            username = self.username,
        );
        let new_password = hash_password(new_password)
            .map_err(|e| ApiError::HashError(format!("Failed to hash password: {e}")))?;

        with_connection(pool, |conn| {
            diesel::update(users.filter(id.eq(self.id)))
                .set(password.eq(new_password))
                .execute(conn)
        })?;

        Ok(())
    }
}

pub trait StoreUserTokenRecord {
    async fn store_user_token_record(&self, pool: &DbPool, token_value: &Token)
        -> Result<(), ApiError>;
}

impl StoreUserTokenRecord for User {
    async fn store_user_token_record(
        &self,
        pool: &DbPool,
        token_value: &Token,
    ) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, user_id};

        with_connection(pool, |conn| {
            diesel::insert_into(crate::schema::tokens::table)
                .values((user_id.eq(self.id), token.eq(token_value.get_token())))
                .execute(conn)
        })?;
        Ok(())
    }
}

pub trait OwnedUserTokenRecord {
    async fn load_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError>;

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError>;

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

impl OwnedUserTokenRecord for User {
    async fn load_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<UserToken, ApiError> {
        use crate::schema::tokens::dsl::{token, tokens, user_id};

        with_connection(pool, |conn| {
            tokens
                .filter(user_id.eq(self.id))
                .filter(token.eq(token_value.get_token()))
                .first::<UserToken>(conn)
        })
    }

    async fn delete_owned_user_token_record(
        &self,
        token_value: &Token,
        pool: &DbPool,
    ) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{token, tokens, user_id};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(user_id.eq(self.id)))
                .filter(token.eq(token_value.get_token()))
                .execute(conn)
        })
    }

    async fn delete_all_user_tokens_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::tokens::dsl::{tokens, user_id};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(user_id.eq(self.id))).execute(conn)
        })
    }
}

pub trait DeleteUserRecord {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError>;
}

impl DeleteUserRecord for User {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| diesel::delete(users.filter(id.eq(self.id))).execute(conn))
    }
}

impl DeleteUserRecord for UserID {
    async fn delete_user_record(&self, pool: &DbPool) -> Result<usize, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| diesel::delete(users.filter(id.eq(self.0))).execute(conn))
    }
}

pub trait CreateUserRecord {
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl CreateUserRecord for NewUser {
    async fn create_user_record(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::users;

        with_connection(pool, |conn| {
            diesel::insert_into(users)
                .values(self)
                .get_result::<User>(conn)
        })
    }
}

pub trait UpdateUserRecord {
    async fn update_user_record(&self, user_id: i32, pool: &DbPool) -> Result<User, ApiError>;
}

impl UpdateUserRecord for UpdateUser {
    async fn update_user_record(&self, user_id: i32, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| {
            diesel::update(users.filter(id.eq(user_id)))
                .set(self)
                .get_result::<User>(conn)
        })
    }
}

pub trait DeleteTokenRecord {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError>;
}

impl DeleteTokenRecord for Token {
    async fn delete_token_record(&self, pool: &DbPool) -> Result<(), ApiError> {
        use crate::schema::tokens::dsl::{token, tokens};

        with_connection(pool, |conn| {
            diesel::delete(tokens.filter(token.eq(&self.0))).execute(conn)
        })?;
        Ok(())
    }
}

pub trait LoadUserRecord {
    async fn load_user_record(&self, pool: &DbPool) -> Result<User, ApiError>;
}

impl LoadUserRecord for User {
    async fn load_user_record(&self, _pool: &DbPool) -> Result<User, ApiError> {
        Ok(self.clone())
    }
}

impl LoadUserRecord for UserID {
    async fn load_user_record(&self, pool: &DbPool) -> Result<User, ApiError> {
        use crate::schema::users::dsl::{id, users};

        with_connection(pool, |conn| users.filter(id.eq(self.0)).first::<User>(conn))
    }
}

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

pub trait UserSearchBackend:
    SelfAccessors<User> + GroupAccessors + UserNamespaceAccessors
{
    async fn search_namespaces_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<Namespace>, ApiError> {
        use crate::schema::namespaces::dsl::{id as namespace_id, namespaces};
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

        let group_id_subquery = self.group_ids_subquery_from_backend();

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
            use crate::{date_search, numeric_search, string_search};
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
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for namespaces",
                        param.field
                    )))
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

    async fn search_classes_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassExpanded>, ApiError> {
        use crate::schema::hubuumclass::dsl::{
            hubuumclass, id as hubuum_class_id, namespace_id as hubuum_classes_nid,
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
                FilterField::JsonSchema => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for classes",
                        param.field
                    )))
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

    async fn search_objects_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObject>, ApiError> {
        use crate::schema::hubuumobject::dsl::{
            hubuumobject, id as hubuum_object_id, namespace_id as hubuum_object_nid,
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
            use crate::{date_search, numeric_search, string_search};
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
                FilterField::ClassId => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject::dsl::hubuum_class_id
                ),
                FilterField::JsonData => {}
                FilterField::Permissions => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for objects",
                        param.field
                    )))
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

    async fn search_class_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumClassRelation>, ApiError> {
        use crate::schema::hubuumclass_relation::dsl::{
            from_hubuum_class_id, hubuumclass_relation, to_hubuum_class_id,
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
                let classes = self.search_classes_from_backend(pool, query_options).await?;
                let class_ids: Vec<i32> = classes.iter().map(|c| c.id).collect();

                if class_ids.is_empty() {
                    debug!(
                        message = "Searching class relations with class names",
                        stage = "Class IDs",
                        user_id = self.id(),
                        result = "No class IDs found, returning empty result"
                    );
                    return Ok(vec![]);
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
            use crate::{date_search, numeric_search};
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
                FilterField::ClassFromName => {}
                FilterField::ClassToName => {}
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for class relations",
                        param.field
                    )))
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumClassRelation);

        trace_query!(base_query, "Searching class relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumclass_relation::all_columns())
                .distinct()
                .load::<HubuumClassRelation>(conn)
        })
    }

    async fn search_object_relations_from_backend(
        &self,
        pool: &DbPool,
        query_options: QueryOptions,
    ) -> Result<Vec<HubuumObjectRelation>, ApiError> {
        use crate::schema::hubuumobject_relation::dsl::{
            from_hubuum_object_id, hubuumobject_relation, to_hubuum_object_id,
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
            .namespaces(pool, &permissions_list)
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

        let mut base_query = hubuumobject_relation.into_boxed();

        base_query = base_query
            .filter(
                from_hubuum_object_id.eq_any(
                    crate::schema::hubuumobject::dsl::hubuumobject
                        .select(crate::schema::hubuumobject::id)
                        .filter(crate::schema::hubuumobject::namespace_id.eq_any(&namespace_ids)),
                ),
            )
            .filter(
                to_hubuum_object_id.eq_any(
                    crate::schema::hubuumobject::dsl::hubuumobject
                        .select(crate::schema::hubuumobject::id)
                        .filter(crate::schema::hubuumobject::namespace_id.eq_any(&namespace_ids)),
                ),
            );

        for param in query_params {
            use crate::{date_search, numeric_search};
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::id
                ),
                FilterField::ClassRelation => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::class_relation_id
                ),
                FilterField::ObjectFrom => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::from_hubuum_object_id
                ),
                FilterField::ObjectTo => numeric_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::to_hubuum_object_id
                ),
                FilterField::CreatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::created_at
                ),
                FilterField::UpdatedAt => date_search!(
                    base_query,
                    param,
                    operator,
                    crate::schema::hubuumobject_relation::dsl::updated_at
                ),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for object relations",
                        param.field
                    )))
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, HubuumObjectRelation);

        trace_query!(base_query, "Searching object relations");

        with_connection(pool, |conn| {
            base_query
                .select(hubuumobject_relation::all_columns())
                .distinct()
                .load::<HubuumObjectRelation>(conn)
        })
    }

    async fn search_objects_related_to_from_backend<O>(
        &self,
        pool: &DbPool,
        object: O,
        query_options: QueryOptions,
    ) -> Result<Vec<ObjectClosureView>, ApiError>
    where
        O: SelfAccessors<HubuumObject> + ClassAccessors,
    {
        use crate::schema::object_closure_view::dsl as obj;

        let query_params = query_options.filters.clone();

        debug!(
            message = "Searching objects related to object",
            stage = "Starting",
            user_id = self.id(),
            object_id = object.id(),
            query_params = ?query_params
        );

        let object_param = ParsedQueryParam::new(
            &FilterField::ObjectFrom.to_string(),
            Some(SearchOperator::Equals { is_negated: false }),
            &object.id().to_string(),
        )?;

        let mut query_params = query_params;
        query_params.push(object_param);

        debug!(
            message = "Searching object relations related to object",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut permissions_list = query_params.permissions()?;
        permissions_list
            .ensure_contains(&[Permissions::ReadObject, Permissions::ReadObjectRelation]);

        let namespace_ids: Vec<i32> = self
            .namespaces(pool, &permissions_list)
            .await?
            .into_iter()
            .map(|n| n.id)
            .collect();

        if namespace_ids.is_empty() {
            debug!(
                message = "Searching object relations related to object",
                stage = "Namespace IDs",
                user_id = self.id(),
                result = "No namespace IDs found, returning empty result"
            );
            return Ok(vec![]);
        }

        debug!(
            message = "Searching object relations related to object",
            stage = "Namespace IDs",
            user_id = self.id(),
            result = "Found namespace IDs",
            namespace_ids = ?namespace_ids
        );

        let mut base_query = obj::object_closure_view.into_boxed();
        base_query = base_query
            .filter(obj::ancestor_namespace_id.eq_any(&namespace_ids))
            .filter(obj::descendant_namespace_id.eq_any(&namespace_ids));

        for param in &query_params {
            use crate::{
                array_search, date_search, json_search, numeric_search, string_search,
            };
            let operator = param.operator.clone();
            match &param.field {
                FilterField::ObjectFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_object_id)
                }
                FilterField::Id | FilterField::ObjectTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_object_id)
                }
                FilterField::ClassFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_class_id)
                }
                FilterField::ClassId | FilterField::Classes | FilterField::ClassTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_class_id)
                }
                FilterField::Namespaces | FilterField::NamespaceId | FilterField::NamespacesTo => {
                    numeric_search!(base_query, param, operator, obj::descendant_namespace_id)
                }
                FilterField::NamespacesFrom => {
                    numeric_search!(base_query, param, operator, obj::ancestor_namespace_id)
                }
                FilterField::Name | FilterField::NameTo => {
                    string_search!(base_query, param, operator, obj::descendant_name)
                }
                FilterField::NameFrom => {
                    string_search!(base_query, param, operator, obj::ancestor_name)
                }
                FilterField::Description | FilterField::DescriptionTo => {
                    string_search!(base_query, param, operator, obj::descendant_description)
                }
                FilterField::DescriptionFrom => {
                    string_search!(base_query, param, operator, obj::ancestor_description)
                }
                FilterField::CreatedAt | FilterField::CreatedAtTo => {
                    date_search!(base_query, param, operator, obj::descendant_created_at)
                }
                FilterField::CreatedAtFrom => {
                    date_search!(base_query, param, operator, obj::ancestor_created_at)
                }
                FilterField::UpdatedAt | FilterField::UpdatedAtTo => {
                    date_search!(base_query, param, operator, obj::descendant_updated_at)
                }
                FilterField::UpdatedAtFrom => {
                    date_search!(base_query, param, operator, obj::ancestor_updated_at)
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
                FilterField::Depth => numeric_search!(base_query, param, operator, obj::depth),
                FilterField::Path => array_search!(base_query, param, operator, obj::path),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for object relations",
                        param.field
                    )))
                }
            }
        }

        crate::apply_query_options!(base_query, query_options, ObjectClosureView);

        trace_query!(base_query, "Searching object relations");

        with_connection(pool, |conn| {
            base_query
                .select(obj::object_closure_view::all_columns())
                .distinct()
                .load::<ObjectClosureView>(conn)
        })
    }
}

impl<T: ?Sized> UserSearchBackend for T where T: SelfAccessors<User> + GroupAccessors + UserNamespaceAccessors {}

pub trait UserPermissions: SelfAccessors<User> + GroupAccessors + GroupMemberships {
    /// ## Check if a user has a set of permissions in a set of namespaces
    ///
    /// All permissions must be present in all namespaces for the function to return true.
    ///
    /// ### Parameters
    ///
    /// * `pool` - A database connection pool
    /// * `permissions` - An iterable of permissions to check for
    /// * `namespaces` - An iterable of namespaces to check against
    ///
    /// ### Returns
    ///
    /// * Nothing if the user has the required permissions, or an ApiError::Forbidden if they do not.
    async fn can<P, N, I>(
        &self,
        pool: &DbPool,
        permissions: P,
        namespaces: I,
    ) -> Result<(), ApiError>
    where
        P: IntoIterator<Item = Permissions>,
        I: IntoIterator<Item = N>,
        N: NamespaceAccessors,
    {
        use crate::models::PermissionFilter;
        use diesel::{dsl::sql, sql_types::BigInt};
        use futures::stream::{self, StreamExt, TryStreamExt};
        use std::collections::HashSet;

        if self.is_admin(pool).await? {
            return Ok(());
        }

        let lookup_table = crate::schema::permissions::dsl::permissions;
        let group_id_field = crate::schema::permissions::dsl::group_id;
        let namespace_id_field = crate::schema::permissions::dsl::namespace_id;

        let group_id_subquery = self.group_ids_subquery_from_backend();

        let namespace_ids: HashSet<i32> = stream::iter(namespaces)
            .map(|ns| async move { ns.namespace_id(pool).await })
            // Batch the futures into groups of 5, to avoid overwhelming the database
            .buffered(5)
            .try_collect()
            .await?;

        let mut base_query = lookup_table
            .into_boxed()
            .filter(namespace_id_field.eq_any(&namespace_ids))
            .filter(group_id_field.eq_any(group_id_subquery));

        // Apply all permission filters
        for perm in permissions {
            base_query = perm.create_boxed_filter(base_query, true);
        }

        // Count the number of distinct namespaces that match all criteria
        let matching_namespaces_count = with_connection(pool, |conn| {
            base_query
                .select(sql::<BigInt>("COUNT(DISTINCT namespace_id)"))
                .first::<i64>(conn)
        })?;

        // Check if the count of matching namespaces equals the number of input namespaces
        if matching_namespaces_count as usize == namespace_ids.len() {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "User does not have the required permissions".to_string(),
            ))
        }
    }
}

impl UserPermissions for User {}
impl UserPermissions for UserID {}

pub trait GroupMemberships: SelfAccessors<User> {
    /// At some point, we need to get the name of the admin group. Right now it's hard coded.
    async fn admin_groupname(&self) -> Result<String, ApiError> {
        Ok(crate::config::get_config()?.admin_groupname.clone())
    }

    /// Check if the user is in a group by name
    ///
    /// This function checks if the user is a member of a group with the specified name.
    ///
    /// ## Parameters
    ///
    /// * `groupname_queried` - The name of the group to check for membership.
    /// * `pool` - The database connection pool.
    ///
    /// ## Returns
    ///
    /// * Ok(true) if the user is in the group
    /// * Ok(false) if the user is not in the group
    /// * Err(ApiError) if something failed.
    async fn is_in_group_by_name(
        &self,
        groupname_queried: &str,
        pool: &DbPool,
    ) -> Result<bool, ApiError> {
        use crate::schema::groups::dsl::{groupname, groups};
        use crate::schema::user_groups::dsl::{user_groups, user_id as ug_user_id};
        use diesel::dsl::{exists, select};

        let is_in_group = with_connection(pool, |conn| {
            select(exists(
                user_groups
                    .inner_join(groups)
                    .filter(ug_user_id.eq(self.id()))
                    .filter(groupname.eq(groupname_queried)),
            ))
            .get_result(conn)
        })?;

        trace!(
            message = "Group by name check result",
            user_id = self.id(),
            groupname = groupname_queried,
            is_in_group = is_in_group,
        );

        Ok(is_in_group)
    }

    /// Check if the user is an admin
    ///
    /// This function checks the user's admin status in the database, but checking if they are
    /// a member of the group with the name "admin".
    async fn is_admin(&self, pool: &DbPool) -> Result<bool, ApiError> {
        let is_admin = self
            .is_in_group_by_name(&self.admin_groupname().await?, pool)
            .await?;

        trace!(
            message = "Admin check result",
            user_id = self.id(),
            is_admin = is_admin,
        );

        Ok(is_admin)
    }
}

impl GroupMemberships for User {}
impl GroupMemberships for UserID {}

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    use crate::models::{Permissions as P, PermissionsList as PL};
    use crate::tests::{TestScope, create_test_group, create_user_with_params};
    use crate::traits::PermissionController;

    // user_idx, namespaces_idx, permissions, expected
    #[rstest]
    #[case::u1_ns1_classread_true(0, vec![0], vec![P::ReadClass], true)]
    #[case::u1_ns1_classcreate_true(0, vec![0], vec![P::CreateClass], true)]
    #[case::u1_ns1_classreadcreate_true(0, vec![0], vec![P::ReadClass, P::CreateClass], true)]
    #[case::u1_ns2_classdelete_true(0, vec![1], vec![P::DeleteClass], true)]
    #[case::u1_ns2_classcreate_true(0, vec![1], vec![P::CreateClass], true)]
    #[case::u1_ns2_classcreatedelete_true(0, vec![1], vec![P::CreateClass, P::DeleteClass], true)]
    #[case::u1_ns12_classcreate_true(0, vec![0, 1], vec![P::CreateClass], true)]
    #[case::u1_ns1_objectread_false(0, vec![0], vec![P::ReadObject], false)]
    #[case::u1_ns1_namespacecreate_false(0, vec![0], vec![P::ReadCollection], false)]
    #[case::u1_ns12_classreadcreate_false(0, vec![0, 1], vec![P::CreateClass, P::ReadClass], false)]
    #[case::u1_ns12_classreadcreatedelete_false(
        0,
        vec![0, 1],
        vec![P::CreateClass, P::ReadClass, P::DeleteClass],
        false
    )]
    #[case::u2_ns1_objectread_true(1, vec![0], vec![P::ReadObject], true)]
    #[case::u2_ns1_objectcreate_true(1, vec![0], vec![P::CreateObject], true)]
    #[case::u2_ns1_objectreadcreate_true(1, vec![0], vec![P::ReadObject, P::CreateObject], true)]
    #[case::u2_ns2_objectdelete_true(1, vec![1], vec![P::DeleteObject], true)]
    #[case::u2_ns2_objectcreate_true(1, vec![1], vec![P::CreateObject], true)]
    #[case::u2_ns2_objectcreatedelete_true(1, vec![1], vec![P::CreateObject, P::DeleteObject], true)]
    #[actix_web::test]
    async fn test_user_can(
        #[case] user_idx: usize,
        #[case] namespaces_idx: Vec<usize>,
        #[case] permissions: Vec<Permissions>,
        #[case] expected: bool,
    ) {
        let scope = TestScope::new();
        let pool = scope.pool.clone();
        let suffix = format!(
            "_{}_{}_{}_{}",
            user_idx,
            namespaces_idx
                .iter()
                .map(|&x| x.to_string())
                .collect::<Vec<String>>()
                .join("_"),
            permissions
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<String>>()
                .join("_"),
            expected
        );

        let namespaces = [
            scope
                .namespace_fixture(&format!("test_user_can_ns1_{suffix}"))
                .await,
            scope
                .namespace_fixture(&format!("test_user_can_ns2_{suffix}"))
                .await,
        ];
        let groups = [
            create_test_group(&pool).await,
            create_test_group(&pool).await,
        ];
        let users = [
            create_user_with_params(&pool, &format!("test_user_can_u1_{suffix}"), "foo").await,
            create_user_with_params(&pool, &format!("test_user_can_u2_{suffix}"), "foo").await,
        ];

        groups[0].add_member(&pool, &users[0]).await.unwrap();
        groups[1].add_member(&pool, &users[1]).await.unwrap();

        namespaces[0]
            .namespace
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::ReadClass]),
            )
            .await
            .unwrap();
        namespaces[1]
            .namespace
            .grant(
                &pool,
                groups[0].id,
                PL::new(vec![P::CreateClass, P::DeleteClass]),
            )
            .await
            .unwrap();

        namespaces[0]
            .namespace
            .grant(
                &pool,
                groups[1].id,
                PL::new(vec![P::CreateObject, P::ReadObject]),
            )
            .await
            .unwrap();
        namespaces[1]
            .namespace
            .grant(
                &pool,
                groups[1].id,
                PL::new(vec![P::CreateObject, P::DeleteObject]),
            )
            .await
            .unwrap();

        let user = &users[user_idx];
        let namespaces = namespaces_idx
            .iter()
            .map(|i| &namespaces[*i].namespace)
            .collect::<Vec<_>>();

        let result = user.can(&pool, permissions, namespaces).await;

        match (result, expected) {
            (Ok(()), true) => {
                // Success case: We expected permission and got it
            }
            (Err(ApiError::Forbidden(_)), false) => {
                // Expected failure case: We expected no permission and got Forbidden error
            }
            (Ok(()), false) => {
                if user.is_admin(&pool).await.unwrap() {
                    panic!("Expected permission check to fail, but it succeeded (user is admin)");
                } else {
                    panic!("Expected permission check to fail, but it succeeded");
                }
            }
            (Err(ApiError::Forbidden(msg)), true) => {
                panic!("Expected permission check to succeed, but got Forbidden error: {msg}");
            }
            (Err(e), _) => {
                panic!("Unexpected error occurred: {e:?}");
            }
        }
    }
}
