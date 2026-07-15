/// ## Trace log a (boxed) diesel query.
///
/// ### Arguments
///
/// * `base_query` - The base query to debug.
#[macro_export]
macro_rules! trace_query {
    ($base_query:expr, $context:expr) => {
        if tracing::level_enabled!(tracing::Level::TRACE) {
            let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&$base_query).to_string();
            tracing::trace!(message = "Query", context = $context, query = %debug_query);
        }
    };
}

/// ## Debug log a (boxed) diesel query.
///
/// ### Arguments
///
/// * `base_query` - The base query to debug.
#[macro_export]
macro_rules! debug_query {
    ($base_query:expr, $context:expr) => {{
        if tracing::level_enabled!(tracing::Level::DEBUG) {
            let debug_query = diesel::debug_query::<diesel::pg::Pg, _>(&$base_query).to_string();
            tracing::debug!(message = "Query", context = $context, query = %debug_query);
        }
    }};
}

diesel::infix_operator!(RegexMatch, " ~ ", backend: diesel::pg::Pg);

pub fn regex_match<T, U, ST>(left: T, right: U) -> RegexMatch<T, U::Expression>
where
    T: diesel::expression::Expression<SqlType = ST>,
    U: diesel::expression::AsExpression<ST>,
    ST: diesel::sql_types::SqlType + diesel::expression::TypedExpressionType,
{
    RegexMatch::new(left, right.as_expression())
}

#[macro_export]
/// ## Check if a user has a set of permissions in a set of collections.
///
/// This is a thin wrapper over the [`UserPermissions::can`] method, but with a more
/// convenient syntax for the caller as the objects we test against may be of different types
/// but all implement the [`CollectionAccessors`] trait.
///
/// ### Arguments
///
/// * `pool` - A database connection pool.
/// * `subject` - The principal (impl [`UserPermissions`]) to check permissions for.
/// * `scopes` - The token scope set as `Option<&[Permissions]>`. `None` = unscoped
///   (full authority); `Some(..)` intersects the check with the scopes (fail-closed,
///   applied even to admins). This argument is **required** — request handlers pass
///   `requestor.scopes()`; truly unscoped internal callers pass `None` explicitly.
/// * `[permissions]` - An iterable of [`Permissions`] to check for.
///   All permissions must be present in all collections.
/// * `objects+`- Objects to check permissions on (impl [`CollectionAccessors`]).
///
/// ### Returns
///
/// * Nothing if the subject has the required permissions, or an [`ApiError::Forbidden`] if they do not.
///
/// ### Example
///
/// ```text
/// use hubuum::can;
/// // `scopes` is `Option<&[Permissions]>` — e.g. `requestor.scopes()` or `None`.
/// can!(pool, subject, scopes, [Permissions::ReadCollection], collection, class, object);
/// can!(pool, subject, scopes, [Permissions::ReadCollection, Permissions::UpdateCollection], collection, class1, class2);
/// ```
///
/// [`UserPermissions::can`]: crate::db::traits::UserPermissions::can
/// [`UserPermissions`]: crate::db::traits::UserPermissions
/// [`CollectionAccessors`]: crate::traits::CollectionAccessors
/// [`Permissions`]: crate::models::Permissions
/// [`ApiError::Forbidden`]: crate::errors::ApiError::Forbidden
macro_rules! can {
    // Scope is a REQUIRED argument — `$scopes: Option<&[Permissions]>`. There is
    // deliberately no convenience form that defaults to `None`, so a missed
    // migration to scope-aware authorization is a compile error, not a silent
    // scope bypass. Resource/task handlers pass `requestor.scopes()`; truly
    // unscoped internal callers pass `None` explicitly.
    ($pool:expr, $subject:expr, $scopes:expr, [$($perm:expr),+], $($collection:expr),+) => {{
        #[allow(unused_imports)]
        use $crate::permissions::AuthzTarget as _;
        #[allow(unused_imports)]
        use $crate::traits::BackendContext as _;
        #[allow(unused_imports)]
        use $crate::traits::CollectionAccessors as _;

        match $crate::traits::BackendContext::permission_backend($pool) {
            Some(permission_backend) if !permission_backend.uses_sql_permission_store() => {
                let resources = vec![
                    $($collection.to_resource_ref($pool.db_pool()).await?),+
                ];
                $crate::permissions::authorize_resources(
                    permission_backend,
                    $pool.db_pool(),
                    $subject,
                    $scopes,
                    vec![$($perm),+],
                    resources,
                ).await?
            }
            _ => {
                $subject.can(
                    $pool.db_pool(),
                    vec![$($perm),+],
                    vec![
                        $($collection.collection_id($pool.db_pool()).await?),+
                    ],
                    $scopes,
                ).await?
            }
        }
    }};
}

#[macro_export]
/// Apply a permission flag filter to a boxed Diesel query that includes the
/// `permissions` table.
macro_rules! apply_permission_filter {
    ($base_query:ident, $permission:expr, $target:expr) => {{
        use $crate::db::prelude::*;
        use $crate::models::Permissions;
        use $crate::schema::permissions;

        $base_query = match $permission {
            Permissions::ReadCollection => {
                $base_query.filter(permissions::has_read_collection.eq($target))
            }
            Permissions::UpdateCollection => {
                $base_query.filter(permissions::has_update_collection.eq($target))
            }
            Permissions::DeleteCollection => {
                $base_query.filter(permissions::has_delete_collection.eq($target))
            }
            Permissions::DelegateCollection => {
                $base_query.filter(permissions::has_delegate_collection.eq($target))
            }
            Permissions::CreateClass => {
                $base_query.filter(permissions::has_create_class.eq($target))
            }
            Permissions::ReadClass => $base_query.filter(permissions::has_read_class.eq($target)),
            Permissions::UpdateClass => {
                $base_query.filter(permissions::has_update_class.eq($target))
            }
            Permissions::DeleteClass => {
                $base_query.filter(permissions::has_delete_class.eq($target))
            }
            Permissions::CreateObject => {
                $base_query.filter(permissions::has_create_object.eq($target))
            }
            Permissions::ReadObject => $base_query.filter(permissions::has_read_object.eq($target)),
            Permissions::UpdateObject => {
                $base_query.filter(permissions::has_update_object.eq($target))
            }
            Permissions::DeleteObject => {
                $base_query.filter(permissions::has_delete_object.eq($target))
            }
            Permissions::CreateClassRelation => {
                $base_query.filter(permissions::has_create_class_relation.eq($target))
            }
            Permissions::ReadClassRelation => {
                $base_query.filter(permissions::has_read_class_relation.eq($target))
            }
            Permissions::UpdateClassRelation => {
                $base_query.filter(permissions::has_update_class_relation.eq($target))
            }
            Permissions::DeleteClassRelation => {
                $base_query.filter(permissions::has_delete_class_relation.eq($target))
            }
            Permissions::CreateObjectRelation => {
                $base_query.filter(permissions::has_create_object_relation.eq($target))
            }
            Permissions::ReadObjectRelation => {
                $base_query.filter(permissions::has_read_object_relation.eq($target))
            }
            Permissions::UpdateObjectRelation => {
                $base_query.filter(permissions::has_update_object_relation.eq($target))
            }
            Permissions::DeleteObjectRelation => {
                $base_query.filter(permissions::has_delete_object_relation.eq($target))
            }
            Permissions::ReadTemplate => {
                $base_query.filter(permissions::has_read_template.eq($target))
            }
            Permissions::CreateTemplate => {
                $base_query.filter(permissions::has_create_template.eq($target))
            }
            Permissions::UpdateTemplate => {
                $base_query.filter(permissions::has_update_template.eq($target))
            }
            Permissions::DeleteTemplate => {
                $base_query.filter(permissions::has_delete_template.eq($target))
            }
            Permissions::ReadRemoteTarget => {
                $base_query.filter(permissions::has_read_remote_target.eq($target))
            }
            Permissions::CreateRemoteTarget => {
                $base_query.filter(permissions::has_create_remote_target.eq($target))
            }
            Permissions::UpdateRemoteTarget => {
                $base_query.filter(permissions::has_update_remote_target.eq($target))
            }
            Permissions::DeleteRemoteTarget => {
                $base_query.filter(permissions::has_delete_remote_target.eq($target))
            }
            Permissions::ExecuteRemoteTarget => {
                $base_query.filter(permissions::has_execute_remote_target.eq($target))
            }
            Permissions::ReadAudit => $base_query.filter(permissions::has_read_audit.eq($target)),
            Permissions::ManageEventSubscription => {
                $base_query.filter(permissions::has_manage_event_subscription.eq($target))
            }
        };
    }};
}

#[macro_export]
/// A numeric search macro
macro_rules! numeric_search {
    ($base_query:expr, $parsed_query_param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, ParsedQueryParamExt as _};

        let (op_pre, _) = $operator.op_and_neg();
        if op_pre == Operator::IsNull {
            $crate::is_null_search!($base_query, $parsed_query_param, $operator, $diesel_field);
        } else {
            let values = $parsed_query_param.value_as_integer()?;

            if !$operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    $operator, $parsed_query_param.field
                )));
            }

            // The values shouldn't be empty at this point, but we can make sure.
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    $parsed_query_param.field
                )));
            }

            let max = values.iter().max().ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Failed to determine max value for field '{}'",
                    $parsed_query_param.field
                ))
            })?;
            let min = values.iter().min().ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Failed to determine min value for field '{}'",
                    $parsed_query_param.field
                ))
            })?;

            let (op, negated) = $operator.op_and_neg();

            if op == Operator::Between && values.len() != 2 {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator 'between' requires 2 values (min,max) for field '{:?}'",
                    $operator,
                )));
            }

            // Sadly a sanity check. We want to use ranges and between for large sets,
            // but diesel is making it hard to create an "or" block inside the query.
            // Ie, we would ideally like to return a list of ints and a list of ranges
            // and combine them along the lines of
            // "WHERE field = any([1,3]) or (field BETWEEN 5 AND 7 OR field BETWEEN 11 AND 17)"
            // while merging with the rest of the filters via AND.
            if (op == Operator::Equals || op == Operator::In) && values.len() > 50 {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{}' is limited to 50 values, got {} (use between?)",
                    op,
                    values.len()
                )));
            }

            match (op, negated) {
                (Operator::Equals, false) | (Operator::In, false) => {
                    $base_query = $base_query.filter($diesel_field.eq_any(values.clone()))
                }
                (Operator::Equals, true) | (Operator::In, true) => {
                    $base_query = $base_query.filter(not($diesel_field.eq_any(values.clone())))
                }
                (Operator::Gt, false) => {
                    $base_query = $base_query.filter($diesel_field.gt(max.clone()))
                }
                (Operator::Gt, true) => {
                    $base_query = $base_query.filter($diesel_field.le(max.clone()))
                }
                (Operator::Gte, false) => {
                    $base_query = $base_query.filter($diesel_field.ge(max.clone()))
                }
                (Operator::Gte, true) => {
                    $base_query = $base_query.filter($diesel_field.lt(max.clone()))
                }
                (Operator::Lt, false) => {
                    $base_query = $base_query.filter($diesel_field.lt(min.clone()))
                }
                (Operator::Lt, true) => {
                    $base_query = $base_query.filter($diesel_field.ge(min.clone()))
                }
                (Operator::Lte, false) => {
                    $base_query = $base_query.filter($diesel_field.le(min.clone()))
                }
                (Operator::Lte, true) => {
                    $base_query = $base_query.filter($diesel_field.gt(min.clone()))
                }
                (Operator::Between, false) => {
                    $base_query = $base_query.filter($diesel_field.between(values[0], values[1]))
                }
                (Operator::Between, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.between(values[0], values[1])))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: numeric)",
                        $operator, $parsed_query_param.field
                    )));
                }
            };
        } // end else (not IsNull)
    }};
}

#[macro_export]
/// A date search macro
macro_rules! date_search {
    ($base_query:expr, $parsed_query_param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::db::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, ParsedQueryParamExt as _};

        let (op_pre, _) = $operator.op_and_neg();
        if op_pre == Operator::IsNull {
            $crate::is_null_search!($base_query, $parsed_query_param, $operator, $diesel_field);
        } else {
            let values = $parsed_query_param.value_as_date()?;

            if !$operator.is_applicable_to(DataType::NumericOrDate) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    $operator, $parsed_query_param.field
                )));
            }

            // The values shouldn't be empty at this point, but we can make sure.
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    $parsed_query_param.field
                )));
            }

            let max = values.iter().max().ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Failed to determine max value for field '{}'",
                    $parsed_query_param.field
                ))
            })?;
            let min = values.iter().min().ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "Failed to determine min value for field '{}'",
                    $parsed_query_param.field
                ))
            })?;

            let (op, negated) = $operator.op_and_neg();

            if op == Operator::Between && values.len() != 2 {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator 'between' requires 2 values (min,max) for field '{}'",
                    $parsed_query_param.field
                )));
            }

            match (op, negated) {
                (Operator::Equals, false) | (Operator::In, false) => {
                    $base_query = $base_query.filter($diesel_field.eq_any(values.clone()))
                }
                (Operator::Equals, true) | (Operator::In, true) => {
                    $base_query = $base_query.filter(not($diesel_field.eq_any(values.clone())))
                }
                (Operator::Gt, false) => {
                    $base_query = $base_query.filter($diesel_field.gt(max.clone()))
                }
                (Operator::Gt, true) => {
                    $base_query = $base_query.filter($diesel_field.le(max.clone()))
                }
                (Operator::Gte, false) => {
                    $base_query = $base_query.filter($diesel_field.ge(max.clone()))
                }
                (Operator::Gte, true) => {
                    $base_query = $base_query.filter($diesel_field.lt(max.clone()))
                }
                (Operator::Lt, false) => {
                    $base_query = $base_query.filter($diesel_field.lt(min.clone()))
                }
                (Operator::Lt, true) => {
                    $base_query = $base_query.filter($diesel_field.ge(min.clone()))
                }
                (Operator::Lte, false) => {
                    $base_query = $base_query.filter($diesel_field.le(min.clone()))
                }
                (Operator::Lte, true) => {
                    $base_query = $base_query.filter($diesel_field.gt(min.clone()))
                }
                (Operator::Between, false) => {
                    $base_query = $base_query.filter($diesel_field.between(values[0], values[1]))
                }
                (Operator::Between, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.between(values[0], values[1])))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: date)",
                        $operator, $parsed_query_param.field
                    )));
                }
            };
        } // end else (not IsNull)
    }};
}

#[macro_export]
/// A macro to search on a field with a list of values
macro_rules! array_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::db::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, ParsedQueryParamExt as _};

        let (op_pre, _) = $operator.op_and_neg();
        if op_pre == Operator::IsNull {
            $crate::is_null_search!($base_query, $param, $operator, $diesel_field);
        } else {
            let values = $param.value_as_integer()?;

            if !$operator.is_applicable_to(DataType::Array) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    $operator, $param.field
                )));
            }

            // The values shouldn't be empty at this point, but we can make sure.
            if values.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    $param.field
                )));
            }

            let (op, negated) = $operator.op_and_neg();

            match (op, negated) {
                (Operator::Contains, false) => {
                    $base_query = $base_query.filter($diesel_field.contains(values))
                }
                (Operator::Contains, true) => {
                    $base_query = $base_query.filter(not($diesel_field.contains(values)))
                }
                (Operator::Equals, false) => {
                    $base_query = $base_query.filter($diesel_field.eq(values))
                }
                (Operator::Equals, true) => {
                    $base_query = $base_query.filter(not($diesel_field.eq(values)))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: array)",
                        $operator, $param.field
                    )));
                }
            }
        } // end else (not IsNull)
    }};
}

#[macro_export]
/// A string search macro
macro_rules! string_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::db::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator};

        let (op_pre, _) = $operator.op_and_neg();
        if op_pre == Operator::IsNull {
            $crate::is_null_search!($base_query, $param, $operator, $diesel_field);
        } else {
            let value = $param.value.clone();

            if !$operator.is_applicable_to(DataType::String) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    $operator, $param.field
                )));
            }

            // The value shouldn't be empty at this point, but we can make sure.
            if value.is_empty() {
                return Err(ApiError::BadRequest(format!(
                    "Searching on field '{}' requires a value",
                    $param.field
                )));
            }

            let (op, negated) = $operator.op_and_neg();

            match (op, negated) {
                (Operator::Equals, false) => {
                    $base_query = $base_query.filter($diesel_field.eq(value))
                }
                (Operator::Equals, true) => {
                    $base_query = $base_query.filter(not($diesel_field.eq(value)))
                }
                (Operator::In, false) => {
                    let values: Vec<String> = value.split(',').map(|s| s.to_string()).collect();
                    $base_query = $base_query.filter($diesel_field.eq_any(values))
                }
                (Operator::In, true) => {
                    let values: Vec<String> = value.split(',').map(|s| s.to_string()).collect();
                    $base_query = $base_query.filter(not($diesel_field.eq_any(values)))
                }
                (Operator::IEquals, false) => {
                    $base_query = $base_query.filter($diesel_field.ilike(value))
                }
                (Operator::IEquals, true) => {
                    $base_query = $base_query.filter(not($diesel_field.ilike(value)))
                }
                (Operator::Contains, false) => {
                    $base_query = $base_query.filter($diesel_field.like(format!("%{}%", value)))
                }
                (Operator::Contains, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.like(format!("%{}%", value))))
                }
                (Operator::StartsWith, false) => {
                    $base_query = $base_query.filter($diesel_field.like(format!("{}%", value)))
                }
                (Operator::StartsWith, true) => {
                    $base_query = $base_query.filter(not($diesel_field.like(format!("{}%", value))))
                }
                (Operator::IStartsWith, false) => {
                    $base_query = $base_query.filter($diesel_field.ilike(format!("{}%", value)))
                }
                (Operator::IStartsWith, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.ilike(format!("{}%", value))))
                }
                (Operator::EndsWith, false) => {
                    $base_query = $base_query.filter($diesel_field.like(format!("%{}", value)))
                }
                (Operator::EndsWith, true) => {
                    $base_query = $base_query.filter(not($diesel_field.like(format!("%{}", value))))
                }
                (Operator::IContains, false) => {
                    $base_query = $base_query.filter($diesel_field.ilike(format!("%{}%", value)))
                }
                (Operator::IContains, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.ilike(format!("%{}%", value))))
                }
                (Operator::IEndsWith, false) => {
                    $base_query = $base_query.filter($diesel_field.ilike(format!("%{}", value)))
                }
                (Operator::IEndsWith, true) => {
                    $base_query =
                        $base_query.filter(not($diesel_field.ilike(format!("%{}", value))))
                }
                (Operator::Like, false) => {
                    $base_query = $base_query.filter($diesel_field.like(value))
                }
                (Operator::Like, true) => {
                    $base_query = $base_query.filter(not($diesel_field.like(value)))
                }
                (Operator::Regex, false) => {
                    $base_query =
                        $base_query.filter($crate::macros::regex_match($diesel_field, value))
                }
                (Operator::Regex, true) => {
                    $base_query =
                        $base_query.filter(not($crate::macros::regex_match($diesel_field, value)))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: string)",
                        $operator, $param.field
                    )));
                }
            }
        } // end else (not IsNull)
    }};
}

#[macro_export]
/// A boolean search macro
macro_rules! boolean_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, ParsedQueryParamExt as _};

        let (op_pre, _) = $operator.op_and_neg();
        if op_pre == Operator::IsNull {
            $crate::is_null_search!($base_query, $param, $operator, $diesel_field);
        } else {
            let value = $param.value_as_boolean()?;

            if !$operator.is_applicable_to(DataType::Boolean) {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' is not applicable to field '{}'",
                    $operator, $param.field
                )));
            }

            let (op, negated) = $operator.op_and_neg();

            match (op, negated) {
                (Operator::Equals, false) => {
                    $base_query = $base_query.filter($diesel_field.eq(value))
                }
                (Operator::Equals, true) => {
                    $base_query = $base_query.filter(not($diesel_field.eq(value)))
                }
                _ => {
                    return Err(ApiError::OperatorMismatch(format!(
                        "Operator '{:?}' not implemented for field '{}' (type: boolean)",
                        $operator, $param.field
                    )));
                }
            }
        } // end else (not IsNull)
    }};
}

#[macro_export]
/// A null check search macro for is_null operator on any field type
macro_rules! is_null_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use $crate::models::search::ParsedQueryParamExt as _;

        let is_null_value = $param.value_as_boolean()?;
        let (_, is_null_negated) = $operator.op_and_neg();
        let should_be_null = is_null_value != is_null_negated;
        if should_be_null {
            $base_query = $base_query.filter($diesel_field.is_null())
        } else {
            $base_query = $base_query.filter($diesel_field.is_not_null())
        }
    }};
}

/// Bind `TransitiveFilterParams` to a query in the agreed parameter order.
///
/// Parameter order bound by this macro:
/// 1. depth_op
/// 2. depth_values
/// 3. depth_negated
/// 4. path_op
/// 5. path_values
/// 6. path_negated
#[macro_export]
macro_rules! bind_transitive_filter_params {
    ($query:expr, $filter:expr) => {{
        $query
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>($filter.depth_op)
            .bind::<
                diesel::sql_types::Nullable<diesel::sql_types::Array<diesel::sql_types::Integer>>,
                _,
            >($filter.depth_values)
            .bind::<diesel::sql_types::Bool, _>($filter.depth_negated)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>($filter.path_op)
            .bind::<
                diesel::sql_types::Nullable<diesel::sql_types::Array<diesel::sql_types::Integer>>,
                _,
            >($filter.path_values)
            .bind::<diesel::sql_types::Bool, _>($filter.path_negated)
    }};
}

/// ## Declare an `i32`-backed id newtype with a validating constructor.
///
/// Generates the tuple struct (with a private field), a validating `new` that rejects
/// non-positive ids, an `id()` accessor for the rare persistence boundaries that still operate on
/// the raw `i32`, and a `Deserialize` impl routed through `new` so an invalid id is rejected at
/// the API edge with a clear `400` rather than surfacing later as a confusing lookup miss.
///
/// `new` and `id` are deliberately *inherent* rather than trait methods: an `id()` on a shared
/// trait would collide with [`SelfAccessors::id`](crate::traits::SelfAccessors::id), and a trait
/// constructor would have to expose an unchecked `from_raw`, defeating the private field.
///
/// ### Arguments
///
/// * `noun` - human-readable name used in the validation error, e.g. `"collection id"`.
///
/// ### Example
///
/// ```
/// use hubuum::int_id_newtype;
///
/// int_id_newtype! {
///     /// Identifier for a collection.
///     pub struct CollectionID;
///     noun = "collection id";
/// }
///
/// assert_eq!(CollectionID::new(42).unwrap().id(), 42);
/// assert!(CollectionID::new(0).is_err());
/// ```
#[macro_export]
macro_rules! int_id_newtype {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident;
        noun = $noun:literal $(;)?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, serde::Serialize, PartialEq, Eq, utoipa::ToSchema)]
        $vis struct $name(i32);

        impl $name {
            #[doc = concat!("Validating constructor: ", $noun, "s are positive integers.")]
            ///
            /// Constructing through `new` (and the `Deserialize` impl, which routes through it)
            /// means an invalid id is rejected at the edge with a clear `400` rather than
            /// surfacing later as a confusing lookup miss.
            pub fn new(id: i32) -> Result<Self, $crate::errors::ApiError> {
                if id <= 0 {
                    return Err($crate::errors::ApiError::BadRequest(format!(
                        concat!("Invalid ", $noun, " '{id}': must be a positive integer"),
                        id = id
                    )));
                }
                Ok(Self(id))
            }

            /// The underlying id. Use at persistence boundaries that still operate on the raw `i32`.
            pub fn id(self) -> i32 {
                self.0
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let id = <i32 as serde::Deserialize>::deserialize(deserializer)?;
                Self::new(id).map_err(serde::de::Error::custom)
            }
        }
    };
}
