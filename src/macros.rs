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

#[macro_export]
/// Check permissions for a user on a namespace, class, or object.
///
/// ## Arguments
///
/// * `request_obj` - The request object (namespace, class, or object).
/// * `pool` - The database pool.
/// * `user` - The user to check permissions for (will be cloned).
/// * `permission+` - The permissions to check for, one or more.
///
/// ## Returns
///
/// This macro causes a return with a `ApiError::Forbidden` if the user does
/// not have the specified permission.
///
/// ## Example
///
/// ```
/// check_permissions!(namespace, pool, requestor.user, Permissions::ReadCollection);
/// check_permissions!(namespace, pool, requestor.user, Permissions::ReadCollection, Permissions::UpdateCollection);
///
/// ```
macro_rules! check_permissions {
    // Captures any number of permissions passed after the user argument and converts them into a vector
    ($request_obj:expr, $pool:expr, $user:expr, $($permissions:expr),+ $(,)?) => {{
        use $crate::errors::ApiError;
        use $crate::traits::NamespaceAccessors;
        use tracing::warn;

        let permissions_vec = vec![$($permissions),+];

        if !$request_obj
            .user_can_all(&$pool, $user.clone(), permissions_vec.clone())
            .await?
        {
            let namespace_id = $request_obj.namespace_id(&$pool).await?;
            let user_id = $user.id();
            warn!(
                message = "Permission denied",
                user_id = user_id,
                namespace_id = namespace_id,
                permissions = ?permissions_vec,
            );
            return Err(ApiError::Forbidden(format!(
                "User {} does not have permissions {:?} on namespace {}",
                user_id, permissions_vec, namespace_id
            )));
        }
    }};
}

#[macro_export]
/// A numeric search macro
macro_rules! numeric_search {
    ($base_query:expr, $parsed_query_param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};
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
                $parsed_query_param.field,
            )));
        }

        let max = values.iter().max().unwrap();
        let min = values.iter().min().unwrap();

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
        if op == Operator::Equals && values.len() > 50 {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator 'equals' is limited to 50 values, got {} (use between?)",
                values.len()
            )));
        }

        match (op, negated) {
            (Operator::Equals, false) => {
                $base_query = $base_query.filter($diesel_field.eq_any(values.clone()))
            }
            (Operator::Equals, true) => {
                $base_query = $base_query.filter(not($diesel_field.eq_any(values.clone())))
            }
            (Operator::Gt, false) => {
                $base_query = $base_query.filter($diesel_field.gt(max.clone()))
            }
            (Operator::Gt, true) => $base_query = $base_query.filter($diesel_field.le(max.clone())),
            (Operator::Gte, false) => {
                $base_query = $base_query.filter($diesel_field.ge(max.clone()))
            }
            (Operator::Gte, true) => {
                $base_query = $base_query.filter($diesel_field.lt(max.clone()))
            }
            (Operator::Lt, false) => {
                $base_query = $base_query.filter($diesel_field.lt(min.clone()))
            }
            (Operator::Lt, true) => $base_query = $base_query.filter($diesel_field.ge(min.clone())),
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
                $base_query = $base_query.filter(not($diesel_field.between(values[0], values[1])))
            }
            _ => {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' not implemented for field '{}'",
                    $operator, $parsed_query_param.field
                )))
            }
        };
    }};
}

#[macro_export]
/// A date search macro
macro_rules! date_search {
    ($base_query:expr, $parsed_query_param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use diesel::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};

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

        let max = values.iter().max().unwrap();
        let min = values.iter().min().unwrap();

        let (op, negated) = $operator.op_and_neg();

        if op == Operator::Between && values.len() != 2 {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator 'between' requires 2 values (min,max) for field '{}'",
                $parsed_query_param.field
            )));
        }

        match (op, negated) {
            (Operator::Equals, false) => {
                $base_query = $base_query.filter($diesel_field.eq_any(values.clone()))
            }
            (Operator::Equals, true) => {
                $base_query = $base_query.filter(not($diesel_field.eq_any(values.clone())))
            }
            (Operator::Gt, false) => {
                $base_query = $base_query.filter($diesel_field.gt(max.clone()))
            }
            (Operator::Gt, true) => $base_query = $base_query.filter($diesel_field.le(max.clone())),
            (Operator::Gte, false) => {
                $base_query = $base_query.filter($diesel_field.ge(max.clone()))
            }
            (Operator::Gte, true) => {
                $base_query = $base_query.filter($diesel_field.lt(max.clone()))
            }
            (Operator::Lt, false) => {
                $base_query = $base_query.filter($diesel_field.lt(min.clone()))
            }
            (Operator::Lt, true) => $base_query = $base_query.filter($diesel_field.ge(min.clone())),
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
                $base_query = $base_query.filter(not($diesel_field.between(values[0], values[1])))
            }
            _ => {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' not implemented for field '{}'",
                    $operator, $parsed_query_param.field
                )))
            }
        };
    }};
}

#[macro_export]
/// A string search macro
macro_rules! string_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use diesel::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};

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
            (Operator::Equals, false) => $base_query = $base_query.filter($diesel_field.eq(value)),
            (Operator::Equals, true) => {
                $base_query = $base_query.filter(not($diesel_field.eq(value)))
            }
            (Operator::Contains, false) => {
                $base_query = $base_query.filter($diesel_field.like(format!("%{}%", value)))
            }
            (Operator::Contains, true) => {
                $base_query = $base_query.filter(not($diesel_field.like(format!("%{}%", value))))
            }
            (Operator::StartsWith, false) => {
                $base_query = $base_query.filter($diesel_field.like(format!("{}%", value)))
            }
            (Operator::StartsWith, true) => {
                $base_query = $base_query.filter(not($diesel_field.like(format!("{}%", value))))
            }
            (Operator::EndsWith, false) => {
                $base_query = $base_query.filter($diesel_field.like(format!("{}%", value)))
            }
            (Operator::EndsWith, true) => {
                $base_query = $base_query.filter(not($diesel_field.like(format!("{}%", value))))
            }
            (Operator::IContains, false) => {
                $base_query = $base_query.filter($diesel_field.ilike(format!("%{}%", value)))
            }
            (Operator::IContains, true) => {
                $base_query = $base_query.filter(not($diesel_field.ilike(format!("%{}%", value))))
            }
            _ => {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' not implemented for field '{}'",
                    $operator, $param.field
                )))
            }
        }
    }};
}

#[macro_export]
/// A boolean search macro
macro_rules! boolean_search {
    ($base_query:expr, $param:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};

        let value = $param.value_as_boolean()?;

        if !$operator.is_applicable_to(DataType::Boolean) {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator '{:?}' is not applicable to field '{}'",
                $operator, $param.field
            )));
        }

        let (op, negated) = $operator.op_and_neg();

        match (op, negated) {
            (Operator::Equals, false) => $base_query = $base_query.filter($diesel_field.eq(value)),
            (Operator::Equals, true) => {
                $base_query = $base_query.filter(not($diesel_field.eq(value)))
            }
            _ => {
                return Err(ApiError::OperatorMismatch(format!(
                    "Operator '{:?}' not implemented for field '{}'",
                    $operator, $param.field
                )))
            }
        }
    }};
}
