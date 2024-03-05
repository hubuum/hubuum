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
    ($base_query:expr, $field:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};
        let values = $field.value_as_integer()?;

        if !$operator.is_applicable_to(DataType::Numeric) {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator {:?} is not applicable to field {}",
                $operator,
                stringify!($diesel_field)
            )));
        }

        // The values shouldn't be empty at this point, but we can make sure.
        if values.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "Searching on field {} requires a value",
                stringify!($diesel_field)
            )));
        }

        let max = values.iter().max().unwrap();
        let min = values.iter().min().unwrap();

        let (op, negated) = $operator.op_and_neg();

        if op == Operator::Between && values.len() != 2 {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator {:?} requires 2 values for field {}",
                $operator,
                stringify!($diesel_field)
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
                    "Operator {:?} not implemented for field {}",
                    $operator,
                    stringify!($diesel_field)
                )))
            }
        };
    }};
}

#[macro_export]
/// A date search macro
macro_rules! date_search {
    ($base_query:expr, $field:expr, $operator:expr, $diesel_field:expr) => {{
        use diesel::dsl::not;
        use diesel::prelude::*;
        use $crate::errors::ApiError;
        use $crate::models::search::{DataType, Operator, SearchOperator};

        let values = $field.value_as_date()?;

        if !$operator.is_applicable_to(DataType::Numeric) {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator {:?} is not applicable to field {}",
                $operator,
                stringify!($diesel_field)
            )));
        }

        // The values shouldn't be empty at this point, but we can make sure.
        if values.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "Searching on field {} requires a value",
                stringify!($diesel_field)
            )));
        }

        let max = values.iter().max().unwrap();
        let min = values.iter().min().unwrap();

        let (op, negated) = $operator.op_and_neg();

        if op == Operator::Between && values.len() != 2 {
            return Err(ApiError::OperatorMismatch(format!(
                "Operator {:?} requires 2 values for field {}",
                $operator,
                stringify!($diesel_field)
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
                    "Operator {:?} not implemented for field {}",
                    $operator,
                    stringify!($diesel_field)
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
                "Operator {:?} is not applicable to field {}",
                $operator,
                stringify!($diesel_field)
            )));
        }

        // The value shouldn't be empty at this point, but we can make sure.
        if value.is_empty() {
            return Err(ApiError::BadRequest(format!(
                "Searching on field {} requires a value",
                stringify!($diesel_field)
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
                    "Operator {:?} not implemented for field {}",
                    $operator,
                    stringify!($diesel_field)
                )))
            }
        }
    }};
}
