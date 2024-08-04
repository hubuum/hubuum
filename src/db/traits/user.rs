use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl, Table};

use tracing::debug;

use crate::models::User;

use crate::traits::SelfAccessors;

use crate::db::DbPool;
use crate::errors::ApiError;

use crate::models::search::{FilterField, ParsedQueryParam};

use crate::{date_search, numeric_search, string_search, trace_query};

impl User {
    pub async fn search_users(
        &self,
        pool: &DbPool,
        query_params: Vec<ParsedQueryParam>,
    ) -> Result<Vec<User>, ApiError> {
        use crate::schema::users::dsl::*;

        debug!(
            message = "Searching users",
            stage = "Starting",
            user_id = self.id(),
            query_params = ?query_params
        );

        let mut conn = pool.get()?;

        let mut base_query = users.into_boxed();

        for param in query_params {
            let operator = param.operator.clone();
            match param.field {
                FilterField::Id => numeric_search!(base_query, param, operator, id),
                FilterField::Username => string_search!(base_query, param, operator, username),
                FilterField::Email => string_search!(base_query, param, operator, email),
                FilterField::CreatedAt => date_search!(base_query, param, operator, created_at),
                FilterField::UpdatedAt => date_search!(base_query, param, operator, updated_at),
                _ => {
                    return Err(ApiError::BadRequest(format!(
                        "Field '{}' isn't searchable (or does not exist) for users",
                        param.field
                    )))
                }
            }
        }

        trace_query!(base_query, "Searching users");

        let result = base_query
            .select(users::all_columns())
            .distinct() // TODO: Is it the joins that makes this required?
            .load::<User>(&mut conn)?;

        Ok(result)
    }
}
