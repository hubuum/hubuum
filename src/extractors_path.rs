use crate::db::DbPool;
use crate::errors::ApiError;
use crate::models::user::User;
use crate::utilities::iam::get_user_by_id;

pub async fn get_user_and_path(
    path: &actix_web::dev::Path<actix_web::dev::Url>,
    pool: &DbPool,
) -> Result<(User, String), ApiError> {
    let user_id = match path.query("user_id").parse::<i32>() {
        Ok(id) => id,
        Err(_) => {
            return Err(ApiError::InternalServerError(
                "Failed to parse user_id".into(),
            ));
        }
    };
    let path = path.as_str().to_string();
    let user = get_user_by_id(pool, user_id)?;
    Ok((user, path))
}
