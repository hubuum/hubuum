use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::UserAccess;
use crate::utilities::response::json_response;
use actix_web::{get, http::StatusCode, web, Responder};
use tracing::debug;

use crate::models::traits::user::ClassAccessors;
use crate::traits::SelfAccessors;

// GET /api/v1/classes, list all classes the user may see.
#[get("")]
async fn list_classes(
    pool: web::Data<DbPool>,
    user: UserAccess,
) -> Result<impl Responder, ApiError> {
    let user = user.user;
    debug!(message = "Listing classes", user_id = user.id());

    let classes = user.classes_read(&pool).await?;
    Ok(json_response(classes, StatusCode::OK))
}
