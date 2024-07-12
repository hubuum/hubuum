use crate::db::DbPool;
use crate::errors::ApiError;
use crate::extractors::{AdminAccess, UserAccess};
use crate::models::search::{parse_query_parameter, ParsedQueryParam};

use crate::traits::SelfAccessors;

use crate::utilities::response::json_response;
use tracing::debug;

use crate::traits::Search;

use actix_web::{get, http::StatusCode, patch, post, routes, web, HttpRequest, Responder};

#[routes]
#[get("")]
#[get("/")]
async fn get_class_relations(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
    req: HttpRequest,
) -> Result<impl Responder, ApiError> {
    let user = requestor.user;
    let query_string = req.query_string();

    let params = match parse_query_parameter(query_string) {
        Ok(params) => params,
        Err(e) => return Err(e),
    };

    debug!(message = "Listing class relations", user_id = user.id());

    let classes = user.search_class_relations(&pool, params).await?;

    Ok(json_response(classes, StatusCode::OK))
}
