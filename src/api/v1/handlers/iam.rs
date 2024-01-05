use crate::errors::ApiError;
use actix_web::ResponseError;
use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};
use diesel::ExpressionMethods;
use diesel::QueryDsl;
use diesel::RunQueryDsl;

use crate::models::group::{Group, GroupID, NewGroup, UpdateGroup};
use crate::models::user::{NewUser, UpdateUser, User, UserID};

use crate::utilities::db::handle_diesel_error;
use crate::utilities::response::{handle_result, json_response, json_response_created};

use serde_json::json;

use crate::db::connection::DbPool;

use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};

use tracing::debug;

#[get("/users")]
pub async fn get_users(
    pool: web::Data<DbPool>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    use crate::schema::users::dsl::users;
    use diesel::RunQueryDsl;

    debug!(
        message = "User list requested",
        requestor = requestor.user.username
    );

    let mut conn = pool.get()?;
    let result = users.load::<User>(&mut conn)?;
    Ok(json_response(result, StatusCode::OK))
}

#[post("/users")]
pub async fn create_user(
    pool: web::Data<DbPool>,
    new_user: web::Json<NewUser>,
    requestor: AdminAccess,
) -> impl Responder {
    debug!(
        message = "User create requested",
        requestor = requestor.user.id,
        new_user = ?new_user
    );

    let result = new_user.into_inner().save(&pool);

    match result {
        Ok(user) => {
            return json_response_created(format!("/api/v1/iam/users/{}", user.id).as_str());
        }
        Err(e) => e.error_response(),
    }
}

#[get("/users/{user_id}/tokens")]
pub async fn get_user_tokens(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> Result<impl Responder, ApiError> {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let uid = user_id.get_id();

    debug!(
        message = "User tokens requested",
        target = uid,
        requestor = requestor.user.id
    );

    let valid_tokens = crate::models::token::valid_tokens_for_user(&mut conn, uid);

    match valid_tokens {
        Ok(token_list) => Ok(json_response(token_list, StatusCode::OK)),
        Err(e) => Err(e),
    }
}

#[get("/users/{user_id}")]
pub async fn get_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: UserAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User get requested",
        target = user_id.get_id(),
        requestor = requestor.user.id
    );

    let user = user_id.get_user(&pool)?;
    Ok(json_response(user, StatusCode::OK))
}

#[patch("/users/{user_id}")]
pub async fn update_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>, // Extracting user_id from path
    updated_user: web::Json<UpdateUser>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User patch requested",
        target = user_id.get_id(),
        requestor = requestor.user.id
    );

    let user = updated_user.into_inner().save(user_id.get_id(), &pool)?;
    Ok(json_response(user, StatusCode::OK))
}

#[delete("/users/{user_id}")]
pub async fn delete_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
) -> Result<impl Responder, ApiError> {
    debug!(
        message = "User delete requested",
        target = user_id.get_id(),
        requestor = requestor.user.id
    );

    let delete_result = user_id.delete(&pool);

    match delete_result {
        Ok(elements) => Ok(json_response(json!(elements), StatusCode::NO_CONTENT)),
        Err(e) => Err(e),
    }
}

#[post("/groups")]
pub async fn create_group(
    pool: web::Data<DbPool>,
    new_group: web::Json<NewGroup>,
    requestor: AdminAccess,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    debug!(
        message = "Group create requested",
        requestor = requestor.user.id,
        new_group = ?new_group
    );

    let result = diesel::insert_into(crate::schema::groups::table)
        .values(&*new_group)
        .execute(&mut conn)
        .map_err(|e| handle_diesel_error(e, "Groupname already exists"));

    match result {
        Ok(res) => {
            return json_response_created(format!("/api/v1/iam/groups/{}", res).as_str());
        }
        Err(e) => e.error_response(),
    }
}

#[get("/groups/{group_id}/members")]
pub async fn get_group_members(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
) -> impl Responder {
    use crate::schema::user_groups::dsl::{group_id as group_id_column, user_groups, user_id};
    use crate::schema::users::dsl::*;
    use diesel::query_dsl::*;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let group_id = group_id.into_inner();

    debug!(
        message = "Group members requested",
        target = group_id,
        requestor = requestor.user.id
    );

    let result = user_groups
        .filter(group_id_column.eq(group_id))
        .inner_join(users.on(id.eq(user_id)))
        .select((id, username, password, email))
        .load::<User>(&mut conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()));

    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[post("/groups/{group_id}/members")]
pub async fn add_group_member(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
    user_id: web::Json<UserID>,
) -> impl Responder {
    let user_id = user_id.into_inner();
    let group_id = group_id.into_inner();

    debug!(
        message = "Adding user to group",
        user = user_id.get_id(),
        group = group_id,
        requestor = requestor.user.id
    );

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result = crate::utilities::iam::add_user_to_group(&mut conn, user_id.get_id(), group_id);
    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[delete("/groups/{group_id}/members")]
pub async fn delete_group_member(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
    user_id: web::Json<UserID>,
) -> impl Responder {
    let user_id = user_id.into_inner();
    let group_id = group_id.into_inner();

    debug!(
        message = "Deleting user from group",
        user = user_id.get_id(),
        group = group_id,
        requestor = requestor.user.id
    );

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result =
        crate::utilities::iam::delete_user_from_group(&mut conn, user_id.get_id(), group_id);
    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[get("/groups/{group_id}")]
pub async fn get_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: UserAccess,
) -> impl Responder {
    use crate::schema::groups::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let group_id = group_id.into_inner();

    debug!(
        message = "Group get requested",
        target = group_id,
        requestor = requestor.user.id
    );

    let result = groups
        .find(group_id)
        .first::<Group>(&mut conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()));

    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[get("/groups")]
pub async fn get_groups(pool: web::Data<DbPool>, requestor: UserAccess) -> impl Responder {
    use crate::schema::groups::dsl::*;
    use diesel::RunQueryDsl;

    debug!(
        message = "Group list requested",
        requestor = requestor.user.id
    );

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result = groups
        .load::<Group>(&mut conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()));
    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[patch("/groups/{group_id}")]
pub async fn update_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    updated_group: web::Json<UpdateGroup>,
    requestor: AdminAccess,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let group_id = group_id.into_inner();

    debug!(
        message = "Group patch requested",
        target = group_id,
        requestor = requestor.user.id
    );

    let result = diesel::update(crate::schema::groups::table.find(group_id))
        .set(&*updated_group)
        .execute(&mut conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()));

    handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR)
}

#[delete("/groups/{group_id}")]
pub async fn delete_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<GroupID>,
    requestor: AdminAccess,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let group_id = group_id.into_inner();

    debug!(
        message = "Group delete requested",
        target = group_id,
        requestor = requestor.user.id
    );

    let result = diesel::delete(crate::schema::groups::table.find(group_id))
        .execute(&mut conn)
        .map_err(|e| ApiError::DatabaseError(e.to_string()));
    handle_result(
        result,
        StatusCode::NO_CONTENT,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
}
