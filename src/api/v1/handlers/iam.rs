use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};

use crate::models::group::{Group, NewGroup, UpdateGroup};
use crate::models::user::{NewUser, UpdateUser, User, UserID};

use crate::utilities::response::{handle_result, json_response};

use crate::models::user::PasswordHashable;
use serde_json::json;

use crate::db::connection::DbPool;

use crate::extractors::{AdminAccess, AdminOrSelfAccess, UserAccess};

use tracing::debug;

use diesel::prelude::*;
use diesel::QueryDsl;

#[get("/users")]
pub async fn get_users(pool: web::Data<DbPool>, requestor: UserAccess) -> impl Responder {
    use crate::schema::users::dsl::users;
    use diesel::RunQueryDsl;

    debug!(
        message = "User list requested",
        requestor = requestor.user.username
    );

    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let result = users.load::<User>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[post("/users")]
pub async fn create_user(
    pool: web::Data<DbPool>,
    new_user: web::Json<NewUser>,
    requestor: AdminAccess,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    debug!(
        message = "User create requested",
        requestor = requestor.user.id,
        new_user = ?new_user
    );

    let mut new_user = new_user.into_inner();

    if let Err(error_message) = new_user.hash_password() {
        return json_response(
            json!({ "message": error_message }),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    let result = diesel::insert_into(crate::schema::users::table)
        .values(&new_user)
        .execute(&mut conn);

    return handle_result(
        result,
        StatusCode::CREATED,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
}

#[get("/users/{user_id}/tokens")]
pub async fn get_user_tokens(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminOrSelfAccess,
) -> impl Responder {
    use crate::schema::tokens::dsl::{expires, tokens, user_id as user_id_column};
    use chrono::prelude::Utc;
    use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let now = Utc::now().naive_utc();

    let uid = user_id.into_inner();

    debug!(
        message = "User tokens requested",
        target = uid,
        requestor = requestor.user.id
    );

    let token_result = tokens
        .filter(user_id_column.eq(uid))
        .filter(expires.gt(now))
        .load::<crate::models::token::Token>(&mut conn);

    return handle_result(
        token_result,
        StatusCode::OK,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
}

#[get("/users/{user_id}")]
pub async fn get_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: UserAccess,
) -> impl Responder {
    use crate::schema::users::dsl::*;
    use diesel::RunQueryDsl;

    let user_id = user_id.into_inner();

    debug!(
        message = "User get requested",
        target = user_id,
        requestor = requestor.user.id
    );

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result = users.find(user_id).first::<User>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[patch("/users/{user_id}")]
pub async fn update_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>, // Extracting user_id from path
    updated_user: web::Json<UpdateUser>,
    requestor: AdminAccess,
) -> impl Responder {
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let user_id = user_id.into_inner();
    let mut updated_user = updated_user.into_inner();

    debug!(
        message = "User patch requested",
        target = user_id,
        requestor = requestor.user.id
    );

    if let Err(error_message) = updated_user.hash_password() {
        return json_response(
            json!({ "message": error_message }),
            StatusCode::INTERNAL_SERVER_ERROR,
        );
    }

    let result = diesel::update(users.filter(id.eq(user_id)))
        .set(&updated_user)
        .execute(&mut conn);

    return handle_result(
        result,
        StatusCode::NO_CONTENT,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
}

#[delete("/users/{user_id}")]
pub async fn delete_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<UserID>,
    requestor: AdminAccess,
) -> impl Responder {
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let user_id = user_id.into_inner();

    debug!(
        message = "User delete requested",
        target = user_id,
        requestor = requestor.user.id
    );

    // Perform the delete operation
    let result = diesel::delete(users.filter(id.eq(user_id))).execute(&mut conn);
    return handle_result(
        result,
        StatusCode::NO_CONTENT,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
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
        .execute(&mut conn);

    return handle_result(
        result,
        StatusCode::CREATED,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
}

#[get("/groups/{group_id}")]
pub async fn get_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<i32>,
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

    let result = groups.find(group_id).first::<Group>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
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
    let result = groups.load::<Group>(&mut conn);
    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[patch("/groups/{group_id}")]
pub async fn update_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<i32>,
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
        .execute(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[delete("/groups/{group_id}")]
pub async fn delete_group(
    pool: web::Data<DbPool>,
    group_id: web::Path<i32>,
    requestor: AdminAccess,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let group_id = group_id.into_inner();

    debug!(
        message = "Group delete requested",
        target = group_id,
        requestor = requestor.user.id
    );

    let result = diesel::delete(crate::schema::groups::table.find(group_id)).execute(&mut conn);
    handle_result(
        result,
        StatusCode::NO_CONTENT,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
}
