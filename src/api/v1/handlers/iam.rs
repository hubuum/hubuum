use actix_web::{delete, get, http::StatusCode, patch, post, web, Responder};

use crate::models::group::{Group, NewGroup, UpdateGroup};
use crate::models::user::{NewUser, UpdateUser, User};
use crate::utilities::auth::hash_password;
use crate::utilities::response::{handle_result, json_response};

use serde_json::json;

use crate::db::connection::DbPool;

use tracing::debug;

use diesel::prelude::*;
use diesel::QueryDsl;

#[get("/users")]
pub async fn get_users(pool: web::Data<DbPool>) -> impl Responder {
    use crate::schema::users::dsl::*;
    use diesel::RunQueryDsl;

    debug!(message = "Retrieving users from database.");

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result = users.load::<User>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[post("/users")]
pub async fn create_user(pool: web::Data<DbPool>, new_user: web::Json<NewUser>) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let result = diesel::insert_into(crate::schema::users::table)
        .values(&*new_user)
        .execute(&mut conn);

    return handle_result(
        result,
        StatusCode::CREATED,
        StatusCode::INTERNAL_SERVER_ERROR,
    );
}

#[get("/users/{user_id}")]
pub async fn get_user(pool: web::Data<DbPool>, user_id: web::Path<i32>) -> impl Responder {
    use crate::schema::users::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let user_id = user_id.into_inner();
    let result = users.find(user_id).first::<User>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[patch("/users/{user_id}")]
pub async fn update_user(
    pool: web::Data<DbPool>,
    user_id: web::Path<i32>, // Extracting user_id from path
    updated_user: web::Json<UpdateUser>,
) -> impl Responder {
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let user_id = user_id.into_inner();
    let updated_user = updated_user.into_inner();

    // If we get a password update, we need to hash it
    let updated_user = if updated_user.password.is_some() {
        let hashed_password_result = hash_password(&updated_user.password.unwrap());

        match hashed_password_result {
            Ok(hashed_password) => UpdateUser {
                password: Some(hashed_password),
                ..updated_user
            },
            Err(_) => {
                return json_response(
                    json!({ "message": "Internal authentication failure, please try again later."}),
                    StatusCode::UNAUTHORIZED,
                )
            }
        }
    } else {
        updated_user
    };

    // Perform the update operation
    let result = diesel::update(users.filter(id.eq(user_id)))
        .set(&updated_user)
        .execute(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[delete("/users/{user_id}")]
pub async fn delete_user(pool: web::Data<DbPool>, user_id: web::Path<i32>) -> impl Responder {
    use crate::schema::users::dsl::*;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let user_id = user_id.into_inner();

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
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

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
pub async fn get_group(pool: web::Data<DbPool>, group_id: web::Path<i32>) -> impl Responder {
    use crate::schema::groups::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let group_id = group_id.into_inner();
    let result = groups.find(group_id).first::<Group>(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[get("/groups")]
pub async fn get_groups(pool: web::Data<DbPool>) -> impl Responder {
    use crate::schema::groups::dsl::*;
    use diesel::RunQueryDsl;

    let mut conn = pool.get().expect("couldn't get db connection from pool");
    let result = groups.load::<Group>(&mut conn);
    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[patch("/groups/{group_id}")]
pub async fn update_group(
    pool: web::Data<DbPool>,
    group_id: web::Json<i32>,
    updated_group: web::Json<UpdateGroup>,
) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let group_id = group_id.into_inner();
    let result = diesel::update(crate::schema::groups::table.find(group_id))
        .set(&*updated_group)
        .execute(&mut conn);

    return handle_result(result, StatusCode::OK, StatusCode::INTERNAL_SERVER_ERROR);
}

#[delete("/groups/{group_id}")]
pub async fn delete_group(pool: web::Data<DbPool>, group_id: web::Json<i32>) -> impl Responder {
    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let group_id = group_id.into_inner();
    let result = diesel::delete(crate::schema::groups::table.find(group_id)).execute(&mut conn);
    handle_result(
        result,
        StatusCode::NO_CONTENT,
        StatusCode::INTERNAL_SERVER_ERROR,
    )
}
