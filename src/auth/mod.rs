// src/auth/mod.rs
use crate::models::{Token, User};
use crate::schema::tokens::dsl::tokens;
use crate::schema::users::dsl::*;
use actix_web::{web, Error, FromRequest, HttpRequest};
use diesel::prelude::*;
use futures::future::{ready, Ready};

use crate::db::connection::DbPool;

pub struct AdminOnly(User);

impl FromRequest for AdminOnly {
    type Error = Error;
    type Future = Ready<Result<Self, Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        // Extract token and look up in DB
        // Assume `pool` is a Data<r2d2::Pool<ConnectionManager<PgConnection>>>
        let pool = req.app_data::<web::Data<DbPool>>().unwrap();

        // Your token extraction logic here

        match check_admin_user(&pool, &extracted_token) {
            Ok(user) if user.is_admin => ready(Ok(AdminOnly(user))),
            _ => ready(Err(ErrorUnauthorized("Unauthorized"))),
        }
    }
}

// Function to check if the user is admin
fn check_admin_user(
    pool: &web::Data<DbPool>,
    token_str: &str,
) -> Result<User, diesel::result::Error> {
    let connection = pool.get()?;
    tokens
        .filter(token.eq(token_str))
        .inner_join(users)
        .select(users::all_columns)
        .first::<User>(&connection)
}

pub struct AdminOrReadOnly(User);

impl FromRequest for AdminOrReadOnly {
    type Error = Error;
    type Future = Ready<Result<Self, Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, payload: &mut actix_web::dev::Payload) -> Self::Future {
        let pool = req.app_data::<web::Data<DbPool>>().unwrap();

        let user = match get_user_by_token(&pool, &extracted_token) {
            Ok(user) => user,
            Err(_) => return ready(Err(ErrorUnauthorized("Unauthorized"))),
        };

        match req.method() {
            // Allow all users for GET requests
            &Method::GET => ready(Ok(AdminOrReadOnly(user))),
            // Restrict other methods to admin users
            _ if user.is_admin => ready(Ok(AdminOrReadOnly(user))),
            _ => ready(Err(ErrorUnauthorized("Unauthorized"))),
        }
    }
}

// Function to get user by token
fn get_user_by_token(
    pool: &web::Data<DbPool>,
    token_str: &str,
) -> Result<User, diesel::result::Error> {
    let connection = pool.get()?;
    tokens
        .filter(token.eq(token_str))
        .inner_join(users)
        .select(users::all_columns)
        .first::<User>(&connection)
}
