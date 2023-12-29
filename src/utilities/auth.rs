extern crate argon2;

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use sha2::{Digest, Sha512};

use crate::db::connection::DbPool;

use tracing::debug;

// Function to hash a password
pub fn hash_password(password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string();

    Ok(password_hash)
}

/// Verify a plaintext password against a hashed password.
///
/// ## Arguments
///
/// * `plaintext_password` - A string slice that holds the plaintext password to be verified
/// * `hashed_password` - A string slice that holds the stored hash to be verified against
///
/// ## Example
///
/// ```
/// use crate::utilities::auth::verify_password;
///
/// let pwcheck = verify_password(plaintext_password, hashed_password);
///
/// if pwcheck.is_ok() {
///    println!("Password is valid!");
/// } else {
///   println!("Password is invalid!");
/// }
///```
pub fn verify_password(password: &str, hash: &str) -> Result<bool, argon2::Error> {
    let parsed_hash_result = PasswordHash::new(&hash);

    let parsed_hash = match parsed_hash_result {
        Ok(parsed_hash) => parsed_hash,
        Err(_) => {
            debug!(message = "Error parsing password hash.", hash = hash);
            return Ok(false);
        }
    };

    return Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok());
}

/// Validate a token against the hash stored in the database. Reasons for the
/// token to be invalid include:
///
/// * The token is not found in database
/// * The token has expired
///
/// ## Arguments
///
/// * `token` - A string slice that holds the token to be validated
/// * `pool` - A DbPool that holds the database connection pool
///
pub fn validate_token(token: &str, pool: &DbPool) -> bool {
    use crate::schema::tokens::dsl::{expires, token as token_column, tokens};
    use chrono::prelude::Utc;
    use diesel::prelude::{ExpressionMethods, QueryDsl, RunQueryDsl};

    let mut conn = pool.get().expect("couldn't get db connection from pool");

    let now = Utc::now().naive_utc();

    let token_result = tokens
        .filter(token_column.eq(token))
        .filter(expires.gt(now))
        .first::<crate::models::token::Token>(&mut conn);

    if token_result.is_err() {
        debug!(
            message = "Token validation failed.",
            error = token_result.err().unwrap().to_string()
        );
        return false;
    }

    true
}

pub fn generate_random_password(length: usize) -> String {
    let mut rng = thread_rng();
    std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(length)
        .collect()
}

pub fn generate_token() -> String {
    let raw = generate_random_password(64);
    let mut hasher = Sha512::new();
    hasher.update(raw);
    let result = hasher.finalize();
    format!("{:x}", result)
}
