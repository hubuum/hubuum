extern crate argon2;

use crate::models::token::Token;

use argon2::{
    Argon2,
    password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash},
};

use rand::{RngExt, distr::Alphanumeric, rng};
use sha2::{Digest, Sha512};

use tracing::debug;

/// Hash a plaintext password.
pub fn hash_password(password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes())
        .map_err(|e| format!("Failed to hash password: {}", e))?
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
/// use hubuum::utilities::auth::{hash_password, verify_password};
///
/// let hashed_password = hash_password("correct horse battery staple").unwrap();
///
/// assert!(verify_password("correct horse battery staple", &hashed_password).unwrap());
/// assert!(!verify_password("wrong password", &hashed_password).unwrap());
/// ```
pub fn verify_password(password: &str, hash: &str) -> Result<bool, argon2::Error> {
    let parsed_hash_result = PasswordHash::new(hash);

    let parsed_hash = match parsed_hash_result {
        Ok(parsed_hash) => parsed_hash,
        Err(_) => {
            debug!(message = "Error parsing password hash.");
            return Ok(false);
        }
    };

    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

pub fn generate_random_password(length: usize) -> String {
    let mut rng = rng();
    std::iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(length)
        .collect()
}

pub fn generate_token() -> Token {
    let raw = generate_random_password(64);
    let mut hasher = Sha512::new();
    hasher.update(raw);
    let result = hasher.finalize();
    Token(result.iter().map(|byte| format!("{byte:02x}")).collect())
}
