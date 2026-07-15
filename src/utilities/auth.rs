extern crate argon2;

use crate::models::token::Token;

use argon2::{
    Argon2,
    password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash},
};

use rand::{RngExt, distr::Alphanumeric, rng};
use sha2::{Digest, Sha512};
use std::fmt;
use std::sync::{Arc, LazyLock};
use tokio::sync::Semaphore;

use tracing::debug;

static DUMMY_PASSWORD_HASH: LazyLock<String> = LazyLock::new(|| {
    hash_password("hubuum-dummy-password-verification-target")
        .expect("the built-in dummy password must be hashable")
});

const PASSWORD_WORK_MAX_CONCURRENCY: usize = 4;

static PASSWORD_WORK_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(PASSWORD_WORK_MAX_CONCURRENCY)));

#[derive(Debug)]
pub struct PasswordWorkError(String);

impl fmt::Display for PasswordWorkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PasswordWorkError {}

async fn run_password_work<T, F>(work: F) -> Result<T, PasswordWorkError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, PasswordWorkError> + Send + 'static,
{
    let permit = PASSWORD_WORK_SEMAPHORE
        .clone()
        .acquire_owned()
        .await
        .map_err(|error| PasswordWorkError(format!("Password worker is unavailable: {error}")))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    })
    .await
    .map_err(|error| PasswordWorkError(format!("Password worker failed: {error}")))?
}

/// Initialize the dummy verifier used to make unknown-user login attempts perform
/// the same expensive password-hash work as wrong-password attempts.
pub fn initialize_dummy_password_hash() {
    LazyLock::force(&DUMMY_PASSWORD_HASH);
}

pub fn verify_dummy_password(password: &str) -> Result<bool, argon2::Error> {
    verify_password(password, &DUMMY_PASSWORD_HASH)
}

pub async fn verify_dummy_password_async(password: String) -> Result<bool, PasswordWorkError> {
    run_password_work(move || {
        verify_dummy_password(&password).map_err(|error| PasswordWorkError(error.to_string()))
    })
    .await
}

/// Hash a plaintext password.
pub fn hash_password(password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_bytes())
        .map_err(|e| format!("Failed to hash password: {}", e))?
        .to_string();

    Ok(password_hash)
}

pub async fn hash_password_async(password: String) -> Result<String, PasswordWorkError> {
    run_password_work(move || {
        hash_password(&password).map_err(|error| PasswordWorkError(error.to_string()))
    })
    .await
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

pub async fn verify_password_async(
    password: String,
    hash: String,
) -> Result<bool, PasswordWorkError> {
    run_password_work(move || {
        verify_password(&password, &hash).map_err(|error| PasswordWorkError(error.to_string()))
    })
    .await
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("correct horse battery staple", true)]
    #[case("wrong password", false)]
    #[tokio::test]
    async fn async_password_workers_hash_and_verify(
        #[case] candidate: &str,
        #[case] expected: bool,
    ) {
        let hash = hash_password_async("correct horse battery staple".to_string())
            .await
            .unwrap();
        assert_eq!(
            verify_password_async(candidate.to_string(), hash)
                .await
                .unwrap(),
            expected
        );
    }
}
