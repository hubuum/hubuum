extern crate argon2;

use crate::models::token::Token;
use hubuum_domain::PlaintextPassword;

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

/// A completed password hash, never a plaintext input to the password worker.
#[derive(Clone)]
pub struct HashedPassword(String);

impl HashedPassword {
    pub fn from_encoded(encoded: String) -> Result<Self, PasswordWorkError> {
        PasswordHash::new(&encoded)
            .map_err(|_| PasswordWorkError("Invalid encoded password hash".into()))?;
        Ok(Self(encoded))
    }

    pub(crate) fn into_encoded(self) -> String {
        self.0
    }
}

impl fmt::Debug for HashedPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HashedPassword(<redacted>)")
    }
}

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

pub async fn verify_dummy_password_async(
    password: PlaintextPassword,
) -> Result<bool, PasswordWorkError> {
    run_password_work(move || {
        verify_dummy_password(password.as_str())
            .map_err(|error| PasswordWorkError(error.to_string()))
    })
    .await
}

/// Hash a plaintext password.
pub fn hash_password(password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let password = PlaintextPassword::new(password.to_string())?;
    hash_validated_password(&password)
}

fn hash_validated_password(
    password: &PlaintextPassword,
) -> Result<String, Box<dyn std::error::Error>> {
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(password.as_str().as_bytes())
        .map_err(|e| format!("Failed to hash password: {}", e))?
        .to_string();

    Ok(password_hash)
}

pub async fn hash_password_async(
    password: PlaintextPassword,
) -> Result<HashedPassword, PasswordWorkError> {
    run_password_work(move || {
        hash_validated_password(&password)
            .map(HashedPassword)
            .map_err(|error| PasswordWorkError(error.to_string()))
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
    if password.is_empty() {
        return Ok(false);
    }
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
    password: PlaintextPassword,
    hash: String,
) -> Result<bool, PasswordWorkError> {
    run_password_work(move || {
        verify_password(password.as_str(), &hash)
            .map_err(|error| PasswordWorkError(error.to_string()))
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

pub(crate) fn generate_token_secret() -> String {
    let raw = generate_random_password(64);
    let mut hasher = Sha512::new();
    hasher.update(raw);
    let result = hasher.finalize();
    result
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub fn generate_token() -> Token {
    Token::issued(generate_token_secret())
        .expect("token hash key-ring configuration must be valid at startup")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[test]
    fn completed_password_hash_debug_is_redacted() {
        let hash =
            HashedPassword::from_encoded(hash_password("redacted-password").unwrap()).unwrap();
        let debug = format!("{hash:?}");
        assert!(!debug.contains(&hash.into_encoded()));
    }

    #[test]
    fn plaintext_cannot_be_loaded_as_a_completed_hash() {
        assert!(HashedPassword::from_encoded("plaintext".into()).is_err());
    }

    #[test]
    fn empty_password_never_authenticates_even_against_a_legacy_empty_hash() {
        let hash = Argon2::default().hash_password(b"").unwrap().to_string();
        assert!(!verify_password("", &hash).unwrap());
    }

    #[rstest]
    #[case("correct horse battery staple", true)]
    #[case("wrong password", false)]
    #[tokio::test]
    async fn async_password_workers_hash_and_verify(
        #[case] candidate: &str,
        #[case] expected: bool,
    ) {
        let hash = hash_password_async(
            PlaintextPassword::new("correct horse battery staple".to_string()).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            verify_password_async(
                PlaintextPassword::new(candidate.to_string()).unwrap(),
                hash.into_encoded()
            )
            .await
            .unwrap(),
            expected
        );
    }
}
