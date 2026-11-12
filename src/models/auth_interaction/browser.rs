use hubuum_domain::{InteractionBrowserBinding, InteractionDigest};
use sha2::{Digest, Sha256};

use crate::errors::{ApiError, InteractionError};

fn digest(domain: &[u8], value: &str) -> InteractionDigest {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(value.as_bytes());
    InteractionDigest::new(super::hex(&hash.finalize())).expect("SHA-256 digest")
}

/// User codes locate a request but never authorize redemption.
pub struct BrowserUserCode(String);
impl BrowserUserCode {
    pub fn generate() -> Self {
        let bytes: [u8; 6] = rand::random();
        Self(super::hex(&bytes).to_ascii_uppercase())
    }
    pub fn parse(value: &str) -> Result<Self, ApiError> {
        if value.len() > 16 {
            return Err(InteractionError::Invalid.into());
        }
        let value: String = value.chars().filter(|c| *c != '-' && *c != ' ').collect();
        if value.len() != 12 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(InteractionError::Invalid.into());
        }
        Ok(Self(value.to_ascii_uppercase()))
    }
    pub fn display(&self) -> String {
        format!("{}-{}-{}", &self.0[..4], &self.0[4..8], &self.0[8..])
    }
    pub fn digest(&self) -> InteractionDigest {
        digest(b"hubuum:interaction:user-code:v1:", &self.0)
    }
}

/// HttpOnly cookie material is never accepted as a client redemption capability.
pub struct BrowserSession {
    cookie: String,
    csrf: String,
}
impl BrowserSession {
    fn random() -> String {
        let bytes: [u8; 32] = rand::random();
        super::hex(&bytes)
    }
    pub fn generate() -> Self {
        Self {
            cookie: Self::random(),
            csrf: Self::random(),
        }
    }
    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        let (cookie, csrf) = raw.split_once('.').ok_or(InteractionError::Invalid)?;
        if [cookie, csrf].iter().any(|part| {
            part.len() != 64
                || !part
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }) {
            return Err(InteractionError::Invalid.into());
        }
        Ok(Self {
            cookie: cookie.into(),
            csrf: csrf.into(),
        })
    }
    pub fn value(&self) -> String {
        format!("{}.{}", self.cookie, self.csrf)
    }
    pub fn csrf(&self) -> &str {
        &self.csrf
    }
    pub fn validate_csrf(&self, challenge: &str) -> Result<(), ApiError> {
        if challenge != self.csrf {
            return Err(InteractionError::Invalid.into());
        }
        Ok(())
    }
    pub fn rotate(&self) -> Self {
        Self {
            cookie: self.cookie.clone(),
            csrf: Self::random(),
        }
    }
    pub fn binding(&self) -> InteractionBrowserBinding {
        InteractionBrowserBinding::try_new(
            digest(b"hubuum:interaction:browser-cookie:v1:", &self.cookie),
            digest(b"hubuum:interaction:browser-csrf:v1:", &self.csrf),
        )
        .expect("independent domain-separated digests")
    }
}
