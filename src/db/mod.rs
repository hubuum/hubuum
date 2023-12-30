pub mod connection;

use crate::errors::ApiError;
use crate::extractors::BearerToken;

pub trait DatabaseOps {
    fn get_valid_token(&self, token: &str) -> Result<BearerToken, ApiError>;
}
