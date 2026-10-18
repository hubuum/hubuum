use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hubuum_domain::ClassId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::errors::ApiError;
use crate::models::{HubuumObjectHistory, search::QueryOptions};
use crate::pagination::{MAX_ENCODED_CURSOR_BYTES, normalized_sorts};

/// Binds history continuation to its exact selection and authorization token.
pub(crate) struct HistoryNameCursor(String);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    scope: String,
    cursor: String,
}

impl HistoryNameCursor {
    pub(crate) fn new(
        class_id: ClassId,
        name: &str,
        options: &QueryOptions,
        principal_id: i32,
        token: (i32, i64),
    ) -> Result<Self, ApiError> {
        let selection = serde_json::json!({
            "class": class_id.id(), "name": name,
            "sort": normalized_sorts::<HubuumObjectHistory>(options.sort())?.iter().map(|sort| (sort.field.to_string(), sort.descending)).collect::<Vec<_>>(),
            "filters": options.filters().iter().map(|filter| (filter.field().to_string(), filter.operator().to_string(), filter.value())).collect::<Vec<_>>(), "principal": principal_id, "token": token,
        });
        Ok(Self(
            URL_SAFE_NO_PAD.encode(Sha256::digest(selection.to_string().as_bytes())),
        ))
    }

    pub(crate) fn decode(&self, cursor: &str) -> Result<String, ApiError> {
        let invalid = || ApiError::BadRequest("Invalid name-history cursor".to_string());
        if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
            return Err(invalid());
        }
        let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if envelope.version != 1 || envelope.cursor.is_empty() {
            return Err(invalid());
        }
        if envelope.scope != self.0 {
            return Err(ApiError::BadRequest("Name-history cursor does not match the class, name, filters, ordering, or authorization token".to_string()));
        }
        Ok(envelope.cursor)
    }

    pub(crate) fn encode(&self, cursor: String) -> Result<String, ApiError> {
        let bytes = serde_json::to_vec(&Envelope {
            version: 1,
            scope: self.0.clone(),
            cursor,
        })
        .map_err(|error| ApiError::InternalServerError(error.to_string()))?;
        let encoded = URL_SAFE_NO_PAD.encode(bytes);
        if encoded.len() > MAX_ENCODED_CURSOR_BYTES {
            return Err(ApiError::BadRequest(
                "Name-history cursor is too large".to_string(),
            ));
        }
        Ok(encoded)
    }
}
