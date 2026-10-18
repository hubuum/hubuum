use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use hubuum_domain::ClassId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::errors::ApiError;
use crate::models::{HubuumObjectHistory, TokenScope, search::QueryOptions};
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
        token_id: i32,
        scope: Option<&TokenScope>,
    ) -> Result<Self, ApiError> {
        Self::for_selection(
            class_id,
            serde_json::json!({"name": name}),
            options,
            principal_id,
            token_id,
            scope,
        )
    }

    pub(crate) fn for_population(
        class_id: ClassId,
        at: DateTime<Utc>,
        options: &QueryOptions,
        principal_id: i32,
        token_id: i32,
        scope: Option<&TokenScope>,
    ) -> Result<Self, ApiError> {
        Self::for_selection(
            class_id,
            serde_json::json!({"at": at, "kind": "population"}),
            options,
            principal_id,
            token_id,
            scope,
        )
    }

    fn for_selection(
        class_id: ClassId,
        selection: serde_json::Value,
        options: &QueryOptions,
        principal_id: i32,
        token_id: i32,
        scope: Option<&TokenScope>,
    ) -> Result<Self, ApiError> {
        // Token revisions also change during transparent credential migration.
        // Bind the actual authorization boundary instead of that metadata.
        let scope = scope
            .map(|scope| {
                let permissions = scope.permissions().map(|permissions| {
                    let mut permissions = permissions.to_vec();
                    permissions.sort_by_key(ToString::to_string);
                    permissions
                });
                Ok::<_, ApiError>(serde_json::json!({
                    "permissions": permissions,
                    "resources": scope.resources()?,
                }))
            })
            .transpose()?;
        let mut binding = serde_json::json!({
            "class": class_id.id(),
            "sort": normalized_sorts::<HubuumObjectHistory>(options.sort())?.iter().map(|sort| (sort.field.to_string(), sort.descending)).collect::<Vec<_>>(),
            "filters": options.filters().iter().map(|filter| (filter.field().to_string(), filter.operator().to_string(), filter.value())).collect::<Vec<_>>(), "principal": principal_id, "token": token_id, "scope": scope,
        });
        binding
            .as_object_mut()
            .expect("object binding")
            .extend(selection.as_object().expect("object selection").clone());
        Ok(Self(
            URL_SAFE_NO_PAD.encode(Sha256::digest(binding.to_string().as_bytes())),
        ))
    }

    pub(crate) fn decode(&self, cursor: &str) -> Result<String, ApiError> {
        let invalid = || ApiError::BadRequest("Invalid history selection cursor".to_string());
        if cursor.len() > MAX_ENCODED_CURSOR_BYTES {
            return Err(invalid());
        }
        let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| invalid())?;
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if envelope.version != 1 || envelope.cursor.is_empty() {
            return Err(invalid());
        }
        if envelope.scope != self.0 {
            return Err(ApiError::BadRequest("History selection cursor does not match the class, selection, filters, ordering, or authorization token".to_string()));
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
                "History selection cursor is too large".to_string(),
            ));
        }
        Ok(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CollectionID, Permissions, TokenResourceScope};
    use rstest::rstest;

    #[rstest]
    #[case::permission(Permissions::UpdateObject, 1)]
    #[case::resource(Permissions::ReadObject, 2)]
    fn cursor_rejects_changed_token_boundary(
        #[case] permission: Permissions,
        #[case] collection: i32,
    ) {
        let scope = |permission, collection| {
            TokenScope::from_request_parts(
                Some(vec![permission]),
                Some(vec![TokenResourceScope::Collection(
                    CollectionID::new(collection).unwrap(),
                )]),
            )
            .unwrap()
            .unwrap()
        };
        let options = QueryOptions::empty();
        let binding = |scope: &TokenScope| {
            HistoryNameCursor::new(
                ClassId::new(1).unwrap(),
                "server01",
                &options,
                1,
                1,
                Some(scope),
            )
            .unwrap()
        };
        let cursor = binding(&scope(Permissions::ReadObject, 1))
            .encode("page".into())
            .unwrap();
        assert!(
            binding(&scope(permission, collection))
                .decode(&cursor)
                .is_err()
        );
    }
}
