//! Durable representation of a credential-bearing import. Ordinary and legacy
//! requests retain their existing wire representation.
use hmac::{Hmac, KeyInit, Mac};
use hubuum_secrets::SecretValue;
use serde_json::{Value, json};
use sha2::Sha256;

use crate::config::get_config;
use crate::errors::ApiError;
use crate::models::{ImportRequest, PrincipalID};
use crate::runtime_settings_secrets::{
    self, ALIAS_KEY_VERSION, EncryptedSecret, RuntimeEncryptionKey,
};
use crate::secrets::resolve_runtime_settings_key;
use crate::tasks::request_hash;

const ENVELOPE_FIELD: &str = "encrypted_import";

pub(crate) async fn prepare(
    request: &ImportRequest,
    actor: PrincipalID,
) -> Result<(Value, String), ApiError> {
    let payload = serde_json::to_value(request)?;
    let digest = request_hash(&payload)?;
    if !request.contains_credentials() {
        return Ok((payload, digest));
    }
    let key_ref = get_config()?
        .runtime_settings_encryption_key_ref
        .clone()
        .ok_or_else(|| {
            ApiError::ServiceUnavailable(
                "Credential imports require HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF".into(),
            )
        })?;
    let key = resolve_runtime_settings_key(&key_ref, None)
        .await
        .map_err(|_| key_unavailable())?;
    seal(request, actor, digest, key_ref, &key)
}

fn key_unavailable() -> ApiError {
    ApiError::ServiceUnavailable("Credential import encryption key is unavailable".into())
}

fn context(actor: PrincipalID, digest: &str) -> String {
    format!("credential-import:v1:{}:{digest}", actor.id())
}

fn seal(
    request: &ImportRequest,
    actor: PrincipalID,
    digest: String,
    key_ref: String,
    key: &RuntimeEncryptionKey,
) -> Result<(Value, String), ApiError> {
    // Persist a keyed fingerprint. An ordinary digest of a predictable import
    // would allow offline checks of password guesses against a database copy.
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key.expose()).expect("HMAC accepts any key length");
    mac.update(b"hubuum:credential-import:fingerprint:v1:");
    mac.update(&actor.id().to_be_bytes());
    mac.update(digest.as_bytes());
    let fingerprint: String = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let plaintext = SecretValue::new(serde_json::to_vec(request)?)
        .map_err(|_| ApiError::InternalServerError("Failed to encode import".into()))?;
    let envelope = runtime_settings_secrets::encrypt(
        key_ref,
        ALIAS_KEY_VERSION.to_owned(),
        key,
        &context(actor, &fingerprint),
        &plaintext,
    )
    .map_err(|_| key_unavailable())?;
    // Task discovery reads only these non-secret fields. The executor uses
    // the authenticated copy of the entire request.
    Ok((
        json!({(ENVELOPE_FIELD): envelope, "dry_run": request.dry_run, "mode": request.mode}),
        fingerprint,
    ))
}

pub(crate) async fn open(
    mut payload: Value,
    actor: PrincipalID,
    digest: Option<&str>,
) -> Result<ImportRequest, ApiError> {
    let Some(envelope) = payload
        .as_object_mut()
        .and_then(|fields| fields.remove(ENVELOPE_FIELD))
    else {
        return Ok(serde_json::from_value(payload)?);
    };
    let envelope: EncryptedSecret = serde_json::from_value(envelope)?;
    let key = resolve_runtime_settings_key(envelope.key_ref(), None)
        .await
        .map_err(|_| key_unavailable())?;
    let digest = digest.ok_or_else(|| {
        ApiError::InternalServerError("Missing credential import fingerprint".into())
    })?;
    unseal(&envelope, actor, digest, &key)
}

fn unseal(
    envelope: &EncryptedSecret,
    actor: PrincipalID,
    digest: &str,
    key: &RuntimeEncryptionKey,
) -> Result<ImportRequest, ApiError> {
    let plaintext = runtime_settings_secrets::decrypt(envelope, key, &context(actor, digest))
        .map_err(|_| {
            ApiError::ServiceUnavailable("Credential import could not be decrypted".into())
        })?;
    Ok(serde_json::from_slice(plaintext.expose())?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn request() -> ImportRequest {
        serde_json::from_value(json!({"version":2,"dry_run":true,"graph":{"principals":[{
            "name":"alice","identity_scope_key":{"name":"local"},"provider_managed":false,
            "kind":"human","password":"import-secret"
        }]}}))
        .unwrap()
    }

    #[test]
    fn queue_representation_round_trips_without_storing_the_password_or_unkeyed_digest() {
        let request = request();
        let actor = PrincipalID::new(1).unwrap();
        let key = RuntimeEncryptionKey::parse(&[7; 32]).unwrap();
        let digest = request_hash(&serde_json::to_value(&request).unwrap()).unwrap();
        let (payload, fingerprint) =
            seal(&request, actor, digest.clone(), "key1".into(), &key).unwrap();
        assert_ne!(digest, fingerprint);
        assert!(!payload.to_string().contains("import-secret"));
        assert_eq!(payload["dry_run"], true);
        let envelope = serde_json::from_value(payload[ENVELOPE_FIELD].clone()).unwrap();
        assert_eq!(
            unseal(&envelope, actor, &fingerprint, &key).unwrap(),
            request
        );
        let (retry, retry_fingerprint) =
            seal(&request, actor, digest, "key1".into(), &key).unwrap();
        assert_eq!(fingerprint, retry_fingerprint);
        assert_ne!(payload[ENVELOPE_FIELD], retry[ENVELOPE_FIELD]);
    }

    #[rstest]
    #[case::another_actor(2, false)]
    #[case::another_request(1, true)]
    fn stored_credentials_are_bound_to_their_task_request(
        #[case] id: i32,
        #[case] change_digest: bool,
    ) {
        let key = RuntimeEncryptionKey::parse(&[7; 32]).unwrap();
        let (payload, digest) = seal(
            &request(),
            PrincipalID::new(1).unwrap(),
            "request".into(),
            "key1".into(),
            &key,
        )
        .unwrap();
        let envelope = serde_json::from_value(payload[ENVELOPE_FIELD].clone()).unwrap();
        assert!(
            unseal(
                &envelope,
                PrincipalID::new(id).unwrap(),
                if change_digest { "different" } else { &digest },
                &key
            )
            .is_err()
        );
    }
}
