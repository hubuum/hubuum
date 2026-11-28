use std::fmt::Write as _;
use std::io::{self, Write};

use hmac::{Hmac, KeyInit, Mac};
use hubuum_domain::PrincipalIdentity;
use hubuum_storage_core::{
    ControlKeyFingerprint, ProtectedTaskKey, StorageTaskKind, TaskReceiptBinding,
    TaskRequestFingerprint, TaskSubmissionIdentity, TaskSubmissionLookup,
};
use hubuum_task_core::IdempotencyKey;
use serde::Serialize;
use sha2::Sha256;

use super::{invalid_control, unavailable};
use crate::errors::ApiError;

const KEY_ID_DOMAIN: &[u8] = b"hubuum.task-control-key.v1\0";
const SUBMISSION_KEY_DOMAIN: &[u8] = b"hubuum.task-submission-key.v1\0";
const REQUEST_DOMAIN: &[u8] = b"hubuum.task-original-request.v1\0";
const LEGACY_DOMAIN: &[u8] = b"hubuum.task-legacy-request.v1\0";

/// The independent repository's key, with separate domains for receipt data.
/// Deliberately has no Debug or serialization implementation.
#[derive(Clone)]
pub(super) struct TaskReceiptKey([u8; 32]);

#[derive(Serialize)]
struct OriginalRequest<'a, T: ?Sized> {
    format: u8,
    operation: &'a str,
    kind: &'a str,
    input: &'a T,
}

struct MacWriter<'a>(&'a mut Hmac<Sha256>);

impl Write for MacWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl TaskReceiptKey {
    pub(super) const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    fn mac(&self, domain: &[u8]) -> Result<Hmac<Sha256>, ApiError> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0)
            .map_err(|_| unavailable("Invalid task-control authentication key"))?;
        mac.update(domain);
        Ok(mac)
    }

    pub(super) fn fingerprint(&self) -> Result<ControlKeyFingerprint, ApiError> {
        ControlKeyFingerprint::new(finish(self.mac(KEY_ID_DOMAIN)?)).map_err(invalid_control)
    }

    pub(super) fn protected_key(
        &self,
        principal: PrincipalIdentity,
        key: &IdempotencyKey,
    ) -> Result<ProtectedTaskKey, ApiError> {
        let mut mac = self.mac(SUBMISSION_KEY_DOMAIN)?;
        mac.update(principal.as_bytes());
        mac.update(key.as_str().as_bytes());
        ProtectedTaskKey::new(finish(mac)).map_err(invalid_control)
    }

    pub(super) fn request_fingerprint<T: Serialize + ?Sized>(
        &self,
        kind: StorageTaskKind,
        operation: &str,
        original: &T,
    ) -> Result<TaskRequestFingerprint, ApiError> {
        if operation.is_empty() || operation.len() > 128 {
            return Err(unavailable("Invalid task submission operation identity"));
        }
        let mut mac = self.mac(REQUEST_DOMAIN)?;
        // Typed request models can contain HashMaps. Normalize every object
        // before hashing, including when serde_json's preserve_order feature
        // is enabled elsewhere in the dependency graph.
        let mut canonical = serde_json::to_value(OriginalRequest {
            format: 1,
            operation,
            kind: kind.as_str(),
            input: original,
        })
        .map_err(|_| ApiError::BadRequest("Cannot fingerprint the task request".into()))?;
        canonical.sort_all_objects();
        // Stream the canonical encoding instead of also retaining encoded bytes.
        serde_json::to_writer(MacWriter(&mut mac), &canonical)
            .map_err(|_| ApiError::BadRequest("Cannot fingerprint the task request".into()))?;
        TaskRequestFingerprint::new(finish(mac)).map_err(invalid_control)
    }

    pub(super) fn legacy_fingerprint(
        &self,
        kind: StorageTaskKind,
        digest: &str,
    ) -> Result<TaskRequestFingerprint, ApiError> {
        // This validates the legacy encoding only. The producer/backfill must
        // separately prove that this is the original legacy request hash.
        let digest = TaskRequestFingerprint::new(digest.to_owned()).map_err(invalid_control)?;
        let mut mac = self.mac(LEGACY_DOMAIN)?;
        mac.update(kind.as_str().as_bytes());
        mac.update(&[0]);
        mac.update(digest.as_str().as_bytes());
        TaskRequestFingerprint::new(finish(mac)).map_err(invalid_control)
    }

    pub(super) fn binding<T: Serialize + ?Sized>(
        &self,
        principal: PrincipalIdentity,
        key: &IdempotencyKey,
        kind: StorageTaskKind,
        operation: &str,
        original: &T,
        legacy_digest: Option<&str>,
    ) -> Result<TaskReceiptBinding, ApiError> {
        let lookup = TaskSubmissionLookup::new(
            TaskSubmissionIdentity::new(principal, self.protected_key(principal, key)?),
            kind,
            self.request_fingerprint(kind, operation, original)?,
        )
        .with_legacy_v1(
            legacy_digest
                .map(|digest| self.legacy_fingerprint(kind, digest))
                .transpose()?,
        );
        Ok(TaskReceiptBinding::new(lookup, self.fingerprint()?))
    }
}

fn finish(mac: Hmac<Sha256>) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in mac.finalize().into_bytes() {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rstest::rstest;
    use serde_json::json;

    use super::*;

    #[rstest]
    #[case::principal(true, false)]
    #[case::key(false, true)]
    fn submission_key_namespaces_do_not_alias(
        #[case] change_principal: bool,
        #[case] change_key: bool,
    ) {
        let material = TaskReceiptKey::new([42; 32]);
        let principal = PrincipalIdentity::generate();
        let first = material
            .protected_key(principal, &IdempotencyKey::new("request").unwrap())
            .unwrap();
        let second = material
            .protected_key(
                if change_principal {
                    PrincipalIdentity::generate()
                } else {
                    principal
                },
                &IdempotencyKey::new(if change_key { "another" } else { "request" }).unwrap(),
            )
            .unwrap();
        assert_ne!(first, second);
    }

    #[rstest]
    #[case::operation(StorageTaskKind::Export, "export_templates.run", json!({"value": 1}))]
    #[case::kind(StorageTaskKind::Backup, "exports.create", json!({"value": 1}))]
    #[case::body(StorageTaskKind::Export, "exports.create", json!({"value": 2}))]
    fn original_request_identity_includes_operation_kind_and_body(
        #[case] kind: StorageTaskKind,
        #[case] operation: &str,
        #[case] original: serde_json::Value,
    ) {
        let material = TaskReceiptKey::new([42; 32]);
        let first = material
            .request_fingerprint(
                StorageTaskKind::Export,
                "exports.create",
                &json!({"value": 1}),
            )
            .unwrap();
        assert_ne!(
            first,
            material
                .request_fingerprint(kind, operation, &original)
                .unwrap()
        );
    }

    #[test]
    fn legacy_comparison_preserves_the_task_kind_boundary() {
        let material = TaskReceiptKey::new([42; 32]);
        let digest = "a".repeat(64);
        assert_ne!(
            material
                .legacy_fingerprint(StorageTaskKind::Import, &digest)
                .unwrap(),
            material
                .legacy_fingerprint(StorageTaskKind::Backup, &digest)
                .unwrap(),
        );
    }

    #[test]
    fn replacing_the_control_key_changes_its_persisted_identity() {
        assert_ne!(
            TaskReceiptKey::new([42; 32]).fingerprint().unwrap(),
            TaskReceiptKey::new([43; 32]).fingerprint().unwrap(),
        );
    }

    #[test]
    fn canonical_json_object_order_is_stable() {
        let material = TaskReceiptKey::new([42; 32]);
        let first: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let second: serde_json::Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();
        assert_eq!(
            material
                .request_fingerprint(StorageTaskKind::Import, "imports.create", &first)
                .unwrap(),
            material
                .request_fingerprint(StorageTaskKind::Import, "imports.create", &second)
                .unwrap(),
        );
    }

    #[test]
    fn typed_request_hash_maps_have_canonical_fingerprints() {
        #[derive(Serialize)]
        struct Request {
            related_objects: HashMap<String, serde_json::Value>,
        }
        let material = TaskReceiptKey::new([42; 32]);
        let entries: Vec<_> = (0..64)
            .map(|index| (index.to_string(), json!({"value": index})))
            .collect();
        let first = Request {
            related_objects: entries.clone().into_iter().collect(),
        };
        let second = Request {
            related_objects: entries.into_iter().rev().collect(),
        };
        assert_eq!(
            material
                .request_fingerprint(StorageTaskKind::Export, "exports.create", &first)
                .unwrap(),
            material
                .request_fingerprint(StorageTaskKind::Export, "exports.create", &second)
                .unwrap(),
        );
    }
}
