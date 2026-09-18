//! Capture only durable discovery facts from already validated task submissions.
use hubuum_storage_core::{StorageTaskKind, StorageTaskMetadata};
use serde_json::{Value, json};

use crate::errors::ApiError;

pub(super) fn capture(
    kind: StorageTaskKind,
    payload: &Value,
) -> Result<StorageTaskMetadata, ApiError> {
    let data = match kind {
        StorageTaskKind::Import => json!({
            "kind": "import", "dry_run": payload["dry_run"].as_bool().unwrap_or(false),
            "atomicity": payload["mode"]["atomicity"].as_str().unwrap_or("strict"),
            "collision_policy": payload["mode"]["collision_policy"].as_str().unwrap_or("abort"),
            "permission_policy": payload["mode"]["permission_policy"].as_str().unwrap_or("abort"),
            "has_failed_items": null,
        }),
        StorageTaskKind::Export => {
            let export = &payload["export"];
            let scope = &export["scope"];
            let target = match scope["kind"].as_str() {
                Some("objects_in_class") => json!({"type":"class", "class_id":scope["class_id"]}),
                Some("related_objects") => {
                    json!({"type":"object", "class_id":scope["class_id"], "object_id":scope["object_id"]})
                }
                _ => Value::Null,
            };
            json!({"kind":"export", "scope_kind":scope["kind"], "target":target,
                "template_id":payload["template_id"],
                "missing_data_policy":export["missing_data_policy"].as_str().unwrap_or("strict"),
                "max_items":export["limits"]["max_items"], "max_output_bytes":export["limits"]["max_output_bytes"],
                "warning_count":null, "truncated":null, "output":{"state":"not_produced"}})
        }
        StorageTaskKind::Backup => {
            json!({"kind":"backup", "include_history":payload["include_history"].as_bool().unwrap_or(true), "output":{"state":"not_produced"}})
        }
        StorageTaskKind::RemoteCall => {
            json!({"kind":"remote_call", "remote_target_id":payload["target_id"], "target":payload["subject"]})
        }
        StorageTaskKind::SchemaValidation => {
            json!({"kind":"schema_validation", "class_id":payload["class_id"], "schema_revision":payload["schema_revision"], "work_kind":payload["kind"]})
        }
        StorageTaskKind::Reindex => {
            json!({"kind":"reindex", "class_id":payload["class_id"], "computation_revision":payload["target_revision"]})
        }
    };
    StorageTaskMetadata::from_persisted(kind, json!({"version":1,"data":data}))
        .map_err(|error| ApiError::InternalServerError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remote_metadata_retains_only_explicit_subject_and_configuration_identity() {
        let metadata = capture(StorageTaskKind::RemoteCall, &json!({"target_id":7,"subject":{"type":"object","class_id":42,"object_id":8},"parameters":{"password":"do-not-retain"},"body_override":"do-not-retain"})).unwrap();
        assert_eq!(
            metadata.to_value(),
            json!({"version":1,"data":{"kind":"remote_call","remote_target_id":7,"target":{"type":"object","class_id":42,"object_id":8}}})
        );
    }
    #[test]
    fn broad_export_query_does_not_create_explicit_targets() {
        let metadata = capture(StorageTaskKind::Export, &json!({"export":{"scope":{"kind":"classes"},"query":"collection_id=42"},"template_id":7})).unwrap();
        assert!(metadata.details().target().is_none());
    }
    #[test]
    fn import_defaults_are_captured_but_pending_outcome_is_unknown() {
        let metadata = capture(StorageTaskKind::Import, &json!({"mode":null})).unwrap();
        assert_eq!(
            metadata.to_value()["data"],
            json!({"kind":"import","dry_run":false,"atomicity":"strict","collision_policy":"abort","permission_policy":"abort","has_failed_items":null})
        );
    }
}
