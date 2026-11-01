use hubuum_storage_core::{StorageTask, TaskFingerprint, TaskMetadataDetails};

use crate::errors::ApiError;
use crate::models::{RemoteTargetInvokeRequest, StoredRemoteCallTaskPayload, TaskKind};
use crate::services::deletion_recovery::TaskReceiptService;
use crate::tasks::request_hash;

#[derive(Clone, Copy)]
pub(crate) enum TaskSubmissionOperation {
    Import,
    Backup,
    Export,
    TemplateExport,
    RemoteInvocation,
    SchemaProfile,
}

impl TaskSubmissionOperation {
    pub(crate) const fn kind(self) -> TaskKind {
        match self {
            Self::Import => TaskKind::Import,
            Self::Backup => TaskKind::Backup,
            Self::Export | Self::TemplateExport => TaskKind::Export,
            Self::RemoteInvocation => TaskKind::RemoteCall,
            Self::SchemaProfile => TaskKind::SchemaProfiling,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Import => "imports.create",
            Self::Backup => "backups.create",
            Self::Export => "exports.create",
            Self::TemplateExport => "export_templates.run",
            Self::RemoteInvocation => "remote_targets.invoke",
            Self::SchemaProfile => "schema_profiles.create",
        }
    }
}

/// Preserve the old comparator where the client alone can reproduce its input.
/// Resolved template defaults are not invertible; remote requests can only be
/// projected from a supported, hash-verified retained payload.
pub(crate) fn legacy_task_fingerprint(
    service: &TaskReceiptService,
    task: &StorageTask,
) -> Result<TaskFingerprint, ApiError> {
    let Some(digest) = task
        .request_hash()
        .filter(|digest| valid_legacy_digest(digest))
    else {
        return Ok(TaskFingerprint::LegacyUnverifiable);
    };
    match task.kind() {
        TaskKind::Import | TaskKind::Backup | TaskKind::SchemaProfiling => service
            .legacy_fingerprint(task.kind(), digest)
            .map(TaskFingerprint::LegacyV1),
        TaskKind::Export => {
            let template_run = task.metadata().is_some_and(|metadata| {
                matches!(
                    metadata.details(),
                    TaskMetadataDetails::Export {
                        template_id: Some(_),
                        ..
                    }
                )
            }) || task
                .request_payload()
                .and_then(|payload| payload.get("template_id"))
                .is_some_and(|id| !id.is_null());
            if template_run {
                Ok(TaskFingerprint::LegacyUnverifiable)
            } else {
                // Missing producer details remain safe: only a direct-export
                // null-template legacy recipe can match this digest.
                service
                    .legacy_fingerprint(task.kind(), digest)
                    .map(TaskFingerprint::LegacyV1)
            }
        }
        TaskKind::RemoteCall => {
            let Some(payload) = task.request_payload() else {
                return Ok(TaskFingerprint::LegacyUnverifiable);
            };
            if request_hash(payload)? != digest {
                return Ok(TaskFingerprint::LegacyUnverifiable);
            }
            let Ok(stored) = serde_json::from_value::<StoredRemoteCallTaskPayload>(payload.clone())
            else {
                return Ok(TaskFingerprint::LegacyUnverifiable);
            };
            // Reject ignored fields and newly supplied defaults. A parse alone
            // cannot establish the meaning of an old request hash.
            if serde_json::to_value(&stored)? != *payload {
                return Ok(TaskFingerprint::LegacyUnverifiable);
            }
            let original = RemoteTargetInvokeRequest {
                subject: stored.subject,
                parameters: stored.parameters,
                body_override: stored.body_override,
            };
            service
                .fingerprint(
                    task.kind(),
                    TaskSubmissionOperation::RemoteInvocation.as_str(),
                    &(stored.target_id, original),
                )
                .map(TaskFingerprint::CanonicalV1)
        }
        TaskKind::Reindex | TaskKind::SchemaValidation => Ok(TaskFingerprint::LegacyUnverifiable),
    }
}

fn valid_legacy_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(all(test, unix))]
mod tests;
