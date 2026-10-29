//! Portable, bounded frames for retained export artifacts in logical backups.
//! Restoring copies bytes into fresh physical identities; a backup never revives
//! a discarded attempt or clears the destination's prior cleanup obligations.
use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, Utc};
use hubuum_domain::TaskId;
use serde::{Deserialize, Serialize};

use super::*;
use crate::{StorageBackupHistorySection, StorageBackupRow, StorageError};

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactBackupFrame {
    Manifest {
        task_id: TaskId,
        expires_at: DateTime<Utc>,
        reference: ArtifactReference,
        manifest: String,
    },
    Block {
        artifact: ArtifactId,
        ordinal: u32,
        content: String,
    },
}
impl ArtifactBackupFrame {
    pub fn header(
        task_id: TaskId,
        expires_at: DateTime<Utc>,
        reference: ArtifactReference,
        manifest: &ArtifactManifest,
    ) -> Self {
        Self::Manifest {
            task_id,
            expires_at,
            reference,
            manifest: STANDARD.encode(manifest.encode()),
        }
    }
    pub fn block(artifact: ArtifactId, ordinal: u32, bytes: &[u8]) -> Self {
        Self::Block {
            artifact,
            ordinal,
            content: STANDARD.encode(bytes),
        }
    }
    pub fn into_row(self) -> Result<StorageBackupRow, StorageError> {
        StorageBackupRow::try_from_value(serde_json::to_value(self).map_err(invalid)?)
            .map_err(|error| error.into_request_error())
    }
}
fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::invalid_input(format!("Invalid backup artifact: {error}"))
}
fn bytes(text: &str, limit: usize) -> Result<Vec<u8>, StorageError> {
    if text.len() > limit.div_ceil(3) * 4 {
        return Err(invalid("encoded frame exceeds limit"));
    }
    let bytes = STANDARD.decode(text).map_err(invalid)?;
    if bytes.len() > limit {
        return Err(invalid("decoded frame exceeds limit"));
    }
    Ok(bytes)
}

pub struct BackupArtifactHeader {
    task_id: TaskId,
    expires_at: DateTime<Utc>,
    reference: ArtifactReference,
    manifest: ArtifactManifest,
}
impl BackupArtifactHeader {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub fn reference(&self) -> &ArtifactReference {
        &self.reference
    }
    pub fn manifest(&self) -> &ArtifactManifest {
        &self.manifest
    }
    pub fn limits(&self) -> ArtifactLimits {
        ArtifactLimits::builder()
            .block_bytes(self.manifest.block_bytes())
            .artifact_bytes(self.manifest.byte_size())
            .blocks(self.manifest.block_count())
            .manifest_bytes(self.manifest.encoded_len() as u32)
            .build()
            .expect("validated manifest limits")
    }
}
pub enum BackupArtifactPart {
    Header(Box<BackupArtifactHeader>),
    Block { ordinal: u32, bytes: Vec<u8> },
}
struct Current {
    artifact: ArtifactId,
    ordinal: u32,
    verifier: ManifestVerifier,
}
/// Enforces frame order, exact EOF, bounded decoding and the original trusted
/// reference. Adapters additionally read back their new staged destination.
#[derive(Default)]
pub struct BackupArtifactDecoder {
    current: Option<Current>,
}
impl BackupArtifactDecoder {
    pub fn push(&mut self, row: StorageBackupRow) -> Result<BackupArtifactPart, StorageError> {
        match serde_json::from_value::<ArtifactBackupFrame>(row.into_value()).map_err(invalid)? {
            ArtifactBackupFrame::Manifest {
                task_id,
                expires_at,
                reference,
                manifest,
            } => {
                self.finish_current()?;
                let manifest = ArtifactManifest::decode(
                    &bytes(
                        &manifest,
                        ArtifactLimits::format_ceiling().manifest_bytes() as usize,
                    )?,
                    reference.identity(),
                    ArtifactLimits::format_ceiling(),
                )
                .map_err(invalid)?;
                if &manifest.manifest_digest() != reference.manifest_digest() {
                    return Err(invalid("manifest digest mismatch"));
                }
                self.current = Some(Current {
                    artifact: reference.identity().artifact(),
                    ordinal: 0,
                    verifier: manifest.verifier(),
                });
                Ok(BackupArtifactPart::Header(Box::new(BackupArtifactHeader {
                    task_id,
                    expires_at,
                    reference,
                    manifest,
                })))
            }
            ArtifactBackupFrame::Block {
                artifact,
                ordinal,
                content,
            } => {
                let current = self
                    .current
                    .as_mut()
                    .ok_or_else(|| invalid("block has no header"))?;
                if current.artifact != artifact || current.ordinal != ordinal {
                    return Err(invalid("block identity or position differs"));
                }
                let bytes = bytes(
                    &content,
                    ArtifactLimits::format_ceiling().block_bytes() as usize,
                )?;
                current.verifier.push(&bytes).map_err(invalid)?;
                current.ordinal += 1;
                Ok(BackupArtifactPart::Block { ordinal, bytes })
            }
        }
    }
    fn finish_current(&mut self) -> Result<(), StorageError> {
        if let Some(current) = self.current.take() {
            current.verifier.finish().map_err(invalid)?;
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<(), StorageError> {
        self.finish_current()
    }
}

/// Validate portable content before a restore can cross its publication boundary.
pub(crate) fn validate_backup_artifacts(
    frames: impl Iterator<Item = Result<StorageBackupRow, StorageError>>,
    outputs: impl Iterator<Item = Result<StorageBackupRow, StorageError>>,
) -> Result<(), StorageError> {
    let mut decoder = BackupArtifactDecoder::default();
    let mut references = BTreeMap::new();
    for row in frames {
        if let BackupArtifactPart::Header(header) = decoder.push(row?)? {
            let artifact = header.reference.identity().artifact();
            if references
                .insert(artifact, (header.task_id, header.reference))
                .is_some()
            {
                return Err(invalid("duplicate artifact"));
            }
        }
    }
    decoder.finish()?;
    for row in outputs {
        let row = row?;
        if let Some(reference) = row
            .get("artifact_reference")
            .filter(|value| !value.is_null())
        {
            let reference: ArtifactReference =
                serde_json::from_value(reference.clone()).map_err(invalid)?;
            let (task, expected) = references
                .remove(&reference.identity().artifact())
                .ok_or_else(|| invalid("output has no portable bytes"))?;
            if row.get("task_id").and_then(serde_json::Value::as_i64) != Some(i64::from(task.id()))
                || expected != reference
            {
                return Err(invalid("output ownership differs"));
            }
        }
    }
    if !references.is_empty() {
        return Err(invalid("unreferenced portable artifact"));
    }
    Ok(())
}

impl crate::StorageBackupReplay {
    pub(crate) fn validate_artifacts(&self) -> Result<(), StorageError> {
        validate_backup_artifacts(
            self.history_rows(StorageBackupHistorySection::ExportArtifacts),
            self.history_rows(StorageBackupHistorySection::ExportOutputs),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde_json::json;

    fn fixture() -> (Vec<StorageBackupRow>, StorageBackupRow) {
        let identity = ArtifactIdentity::new(DestinationId::generate());
        let limits = ArtifactLimits::builder()
            .block_bytes(4)
            .artifact_bytes(8)
            .blocks(2)
            .build()
            .unwrap();
        let mut builder = ManifestBuilder::new(identity.clone(), limits);
        builder.push(b"abcd").unwrap();
        builder.push(b"efg").unwrap();
        let manifest = builder.finish().unwrap();
        let reference =
            ArtifactReference::from_catalog(identity.clone(), manifest.manifest_digest());
        let rows = vec![
            ArtifactBackupFrame::header(
                TaskId::new(7).unwrap(),
                Utc::now(),
                reference.clone(),
                &manifest,
            )
            .into_row()
            .unwrap(),
            ArtifactBackupFrame::block(identity.artifact(), 0, b"abcd")
                .into_row()
                .unwrap(),
            ArtifactBackupFrame::block(identity.artifact(), 1, b"efg")
                .into_row()
                .unwrap(),
        ];
        let output =
            StorageBackupRow::try_from_value(json!({"task_id":7,"artifact_reference":reference}))
                .unwrap();
        (rows, output)
    }
    #[test]
    fn complete_portable_artifact_matches_its_output() {
        let (rows, output) = fixture();
        validate_backup_artifacts(rows.into_iter().map(Ok), [Ok(output)].into_iter()).unwrap();
    }
    #[rstest]
    #[case::truncated("truncated")]
    #[case::reordered("reordered")]
    #[case::corrupt("corrupt")]
    #[case::duplicate("duplicate")]
    #[case::unreferenced("unreferenced")]
    #[case::wrong_owner("wrong_owner")]
    #[case::wrong_digest("wrong_digest")]
    #[case::trailing_block("trailing_block")]
    fn rejects_incomplete_or_substituted_portable_content(#[case] fault: &str) {
        let (mut rows, output) = fixture();
        let mut outputs = vec![output];
        match fault {
            "truncated" => {
                rows.pop();
            }
            "reordered" => rows.swap(1, 2),
            "corrupt" => {
                let mut row = rows[1].clone().into_value();
                row["content"] = json!(STANDARD.encode(b"bad!"));
                rows[1] = StorageBackupRow::try_from_value(row).unwrap();
            }
            "duplicate" => rows.extend(rows.clone()),
            "unreferenced" => outputs.clear(),
            "wrong_owner" | "wrong_digest" => {
                let mut row = outputs.pop().unwrap().into_value();
                if fault == "wrong_owner" {
                    row["task_id"] = json!(8);
                } else {
                    row["artifact_reference"]["manifest_digest"][0] = json!(256);
                }
                outputs.push(StorageBackupRow::try_from_value(row).unwrap());
            }
            "trailing_block" => rows.push(rows[2].clone()),
            _ => unreachable!(),
        }
        assert!(
            validate_backup_artifacts(rows.into_iter().map(Ok), outputs.into_iter().map(Ok))
                .is_err()
        );
    }
}
