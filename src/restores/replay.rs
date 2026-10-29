//! Streaming envelope parsing and canonical serialization of backup row spools.
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{BufReader, Read};

use chrono::{DateTime, Utc};
use hubuum_domain::{JsonSchemaLimits, LegacyIdentityCohort};
use hubuum_storage_core::{
    StorageBackupBudget, StorageBackupHistorySection as History, StorageBackupReplay,
    StorageBackupReplayBuilder, StorageBackupRow, StorageBackupStateSection as State,
    StorageIdentityUpgrade,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::errors::ApiError;
use crate::models::backup::BACKUP_MANIFEST_EXCLUSIONS;
use crate::models::{
    BackupDocument, BackupHistory, BackupManifest, BackupState, CURRENT_BACKUP_VERSION,
    RestoreValidationSummary,
};
use crate::storage::{BackupSnapshotStorage, StorageContext, storage_handle};

pub(crate) struct ReplayDocument {
    pub metadata: BackupDocument,
    pub snapshot: StorageBackupReplay,
    unresolved_identity_references: usize,
}
#[derive(Serialize)]
struct Sections<T> {
    sections: T,
}
impl Serialize for ReplayDocument {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Document<'a, T, H> {
            backup_version: i32,
            created_at: DateTime<Utc>,
            source_version: &'a str,
            state: Sections<T>,
            history: Option<Sections<H>>,
            manifest: &'a BackupManifest,
        }
        Document {
            backup_version: self.metadata.backup_version,
            created_at: self.metadata.created_at,
            source_version: &self.metadata.source_version,
            state: Sections {
                sections: self.snapshot.state(),
            },
            history: self
                .snapshot
                .history()
                .map(|sections| Sections { sections }),
            manifest: &self.metadata.manifest,
        }
        .serialize(serializer)
    }
}
impl ReplayDocument {
    #[cfg(feature = "embedded-migrations")]
    pub(crate) fn at_restore_boundary(
        mut self,
        context: hubuum_storage_core::StorageRestoreIdentityContext,
    ) -> Result<Self, ApiError> {
        let upgrade = if self.metadata.backup_version >= 11 {
            StorageIdentityUpgrade::Native
        } else {
            StorageIdentityUpgrade::Legacy(context.cohort())
        };
        let prepared = self
            .snapshot
            .upgrade_logical_identities(upgrade)?
            .at_restore_boundary(context)?;
        self.unresolved_identity_references = prepared.unresolved_references();
        self.snapshot = prepared.into_replay();
        Ok(self)
    }

    pub fn summary(&self) -> Result<RestoreValidationSummary, ApiError> {
        let total_items = self
            .metadata
            .manifest
            .item_counts
            .values()
            .try_fold(0i64, |total, count| total.checked_add(*count))
            .ok_or_else(|| ApiError::PayloadTooLarge("Backup row count overflow".into()))?;
        Ok(RestoreValidationSummary {
            backup_version: self.metadata.backup_version,
            source_version: self.metadata.source_version.clone(),
            includes_history: self.snapshot.includes_history(),
            total_items,
            identity_quarantine: self.metadata.backup_version < 11,
            unresolved_identity_references: u64::try_from(self.unresolved_identity_references)
                .map_err(|_| {
                    ApiError::PayloadTooLarge("Identity reference count overflow".into())
                })?,
        })
    }
    pub fn state_sha256(&self) -> Result<String, ApiError> {
        super::serialized_sha256(&Sections {
            sections: self.snapshot.state(),
        })
    }
}

pub(crate) async fn capture(
    backend: &impl StorageContext,
    include_history: bool,
    budget: StorageBackupBudget,
) -> Result<ReplayDocument, ApiError> {
    let snapshot = storage_handle(backend)
        .capture_backup_replay(include_history, budget)
        .await?
        .validate_resources()?
        .upgrade_logical_identities(StorageIdentityUpgrade::Native)?;
    let unresolved_identity_references = snapshot.unresolved_references();
    let snapshot = snapshot.into_replay();
    if snapshot.includes_history() != include_history {
        return Err(ApiError::InternalServerError(
            "Captured backup history does not match the request".into(),
        ));
    }
    let mut counts = BTreeMap::new();
    for &s in State::ALL {
        counts.insert(s.to_string(), snapshot.state_count(s) as i64);
    }
    if include_history {
        for &s in History::ALL {
            counts.insert(format!("history.{s}"), snapshot.history_count(s) as i64);
        }
    }
    Ok(ReplayDocument {
        unresolved_identity_references,
        metadata: BackupDocument {
            backup_version: CURRENT_BACKUP_VERSION,
            created_at: Utc::now(),
            source_version: env!("CARGO_PKG_VERSION").into(),
            state: BackupState::default(),
            history: include_history.then(BackupHistory::default),
            manifest: BackupManifest {
                item_counts: counts,
                exclusions: BACKUP_MANIFEST_EXCLUSIONS
                    .iter()
                    .map(|s| (*s).to_owned())
                    .collect(),
            },
        },
        snapshot,
    })
}

struct HashReader<R> {
    inner: R,
    hash: Sha256,
    count: usize,
    limit: usize,
    exceeded: bool,
}
impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        let allowed = output
            .len()
            .min(self.limit.saturating_sub(self.count).saturating_add(1));
        let size = self.inner.read(&mut output[..allowed])?;
        if size > self.limit.saturating_sub(self.count) {
            self.exceeded = true;
            return Err(std::io::Error::other("Backup byte limit exceeded"));
        }
        self.hash.update(&output[..size]);
        self.count += size;
        Ok(size)
    }
}

pub(crate) fn parse(
    reader: impl Read,
    max_bytes: usize,
    limits: JsonSchemaLimits,
) -> Result<(ReplayDocument, String, usize), ApiError> {
    // Rows have at least two encoded bytes. The canonical spool can add legacy
    // defaults, so allow bounded expansion independently of the input byte cap.
    let budget =
        StorageBackupBudget::new(max_bytes.saturating_mul(4).max(1), (max_bytes / 2).max(1))
            .map_err(|e| ApiError::from(e.into_request_error()))?;
    let mut builder = StorageBackupReplayBuilder::new(budget, limits, true)?;
    let mut reader = BufReader::new(HashReader {
        inner: reader,
        hash: Sha256::new(),
        count: 0,
        limit: max_bytes,
        exceeded: false,
    });
    let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
    let mut counts = BTreeMap::new();
    let parsed = Envelope {
        builder: &mut builder,
        counts: &mut counts,
    }
    .deserialize(&mut deserializer);
    let parsed = parsed.and_then(|metadata| {
        deserializer.end()?;
        Ok(metadata)
    });
    let reader = reader.into_inner();
    if reader.exceeded {
        return Err(ApiError::PayloadTooLarge(format!(
            "Backup document exceeds the configured {max_bytes} byte limit"
        )));
    }
    let metadata = parsed.map_err(|e| {
        ApiError::BadRequest(format!("Backup document is not valid backup JSON: {e}"))
    })?;
    metadata.validate_version()?;
    super::validate_backup_metadata(&metadata)?;
    super::validate_manifest_counts(&metadata.manifest, &counts)?;
    if metadata.history.is_none() {
        builder = builder.without_history()?;
    }
    let snapshot = builder.finish_for_restore(metadata.backup_version)?;
    let upgrade = if metadata.backup_version >= 11 {
        StorageIdentityUpgrade::Native
    } else {
        // Validate the full legacy upgrade in quarantine before accepting a
        // stage. Confirmation binds final assignments to the persisted job;
        // this preview cohort is discarded and never identifies published data.
        StorageIdentityUpgrade::Legacy(LegacyIdentityCohort::generate())
    };
    let preview = snapshot.upgrade_logical_identities(upgrade)?;
    let unresolved_identity_references = preview.unresolved_references();
    let document = ReplayDocument {
        metadata,
        snapshot,
        unresolved_identity_references,
    };
    document.summary()?;
    Ok((
        document,
        reader
            .hash
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
        reader.count,
    ))
}

struct Envelope<'a> {
    builder: &'a mut StorageBackupReplayBuilder,
    counts: &'a mut BTreeMap<String, i64>,
}
impl<'de> DeserializeSeed<'de> for Envelope<'_> {
    type Value = BackupDocument;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Envelope<'_> {
    type Value = BackupDocument;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a backup document")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut seen = BTreeSet::new();
        let (mut version, mut created, mut source, mut manifest, mut state, mut history) =
            (None, None, None, None, None, None);
        while let Some(key) = map.next_key::<String>()? {
            if !seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("Duplicate backup field '{key}'")));
            }
            match key.as_str() {
                "backup_version" => version = Some(map.next_value()?),
                "created_at" => created = Some(map.next_value()?),
                "source_version" => source = Some(map.next_value()?),
                "manifest" => manifest = Some(map.next_value()?),
                "state" => {
                    map.next_value_seed(SectionObject {
                        builder: self.builder,
                        counts: self.counts,
                        history: false,
                    })?;
                    state = Some(());
                }
                "history" => {
                    history = map.next_value_seed(OptionalHistory {
                        builder: self.builder,
                        counts: self.counts,
                    })?
                }
                _ => return Err(de::Error::custom(format!("Unknown backup field '{key}'"))),
            }
        }
        state.ok_or_else(|| de::Error::missing_field("state"))?;
        let state = BackupState {
            sections: State::ALL
                .iter()
                .filter(|s| self.counts.contains_key(s.as_str()))
                .map(|&s| (s, Vec::new()))
                .collect(),
        };
        let history = history.map(|()| BackupHistory {
            sections: History::ALL
                .iter()
                .filter(|s| self.counts.contains_key(&format!("history.{s}")))
                .map(|&s| (s, Vec::new()))
                .collect(),
        });
        Ok(BackupDocument {
            backup_version: version.ok_or_else(|| de::Error::missing_field("backup_version"))?,
            created_at: created.ok_or_else(|| de::Error::missing_field("created_at"))?,
            source_version: source.ok_or_else(|| de::Error::missing_field("source_version"))?,
            manifest: manifest.ok_or_else(|| de::Error::missing_field("manifest"))?,
            state,
            history,
        })
    }
}
struct OptionalHistory<'a> {
    builder: &'a mut StorageBackupReplayBuilder,
    counts: &'a mut BTreeMap<String, i64>,
}
impl<'de> DeserializeSeed<'de> for OptionalHistory<'_> {
    type Value = Option<()>;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_option(self)
    }
}
impl<'de> Visitor<'de> for OptionalHistory<'_> {
    type Value = Option<()>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("backup history or null")
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_some<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        SectionObject {
            builder: self.builder,
            counts: self.counts,
            history: true,
        }
        .deserialize(d)?;
        Ok(Some(()))
    }
}
struct SectionObject<'a> {
    builder: &'a mut StorageBackupReplayBuilder,
    counts: &'a mut BTreeMap<String, i64>,
    history: bool,
}
impl<'de> DeserializeSeed<'de> for SectionObject<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for SectionObject<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a sections object")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        if map.next_key::<String>()?.as_deref() != Some("sections") {
            return Err(de::Error::custom("Expected backup sections"));
        }
        map.next_value_seed(SectionMap(self))?;
        if map.next_key::<String>()?.is_some() {
            return Err(de::Error::custom("Unexpected or duplicate sections field"));
        }
        Ok(())
    }
}
struct SectionMap<'a>(SectionObject<'a>);
impl<'de> DeserializeSeed<'de> for SectionMap<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for SectionMap<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("named row sections")
    }
    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
        while let Some(name) = map.next_key::<String>()? {
            let key = if self.0.history {
                let section = History::deserialize(
                    de::value::StringDeserializer::<M::Error>::new(name.clone()),
                )?;
                self.0
                    .builder
                    .begin_history(section)
                    .map_err(de::Error::custom)?;
                format!("history.{name}")
            } else {
                let section = State::deserialize(de::value::StringDeserializer::<M::Error>::new(
                    name.clone(),
                ))?;
                self.0
                    .builder
                    .begin_state(section)
                    .map_err(de::Error::custom)?;
                name
            };
            let count = map.next_value_seed(RowArray(self.0.builder))?;
            self.0.counts.insert(key, count);
        }
        Ok(())
    }
}
struct RowArray<'a>(&'a mut StorageBackupReplayBuilder);
impl<'de> DeserializeSeed<'de> for RowArray<'_> {
    type Value = i64;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<i64, D::Error> {
        d.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for RowArray<'_> {
    type Value = i64;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("backup rows")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<i64, A::Error> {
        let mut count = 0i64;
        while let Some(row) = seq.next_element::<StorageBackupRow>()? {
            self.0.push(row).map_err(de::Error::custom)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("Backup row count overflow"))?;
        }
        Ok(count)
    }
}

#[cfg(feature = "embedded-migrations")]
pub(crate) fn compare(source: &ReplayDocument, restored: &ReplayDocument) -> Result<(), ApiError> {
    use serde_json::Value;
    let mismatch = |section: &str| {
        ApiError::InternalServerError(format!(
            "Restored section '{section}' does not match the verified backup"
        ))
    };
    for &section in State::ALL {
        let mut left = source.snapshot.state_rows(section);
        let mut right = restored.snapshot.state_rows(section);
        loop {
            match (left.next(), right.next()) {
                (None, None) => break,
                (Some(a), Some(b)) => {
                    if a? != b? {
                        return Err(mismatch(section.as_str()));
                    }
                }
                _ => return Err(mismatch(section.as_str())),
            }
        }
    }
    if source.snapshot.includes_history() != restored.snapshot.includes_history() {
        return Err(mismatch("history"));
    }
    if !source.snapshot.includes_history() {
        return Ok(());
    }
    for &section in History::ALL {
        if section == History::ExportArtifacts {
            if artifact_fingerprints(&source.snapshot)?
                != artifact_fingerprints(&restored.snapshot)?
            {
                return Err(mismatch(section.as_str()));
            }
        } else if section == History::AuditEvents {
            // Preserve multiplicities while permitting the one new provenance row.
            let mut retained = BTreeMap::<String, usize>::new();
            for row in source.snapshot.history_rows(section) {
                let mut row = row?;
                row.normalize_legacy_history(section);
                row.canonicalize_history(section);
                *retained.entry(super::serialized_sha256(&row)?).or_default() += 1;
            }
            let mut added = 0;
            for row in restored.snapshot.history_rows(section) {
                let row = row?;
                let key = super::serialized_sha256(&row)?;
                if let Some(count) = retained.get_mut(&key).filter(|count| **count > 0) {
                    *count -= 1;
                } else {
                    if row.get("entity_type").and_then(Value::as_str) != Some("restore")
                        || row.get("action").and_then(Value::as_str) != Some("succeeded")
                    {
                        return Err(mismatch(section.as_str()));
                    }
                    added += 1;
                }
            }
            if added != 1 || retained.values().any(|count| *count != 0) {
                return Err(mismatch(section.as_str()));
            }
        } else if section == History::ComputedFieldHistory {
            let source_rows = source
                .snapshot
                .history_rows(section)
                .collect::<Result<Vec<_>, _>>()?;
            let restored_rows = restored
                .snapshot
                .history_rows(section)
                .collect::<Result<Vec<_>, _>>()?;
            if source_rows == restored_rows {
                continue;
            }
            let ids = source_rows
                .iter()
                .filter_map(|r| r.get("history_entry_id").and_then(Value::as_i64))
                .collect::<BTreeSet<_>>();
            let at = restored_rows
                .iter()
                .filter(|r| {
                    !r.get("history_entry_id")
                        .and_then(Value::as_i64)
                        .is_some_and(|id| ids.contains(&id))
                })
                .filter_map(|r| r.get("valid_from").and_then(Value::as_str))
                .filter_map(|at| DateTime::parse_from_rfc3339(at).ok().map(|at| at.to_utc()))
                .min()
                .filter(|at| *at >= source.metadata.created_at)
                .ok_or_else(|| mismatch(section.as_str()))?;
            if source.snapshot.computed_history_at_boundary(at)? != restored_rows {
                return Err(mismatch(section.as_str()));
            }
        } else {
            let mut left = source.snapshot.history_rows(section);
            let mut right = restored.snapshot.history_rows(section);
            loop {
                match (left.next(), right.next()) {
                    (None, None) => break,
                    (Some(a), Some(b)) => {
                        let mut a = a?;
                        a.normalize_legacy_history(section);
                        a.canonicalize_history(section);
                        let mut b = b?;
                        if section == History::ExportOutputs {
                            a = normalize_artifact_reference(a)?;
                            b = normalize_artifact_reference(b)?;
                        }
                        if a != b {
                            return Err(mismatch(section.as_str()));
                        }
                    }
                    _ => return Err(mismatch(section.as_str())),
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "embedded-migrations")]
fn artifact_fingerprints(
    snapshot: &StorageBackupReplay,
) -> Result<BTreeMap<i32, serde_json::Value>, ApiError> {
    use hubuum_storage_core::artifacts::{BackupArtifactDecoder, BackupArtifactPart};
    let mut decoder = BackupArtifactDecoder::default();
    let mut outputs = BTreeMap::new();
    for row in snapshot.history_rows(History::ExportArtifacts) {
        if let BackupArtifactPart::Header(header) = decoder.push(row?)? {
            outputs.insert(
                header.task_id().id(),
                serde_json::json!({
                    "expires_at": header.expires_at(), "bytes": header.manifest().byte_size(),
                    "sha256": header.manifest().stream_digest(),
                }),
            );
        }
    }
    decoder.finish()?;
    Ok(outputs)
}

#[cfg(feature = "embedded-migrations")]
fn normalize_artifact_reference(row: StorageBackupRow) -> Result<StorageBackupRow, ApiError> {
    let mut value = row.into_value();
    if value
        .get("artifact_reference")
        .is_some_and(|reference| !reference.is_null())
    {
        // Fresh physical identities are expected. The independently verified
        // fingerprint section above retains content and task/expiry binding.
        value["artifact_reference"] = serde_json::Value::Bool(true);
    } else if let Some(fields) = value.as_object_mut() {
        fields.remove("artifact_reference");
    }
    StorageBackupRow::try_from_value(value).map_err(|error| error.into_request_error().into())
}
