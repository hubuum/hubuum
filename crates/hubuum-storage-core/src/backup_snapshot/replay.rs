//! Private, replayable row storage. The file handle never escapes the proof.
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use hubuum_domain::{IdentityReference, JsonSchemaLimits, LegacyIdentityCohort};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::Value;

use super::identities::{self, IdentityValidation, StorageIdentityReplay, StorageIdentityUpgrade};
use super::view::{BackupView, ReadError, Rows};
use super::{
    StorageBackupBudget, StorageBackupCaptureProgress, StorageBackupHistorySection as History,
    StorageBackupRow, StorageBackupSnapshot, StorageBackupStateSection as State,
};
use crate::StorageError;
use crate::{StorageIdentityEvidence, StorageIdentitySubject};

fn io_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::internal(format!("Backup spool I/O failed: {error}"))
}
fn invalid(message: impl Into<String>) -> StorageError {
    StorageError::invalid_input(message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Section {
    State(State),
    History(History),
}
#[derive(Clone, Copy, Debug, Default)]
struct Extent {
    start: u64,
    end: u64,
    count: usize,
}

/// Append-only construction with explicit byte and row accounting. Each section
/// must be supplied once, contiguously, including empty sections.
pub struct StorageBackupReplayBuilder {
    writer: BufWriter<File>,
    state: BTreeMap<State, Extent>,
    history: Option<BTreeMap<History, Extent>>,
    active: Option<Section>,
    offset: u64,
    budget: StorageBackupBudget,
    progress: StorageBackupCaptureProgress,
    limits: JsonSchemaLimits,
}
impl StorageBackupReplayBuilder {
    pub fn new(
        budget: StorageBackupBudget,
        limits: JsonSchemaLimits,
        history: bool,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            writer: BufWriter::new(tempfile::tempfile().map_err(io_error)?),
            state: BTreeMap::new(),
            history: history.then(BTreeMap::new),
            active: None,
            offset: 0,
            budget,
            progress: StorageBackupCaptureProgress::new(budget),
            limits,
        })
    }
    pub fn begin_state(&mut self, section: State) -> Result<(), StorageError> {
        self.begin(Section::State(section))
    }
    pub fn begin_history(&mut self, section: History) -> Result<(), StorageError> {
        self.begin(Section::History(section))
    }
    fn begin(&mut self, section: Section) -> Result<(), StorageError> {
        let extent = Extent {
            start: self.offset,
            end: self.offset,
            count: 0,
        };
        let previous = match section {
            Section::State(s) => self.state.insert(s, extent),
            Section::History(s) => self
                .history
                .as_mut()
                .ok_or_else(|| invalid("History was not enabled for this backup"))?
                .insert(s, extent),
        };
        if previous.is_some() {
            return Err(invalid("Duplicate backup section"));
        }
        self.active = Some(section);
        Ok(())
    }
    pub fn push(&mut self, row: StorageBackupRow) -> Result<(), StorageError> {
        let section = self
            .active
            .ok_or_else(|| invalid("Begin a backup section before appending rows"))?;
        self.progress.scan_row()?;
        self.progress.retain_row(&row)?;
        // The accounting pass above bounds this individual allocation too.
        let bytes = serde_json::to_vec(&row).map_err(io_error)?;
        self.writer.write_all(&bytes).map_err(io_error)?;
        self.writer.write_all(b"\n").map_err(io_error)?;
        self.offset = self
            .offset
            .checked_add(bytes.len() as u64 + 1)
            .ok_or_else(|| invalid("Backup spool size overflow"))?;
        let extent = match section {
            Section::State(s) => self.state.get_mut(&s).expect("active section"),
            Section::History(s) => self
                .history
                .as_mut()
                .expect("active history")
                .get_mut(&s)
                .expect("active section"),
        };
        extent.end = self.offset;
        extent.count += 1;
        Ok(())
    }
    /// Mark a parsed document as history-free before sealing it.
    pub fn without_history(mut self) -> Result<Self, StorageError> {
        if self.history.as_ref().is_some_and(|h| !h.is_empty()) {
            return Err(invalid("Cannot discard supplied backup history"));
        }
        self.history = None;
        Ok(self)
    }
    fn seal(mut self) -> Result<StorageBackupReplay, StorageError> {
        self.writer.flush().map_err(io_error)?;
        let file = self.writer.into_inner().map_err(io_error)?;
        Ok(StorageBackupReplay {
            file: Arc::new(Mutex::new(file)),
            state: self.state,
            history: self.history,
            limits: self.limits,
            budget: self.budget,
        })
    }
    /// Validate a capture using the same structural proof as a materialized snapshot.
    pub fn finish(self) -> Result<StorageBackupReplay, StorageError> {
        self.seal()?.normalize(None)
    }
    /// Apply legacy repairs and destination resource validation before restore.
    pub fn finish_for_restore(
        self,
        backup_version: i32,
    ) -> Result<StorageBackupReplay, StorageError> {
        self.seal()?.normalize(Some(backup_version))
    }
}

/// A validated snapshot whose row bodies live in an owner-only, automatically
/// removed spool. Clones share immutable contents; replay cursors are independent.
#[derive(Clone, Debug)]
pub struct StorageBackupReplay {
    file: Arc<Mutex<File>>,
    state: BTreeMap<State, Extent>,
    history: Option<BTreeMap<History, Extent>>,
    limits: JsonSchemaLimits,
    budget: StorageBackupBudget,
}

struct PositionedReader {
    file: Arc<Mutex<File>>,
    position: u64,
    end: u64,
}
impl Read for PositionedReader {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = bytes
            .len()
            .min(usize::try_from(self.end - self.position).unwrap_or(usize::MAX));
        if count == 0 {
            return Ok(0);
        }
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("Backup spool lock poisoned"))?;
        file.seek(SeekFrom::Start(self.position))?;
        let count = file.read(&mut bytes[..count])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Backup spool was truncated",
            ));
        }
        self.position += count as u64;
        Ok(count)
    }
}
/// Fallible, bounded replay of one section; an I/O or decoding failure ends it.
pub struct StorageBackupReplayRows {
    reader: BufReader<PositionedReader>,
    remaining: usize,
    buffer: Vec<u8>,
}
impl Iterator for StorageBackupReplayRows {
    type Item = Result<StorageBackupRow, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.buffer.clear();
        let result = self
            .reader
            .read_until(b'\n', &mut self.buffer)
            .map_err(io_error)
            .and_then(|count| {
                if count == 0 {
                    return Err(io_error("missing backup row"));
                }
                serde_json::from_slice(&self.buffer).map_err(io_error)
            });
        if result.is_err() {
            self.remaining = 0;
        } else {
            self.remaining -= 1;
        }
        Some(result)
    }
}
impl StorageBackupReplay {
    pub(super) fn identity_restore_boundary(
        &self,
        cohort: LegacyIdentityCohort,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<StorageIdentityReplay, StorageError> {
        let prepared = self.at_restore_boundary(at)?;
        let identities = super::identity_baseline::BaselineIdentities::new(self, cohort)?;
        let mut builder = StorageBackupReplayBuilder::new(prepared.budget, prepared.limits, true)?;
        for &section in State::ALL {
            builder.begin_state(section)?;
            for row in prepared.state_rows(section) {
                let mut row = row?;
                if !self.includes_history()
                    && super::revisions::REVISION_HISTORY_SECTIONS
                        .iter()
                        .any(|(_, state)| *state == section)
                {
                    identities.resource(&mut row)?;
                }
                builder.push(row)?;
            }
        }
        for &section in History::ALL {
            builder.begin_history(section)?;
            for (index, row) in prepared.history_rows(section).enumerate() {
                let mut row = row?;
                if index >= self.history_count(section) {
                    identities.history(section, &mut row)?;
                }
                builder.push(row)?;
            }
        }
        builder
            .finish()?
            .upgrade_logical_identities(StorageIdentityUpgrade::Native)
    }
    /// Validate or upgrade identity evidence while data remains quarantined in
    /// a bounded replay spool. Repeated legacy attempts with the same persisted
    /// cohort produce the same assignments, including history-only resources.
    pub fn upgrade_logical_identities(
        &self,
        upgrade: StorageIdentityUpgrade,
    ) -> Result<StorageIdentityReplay, StorageError> {
        let cohort = match upgrade {
            StorageIdentityUpgrade::Native => None,
            StorageIdentityUpgrade::Legacy(cohort) => Some(cohort),
        };
        let mut open_versions = BTreeMap::new();
        let mut legacy_principals = BTreeMap::new();
        if let Some(cohort) = cohort {
            for row in self.state_rows(State::Principals) {
                let row = row?;
                let subject = identities::legacy_subject(
                    identities::IdentityFamily::Principal,
                    State::Principals.as_str(),
                    &row,
                    cohort,
                )?;
                legacy_principals.insert(row.get("id").and_then(Value::as_i64), subject);
            }
            for &(history, state) in super::revisions::REVISION_HISTORY_SECTIONS {
                for row in self.history_rows(history) {
                    let row = row?;
                    if row.get("valid_to").is_none_or(|value| !value.is_null()) {
                        continue;
                    }
                    if let Some(StorageIdentitySubject::Resource {
                        identity: IdentityReference::Known { identity },
                        version,
                    }) = identities::legacy_subject(
                        identities::history_family(history),
                        history.as_str(),
                        &row,
                        cohort,
                    )? && open_versions.insert((state, identity), version).is_some()
                    {
                        return Err(invalid(
                            "Legacy backup contains multiple open versions for one resource incarnation",
                        ));
                    }
                }
            }
        }
        let budget = StorageBackupBudget::new(
            self.budget.max_bytes().saturating_mul(4),
            self.budget.max_rows(),
        )
        .map_err(|error| error.into_request_error())?;
        // Native evidence validation changes no row. Retain the existing
        // structural proof and spool instead of serializing and normalizing
        // every payload again merely to attach an identity proof.
        let mut builder = cohort
            .map(|_| StorageBackupReplayBuilder::new(budget, self.limits, self.includes_history()))
            .transpose()?;
        let mut validation = IdentityValidation::default();
        let mut unresolved_references = 0;
        let mut prepare = |family,
                           section: &str,
                           mut row: StorageBackupRow,
                           state: Option<State>|
         -> Result<StorageBackupRow, StorageError> {
            if let Some(cohort) = cohort {
                identities::upgrade_schema_snapshot(&mut row, cohort)?;
                let mut subject = identities::legacy_subject(family, section, &row, cohort)?;
                if family == identities::IdentityFamily::PrincipalExtension {
                    subject = legacy_principals
                        .get(&row.get("id").and_then(Value::as_i64))
                        .copied()
                        .ok_or_else(|| invalid("Principal extension has no owning principal"))?;
                }
                if let (
                    Some(state),
                    Some(StorageIdentitySubject::Resource {
                        identity: IdentityReference::Known { identity },
                        version,
                    }),
                ) = (state, &mut subject)
                    && let Some(open_version) = open_versions.get(&(state, *identity))
                {
                    *version = *open_version;
                }
                let references = identities::legacy_references(&row, cohort)?;
                let evidence = StorageIdentityEvidence::try_new(subject, references)
                    .map_err(|error| error.into_request_error())?;
                row.0.insert(
                    "logical_identity".into(),
                    serde_json::to_value(evidence).map_err(io_error)?,
                );
            }
            let evidence = validation.row(family, &row, state.is_none())?;
            unresolved_references += evidence
                .references()
                .values()
                .filter(|reference| !reference.is_resolved())
                .count();
            if evidence.subject().is_some_and(|subject| {
                matches!(
                    subject,
                    StorageIdentitySubject::Resource {
                        identity: IdentityReference::Unresolved { .. },
                        ..
                    } | StorageIdentitySubject::Principal {
                        identity: IdentityReference::Unresolved { .. }
                    } | StorageIdentitySubject::Task {
                        identity: IdentityReference::Unresolved { .. }
                    }
                )
            }) {
                unresolved_references += 1;
            }
            Ok(row)
        };
        for &section in State::ALL {
            if let Some(builder) = &mut builder {
                builder.begin_state(section)?;
            }
            for row in self.state_rows(section) {
                let row = row?;
                let row = match identities::state_family(section) {
                    Some(family) => prepare(family, section.as_str(), row, Some(section))?,
                    None => row,
                };
                if let Some(builder) = &mut builder {
                    builder.push(row)?;
                }
            }
        }
        if self.includes_history() {
            for &section in History::ALL {
                if let Some(builder) = &mut builder {
                    builder.begin_history(section)?;
                }
                for row in self.history_rows(section) {
                    let row = prepare(
                        identities::history_family(section),
                        section.as_str(),
                        row?,
                        None,
                    )?;
                    if let Some(builder) = &mut builder {
                        builder.push(row)?;
                    }
                }
            }
        }
        validation.finish()?;
        Ok(StorageIdentityReplay {
            replay: match builder {
                Some(builder) => builder.finish()?,
                None => self.clone(),
            },
            legacy_cohort: cohort,
            unresolved_references,
        })
    }

    fn rows(&self, extent: Extent) -> StorageBackupReplayRows {
        StorageBackupReplayRows {
            reader: BufReader::new(PositionedReader {
                file: self.file.clone(),
                position: extent.start,
                end: extent.end,
            }),
            remaining: extent.count,
            buffer: Vec::new(),
        }
    }
    pub fn state_rows(&self, section: State) -> StorageBackupReplayRows {
        self.rows(self.state[&section])
    }
    pub fn history_rows(&self, section: History) -> StorageBackupReplayRows {
        self.rows(
            self.history
                .as_ref()
                .map_or(Extent::default(), |h| h[&section]),
        )
    }
    pub fn includes_history(&self) -> bool {
        self.history.is_some()
    }
    pub fn state_count(&self, section: State) -> usize {
        self.state[&section].count
    }
    pub fn history_count(&self, section: History) -> usize {
        self.history.as_ref().map_or(0, |h| h[&section].count)
    }
    pub fn state(&self) -> impl Serialize + '_ {
        StateSerialization(self)
    }
    pub fn history(&self) -> Option<impl Serialize + '_> {
        self.history.as_ref().map(|_| HistorySerialization(self))
    }
    pub fn materialize(&self) -> Result<StorageBackupSnapshot, StorageError> {
        let state_sections = State::ALL
            .iter()
            .map(|&s| Ok((s, self.state_rows(s).collect::<Result<_, _>>()?)))
            .collect::<Result<_, StorageError>>()?;
        let history_sections = if self.includes_history() {
            Some(
                History::ALL
                    .iter()
                    .map(|&s| Ok((s, self.history_rows(s).collect::<Result<_, _>>()?)))
                    .collect::<Result<_, StorageError>>()?,
            )
        } else {
            None
        };
        Ok(StorageBackupSnapshot {
            schema_limits: self.limits,
            state_sections,
            history_sections,
        })
    }
    pub fn from_snapshot(
        snapshot: StorageBackupSnapshot,
        budget: StorageBackupBudget,
    ) -> Result<Self, StorageError> {
        let mut builder = StorageBackupReplayBuilder::new(
            budget,
            snapshot.schema_limits(),
            snapshot.includes_history(),
        )?;
        let (state, history) = snapshot.into_parts();
        for (section, rows) in state {
            builder.begin_state(section)?;
            for row in rows {
                builder.push(row)?;
            }
        }
        if let Some(history) = history {
            for (section, rows) in history {
                builder.begin_history(section)?;
                for row in rows {
                    builder.push(row)?;
                }
            }
        }
        // The input already carries the exact same validation proof.
        builder.seal()
    }
    pub fn validate_resources(self) -> Result<Self, StorageError> {
        super::resources::validate_view(&self, self.limits).map_err(ReadError::into_storage)?;
        Ok(self)
    }
    pub fn with_schema_limits(mut self, limits: JsonSchemaLimits) -> Result<Self, StorageError> {
        if self.limits != limits {
            self.limits = limits;
            super::schemas::validate(&self).map_err(ReadError::into_storage)?;
        }
        Ok(self)
    }
    fn validate(&self, resources: bool) -> Result<(), StorageError> {
        let result = (|| {
            if resources {
                super::resources::validate_view(self, self.limits)?;
            }
            super::revisions::validate_backup_revisions(self)?;
            super::schemas::validate(self)?;
            super::query_usage::validate(self)?;
            super::computed::validate(self)
        })();
        result.map_err(ReadError::into_storage)
    }

    /// Retained computed intervals plus the required restore-boundary observations.
    pub fn computed_history_at_boundary(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<StorageBackupRow>, StorageError> {
        let mut rows = self
            .history_rows(History::ComputedFieldHistory)
            .collect::<Result<Vec<_>, _>>()?;
        rows.extend(super::computed::missing_baselines(self, at).map_err(ReadError::into_storage)?);
        Ok(rows)
    }
    /// Prepare deterministic history baselines without retaining the row bodies.
    pub fn at_restore_boundary(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self, StorageError> {
        let at = chrono::DateTime::from_timestamp_micros(at.timestamp_micros())
            .expect("valid timestamp");
        let computed =
            super::computed::missing_baselines(self, at).map_err(ReadError::into_storage)?;
        if self.includes_history() && computed.is_empty() {
            return Ok(self.clone());
        }
        let budget = StorageBackupBudget::new(
            self.budget.max_bytes().saturating_mul(4),
            self.budget.max_rows().saturating_mul(3),
        )
        .map_err(|e| e.into_request_error())?;
        let mut builder = StorageBackupReplayBuilder::new(budget, self.limits, true)?;
        for &section in State::ALL {
            builder.begin_state(section)?;
            for row in self.state_rows(section) {
                builder.push(row?)?;
            }
        }
        for &section in History::ALL {
            builder.begin_history(section)?;
            if self.includes_history() {
                for row in self.history_rows(section) {
                    builder.push(row?)?;
                }
            } else if let Some((_, state)) = super::revisions::REVISION_HISTORY_SECTIONS
                .iter()
                .find(|(h, _)| *h == section)
            {
                let mut rows = self.state_rows(*state);
                let mut offset = self.state[state].start;
                let mut index = Vec::with_capacity(self.state_count(*state));
                while let Some(row) = rows.next() {
                    let row = row?;
                    let end = offset + rows.buffer.len() as u64;
                    index.push((
                        row.get("id").and_then(Value::as_i64),
                        Extent {
                            start: offset,
                            end,
                            count: 1,
                        },
                    ));
                    offset = end;
                }
                index.sort_unstable_by_key(|(id, _)| *id);
                for (ordinal, (_, extent)) in index.into_iter().enumerate() {
                    let row = self.rows(extent).next().expect("indexed row")?;
                    builder.push(super::baseline::baseline(&row, (ordinal + 1) as i64, at))?;
                }
            } else if section == History::ClassSchemaHistory {
                let mut schemas = self
                    .state_rows(State::ClassSchemaRevisions)
                    .collect::<Result<Vec<_>, _>>()?;
                schemas.sort_by_key(|r| {
                    (
                        r.get("class_id").and_then(Value::as_i64),
                        r.get("revision").and_then(Value::as_i64),
                    )
                });
                for (i, row) in schemas.into_iter().enumerate() {
                    builder.push(StorageBackupRow::try_from_value(serde_json::json!({
                        "id": i + 1, "class_id": row.get("class_id"), "revision": row.get("revision"),
                        "snapshot": row.fields(), "operation": "create", "occurred_at": at,
                        "actor_principal_id": null, "task_id": null
                    })).expect("literal object"))?;
                }
            }
            if section == History::ComputedFieldHistory {
                for row in &computed {
                    builder.push(row.clone())?;
                }
            }
        }
        builder.seal()
    }
    fn normalize(self, version: Option<i32>) -> Result<Self, StorageError> {
        for &section in State::ALL {
            if !self.state.contains_key(&section)
                && !matches!(
                    section,
                    State::EventSinkCollectionGrants | State::QueryUsageDeclarations
                )
            {
                return Err(invalid(format!(
                    "Backup snapshot is missing required state section '{section}'"
                )));
            }
        }
        if let Some(history) = &self.history {
            for &section in History::ALL {
                if !history.contains_key(&section) && section != History::ComputedFieldHistory {
                    return Err(invalid(format!(
                        "Backup snapshot is missing required history section '{section}'"
                    )));
                }
            }
        }
        // Cross-task discovery uses only retained output metadata. Other sections
        // are normalized one row at a time, including the arbitrarily large ones.
        let mut discovery = super::task_discovery::DiscoveryIndex::default();
        if let Some(extent) = self
            .history
            .as_ref()
            .and_then(|h| h.get(&History::ExportOutputs))
        {
            for row in self.rows(*extent) {
                discovery
                    .insert(&row?)
                    .map_err(|e| e.into_request_error())?;
            }
        }
        let mut builder =
            StorageBackupReplayBuilder::new(self.budget, self.limits, self.includes_history())?;
        for &section in State::ALL {
            builder.begin_state(section)?;
            if let Some(extent) = self.state.get(&section) {
                for row in self.rows(*extent) {
                    let mut state = BTreeMap::from([(section, vec![row?])]);
                    if let Some(version) = version {
                        super::normalization::normalize_legacy_event_configuration(
                            version, &mut state, None,
                        );
                        super::normalization::normalize_legacy_class_schema_policies(
                            version, &mut state, None,
                        );
                        super::normalization::normalize_legacy_relation_labels(
                            version, &mut state, None,
                        );
                    }
                    if section == State::EventSinks {
                        StorageBackupSnapshot::normalize_legacy_event_sink_access(&mut state);
                    }
                    builder.push(state.remove(&section).expect("source section").remove(0))?;
                }
            } else if section == State::EventSinkCollectionGrants {
                let mut grants = BTreeSet::new();
                for row in self.state_rows(State::EventSubscriptions) {
                    let row = row?;
                    if let (Some(sink), Some(collection)) = (
                        row.get("sink_id").and_then(Value::as_i64),
                        row.get("collection_id").and_then(Value::as_i64),
                    ) {
                        grants.insert((sink, collection));
                    }
                }
                for (sink, collection) in grants {
                    builder.push(
                        StorageBackupRow::try_from_value(
                            serde_json::json!({"sink_id":sink,"collection_id":collection}),
                        )
                        .expect("literal object"),
                    )?;
                }
            }
        }
        if let Some(history) = &self.history {
            for &section in History::ALL {
                builder.begin_history(section)?;
                if let Some(extent) = history.get(&section) {
                    for row in self.rows(*extent) {
                        let mut rows = BTreeMap::from([(section, vec![row?])]);
                        if let Some(version) = version {
                            let mut state = BTreeMap::new();
                            super::normalization::normalize_legacy_event_configuration(
                                version,
                                &mut state,
                                Some(&mut rows),
                            );
                            super::normalization::normalize_legacy_class_schema_policies(
                                version,
                                &mut state,
                                Some(&mut rows),
                            );
                            super::normalization::normalize_legacy_relation_labels(
                                version,
                                &mut state,
                                Some(&mut rows),
                            );
                        }
                        let mut row = rows.remove(&section).expect("source section").remove(0);
                        if section == History::TerminalTasks {
                            discovery
                                .apply(&mut row)
                                .map_err(|e| e.into_request_error())?;
                        }
                        row.canonicalize_history(section);
                        if section == History::TerminalTasks {
                            crate::task_control::validate_control_snapshot(&row)
                                .map_err(|e| e.into_request_error())?;
                            if let Some(value) = row.get("discovery_metadata") {
                                let kind = row
                                    .get("kind")
                                    .and_then(Value::as_str)
                                    .and_then(crate::StorageTaskKind::from_persisted)
                                    .ok_or_else(|| invalid("Invalid task metadata kind"))?;
                                let metadata =
                                    crate::StorageTaskMetadata::from_persisted(kind, value.clone())
                                        .map_err(|e| e.into_request_error())?;
                                row.0
                                    .insert("discovery_metadata".into(), metadata.to_value());
                            }
                        }
                        builder.push(row)?;
                    }
                }
            }
        }
        let normalized = builder.seal()?;
        normalized.validate(version.is_some())?;
        Ok(normalized)
    }
}
impl BackupView for StorageBackupReplay {
    fn state_rows(&self, section: State) -> Result<Rows<'_>, ReadError> {
        let extent = *self
            .state
            .get(&section)
            .ok_or_else(|| ReadError::invalid(format!("Missing backup section '{section}'")))?;
        Ok(Rows::new(extent.count, move || {
            Box::new(
                self.rows(extent)
                    .map(|r| r.map(Cow::Owned).map_err(ReadError::from)),
            )
        }))
    }
    fn history_rows(&self, section: History) -> Result<Rows<'_>, ReadError> {
        let extent = match &self.history {
            None => Extent::default(),
            Some(h) => *h
                .get(&section)
                .ok_or_else(|| ReadError::invalid(format!("Missing backup section '{section}'")))?,
        };
        Ok(Rows::new(extent.count, move || {
            Box::new(
                self.rows(extent)
                    .map(|r| r.map(Cow::Owned).map_err(ReadError::from)),
            )
        }))
    }
    fn has_state(&self, section: State) -> bool {
        self.state.contains_key(&section)
    }
    fn includes_history(&self) -> bool {
        self.includes_history()
    }
    fn schema_limits(&self) -> JsonSchemaLimits {
        self.limits
    }
}
struct StateSerialization<'a>(&'a StorageBackupReplay);
impl Serialize for StateSerialization<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.state.len()))?;
        for (&section, &extent) in &self.0.state {
            map.serialize_entry(&section, &RowSerialization(self.0, extent))?;
        }
        map.end()
    }
}
struct HistorySerialization<'a>(&'a StorageBackupReplay);
impl Serialize for HistorySerialization<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let history = self.0.history.as_ref().expect("history serializer");
        let mut map = serializer.serialize_map(Some(history.len()))?;
        for (&section, &extent) in history {
            map.serialize_entry(&section, &RowSerialization(self.0, extent))?;
        }
        map.end()
    }
}
struct RowSerialization<'a>(&'a StorageBackupReplay, Extent);
impl Serialize for RowSerialization<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.1.count))?;
        for row in self.0.rows(self.1) {
            seq.serialize_element(&row.map_err(serde::ser::Error::custom)?)?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageErrorKind;
    use serde_json::json;

    fn source() -> StorageBackupSnapshot {
        let mut state = State::ALL
            .iter()
            .map(|&s| (s, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        state.insert(
            State::Objects,
            [2, 1]
                .into_iter()
                .map(|id| {
                    StorageBackupRow::try_from_value(
                        json!({"id":id,"revision":7,"class_id":1,"data":{"text":"line\n雪"}}),
                    )
                    .unwrap()
                })
                .collect(),
        );
        StorageBackupSnapshot::try_new(state, None).unwrap()
    }
    fn replay() -> StorageBackupReplay {
        StorageBackupReplay::from_snapshot(
            source(),
            StorageBackupBudget::new(100_000, 100).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn independent_replay_cursors_do_not_share_offsets() {
        let snapshot = replay();
        let mut first = snapshot.state_rows(State::Objects);
        let mut second = snapshot.state_rows(State::Objects);
        let first_row = first.next().unwrap().unwrap();
        let second_row = first.next().unwrap().unwrap();
        assert_eq!(
            [
                second.next().unwrap().unwrap(),
                second.next().unwrap().unwrap()
            ],
            [first_row, second_row]
        );
    }
    #[test]
    fn truncated_spool_is_a_fallible_read() {
        let snapshot = replay();
        snapshot.file.lock().unwrap().set_len(0).unwrap();
        let mut rows = snapshot.state_rows(State::Objects);
        assert_eq!(
            rows.next().unwrap().unwrap_err().kind(),
            StorageErrorKind::Internal
        );
        assert!(rows.next().is_none());
    }
    #[test]
    fn history_free_replay_preserves_deterministic_baseline_identities() {
        let at = chrono::DateTime::parse_from_rfc3339("2026-10-24T12:00:00.123456789Z")
            .unwrap()
            .to_utc();
        let actual = replay()
            .at_restore_boundary(at)
            .unwrap()
            .materialize()
            .unwrap();
        let (state, history) = actual.into_parts();
        assert_eq!(state, source().into_parts().0);
        let rows = &history.unwrap()[&History::ObjectHistory];
        let identities = rows
            .iter()
            .map(|row| {
                (
                    row.get("id").unwrap().as_i64().unwrap(),
                    row.get("history_entry_id").unwrap().as_i64().unwrap(),
                    row.get("revision").unwrap().as_i64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(identities, vec![(1, 1, 7), (2, 2, 7)]);
        assert!(
            rows.iter()
                .all(|row| row.get("valid_from") == Some(&json!("2026-10-24T12:00:00.123456Z")))
        );
    }
    #[test]
    fn spool_row_budget_rejects_the_next_row() {
        let mut builder = StorageBackupReplayBuilder::new(
            StorageBackupBudget::new(100, 1).unwrap(),
            JsonSchemaLimits::default(),
            false,
        )
        .unwrap();
        builder.begin_state(State::Objects).unwrap();
        let row = StorageBackupRow::try_from_value(json!({})).unwrap();
        builder.push(row.clone()).unwrap();
        assert_eq!(
            builder.push(row).unwrap_err().kind(),
            StorageErrorKind::InputTooLarge
        );
    }
    #[test]
    fn duplicate_sections_are_rejected() {
        let mut builder = StorageBackupReplayBuilder::new(
            StorageBackupBudget::new(100, 1).unwrap(),
            JsonSchemaLimits::default(),
            false,
        )
        .unwrap();
        builder.begin_state(State::Objects).unwrap();
        assert!(builder.begin_state(State::Objects).is_err());
    }
}
