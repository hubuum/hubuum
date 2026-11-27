//! Catalog metadata and fixed-width version commitments for the native store.
use hubuum_domain::{ResourceIdentity, VersionIdentity};
use hubuum_storage_core::{
    HISTORY_ARCHIVE_CATALOG_BYTES, HISTORY_ARCHIVE_MAX_VERSIONS, HistoryArchiveCatalog,
    StorageRecordIdentity, history_archive_encoded_bytes,
};
use serde_json::{Value, from_slice, from_value, to_value};
use zstd::{bulk, zstd_safe};

use super::json;
use crate::PostgresStorageError;

const MAGIC: &[u8; 5] = b"HUBC\x01";
const PREFIX_BYTES: usize = MAGIC.len() + 16 + 8 + 4 + 4;
const COMMITMENT_BYTES: usize = 16 + 32;
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const SINGLE_SEGMENT: u8 = 1 << 5;
const DICTIONARY_ID: u8 = 0b11;

fn invalid() -> PostgresStorageError {
    PostgresStorageError::database("Invalid packed archive catalog")
}

fn field<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], PostgresStorageError> {
    let (field, rest) = bytes.split_first_chunk().ok_or_else(invalid)?;
    *bytes = rest;
    Ok(*field)
}

fn versions(metadata: &mut Value) -> Result<&mut Vec<Value>, PostgresStorageError> {
    metadata
        .get_mut("versions")
        .and_then(Value::as_array_mut)
        .ok_or_else(invalid)
}

/// Keep high-entropy UUIDs and SHA-256 commitments verbatim. The rest of the
/// validated logical catalog, including location, dependencies and full clock
/// precision, remains ordinary JSON inside one bounded Zstandard frame.
pub(super) fn encode(catalog: &HistoryArchiveCatalog) -> Result<Vec<u8>, PostgresStorageError> {
    let mut metadata = json(catalog, HISTORY_ARCHIVE_CATALOG_BYTES)?;
    let mut commitments = Vec::with_capacity(catalog.versions().len() * COMMITMENT_BYTES);
    for (value, descriptor) in versions(&mut metadata)?.iter_mut().zip(catalog.versions()) {
        let fields = value.as_object_mut().ok_or_else(invalid)?;
        fields.remove("identity").ok_or_else(invalid)?;
        let digest: [u8; 32] = from_value(fields.remove("payload_digest").ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        commitments.extend_from_slice(descriptor.identity().version().as_bytes());
        commitments.extend_from_slice(&digest);
    }
    let header = history_archive_encoded_bytes(&metadata, HISTORY_ARCHIVE_CATALOG_BYTES)?;
    let mut encoder = bulk::Compressor::new(3).map_err(|_| invalid())?;
    // A single segment's declared size also bounds its decoding window.
    encoder.window_log(22).map_err(|_| invalid())?;
    let frame = encoder.compress(&header).map_err(|_| invalid())?;
    let mut output = Vec::with_capacity(PREFIX_BYTES + frame.len() + commitments.len());
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(catalog.resource().as_bytes());
    output.extend_from_slice(&catalog.generation().value().to_be_bytes());
    output.extend_from_slice(&(catalog.versions().len() as u32).to_be_bytes());
    output.extend_from_slice(&(frame.len() as u32).to_be_bytes());
    output.extend_from_slice(&frame);
    output.extend_from_slice(&commitments);
    Ok(output)
}

pub(super) fn decode(bytes: &[u8]) -> Result<HistoryArchiveCatalog, PostgresStorageError> {
    if bytes.len() > HISTORY_ARCHIVE_CATALOG_BYTES {
        return Err(invalid());
    }
    let mut body = bytes.strip_prefix(MAGIC).ok_or_else(invalid)?;
    let resource = ResourceIdentity::from_bytes(field(&mut body)?).map_err(|_| invalid())?;
    let generation = i64::from_be_bytes(field(&mut body)?);
    let count = u32::from_be_bytes(field(&mut body)?) as usize;
    let frame_len = u32::from_be_bytes(field(&mut body)?) as usize;
    let (frame, commitments) = body.split_at_checked(frame_len).ok_or_else(invalid)?;
    let (members, remainder) = commitments.as_chunks::<COMMITMENT_BYTES>();
    if !remainder.is_empty()
        || count != members.len()
        || count > HISTORY_ARCHIVE_MAX_VERSIONS
        || !frame.starts_with(&ZSTD_MAGIC)
        || frame
            .get(ZSTD_MAGIC.len())
            .is_none_or(|flags| flags & (SINGLE_SEGMENT | DICTIONARY_ID) != SINGLE_SEGMENT)
        || zstd_safe::find_frame_compressed_size(frame).ok() != Some(frame.len())
    {
        return Err(invalid());
    }
    let declared = zstd_safe::get_frame_content_size(frame)
        .ok()
        .flatten()
        .filter(|length| *length <= HISTORY_ARCHIVE_CATALOG_BYTES as u64)
        .ok_or_else(invalid)?;
    let decoded = bulk::decompress(frame, declared as usize).map_err(|_| invalid())?;
    if decoded.len() as u64 != declared {
        return Err(invalid());
    }
    let mut metadata: Value = from_slice(&decoded).map_err(|_| invalid())?;
    if metadata.get("resource") != Some(&to_value(resource).map_err(|_| invalid())?)
        || metadata.get("generation").and_then(Value::as_i64) != Some(generation)
    {
        return Err(invalid());
    }
    let entries = versions(&mut metadata)?;
    if entries.len() != members.len() {
        return Err(invalid());
    }
    for (value, member) in entries.iter_mut().zip(members) {
        let version =
            VersionIdentity::from_bytes(member[..16].try_into().expect("fixed version bytes"))
                .map_err(|_| invalid())?;
        let fields = value.as_object_mut().ok_or_else(invalid)?;
        if fields.contains_key("identity") || fields.contains_key("payload_digest") {
            return Err(invalid());
        }
        fields.insert(
            "identity".into(),
            to_value(StorageRecordIdentity::new(resource, version)).map_err(|_| invalid())?,
        );
        fields.insert(
            "payload_digest".into(),
            to_value(&member[16..]).map_err(|_| invalid())?,
        );
    }
    from_value(metadata).map_err(|error| {
        PostgresStorageError::invalid_persisted_value("packed history catalog", error)
    })
}

#[cfg(test)]
mod tests;
