use super::{
    ArtifactError, ArtifactResult, DateTime, Entry, MAX_RECORDS, ResourceIdentity, Utc,
    VersionIdentity, VersionInterval,
};

const ENTRY_BYTES: usize = 64;

#[derive(Clone, Copy)]
#[repr(u16)]
pub(super) enum SegmentFormat {
    Json = 1,
    Compact = 2,
}

impl SegmentFormat {
    pub(super) fn header(self) -> &'static [u8; 5] {
        match self {
            Self::Json => b"HUBH\x01",
            Self::Compact => b"HUBH\x02",
        }
    }

    pub(super) fn footer(self) -> &'static [u8; 5] {
        match self {
            Self::Json => b"HUBI\x01",
            Self::Compact => b"HUBI\x02",
        }
    }

    pub(super) fn from_header(header: &[u8]) -> ArtifactResult<Self> {
        [Self::Json, Self::Compact]
            .into_iter()
            .find(|format| format.header() == header)
            .ok_or_else(|| ArtifactError::Corrupt("Unsupported history segment format".into()))
    }

    pub(super) fn max_index_bytes(self) -> usize {
        match self {
            Self::Json => super::INDEX_BYTES,
            Self::Compact => MAX_RECORDS * ENTRY_BYTES,
        }
    }

    pub(super) fn decode_index(self, bytes: &[u8]) -> ArtifactResult<Vec<Entry>> {
        match self {
            Self::Json => serde_json::from_slice(bytes)
                .map_err(|_| ArtifactError::Corrupt("Invalid history index".into())),
            Self::Compact => {
                let (entries, remainder) = bytes.as_chunks::<ENTRY_BYTES>();
                if !remainder.is_empty() {
                    return Err(ArtifactError::Corrupt(
                        "Partial compact history index entry".into(),
                    ));
                }
                entries.iter().map(decode_entry).collect()
            }
        }
    }
}

pub(super) fn encode_compact(entries: &[Entry]) -> Vec<u8> {
    let mut index = Vec::with_capacity(entries.len() * ENTRY_BYTES);
    for entry in entries {
        index.extend_from_slice(entry.interval.resource.as_bytes());
        index.extend_from_slice(entry.interval.version.as_bytes());
        for instant in [entry.interval.valid_from, entry.interval.valid_to] {
            index.extend_from_slice(&instant.timestamp().to_le_bytes());
            index.extend_from_slice(&instant.timestamp_subsec_nanos().to_le_bytes());
        }
        index.extend_from_slice(&entry.offset.to_le_bytes());
        index.extend_from_slice(&entry.length.to_le_bytes());
    }
    index
}

// The complete fixed-width entry has been checked before any fields are consumed.
fn take<const N: usize>(input: &mut &[u8]) -> [u8; N] {
    let (field, rest) = input.split_first_chunk().expect("fixed-width index entry");
    *input = rest;
    *field
}

fn decode_instant(input: &mut &[u8]) -> ArtifactResult<DateTime<Utc>> {
    DateTime::from_timestamp(
        i64::from_le_bytes(take(input)),
        u32::from_le_bytes(take(input)),
    )
    .ok_or_else(|| ArtifactError::Corrupt("Invalid history index timestamp".into()))
}

fn decode_entry(entry: &[u8; ENTRY_BYTES]) -> ArtifactResult<Entry> {
    let mut input = entry.as_slice();
    let resource = ResourceIdentity::from_bytes(take(&mut input))
        .map_err(|_| ArtifactError::Corrupt("Invalid history index resource".into()))?;
    let version = VersionIdentity::from_bytes(take(&mut input))
        .map_err(|_| ArtifactError::Corrupt("Invalid history index version".into()))?;
    let interval = VersionInterval::new(
        resource,
        version,
        decode_instant(&mut input)?,
        decode_instant(&mut input)?,
    )
    .map_err(|error| ArtifactError::Corrupt(error.to_string()))?;
    Ok(Entry {
        interval,
        offset: u32::from_le_bytes(take(&mut input)),
        length: u32::from_le_bytes(take(&mut input)),
    })
}
