use super::{
    ArtifactError, ArtifactResult, DateTime, Entry, MAX_RECORDS, ResourceIdentity, Utc,
    VersionIdentity, VersionInterval,
};

mod compressed;
mod version_key;
pub(super) use version_key::{KeyWidth, VersionKey};

const ENTRY_BYTES: usize = 64;

pub(super) struct IndexedEntry {
    pub(super) resource: ResourceIdentity,
    pub(super) version: VersionKey,
    pub(super) from: DateTime<Utc>,
    pub(super) to: DateTime<Utc>,
    pub(super) offset: u32,
    pub(super) length: u32,
}

impl IndexedEntry {
    pub(super) fn matches(&self, interval: &VersionInterval, width: KeyWidth) -> bool {
        self.resource == interval.resource
            && self.version == width.key(interval.version)
            && self.from == interval.valid_from
            && self.to == interval.valid_to
    }
}

impl From<Entry> for IndexedEntry {
    fn from(entry: Entry) -> Self {
        Self {
            resource: entry.interval.resource,
            version: KeyWidth::Full.key(entry.interval.version),
            from: entry.interval.valid_from,
            to: entry.interval.valid_to,
            offset: entry.offset,
            length: entry.length,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u16)]
pub(super) enum SegmentFormat {
    Json = 1,
    Compact = 2,
    CompressedIndex = 3,
}

impl SegmentFormat {
    pub(super) fn header(self) -> &'static [u8; 5] {
        match self {
            Self::Json => b"HUBH\x01",
            Self::Compact => b"HUBH\x02",
            Self::CompressedIndex => b"HUBH\x03",
        }
    }

    pub(super) fn footer(self) -> &'static [u8; 5] {
        match self {
            Self::Json => b"HUBI\x01",
            Self::Compact => b"HUBI\x02",
            Self::CompressedIndex => b"HUBI\x03",
        }
    }

    pub(super) fn from_header(header: &[u8]) -> ArtifactResult<Self> {
        [Self::Json, Self::Compact, Self::CompressedIndex]
            .into_iter()
            .find(|format| format.header() == header)
            .ok_or_else(|| ArtifactError::Corrupt("Unsupported history segment format".into()))
    }

    pub(super) fn max_index_bytes(self) -> usize {
        match self {
            Self::Json => super::INDEX_BYTES,
            Self::Compact => MAX_RECORDS * ENTRY_BYTES,
            Self::CompressedIndex => 1024 * 1024,
        }
    }

    pub(super) fn decode_index(
        self,
        bytes: &[u8],
    ) -> ArtifactResult<(KeyWidth, Vec<IndexedEntry>)> {
        match self {
            Self::Json => {
                let entries: Vec<Entry> = serde_json::from_slice(bytes)
                    .map_err(|_| ArtifactError::Corrupt("Invalid history index".into()))?;
                Ok((
                    KeyWidth::Full,
                    entries.into_iter().map(Into::into).collect(),
                ))
            }
            Self::Compact => decode_binary(bytes, KeyWidth::Full),
            Self::CompressedIndex => {
                let (&width, frame) = bytes.split_first().ok_or_else(|| {
                    ArtifactError::Corrupt("Missing history index key width".into())
                })?;
                let width = KeyWidth::from_byte(width)?;
                let stride = entry_bytes(width);
                let columns = compressed::decode(frame, stride)?;
                let count = columns.len() / stride;
                let mut entries = Vec::with_capacity(count);
                let mut row = [0_u8; ENTRY_BYTES];
                for position in 0..count {
                    for (field, column) in row.iter_mut().zip(columns.chunks_exact(count)) {
                        *field = field.wrapping_add(column[position]);
                    }
                    entries.push(decode_entry(&row[..stride], width)?);
                }
                Ok((width, entries))
            }
        }
    }
}

pub(super) fn encode_compressed(entries: &[Entry]) -> ArtifactResult<Vec<u8>> {
    let width = KeyWidth::for_entries(entries);
    let mut index = Vec::with_capacity(entries.len() * entry_bytes(width));
    for entry in entries {
        index.extend_from_slice(entry.interval.resource.as_bytes());
        index.extend_from_slice(&entry.interval.version.as_bytes()[..width as usize]);
        for instant in [entry.interval.valid_from, entry.interval.valid_to] {
            index.extend_from_slice(&instant.timestamp().to_le_bytes());
            index.extend_from_slice(&instant.timestamp_subsec_nanos().to_le_bytes());
        }
        index.extend_from_slice(&entry.offset.to_le_bytes());
        index.extend_from_slice(&entry.length.to_le_bytes());
    }
    // Store one column per byte position. Adjacent timestamps and frame
    // offsets often advance by a fixed step. Differences are modulo 256,
    // so arbitrary UUIDs and non-monotonic record order also round-trip.
    let stride = entry_bytes(width);
    let mut columns = Vec::with_capacity(index.len());
    for field in 0..stride {
        let mut previous = 0;
        for row in index.chunks_exact(stride) {
            let current = row[field];
            columns.push(current.wrapping_sub(previous));
            previous = current;
        }
    }
    let frame = compressed::encode(&columns)?;
    let mut encoded = Vec::with_capacity(1 + frame.len());
    encoded.push(width as u8);
    encoded.extend_from_slice(&frame);
    Ok(encoded)
}

fn entry_bytes(width: KeyWidth) -> usize {
    ENTRY_BYTES - 16 + width as usize
}

fn decode_binary(bytes: &[u8], width: KeyWidth) -> ArtifactResult<(KeyWidth, Vec<IndexedEntry>)> {
    let entries = bytes.chunks_exact(entry_bytes(width));
    if !entries.remainder().is_empty() {
        return Err(ArtifactError::Corrupt(
            "Partial compact history index entry".into(),
        ));
    }
    Ok((
        width,
        entries
            .map(|entry| decode_entry(entry, width))
            .collect::<ArtifactResult<_>>()?,
    ))
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

fn decode_entry(mut input: &[u8], width: KeyWidth) -> ArtifactResult<IndexedEntry> {
    let resource = ResourceIdentity::from_bytes(take(&mut input))
        .map_err(|_| ArtifactError::Corrupt("Invalid history index resource".into()))?;
    let version = width.decode(&mut input)?;
    let from = decode_instant(&mut input)?;
    let to = decode_instant(&mut input)?;
    if from >= to {
        return Err(ArtifactError::Corrupt(
            "Invalid history index interval".into(),
        ));
    }
    Ok(IndexedEntry {
        resource,
        version,
        from,
        to,
        offset: u32::from_le_bytes(take(&mut input)),
        length: u32::from_le_bytes(take(&mut input)),
    })
}
