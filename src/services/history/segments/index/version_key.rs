use super::{ArtifactError, ArtifactResult, Entry, VersionIdentity};

/// A candidate locator inside one immutable index. Only a decoded record can
/// supply the complete version identity for a shortened key.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::services::history::segments) struct VersionKey([u8; 16]);

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
pub(in crate::services::history::segments) enum KeyWidth {
    One = 1,
    Two = 2,
    Four = 4,
    Eight = 8,
    Full = 16,
}

impl KeyWidth {
    pub(super) fn from_byte(byte: u8) -> ArtifactResult<Self> {
        match byte {
            1 => Ok(Self::One),
            2 => Ok(Self::Two),
            4 => Ok(Self::Four),
            8 => Ok(Self::Eight),
            16 => Ok(Self::Full),
            _ => Err(ArtifactError::Corrupt(
                "Invalid history index key width".into(),
            )),
        }
    }

    pub(super) fn for_entries(entries: &[Entry]) -> Self {
        let mut ids: Vec<_> = entries
            .iter()
            .map(|entry| entry.interval.version.as_bytes())
            .collect();
        ids.sort_unstable();
        for width in [Self::One, Self::Two, Self::Four, Self::Eight] {
            let len = width as usize;
            if ids.windows(2).all(|ids| ids[0][..len] != ids[1][..len]) {
                return width;
            }
        }
        Self::Full
    }

    pub(in crate::services::history::segments) fn key(self, id: VersionIdentity) -> VersionKey {
        let mut bytes = *id.as_bytes();
        bytes[self as usize..].fill(0);
        VersionKey(bytes)
    }

    pub(super) fn decode(self, input: &mut &[u8]) -> ArtifactResult<VersionKey> {
        // The caller checked the full entry size for this width.
        let (prefix, rest) = input.split_at(self as usize);
        let mut bytes = [0; 16];
        bytes[..prefix.len()].copy_from_slice(prefix);
        if self == Self::Full {
            VersionIdentity::from_bytes(bytes)
                .map_err(|_| ArtifactError::Corrupt("Invalid history index version".into()))?;
        }
        *input = rest;
        Ok(VersionKey(bytes))
    }
}
