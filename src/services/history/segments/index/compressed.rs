// One independent, dictionary-free Zstandard frame contains the index byte columns. Single-segment framing makes its decoded size
// the upper bound on the decoder's window as well.
use super::{ArtifactError, ArtifactResult, MAX_RECORDS};
use zstd::bulk;
use zstd::zstd_safe::{find_frame_compressed_size, get_frame_content_size};

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];
const SINGLE_SEGMENT: u8 = 1 << 5;
const DICTIONARY_ID: u8 = 0b11;

pub(super) fn encode(entries: &[u8]) -> ArtifactResult<Vec<u8>> {
    let failed = |error| ArtifactError::Backend(format!("Encoding history index: {error}"));
    let mut encoder = bulk::Compressor::new(3).map_err(failed)?;
    // Every input fits one window. The bulk encoder knows the exact source size.
    encoder.window_log(20).map_err(failed)?;
    encoder.compress(entries).map_err(failed)
}

pub(super) fn decode(frame: &[u8], entry_bytes: usize) -> ArtifactResult<Vec<u8>> {
    let invalid = || ArtifactError::Corrupt("Invalid compressed history index frame".into());
    let Some((&flags, _)) = frame
        .strip_prefix(&ZSTD_MAGIC)
        .and_then(|header| header.split_first())
    else {
        return Err(invalid());
    };
    if flags & (SINGLE_SEGMENT | DICTIONARY_ID) != SINGLE_SEGMENT
        || find_frame_compressed_size(frame).ok() != Some(frame.len())
    {
        return Err(invalid());
    }
    let declared = get_frame_content_size(frame)
        .ok()
        .flatten()
        .ok_or_else(invalid)?;
    if declared > (MAX_RECORDS * entry_bytes) as u64 {
        return Err(ArtifactError::Limit(
            "Decoded history index exceeds record budget".into(),
        ));
    }
    if !declared.is_multiple_of(entry_bytes as u64) {
        return Err(invalid());
    }
    let bytes = bulk::decompress(frame, declared as usize).map_err(|_| invalid())?;
    if bytes.len() as u64 != declared {
        return Err(invalid());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::super::ENTRY_BYTES;
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::empty(0)]
    #[case::one(1)]
    #[case::largest(MAX_RECORDS)]
    fn index_frame_roundtrip(#[case] entries: usize) {
        let bytes: Vec<_> = (0..entries * ENTRY_BYTES)
            .map(|i| (i % 251) as u8)
            .collect();
        assert_eq!(
            decode(&encode(&bytes).unwrap(), ENTRY_BYTES).unwrap(),
            bytes
        );
    }

    #[rstest]
    #[case::truncated(0)]
    #[case::trailing_byte(1)]
    #[case::second_frame(2)]
    #[case::dictionary_flag(3)]
    #[case::unbounded_window(4)]
    #[case::skippable_frame(5)]
    fn only_one_sized_dictionary_free_frame_is_valid(#[case] variant: u8) {
        let mut frame = encode(&[0; ENTRY_BYTES]).unwrap();
        match variant {
            0 => {
                frame.pop();
            }
            1 => frame.push(0),
            2 => frame.extend_from_within(..),
            3 => frame[4] |= 1,
            4 => frame[4] &= !SINGLE_SEGMENT,
            _ => frame = vec![0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0],
        }
        assert!(matches!(
            decode(&frame, ENTRY_BYTES),
            Err(ArtifactError::Corrupt(_))
        ));
    }

    #[test]
    fn decoded_size_is_bounded_before_allocating_entries() {
        let frame = bulk::compress(&vec![0; (MAX_RECORDS + 1) * ENTRY_BYTES], 3).unwrap();
        assert!(matches!(
            decode(&frame, ENTRY_BYTES),
            Err(ArtifactError::Limit(_))
        ));
    }

    #[test]
    fn decoded_size_must_cover_whole_entries() {
        let frame = bulk::compress(&[0; ENTRY_BYTES - 1], 3).unwrap();
        assert!(matches!(
            decode(&frame, ENTRY_BYTES),
            Err(ArtifactError::Corrupt(_))
        ));
    }
}
