use super::*;
use rstest::rstest;

#[rstest]
#[case::magic(0)]
#[case::resource(5)]
#[case::generation(28)]
#[case::count(32)]
#[case::frame_length(36)]
#[case::frame(37)]
fn corrupt_catalog_fields_fail_validation(#[case] offset: usize) {
    let catalog = HistoryArchiveCatalog::empty(ResourceIdentity::generate());
    let mut bytes = encode(&catalog).unwrap();
    bytes[offset] ^= 1;
    assert!(decode(&bytes).is_err());
}

#[rstest]
#[case::incomplete_commitment(1)]
#[case::extra_complete_commitment(COMMITMENT_BYTES)]
fn unclaimed_commitments_fail_validation(#[case] extra: usize) {
    let catalog = HistoryArchiveCatalog::empty(ResourceIdentity::generate());
    let mut bytes = encode(&catalog).unwrap();
    bytes.resize(bytes.len() + extra, 1);
    assert!(decode(&bytes).is_err());
}

#[rstest]
#[case::dictionary(0x01)]
#[case::unbounded_window(0x20)]
fn only_a_single_dictionary_free_segment_can_supply_catalog_metadata(#[case] toggle: u8) {
    let mut bytes = encode(&HistoryArchiveCatalog::empty(ResourceIdentity::generate())).unwrap();
    bytes[PREFIX_BYTES + ZSTD_MAGIC.len()] ^= toggle;
    assert!(decode(&bytes).is_err());
}
