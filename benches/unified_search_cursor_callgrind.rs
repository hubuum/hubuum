use hubuum::models::{decode_cursor, encode_cursor, UnifiedSearchCursorToken};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

// Pagination cursor round-trip: serde serialization + base64url encode, then
// base64url decode + serde deserialization. One encode and one decode run for
// every page of unified-search results.
fn cursor_token() -> UnifiedSearchCursorToken {
    UnifiedSearchCursorToken {
        rank: 2,
        name: "asset-001".to_string(),
        id: 4242,
    }
}

#[library_benchmark]
fn bench_unified_search_cursor_roundtrip() -> usize {
    let encoded = encode_cursor(black_box(&cursor_token())).expect("cursor should encode");
    let decoded = decode_cursor(black_box(&encoded)).expect("cursor should decode");

    black_box(encoded.len() + decoded.name.len())
}

library_benchmark_group!(name = benches; benchmarks = bench_unified_search_cursor_roundtrip);
main!(library_benchmark_groups = benches);
