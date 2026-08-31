use gungraun::{library_benchmark, library_benchmark_group, main};
use hubuum::models::Token;
use std::hint::black_box;

// Token storage hashing (HMAC-SHA256 + hex encoding) runs on every
// authenticated request to look up the presented bearer token. The HMAC key is
// resolved once from config (an ephemeral key is generated when unset), so this
// benchmark is self-contained and deterministic in instruction count.
const RAW_TOKEN: &str = "hubuum_pat_0123456789abcdef0123456789abcdef0123456789abcdef";

fn setup() -> &'static str {
    // Application startup resolves the process-wide key before bearer-token
    // authentication begins. Keep that one-time provider/cache initialization
    // outside the measured per-request hashing path.
    let _ = Token::storage_hash_from_raw("");
    RAW_TOKEN
}

#[library_benchmark(setup = setup)]
fn bench_token_storage_hash(raw_token: &str) -> usize {
    let digest = Token::storage_hash_from_raw(black_box(raw_token));

    black_box(digest.len())
}

library_benchmark_group!(name = benches; benchmarks = bench_token_storage_hash);
main!(library_benchmark_groups = benches);
