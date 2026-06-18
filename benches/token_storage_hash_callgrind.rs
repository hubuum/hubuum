use hubuum::models::Token;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

// Token storage hashing (HMAC-SHA256 + hex encoding) runs on every
// authenticated request to look up the presented bearer token. The HMAC key is
// resolved once from config (an ephemeral key is generated when unset), so this
// benchmark is self-contained and deterministic in instruction count.
const RAW_TOKEN: &str = "hubuum_pat_0123456789abcdef0123456789abcdef0123456789abcdef";

#[library_benchmark]
fn bench_token_storage_hash() -> usize {
    let digest = Token::storage_hash_from_raw(black_box(RAW_TOKEN));

    black_box(digest.len())
}

library_benchmark_group!(name = benches; benchmarks = bench_token_storage_hash);
main!(library_benchmark_groups = benches);
