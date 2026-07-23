use hubuum::models::parse_unified_search_query_with_limits;
use hubuum::pagination::PageLimits;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

// Unified-search query parsing: percent-decoding, kind-set parsing, boolean and
// limit handling across every supported parameter. The config-free entry point
// takes the page limits explicitly so the parse path holds no global state.
const QUERY: &str = concat!(
    "q=asset%20server",
    "&kinds=collection,class,object",
    "&limit_per_kind=25",
    "&search_class_schema=true",
    "&search_object_data=false",
);

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 1000;

#[library_benchmark]
fn bench_parse_unified_search_query() -> usize {
    let page_limits = PageLimits::new(DEFAULT_LIMIT, MAX_LIMIT).unwrap();
    let parsed = parse_unified_search_query_with_limits(black_box(QUERY), page_limits)
        .expect("benchmark unified query should parse");

    black_box(parsed.kinds.len() + parsed.limit_per_kind)
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_unified_search_query);
main!(library_benchmark_groups = benches);
