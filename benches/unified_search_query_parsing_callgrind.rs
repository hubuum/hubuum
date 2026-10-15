use gungraun::{library_benchmark, library_benchmark_group, main};
use hubuum::models::parse_unified_search_query_with_limits;
use hubuum::pagination::PageLimits;
use std::hint::black_box;
use std::sync::LazyLock;

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

// String scanning depends on input alignment. Cover each offset explicitly so
// unrelated changes to the binary's string layout cannot change the workload.
#[repr(align(16))]
struct AlignedQuery([u8; QUERY.len() + 15]);

fn query_at_alignment(offset: usize) -> &'static str {
    static QUERIES: LazyLock<[AlignedQuery; 16]> = LazyLock::new(|| {
        std::array::from_fn(|offset| {
            let mut query = AlignedQuery([0; QUERY.len() + 15]);
            query.0[offset..offset + QUERY.len()].copy_from_slice(QUERY.as_bytes());
            query
        })
    });
    std::str::from_utf8(&QUERIES[offset].0[offset..offset + QUERY.len()]).unwrap()
}

#[library_benchmark(setup = query_at_alignment)]
#[bench::alignment_0(0)]
#[bench::alignment_1(1)]
#[bench::alignment_2(2)]
#[bench::alignment_3(3)]
#[bench::alignment_4(4)]
#[bench::alignment_5(5)]
#[bench::alignment_6(6)]
#[bench::alignment_7(7)]
#[bench::alignment_8(8)]
#[bench::alignment_9(9)]
#[bench::alignment_10(10)]
#[bench::alignment_11(11)]
#[bench::alignment_12(12)]
#[bench::alignment_13(13)]
#[bench::alignment_14(14)]
#[bench::alignment_15(15)]
fn bench_parse_unified_search_query(query: &str) -> usize {
    let page_limits = PageLimits::new(DEFAULT_LIMIT, MAX_LIMIT).unwrap();
    let parsed = parse_unified_search_query_with_limits(black_box(query), page_limits)
        .expect("benchmark unified query should parse");

    black_box(parsed.kinds.len() + parsed.limit_per_kind)
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_unified_search_query);
main!(library_benchmark_groups = benches);
