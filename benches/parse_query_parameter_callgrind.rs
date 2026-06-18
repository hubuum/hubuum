use hubuum::models::search::parse_query_parameter;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

const COMPLEX_QUERY: &str = concat!(
    "name__not_icontains=archived",
    "&description__icontains=router",
    "&permissions=CanRead",
    "&namespaces=1-3,8",
    "&validate_schema=true",
    "&json_data__contains=metadata,owner=platform",
    "&json_data__gte=metrics,cpu=42",
    "&created_at__gte=2024-01-01",
    "&updated_at__lte=2024-12-31",
    "&sort=-created_at,name.asc",
    "&limit=50",
);

#[library_benchmark]
fn bench_parse_query_parameter() -> usize {
    let options =
        parse_query_parameter(black_box(COMPLEX_QUERY)).expect("benchmark query should parse");

    black_box(options.filters.len() + options.sort.len() + options.limit.unwrap_or_default())
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_query_parameter);
main!(library_benchmark_groups = benches);
