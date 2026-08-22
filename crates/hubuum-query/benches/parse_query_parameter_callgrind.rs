use gungraun::{library_benchmark, library_benchmark_group, main};
use hubuum_query::{
    FilterField, MAX_QUERY_FILTERS, ParsedQueryParam, QueryFilters, SearchOperator,
    parse_query_parameter,
};
use std::hint::black_box;

const COMPLEX_QUERY: &str = concat!(
    "name__not_icontains=archived",
    "&description__icontains=router",
    "&permissions=CanRead",
    "&collections=1-3,8",
    "&validate_schema=true",
    "&json_data__contains=metadata,owner=platform",
    "&json_data__gte=metrics,cpu=42",
    "&created_at__gte=2024-01-01",
    "&updated_at__lte=2024-12-31",
    "&sort=-created_at,name.asc",
    "&include_total=false",
);

#[library_benchmark]
fn bench_parse_query_parameter() -> usize {
    let options =
        parse_query_parameter(black_box(COMPLEX_QUERY)).expect("benchmark query should parse");

    black_box(options.filters().len() + options.sort().len())
}

fn filter() -> ParsedQueryParam {
    ParsedQueryParam::from_parts(
        FilterField::Name,
        SearchOperator::Contains { is_negated: false },
        "benchmark-value",
    )
}

#[library_benchmark]
fn bench_bounded_filter_push() -> usize {
    let mut filters = QueryFilters::default();
    for _ in 0..MAX_QUERY_FILTERS {
        filters
            .try_push(black_box(filter()))
            .expect("benchmark filters stay within the public bound");
    }
    black_box(filters.len())
}

#[library_benchmark]
fn bench_vec_filter_push_control() -> usize {
    let mut filters = Vec::new();
    for _ in 0..MAX_QUERY_FILTERS {
        filters.push(black_box(filter()));
    }
    black_box(filters.len())
}

fn bounded_filters() -> QueryFilters {
    QueryFilters::new((0..MAX_QUERY_FILTERS).map(|_| filter()).collect::<Vec<_>>())
        .expect("benchmark filters stay within the public bound")
}

fn vec_filters() -> Vec<ParsedQueryParam> {
    (0..MAX_QUERY_FILTERS).map(|_| filter()).collect()
}

#[library_benchmark(setup = bounded_filters)]
fn bench_bounded_filter_retain(mut filters: QueryFilters) -> usize {
    let mut index = 0;
    filters
        .try_retain(|_| {
            index += 1;
            black_box(index % 2 == 0)
        })
        .expect("retaining ordinary filters preserves query invariants");
    black_box(filters.len())
}

#[library_benchmark(setup = vec_filters)]
fn bench_vec_filter_retain_control(mut filters: Vec<ParsedQueryParam>) -> usize {
    let mut index = 0;
    filters.retain(|_| {
        index += 1;
        black_box(index % 2 == 0)
    });
    black_box(filters.len())
}

library_benchmark_group!(
    name = benches;
    benchmarks =
        bench_parse_query_parameter,
        bench_bounded_filter_push,
        bench_vec_filter_push_control,
        bench_bounded_filter_retain,
        bench_vec_filter_retain_control
);
main!(library_benchmark_groups = benches);
