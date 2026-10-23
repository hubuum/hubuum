use criterion::{Criterion, criterion_group, criterion_main};
use hubuum::models::parse_unified_search_query_with_limits;
use hubuum::pagination::PageLimits;
use hubuum_query::parse_query_parameter;
use std::hint::black_box;

fn search(c: &mut Criterion) {
    let limits = PageLimits::new(50, 1000).unwrap();
    let query = "q=asset%20server&kinds=collection,class,object&limit_per_kind=25&search_class_schema=true&search_object_data=false";
    assert!(parse_unified_search_query_with_limits(query, limits).is_ok());
    c.bench_function("release_search/unified_parse", |b| {
        b.iter(|| {
            black_box(parse_unified_search_query_with_limits(black_box(query), limits).unwrap())
        })
    });
    let filter = "name__contains=host&id__in=1,2,3,4,5&sort=name.asc";
    assert!(parse_query_parameter(filter).is_ok());
    c.bench_function("release_search/filter_parse", |b| {
        b.iter(|| black_box(parse_query_parameter(black_box(filter)).unwrap()))
    });
}
criterion_group!(benches, search);
criterion_main!(benches);
