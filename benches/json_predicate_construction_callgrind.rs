use gungraun::{library_benchmark, library_benchmark_group, main};
use hubuum::models::search::{ParsedQueryParam, SearchOperator};
use hubuum_storage_postgres::compile_json_filter_for_benchmark;
use std::hint::black_box;

fn json_filter_fixtures() -> [ParsedQueryParam; 4] {
    [
        ParsedQueryParam::new(
            "json_data",
            Some(SearchOperator::Contains { is_negated: false }),
            "metadata,owner=platform",
        )
        .expect("valid text filter"),
        ParsedQueryParam::new(
            "json_data",
            Some(SearchOperator::Gte { is_negated: false }),
            "metrics,cpu=42",
        )
        .expect("valid numeric filter"),
        ParsedQueryParam::new(
            "json_data",
            Some(SearchOperator::Gt { is_negated: false }),
            "metadata,created_at=2024-01-01",
        )
        .expect("valid date filter"),
        ParsedQueryParam::new(
            "json_schema",
            Some(SearchOperator::IEquals { is_negated: true }),
            "properties,title,type=string",
        )
        .expect("valid schema filter"),
    ]
}

#[library_benchmark]
fn bench_build_json_predicates() {
    for param in black_box(json_filter_fixtures()) {
        black_box(
            compile_json_filter_for_benchmark(&param)
                .expect("benchmark JSON filter should build a predicate"),
        );
    }
}

library_benchmark_group!(name = benches; benchmarks = bench_build_json_predicates);
main!(library_benchmark_groups = benches);
