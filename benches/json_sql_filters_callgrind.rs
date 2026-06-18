use hubuum::models::search::{ParsedQueryParam, SearchOperator};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
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
fn bench_build_json_sql_filters() -> usize {
    let mut total = 0;

    for param in black_box(json_filter_fixtures()) {
        let component = param
            .as_json_sql()
            .expect("benchmark JSON filter should build SQL");
        total += component.sql.len() + component.bind_variables.len();
    }

    black_box(total)
}

library_benchmark_group!(name = benches; benchmarks = bench_build_json_sql_filters);
main!(library_benchmark_groups = benches);
