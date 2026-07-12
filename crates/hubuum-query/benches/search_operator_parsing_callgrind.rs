use hubuum_query::SearchOperator;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

const OPERATORS: [&str; 12] = [
    "equals",
    "not_equals",
    "icontains",
    "not_icontains",
    "startswith",
    "iendswith",
    "gte",
    "lte",
    "between",
    "not_between",
    "regex",
    "like",
];

#[library_benchmark]
fn bench_parse_search_operators() -> usize {
    let mut total = 0;

    for operator in black_box(OPERATORS) {
        let parsed =
            SearchOperator::new_from_string(operator).expect("benchmark operator should parse");
        let (_, is_negated) = parsed.op_and_neg();
        total += is_negated as usize;
    }

    black_box(total)
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_search_operators);
main!(library_benchmark_groups = benches);
