use hubuum::models::search::{Operator, get_jsonb_field_type_from_value_and_operator};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

// Each (value, operator) pair drives the speculative type inference used when
// building JSONB filter SQL: dates and numbers are parse-probed, ranges are
// split and re-probed, and string operators short-circuit. This runs for every
// JSON field filter in a search query.
const CASES: [(&str, Operator); 7] = [
    ("2024-01-15T10:30:00Z", Operator::Equals),
    ("true", Operator::Equals),
    ("42", Operator::Gte),
    ("3.14159", Operator::Lt),
    ("2024-01-01,2024-12-31", Operator::Between),
    ("platform", Operator::IContains),
    ("router", Operator::Contains),
];

#[library_benchmark]
fn bench_jsonb_type_inference() -> usize {
    let mut total = 0;

    for (value, operator) in black_box(CASES) {
        if get_jsonb_field_type_from_value_and_operator(value, operator).is_some() {
            total += 1;
        }
    }

    black_box(total)
}

library_benchmark_group!(name = benches; benchmarks = bench_jsonb_type_inference);
main!(library_benchmark_groups = benches);
