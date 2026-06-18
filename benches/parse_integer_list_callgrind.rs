use hubuum::utilities::extensions::parse_integer_list;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

#[library_benchmark]
fn bench_parse_integer_list() -> usize {
    let numbers = parse_integer_list(black_box("1-250,260-512,42,42,-8--1"))
        .expect("benchmark integer list should parse");

    black_box(numbers.len())
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_integer_list);
main!(library_benchmark_groups = benches);
