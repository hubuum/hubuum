use hubuum_templates::SizeLimitedWriter;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::io::Write;

#[library_benchmark]
fn bench_size_limited_writer() -> usize {
    let mut writer = SizeLimitedWriter::new(black_box(8 * 1024));
    for _ in 0..128 {
        writer
            .write_all(black_box(b"template output line\n"))
            .expect("benchmark output should fit");
    }

    black_box(
        writer
            .into_string()
            .expect("benchmark output is UTF-8")
            .len(),
    )
}

library_benchmark_group!(name = benches; benchmarks = bench_size_limited_writer);
main!(library_benchmark_groups = benches);
