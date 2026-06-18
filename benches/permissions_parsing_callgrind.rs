use hubuum::models::Permissions;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

// Permission strings span the full match arm and exercise both early- and
// late-matching variants. This conversion runs while parsing the `permissions`
// filter on search requests.
const PERMISSIONS: [&str; 8] = [
    "ReadCollection",
    "DelegateCollection",
    "CreateClass",
    "DeleteObject",
    "UpdateClassRelation",
    "ReadObjectRelation",
    "CreateTemplate",
    "DeleteTemplate",
];

#[library_benchmark]
fn bench_parse_permissions() -> usize {
    let mut total = 0;

    for permission in black_box(PERMISSIONS) {
        let parsed =
            Permissions::from_string(permission).expect("benchmark permission should parse");
        total += black_box(parsed) as usize;
    }

    black_box(total)
}

library_benchmark_group!(name = benches; benchmarks = bench_parse_permissions);
main!(library_benchmark_groups = benches);
