# Release optimization profiles

Release builds now use `opt-level = 3`, one codegen unit, and fat LTO. The
measured tradeoff below supports that choice. This experiment varies
only `opt-level`, comparing the previous `z` size optimization with level `3`.
It does not change compiler, features, corpus, allocator, TLS selection, LTO or
codegen units between variants.

## Reproduction

Use Python 3.11+ with its standard library, GNU time, `strip`, Cargo, and Docker
or Podman on Linux. Run on an idle machine with enough space for two isolated
Cargo targets and container layers:

```bash
python3 scripts/compare-release-profiles.py \
  --output target/release-profile-comparison --jobs 6
```

`--container-engine podman` selects Podman directly. `--skip-containers` is for
iteration; it cannot provide the complete artifact comparison. The tool fails
on build, benchmark or image errors and never reports missing workloads as a
successful comparison. Reusing an output directory is allowed, but the report
marks existing target caches so warm builds are not described as cold builds.

Each profile builds the server, administrator and template worker with default
features plus `tls-rustls,tls-openssl,embedded-migrations`, then builds and runs
four deterministic library benchmark targets. Each file has its own Cargo
benchmark entry so ordinary CI can measure it independently:

| Target | Workload | Measured scope |
| --- | --- | --- |
| `release_serialization_criterion` | 1,024 JSON objects with escaped strings, addresses and integer arrays | Compact encoding and decoding; bytes per second |
| `release_schema_criterion` | One compiled object schema, 256 bounded integer values | Valid instance inspection; values per second |
| `release_search_criterion` | Unified search and a filtered/sorted object query | Request parsing and normalization without global configuration or database I/O |
| `release_template_criterion` | 128 object rows rendered through curated helpers and a bounded writer | Warm MiniJinja rendering; rows per second, excluding subprocess startup |

The template benchmark uses the same interpreter, conversion helpers and output
writer as production with explicit fuel, recursion and output limits. The
production worker's isolation boundary is not part of the timed library case.
The search cases measure CPU-side query preparation, not database search
throughput. Neither case alone predicts end-to-end server request latency.

## Evidence and interpretation

`report.json` records the compiler and Cargo versions, CPU description, platform,
feature set, source revision and dirty state, hashes of every workload source,
parallel build count, cache state, mean/median latency and confidence intervals,
throughput where defined, and build/benchmark process resource measurements.
GNU time's maximum child RSS is the maximum individual child measurement; it is
not simultaneous process-tree memory. Build timing depends on filesystem cache,
competing work and parallelism and must be interpreted with those conditions.
For container builds, GNU time observes the engine client and its descendants;
those RSS/CPU values do not measure a separate Docker daemon or every process
inside the build container. Host Cargo-build and benchmark-process resources are
reported separately.

Executable sizes include stripped and unstripped measurements. Container sizes
come from image inspection and are uncompressed image bytes, not registry
download sizes. Container builds use the real production Dockerfile and the same
TLS feature combination, passing the optimization override through Cargo's
configuration argument. Both final images therefore exercise the restricted
manifest-only dependency stage and contain all three published executables.

Review per-workload changes and artifact costs together; do not infer that size
optimization is wrong merely because its name suggests a throughput tradeoff.
Performance conclusions should be based on a repeat measurement after builds
finish, with the machine otherwise idle. Keep raw reports beside the recorded
decision so the choice can be re-evaluated after compiler or dependency changes.

## Decision

Use `opt-level = 3` for release builds, retaining one codegen unit and fat LTO.
The measured CPU workloads consistently benefit, with the largest improvement
in JSON encoding. The additional artifact size and build resources are accepted
for that execution-speed benefit. This does not establish an HTTP throughput
percentage: database I/O, authorization, and worker startup are outside these
library cases. No configuration or database migration is required.

The host measurements used Rust 1.98.0 / LLVM 22.1.8 on an Intel Xeon Silver 4216
(16 cores, 32 logical CPUs), approximately 62 GiB RAM, Linux x86-64, and six Cargo
jobs. Both variants used default features plus `tls-rustls,tls-openssl,embedded-migrations`.
The production Docker image used its pinned Alpine toolchain, which reports Rust
1.99.1; container variants share that compiler, while host variants share 1.98.0.
Do not compare host and container bytes as if their target/toolchain were equal.

### Runtime repeats

The [complete machine-readable report](release_profiles.json) retains both quiet
repeats, confidence intervals, medians, throughput, process resources, source and
executable hashes, and the earlier contended run. The table uses the second quiet
repeat's mean. Both repeats improved every workload; the profile order was
`z`, `3` first and `3`, `z` second. They ran sequentially on CPU 15 after the
experiment's builds and test suites finished; existing host services remained
running. Each case used 50 samples, a two-second warm-up, and five seconds of
measurement.

| Workload | `z` mean, µs | `3` mean, µs | Mean latency change |
| --- | --- | --- | --- |
| JSON encode, 1,024 objects | 1518.098 | 524.964 | -65.4% |
| JSON decode, 1,024 objects | 3782.039 | 2896.218 | -23.4% |
| Schema validation, 256 values | 27.710 | 19.801 | -28.5% |
| Unified search parsing | 1.242 | 0.923 | -25.6% |
| Filter parsing | 1.377 | 0.788 | -42.8% |
| Warm template, 128 rows | 69.724 | 54.929 | -21.2% |

These repeats execute the exact Cargo-reported benchmark binaries, avoiding
recompilation between measurements. To repeat this step after the driver has
built them, select each target's `executable` from its `*-build.log` JSON and
set `profile_target` to that profile's Cargo target directory. Then run:

```bash
CARGO_TARGET_DIR="$profile_target" taskset -c 15 "$benchmark_executable" \
  --bench --sample-size 50 --warm-up-time 2 --measurement-time 5 --noplot
```

Use the same available CPU for both profiles on another machine. Reversing the
profile order checks for order effects; neither the absolute timings nor these
ratios are universal guarantees.

### Artifact and build cost

| Artifact | `z` bytes | `3` bytes | Change |
| --- | --- | --- | --- |
| Stripped host `hubuum-server` | 45,742,200 | 69,140,824 | +51.2% |
| Stripped host `hubuum-admin` | 12,595,040 | 20,614,080 | +63.7% |
| Stripped host `hubuum-template-worker` | 1,249,400 | 1,824,024 | +46.0% |
| Production image, uncompressed | 87,260,858 | 119,140,027 | +36.5% |

Cold Cargo target directories produced the following build observations. Other
builds and database tests were active during these runs, so wall times are not
an isolated estimate of the optimization level's build-time penalty. The raw
report also retains user/system CPU, benchmark compilation, process RSS,
unstripped sizes, image identifiers, and container-client build measurements.

| Host production build | `z` | `3` |
| --- | --- | --- |
| Wall seconds | 998.17 | 2247.71 |
| User + system CPU seconds | 1699.38 | 3109.34 |
| Maximum individual-child RSS bytes | 4,665,016,320 | 6,279,503,872 |

Size-focused downstream builds can explicitly select the previous profile with
`CARGO_PROFILE_RELEASE_OPT_LEVEL=z`; this is a build choice, not a runtime setting.
Re-evaluate the tradeoff after substantial compiler, dependency, or corpus changes.
