# Release optimization profiles

The release profile uses one codegen unit and fat LTO. This experiment varies
only `opt-level`, comparing the existing `z` size optimization with level `3`.
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
