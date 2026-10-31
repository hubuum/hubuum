# Benchmark CI controls

Correctness validation, performance scope, target tags, and measurement effort are
independent controls. Ordinary ready PRs run affected performance work with the
`standard` profile. Drafts retain their lightweight correctness tier and skip
automatic performance work unless a performance selector is present.

## Validation and compatibility labels

| Label | Correctness validation | Performance work |
| --- | --- | --- |
| `ci:validate` | Full production validation, including feature, platform, contract, and container checks | Normal affected-path policy; no forced benchmarks |
| `ci:benchmarks` | Normal correctness policy | All four scopes, at least standard effort |
| `ci:full` | Full production validation | All four scopes, at least standard effort |

Production container checks and the required CI gate remain independent of optional
performance selection. Existing `ci:scale-large` and `ci:scale-huge` controls and
scale schedules remain separate; generic force-all labels do not enable scale runs.

## Scopes tags profiles and targets

| Concept | Meaning | Values or examples | GitHub label |
| --- | --- | --- | --- |
| Scope | Whole performance suite | `micro`, `postgres`, `runtime`, `resources` | `ci:bench:scope:postgres` |
| Tag | Subject covered by benchmark targets | `query`, `auth`, `schema`, `templates`, `storage`, `computed`, `budgets` | `ci:bench:tag:query` |
| Profile | Measurement effort within the selected work | `quick`, `standard`, `thorough` | `ci:bench:profile:thorough` |
| Target | One benchmark executable and backend | `criterion:Cargo.toml:password_hashing_criterion` | Manual input only |

The `micro` scope contains non-PostgreSQL Criterion and Gungraun benchmarks.
`postgres` contains benchmarks requiring `postgres-bench`, including moved targets
whose base revision required that feature. `runtime` measures server behavior;
`resources` measures template/schema resource use. Tags can span scopes: `schema`
selects matching microbenchmarks and the resource suite.

Scope names and tag names are distinct. `standard` names a measurement profile,
never a scope. The historical `hubuum-benchmarks-standard` cache namespace is
retained internally for cache compatibility with the renamed micro scope.

Explicit scopes, tags, and targets replace automatic performance scope and are
unioned. For example, `ci:bench:tag:query` plus `ci:bench:profile:thorough` requests
query targets at thorough effort; it does not request PostgreSQL or resource work.
Adding `ci:bench:scope:postgres` also requests the complete PostgreSQL suite.

`ci:full` and `ci:benchmarks` override narrowing and select every scope. The
strongest profile wins, and those labels enforce at least `standard`. A profile
label alone requests affected work, including on a draft. Unknown selectors fail
with an actionable error. Removing a selector or changing draft status recalculates
selection; unrelated labels do not cancel active CI or benchmark jobs.

The workflow can also be dispatched with `scopes` (`auto`, `all`, or a comma-separated
list), `tags`, `targets`, `profile`, and `base_sha`. Manual dispatch preserves the
existing cache-warming default on `main`: `warm_only=true` compiles the complete
catalog without measuring it. Set `warm_only=false` to compare the requested
scopes/tags/targets; comparisons default to `main` as their base.

## Measurement profiles

| Profile | Criterion samples | Warmup | Measurement target | Bootstrap resamples | Result policy |
| --- | ---: | ---: | ---: | ---: | --- |
| `quick` | 10 | 0.5 seconds | 1 second | 1,000 | Advisory regression feedback; command failures still fail |
| `standard` | 20 | 1 second | 2 seconds | 10,000 | Existing regression checks |
| `thorough` | 100 | 3 seconds | 10 seconds | 100,000 | Longer performance investigation |

Quick profiles select representative cases for the larger schema, computed, and
PostgreSQL targets. Criterion measurement times are targets, not hard deadlines.
Gungraun retains its native instruction measurement unless a target explicitly
configures backend arguments. Runtime/resource suites retain their correctness
assertions and minimum measurement settings for every profile.

Standalone Rust defaults remain useful for local runs, while CLI settings override
them. When an older base hard-codes per-group budgets, the planner applies the same
literal budgets to both revisions and records that fallback. Such a comparison may
therefore take longer than the requested profile. Benchmark history distinguishes
profiles; runtime-only profile changes reuse the same executable identities.

The default stays `standard` until cold/warm evidence and regression sensitivity
justify changing routine feedback to `quick`. Required checks are not replaced by
uncalibrated advisory results.

## Selection and caches

[The repository policy](https://github.com/hubuum/hubuum/blob/main/.github/benchmark-policy.json) owns tag assignments,
profiles, and changed-path rules. Discovery remains the benchmark inventory.
Known query changes select query targets; shared and unknown inputs expand
conservatively. New benchmark inputs, moved paths, shared fixtures, manifests,
features, toolchains, and dynamically embedded inputs must keep classifier coverage.

Both revisions use comparable cases and effective settings. Selection happens
before compilation. Full per-scope catalogs remain available for exact, verified
subset reuse from warmed executable bundles; missing or incompatible bundles fall
back to compiling the selected targets. Measurements are always fresh.

PRs restore large build caches without saving duplicates. Main warms shared builds
and executables. The same restore-only PR policy applies to correctness/fuzz/scale
Rust caches and the production-container cache exporter so they do not displace
shared benchmark entries. Builds and same-run artifact sharing remain enabled.
The cache-maintenance workflow inventories entries before deleting
at most 20 obsolete entries per run. It manages only the two benchmark namespaces,
keeps recent main entries, and bounds retained PR entries. Manual maintenance
defaults to a dry run; the daily schedule applies the bounded policy. Inventories
retain cache keys, sizes, access times, candidates, and deletion results.

## Final performance evidence

The selection job exposes scopes, tags, profile, and reasons even when no performance
work is selected. Per-target evidence also records skipped targets and effective
arguments. The final publisher runs after the benchmark workflow completes and
combines micro, PostgreSQL, runtime, and resource status, including failures,
cancellations, and unavailable artifacts. Older runs cannot overwrite newer work.

Runner time sums job durations. Elapsed span runs from the first job start to the
last completion and includes gaps and dependency waits. Creation-to-completion
latency is reported separately. Artifact transfers and compilation/execution phases
are components of runner time, not additional totals. Queue time is unavailable
when timestamps cannot isolate it. Publisher and scale-workflow time remain
separate. Shared-action lane reports are explicitly provisional.

Validate savings with fixed revisions, toolchain, runner image, features, and target
scope. Record cold and warm runs, cache inventory, compilation, executable hits,
fresh/rebuilt artifacts, execution, transfers, elapsed time, and runner time. A
smaller selected scope and an actual warm executable hit are separate sources of
savings and should be reported separately.
