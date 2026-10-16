# Portable regex testing and limits

`hubuum-regex` owns the accepted language and the shared matching corpus. Storage
adapters compile its private-fielded `Pattern`; they do not reinterpret the
original source. See the [query guide](querying.md) and
[crate policy](rust_api/hubuum-regex.md) for the language contract.

This page records the regex portions of [#259](https://github.com/hubuum/hubuum/issues/259)
and [#410](https://github.com/hubuum/hubuum/issues/410). The wider fuzzing,
resilience, dynamic-policy, and shared-budget work remains in those issues.

## Limits inventory

| Dimension | Policy and owner | Enforcement point | Meaning and termination |
| --- | --- | --- | --- |
| Source bytes | `hubuum-regex::MAX_PATTERN_BYTES`: 1,024 UTF-8 bytes | First check in `Pattern::new`, before AST parsing | Oversized source is rejected without parsing or including it in a parser diagnostic. |
| Syntax nesting | `MAX_NESTING_DEPTH`: 16 | The AST parser's depth check | Rejects excessive nesting; this is not a promise that no AST allocation occurs before rejection. |
| Explicit repetition | `MAX_REPETITION`: 255 | AST visitor, and again during normalization | Rejects an oversized bound even when a later simplification would remove the expression. |
| Expanded expression cost | `MAX_COMPLEXITY`: 4,096 | Recursive normalization, using saturating arithmetic and checking child and accumulated costs | Counts normalized structure and repetition expansion, not heap bytes, elapsed time, or rows. |
| Memory compiled NFA | The pinned `regex` engine's 10 MiB default size limit | `MemoryRegexCompiler` calls `Regex::new` | Limits compiled NFA size. An accepted portable pattern that cannot compile is an adapter defect. This limit is not a total RSS bound. |
| Memory matching | Native boolean matching, with worst-case `O(m * n)` search work | `MemoryRegex::is_match`; `m` is compiled pattern size, `n` is text length | Does not impose a regex-specific wall-clock deadline or preempt a synchronous row scan. More rows and longer text still require more work. |
| PostgreSQL statement time | Application `HUBUUM_DB_STATEMENT_TIMEOUT_MS`, default 30,000 ms | Pool connection initialization in `pool.rs` | PostgreSQL cancels an over-budget statement at its interrupt checkpoints. Zero skips the application-configured session timeout; server settings can still apply. |
| Scoped PostgreSQL time | `StorageQueryBudget` and remaining task execution time | `runtime.rs` applies transaction-local `statement_timeout` | Uses the smaller supplied query/task limit, with task time clamped to PostgreSQL's millisecond range. A scoped setting replaces the session value; it is not automatically the minimum of that value and the pool setting. |

The source, nesting, repetition, and complexity limits are fixed language
semantics. They have no disabled/unlimited setting and cannot vary by backend.
Unicode HIR translation happens before the normalized-cost rejection, so a
4,096-unit cost limit must not be presented as a 4,096-node allocation ceiling
for the entire constructor. Class ranges and literal scalars contribute cost;
sequence/alternation nodes add their child costs and one node. A repetition adds
one node to its child cost multiplied by its upper bound, or by `minimum + 1`
when unbounded, using at least one copy.

The memory engine also has a default 2 MiB lazy-DFA cache capacity. Neither that
capacity nor its NFA limit accounts for all parser, matcher, corpus, row, or
process allocations. These defaults are implementation details of the versions
in `Cargo.lock`, not new configuration supplied by `hubuum-regex`.

`StorageQueryBudget::from_millis(0)` yields no scoped budget. PostgreSQL's adapter
settings and native integration-test pools can also use zero; the application's
30-second default must not be assumed by an independent adapter consumer.
Carrying an execution scope through memory does not add interruption points
inside its regex matcher.

### Accounting decision

Keep the fixed acceptance checks in `hubuum-regex` and execution policy in the
adapters/executors. No generic budget crate is introduced by this work: the
language's structural cost and a database statement deadline are different units
with different enforcement mechanisms. A reusable writer or work counter should
be extracted only when actual consumers share its accounting and exhaustion
semantics. Dynamic policy selection, task-wide accounting, and cancellation
guarantees remain separate work in #410.

## Test layers

- The crate-owned `conformance` corpus specifies accepted patterns and expected
  boolean results. Both adapters execute it.
- `crates/hubuum-storage-postgres/tests/regex_pattern.rs` also performs 15,768
  deterministic native PostgreSQL/memory comparisons.
- `fuzz/tests/limits.rs` checks exact and immediately excessive byte, nesting,
  repetition, and normalized-cost boundaries through the public constructor.
- `regex_pattern` fuzzes UTF-8 source, deterministic construction, rejection of
  oversized input, and a conservative 64 KiB diagnostic ceiling.
- `regex_backends` compares the actual memory and PostgreSQL compilers and
  matchers for accepted patterns and non-NUL Unicode text. Native compilation
  errors and differing results fail the run.

The fuzz package is a separate Cargo workspace under `fuzz/`. It does not add
instrumentation or `libfuzzer-sys` to the application workspace, published SDK,
or production container. The seed generator reads the shared corpus directly
and adds near-limit inputs. There is no copied backend translator or SQL mock.

## Run locally

Use the pinned nightly and cargo-fuzz versions from the workflow. The native
target also needs a C++ compiler, PostgreSQL client development libraries, and
an isolated UTF-8 PostgreSQL server. Python tooling, when used, remains Python
3.11+ with the standard library only.

```bash
rustup toolchain install nightly-2026-08-29 --profile minimal --component clippy --component rustfmt
cargo install cargo-fuzz --version '=0.13.2' --locked
cargo +nightly-2026-08-29 fetch --manifest-path fuzz/Cargo.toml --locked
cargo +nightly-2026-08-29 test --manifest-path fuzz/Cargo.toml --test limits --locked
cargo +nightly-2026-08-29 run --manifest-path fuzz/Cargo.toml --example seed --locked

CARGO_NET_OFFLINE=true cargo +nightly-2026-08-29 fuzz run regex_pattern --fuzz-dir fuzz --codegen-units 16 -- \
  -max_total_time=45 -timeout=5 -rss_limit_mb=1536 -max_len=2048 -dict="$PWD/fuzz/regex.dict"
```

For native matching, provision the same pinned PostgreSQL image as CI. The
fixture needs no application tables or migrations:

```bash
docker run --rm --detach --name hubuum-regex-fuzz \
  --memory=1g --cpus=2 -p 127.0.0.1:55466:5432 \
  -e POSTGRES_DB=hubuum_regex_fuzz -e POSTGRES_PASSWORD=regex-fuzz \
  postgres:18.6@sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722
# Wait until the fixture is ready before running the target.
docker exec hubuum-regex-fuzz pg_isready -U postgres -d hubuum_regex_fuzz
export HUBUUM_REGEX_FUZZ_DATABASE_URL=postgres://postgres:regex-fuzz@127.0.0.1:55466/hubuum_regex_fuzz
CARGO_NET_OFFLINE=true cargo +nightly-2026-08-29 fuzz run regex_backends --fuzz-dir fuzz --features postgres --codegen-units 16 -- \
  -max_total_time=45 -timeout=5 -rss_limit_mb=1536 -max_len=2049 -dict="$PWD/fuzz/regex.dict"
docker stop hubuum-regex-fuzz
```

The target requires a loopback host and the dedicated `hubuum_regex_fuzz`
database. It never falls back to application database environment variables.
Its input format is UTF-8 pattern bytes, one NUL delimiter, then UTF-8 text;
text is limited to 1,024 bytes and cannot contain NUL. The source retains the
language's 1,024-byte limit.

The native session is read-only and has a one-second statement timeout. A
PostgreSQL timeout is permitted execution-policy exhaustion, not a boolean
disagreement: the harness checks that a subsequent query succeeds before
continuing. Other native failures fail the run. The fuzzer's five-second input
timeout and 1,536 MiB process RSS limit also cover compilation and client-side
work. The service's memory/CPU limits and the workflow's 60-minute timeout are
additional harness limits; none redefines the portable language.

Cargo-fuzz 0.13.2 does not expose `--locked`. Fetch with Cargo's `--locked`
first, fuzz offline, and check that `fuzz/Cargo.lock` has not changed. Keep the
regex parser/engine versions aligned with the application lockfile when updating
dependencies. Sixteen code-generation units and lower adapter optimization
reduce instrumented build cost; sanitizers, overflow checks, and the real
compiler implementations remain enabled.

## CI, replay, and minimization

The `Portable regex fuzzing` workflow runs both targets for 45 seconds each on
affected pull requests and `main` pushes, and for 600 seconds each daily and on
manual dispatch. The classifier includes the fuzz harness, dictionary, seed and
regression inputs, workspace dependency changes, and its own workflow. Unknown
paths remain conservative.

The standalone lockfile is audited with the root dependency/license policy via
`scripts/run-cargo-deny.sh REPORT fuzz/Cargo.toml`. NCSA covers the bundled
libFuzzer engine; the fuzz dependencies remain outside production artifacts.

Each run starts from the language corpus and committed regressions. Generated
corpora and failure inputs are uploaded for 30 days, together with the fuzz
lockfile. Use only synthetic inputs; do not add application data or credentials
to a seed or failure artifact. Downloaded corpora can be supplied as the corpus
argument to resume exploration.

Keep the native fixture and its dedicated environment variable active while
replaying or minimizing a backend failure:

```bash
# Replace the example filename with the artifact reported by cargo-fuzz.
cargo +nightly-2026-08-29 fuzz run regex_backends fuzz/artifacts/regex_backends/crash-example \
  --fuzz-dir fuzz --features postgres --codegen-units 16
cargo +nightly-2026-08-29 fuzz tmin regex_backends fuzz/artifacts/regex_backends/crash-example \
  --fuzz-dir fuzz --features postgres --codegen-units 16 -- -timeout=5 -rss_limit_mb=1536
cargo +nightly-2026-08-29 fuzz cmin regex_pattern --fuzz-dir fuzz --codegen-units 16
```

Commit a confirmed minimized input under `fuzz/regressions/regex_pattern/` or
`fuzz/regressions/regex_backends/`; the seed generator includes these files in
future runs. Record the revision, toolchain, PostgreSQL image, target, flags,
and reproduction command in the issue/PR. Promote portable matching failures
into the language-owned conformance corpus with an explicit expected result.
Keep engine-specific compilation, timeout, and connection-recovery regressions
in adapter tests. Fuzzing complements deterministic contracts and does not prove
that all execution costs or cancellation paths are bounded.

The [cargo-fuzz guide](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html)
describes replay and minimization in more detail.
