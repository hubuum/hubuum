# Testing guide

Use the language that owns the behavior being verified. Rust tests prove Hubuum's
application contracts. Python tests prove repository tooling and the assembled
deployment. A Python process starting a Rust test does not move that contract
into Python.

## Ownership

| Behavior | Owner | Location |
| --- | --- | --- |
| Domain validation, newtypes, permissions, services and API responses | Rust | `src/tests/`, colocated Rust tests, `tests/*.rs` and their modules |
| SQL constraints, transactions, locking, migrations, recovery and query budgets | Rust | PostgreSQL adapter and application integration tests |
| Metric emission, labels and in-process instrumentation | Rust | Observability and application tests |
| Production LDAP, SMTP, AMQP, Valkey and webhook semantics | Rust, with Python provisioning fixtures | `tests/event_transport_contract.rs`, `tests/integration_services/` |
| Installer rendering, command failure handling, release/CI policy and artifact drift | Python local regressions | `tests/python/unit/` |
| Caddy routing, TLS/authentication, Grafana queries, Prometheus rules/alerts and deployment lifecycle | Python integration | `tests/python/integration/` |
| Packaged-server corpus import/export and upgrade smoke evidence | Python integration | Corpus harnesses and existing shell upgrade drivers |

Keep exhaustive API status, permission and validation cases in Rust. Python may
make a small number of API requests to demonstrate that a built image and its
surrounding services work together. It should not duplicate the Rust API matrix
or implement a second version of domain rules. Promtool fixtures own evaluation
of PromQL alert and recording expressions; Rust owns the emitted metrics.

## Rust suite

Set `HUBUUM_TEST_DB_HOST`, `HUBUUM_TEST_DB_PORT`, `HUBUUM_TEST_DB_USER` and
`HUBUUM_TEST_DB_PASSWORD` in `.env` for a disposable PostgreSQL instance. Then run
the established parallel suite:

```bash
source .env && ./run_tests.sh
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

The runner includes PostgreSQL adapter tests and a resource-limited JSON Schema
probe. Python applies the probe's process limits; its assertions remain in Rust.
Use Rust's shared fixtures and scoped names for application data. Never replace
database concurrency or migration coverage with a Python mock.

## Python local regressions

Python 3.11+ and the standard library are sufficient. The tests use local files,
fake executables and loopback HTTP servers; they do not start containers, build
Rust binaries or require a running database. Git, Bash, curl and Cargo metadata
must be available for the tools they exercise. No pip installs or virtual
environment are needed.

```bash
# Discover every local test, also the default with no arguments.
python3 -I -S tests/python/run.py unit

# List tests, run one area, or select a module/class/method.
python3 tests/python/run.py unit --list
python3 tests/python/run.py unit deployment
python3 tests/python/run.py unit policies.test_rust_api
```

The categories are `deployment`, `monitoring`, `policies` and `tooling`. Add a
`test_*.py` module with `unittest.TestCase` classes in the appropriate category;
discovery includes it automatically. New subdirectories need `__init__.py` for
Python 3.11 discovery. Put shared fixtures in `tests/python/support/`, not in a
test module imported by another test. Missing selections, import errors and an
empty suite fail instead of silently passing.

## Explicit integration runs

Integration commands are opt-in and list their options with `--help`. Container
tests need Docker (or rootful Podman where documented); transport fixtures also
need OpenSSL and the Rust toolchain. The `event-transports` harness requires a
Linux host: its disposable CA uses `SSL_CERT_FILE`, which the production TLS
verifier on macOS and Windows does not honor. Unsupported hosts fail before
building or provisioning; the required Production integration contracts CI job
runs this harness on Linux. Build the production image before using it:

```bash
docker build --build-arg 'CARGO_BUILD_FLAGS=-F tls-rustls -F tls-openssl --locked --release' --tag hubuum-server:verify .
python3 tests/python/run.py integration monitoring --image hubuum-server:verify --report monitoring-acceptance.json
python3 tests/python/run.py integration monitoring-fixture --engine docker
python3 tests/python/run.py integration event-transports
python3 tests/python/run.py integration corpus verify --image hubuum-server:verify
python3 tests/python/run.py integration atlas verify --image hubuum-server:verify
```

The monitoring acceptance run imports real data, checks both replicas against SQL,
compares dashboard queries through Grafana and Prometheus, waits for the real
five-minute alert hold, and tests update/reinstall/purge. It cleans up its unique
project and emits non-secret evidence. The transport driver provisions TLS
services and executes Rust assertions. `integration schema-budget` runs the Rust
resource probe; `integration treetop-server` serves the conformance fixture.
The corpus commands also support offline `check` and explicit `generate` modes.

## CI and maintenance

The Python tests job discovers the entire local suite on Python 3.11 and 3.12
with site packages disabled. Separate required monitoring integration jobs run
Docker and rootful Podman. The production-container job runs real-server
monitoring acceptance and the corpus checks against the image it just built.
Production transport assertions run in their dedicated Rust contract job.

Operational commands stay in `scripts/`: for example, `observability.py` generates
and validates assets and records operator jobs. Test drivers live under
`tests/python/`; do not add standalone `scripts/test-*.py` entrypoints or make an
operational tool import the test suite. Existing shell lifecycle and compatibility
tests remain shell entrypoints and use the suite when they need a Python fixture.

When moving a test or fixture, update all callers and documentation, then verify
`scripts/classify-ci-changes.sh` and its regression tests. A new unit test needs no
CI command-list edit; an integration command must remain explicitly wired into
the appropriate required job.
