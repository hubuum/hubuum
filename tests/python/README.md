# Python test suite

Run all local regressions with Python 3.11+ and no third-party packages:

```bash
python3 -I -S tests/python/run.py unit
python3 tests/python/run.py unit --list
python3 tests/python/run.py integration --help
```

- `unit/`: automatically discovered tooling, policy, installer and monitoring regressions.
- `integration/`: explicit live-system checks and drivers for Rust fixture tests.
- `support/`: shared paths, script loading and reusable fixtures.

See the [testing guide](../../docs/testing.md) for the Rust/Python ownership
boundary, prerequisites, commands and CI coverage.
