# Portable regex fuzz targets

This separate Cargo workspace tests `hubuum-regex` and the actual memory and
PostgreSQL compilers. It is excluded from production containers and the root
workspace.

See [the regex testing guide](../docs/regex-testing.md) for setup, limits,
synthetic seeds, CI, replay, and minimization. `regex_pattern` needs no database;
`regex_backends` requires the `postgres` feature and a disposable fixture.
