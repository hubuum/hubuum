# Deployment verification

The [deployment guide](../deployment.md#updates) owns the operator procedure.
An offline migration requires downtime and its documented recovery procedure;
rolling-compatible test results from older releases do not override that rule.

## Proxy availability checks

The rollout test allows one retry of a read-only health/readiness probe after a
pre-response Caddy disconnect, within the original seven-second deadline. It
reports retries separately. HTTP errors, connection refusal, timeouts, partial
responses, and repeated disconnects fail the test. This checks HTTP availability,
not uninterrupted TCP connections or safe replay of writes.

## Historical adoption and adjacent releases

`scripts/test-single-host-zero-downtime.sh` adopts a pinned v0.0.1 installation
through the required offline migration, then checks subsequent rolling updates
of the migrated installation. Its name does not mean the initial migration is
free of downtime. The old release combines API and worker processes, so drain
its tasks and stop its writers before migration.

`scripts/test-adjacent-release-upgrade.sh` resolves the latest stable release by
immutable image digest and seeds representative data through its API. After
draining the worker, the candidate's migration preflight chooses the test path:

- **Rolling:** keep the old API available, exercise reads and writes through both
  versions after migration, and test an application-only rollback.
- **Offline:** stop the old API, snapshot PostgreSQL, and migrate with no old
  writers. Recovery restores that snapshot before starting the old binaries.

Both paths verify worker operation and return to the candidate. The test also
checks backup creation and isolated restores. Its evidence applies to the
specific tested versions and migration mode; it does not establish compatibility
with every older release or a database downgrade path.
