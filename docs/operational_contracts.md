# Operational Contract Compatibility

Hubuum versions the non-HTTP interfaces that operators, deployment tooling,
and integrations consume. The canonical machine-readable snapshot is
[`operational-contract.json`](operational-contract.json). It complements, but
does not replace, the OpenAPI contract.

## Covered Contracts

The snapshot is generated from typed or validated sources and records:

- Prometheus metric names, types, units, label names and bounded domains,
  histogram buckets, feature ownership, and process or database scope;
- environment-variable ownership, value types, non-secret defaults, numeric
  bounds, allowed enum values, cross-field constraints, process-role
  applicability, secret classification, and running-configuration exposure;
- event-envelope, nested provenance, and sink-payload fields and nullability,
  actor/entity/action catalogs, schema-version semantics, and redaction rules;
- backup and import versions and section catalogs, plus export formats and
  supported enum values; and
- public `hubuum-server` and `hubuum-admin` flags, environment bindings,
  defaults, value domains, conflicts, requirements, stable output modes, and
  exit-code categories.

The generated [Metric Reference](metrics-reference.md) is the operator-facing
view of the metric portion of the same registry.

## Regeneration

The operational CLI contract includes embedded migration flags, so regenerate
with the production feature enabled:

```bash
cargo run --quiet --features embedded-migrations \
  --bin hubuum-operational-contracts -- json \
  > docs/operational-contract.json
cargo run --quiet --features embedded-migrations \
  --bin hubuum-operational-contracts -- metrics-markdown \
  > docs/metrics-reference.md
```

CI regenerates both files and fails on exact drift. Unit tests also enforce the
committed output and reject duplicate metrics, histograms without explicit
buckets, and obvious high-cardinality labels such as usernames, user IDs,
object IDs, task IDs, raw paths, and error messages.

`scripts/release.sh prepare` regenerates the versioned JSON snapshot after it
updates the package version. Release-readiness and version-bump checks reject a
snapshot whose `release` field does not match `Cargo.toml`.

Secret defaults are represented only by `default_is_set`; their values are
never serialized into the snapshot. Machine-dependent defaults are represented
by `dynamic_default` instead of serializing the value observed on the machine
that generated the snapshot.

## Compatibility Classification

Changes are classified against the latest stable release snapshot:

- **Additive:** a new metric, variable, command, optional field, or catalog
  value that leaves existing consumers valid; removal of a configuration
  constraint; or relaxed CLI required, conflict, dependency, or value-count
  rules.
- **Behavioral:** changed defaults, descriptions, aggregation scope, or an
  expanded bounded label domain. These require review and may require an
  operator note even when they do not block compatibility.
- **Breaking:** removals, narrowed numeric or enum domains, changed metric types, units,
  labels or buckets, added configuration constraints, new required CLI options,
  removed or replaced CLI environment bindings, stronger CLI requirements,
  weaker secret classification, or format shape changes without the
  corresponding version increase.

Event-envelope shape changes must increase both production event versions:
`schema_version` for base audit documents and `revision_aware_schema_version`
for documents containing revision-bearing snapshots. These values come from the
audit-document version sources used by event writes, stored-event projection,
and sink fan-out; changing the envelope builder's default does not satisfy the
gate. Import section names come from the serialized graph's Serde keys.
Backup and import shape or section changes must increase their document version. The
checker reports both the shape change and a missing version bump so a format
change cannot silently reuse an old version number.

## Intentional Breaks

Intentional breaking changes use
`.github/operational-contract-breaking-exceptions.json`. Each exception must:

- name the exact baseline release;
- contain only the reported change fingerprints it accepts;
- have an expiry date;
- explain the reason and migration action; and
- identify text present in the candidate release's changelog notes.

Expired, stale, baseline-mismatched, or undocumented exceptions do not suppress
the gate. There is no global compatibility bypass.

## Local Policy Test

Run the fixture suite with:

```bash
scripts/test-operational-contract-compatibility.sh
```

The suite covers compatible additions, removed metrics, changed defaults,
configuration constraints, required CLI additions, environment-binding
changes, release candidates, format version enforcement, narrow exceptions,
expiry, missing baselines, and baseline tag/digest binding.
