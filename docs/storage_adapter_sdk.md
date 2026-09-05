# Storage Adapter SDK Compatibility

Status: accepted; current experimental release train is `0.2`.

## Supported Crate Graph

The statically linked storage adapter SDK consists of exactly these crates:

| Crate | Supported purpose | Supported features |
| --- | --- | --- |
| `hubuum-computed-fields` | Validated computed-field definitions and deterministic evaluation | Default only |
| `hubuum-domain` | Validated identifiers, revisions, JSON Patch, and domain values | Default; `openapi` |
| `hubuum-events-core` | Event catalog, envelopes, filters, and mutation provenance | Default; `schema` |
| `hubuum-query` | Bounded, backend-neutral query parsing and values | Default only |
| `hubuum-task-core` | Task identity and idempotency values | Default only |
| `hubuum-storage-core` | Complete capability traits, DTOs, errors, transactions, and aggregate | Default only |
| `hubuum-storage-conformance` | Reusable behavioral certification for complete adapters | Default only |

Their publication graph is closed:

```text
hubuum-storage-conformance
        |          |
        |          +-- hubuum-domain
        v
hubuum-storage-core
    |       |       |       |       |
    |       |       |       |       +-- hubuum-task-core
    |       |       |       +---------- hubuum-query
    |       |       +------------------ hubuum-events-core
    |       +-------------------------- hubuum-domain
    +---------------------------------- hubuum-computed-fields
```

Every in-graph dependency uses an exact version requirement. The seven packages
share one version and are released together. The root application and
`hubuum-storage-postgres` are not part of the supported SDK. PostgreSQL remains
the in-repository reference implementation, while an external adapter depends
only on the graph above.

The SDK is a Rust source compatibility contract for static Cargo composition.
It is not a dynamic plugin ABI, wire protocol, runtime capability handshake, or
server embedding API.

## Versioning and Aggregate Evolution

The SDK remains `experimental-public` during the `0.x` series:

- a patch release is source compatible with its minor line;
- a minor release may make a documented breaking change;
- all seven crates still advance together, even when only one crate changes; and
- adapter manifests use an exact requirement for `hubuum-storage-core` and the
  matching `hubuum-storage-conformance` release.

At `1.0.0`, ordinary Semantic Versioning applies: compatible additions use a
minor release, fixes use a patch release, and incompatible changes use a major
release.

`StorageBackend` is the mandatory aggregate. Adding a required supertrait or
trait method, removing or changing a method, changing a carrier or result, or
changing a closed semantic vocabulary is incompatible for adapter authors. It
therefore requires a coordinated minor release before `1.0.0` and a major
release afterward. The project will not hide a required capability behind an
unsupported default. A versioned parallel aggregate is introduced only if a
future migration genuinely requires two application contract generations to
coexist; it is not the routine evolution mechanism.

`cargo-semver-checks` runs against the latest crates.io release for every SDK
crate. Its result supplements the policy above: a change that is behaviorally
breaking still requires the coordinated incompatible release even if a source
API checker cannot detect it.

## MSRV and Features

The minimum supported Rust version is Rust 1.88 for the entire release train.
Every documented feature combination must build and be usable on that version.
Raising the MSRV requires an incompatible coordinated release and migration
notice during `0.x`; after `1.0.0`, it follows the project's documented major
release policy unless a future policy explicitly establishes an MSRV window.

The two optional features are additive:

- `hubuum-domain/openapi` adds Utoipa schema implementations; and
- `hubuum-events-core/schema` adds Utoipa schema implementations and enables
  `hubuum-domain/openapi`.

Default behavior must not change when either feature is disabled. Removing or
renaming a feature, making it mandatory, or changing a feature so it removes
an API is incompatible. New additive features are compatible when existing
feature combinations keep their behavior.

## Enum Evolution

Public SDK enums have an explicit closed or extensible policy.

`hubuum-storage-core::StorageCapability` is extensible and carries
`#[non_exhaustive]`. Downstream diagnostics must include a wildcard match arm;
new capability labels are compatible additions.

Every other public SDK enum is a closed semantic vocabulary. Adding, removing,
renaming, or reinterpreting a variant is an incompatible change. The audited
closed set is:

- `hubuum-computed-fields`: `DefinitionError`, `FieldErrorCode`, `Operation`, and
  `ResultType`;
- `hubuum-domain`: `EventDeliveryStatus`, `JsonPatchErrorKind`,
  `JsonSchemaErrorKind`, `MaintenanceState`, `PrincipalKind`,
  `ResourceRevisionError`, and `StorageJsonValidationError`;
- `hubuum-events-core`: `Action`, `ActorKind`, `EntityType`,
  `EventCatalogError`, `EventFilterError`, and `EventSinkSecretError`;
- `hubuum-query`: `ComputedFieldScope`, `ComputedQueryValueType`,
  `CursorCodecError`, `CursorValue`, `DataType`, `Operator`,
  `QueryError`, `QueryScalarType`, `RelatedClassField`,
  `RelatedFilterTarget`, `RelatedObjectField`, `SearchOperator`,
  `StructuredQueryExpression`, and `StructuredQueryField`;
- `hubuum-task-core`: `IdempotencyKeyError`; and
- `hubuum-storage-core`: all public enums except `StorageCapability`, including
  authorization permissions, errors, lifecycle selectors, query dimensions,
  task and restore states, import policies, mutation outcomes, notifications,
  and execution call sites.

Error enums are deliberately closed because their classifications are part of
the portable contract. A new failure category requires adapter and caller
review rather than being silently absorbed by a wildcard. Private adapter
enums and application HTTP enums are outside this SDK policy.

## Deprecation and Removal

A supported item is normally deprecated for at least one coordinated minor
release before removal. Removal occurs only in an incompatible release and the
changelog gives the replacement and migration action. A required aggregate
method is not deprecated until its replacement can express the complete
semantics and the conformance suite covers the migration.

An urgent security or soundness problem may require immediate removal or
restriction. That release must contain an explicit security note, affected
versions, and the safest available migration.

## Release Process

SDK releases are distinct from server `vX.Y.Z` releases. Maintainers:

1. choose one version for all seven packages and update every exact in-graph
   dependency plus `Cargo.lock`;
2. record supported additions, changes, deprecations, and every breaking
   migration in `CHANGELOG.md`;
3. update the crate policy documents, storage contract, method registry,
   semantic evidence, and conformance behavior together;
4. run the Rust API policy, package, rustdoc, SemVer, formatting, lint, and full
   repository suites;
5. publish in dependency order, waiting for crates.io index visibility between
   layers: `hubuum-computed-fields`, `hubuum-domain`, `hubuum-query`, and
   `hubuum-task-core` first, `hubuum-events-core` second, `hubuum-storage-core`
   third, and `hubuum-storage-conformance` last; and
6. create an annotated `storage-sdk-vX.Y.Z` tag and release notes only after all
   seven immutable crate versions are visible.

The local publication checks are:

```bash
python3 scripts/check-rust-api-policy.py
python3 scripts/test-rust-api-policy.py
cargo package --locked \
  --package hubuum-computed-fields \
  --package hubuum-domain \
  --package hubuum-events-core \
  --package hubuum-query \
  --package hubuum-task-core \
  --package hubuum-storage-core \
  --package hubuum-storage-conformance
```

CI builds rustdoc with warnings denied and all features enabled. It runs
`cargo-semver-checks` once a registry baseline exists. Publication itself is a
maintainer-controlled operation because crates.io versions cannot be replaced.

## Adapter Upgrade Process

Adapter authors should upgrade `hubuum-storage-core` and
`hubuum-storage-conformance` to the same exact SDK version, update every direct
SDK dependency to that version, and then:

1. compile the complete `StorageBackend` aggregate;
2. run the unchanged portable conformance suite;
3. run backend-native transaction, consistency, migration, failure, and
   connection-loss tests; and
4. review the SDK changelog for behavior changes that compile-time checks do
   not express.

An adapter version supports exactly the SDK version it declares. Applications
select adapters statically and do not negotiate compatibility at runtime.

## Data, Errors, Runtime, and Security

Crate-specific policy documents under `docs/rust_api/` define supported
entrypoints, error and panic behavior, runtime and cancellation expectations,
serialization guarantees, and secret-redaction requirements. The
[storage contract](storage_boundary/contract.md),
[query semantics](storage_boundary/query-semantics.md), and
[testing contract](storage_boundary/testing.md) are normative for adapter
behavior.

## Upgrading from 0.1 to 0.2

Update all seven SDK dependencies together. Relation query constructors now
require `TraversalBudget`; adapters must enforce its depth and generated-work
limits before final sorting and pagination. Implement the claimed import methods
with atomic domain changes and item receipts, and reject an expired or replaced
claim at commit.

The [generated inventory](generated/project_inventory.md) records current package
versions and minimum Rust versions. The server's configuration and deployment
upgrade actions are in the [runtime hardening guide](runtime_hardening.md).
