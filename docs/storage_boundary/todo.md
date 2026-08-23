# Storage Boundary TODO

This document tracks work deliberately deferred beyond the initial storage
boundary extraction. These items are not merge blockers for that extraction,
but they must be resolved before the boundary is presented as a stable external
adapter SDK unless an item says otherwise.

## Validate the Boundary with a Second Complete Adapter

- [ ] Implement every `StorageBackend` capability in a genuinely independent
  adapter that does not reuse PostgreSQL rows, queries, or adapter-private
  helpers.
- [ ] Run the unchanged conformance, service, transaction, readiness, and HTTP
  suites against it.
- [ ] Record contract friction found by the implementation and fix abstractions
  that encode PostgreSQL mechanics rather than application semantics.

This is complete when the second adapter can be selected through the normal
application composition path and passes the same portable acceptance criteria
as PostgreSQL. It does not need to be production-ready to provide useful design
evidence.

## Finish Method-Specific Query Matrices

- [ ] Document the exact supported filters, sort keys, cursor behavior,
  visibility rules, and count semantics for every pageable or searchable
  storage method.
- [ ] Distinguish unsupported combinations from invalid requests and define the
  required portable error for each case.
- [ ] Keep the matrices synchronized with query DTOs, adapter implementations,
  and conformance scenarios.

This is complete when an adapter author does not need to inspect PostgreSQL
queries or application handlers to determine a method's observable query
semantics.

## Bound Delegated Search Candidate Processing

- [ ] Replace full candidate materialization in delegated unified search with a
  backend-neutral, bounded candidate-page and authorization protocol.
- [ ] Preserve filtering, deterministic ordering, authorize-before-page
  behavior, cursor semantics, and exact authorized totals across batches.
- [ ] Add conformance and performance scenarios whose candidate set is much
  larger than the requested page.

This is complete when external authorization cannot force an adapter or the
application to retain an unbounded candidate set while producing one search
page.

## Make Semantic Evidence Method-Aware

- [ ] Give every contract method an explicit effect classification and
  method-specific evidence.
- [ ] Make the coverage guard verify that named scenarios invoke the method and
  assert the relevant effects, rather than checking only inventory and test
  names.
- [ ] Infer context-free writers mechanically from the contract, or enforce an
  equivalent exhaustive classification, so the mutation inventory cannot omit
  a new writer silently.

This work should improve evidence about observable behavior. Exact line, branch,
and failure-schedule coverage remain separate diagnostic concerns.

## Continue Auditing Adapter-Returned DTOs

- [ ] Use `external_adapter_values.rs` as the inventory for the remaining
  infallible projection DTOs.
- [ ] Decide for each value whether it is a validated invariant-bearing type or
  a provenance-only projection, and document that distinction accurately.
- [ ] Add fallible constructors or builders where meaningful invalid states can
  cross the boundary, and map corrupt persisted representations to
  `StorageErrorKind::Backend` consistently.

This audit must finish before publishing an external SDK. The current
documentation intentionally does not claim that every infallible projection
encodes every semantic invariant.

## Define the External DTO Naming Policy

- [ ] Decide which values use the `Storage` prefix and which rely on their
  capability module for namespacing.
- [ ] Use one order for capability, operation, and role terms in requests,
  queries, records, snapshots, outcomes, and builders.
- [ ] Give flat and expanded resource projections names that state their shape,
  then apply the policy in one focused compatibility change.

This is complete when adapter authors can predict a boundary type's name from
its capability, operation, and projection shape. Style-only renames remain
deferred until the supported publication surface is selected.

## Standardize Audit-Document Construction

- [ ] Define one backend-independent path for constructing canonical audit
  documents, including entity snapshots, metadata, summaries, and schema
  versions.
- [ ] Keep persistence rows and backend-native serialization details private to
  each adapter.
- [ ] Add portable conformance cases proving that equivalent mutations produce
  equivalent audit documents across adapters.

This is complete when a new adapter can construct required audit documents
without copying behavior from PostgreSQL or depending on the root application
crate.

## Establish Publication Policy

- [ ] Define the crates that form the supported external surface and close
  their publication dependency graph.
- [ ] Set SemVer, minimum supported Rust version (MSRV), deprecation, feature,
  and compatibility policies.
- [ ] Document the release and upgrade process for adapter authors, including
  how breaking contract changes are announced and tested.

Dynamic plugin discovery, runtime loading, and a stable plugin ABI are separate
design concerns. Publishing Rust crates for statically linked adapters does not
require solving them, and this TODO does not put them on the storage-boundary
critical path.
