# Contribute to Hubuum

Start with the [development guide](../development.md) to build, test, and run the
server. Read the repository's
[contributor instructions](https://github.com/hubuum/hubuum/blob/main/AGENTS.md)
before preparing changes.

## Choose an area

| Work | Starting point |
| --- | --- |
| Documentation and examples | [Documentation workflow](documentation.md) and [review roadmap](documentation-review.md) |
| Application use cases and persistence | [Application and storage boundary](../storage_boundary.md) |
| A new storage adapter | [Backend author guide](../storage_boundary/backend-author-guide.md) and [SDK compatibility](../storage_adapter_sdk.md) |
| Background work | [Task system internals](../task_system.md) |
| Performance | [Scale benchmarks](../scale_benchmarks.md) and [runtime measurements](../performance/runtime_hardening.md) |
| Release preparation | [Release process](../releasing.md) and [operational contracts](../operational_contracts.md) |
| CLI, frontend, or HTTP clients | The relevant [ecosystem repository](../ecosystem.md) |

## Keep contracts reviewable

Behavior changes need focused tests and corresponding documentation. API shape
changes also require a regenerated OpenAPI document. Review the changelog for
every pull request and make upgrade actions explicit for breaking changes.

The root `hubuum` library is an internal application composition crate. Its
public Rust visibility does not promise a third-party embedding API. Consult
the [Rust API policy](../rust_api_boundary.md) and per-crate policies before
changing workspace interfaces.
