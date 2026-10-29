# Lifecycle delivery plan

Implementation tracking for [#503](https://github.com/hubuum/hubuum/issues/503),
starting 2026-10-27. The issue remains the design authority and the three parent
epics retain their complete requirements. This plan orders reviewable work; it
does not claim any archive or permanent-deletion behavior has shipped.

The [lifecycle audit contract proposal](lifecycle_audit_contract.md) tracks #550's
transition matrix, exact membership evidence, safe sink views and shared
consumer fixtures. Its acceptance gates apply to the owning lifecycle features;
the proposal does not activate those features or block unrelated foundations.

The [task receipt and policy contract review](lifecycle_task_policy_contract.md)
records #545's feature-specific submission outcomes and proposed preview/apply
boundary. Its examples do not advertise unimplemented policy routes; audit and
sink contracts remain coordinated separately in #550.

## Delivery order

| Step | Issues | Review scope and acceptance gate |
| --- | --- | --- |
| 1 | #504 | Inventory, reproducible fixtures, current measurements and numerical budgets. First implementation in this series. |
| 2 | #505 | Identity coverage matrix first, then UUID backfill, historical references and compatible backup/restore. Prove reused integers cannot associate unrelated resources. |
| 3 | #506 | Restore-isolated policy and admission accounting for existing workloads. Cross-restore identity references require #505. |
| 4 | #507 | Coordinated claiming, caps, bounded promotion and reduced-capacity fallback. Requires #506 and #504 measurements. |
| 5 | #508 | Verified PostgreSQL artifacts, producer integration, then recovery and real pipeline measurements. Can start independently; acceptance requires #504. |
| 6 | #509, #510 | S3 and filesystem adapters after #508. Consolidate common conformance fixtures when the PR stays focused; retain provider-specific tests. |
| 7 | #511 | Extend #505/#506 with deletion authority and independent checkpoints before enabling destructive guarantees. |
| 8 | #512, #513 | Receipts and hot-only expiry after #511; #513 also requires #504. Consolidate only if both use the same bounded cleanup executor. |
| 9 | #514 | Exact/as-of archive prototype after #504/#505/#508. Measure with hot copies retained. |
| 10 | #515 | Connect #513/#514/#509 into movement and verified source retirement. |
| 11 | #516 | Durable waiting after #506; independently deliverable before step 10. |
| 12 | #517 | Cold provider after #509/#515/#516, including actual provider evidence. |
| 13 | #518 | Targeted erasure after #511/#513/#515, covering all enabled adapters. |
| 14 | #519 | Bounded timeline/predicate tasks after #514/#516. |
| 15 | #520 | Destination/tier migration after #515/#510/#517. |
| 16 | #521 | Expensive API admission after #506/#504; independently deliverable from step 3 onward. |

Proceed issue by issue, revisiting independent branches as prerequisites become
available. Keep implementation prerequisites distinct from acceptance evidence.
Consolidate related changes when it avoids repeating schema/interface work;
independent foundations do not belong in one large PR just to share CI. Use a
GitHub stack only for a strict linear dependency with independently reviewable
layers. Each PR has at most one new migration version; never rewrite merged
migrations.

## Crate and trait ownership

| Boundary | Ownership and constraints |
| --- | --- |
| `hubuum-domain` | Distinct private-field resource, principal and version identities. Preserve integer REST identifiers; validate once at decoding. |
| `hubuum-task-core` | Runtime-neutral task states, policy values and legal transitions. Separate occupied execution, outstanding work, resident bytes and cumulative accounting. |
| `hubuum-budget` | Reuse byte-limit and bounded-writer primitives rather than creating a competing generic implementation. |
| `hubuum-storage-core` | Complete hot-backend operation contracts with validated requests, claims and results. No SQL, pools, global configuration or Actix. |
| `hubuum-storage-postgres` | Queries, constraints, transactions, protected control state, migrations and adapter-owned errors. Convert errors at the adapter boundary. |
| Artifact contract, introduced with #508 | Small immutable byte-store interface separate from `StorageBackend`. Distinguish artifact, write attempt, destination and object version. Verification proof types accompany durable fences. Choose crate placement with a concrete consumer. |
| Provider crates, introduced with #509/#510 | Provider SDK/filesystem details stay private; expose crate-owned receipts, retrieval states and deletion outcomes. |
| `hubuum-scale-core` | Config-free workload recipes and aggregate reports; benchmark traits are separate from production capabilities. |
| `hubuum-scale-benchmark` | CLI/process orchestration; selected adapters own fixture writes and physical measurements. |

Every new interface needs a real caller. Preserve proof types across services
and storage instead of converting back to primitives and revalidating. Keep
third-party implementation types private unless they are the intended integration
surface. Maintain the existing storage SDK compatibility policy. A standalone
crate promotion requires its own dependency, packaging and compatibility review;
internal public visibility alone does not publish or stabilize a crate.

## Verification and progress

Each issue retains its acceptance matrix and #503's cross-cutting cases: restore
with reused integers, retry after task purge, late commits, closure during
provider waits, unequal-duration overload, corruption, and crashes/erasure at
every transfer boundary. Mocks cannot establish real producer/provider recovery
or resource guarantees.

Run the repository test runner, strict clippy and rustfmt. Regenerate OpenAPI
for endpoint/schema changes. Verify CI classification for new fixtures and
embedded inputs, and markdown lint for documentation. Manifest/workspace/container
changes additionally require Docker parity and production image builds. Review
the changelog in every PR; explicitly document supported API/backup/configuration
breaks and upgrade actions. Internal benchmark-only work has no user-facing
changelog entry and must say so in its PR description.

- #504: merged in #522 with a reproducible synthetic baseline, initial targets
  and acceptance gates. See
  [lifecycle budgets](performance/lifecycle_budgets.md).
- #505: durable logical identities and compatible backup/restore merged in #524.
- #506: shared admission and restore-isolated capacity accounting merged in #525.
- #507: implemented in draft #527; P3/P4 performance acceptance remains open.
  See [scheduler measurements](task_scheduling.md#measurement).
- #508: dormant artifact staging in draft #523, with dependent backup producer
  staging in draft #528. Production activation and pipeline evidence remain open.
- #516: dormant durable-wait foundation adds checkpoints and fenced wake tokens.
  Built-in producers remain disabled; see [durable waiting](task_admission.md#durable-waiting-foundation).
- Other #509–#521 milestones retain their issue-specific acceptance gates.
