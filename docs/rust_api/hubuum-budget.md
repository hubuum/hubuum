# `hubuum-budget` Rust API Policy

Status: experimental public API in the storage SDK `0.7` release train.

## Purpose and Compatibility

Standard-library byte accounting and bounded `std::io::Write` destinations,
shared by backup capture/serialization and template/export rendering. The
crate follows the [storage SDK policy](../storage_adapter_sdk.md), including
Rust 1.88, exact in-graph dependencies, and coordinated releases. It has no
optional features, runtime dependencies, global configuration, or wire format.

`ByteLimit` is an immutable finite policy snapshot; zero accepts no nonempty
output. `ByteBudget` tracks cumulative accepted bytes with overflow-safe,
all-or-nothing charges. Rejection leaves usage unchanged and sets a sticky
observation. Smaller later charges remain possible; all-or-nothing operations
must propagate the first failure. `BudgetExceeded` contains only counts.

`LimitedWriter` checks each fragment before calling its destination, counts
successful partial writes, preserves I/O errors, and delegates flush. Its
intentional integration surface is `std::io::Write`. It allocates no buffer;
the destination owns capacity and durability. `into_inner` ends accounting.
Dropping the writer releases the destination but cannot undo accepted output.

`ByteBudget::writer` creates an exclusively borrowed `ChildWriter`. Each accepted
write consumes both the child's effective cap and the parent. The child cap is
clamped to remaining parent capacity while the exclusive borrow prevents
competing charges. Dropping a child never refunds accepted bytes. This supports
sequential batch composition without locks; concurrent reservations are outside
the API. Equal parent/child limits attribute exhaustion to the child.

## Ownership and Verification

Hubuum maintainers own this crate. Unit tests cover exact/zero/overflow bounds,
partial I/O, early termination, errors, and drop. Production backup capture and
isolated template tests verify the actual enforcement boundaries. The root
`operation_budgets_criterion` benchmark compares the migrated consumers with
their previous algorithms. See [operation budgets](../operation_budgets.md)
for policy selection, resource semantics, and the extraction decision.
