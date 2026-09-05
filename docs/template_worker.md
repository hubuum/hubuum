# Template worker

Install `hubuum-template-worker` beside the application binaries. Production
containers and release archives include it; local builds use the workspace's
default members. Tests and standalone template benchmarks must build it first:

```sh
cargo build --locked -p hubuum-templates --bin hubuum-template-worker
```

Every render and syntax/composition validation runs in a fresh process with a
128 MiB budget for live Rust heap allocations. The parent admits at most four
workers at a time, allows sixteen additional admitted operations to wait, and
rejects excess work immediately. The five-second deadline includes admission and
transport. Worker allocation failure, timeout, or abnormal termination becomes
a template error; it does not abort the server. The allocator budget does not
claim to measure total resident memory: stack, executable pages, and allocator
bookkeeping remain additional process overhead.

The server awaits worker admission, pipe I/O, and completion asynchronously.
A dedicated runtime thread supervises children independently of HTTP runtimes.
Cancellation removes queued work or kills an executing child; capacity is retained
until the child is reaped. Graceful shutdown closes admission, cancels outstanding
work, and drains children before telemetry shuts down. Response JSON decoding
runs on a bounded number of blocking tasks, with capacity retained until it ends.
Borrowed request serialization remains synchronous and size-bounded; this CPU
cost and process startup are still included in operation measurements.

Operational lifecycle logs and `hubuum_template_worker_events` counters identify
admission, startup, completion, overload, cancellation, shutdown, deadlines, and
failure. `hubuum_template_worker_duration` records terminal operation duration.
Both metrics use only the bounded `event` label. Trace spans associate operations
with their caller; logs include an operation ID and child PID, never template
sources, names, context, rendered output, or worker-provided error messages.
Normal lifecycle logs are debug-level; resource and execution failures are warnings.
Export, remote-call, and delivery failures retain their existing task/event paths.
Worker lifecycle telemetry is not a new persisted, subscribable domain event.

The protocol accepts at most 16 MiB of serialized input. Ordinary template
outputs default to 1 MiB; exports keep their configured output limit up to a
hard 32 MiB ceiling. Remote URLs and individual headers have 8 KiB limits,
with at most 128 headers and 64 KiB of rendered header content in aggregate.
Email subjects have a 4 KiB limit. Fuel and recursion limits still apply. Macros, captures,
includes and normal expressions remain supported. Compilation is isolated too.
The former application-wide compiled-template cache is removed, so workloads
with many tiny renders pay process startup and compilation costs.

See [the template and schema measurements](performance/template_worker.md) for
startup and concurrent execution costs and their measurement limits.
