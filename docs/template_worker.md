# Template worker

Install `hubuum-template-worker` beside the application binaries. Production
containers and release archives include it; local builds use the workspace's
default members. Tests and standalone template benchmarks must build it first:

```sh
cargo build --locked -p hubuum-templates --bin hubuum-template-worker
```

Each single execution or batch runs in a fresh process with a
128 MiB budget for live Rust heap allocations shared by the entire operation. The parent admits at most four
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

Email delivery renders its subject and body in one worker, compiling each once.
Remote calls render their URL, headers and optional body in one worker; target
syntax validation also uses one batch. Batches contain at most 130 templates and
serialize their shared context once. Results retain insertion order, and a failed
entry fails the whole batch without returning partial output. Each entry has its
own environment, includes, fuel, recursion, escaping and missing-value settings;
no compiled templates or request data survive the child process.

The five-second deadline and 16 MiB serialized-input limit cover the whole batch,
including all sources and the shared context. These aggregate limits are stricter
than giving every field a fresh deadline and input allowance. Reduce combined
template size or execution work if an existing email or remote target exceeds
them. Remote targets with more than 128 headers are rejected during validation.

The protocol accepts at most 16 MiB of serialized input. Ordinary template
outputs default to 1 MiB; exports keep their configured output limit up to a
hard 32 MiB ceiling. Remote URLs and individual headers have 8 KiB limits,
with at most 128 headers and 64 KiB of rendered header content in aggregate.
Email subjects have a 4 KiB limit. Fuel and recursion limits still apply. Macros, captures,
includes and normal expressions remain supported. Compilation is isolated too.
The former application-wide compiled-template cache is removed, so workloads
with many tiny renders pay process startup and compilation costs. Batching pays
startup once per logical operation. The batch also enforces an aggregate rendered
output cap, never above 32 MiB, and returns at most 1,024 missing-value warnings.
Remote header content, including names, must still fit 64 KiB before HTTP execution.
Worker counters and durations describe the whole batch, and each result reports
the same whole-worker peak live heap measurement.

See [the template and schema measurements](performance/template_worker.md) for
startup and concurrent execution costs and their measurement limits.
