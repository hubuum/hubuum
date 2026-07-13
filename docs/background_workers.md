# Background Worker Lifecycle

Hubuum owns its task, event fan-out, event delivery, retention, and PostgreSQL
notification workers as one process lifecycle. Workers start only after database
initialization and HTTP binding succeed. Every worker thread is named, retained,
and given the same cancellation signal.

## Graceful Shutdown

Actix handles the operating-system shutdown signal and first stops accepting
HTTP traffic. When the HTTP server finishes, Hubuum shuts down background work
in this order:

1. Request cancellation for every worker and notification listener.
2. Wake polling sleeps and stop starting new database iterations.
3. Cancel active event iterations. Claimed rows remain protected by their
   existing claim timeout and can be reclaimed normally.
4. Cancel an active task execution and mark its task failed with a sanitized
   service-unavailable result instead of leaving it in an active state.
5. Run `UNLISTEN` on PostgreSQL notification connections that remain open and
   release them. An already-closed session has already released its listener.
6. Join all named worker threads, bounded by 30 seconds.
7. Drop the process database pool only after worker shutdown completes.

The ordering ensures a worker cannot start new database work after pool
shutdown begins. A worker that does not stop within the bound is logged by name;
its pool clone remains alive, so the underlying pool cannot be dropped while
that worker could still use it.

Shutdown logs contain worker names, counts, elapsed time, and timeout status.
They do not contain database URLs, credentials, event payloads, or task input.

## PostgreSQL Notifications

Event fan-out and delivery listeners select between the notification stream and
the shared shutdown signal. This wakes an otherwise idle `LISTEN` connection
immediately. The listener executes `UNLISTEN` before returning an open
connection, and the process pool closes its sessions after all workers have
joined.

## Startup Failures

Workers are not started if database initialization or HTTP binding fails. This
prevents detached workers from surviving a failed server startup path.
