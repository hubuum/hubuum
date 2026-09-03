# Database Pool Tuning and Load Testing

Hubuum's primary asynchronous bb8 PostgreSQL pool is shared by HTTP handlers,
task workers, event workers, and retention work. Each enabled task, event
fan-out, or event-delivery notification listener uses a dedicated
one-connection pool. Task-enabled processes use another dedicated
one-connection pool for task lease renewal. Tune the primary pool as a database
concurrency limit, not as a mirror of the Actix worker count, and include the
auxiliary connections in the instance's total PostgreSQL connection budget.

For deterministic large and huge datasets, stateful mixed workloads, and
backup/restore lifecycle measurements, use the
[scale operational benchmark suite](scale_benchmarks.md).

## Connection Budget

Start with the PostgreSQL connection budget for one Hubuum instance:

```text
per_instance_budget =
    floor(
        (postgres_max_connections - reserved_connections - other_client_connections)
        / hubuum_instance_count
    )
```

Set `HUBUUM_DB_POOL_SIZE` no higher than that budget. Leave capacity for
administration, migrations, monitoring, failover overlap, and non-Hubuum
clients.

Notification listeners do not reduce primary-pool capacity, so a task-only or
event-only worker can execute with `HUBUUM_DB_POOL_SIZE=1`. They do count toward
PostgreSQL's total connection budget: one connection for each enabled task,
fan-out, and delivery listener. Task lease renewal can use one additional
connection while tasks are active.

The pool is a deliberate backpressure boundary. More Actix or task workers do
not require the same number of connections; they wait asynchronously when the
pool is busy.

## Relevant Settings

- `HUBUUM_DB_POOL_SIZE` controls the maximum number of connections in the
  primary pool. The default is `10`; dedicated notification-listener and task
  lease pools are additional to this limit.
- `HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS` bounds how long work waits for a
  connection. The default is `2000` ms. Keep it below the external request or
  proxy timeout so overload fails predictably inside Hubuum.
- `HUBUUM_DB_STATEMENT_TIMEOUT_MS` is the pool-global PostgreSQL statement
  timeout. The default is `30000` ms. PostgreSQL cancels statements that exceed
  it, freeing the connection for later work.
- `HUBUUM_EXPORT_DB_STATEMENT_TIMEOUT_MS` can impose a lower export-only
  statement timeout without shortening unrelated database work.

The current bb8 defaults maintain no minimum idle count, validate a connection
when it is checked out, close connections after a maximum lifetime of 30
minutes, and close excess idle connections after 10 minutes.

## Pool Observability

An administrator can inspect `/api/v0/meta/db`. The response includes:

- current maximum, total, idle, in-use, and available connection capacity;
- current pending acquisitions;
- cumulative direct, waited, and timed-out acquisitions;
- cumulative acquisition wait time;
- created connections and connections closed as broken, invalid, expired, or
  idle.

Use deltas between samples for rates. In particular:

- sustained `pending_acquisitions` indicates saturation;
- growth in `acquisitions_waited` is early evidence of contention;
- any steady-state growth in `acquisitions_timed_out` means the pool or database
  cannot serve the offered load within the configured acquisition timeout;
- growth in `connections_closed_broken` can indicate transaction cancellation,
  network instability, or database restarts.

The PostgreSQL `active_connections` value is database-wide and is not the same
as Hubuum's pool-local `in_use_connections`.

## Repeatable Load Test

The k6 scenario in [`load-tests/pool.js`](../load-tests/pool.js) drives a
constant request arrival rate. Run it only against an isolated test deployment
with production-like data and database latency.

The test requires an API token. Avoid placing tokens in shell history or
committed files; inject them through the environment used by the test runner.

```bash
HUBUUM_LOAD_BASE_URL="https://hubuum.test.example" \
HUBUUM_LOAD_TOKEN="$TEST_ADMIN_TOKEN" \
HUBUUM_LOAD_RATE="50" \
HUBUUM_LOAD_DURATION="2m" \
k6 run load-tests/pool.js
```

`HUBUUM_LOAD_PATHS` accepts `|`-separated paths. The default is a collection
listing that skips the exact count query. A mixed read test can be run with:

```bash
HUBUUM_LOAD_PATHS="/api/v1/collections?limit=25&include_total=false|/api/v1/collections?limit=25&include_total=true|/api/v1/search?q=server" \
HUBUUM_LOAD_RATE="100" \
HUBUUM_LOAD_DURATION="5m" \
k6 run load-tests/pool.js
```

Confirm every configured path and token manually before a long run. Permission
failures and invalid paths count as failed requests.

## Tuning Procedure

1. Record `/api/v0/meta/db`, PostgreSQL CPU, locks, connection count, and query
   latency before the run.
2. Test pool sizes such as `5`, `10`, and `20`, subject to the per-instance
   connection budget. Restart Hubuum between runs so cumulative counters reset.
3. Keep the data set, instance count, request mix, arrival rate, and duration
   fixed while comparing pool sizes.
4. Record throughput, p50/p95/p99 latency, failed requests, pending
   acquisitions, acquisition timeouts, and PostgreSQL saturation indicators.
5. Increase the arrival rate until the service reaches its intended operating
   limit, then run an overload case above that limit.
6. Select the smallest pool that meets the latency target without steady-state
   acquisition timeouts or unacceptable PostgreSQL saturation.

The overload case should produce bounded errors after the acquisition timeout,
not unbounded latency or process instability. Async database access prevents
waiters from occupying blocking threads, but it does not make PostgreSQL
queries faster or increase the database connection budget.

## Transaction Cancellation

If an async transaction future is cancelled, diesel-async marks a connection
with an open or indeterminate transaction as broken. bb8 discards it instead of
returning it to the pool, and PostgreSQL rolls the transaction back when the
connection closes. This is safe but causes connection churn, visible through
`connections_closed_broken` and `connections_created`.

Server-side statement timeouts are preferable to cancelling an outer future:
the query returns an error normally, allowing the transaction helper to execute
its rollback path without replacing the connection.
