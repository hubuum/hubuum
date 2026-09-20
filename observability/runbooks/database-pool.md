# Database pool saturation

## Meaning

A process has used more than 90% of its configured pool for ten minutes.

## Diagnose

Identify the instance and runtime role. Check acquisition failures, query latency, PostgreSQL locks and connection capacity. Inspect slow work before increasing pool size; aggregate database connection limits across replicas.

## Recover

Reduce or pause the identified workload, resolve blocking queries, or restore database capacity. Avoid raising all replica pools together without a database capacity calculation.
