# Task worker errors

## Meaning

Worker loop errors have continued to appear in the rolling ten-minute window for five minutes.

## Diagnose

Inspect bounded worker logs, database connection failures, schema readiness and runtime configuration. This reports loop errors, not every individual failed task.

## Recover

Resolve the underlying database/configuration failure and verify workers claim new work. Follow graceful shutdown procedures when restarting workers.
