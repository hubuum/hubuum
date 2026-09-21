# Delayed task queue

## Meaning

The oldest queued task of a kind has exceeded five minutes for ten minutes. Database gauges are deduplicated across scrapers.

## Diagnose

Check worker readiness, configured worker counts, active long-running jobs, database connectivity and lease recovery. Inspect authorized task records for the affected kind.

## Recover

Restore workers or capacity. Cancel only work whose consequences you understand; remote calls may already have external effects. Do not delete task rows to clear the alert.
