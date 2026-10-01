# Delayed task queue

Alert: `HubuumTaskQueueDelayed`.

## Meaning

The oldest queued task of a kind has exceeded five minutes for ten minutes. Database gauges are deduplicated across scrapers.

## Diagnose

Check worker readiness, configured worker counts, active long-running jobs, database connectivity and lease recovery. Inspect authorized task records for the affected kind.

## Recover

Restore workers or capacity. Cancel only work whose consequences you understand; remote calls may already have external effects. Do not delete task rows to clear the alert.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
max by (deployment, kind) (hubuum_task_oldest_age_seconds{state="queued"}) > 300
```

Inspect non-secret settings with an administrator token:

```sh
curl --fail --silent -H "Authorization: Bearer $HUBUUM_TOKEN" \
  "$HUBUUM_API/api/v1/admin/config" | jq .
```

Check the affected process logs for the same time window. For a single host:

```sh
cd /opt/hubuum
sudo docker compose logs --since 15m hubuum-api hubuum-api-standby
```

Use `sudo podman compose` for Podman, or the equivalent workload logs in a
distributed installation. Keep credentials and raw resource details in controlled
logs, not metric labels or alert titles.

Escalate to the deployment owner when the symptom persists after the documented
recovery, affects every replica, or indicates an integrity problem. Confirm the
underlying operation succeeds and observe a full alert window with current
scrapes. A vanished series alone does not prove recovery.

Tune thresholds and `for` durations in the shared rule file to the deployment's
workload. Review [service objectives and aggregation](../README.md#service-objectives)
and the [metrics guide](../../docs/metrics.md) before changing queries.
