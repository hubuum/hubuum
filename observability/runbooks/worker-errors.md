# Task worker errors

Alert: `HubuumWorkerErrors`.

## Meaning

Worker loop errors have continued to appear in the rolling ten-minute window for five minutes.

## Diagnose

Inspect bounded worker logs, database connection failures, schema readiness and runtime configuration. This reports loop errors, not every individual failed task.

## Recover

Resolve the underlying database/configuration failure and verify workers claim new work. Follow graceful shutdown procedures when restarting workers.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
sum by (deployment) (increase(hubuum_task_worker_iterations_total{outcome="error"}[10m])) > 0
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
