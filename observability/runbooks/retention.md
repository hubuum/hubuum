# Retention and archive operations

## Alerts and impact

`HubuumOutputCleanupFailures`, `HubuumEventRetentionFailures`.

Export/backup output cleanup or event-retention database work is failing. Retained data may grow and exhaust storage.

## First checks

Inspect retention settings, database locks, filesystem capacity, archive permissions and event-retention logs. Use external archive-job monitoring for independently scheduled archive verification.

```promql
sum by (deployment, kind) (increase(hubuum_task_output_cleanup_failures_total[1h]))
```

Inspect the non-secret running configuration with an administrator token:

```sh
curl --fail --silent -H "Authorization: Bearer $HUBUUM_TOKEN" \
  "$HUBUUM_API/api/v1/admin/config" | jq .
```

On a single host, inspect the relevant service without changing its state:

```sh
cd /opt/hubuum
sudo docker compose logs --since 15m hubuum-api hubuum-api-standby
```

For Podman use `sudo podman compose`; for a distributed deployment inspect the
corresponding workload's logs and probes. Keep credentials and provider details
out of incident titles and metric labels.

## Recovery and escalation

Repair storage access or restore capacity and let the normal bounded worker retry. Do not mass-delete rows, remove archive files or shorten retention without an approved retention decision.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/events.md).
