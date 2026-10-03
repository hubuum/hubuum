# Recovery and supervised operator jobs

## Alerts and impact

`HubuumOperatorJobFailed`, `HubuumOperatorJobOverdue`.

An expected restore verification, archive, or retention job failed or has no successful result within its maximum age. Recovery readiness is unproven until verification succeeds.

## First checks

Inspect the scheduler, node exporter textfile collector and the job output. An expected series without a last-success series means the job has never succeeded. A missing exporter must be monitored independently.

```promql
time() - hubuum_operator_job_last_success_timestamp_seconds
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

Fix the failing job and run a new verification against an isolated disposable database. Never restore over production to silence a monitoring alert. Serialize each deployment/operation job and verify its exit code reflects all integrity checks.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/backup-restore.md).
