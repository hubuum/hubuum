# Backup task failure

Alert: `HubuumBackupFailed`.

## Meaning

At least one backup task failed in the last hour. The alert clears when that observation leaves the window, not necessarily when a backup succeeds.

## Diagnose

Inspect the authorized backup task result, configured size limits, database capacity and output storage. Establish the age of the last independently verified recoverable backup.

## Recover

Correct the cause, create a new backup and verify it through an isolated restore. This alert cannot prove that scheduled backups ran, that an artifact is intact, or that recovery works.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
sum by (deployment) (increase(hubuum_task_completions_total{kind="backup",final_status="failed"}[1h])) > 0
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
