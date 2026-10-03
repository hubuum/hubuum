# Database pool saturation

Alert: `HubuumDatabasePoolSaturated`.

## Meaning

A process has used more than 90% of its configured pool for ten minutes.

## Diagnose

Identify the instance and runtime role. Check acquisition failures, query latency, PostgreSQL locks and connection capacity. Inspect slow work before increasing pool size; aggregate database connection limits across replicas.

## Recover

Reduce or pause the identified workload, resolve blocking queries, or restore database capacity. Avoid raising all replica pools together without a database capacity calculation.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
max by (deployment, instance) (hubuum_db_pool_connections{state="checked_out"}) / max by (deployment, instance) (hubuum_db_pool_connections{state="configured"}) > 0.9
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
