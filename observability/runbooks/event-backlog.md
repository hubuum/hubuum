# Delayed event pipeline

Alert: `HubuumEventBacklog`.

## Meaning

The oldest actionable fanout or delivery item has exceeded five minutes for ten minutes.

## Diagnose

Use the queue label to distinguish fanout from delivery. Inspect event worker configuration and the authorized event-delivery health endpoint. Check destination availability and secret resolution.

## Recover

Restore the failed dependency or correct the sink configuration. Retry dead deliveries only after checking whether external effects already occurred. Do not erase queued audit events.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
max by (deployment, queue) (hubuum_event_oldest_age_seconds) > 300
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
