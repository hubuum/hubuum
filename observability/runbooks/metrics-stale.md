# Stale metrics refresh

Alert: `HubuumMetricsRefreshStale`.

## Meaning

A process has not refreshed a metric source for five minutes, sustained for another five minutes.
The alert also fires after five minutes when a source has failed without ever
refreshing successfully. It clears once that source refreshes successfully.

## Diagnose

Check the source label, refresh-failure counters, database availability and scrape target health. Inventory gauges may be stale even while HTTP scrapes succeed.

## Recover

Restore metric collection before trusting queue and inventory panels. Missing scrape targets need a separate Prometheus up alert under your deployment monitoring policy.

## Inspect and confirm

Evaluate the alert expression in Prometheus, selecting the affected deployment:

```promql
time() - hubuum_metrics_refresh_last_success_timestamp_seconds > 300 or (hubuum_metrics_refresh_failures_total > 0 unless hubuum_metrics_refresh_last_success_timestamp_seconds)
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
