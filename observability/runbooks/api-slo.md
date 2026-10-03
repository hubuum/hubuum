# API service objectives

## Alerts and impact

`HubuumApiAvailabilityBurn`, `HubuumApiAvailabilitySlowBurn`, `HubuumApiLatencyBurn`.

API errors or slow successful responses are consuming the configured 30-day error budget. Users see failed requests or latency.

## First checks

Break down HTTP status and latency by stable route template. Inspect PostgreSQL checkout latency, worker load and recent deployments. Probes and client errors do not contribute to these SLIs.

```promql
hubuum:api_error_budget_burn:rate1h
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

Reduce load or roll back a compatible application change after checking migration compatibility. Tune objectives to workload evidence; do not hide errors by adding exclusions.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/performance.md).
