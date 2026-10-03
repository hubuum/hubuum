# Process and API availability

## Alerts and impact

`HubuumScrapeUnavailable`, `HubuumNoReadyApi`, `HubuumFileDescriptorsHigh`.

A configured process cannot be scraped, every configured readiness probe is failing, or a process has used over 90% of its file descriptors. Requests may fail even if another replica still answers.

## First checks

Inspect service status, /readyz, recent rollouts, schema readiness and database connectivity. A scrape failure is not proof that the API is unavailable; compare the independent readiness probes.

```promql
up{job="hubuum"}
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

Restart or replace one unhealthy replica at a time after identifying the cause. Increase a descriptor limit only after ruling out leaks. Do not restart every replica together.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/deployment.md).
