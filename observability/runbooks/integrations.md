# External integration probes

## Alerts and impact

`HubuumIntegrationProbeFailed`.

An explicitly configured dependency probe is failing. The affected integration may not accept authentication, queries or deliveries.

## First checks

Check the bounded component label, network reachability, certificates and provider health. A successful TCP probe proves connectivity only; inspect authorized delivery health and controlled logs for semantic failures.

```promql
probe_success{job="hubuum-integrations"}
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

Restore the dependency and confirm an actual operation succeeds. Retry individual deliveries only after assessing duplicate side effects; do not mass-replay messages.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/integration-coverage.md).
