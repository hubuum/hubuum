# Identity dependency failures

## Alerts and impact

`HubuumIdentityBackendFailures`, `HubuumEphemeralTokenKey`.

Shared limiter operations, login processing or external authorization are failing, or a token key will not survive restart. Users may be unable to sign in or use existing tokens.

## First checks

Check controlled authentication logs, LDAP and Treetop reachability, Valkey status, secret-file permissions, and non-secret administrator configuration. Login internal errors do not uniquely identify LDAP failures.

```promql
sum by (deployment, class) (increase(hubuum_api_errors_total{class="permission_backend_unavailable"}[10m]))
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

Restore dependency availability or the intended secret mount. Do not disable authorization or rotate token keys merely to clear an alert; coordinate any key change across replicas.

Escalate to the deployment owner if the symptom persists through a normal retry
window, affects all replicas, or suggests data integrity problems. Preserve
controlled logs and the time range before changing configuration.

## Confirm recovery and tune

Confirm the alert returns to inactive and the underlying operation succeeds.
Observe at least one full alert window; an absent series alone is not recovery.
Thresholds and `for` windows live in the shared Prometheus rules. Tune them to
the deployment's workload and documented objective, keeping the two windows of
an SLO alert consistent.

See [the operator package](../README.md) and [the detailed guide](../../docs/external_auth.md).
