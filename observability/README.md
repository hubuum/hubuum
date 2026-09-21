# Hubuum operator package

This initial package provides a Grafana overview and Prometheus alerts for the
metrics Hubuum already exposes. Thresholds are starting points; adjust them to
your workload. This is the first part of
[#257](https://github.com/hubuum/hubuum/issues/257), not a complete SLO package.

## Install

1. Scrape every API and worker process directly, with metrics enabled. Do not
   scrape a load-balanced application URL. Set a stable `deployment` label on
   every target sharing one Hubuum database, and use distinct values for separate
   databases. Keep the normal unique `instance` label for each process.
2. Add `prometheus/alerts.json` to Prometheus `rule_files`. JSON is valid YAML
   and can be consumed directly as a Prometheus rule file. Configure Alertmanager
   routing for the `severity="warning"` alerts under your own on-call policy.
3. Import `dashboards/overview.json` into Grafana. Select your Prometheus data
   source and deployment. Edit this JSON directly; it has no generation step.
4. Exercise alerts in staging before enabling notifications. Each alert links
   to a runbook in this repository; pin those links to your deployed release if
   you need immutable instructions.

Database-wide queue gauges use `max` across replicas, never `sum`. Worker error
and completion counters are process-local and are summed after calculating their
increase or rate. Pool utilization remains per process. Missing data is not
converted to zero; the metrics-refresh alert helps identify stale inventory,
while your deployment monitoring must detect missing scrapes with `up`.
The refresh alert also detects sources that fail before their first successful
refresh. Worker-error and failed-backup counters expose a zero baseline at
startup; scrape it before running work to observe the first failure. Events
before the first scrape or between a process restart and its first scrape can
still be missed by counter-based alerts.

The backup alert reports observed failures, not missed scheduled backups or
recoverability. Worker loop errors do not cover every failed task. Long-running
jobs may justify different queue thresholds. Metric refresh and scrape intervals
must be substantially shorter than the alert windows.

## Validate

With Python 3.11+ and Docker available:

```sh
python3 scripts/check-observability.py --promtool
```

The validator checks metric names, selector labels and enumerated values against
`docs/operational-contract.json`, verifies runbooks and fixture presence, then
uses digest-pinned Prometheus 3.5.0 to check and evaluate the rules. Fixtures cover
firing and recovery for every alert and duplicate database gauges. CI runs this
validation on code changes and release tags. No third-party Python packages are
required. Omit `--promtool` for the static checks alone.

Prometheus Operator installations can copy the `groups` value into a
`PrometheusRule.spec.groups` object. This package does not install a controller,
Helm chart, Grafana instance, or notification receiver.

## Follow-up scope

Dedicated API/SLO, integration, retention and recovery dashboards, burn-rate
rules and a packaged Prometheus Operator deployment remain in #257. Health-check
traffic is not used to infer application availability in this initial package.
