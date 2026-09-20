# Stale metrics refresh

## Meaning

A process has not refreshed a metric source for five minutes, sustained for another five minutes.

## Diagnose

Check the source label, refresh-failure counters, database availability and scrape target health. Inventory gauges may be stale even while HTTP scrapes succeed.

## Recover

Restore metric collection before trusting queue and inventory panels. Missing scrape targets need a separate Prometheus up alert under your deployment monitoring policy.
