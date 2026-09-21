# Delayed event pipeline

## Meaning

The oldest actionable fanout or delivery item has exceeded five minutes for ten minutes.

## Diagnose

Use the queue label to distinguish fanout from delivery. Inspect event worker configuration and the authorized event-delivery health endpoint. Check destination availability and secret resolution.

## Recover

Restore the failed dependency or correct the sink configuration. Retry dead deliveries only after checking whether external effects already occurred. Do not erase queued audit events.
