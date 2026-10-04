# Production integration coverage

Run `python3 tests/python/run.py integration event-transports` with Python 3.11+, OpenSSL and
Docker. Production features are compiled with the locked dependency graph.
Fixture images are pinned by digest in the runner and `integration-fixtures.py`.
Only loopback ports are exposed; private CAs and disposable credentials are
removed with the fixtures. Do not upload raw container logs: upstream startup
scripts can print fixture credentials.

| Integration | Real-service evidence | Additional evidence or remaining scope |
| --- | --- | --- |
| OpenLDAP | Verified LDAPS and STARTTLS, login, rejected credentials/filter injection, stable-subject refresh, untrusted CA, restart | Application group reconciliation and stale-cache policy have separate application tests; production directory vendor profiles are not claimed |
| Mailpit SMTP | Authenticated implicit TLS, stored recipient/body, 451/550 recipient rejection, invalid password, untrusted CA, cached transport restart | Errors currently share the sink failure surface; permanent SMTP failures do not imply a new delivery retry policy |
| RabbitMQ | Publisher confirms, mandatory unroutable rejection, event identity/payload, cached sink restart over TLS | Delivery leases, fencing and dead-letter behavior remain storage/application contracts |
| Valkey Streams | Verified TLS, identity/payload, exact trimming, cached sink restart | Shared login-limiter tests run in their separate required CI job |
| [Chat webhook recipes](webhook_notifications.md) | Local HTTPS fixtures check the published Slack, Mattermost, Discord, and Apprise gateway examples with the production renderer and transport | Payload escaping, test markers, Discord length/mentions and `wait=true`, bearer authentication, acknowledgements, rate limits, and retry classification; these are contract fixtures, not live provider deployments |
| HTTPS | Private CA trust/rejection, redirect refusal without following, 503, deadlines, bounded response preview | Destination screening and header policy also have deterministic adapter tests |

Full CI and tag validation run these contracts. Release publication requires
successful transport and login-limiter jobs; absent fixtures cannot silently
pass because the runner explicitly enables ignored tests and required environment
lookups fail when missing.

This is a release baseline for [#248](https://github.com/hubuum/hubuum/issues/248),
not completion of its entire resilience matrix. Multi-replica limiter outage
recovery, directory mutation/deprovisioning against a real directory, transport
hostname-mismatch matrices, and end-to-end delivery failure/fencing across actual
transports remain follow-ups. No compatibility with every vendor extension is
implied by these fixtures.
