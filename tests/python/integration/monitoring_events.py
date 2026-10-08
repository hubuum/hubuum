"""Known event deliveries through the installed server and monitoring stack.

Rust owns exhaustive delivery semantics. This scenario verifies that the packaged
worker, database snapshots, recording rules, Grafana and alert evaluator agree
about two real deliveries of one API mutation. No queue rows are manufactured.
"""

import json
import time

from integration.monitoring import canonical, require, wait_for


STATES = ('total', 'pending', 'in_flight', 'succeeded', 'failed', 'dead', 'retryable')
INSTANCES = ('hubuum-api', 'hubuum-api-standby')
ALERT = 'HubuumEventDeadLetters'


def counts(**values):
    return dict.fromkeys(STATES, 0) | values


def queue_matches(rows, expected, replicas=False):
    """Reject empty, duplicate, missing or incorrectly deduplicated series."""
    wanted = {(instance, state): value for instance in (INSTANCES if replicas else ('',))
              for state, value in expected.items()}
    observed = {}
    for row in rows:
        labels = row['metric']
        if labels.get('queue') != 'delivery':
            continue
        if labels.get('deployment') != 'localhost':
            return False
        key = (labels.get('instance', ''), labels.get('state'))
        if key in observed:
            return False
        observed[key] = float(row['value'][1])
    return observed == wanted


class EventScenario:
    def __init__(self, installation):
        self.installation = installation
        self.worker = installation.project + '-event-worker'
        self.worker_started = False
        self.caddyfile = installation.directory / 'Caddyfile'
        self.original_caddy = self.caddyfile.read_text()
        self.report = {'snapshots': [], 'alert': {'transitions': []}}
        installation.report['event_delivery'] = self.report

    def sql(self, query):
        return json.loads(self.installation.compose('exec', '-T', 'postgres', 'psql',
                          '-U', 'hubuum', '-d', 'hubuum', '-Atc', query))

    def api(self, path, payload=None, method=None, status=200):
        code, body = self.installation.request(path, 'api', payload, method=method)
        require(code == status, f'Event fixture API {path} returned {code}, expected {status}')
        return body

    def receiver(self, retry_status):
        # Reuse the installation's TLS terminator and disposable CA. Only the
        # additional internal virtual host receives fixture webhooks. Access
        # logs correlate the real HTTPS requests with the database event UUID.
        self.caddyfile.write_text(self.original_caddy + f'''
https://caddy {{
    tls internal
    log {{
        output file /data/monitoring-events.json
        format json
    }}
    respond /accepted 200
    respond /retry {retry_status}
}}
''')
        self.installation.compose('exec', '-T', 'caddy', 'caddy', 'reload',
                                  '--config', '/etc/caddy/Caddyfile', '--adapter', 'caddyfile')

    def seed(self):
        require(self.sql('select count(*) from event_deliveries') == 0,
                'Event scenario requires a fresh delivery queue')
        # Drain historical Atlas events before creating subscriptions.
        wait_for('Atlas fanout to drain', lambda: self.sql(
            'select count(*) from events where dispatched_at is null') == 0)
        collection = self.api('/api/v1/collections')[0]
        self.collection_id = collection['id']
        sink = self.api('/api/v1/event-sinks', {
            'name': 'monitoring-acceptance', 'kind': 'webhook', 'config': {},
        }, status=201)
        self.api(f'/api/v1/event-sinks/{sink["id"]}/collections/{self.collection_id}',
                 method='PUT', status=204)
        self.subscriptions = {}
        for route in ('accepted', 'retry'):
            subscription = self.api(f'/api/v1/collections/{self.collection_id}/event-subscriptions', {
                'sink_id': sink['id'], 'name': 'monitoring-' + route,
                'entity_types': ['collection'], 'actions': ['updated'],
                'filter': {'entity_ids': [self.collection_id]},
                'routing': {'url': 'https://caddy/' + route},
            }, status=201)
            self.subscriptions[route] = subscription['id']
        self.api(f'/api/v1/collections/{self.collection_id}',
                 {'description': 'Known monitoring acceptance mutation'}, method='PATCH', status=202)
        wait_for('two deliveries from one collection mutation', lambda: len(self.rows()) == 2)
        rows = self.rows()
        require({row['subscription_id'] for row in rows} == set(self.subscriptions.values()),
                'Unexpected delivery subscriptions')
        require(len({row['event_uuid'] for row in rows}) == 1, 'Deliveries must refer to the same event')
        require(all(row['entity_type'] == 'collection' and row['entity_id'] == self.collection_id
                    and row['action'] == 'updated' for row in rows), 'Wrong canonical event delivered')
        self.event_uuid = rows[0]['event_uuid']
        self.delivery_id = next(row['id'] for row in rows if row['subscription_id'] == self.subscriptions['retry'])
        self.report['event_uuid'] = self.event_uuid

    def rows(self):
        return self.sql('''select coalesce(json_agg(row_to_json(deliveries)), '[]'::json) from (
            select d.id, d.subscription_id, d.status, d.attempts,
                   d.status = 'failed' and d.next_attempt_at <= now() as retryable,
                   e.event_id as event_uuid, e.entity_type, e.entity_id, e.action
            from event_deliveries d join events e on e.id = d.event_id order by d.id
        ) deliveries''')

    def sql_counts(self):
        observed = counts()
        for row in self.rows():
            observed['total'] += 1
            observed[row['status']] += 1
            observed['retryable'] += int(row['retryable'])
        return observed

    def snapshot(self, name, expected, attempts):
        print('  event delivery: ' + name, flush=True)
        self.report['stage'] = name
        wait_for(name + ' SQL counts', lambda: self.sql_counts() == expected, timeout=180)
        rows = self.rows()
        require({row['subscription_id']: row['attempts'] for row in rows} == {
            self.subscriptions['accepted']: 0, self.subscriptions['retry']: attempts,
        }, 'Unexpected failed-attempt counts')
        health = self.api('/api/v1/event-deliveries/health')
        require(health['delivery']['counts'] == expected, 'Delivery health API differs from SQL')
        raw = 'hubuum_event_queue_items{queue="delivery"}'
        recorded = 'hubuum:event_queue:items{queue="delivery"}'
        wait_for(name + ' on both scrape targets', lambda: queue_matches(
            self.installation.query(raw), expected, replicas=True))
        wait_for(name + ' recording convergence', lambda: queue_matches(
            self.installation.query(recorded), expected))
        code, dashboard = self.installation.request('/grafana/api/dashboards/uid/hubuum-events', 'grafana')
        require(code == 200, 'Events dashboard unavailable')
        panel = next(p for p in dashboard['dashboard']['panels'] if p['title'] == 'Event queue states')
        require(len(panel['targets']) == 1, 'Review event panel acceptance for changed target layout')
        expression = panel['targets'][0]['expr'].replace('$deployment', 'localhost')
        at = time.time()
        direct = self.installation.query(expression, at)
        grafana = self.installation.query(expression, at, grafana=True)
        require(queue_matches(grafana, expected), f'Grafana shows wrong {name} counts')
        require(canonical(direct) == canonical(grafana), 'Grafana event panel differs from Prometheus')
        # Keep the known states stable across the entire cache/scrape check.
        require(self.sql_counts() == expected, name + ' changed before observation completed')
        self.report['snapshots'].append({'state': name, 'sql': expected, 'api': health['delivery']['counts'],
                                         'prometheus': self.installation.query(raw), 'grafana': grafana,
                                         'failed_attempts': attempts})

    def start_worker(self):
        ca = self.installation.root / 'event-ca.crt'
        ca.write_text(self.installation.compose('exec', '-T', 'caddy', 'cat',
                      '/data/caddy/pki/authorities/local/root.crt'))
        ca.chmod(0o644)
        # Inherit the installed image, opaque runtime DB credentials and network.
        # Trust just the fixture CA inside this Linux worker; TLS verification
        # stays enabled and no host trust store is changed. Private targets are
        # an existing supported setting, scoped to this internal fixture worker.
        self.installation.compose('run', '-d', '--no-deps', '--name', self.worker,
            '-e', 'SSL_CERT_FILE=/tmp/event-ca.crt', '-e', 'SSL_CERT_DIR=/tmp/empty-roots',
            '-v', f'{ca}:/tmp/event-ca.crt:ro', 'hubuum-api', '--runtime-role', 'worker',
            '--task-workers', '0', '--event-fanout-workers', '0', '--event-delivery-workers', '1',
            '--event-delivery-max-attempts', '2', '--event-delivery-retry-backoff-base-ms', '120000',
            '--event-delivery-retry-backoff-max-ms', '120000', '--remote-call-allow-private-targets')
        self.worker_started = True

    def received(self, expected):
        content = self.installation.compose('exec', '-T', 'caddy', 'cat', '/data/monitoring-events.json')
        rows = [json.loads(line) for line in content.splitlines()]
        observed = []
        for row in rows:
            request = row['request']
            headers = {key.lower(): value for key, value in request['headers'].items()}
            require(request['method'] == 'POST' and 'tls' in request, 'Receiver must observe verified HTTPS POSTs')
            require(headers.get('idempotency-key') == [self.event_uuid], 'Receiver saw a different event UUID')
            observed.append((request['uri'], row['status']))
        require(sorted(observed) == sorted(expected), f'Unexpected receiver requests: {observed}')
        self.report['receiver_requests'] = observed

    def alert_state(self):
        code, rules = self.installation.request('/prometheus/api/v1/rules?type=alert', 'prometheus')
        require(code == 200, 'Cannot read event alert rule')
        rule = next(r for g in rules['data']['groups'] for r in g['rules'] if r['name'] == ALERT)
        require(rule['duration'] == 600 and rule['health'] == 'ok', 'Use the healthy production ten-minute event alert')
        alerts = rule['alerts']
        require(len(alerts) <= 1, 'Shared-database event alert was duplicated by replica')
        if alerts:
            require(alerts[0]['labels']['deployment'] == 'localhost'
                    and float(alerts[0]['value']) == 1, 'Event alert must report exactly one dead delivery')
        state = rule['state']
        transitions = self.report['alert']['transitions']
        if not transitions or transitions[-1]['state'] != state:
            transitions.append({'state': state, 'time': time.time()})
            print('  event dead-letter alert: ' + state, flush=True)
        return state

    def run(self):
        try:
            require(self.alert_state() == 'inactive', 'Dead-letter alert active before the scenario')
            self.receiver(503)
            self.seed()
            self.snapshot('pending', counts(total=2, pending=2), 0)
            self.start_worker()
            wait_for('first webhook failure', lambda: self.sql_counts() == counts(total=2, succeeded=1, failed=1))
            # Hold the worker, not the database, while a real retry deadline
            # elapses. This makes both failed/not-due and retryable gauges
            # observable for the normal 30-second DB cache and scrape cadence.
            self.installation.run(self.installation.engine, 'pause', self.worker)
            try:
                self.snapshot('failed', counts(total=2, succeeded=1, failed=1), 1)
                self.received([('/accepted', 200), ('/retry', 503)])
                self.snapshot('retryable', counts(total=2, succeeded=1, failed=1, retryable=1), 1)
            finally:
                self.installation.run(self.installation.engine, 'unpause', self.worker)
            self.snapshot('dead', counts(total=2, succeeded=1, dead=1), 2)
            self.received([('/accepted', 200), ('/retry', 503), ('/retry', 503)])
            wait_for('production dead-letter alert pending', lambda: self.alert_state() == 'pending')
            # The independent five-minute scrape alert fits inside the event
            # alert's ten-minute hold. alert() restores the standby and waits
            # for recovery before subsequent assertions require both replicas.
            self.installation.alert()
            wait_for('production dead-letter alert firing', lambda: self.alert_state() == 'firing', timeout=720)
            self.receiver(200)
            self.api(f'/api/v1/event-deliveries/{self.delivery_id}/retry', method='POST')
            self.snapshot('recovered', counts(total=2, succeeded=2), 2)
            self.received([('/accepted', 200), ('/retry', 503), ('/retry', 503), ('/retry', 200)])
            wait_for('dead-letter alert recovery', lambda: self.alert_state() == 'inactive')
            require(not self.installation.query(f'ALERTS{{alertname="{ALERT}"}}'), 'Stale event alert after recovery')
            self.report['alert']['recovered'] = True
        finally:
            # The outer fixture cleanup also removes this project's one-off
            # worker if a failed startup prevents reaching this cleanup.
            if self.worker_started:
                self.installation.run(self.installation.engine, 'rm', '-fv', self.worker)
            self.caddyfile.write_text(self.original_caddy)
            self.installation.compose('exec', '-T', 'caddy', 'caddy', 'reload',
                                      '--config', '/etc/caddy/Caddyfile', '--adapter', 'caddyfile')
