"""Exercise the actual installer, server, PostgreSQL and monitoring services.

Only root guards, download sources, global names, published ports and the
bridge subnet are adapted for an isolated fixture. Rollout, database creation,
health checks, metrics, recording rules and alert durations remain real.
"""

from pathlib import Path
import argparse
import base64
import http.cookiejar
import ipaddress
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from support import ROOT


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def canonical(rows):
    """Prometheus vector order is unspecified, including through Grafana."""
    return sorted(rows, key=lambda row: sorted(row['metric'].items()))


def wait_for(description, predicate, timeout=120):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(2)
    raise AssertionError(f"Timed out waiting for {description}; last transport error: {last_error}")


class Installation:
    def __init__(self, directory, image, mode):
        self.root = Path(directory)
        self.directory = self.root / 'installation'
        self.scripts = self.root / 'scripts'
        self.scripts.mkdir()
        self.project = 'hubuum-monitoring-' + uuid.uuid4().hex[:12]
        self.image, self.mode = image, mode
        self.token = None
        self.context = None
        self.report = {'mode': mode, 'image': image, 'project': self.project}
        self.owned_volumes = set()
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            self.port = listener.getsockname()[1]
        self.base = f'https://localhost:{self.port}'
        self.engine = shutil.which('docker')
        require(self.engine is not None, 'Docker Compose is required')
        self.run(self.engine, 'image', 'inspect', image)
        networks = json.loads(self.run(self.engine, 'network', 'inspect',
                                       *self.run(self.engine, 'network', 'ls', '-q').split()))
        occupied = [ipaddress.ip_network(config['Subnet']) for network in networks
                    for config in (network['IPAM'].get('Config') or []) if config.get('Subnet')]
        offset = int(uuid.uuid4().hex[:4], 16) % 256
        candidates = (ipaddress.ip_network(f'172.{major}.{(offset + minor) % 256}.0/24')
                      for major in range(31, 15, -1) for minor in range(256))
        self.subnet = str(next(network for network in candidates
                               if not any(network.version == other.version and network.overlaps(other) for other in occupied)))
        for name in ('install-single-host.sh', 'update-single-host.sh', 'single-host-rollout.sh',
                     'single-host-monitoring.sh', 'stop-single-host.sh', 'uninstall-single-host.sh'):
            source = (ROOT / 'scripts' / name).read_text()
            (self.scripts / name).write_text(source.replace('if [[ "$EUID" -ne 0 ]]; then', 'if false; then'))
        self.environment = os.environ | {'COMPOSE_PROJECT_NAME': self.project,
                                        'HUBUUM_ROLLOUT_HEALTH_TIMEOUT_SECONDS': '120'}
        self.prepare_adapters()

    def run(self, *arguments, env=None):
        result = subprocess.run(arguments, text=True, capture_output=True, env=env, timeout=600)
        # Do not include captured output: admin commands may return credentials.
        require(result.returncode == 0, f"Command failed ({result.returncode}): {arguments[:2]}")
        return result.stdout

    def prepare_adapters(self):
        binaries = self.root / 'bin'
        binaries.mkdir()
        # Adapt files immediately before every Compose invocation, including
        # those made by the installer/updater. Do not stub the rollout itself.
        adapter = '''#!/usr/bin/env python3
import os, re, shutil, sys
from pathlib import Path
args = sys.argv[1:]
if Path(sys.argv[0]).name == 'curl':
    prefix = 'https://raw.githubusercontent.com/hubuum/hubuum/main/observability/'
    urls = [arg for arg in args if arg.startswith(prefix)]
    if urls:
        asset = urls[0][len(prefix):]
        source = Path(ASSETS) / asset
        source.resolve().relative_to(Path(ASSETS).resolve())
        shutil.copyfile(source, args[args.index('-o') + 1])
        sys.exit(0)
    os.execv(CURL, [CURL, *args])
if args and args[0] == 'compose' and Path('compose.yml').exists():
    path = Path('compose.yml')
    text = re.sub(r'^    container_name:.*\\n', '', path.read_text(), flags=re.M)
    text = text.replace('      - "80:80"\\n', '')
    text = text.replace('      - "443:443"', '      - "127.0.0.1:PORT:443"')
    path.write_text(text)
    env = Path('.env')
    env.write_text(re.sub(r'^MONITORING_HOST=.*$', 'MONITORING_HOST=localhost:PORT', env.read_text(), flags=re.M))
    # Keep the supplied local CI image. Dependencies are pulled by Compose up
    # when absent; update tests must not replace the image under test.
    if 'pull' in args:
        sys.exit(0)
os.execv(ENGINE, [ENGINE, *args])
'''.replace('PORT', str(self.port))
        settings = f'ASSETS = {str(ROOT / "observability")!r}\nCURL = {shutil.which("curl")!r}\nENGINE = {self.engine!r}\n'
        adapter = adapter.replace('import os, re, shutil, sys\n', 'import os, re, shutil, sys\n' + settings)
        for name in ('docker', 'curl'):
            path = binaries / name
            path.write_text(adapter)
            path.chmod(0o755)
        self.environment['PATH'] = str(binaries) + os.pathsep + os.environ['PATH']

    def script(self, name, *args):
        source = self.directory / name if (self.directory / name).exists() else self.scripts / name
        self.run('bash', str(source), '--dir', str(self.directory), '--engine', 'docker',
                 *args, env=self.environment)

    def compose(self, *args):
        return self.run(self.engine, 'compose', '-p', self.project, '--env-file',
                        str(self.directory / '.env'), '-f', str(self.directory / 'compose.yml'), *args)

    def values(self):
        return dict(line.split('=', 1) for line in (self.directory / '.env').read_text().splitlines()
                    if '=' in line and not line.startswith('#'))

    def request(self, path, auth=None, payload=None):
        headers = {'Content-Type': 'application/json'}
        if auth in ('prometheus', 'grafana'):
            key = 'PROMETHEUS_PASSWORD' if auth == 'prometheus' else 'GRAFANA_ADMIN_PASSWORD'
            auth = 'Basic ' + base64.b64encode(('admin:' + self.values()[key]).encode()).decode()
        elif auth == 'api':
            auth = 'Bearer ' + self.token
        if auth:
            headers['Authorization'] = auth
        request = urllib.request.Request(self.base + path, headers=headers,
                                         data=json.dumps(payload).encode() if payload is not None else None)
        try:
            with urllib.request.urlopen(request, context=self.context, timeout=10) as response:
                code, body = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, body = error.code, error.read()
        try:
            body = json.loads(body)
        except ValueError:
            body = body.decode()
        return code, body

    def query(self, expression, at=None, grafana=False):
        parameters = {'query': expression, 'time': time.time() if at is None else at}
        prefix = '/grafana/api/datasources/proxy/uid/hubuum-prometheus' if grafana else '/prometheus'
        code, body = self.request(prefix + '/api/v1/query?' + urllib.parse.urlencode(parameters),
                                  'grafana' if grafana else 'prometheus')
        require(code == 200 and body['status'] == 'success', f'Query failed: {expression}')
        return body['data']['result']

    def start(self):
        options = ['--mode', self.mode, '--api', 'localhost', '--email', 'test@example.invalid',
                   '--backend-image', self.image, '--monitoring', '--monitoring-ref', 'main',
                   '--script-base-url', self.scripts.as_uri(), '--network-subnet', self.subnet,
                   '--no-systemd', '--no-pull']
        if self.mode == 'all':
            options += ['--web', 'localhost', '--shared-host-routing', 'direct']
        self.script('install-single-host.sh', *options)
        certificate = self.compose('exec', '-T', 'caddy', 'cat', '/data/caddy/pki/authorities/local/root.crt')
        self.context = ssl.create_default_context(cadata=certificate)
        self.healthy()
        # This catches the inherited HTTP-healthcheck regression independently
        # of scrape health: the restore executor is not a metrics target.
        containers = json.loads(self.run(self.engine, 'inspect', *self.compose('ps', '-q').split()))
        for service in ('postgres', 'hubuum-restore-executor', 'hubuum-api', 'hubuum-api-standby', 'prometheus', 'grafana'):
            container = next(c for c in containers if c['Config']['Labels']['com.docker.compose.service'] == service)
            require(container['State']['Health']['Status'] == 'healthy', f'{service} is unhealthy')
        for container in containers:
            if container['Config']['Labels']['com.docker.compose.service'] in ('prometheus', 'grafana'):
                require(not container['HostConfig']['PortBindings'], 'Monitoring published an unprotected port')
                require(container['HostConfig']['Memory'] == 512 * 1024**2, 'Monitoring memory limit changed')
                require(container['HostConfig']['NanoCpus'] == 1_000_000_000, 'Monitoring CPU limit changed')
        code, flags = self.request('/prometheus/api/v1/status/flags', 'prometheus')
        require(code == 200, 'Cannot read effective Prometheus retention')
        require(flags['data']['storage.tsdb.retention.time'] == '31d', 'Prometheus time retention changed')
        require(flags['data']['storage.tsdb.retention.size'] == '5GiB', 'Prometheus size retention changed')
        self.report['fresh_install'] = 'all required containers healthy; monitoring private and resource-limited'
        self.report['retention'] = {'time': '31d', 'size': '5GiB'}

    def healthy(self):
        def ready():
            code, result = self.request('/prometheus/api/v1/targets', 'prometheus')
            targets = result['data']['activeTargets'] if code == 200 else []
            grafana = self.request('/grafana/api/health')[0]
            self.report['last_health'] = {'prometheus_status': code, 'grafana_status': grafana,
                'targets': [{'instance': t['labels']['instance'], 'health': t['health'],
                             'error': t['lastError']} for t in targets]}
            return len(targets) == 2 and all(t['health'] == 'up' for t in targets) and grafana == 200
        wait_for('both real scrape targets and Grafana', ready)
        containers = json.loads(self.run(self.engine, 'inspect', *self.compose('ps', '-aq').split()))
        self.owned_volumes.update(mount['Name'] for container in containers
                                 for mount in container['Mounts'] if mount['Type'] == 'volume')

    def authenticate(self):
        for path in ('/prometheus/api/v1/targets', '/grafana/api/search'):
            require(self.request(path)[0] == 401, f'{path} allows anonymous queries')
            bad = 'Basic ' + base64.b64encode(b'admin:incorrect').decode()
            require(self.request(path, bad)[0] == 401, f'{path} accepts an incorrect password')
        jar = http.cookiejar.CookieJar()
        client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar),
                                            urllib.request.HTTPSHandler(context=self.context))
        request = urllib.request.Request(self.base + '/grafana/login',
                    headers={'Content-Type': 'application/json', 'Origin': self.base},
                    data=json.dumps({'user': 'admin', 'password': self.values()['GRAFANA_ADMIN_PASSWORD']}).encode())
        with client.open(request, timeout=10) as response:
            require(response.status == 200, 'Native Grafana login failed')
        with client.open(self.base + '/grafana/api/search?tag=hubuum', timeout=10) as response:
            dashboards = json.load(response)
            require(len([d for d in dashboards if d['type'] == 'dash-db']) == 7, 'Expected seven provisioned dashboards')
        require(any(c.name == 'grafana_session' and c.secure and c.has_nonstandard_attr('HttpOnly') for c in jar),
                'Grafana session cookie must be secure and HttpOnly')
        require(self.request('/grafana/api/datasources/uid/hubuum-prometheus/health', 'grafana')[1]['status'] == 'OK',
                'Grafana datasource is unhealthy')
        output = self.compose('exec', '-T', 'hubuum-api', 'hubuum-admin', '--reset-password', 'admin')
        match = re.search(r'Password for user admin reset to: (\S+)', output)
        require(match, 'Could not obtain the temporary Hubuum administrator password')
        code, session = self.request('/api/v0/auth/login', payload={'name': 'admin', 'password': match[1]})
        require(code == 200, 'Hubuum login failed')
        self.token = session['token']
        self.report['authentication'] = 'anonymous/incorrect passwords rejected; native secure Grafana session works'

    def task(self, path, payload):
        code, submitted = self.request(path, 'api', payload)
        require(code == 202, f'{path} submission failed: {code}')
        def complete():
            code, task = self.request(path + '/' + str(submitted['id']), 'api')
            require(code == 200, f'{path} task lookup failed')
            if task['status'] in ('failed', 'cancelled', 'partially_succeeded'):
                raise AssertionError(f'{path} task failed: {task["status"]}')
            return task if task['status'] == 'succeeded' else None
        return wait_for(path + ' completion', complete)

    def data(self):
        self.task('/api/v1/imports', json.loads((ROOT / 'docs/assets/atlas/atlas.import.json').read_text()))
        code, server = self.request('/api/v1/classes/by-name/Server', 'api')
        require(code == 200, 'Atlas Server class missing')
        task = self.task('/api/v1/exports', {'scope': {'kind': 'objects_in_class', 'class_id': server['id']}, 'query': 'sort=name'})
        code, output = self.request(f'/api/v1/exports/{task["id"]}/output', 'api')
        require(code == 200 and output['meta']['count'] == 3, 'Server export must contain three objects')
        sql = "select json_build_object('classes',(select count(*) from hubuumclass),'objects',(select count(*) from hubuumobject),'collections',(select count(*) from collections));"
        expected = json.loads(self.compose('exec', '-T', 'postgres', 'psql', '-U', 'hubuum', '-d', 'hubuum', '-Atc', sql))
        require(expected == {'classes': 4, 'objects': 10, 'collections': 3}, 'Atlas SQL inventory differs')
        def replicas_match(expression, count):
            rows = self.query(expression)
            return len(rows) == 2 and all(float(r['value'][1]) == count for r in rows)
        for entity, count in expected.items():
            expression = f'hubuum_inventory_entities{{entity_type="{entity}"}}'
            wait_for(f'{entity} inventory cache convergence', lambda: replicas_match(expression, count))
        for kind in ('import', 'export'):
            wait_for(f'{kind} task cache convergence', lambda: replicas_match(f'hubuum_tasks{{kind="{kind}",status="succeeded"}}', 1))
        completion = self.query('hubuum_export_completions_total')
        require(len(completion) == 1 and completion[0]['metric']['instance'] == 'hubuum-api'
                and float(completion[0]['value'][1]) == 1, 'Export must execute only on the worker-enabled primary')
        roles = {r['metric']['instance']: r['metric']['role'] for r in self.query('hubuum_runtime_info')}
        require(roles == {'hubuum-api': 'all', 'hubuum-api-standby': 'api'}, 'Scrape targets have incorrect runtime roles')
        self.report['inventory'] = expected
        self.report['tasks'] = 'one successful import and export on both snapshots; three exported objects; primary executes export'

    def traffic(self):
        def expression(code):
            return f'sum(hubuum_http_requests_total{{route="/api/v1/classes",method="GET",status_code="{code}"}})'
        def count(code):
            rows = self.query(expression(code))
            return float(rows[0]['value'][1]) if rows else 0
        for auth, expected in (('api', 200), (None, 401)):
            require(self.request('/api/v1/classes', auth)[0] == expected, 'Could not prime traffic counters')
        wait_for('baseline traffic scrape', lambda: count(200) > 0 and count(401) > 0)
        before = {code: count(code) for code in (200, 401)}
        for auth, expected, amount in (('api', 200, 60), (None, 401, 20)):
            for _ in range(amount):
                require(self.request('/api/v1/classes', auth)[0] == expected, 'Unexpected workload response')
        self.compose('exec', '-T', 'hubuum-api', 'sh', '-ec',
                     'for i in $(seq 1 20); do wget -q -O /dev/null http://127.0.0.1:8080/readyz; done')
        wait_for('exact request counter deltas', lambda: count(200) - before[200] == 60 and count(401) - before[401] == 20)
        wait_for('positive recorded API rate', lambda: any(float(r['value'][1]) > 0 for r in self.query('hubuum:api_requests:rate5m')))
        records = json.loads((ROOT / 'observability/prometheus/recording-rules.json').read_text())['groups'][0]['rules']
        for name in ('hubuum:api_requests:rate5m', 'hubuum:pool_utilization:ratio', 'hubuum:event_queue:items'):
            expression = next(r['expr'] for r in records if r['record'] == name)
            stamp = self.query(f'timestamp({name})')[0]['value'][1]
            def normalized(rows):
                return sorted((tuple(sorted((k, v) for k, v in r['metric'].items() if k != '__name__')), r['value'][1]) for r in rows)
            require(normalized(self.query(name, stamp)) == normalized(self.query(expression, stamp)), f'{name} recording differs')
        # Independently define the SLI selector to catch a changed recording
        # that starts counting probes or expected client errors.
        stamp = self.query('timestamp(hubuum:api_requests:rate5m)')[0]['value'][1]
        sli = 'sum by (deployment) (rate(hubuum_http_requests_total{route=~"/api/.*",status_family=~"2xx|3xx|5xx"}[5m]))'
        recorded = self.query('hubuum:api_requests:rate5m', stamp)[0]['value'][1]
        require(recorded == self.query(sli, stamp)[0]['value'][1], 'SLI includes probes or client errors')
        self.report['http_counter_deltas'] = {'200': 60, '401': 20}
        self.report['readiness_probes'] = 20

    def panels(self):
        panels = []
        for path in sorted((ROOT / 'observability/dashboards').glob('*.json')):
            dashboard = json.loads(path.read_text())
            code, installed = self.request('/grafana/api/dashboards/uid/' + dashboard['uid'], 'grafana')
            require(code == 200 and len(installed['dashboard']['panels']) == len(dashboard['panels']), f'{path.name} was not provisioned')
            for panel in dashboard['panels']:
                for target in panel.get('targets', []):
                    expression, at = target['expr'].replace('$deployment', 'localhost'), time.time()
                    direct = self.query(expression, at)
                    via = self.query(expression, at, grafana=True)
                    require(canonical(direct) == canonical(via), f'Grafana panel differs: {panel["title"]}')
                    panels.append({'dashboard': dashboard['uid'], 'panel': panel['title'], 'series': len(via)})
        code, rules = self.request('/prometheus/api/v1/rules', 'prometheus')
        require(code == 200, 'Cannot read Prometheus rules')
        require(all(r['health'] == 'ok' for g in rules['data']['groups'] for r in g['rules']), 'A rule failed evaluation')
        require(not self.query('probe_success{job="hubuum-readiness"}'), 'Missing optional probes must not report success')
        self.report['panels'] = panels

    def alert(self):
        events = []
        code, rules = self.request('/prometheus/api/v1/rules', 'prometheus')
        require(code == 200, 'Cannot read configured alert')
        rule = next(r for g in rules['data']['groups'] for r in g['rules'] if r['name'] == 'HubuumScrapeUnavailable')
        require(rule['duration'] == 300, 'Use the production five-minute alert hold')
        self.compose('stop', 'hubuum-api-standby')
        try:
            def firing():
                # With shared-host routing, public /readyz belongs to the web
                # service. Probe the API itself and exercise its public route.
                self.compose('exec', '-T', 'hubuum-api', 'wget', '-q', '-O', '/dev/null',
                             'http://127.0.0.1:8080/readyz')
                require(self.request('/api/v1/classes', 'api')[0] == 200,
                        'Public API became unavailable during standby outage')
                code, body = self.request('/prometheus/api/v1/alerts', 'prometheus')
                require(code == 200, 'Cannot read alerts')
                rows = [a for a in body['data']['alerts'] if a['labels']['alertname'] == 'HubuumScrapeUnavailable'
                        and a['labels']['instance'] == 'hubuum-api-standby']
                state = rows[0]['state'] if rows else 'inactive'
                if not events or events[-1]['state'] != state:
                    events.append({'state': state, 'time': time.time()})
                    print(f'  standby alert: {state}', flush=True)
                return state == 'firing'
            wait_for('the production scrape alert to fire', firing, timeout=450)
            require(any(e['state'] == 'pending' for e in events), 'Alert skipped pending state')
        finally:
            self.compose('start', 'hubuum-api-standby')
        self.healthy()
        wait_for('scrape alert recovery', lambda: not self.query('ALERTS{alertname="HubuumScrapeUnavailable",instance="hubuum-api-standby"}'))
        self.report['alert'] = {'transitions': events, 'recovered': True, 'primary_stayed_ready': True}

    def resources(self, kind):
        return set(self.run(self.engine, kind, 'ls', '--filter', f'label=com.docker.compose.project={self.project}', '--format', '{{.Name}}').split())

    def lifecycle(self):
        def step(name, script, *args):
            self.report['lifecycle_step'] = name
            print('  lifecycle: ' + name, flush=True)
            self.script(script, *args)

        at = time.time()
        history = canonical(self.query('up{job="hubuum"}', at))
        keys = ('GRAFANA_ADMIN_PASSWORD', 'GRAFANA_SECRET_KEY', 'PROMETHEUS_PASSWORD')
        secrets = {key: self.values()[key] for key in keys}
        volumes = self.resources('volume')
        code, _ = self.request('/grafana/api/dashboards/db', 'grafana', {'dashboard': {'uid': 'persistence-test', 'title': 'Persistence test', 'panels': [], 'schemaVersion': 39}})
        require(code == 200, 'Could not create persistence marker')
        def preserved():
            self.healthy()
            require({key: self.values()[key] for key in keys} == secrets, 'Monitoring credentials changed')
            require(self.request('/grafana/api/dashboards/uid/persistence-test', 'grafana')[0] == 200, 'Grafana data lost')
            require(canonical(self.query('up{job="hubuum"}', at)) == history, 'Prometheus history lost')
            require(self.resources('volume') == volumes, 'Data volumes changed')
            # Also catches a PGDATA/mount change that silently creates a fresh DB.
            require(self.compose('exec', '-T', 'postgres', 'psql', '-U', 'hubuum', '-d', 'hubuum', '-Atc', 'select count(*) from hubuumobject').strip() == '10', 'PostgreSQL inventory lost')
        step('update', 'update-single-host.sh')
        preserved()
        path = self.directory / '.env'
        path.write_text(path.read_text().replace('MONITORING_ENABLED=true', 'MONITORING_ENABLED=false'))
        step('disable', 'update-single-host.sh')
        services = self.run(self.engine, 'ps', '-a', '--filter', f'label=com.docker.compose.project={self.project}', '--format', '{{.Label "com.docker.compose.service"}}').split()
        require(not {'prometheus', 'grafana'}.intersection(services), 'Disabled monitoring containers remain')
        require(self.resources('volume') == volumes, 'Disabling monitoring deleted volumes')
        require('import monitoring' not in (self.directory / 'Caddyfile').read_text(), 'Disabled monitoring routes remain')
        step('re-enable', 'update-single-host.sh', '--monitoring')
        preserved()
        step('uninstall', 'uninstall-single-host.sh')
        require(self.directory.exists() and self.resources('volume') == volumes, 'Ordinary uninstall deleted data')
        step('reinstall', 'install-single-host.sh', '--no-pull')
        preserved()
        step('purge', 'uninstall-single-host.sh', '--purge')
        require(not self.directory.exists(), 'Purge left installation files')
        require(not self.resources('volume') and not self.resources('network'), 'Purge left volumes or networks')
        remaining_volumes = set(self.run(self.engine, 'volume', 'ls', '--format', '{{.Name}}').split())
        require(not self.owned_volumes.intersection(remaining_volumes), 'Purge left an anonymous volume')
        remaining = self.run(self.engine, 'ps', '-a', '--filter', f'label=com.docker.compose.project={self.project}', '--format', '{{.ID}}')
        require(not remaining.strip(), 'Purge left containers')
        self.report['lifecycle'] = 'update, disable/re-enable and uninstall/restart preserve credentials, database and history; purge removes all resources'

    def failure_diagnostics(self):
        """Retain useful failure evidence without publishing fixture credentials."""
        ids = self.compose('ps', '-aq').split()
        if not ids:
            return
        containers = json.loads(self.run(self.engine, 'inspect', *ids))
        self.report['containers'] = [
            {'service': c['Config']['Labels']['com.docker.compose.service'],
             'status': c['State']['Status'],
             'health': c['State'].get('Health', {}).get('Status'),
             'exit_code': c['State']['ExitCode'],
             'oom_killed': c['State']['OOMKilled'],
             'restart_count': c['RestartCount']}
            for c in containers]
        # Only startup failures need service logs. Later stages already report
        # query/health evidence and may have processed application payloads.
        if self.report.get('stage') != 'start':
            return
        secrets = {value.strip("'\"") for key, value in self.values().items()
                   if re.search('PASSWORD|SECRET|TOKEN|KEY|URL', key) and value}
        for container in containers:
            secrets.update(value for setting in container['Config']['Env']
                           for key, _, value in [setting.partition('=')]
                           if re.search('PASSWORD|SECRET|TOKEN|KEY|URL', key) and value)
        self.report['startup_logs'] = {}
        for container in containers:
            service = container['Config']['Labels']['com.docker.compose.service']
            if service not in ('hubuum-api', 'hubuum-api-standby', 'hubuum-restore-executor'):
                continue
            logs = self.compose('logs', '--no-color', '--tail', '30', service)
            for secret in sorted(secrets, key=len, reverse=True):
                logs = logs.replace(secret, '[redacted]')
            logs = re.sub(r'(?:postgres(?:ql)?|https?)://[^\s"<>]+', '[redacted URL]', logs)
            self.report['startup_logs'][service] = logs[-12000:]

    def cleanup(self):
        try:
            if (self.directory / 'compose.yml').exists():
                self.compose('down', '--volumes', '--remove-orphans')
        finally:
            # Also handle an interrupted purge that removed configuration before
            # finishing deletion. Only this run's unique project is owned.
            ids = self.run(self.engine, 'ps', '-aq', '--filter',
                           f'label=com.docker.compose.project={self.project}').split()
            if ids:
                self.run(self.engine, 'rm', '-fv', *ids)
            for kind in ('network', 'volume'):
                names = self.resources(kind)
                if kind == 'volume':
                    remaining = set(self.run(self.engine, 'volume', 'ls', '--format', '{{.Name}}').split())
                    names.update(self.owned_volumes.intersection(remaining))
                if names:
                    self.run(self.engine, kind, 'rm', *sorted(names))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='hubuum-server:ci', help='already-built production server image')
    parser.add_argument('--mode', choices=('backend', 'all'), default='backend')
    parser.add_argument('--report', type=Path, help='write non-secret JSON results, including the failing stage')
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix='hubuum-monitoring-acceptance-') as directory:
        installation = None
        report = {'stage': 'setup', 'result': 'failed'}
        try:
            installation = Installation(directory, args.image, args.mode)
            report = installation.report
            for stage in ('start', 'authenticate', 'data', 'traffic', 'panels', 'alert', 'lifecycle'):
                print('Monitoring acceptance: ' + stage, flush=True)
                installation.report['stage'] = stage
                getattr(installation, stage)()
            installation.report['result'] = 'passed'
        except BaseException:
            report['result'] = 'failed'
            if installation and (installation.directory / 'compose.yml').exists():
                try:
                    installation.failure_diagnostics()
                except (AssertionError, OSError, subprocess.TimeoutExpired):
                    pass
            raise
        finally:
            try:
                if installation:
                    installation.cleanup()
            finally:
                if args.report:
                    args.report.parent.mkdir(parents=True, exist_ok=True)
                    args.report.write_text(json.dumps(report, indent=2) + '\n')
        print('Real-server monitoring acceptance passed; all temporary resources purged.', flush=True)
