"""Disposable server-side browser test fixture. Never sends capabilities to JS."""
import argparse
import hmac
import secrets
import ssl
from http.cookies import SimpleCookie

import html
import http.client
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = None
OPTIONS = None
ASSETS = Path(__file__).resolve().parents[1] / "support" / "auth_browser"
BFF_SESSIONS = {}
RECORDS = []
CASES = {}
LOCK = threading.Lock()


def api(path, body=None, authorization=None, method='POST'):
    connection = http.client.HTTPConnection('127.0.0.1', OPTIONS.backend_port, timeout=40)
    headers = {'Content-Type': 'application/json'}
    if authorization:
        headers['Authorization'] = authorization
    connection.request(method, path, json.dumps(body) if body is not None else None, headers)
    response = connection.getresponse()
    raw = response.read()
    try:
        data = json.loads(raw)
    except ValueError:
        data = None
    connection.close()
    return response.status, data


def finish(case, action='complete', override=None):
    begun = case['begun']
    body = {'client_id': 'browser-qa'}
    if action == 'complete' and begun.get('operation'):
        body['operation'] = begun['operation']
    if override:
        body.update(override)
    status, result = api(f'/api/v1/auth/interactions/{begun["id"]}/{action}', body,
                         'Hubuum-Interaction ' + begun['redemption_secret'])
    case['last_result'] = result
    summary = {'action': action, 'status': status}
    if isinstance(result, dict):
        summary['result_keys'] = list(result)
        summary['reason'] = result.get('reason')
        if 'candidate' in result:
            summary['candidate_state'] = result['candidate'].get('state')
            summary['has_receipt'] = bool(result['candidate'].get('verification_receipt'))
        if 'access_token' in result:
            case['session'] = result
    case.setdefault('results', []).append(summary)
    save()
    return summary


def save():
    summaries = {name: {'id': case['begun']['id'], 'results': case.get('results', [])} for name, case in CASES.items()}
    (ROOT/'case-results.json').write_text(json.dumps(summaries, indent=2))


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, body, status=200, content_type='application/json', cookie=None):
        raw = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Length', str(len(raw)))
        if cookie:
            self.send_header('Set-Cookie', cookie)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        self.dispatch()

    def do_POST(self):
        self.dispatch()

    def do_OPTIONS(self):
        self.dispatch()

    def dispatch(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        control = self.server.server_port == OPTIONS.control_port
        if control and (self.headers.get('Origin') or self.headers.get('Sec-Fetch-Site') or self.headers.get('Host') != f'127.0.0.1:{OPTIONS.control_port}'):
            return self.reply('Server-to-server control only', 403, 'text/plain')
        if self.path.startswith('/control/') and not control:
            return self.reply('Not found', 404, 'text/plain')
        if control and not self.path.startswith('/control/'):
            return self.reply('Not found', 404, 'text/plain')
        if self.path == '/__qa':
            return self.reply((ASSETS/'harness.html').read_text().replace('@@ATTACKER@@', OPTIONS.attacker_origin), content_type='text/html')
        if self.path == '/__qa/records':
            return self.reply(RECORDS)
        if self.path == '/__qa/results':
            data = json.loads(raw)
            name = 'chromium' if 'Chrome/' in data['user_agent'] else 'safari'
            (ROOT/f'{name}-transport.json').write_text(json.dumps(data, indent=2))
            return self.reply({'saved': True})
        if self.path.startswith('/control/'):
            body = json.loads(raw or b'{}')
            if self.path == '/control/begin':
                name = body.pop('case')
                privileged = body.get('purpose', 'login') != 'login'
                credentials = json.loads((ROOT/'admin.json').read_text())
                request = {'client_id':'browser-qa','provider_id':'local','purpose':'login','delivery':'poll','interaction_mode':'browser', **body}
                status, begun = api('/api/v1/auth/interactions', request, 'Bearer '+credentials['token'] if privileged else None)
                if status != 201:
                    return self.reply({'status': status, 'error': begun}, status)
                CASES[name] = {'begun': begun, 'return_nonce': request.get('return_nonce')}
                save()
                return self.reply({'case':name,'id':begun['id'],'next':begun['next']})
            if self.path == '/control/action':
                return self.reply(finish(CASES[body['case']], body.get('action','complete'), body.get('override')))
            if self.path == '/control/private-result':
                return self.reply(CASES[body['case']].get('last_result'))
            if self.path == '/control/results':
                return self.reply(json.loads((ROOT/'case-results.json').read_text()))
        if self.path.startswith('/start?'):
            name = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get('case', [''])[0]
            case = CASES.get(name)
            if not case or not case['return_nonce']:
                return self.reply('Unknown BFF case', 404, 'text/plain')
            session = secrets.token_urlsafe(32)
            BFF_SESSIONS[session] = name
            next_step = case['begun']['next']
            return self.reply('<!doctype html><title>BFF test start</title><h1>BFF session started</h1><p>Code: '
                              + html.escape(next_step['user_code']) + '</p><a href="'
                              + html.escape(next_step['verification_uri'], quote=True) + '">Open hosted sign-in</a>',
                              content_type='text/html', cookie=f'__Host-qa-bff={session}; Secure; HttpOnly; SameSite=Lax; Path=/; Max-Age=600')
        if self.path.startswith('/callback?'):
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            cookies = SimpleCookie(self.headers.get('Cookie', ''))
            session = cookies.get('__Host-qa-bff')
            name = BFF_SESSIONS.get(session.value) if session else None
            case = CASES.get(name)
            if (set(params) != {'interaction_id', 'nonce'} or not case
                    or params['interaction_id'] != [case['begun']['id']]
                    or len(params['nonce']) != 1
                    or not hmac.compare_digest(params['nonce'][0], case['return_nonce'])):
                return self.reply('Invalid callback session or nonce', 400, 'text/plain')
            del BFF_SESSIONS[session.value]
            result = finish(case)
            return self.reply('<!doctype html><title>BFF test result</title><h1>BFF handoff verified</h1><p>Only interaction ID and a nonce bound to this browser session reached the callback.</p><pre>'
                              + html.escape(json.dumps(result, indent=2)) + '</pre>', content_type='text/html')
        if self.path == '/cookie-probe':
            names = sorted(SimpleCookie(self.headers.get('Cookie', '')).keys())
            return self.reply('<!doctype html><title>Cookie probe</title><h1>Cookie names received</h1><pre>'
                              + html.escape(json.dumps(names)) + '</pre>', content_type='text/html')
        connection = http.client.HTTPConnection('127.0.0.1', OPTIONS.backend_port, timeout=40)
        headers = {k:v for k,v in self.headers.items() if k.lower() not in {'connection','transfer-encoding'}}
        connection.request(self.command, self.path, raw, headers)
        response = connection.getresponse()
        response_headers = response.getheaders()
        content = response.read()
        record = {'method':self.command,'path':self.path,'status':response.status,
                  'origin':self.headers.get('Origin'),'sec_fetch_site':self.headers.get('Sec-Fetch-Site'),
                  'sec_fetch_mode':self.headers.get('Sec-Fetch-Mode'),
                  'response_headers':{k.lower():v for k,v in response_headers if k.lower() != 'set-cookie'},
                  'cookie_names': sorted(SimpleCookie(self.headers.get('Cookie', '')).keys()),
                  'cookie_attributes': [v.split(';')[1:] for k,v in response_headers if k.lower() == 'set-cookie'],
                  'cookie_set':any(k.lower() == 'set-cookie' for k,v in response_headers),
                  'secret_in_response':any(secret.encode() in content for case in CASES.values() for secret in [case['begun']['redemption_secret']])}
        with LOCK:
            RECORDS.append(record)
            with (ROOT/'requests.jsonl').open('a') as out:
                out.write(json.dumps(record)+'\n')
        self.send_response(response.status)
        for key,value in response_headers:
            if key.lower() not in {'connection','transfer-encoding','content-length'}:
                self.send_header(key,value)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)
        connection.close()

def main(argv=None):
    """Serve disposable HTTPS browser fixtures; never use against a real database."""
    global ROOT, OPTIONS
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--certificate', required=True)
    parser.add_argument('--key', required=True)
    parser.add_argument('--attacker-origin', required=True)
    parser.add_argument('--backend-port', type=int, default=8590)
    parser.add_argument('--tls-ports', type=int, nargs='+', default=[8591, 8592, 8593])
    parser.add_argument('--control-port', type=int, default=8595)
    parser.add_argument('--http-probe-port', type=int, default=8594)
    OPTIONS = parser.parse_args(argv)
    ROOT = OPTIONS.state_dir
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    if ROOT.stat().st_mode & 0o077:
        parser.error('State directory must have mode 0700')
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(OPTIONS.certificate, OPTIONS.key)
    servers = []
    try:
        for port in [*OPTIONS.tls_ports, OPTIONS.control_port, OPTIONS.http_probe_port]:
            server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
            if port in OPTIONS.tls_ports:
                server.socket = context.wrap_socket(server.socket, server_side=True)
            servers.append(server)
            threading.Thread(target=server.serve_forever, daemon=True).start()
        print('Browser fixture ready on loopback; TLS certificate validation is required.', flush=True)
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        for server in servers:
            server.shutdown()
            server.server_close()
    return 0
