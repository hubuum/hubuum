"""Disposable verified-TLS effect counter for real remote-task recovery probes."""

import contextlib
import http.server
import json
import ssl
import subprocess
import threading
import urllib.parse


class EffectFixture:
    def __init__(self, directory):
        self.directory = directory
        self.counts = {}
        self.response_gates = {}
        self.lock = threading.Lock()
        self.servers = []
        self.threads = []

    def hold_response(self, probe):
        if not isinstance(probe, str) or not 1 <= len(probe) <= 128:
            raise ValueError("Invalid synthetic effect probe")
        with self.lock:
            if probe in self.counts or probe in self.response_gates or len(self.response_gates) >= 8:
                raise ValueError("Effect response gate must be unique and bounded")
            self.response_gates[probe] = threading.Event()

    def release_response(self, probe):
        with self.lock:
            self.response_gates[probe].set()

    def __enter__(self):
        self.directory.mkdir(mode=0o700)
        self.certificate = self.directory / "ca.pem"
        ca_key = self.directory / "ca.key"
        server_certificate = self.directory / "server.pem"
        key = self.directory / "server.key"
        request = self.directory / "server.csr"
        extensions = self.directory / "server.ext"
        extensions.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            "subjectAltName=DNS:retention-effect,DNS:localhost\n",
            encoding="ascii",
        )
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
            "-subj", "/CN=Hubuum retention fixture CA",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
            "-keyout", str(ca_key), "-out", str(self.certificate),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        subprocess.run([
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
            "-subj", "/CN=retention-effect", "-keyout", str(key), "-out", str(request),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        subprocess.run([
            "openssl", "x509", "-req", "-in", str(request), "-CA", str(self.certificate),
            "-CAkey", str(ca_key), "-set_serial", "1", "-days", "2",
            "-extfile", str(extensions), "-out", str(server_certificate),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        ca_key.chmod(0o600)
        key.chmod(0o600)
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def respond(self, status, value):
                content = json.dumps(value).encode()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError, ssl.SSLError):
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length", "0"))
                    if not 0 < length <= 65536:
                        raise ValueError("bounded request required")
                    body = json.loads(self.rfile.read(length))
                    probe = body["probe"]
                    if not isinstance(probe, str) or not 1 <= len(probe) <= 128:
                        raise ValueError("invalid probe")
                except (ValueError, KeyError, TypeError):
                    self.respond(400, {"error": "invalid synthetic probe"})
                    return
                with fixture.lock:
                    fixture.counts[probe] = fixture.counts.get(probe, 0) + 1
                    gate = fixture.response_gates.get(probe)
                if gate is not None and not gate.wait(900):
                    self.respond(504, {"error": "synthetic response gate expired"})
                    return
                self.respond(200, {"received": True})

            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                probe = query.get("probe", [""])[0]
                with fixture.lock:
                    count = fixture.counts.get(probe, 0)
                self.respond(200, {"count": count})

        try:
            for address, tls in (("0.0.0.0", True), ("127.0.0.1", False)):
                server = http.server.ThreadingHTTPServer((address, 0), Handler)
                server.daemon_threads = True
                self.servers.append(server)
                if tls:
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    context.load_cert_chain(server_certificate, key)
                    server.socket = context.wrap_socket(server.socket, server_side=True)
                    self.remote_url = f"https://retention-effect:{server.server_port}/hook"
                else:
                    self.counter_url = f"http://127.0.0.1:{server.server_port}/count"
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                self.threads.append(thread)
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        with self.lock:
            for gate in self.response_gates.values():
                gate.set()
        for index, server in enumerate(self.servers):
            if index < len(self.threads):
                server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join()
