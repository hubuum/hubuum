#!/usr/bin/env python3
"""Run production event adapters against disposable, verified-TLS fixtures."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import base64
import contextlib
import http.server
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
RABBITMQ = (
    "docker.io/library/rabbitmq:4.2-management@sha256:"
    "b05cfa8ce8177ee3a27654ae3c4c855e57ed727bda931a45ea220af2f3f43386"
)
VALKEY = (
    "docker.io/valkey/valkey:9-alpine@sha256:"
    "ee91f7a174ac4d6a6b0685b3a60e321f0a9dbbb691f9b0e285be2ba1d1be8328"
)
CARGO_TEST = (
    "cargo", "test", "--locked", "--features", "production-all",
    "--test", "event_transport_contract",
)


def command(*args, timeout=120):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        # Commands can contain fixture credentials. Only identify the operation.
        raise RuntimeError(f"{args[0]} {args[1]} failed (exit {result.returncode})")
    return result.stdout.strip()


def certificates(directory):
    directory.mkdir()
    directory.chmod(0o755)
    command(
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
        "-subj", "/CN=Hubuum disposable contract CA", "-keyout", str(directory / "ca.key"),
        "-out", str(directory / "ca.pem"), "-addext", "basicConstraints=critical,CA:TRUE",
    )
    command(
        "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
        "-subj", "/CN=localhost", "-keyout", str(directory / "server.key"),
        "-out", str(directory / "server.csr"),
    )
    extensions = directory / "extensions.cnf"
    extensions.write_text(
        "subjectAltName=DNS:localhost,IP:127.0.0.1\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature,keyEncipherment\n"
        "extendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    command(
        "openssl", "x509", "-req", "-in", str(directory / "server.csr"),
        "-CA", str(directory / "ca.pem"), "-CAkey", str(directory / "ca.key"),
        "-CAcreateserial", "-days", "2", "-extfile", str(extensions),
        "-out", str(directory / "server.pem"),
    )
    # The outer temporary directory is private; container service users need
    # read access to the disposable leaf key inside their read-only mount.
    for filename in ("ca.pem", "server.pem", "server.key"):
        (directory / filename).chmod(0o644)


class HttpsFixture(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, directory):
        super().__init__(("127.0.0.1", 0), HttpsHandler)
        self.observed = {}
        self.observed_lock = threading.Lock()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(directory / "server.pem", directory / "server.key")
        self.socket = context.wrap_socket(self.socket, server_side=True)


class HttpsHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, status, body, location=None):
        with contextlib.suppress(BrokenPipeError, ConnectionResetError, ssl.SSLError):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", "application/json")
            if location:
                self.send_header("Location", location)
            self.end_headers()
            self.wfile.write(body)

    def do_GET(self):
        _, behavior, identity = self.path.split("/", 2)
        with self.server.observed_lock:
            if behavior == "observed":
                result = list(self.server.observed.get(identity, []))
            else:
                self.server.observed.setdefault(identity, []).append(behavior)
                result = []
        self.reply(200, json.dumps(result).encode())

    def do_POST(self):
        _, behavior, identity = self.path.split("/", 2)
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_000_000:
            self.reply(413, b"{}")
            return
        self.rfile.read(length)
        with self.server.observed_lock:
            self.server.observed.setdefault(identity, []).append(behavior)
        if behavior == "redirect":
            self.reply(307, b"{}", f"/destination/{identity}")
        elif behavior == "retry":
            self.reply(503, b"{}")
        elif behavior == "oversized":
            self.reply(200, b"x" * 2048)
        elif behavior == "slow":
            time.sleep(1)
            self.reply(200, b"{}")
        else:
            self.reply(200, b"{}")


def mapped_port(container, port):
    return int(command("docker", "port", container, str(port)).rsplit(":", 1)[1])


def loopback_ports(count):
    # Select distinct unused ports, then publish those explicit numbers. Docker
    # may allocate new host ports after a restart when the mapping omits them.
    with contextlib.ExitStack() as sockets:
        listeners = [sockets.enter_context(socket.socket()) for _ in range(count)]
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]


def wait_until_ready(probe, label):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            probe()
            return
        except (RuntimeError, OSError, urllib.error.URLError):
            time.sleep(0.5)
    raise RuntimeError(f"{label} fixture did not become ready within 120 seconds")


def run(root, containers, servers, networks):
    tls = root / "tls"
    untrusted = root / "untrusted"
    certificates(tls)
    certificates(untrusted)
    password = "fixture:p@ss/" + secrets.token_hex(12)
    suffix = secrets.token_hex(6)
    rabbit = "hubuum-transport-contract-amqp-" + suffix
    valkey = "hubuum-transport-contract-valkey-" + suffix
    network = "hubuum-transport-contract-" + suffix
    config = root / "rabbitmq.conf"
    config.write_text(
        "listeners.tcp = none\nlisteners.ssl.default = 5671\n"
        "ssl_options.cacertfile = /fixtures/ca.pem\n"
        "ssl_options.certfile = /fixtures/server.pem\n"
        "ssl_options.keyfile = /fixtures/server.key\n"
        "ssl_options.verify = verify_none\nssl_options.fail_if_no_peer_cert = false\n",
        encoding="utf-8",
    )
    config.chmod(0o644)
    for image in (RABBITMQ, VALKEY):
        print(f"Pinned fixture: {image}", flush=True)
        command("docker", "pull", image, timeout=300)
    # Keep the network namespace alive while individual services restart. This
    # also avoids rebinding rootless Podman's host forwarder during recovery.
    networks.append(network)
    command("docker", "network", "create", network)
    amqp_port, management_port, valkey_port = loopback_ports(3)
    containers.append(rabbit)
    command(
        "docker", "run", "--detach", "--name", rabbit,
        "--network", network,
        "--publish", f"127.0.0.1:{amqp_port}:5671",
        "--publish", f"127.0.0.1:{management_port}:15672",
        "--volume", f"{tls}:/fixtures:ro,z", "--volume", f"{config}:/etc/rabbitmq/rabbitmq.conf:ro,Z",
        "--env", "RABBITMQ_DEFAULT_USER=contract", "--env", f"RABBITMQ_DEFAULT_PASS={password}",
        RABBITMQ,
    )
    containers.append(valkey)
    command(
        "docker", "run", "--detach", "--name", valkey,
        "--publish", f"127.0.0.1:{valkey_port}:6379",
        "--network", network,
        "--volume", f"{tls}:/fixtures:ro,z", VALKEY, "valkey-server", "--port", "0",
        "--tls-port", "6379", "--tls-cert-file", "/fixtures/server.pem",
        "--tls-key-file", "/fixtures/server.key", "--tls-ca-cert-file", "/fixtures/ca.pem",
        "--tls-auth-clients", "no", "--requirepass", password,
    )
    management = f"http://127.0.0.1:{mapped_port(rabbit, 15672)}"
    authorization = base64.b64encode(f"contract:{password}".encode()).decode()

    def rabbit_ready():
        request = urllib.request.Request(
            management + "/api/overview", headers={"Authorization": "Basic " + authorization}
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            if response.status != 200:
                raise RuntimeError("RabbitMQ management not ready")

    wait_until_ready(rabbit_ready, "RabbitMQ")
    def valkey_ready():
        reply = command(
            "docker", "exec", valkey, "valkey-cli", "--tls", "--cacert", "/fixtures/ca.pem",
            "--no-auth-warning", "-a", password, "ping", timeout=5,
        )
        if reply != "PONG":
            raise RuntimeError("Valkey did not acknowledge the authenticated TLS probe")

    wait_until_ready(valkey_ready, "Valkey")
    for directory in (tls, untrusted):
        server = HttpsFixture(directory)
        servers.append(server)
        threading.Thread(target=server.serve_forever, daemon=True).start()
    empty_roots = root / "empty-roots"
    empty_roots.mkdir()
    env = dict(os.environ)
    valkey_port = mapped_port(valkey, 6379)
    env.update({
        "SSL_CERT_FILE": str(tls / "ca.pem"), "SSL_CERT_DIR": str(empty_roots),
        "HUBUUM_CONTRACT_PASSWORD": password,
        "HUBUUM_CONTRACT_AMQP_URI": f"amqps://contract:{{secret}}@127.0.0.1:{mapped_port(rabbit, 5671)}/%2f",
        "HUBUUM_CONTRACT_AMQP_MANAGEMENT": management,
        "HUBUUM_CONTRACT_AMQP_CONTAINER": rabbit,
        "HUBUUM_CONTRACT_VALKEY_URI": f"rediss://:{{secret}}@127.0.0.1:{valkey_port}/0",
        "HUBUUM_CONTRACT_VALKEY_INSPECT_URI": f"rediss://:{urllib.parse.quote(password, safe='')}@127.0.0.1:{valkey_port}/0",
        "HUBUUM_CONTRACT_VALKEY_CONTAINER": valkey,
        "HUBUUM_CONTRACT_HTTPS_URL": f"https://127.0.0.1:{servers[0].server_port}",
        "HUBUUM_CONTRACT_UNTRUSTED_HTTPS_URL": f"https://127.0.0.1:{servers[1].server_port}",
    })
    # Match production features. Missing fixtures panic in the Rust suite; it
    # cannot pass by skipping individual cases when an environment URL is absent.
    result = subprocess.run(
        [*CARGO_TEST, "--", "--ignored", "--test-threads=1"],
        cwd=ROOT, env=env, timeout=1800, check=False,
    )
    return result.returncode


def main():
    # Build with the normal system trust store before selecting the fixture CA.
    build = subprocess.run([*CARGO_TEST, "--no-run"], cwd=ROOT, timeout=1800, check=False)
    if build.returncode:
        return build.returncode
    containers = []
    servers = []
    networks = []
    with tempfile.TemporaryDirectory(prefix="hubuum-transport-contract-") as temporary:
        try:
            return run(Path(temporary), containers, servers, networks)
        finally:
            for server in servers:
                server.shutdown()
                server.server_close()
            for container in reversed(containers):
                subprocess.run(
                    ["docker", "rm", "--force", "--volumes", container],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=False,
                )
            for network in networks:
                subprocess.run(
                    ["docker", "network", "rm", network],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=False,
                )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.TimeoutExpired:
        sys.exit("Event transport fixture or test command exceeded its deadline")
    except RuntimeError as error:
        sys.exit(str(error))
