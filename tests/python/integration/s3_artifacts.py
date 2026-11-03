"""Run Rust S3 contracts against a pinned real MinIO server with private TLS."""
import argparse
import hashlib
import os
from pathlib import Path
import platform
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from support import ROOT

RELEASE = "RELEASE.2025-09-07T16-13-09Z"
# GitHub release API asset `digest` fields, not a checksum fetched beside a
# mutable download. These pin the actual executable bytes before chmod/exec.
DIGESTS = {
    ("Linux", "x86_64"): ("linux-amd64", "7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f"),
    ("Linux", "aarch64"): ("linux-arm64", "5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d"),
    ("Darwin", "arm64"): ("darwin-arm64", "7c3b3039b76e55a1b80935848ed83998d5e8d317374f87851f46a019ff5c0aa4"),
    ("Darwin", "x86_64"): ("darwin-amd64", "4759080aeef7385aaceaac1131c30aaeb99605921553967dc6d3ef4e16ac64f9"),
}


def fixture_identity(system=None, machine=None):
    key = (system or platform.system(), machine or platform.machine())
    try:
        target, digest = DIGESTS[key]
    except KeyError as error:
        raise ValueError(f"S3 fixture requires supported Linux/macOS, found {key}") from error
    name = f"minio.{target}.{RELEASE}"
    return f"https://github.com/minio/minio/releases/download/{RELEASE}/{name}", digest


def verified_binary(directory):
    url, expected = fixture_identity()
    path = directory / "minio"
    digest = hashlib.sha256()
    with urllib.request.urlopen(url, timeout=60) as response, path.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
            digest.update(chunk)
    if digest.hexdigest() != expected:
        path.unlink()
        raise ValueError("MinIO executable SHA-256 mismatch; refusing to execute")
    path.chmod(0o700)
    print(f"S3 fixture: {url} sha256:{expected}", flush=True)
    return path


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Fixture:
    def __init__(self, directory, binary):
        self.directory = directory
        self.binary = binary
        self.port = free_port()
        self.process = None
        self.log = (directory / "minio.log").open("wb")
        self.certs = directory / "certs"
        self.certs.mkdir()
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(directory / "ca.key"), "-out", str(directory / "ca.crt"),
            "-days", "1", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext", "basicConstraints=critical,CA:TRUE",
            "-addext", "keyUsage=critical,keyCertSign,cRLSign",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([
            "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
            "-keyout", str(self.certs / "private.key"), "-out", str(directory / "server.csr"),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        extensions = directory / "server.ext"
        extensions.write_text("basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\nauthorityKeyIdentifier=keyid,issuer\n")
        subprocess.run([
            "openssl", "x509", "-req", "-in", str(directory / "server.csr"), "-CA", str(directory / "ca.crt"),
            "-CAkey", str(directory / "ca.key"), "-CAcreateserial", "-days", "1", "-extfile", str(extensions),
            "-out", str(self.certs / "public.crt"),
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (self.certs / "private.key").chmod(0o600)
        self.tls = ssl.create_default_context(cafile=str(directory / "ca.crt"))

    @property
    def endpoint(self):
        return f"https://localhost:{self.port}"

    def start(self):
        if self.process is not None:
            raise RuntimeError("MinIO already started")
        env = dict(os.environ, MINIO_ROOT_USER="hubuum-fixture", MINIO_ROOT_PASSWORD="hubuum-fixture-secret",
                   MINIO_BROWSER="off", MINIO_UPDATE="off")
        self.process = subprocess.Popen([
            str(self.binary), "--certs-dir", str(self.certs), "server",
            "--address", f"127.0.0.1:{self.port}", str(self.directory / "data"),
        ], env=env, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 30
        last_error = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("MinIO exited before TLS readiness")
            try:
                with urllib.request.urlopen(self.endpoint + "/minio/health/live", context=self.tls, timeout=1):
                    return
            except (urllib.error.URLError, TimeoutError) as error:
                last_error = error
                time.sleep(0.1)
        self.log.flush()
        raise RuntimeError(f"MinIO TLS readiness deadline exceeded: {last_error}; "
                           + (self.directory / "minio.log").read_text()[-4000:])

    def stop(self):
        if self.process is not None:
            self.process.kill()
            self.process.wait(timeout=10)
            self.process = None

    def close(self):
        self.stop()
        self.log.close()

    def provision_benchmark_bucket(self):
        # Fixture provisioning only; production S3 operations remain in Rust.
        common = ["curl", "--fail", "--silent", "--show-error", "--noproxy", "*",
                  "--cacert", str(self.directory / "ca.crt"), "--aws-sigv4", "aws:amz:us-east-1:s3",
                  "--user", "hubuum-fixture:hubuum-fixture-secret", "-X", "PUT"]
        subprocess.run([*common, "-H", "x-amz-bucket-object-lock-enabled: true",
                        self.endpoint + "/hubuum-artifact-bench"], check=True)
        subprocess.run([*common, "--data-binary",
                        '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>',
                        self.endpoint + "/hubuum-artifact-bench?versioning"], check=True)

    def run(self, command):
        env = dict(os.environ, HUBUUM_TEST_S3_ENDPOINT=self.endpoint,
                   HUBUUM_TEST_S3_CA=str(self.directory / "ca.crt"), HUBUUM_TEST_S3_CONTROL=str(self.directory))
        process = subprocess.Popen(command, cwd=ROOT, env=env)
        try:
            deadline = time.monotonic() + 1800
            while process.poll() is None:
                request = self.directory / "request"
                if request.exists():
                    action = request.read_text()
                    request.unlink()
                    if action not in {"stop", "start", "restart"}:
                        raise ValueError("Unknown fixture lifecycle request")
                    if action in {"stop", "restart"}:
                        self.stop()
                    if action in {"start", "restart"}:
                        self.start()
                    (self.directory / "ack").write_text(action)
                if time.monotonic() >= deadline:
                    raise RuntimeError("S3 acceptance command exceeded 30 minutes")
                time.sleep(0.05)
            if process.returncode:
                raise RuntimeError(f"S3 acceptance command failed with exit {process.returncode}")
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", action="store_true", help="also measure real S3 write/verify/publish and read flows")
    parser.add_argument("--filter", default="s3::tests::live_", help="Rust test filter for focused fixture diagnostics")
    parser.add_argument("--benchmark-executable", type=Path, help="run an already built benchmark inside the fixture")
    parser.add_argument("benchmark_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if not shutil.which("openssl"):
        raise ValueError("openssl is required for private TLS fixture certificates")
    with tempfile.TemporaryDirectory(prefix="hubuum-s3-") as temporary:
        directory = Path(temporary)
        binary = verified_binary(directory)
        fixture = Fixture(directory, binary)
        try:
            fixture.start()
            if args.benchmark_executable:
                fixture.provision_benchmark_bucket()
                arguments = args.benchmark_arguments
                if arguments[:1] == ["--"]:
                    arguments = arguments[1:]
                fixture.run([str(args.benchmark_executable.resolve()), *arguments])
            else:
                fixture.run(["cargo", "test", "--locked", "-p", "hubuum-artifacts", "--features", "s3", "--lib",
                             args.filter, "--", "--ignored", "--test-threads=1", "--nocapture"])
            if args.benchmark:
                fixture.run(["cargo", "bench", "--locked", "-p", "hubuum-artifacts", "--features", "s3",
                             "--bench", "s3_artifact_storage_criterion", "--", "--noplot"])
        finally:
            fixture.close()
    return 0
