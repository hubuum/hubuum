"""Disposable production-image OAuth qualification; no personal secrets in CI.

The synthetic provider is deliberately different from GitHub: Basic client
authentication, nested string subjects, explicit headers and profile groups.
The optional GitHub mode pauses for an authorized human browser sign-in.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import secrets
import ssl
import sys
import threading
import time
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from integration.keycloak import Browser, Fixture as BaseFixture, Forms, POSTGRES, command


class Provider(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, status, document):
        body = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        fixture = self.server.fixture
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/authorize":
            query = urllib.parse.parse_qs(parsed.query)
            if (query.get("client_id") != ["fixture"] or query.get("code_challenge_method") != ["S256"]
                    or query.get("response_type") != ["code"] or query.get("redirect_uri") != [fixture.callback]):
                return self.reply(400, {"error": "invalid_request"})
            code = secrets.token_urlsafe(32)
            fixture.codes[code] = query["code_challenge"][0]
            self.send_response(302)
            self.send_header("Location", fixture.callback + "?" + urllib.parse.urlencode({
                "state": query["state"][0], "code": code}))
            self.end_headers()
        elif parsed.path == "/account":
            if (self.headers.get("Authorization") not in fixture.access_tokens
                    or self.headers.get("X-Profile-Version") != "2"):
                return self.reply(401, {"error": "invalid_token"})
            self.reply(200, {"account": {"key": "Case:001", "display": fixture.display_name},
                             "roles": ["staff"]})
        else:
            self.reply(404, {})

    def do_POST(self):
        fixture = self.server.fixture
        if self.path != "/token":
            return self.reply(404, {})
        raw = self.rfile.read(min(int(self.headers.get("Content-Length", "0")), 16384))
        form = urllib.parse.parse_qs(raw.decode())
        expected = "Basic " + base64.b64encode(("fixture:" + fixture.client_secret).encode()).decode()
        challenge = fixture.codes.pop(form.get("code", [""])[0], None)
        received = base64.urlsafe_b64encode(hashlib.sha256(form.get("code_verifier", [""])[0].encode()).digest()).decode().rstrip("=")
        if (not hmac.compare_digest(self.headers.get("Authorization", ""), expected)
                or challenge is None or not hmac.compare_digest(challenge, received)
                or form.get("redirect_uri") != [fixture.callback]
                or form.get("grant_type") != ["authorization_code"]):
            return self.reply(400, {"error": "invalid_grant"})
        access = secrets.token_urlsafe(32)
        fixture.access_tokens.add("Bearer " + access)
        self.reply(200, {"access_token": access, "token_type": "Bearer", "scope": "identity"})


class Fixture(BaseFixture):
    def __init__(self, image, evidence, provider_port, server_port, github_client=None, secret_file=None):
        super().__init__(image, evidence, provider_port, server_port)
        self.github_client = github_client
        if github_client:
            self.client_secret = secret_file.read_text().strip()
        self.codes = {}
        self.access_tokens = set()
        self.display_name = "Synthetic Person"
        self.provider_http = None

    @property
    def callback(self):
        return self.origin + "/api/v1/auth/callbacks/" + self.provider_id

    def run(self, name, *arguments):
        name = self.prefix + "-" + name
        command("docker", "run", "--detach", "--name", name, "--label", "codex.task=hubuum-578", *arguments)
        self.containers.append(name)
        return name

    def start(self):
        mount = self.certificates()
        extensions = self.root / "extensions"
        extensions.write_text(extensions.read_text().replace("DNS:localhost,", "DNS:localhost,DNS:host.docker.internal,"))
        command("openssl", "x509", "-req", "-in", str(self.root / "server.csr"), "-CA", str(mount / "ca.pem"),
                "-CAkey", str(self.root / "ca.key"), "-CAcreateserial", "-days", "2", "-extfile", str(extensions),
                "-out", str(mount / "server.pem"))
        command("docker", "network", "create", "--label", "codex.task=hubuum-578", self.network)
        password = secrets.token_urlsafe(32)
        self.database = self.run("postgres", "--network", self.network, "--network-alias", "postgres",
            "--tmpfs", "/var/lib/postgresql", "--env-file", self.env("postgres", {
                "POSTGRES_PASSWORD": password, "POSTGRES_DB": "hubuum"}), POSTGRES)
        deadline = time.monotonic() + 60
        while True:
            try:
                command("docker", "exec", self.database, "pg_isready", "-U", "postgres")
                break
            except RuntimeError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)
        (mount / "outbound-ca.pem").write_text(command("docker", "run", "--rm", "--entrypoint", "cat", self.image,
            "/etc/ssl/certs/ca-certificates.crt") + "\n" + (mount / "ca.pem").read_text())
        server_env = self.env("server", {
            "HUBUUM_DATABASE_URL": f"postgres://postgres:{password}@postgres/hubuum",
            "HUBUUM_BIND_IP": "0.0.0.0", "HUBUUM_BIND_PORT": self.server_port,
            "HUBUUM_CLIENT_ALLOWLIST": "*", "HUBUUM_TLS_CERT_PATH": "/fixture/server.pem",
            "HUBUUM_TLS_KEY_PATH": "/fixture/server.key", "SSL_CERT_FILE": "/fixture/outbound-ca.pem",
            "HUBUUM_AUTH_RUNTIME_ALLOW_PRIVATE_TARGETS": "true",
            "HUBUUM_AUTH_BROWSER": json.dumps({"origin": self.origin, "return_targets": [
                {"id": "bff", "client_id": "oidc-qa", "uri": self.origin + "/fixture-callback"}]}),
            "HUBUUM_OAUTH_SECRET_COMPANY": self.client_secret,
            "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF": "fixture",
            "HUBUUM_RUNTIME_SETTINGS_KEY_FIXTURE": secrets.token_hex(16),
            "HUBUUM_TOKEN_HASH_KEY": secrets.token_urlsafe(48)})
        command("docker", "run", "--rm", "--network", self.network, "--env-file", server_env,
                "--mount", f"type=bind,src={mount},dst=/fixture,readonly", "--entrypoint", "hubuum-admin", self.image, "--migrate")
        self.server = self.run("server", "--network", self.network,
            "--add-host", "host.docker.internal:host-gateway", "--publish", f"127.0.0.1:{self.server_port}:{self.server_port}",
            "--env-file", server_env, "--mount", f"type=bind,src={mount},dst=/fixture,readonly", self.image)
        self.wait(self.origin + "/healthz")
        output = command("docker", "exec", self.server, "hubuum-admin", "--reset-password", "admin")
        self.local_password = output.rsplit("reset to: ", 1)[1].splitlines()[0]
        self.admin = self.api("/api/v1/auth/login", {"name": "admin", "password": self.local_password,
                                                   "client_id": "oidc-qa"})["access_token"]
        if not self.github_client:
            self.provider_http = ThreadingHTTPServer(("0.0.0.0", self.provider_port), Provider)
            self.provider_http.fixture = self
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(mount / "server.pem", mount / "server.key")
            self.provider_http.socket = tls.wrap_socket(self.provider_http.socket, server_side=True)
            threading.Thread(target=self.provider_http.serve_forever, daemon=True).start()
        identity = {"profile": "github" if self.github_client else "synthetic-json-user", "database": POSTGRES,
            "server": json.loads(command("docker", "image", "inspect", self.image))[0]["Id"],
            "source_commit": command("git", "rev-parse", "HEAD"),
            "source_dirty": bool(command("git", "status", "--porcelain")),
            "fixture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        (self.evidence / "images.json").write_text(json.dumps(identity, indent=2) + "\n")

    def authorize(self, interaction):
        if self.github_client:
            print(f"Open {interaction['next']['verification_uri']} and enter {interaction['next']['user_code']}.", flush=True)
            print("Complete GitHub sign-in and explicitly approve the Hubuum request. The fixture waits up to 5 minutes.", flush=True)
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline:
                status, _, raw = self.browser.request(self.origin + f"/api/v1/auth/interactions/{interaction['id']}/status", {"client_id": "oidc-qa"},
                    headers={"Authorization": "Hubuum-Interaction " + interaction["redemption_secret"]})
                if status == 200 and json.loads(raw).get("state") == "ready":
                    return
                time.sleep(5)
            raise AssertionError("Authorized GitHub browser qualification did not finish")
        browser = Browser(self.root / "mount" / "ca.pem")
        url, status, page = browser.follow(interaction["next"]["verification_uri"])
        assert status == 200
        url, status, page = browser.submit(url, page, {"user_code": interaction["next"]["user_code"]})
        url, status, page = browser.submit(url, page)
        links = Forms(page).links
        assert len(links) == 1, "Provider redirect missing"
        url, status, page = browser.follow(links[0])
        assert status == 200 and "Approve this request?" in page, f"OAuth callback failed (status {status})"
        browser.submit(url, page, {"decision": "approve"})

    def qualify(self):
        if self.github_client:
            definition = {"profile": "github", "authorization_endpoint": "https://github.com/login/oauth/authorize",
                "token_endpoint": "https://github.com/login/oauth/access_token", "profile_endpoint": "https://api.github.com/user",
                "client_id": self.github_client, "client_authentication": "client_secret_post", "scopes": [],
                "subject_pointer": "/id", "subject_type": "integer", "name_pointer": "/login",
                "membership": {"mode": "local_only", "refresh_interval_seconds": 5, "max_stale_seconds": 15}}
        else:
            definition = {"profile": "json_user", "authorization_endpoint": self.provider_origin + "/authorize",
                "token_endpoint": f"https://host.docker.internal:{self.provider_port}/token",
                "profile_endpoint": f"https://host.docker.internal:{self.provider_port}/account",
                "client_id": "fixture", "client_authentication": "client_secret_basic", "scopes": ["identity"],
                "subject_pointer": "/account/key", "subject_type": "string", "name_pointer": "/account/display",
                "profile_headers": {"x-profile-version": "2"},
                "membership": {"mode": "provider_groups", "claim_source": "profile", "claim_pointer": "/roles",
                    "key_mode": "immutable_id", "refresh_interval_seconds": 5, "max_stale_seconds": 15}}
        definition.update({"admission": {"mode": "provision_without_grants"}, "credential": {"action": "reference", "alias": "company"}})
        provider = {"name": "company", "enabled": True, "source": "runtime", "kind": "oauth", "configuration": definition}
        candidate = self.api("/api/v1/admin/runtime-settings/security-candidates",
            {"group": "authentication", "expected_revision": 1, "providers": [provider]}, token=self.admin)
        self.provider_id = candidate["providers"][0]["provider_id"]
        if self.github_client:
            print(f"Set the OAuth App callback to {self.callback}", flush=True)
            input("Press Enter after saving the callback: ")
        interaction = self.begin(purpose="provider_verification", candidate_id=candidate["id"])
        self.authorize(interaction)
        verified = self.complete(interaction)
        assert "access_token" not in verified and verified["candidate"]["state"] == "verified"
        self.passed("non-activating OAuth candidate preview")
        approval = self.api("/api/v1/iam/credential-approvals", {"password": self.local_password, "operation": {
            "kind": "activate_security_candidate", "candidate_id": candidate["id"], "group": "authentication", "expected_revision": 1}}, token=self.admin)
        status, _, _ = self.browser.request(self.origin + f"/api/v1/admin/runtime-settings/security-candidates/{candidate['id']}/activate",
            {"expected_revision": 1, "verification_receipt": verified["candidate"]["verification_receipt"]},
            headers={"Authorization": "Bearer " + self.admin, "X-Hubuum-Credential-Approval": approval["approval"]})
        assert status == 200, "OAuth activation failed"
        self.passed("verified OAuth activation with fresh local approval")
        methods = self.api("/api/v1/auth/methods")["methods"]
        selected = next(method for method in methods if method["provider_id"] == self.provider_id)
        assert selected["kind"] == "oauth" and selected["fresh_authentication"] is False
        self.passed("accurate OAuth method discovery")
        for delivery in ("poll", "bff"):
            extra = {} if delivery == "poll" else {"delivery": "bff", "return_target_id": "bff", "return_nonce": secrets.token_urlsafe(32)}
            interaction = self.begin(**extra)
            self.authorize(interaction)
            session = self.complete(interaction)
            assert session["access_token"]
            self.api("/api/v1/auth/validate", token=session["access_token"])
            self.passed(f"OAuth {delivery} login")
        time.sleep(6)
        self.api("/api/v1/auth/validate", token=session["access_token"])
        self.passed("encrypted upstream credential eligibility renewal")
        response = self.api("/api/v1/auth/interactions", {"client_id": "oidc-qa", "provider_id": self.provider_id,
            "purpose": "credential_approval", "delivery": "poll", "operation": {"kind": "create_token",
            "principal_id": self.api("/api/v1/iam/me", token=session["access_token"])["principal"]["principal_id"], "token": {"name": "must-not-create"}}},
            token=session["access_token"], expected=(422,))
        assert response["reason"] == "capability_unsupported"
        self.passed("fresh OAuth credential approval explicitly unsupported")
        self.api("/api/v1/auth/logout", {}, token=session["access_token"])
        self.api("/api/v1/auth/validate", token=session["access_token"], expected=(401,))
        self.passed("local logout invalidates OAuth session")

    def cleanup(self):
        if self.provider_http:
            self.provider_http.shutdown()
            self.provider_http.server_close()
        super().cleanup()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--provider-port", type=int, default=48479)
    parser.add_argument("--server-port", type=int, default=48480)
    parser.add_argument("--github-client")
    parser.add_argument("--github-secret-file", type=Path)
    options = parser.parse_args(argv)
    if bool(options.github_client) != bool(options.github_secret_file):
        parser.error("GitHub mode needs both --github-client and --github-secret-file")
    fixture = Fixture(options.image, options.evidence, options.provider_port, options.server_port,
                      options.github_client, options.github_secret_file)
    try:
        fixture.start()
        fixture.qualify()
    except Exception as error:
        fixture.failed(error)
        raise SystemExit(1) from None
    finally:
        fixture.cleanup()


if __name__ == "__main__":
    main()
