"""Pinned real Keycloak qualification using only Python's standard library.

Runs an explicitly selected production Hubuum image in disposable Docker
containers. Secrets and private keys stay in an ephemeral directory; evidence
contains only assertions, image identities and non-secret configuration.
"""
from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import html.parser
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import shutil
import ssl
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

KEYCLOAK = "quay.io/keycloak/keycloak:26.8.2@sha256:c68e286cacd87b942ac1939a0a1481461109d6acd3a57dc496969a319a76bbf0"
POSTGRES = "postgres:18.6@sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722"


def command(*args, input=None):
    try:
        result = subprocess.run(args, input=input, text=True, capture_output=True, check=False, timeout=600)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{args[0]} exceeded fixture command deadline") from None
    if result.returncode:
        # Commands may contain fixture credentials. Never print arguments/output.
        raise RuntimeError(f"{args[0]} failed with exit status {result.returncode}")
    return result.stdout.strip()


class Forms(html.parser.HTMLParser):
    def __init__(self, document):
        super().__init__(convert_charrefs=True)
        self.forms = []
        self.links = []
        self.current = None
        self.feed(document)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "form":
            self.current = {"action": attrs.get("action", ""), "fields": {}}
            self.forms.append(self.current)
        elif tag == "input" and self.current is not None and attrs.get("name"):
            self.current["fields"][attrs["name"]] = attrs.get("value", "")
        elif tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])

    def handle_endtag(self, tag):
        if tag == "form":
            self.current = None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Browser:
    def __init__(self, ca):
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(ca))),
            urllib.request.HTTPCookieProcessor(self.cookies), NoRedirect())

    def request(self, url, body=None, *, method=None, headers=None, form=False):
        headers = dict(headers or {})
        if body is not None:
            headers["Content-Type"] = ("application/x-www-form-urlencoded" if form else "application/json")
            body = (urllib.parse.urlencode(body) if form else json.dumps(body)).encode()
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            response = self.opener.open(request, timeout=40)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data = response.read(1024 * 1024)
            return response.status, response.headers, data.decode()

    def follow(self, url, body=None, *, headers=None, form=False, stop_origin=None):
        result = self.request(url, body, headers=headers, form=form)
        for _ in range(16):
            status, response_headers, content = result
            if status not in {301, 302, 303, 307, 308}:
                return url, status, content
            url = urllib.parse.urljoin(url, response_headers["Location"])
            if stop_origin and url.startswith(stop_origin):
                return url, status, content
            result = self.request(url)
        raise AssertionError("Redirect chain exceeded bound")

    def submit(self, url, document, fields=None):
        forms = Forms(document).forms
        if len(forms) != 1:
            raise AssertionError(f"Expected one form, received {len(forms)}")
        form = forms[0]
        values = {**form["fields"], **(fields or {})}
        target = urllib.parse.urljoin(url, form["action"])
        origin = urllib.parse.urlsplit(url)
        return self.follow(target, values, form=True,
                           headers={"Origin": f"{origin.scheme}://{origin.netloc}"})


def totp(secret, at=None):
    counter = int(time.time() if at is None else at) // 30
    key = base64.b32decode(secret + "=" * (-len(secret) % 8))
    digest = hmac.new(key, struct.pack(">Q", counter), hashlib.sha1).digest()
    offset = digest[-1] & 15
    return f"{(struct.unpack('>I', digest[offset:offset + 4])[0] & 0x7fffffff) % 1000000:06d}"


class Fixture:
    def __init__(self, image, evidence, provider_port, server_port, group_source="id_token"):
        self.image = image
        self.group_source = group_source
        self.evidence = evidence
        self.root = Path(tempfile.mkdtemp(prefix="hubuum-oidc-fixture-"))
        self.prefix = "hubuum-oidc-" + uuid.uuid4().hex[:10]
        self.network = self.prefix + "-network"
        self.containers = []
        self.provider_port = provider_port
        self.server_port = server_port
        self.provider_origin = f"https://localhost:{provider_port}"
        self.issuer = self.provider_origin + "/realms/hubuum"
        self.origin = f"https://localhost:{server_port}"
        self.client_secret = secrets.token_urlsafe(32)
        self.password = secrets.token_urlsafe(24)
        self.admin_password = secrets.token_urlsafe(24)
        self.otp_secret = base64.b32encode(secrets.token_bytes(20)).decode().rstrip("=")
        self.results = []
        self.last_begin = 0.0
        self.stage = "initialization"
        self.evidence.mkdir(parents=True, exist_ok=True)
        for name in ("failure.json", "results.json", "images.json"):
            (self.evidence / name).unlink(missing_ok=True)

    def env(self, name, values):
        path = self.root / (name + ".env")
        path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))
        path.chmod(0o600)
        return str(path)

    def run(self, name, *arguments):
        name = self.prefix + "-" + name
        command("docker", "run", "--detach", "--name", name,
                "--label", "codex.task=hubuum-577", *arguments)
        self.containers.append(name)
        return name

    def certificates(self):
        mount = self.root / "mount"
        mount.mkdir(mode=0o755)
        command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                "-subj", "/CN=Hubuum disposable fixture CA", "-keyout", str(self.root / "ca.key"),
                "-out", str(mount / "ca.pem"), "-addext", "basicConstraints=critical,CA:TRUE",
                "-addext", "keyUsage=critical,keyCertSign,cRLSign")
        command("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
                "-keyout", str(mount / "server.key"), "-out", str(self.root / "server.csr"))
        extensions = self.root / "extensions"
        extensions.write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
        command("openssl", "x509", "-req", "-in", str(self.root / "server.csr"), "-CA", str(mount / "ca.pem"),
                "-CAkey", str(self.root / "ca.key"), "-CAcreateserial", "-days", "2", "-extfile", str(extensions),
                "-out", str(mount / "server.pem"))
        # Mounted fixture secrets are readable by the images' unprivileged users;
        # the enclosing host temporary directory remains mode 0700.
        (mount / "server.key").chmod(0o644)
        self.browser = Browser(mount / "ca.pem")
        return mount

    def start(self):
        self.stage = "certificates"
        mount = self.certificates()
        realm = {
            "realm": "hubuum", "enabled": True, "sslRequired": "all",
            "revokeRefreshToken": True, "refreshTokenMaxReuse": 0,
            "groups": [{"name": "engineering"}],
            "clients": [{"clientId": "hubuum", "enabled": True, "protocol": "openid-connect",
                "publicClient": False, "secret": self.client_secret, "standardFlowEnabled": True,
                "directAccessGrantsEnabled": False, "implicitFlowEnabled": False,
                "redirectUris": [], "attributes": {"pkce.code.challenge.method": "S256"},
                "protocolMappers": [{"name": "groups", "protocol": "openid-connect",
                    "protocolMapper": "oidc-group-membership-mapper", "consentRequired": False,
                    "config": {"full.path": "true", "claim.name": "groups", "id.token.claim": "true",
                               "access.token.claim": "true", "userinfo.token.claim": "true"}}]}],
            "users": [{"username": "alice", "enabled": True, "emailVerified": True,
                "email": "alice@example.invalid", "firstName": "Alice", "lastName": "Fixture",
                "groups": ["/engineering"], "credentials": [{"type": "password", "value": self.password,
                                                               "temporary": False}]}],
        }
        realm["users"].append({"username": "logout", "enabled": True, "emailVerified": True,
            "email": "logout@example.invalid", "firstName": "Logout", "lastName": "Fixture", "groups": ["/engineering"],
            "credentials": [{"type": "password", "value": self.password, "temporary": False}]})
        realm["users"].append({"username": "mfa", "enabled": True, "emailVerified": True,
            "email": "mfa@example.invalid", "firstName": "MFA", "lastName": "Fixture", "groups": ["/engineering"],
            "credentials": [{"type": "password", "value": self.password, "temporary": False},
                {"type": "otp", "secretData": json.dumps({"value": self.otp_secret}),
                 "credentialData": json.dumps({"subType": "totp", "digits": 6, "counter": 0, "period": 30,
                                               "algorithm": "HmacSHA1", "secretEncoding": "BASE32"})}]})
        (mount / "realm.json").write_text(json.dumps(realm))
        command("docker", "network", "create", "--label", "codex.task=hubuum-577", self.network)
        database_password = secrets.token_urlsafe(24)
        self.stage = "database startup"
        self.database = self.run("postgres", "--network", self.network, "--network-alias", "postgres",
            "--tmpfs", "/var/lib/postgresql", "--env-file", self.env("postgres", {
                "POSTGRES_PASSWORD": database_password, "POSTGRES_DB": "hubuum"}), POSTGRES)
        self.stage = "provider startup"
        self.keycloak = self.run("keycloak", "--network", self.network,
            "--publish", f"127.0.0.1:{self.provider_port}:{self.provider_port}",
            "--publish", f"127.0.0.1:{self.server_port}:{self.server_port}",
            "--mount", f"type=bind,src={mount},dst=/fixture,readonly",
            "--mount", f"type=bind,src={mount / 'realm.json'},dst=/opt/keycloak/data/import/realm.json,readonly",
            "--env-file", self.env("keycloak", {"KC_BOOTSTRAP_ADMIN_USERNAME": "fixture-admin",
                "KC_BOOTSTRAP_ADMIN_PASSWORD": self.admin_password}), KEYCLOAK, "start", "--import-realm",
            "--hostname", self.provider_origin, "--https-port", str(self.provider_port),
            "--https-certificate-file", "/fixture/server.pem", "--https-certificate-key-file", "/fixture/server.key")
        self.wait(self.issuer + "/.well-known/openid-configuration")
        self.stage = "server migration"
        server_env = self.env("server", {
            "HUBUUM_DATABASE_URL": f"postgres://postgres:{database_password}@postgres/hubuum",
            "HUBUUM_BIND_IP": "0.0.0.0", "HUBUUM_BIND_PORT": self.server_port,
            # The published ports bind only host loopback; Docker NAT changes
            # the peer address seen by the isolated server container.
            "HUBUUM_CLIENT_ALLOWLIST": "*",
            "HUBUUM_TLS_CERT_PATH": "/fixture/server.pem", "HUBUUM_TLS_KEY_PATH": "/fixture/server.key",
            "SSL_CERT_FILE": "/fixture/ca.pem", "HUBUUM_AUTH_RUNTIME_ALLOW_PRIVATE_TARGETS": "true",
            "HUBUUM_AUTH_BROWSER": json.dumps({"origin": self.origin, "return_targets": [
                {"id": "bff", "client_id": "oidc-qa", "uri": self.origin + "/fixture-callback"}]}),
            "HUBUUM_OIDC_SECRET_COMPANY": self.client_secret,
            "HUBUUM_RUNTIME_SETTINGS_ENCRYPTION_KEY_REF": "fixture",
            "HUBUUM_RUNTIME_SETTINGS_KEY_FIXTURE": secrets.token_hex(16),
            "HUBUUM_TOKEN_HASH_KEY": secrets.token_urlsafe(48),
        })
        self.server_env = server_env
        command("docker", "run", "--rm", "--network", self.network, "--env-file", server_env,
                "--mount", f"type=bind,src={mount},dst=/fixture,readonly",
                "--entrypoint", "hubuum-admin", self.image, "--migrate")
        self.stage = "server startup"
        self.server = self.run("server", "--network", "container:" + self.keycloak,
            "--env-file", server_env, "--mount", f"type=bind,src={mount},dst=/fixture,readonly", self.image)
        self.wait(self.origin + "/healthz")
        output = command("docker", "exec", self.server, "hubuum-admin", "--reset-password", "admin")
        self.local_password = output.rsplit("reset to: ", 1)[1].splitlines()[0]
        self.admin = self.api("/api/v1/auth/login", {"name": "admin", "password": self.local_password,
                                                   "client_id": "oidc-qa"})["access_token"]
        self.stage = "local administration"
        identity = {"provider": KEYCLOAK, "database": POSTGRES,
            "server": json.loads(command("docker", "image", "inspect", self.image))[0]["Id"],
            "source_commit": command("git", "rev-parse", "HEAD"),
            "source_dirty": bool(command("git", "status", "--porcelain")),
            "fixture_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "group_source": self.group_source}
        (self.evidence / "images.json").write_text(json.dumps(identity, indent=2) + "\n")

    def wait(self, url):
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            try:
                if self.browser.request(url)[0] == 200:
                    return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(1)
        raise AssertionError("Fixture readiness deadline exceeded")

    def api(self, path, body=None, *, method=None, token=None, expected=(200, 201)):
        self.stage = "API " + path.split("?")[0]
        headers = {"Authorization": "Bearer " + token} if token else {}
        status, _, raw = self.browser.request(self.origin + path, body, method=method, headers=headers)
        if status not in expected:
            # Only stable sanitized API reasons enter the report.
            try:
                reason = json.loads(raw).get("reason", "unspecified")
            except ValueError:
                reason = "non-json"
            raise AssertionError(f"API {path.split('?')[0]} returned {status} ({reason})")
        return json.loads(raw) if raw else None

    def provider_api(self, path, body=None, *, method=None):
        self.stage = "provider administration"
        status, _, raw = self.browser.request(self.provider_origin + "/realms/master/protocol/openid-connect/token",
            {"grant_type": "password", "client_id": "admin-cli", "username": "fixture-admin", "password": self.admin_password}, form=True)
        assert status == 200, "Fixture administration failed"
        self.provider_admin = json.loads(raw)["access_token"]
        status, _, raw = self.browser.request(self.provider_origin + "/admin/realms/hubuum" + path, body, method=method,
            headers={"Authorization": "Bearer " + self.provider_admin})
        assert status in {200, 201, 204}, f"Provider administration returned {status}"
        return json.loads(raw) if raw else None

    def begin(self, token=None, **extra):
        # Begin, browser attachment, callback and its replay each consume the
        # production 30/minute admission budget. Pace this accelerated browser
        # driver so it qualifies protocol defenses with normal limits enabled.
        time.sleep(max(0, 8 - (time.monotonic() - self.last_begin)))
        self.last_begin = time.monotonic()
        interaction = self.api("/api/v1/auth/interactions", {
            "client_id": "oidc-qa", "provider_id": self.provider_id, "purpose": "login", "delivery": "poll", **extra,
        }, token=token or (self.admin if extra.get("purpose") == "provider_verification" else None))
        if "operation" in extra:
            interaction["_operation"] = extra["operation"]
        return interaction

    def complete(self, interaction, expected=(200,)):
        status, _, raw = self.browser.request(self.origin + f'/api/v1/auth/interactions/{interaction["id"]}/complete',
            {"client_id": "oidc-qa"}, headers={"Authorization": "Hubuum-Interaction " + interaction["redemption_secret"]})
        assert status in expected, f"Interaction completion returned {status}"
        return json.loads(raw)

    def authorize(self, interaction, browser=None, decision="approve", mutate_callback=None, username="alice", defer_callback=False):
        self.stage = "browser authorization"
        browser = browser or Browser(self.root / "mount" / "ca.pem")
        url, status, page = browser.follow(interaction["next"]["verification_uri"])
        assert status == 200, "Hosted entry unavailable"
        url, status, page = browser.submit(url, page, {"user_code": interaction["next"]["user_code"]})
        assert status == 200, "Browser attachment failed"
        url, status, page = browser.submit(url, page)
        assert status == 200, "Provider redirect unavailable"
        links = [link for link in Forms(page).links if link.startswith(self.provider_origin)]
        assert len(links) == 1, "Expected one trusted provider link"
        authorization = urllib.parse.parse_qs(urllib.parse.urlsplit(links[0]).query)
        assert authorization["code_challenge_method"] == ["S256"]
        assert authorization["response_type"] == ["code"]
        if "_operation" in interaction:
            assert authorization["max_age"] == ["0"]
        url, status, page = browser.follow(links[0], stop_origin=self.origin)
        prompted = False
        while not url.startswith(self.origin):
            assert status == 200, "Provider sign-in unavailable"
            forms = Forms(page).forms
            assert forms, "Expected provider authentication form"
            fields = forms[0]["fields"]
            if "password" in fields:
                values = {"username": username, "password": self.password}
                prompted = True
            elif "otp" in fields:
                values = {"otp": totp(self.otp_secret)}
                browser.otp_completed = True
            else:
                raise AssertionError("Unexpected provider authentication step")
            target = urllib.parse.urljoin(url, forms[0]["action"])
            url, status, page = browser.follow(target, {**fields, **values}, form=True, stop_origin=self.origin,
                headers={"Origin": self.provider_origin})
        callback = url
        if defer_callback:
            return browser, callback, prompted
        if mutate_callback:
            callback = mutate_callback(callback)
        status, _, page = browser.request(callback)
        if mutate_callback:
            assert status == 401, "Invalid callback was accepted"
            return browser, None, prompted
        assert status == 303, "OIDC callback rejected"
        # The callback's original URL is secret; keep it in memory for replay only.
        url, status, page = browser.follow(self.origin + f'/api/v1/auth/interactions/browser/{interaction["id"]}')
        assert status == 200 and "Approve this request?" in page, "Explicit approval missing"
        url, status, page = browser.submit(url, page, {"decision": decision})
        assert status in {200, 404}, "Decision failed"
        replay_status = browser.request(callback)[0]
        assert replay_status in {401, 409}, f"Callback replay returned unexpected status {replay_status}"
        return browser, url, prompted

    def passed(self, name):
        self.results.append({"case": name, "passed": True})
        (self.evidence / "results.json").write_text(json.dumps(self.results, indent=2) + "\n")
        print(f"PASS {name}", flush=True)

    def failed(self, error):
        report = {"stage": self.stage, "error_type": type(error).__name__}
        # Only our controlled assertion/command messages are safe for evidence.
        # urllib exceptions may embed an authorization URL or response material.
        if isinstance(error, (AssertionError, RuntimeError)):
            report["reason"] = str(error)
        (self.evidence / "failure.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"FAIL {self.stage} ({type(error).__name__})", flush=True)

    def qualify(self):
        definition = {"issuer": self.issuer, "client_id": "hubuum", "client_authentication": "client_secret_basic",
            "scopes": ["openid", "profile", "email"], "signing_algorithms": ["RS256"], "retain_refresh_tokens": True,
            "membership": {"mode": "provider_groups", "refresh_interval_seconds": 5, "max_stale_seconds": 15,
                "claim_source": self.group_source, "claim_pointer": "/groups", "key_mode": "full_path"},
            "admission": {"mode": "provision_without_grants"}, "credential": {"action": "reference", "alias": "company"}}
        self.provider_definition = {"name": "company", "enabled": True, "source": "runtime", "kind": "oidc", "configuration": definition}
        candidate = self.api("/api/v1/admin/runtime-settings/security-candidates",
            {"group": "authentication", "expected_revision": 1, "providers": [self.provider_definition]}, token=self.admin)
        self.provider_id = candidate["providers"][0]["provider_id"]
        client = self.provider_api("/clients?clientId=hubuum")[0]
        client["redirectUris"] = [self.origin + "/api/v1/auth/callbacks/" + self.provider_id]
        self.provider_api("/clients/" + client["id"], client, method="PUT")
        interaction = self.begin(purpose="provider_verification", candidate_id=candidate["id"])
        self.authorize(interaction)
        result = self.complete(interaction)
        assert "access_token" not in result and result["candidate"]["state"] == "verified"
        self.passed("candidate browser sign-in produces preview without session")
        approval = self.api("/api/v1/iam/credential-approvals", {"password": self.local_password, "operation": {
            "kind": "activate_security_candidate", "candidate_id": candidate["id"], "group": "authentication", "expected_revision": 1}}, token=self.admin)
        status, _, raw = self.browser.request(self.origin + f'/api/v1/admin/runtime-settings/security-candidates/{candidate["id"]}/activate',
            {"expected_revision": 1, "verification_receipt": result["candidate"]["verification_receipt"]},
            headers={"Authorization": "Bearer " + self.admin, "X-Hubuum-Credential-Approval": approval["approval"]})
        assert status == 200, "Candidate activation failed"
        self.passed("verified candidate activates with fresh local approval")
        interaction = self.begin()
        browser, _, prompted = self.authorize(interaction)
        self.session = self.complete(interaction)
        assert self.session["access_token"] and prompted
        self.passed("local CLI polling and one-use callback")
        interaction = self.begin()
        self.authorize(interaction)
        assert self.complete(interaction)["access_token"]
        self.passed("remote-browser CLI polling")
        interaction = self.begin()
        _, _, prompted = self.authorize(interaction, browser)
        assert self.complete(interaction)["access_token"] and not prompted
        self.passed("Keycloak SSO preserves explicit Hubuum approval")
        interaction = self.begin()
        self.authorize(interaction, decision="deny")
        assert self.complete(interaction, expected=(403,))["reason"] == "access_denied"
        self.passed("browser denial issues no session")
        interaction = self.begin(delivery="bff", return_target_id="bff", return_nonce=secrets.token_urlsafe(32))
        _, returned, _ = self.authorize(interaction)
        parameters = urllib.parse.parse_qs(urllib.parse.urlsplit(returned).query)
        assert set(parameters) == {"interaction_id", "nonce"} and parameters["interaction_id"] == [interaction["id"]]
        assert self.complete(interaction)["access_token"]
        self.passed("web BFF return contains only interaction identifier and nonce")
        def mixup(url):
            parsed = urllib.parse.urlsplit(url)
            parameters = urllib.parse.parse_qs(parsed.query)
            parameters["iss"] = [self.provider_origin + "/realms/another"]
            return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(parameters, doseq=True)))
        self.authorize(self.begin(), mutate_callback=mixup)
        self.passed("issuer mix-up rejected")
        mfa = self.begin()
        mfa_browser, _, _ = self.authorize(mfa, username="mfa")
        mfa_session = self.complete(mfa)
        assert getattr(mfa_browser, "otp_completed", False) and mfa_session["access_token"]
        self.passed("real Keycloak password plus TOTP")
        principal = int(self.sql("SELECT id FROM principals WHERE name='alice' AND provider_managed"))
        operation = {"kind": "create_token", "principal_id": principal, "token": {"name": "qualified-pat",
            "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).replace(tzinfo=None).isoformat()}}
        fresh = self.begin(token=self.session["access_token"], purpose="credential_approval", operation=operation)
        _, _, prompted = self.authorize(fresh, browser)
        approval = self.complete(fresh)
        assert prompted and approval["approval"]
        token_url = self.origin + f"/api/v1/iam/principals/{principal}/tokens"
        token_headers = {"Authorization": "Bearer " + self.session["access_token"],
                         "X-Hubuum-Credential-Approval": approval["approval"]}
        status, _, raw = self.browser.request(token_url, operation["token"], headers=token_headers)
        assert status == 201, f"Approved token creation returned {status}"
        self.pat = json.loads(raw)["token"]
        assert self.browser.request(token_url, operation["token"], headers=token_headers)[0] == 403, "Approval reused"
        self.passed("existing SSO must reauthenticate for operation-bound approval")
        before = self.provider_api("/keys")["active"]["RS256"]
        realm_id = self.provider_api("")["id"]
        self.provider_api("/components", {"name": "fixture-rotated", "parentId": realm_id,
            "providerType": "org.keycloak.keys.KeyProvider", "providerId": "rsa-generated",
            "config": {"priority": ["200"], "enabled": ["true"], "active": ["true"], "algorithm": ["RS256"], "keySize": ["2048"]}})
        assert self.provider_api("/keys")["active"]["RS256"] != before
        rotated = self.begin()
        self.authorize(rotated, browser)
        assert self.complete(rotated)["access_token"]
        self.passed("signing-key rotation observed without restart")
        user = self.provider_api("/users?username=alice&exact=true")[0]
        group = self.provider_api("/groups?search=engineering&exact=true")[0]
        self.provider_api(f'/users/{user["id"]}/groups/{group["id"]}', method="DELETE")
        time.sleep(6)
        self.api("/api/v1/auth/validate", token=self.pat)
        assert self.sql(f"SELECT count(*) FROM group_membership_sources WHERE principal_id={principal} AND source='external'") == "0"
        self.passed("due refresh removes provider-owned memberships")
        user["enabled"] = False
        self.provider_api("/users/" + user["id"], user, method="PUT")
        time.sleep(6)
        self.api("/api/v1/auth/validate", token=self.session["access_token"], expected=(401,))
        self.api("/api/v1/auth/validate", token=self.pat, expected=(401,))
        assert self.sql(f"SELECT count(*) FROM auth_upstream_credentials WHERE principal_id={principal}") == "0"
        self.passed("deprovisioned identity loses sessions and retained credentials")
        logout_interaction = self.begin()
        self.authorize(logout_interaction, username="logout")
        logout_session = self.complete(logout_interaction)
        command("docker", "pause", self.keycloak)
        try:
            started = time.monotonic()
            self.api("/api/v1/auth/logout", {}, token=logout_session["access_token"])
            assert time.monotonic() - started < 3, "Local logout waited for unavailable provider"
        finally:
            command("docker", "unpause", self.keycloak)
        session_id = str(uuid.UUID(logout_session["session_id"]))
        assert self.sql(f"SELECT count(*) FROM auth_upstream_credentials WHERE session_id='{session_id}'") == "0"
        self.api("/api/v1/auth/validate", token=logout_session["access_token"], expected=(401,))
        self.passed("local logout destroys credentials without waiting for unavailable IdP")
        pending = self.begin()
        pending_browser, callback, _ = self.authorize(pending, mfa_browser, username="mfa", defer_callback=True)
        disabled = json.loads(json.dumps(self.provider_definition))
        disabled.update({"provider_id": self.provider_id, "enabled": False})
        disabled["configuration"]["credential"] = {"action": "keep"}
        candidate = self.api("/api/v1/admin/runtime-settings/security-candidates",
            {"group": "authentication", "expected_revision": 2, "providers": [disabled]}, token=self.admin)
        approval = self.api("/api/v1/iam/credential-approvals", {"password": self.local_password, "operation": {
            "kind": "activate_security_candidate", "candidate_id": candidate["id"], "group": "authentication", "expected_revision": 2}}, token=self.admin)
        status, _, _ = self.browser.request(self.origin + f'/api/v1/admin/runtime-settings/security-candidates/{candidate["id"]}/activate',
            {"expected_revision": 2, "verification_receipt": candidate["verification_receipt"]},
            headers={"Authorization": "Bearer " + self.admin, "X-Hubuum-Credential-Approval": approval["approval"]})
        assert status == 200, "Provider disable failed"
        self.api("/api/v1/auth/validate", token=mfa_session["access_token"], expected=(401,))
        assert pending_browser.request(callback)[0] in {401, 409}, "Disabled provider callback completed"
        assert self.sql("SELECT count(*) FROM auth_upstream_credentials") == "0"
        self.passed("provider disable revokes sessions, retained credentials and in-flight callback")


    def sql(self, query):
        return command("docker", "exec", "-i", self.database, "psql", "-U", "postgres", "-d", "hubuum",
                       "-At", "-v", "ON_ERROR_STOP=1", input=query)


    def cleanup(self):
        for container in reversed(self.containers):
            subprocess.run(["docker", "rm", "-f", "-v", container], capture_output=True, check=False)
        subprocess.run(["docker", "network", "rm", self.network], capture_output=True, check=False)
        shutil.rmtree(self.root)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--provider-port", type=int, default=48477)
    parser.add_argument("--server-port", type=int, default=48478)
    parser.add_argument("--group-source", choices=("id_token", "userinfo"), default="id_token")
    options = parser.parse_args()
    fixture = Fixture(options.image, options.evidence, options.provider_port, options.server_port, options.group_source)
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
