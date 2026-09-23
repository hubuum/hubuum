#!/usr/bin/env python3
"""Exercise compatibility credential requests against a local HTTP fixture."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest


HELPER = Path(__file__).with_name("adjacent-release-api.sh")
USER = {"name": "fixture-user", "password": "new-fixture-password"}
TOKEN = {"name": "fixture-token", "scope": {"permissions": ["ReadObject"]}}
EXPIRY = "2026-09-23T12:34:56.123456"
APPROVAL = {"approval": "fixture-approval", "token_expires_at": EXPIRY}


class CredentialRequestTests(unittest.TestCase):
    def request(self, command, payload, responses):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def handle_request(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                requests.append({
                    "method": self.command,
                    "path": self.path,
                    "headers": {key.lower(): value for key, value in self.headers.items()},
                    "body": json.loads(body) if body else None,
                })
                index = len(requests) - 1
                status, response = (responses[index] if index < len(responses)
                                    else (500, {"error": "unexpected request"}))
                raw = json.dumps(response).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            do_POST = handle_request
            do_GET = handle_request

            def log_message(self, *_args):
                pass

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
            thread.start()
            try:
                with tempfile.TemporaryDirectory() as directory:
                    result = subprocess.run([
                        "bash", "-c", """
set -euo pipefail
test_root="$1"
fixture_url="$2"
source "$3"
admin_token=fixture-token
admin_password=fixture-password
service_url() { printf '%s' "$fixture_url"; }
""" + command,
                        "fixture", directory, f"http://127.0.0.1:{server.server_port}",
                        str(HELPER), json.dumps(payload),
                    ], capture_output=True, text=True, timeout=10)
            finally:
                server.shutdown()
                thread.join(timeout=5)
        return result, requests

    def test_user_creation_preserves_the_approved_request_and_bearer(self):
        result, requests = self.request('create_user previous-api "$4"', USER,
                                        [(201, APPROVAL), (201, {"id": 7})])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(requests[0]["path"], "/api/v1/iam/credential-approvals")
        self.assertEqual(requests[0]["body"], {
            "password": "fixture-password", "operation": {"kind": "create_user", "user": USER},
        })
        self.assertEqual(requests[1]["path"], "/api/v1/iam/users")
        self.assertEqual(requests[1]["body"], USER)
        self.assertEqual(requests[1]["headers"]["x-hubuum-credential-approval"], "fixture-approval")
        self.assertTrue(all(request["headers"]["authorization"] == "Bearer fixture-token"
                            for request in requests))

    def test_token_creation_copies_the_exact_approved_expiry(self):
        for requested_expiry in (None, "2026-09-23T12:34:56.123456789"):
            with self.subTest(requested_expiry=requested_expiry):
                token = TOKEN | {"expires_at": requested_expiry}
                result, requests = self.request('create_principal_token previous-api 7 "$4"', token,
                                                [(201, APPROVAL), (201, {"token": "issued"})])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(requests[0]["body"]["operation"], {
                    "kind": "create_token", "principal_id": 7, "token": token,
                })
                self.assertEqual(requests[1]["path"], "/api/v1/iam/principals/7/tokens")
                self.assertEqual(requests[1]["body"], token | {"expires_at": EXPIRY})
                self.assertEqual(requests[1]["headers"]["x-hubuum-credential-approval"],
                                 "fixture-approval")

    def test_absent_approval_endpoint_preserves_legacy_requests(self):
        for command, payload in (('create_user previous-api "$4"', USER),
                                 ('create_principal_token previous-api 7 "$4"', TOKEN)):
            with self.subTest(command=command):
                result, requests = self.request(command, payload, [(404, {}), (201, {"id": 7})])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(requests[1]["body"], payload)
                self.assertNotIn("x-hubuum-credential-approval", requests[1]["headers"])

    def test_rejected_approval_never_submits_the_credential_write(self):
        for status in (401, 403, 429, 503):
            with self.subTest(status=status):
                result, requests = self.request('create_user previous-api "$4"', USER,
                                                [(status, {"error": "approval rejected"})])
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(requests), 1)

    def test_malformed_approval_never_submits_the_credential_write(self):
        for response in ({}, {"approval": ""}, {"approval": 12},
                         {"approval": "fixture-approval"},
                         APPROVAL | {"token_expires_at": None}):
            with self.subTest(response=response):
                result, requests = self.request('create_principal_token previous-api 7 "$4"', TOKEN,
                                                [(201, response)])
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(requests), 1)

    def test_unexpected_approval_success_status_is_rejected(self):
        for status in (200, 204):
            with self.subTest(status=status):
                result, requests = self.request('create_user previous-api "$4"', USER,
                                                [(status, APPROVAL)])
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(requests), 1)

    def test_single_use_approval_is_not_sent_with_later_requests(self):
        result, requests = self.request(
            'create_user previous-api "$4"\napi_request previous-api GET /api/v1/iam/groups',
            USER, [(201, APPROVAL), (201, {"id": 7}), (200, [])],
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("x-hubuum-credential-approval", requests[2]["headers"])

    def test_ordinary_not_found_response_is_still_an_error(self):
        result, _ = self.request('api_request previous-api GET /missing', {}, [(404, {})])
        self.assertNotEqual(result.returncode, 0)

    def test_rejected_approved_write_is_not_retried_without_approval(self):
        result, requests = self.request('create_user previous-api "$4"', USER,
                                        [(201, APPROVAL), (403, {"error": "rejected"})])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(requests), 2)


if __name__ == "__main__":
    unittest.main()
