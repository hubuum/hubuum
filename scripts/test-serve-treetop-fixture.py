#!/usr/bin/env python3
"""Regression tests for the ordered Treetop fixture server."""

from __future__ import annotations

import functools
import http.server
import importlib.util
import pathlib
import tempfile
import threading
import unittest
import urllib.error
import urllib.request


SCRIPT_PATH = pathlib.Path(__file__).with_name("serve-treetop-fixture.py")
SPEC = importlib.util.spec_from_file_location("serve_treetop_fixture", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class QuietFixtureHandler(MODULE.FixtureHandler):
    def log_message(self, format: str, *args: object) -> None:
        del format, args


class FixtureServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        fixture_dir = pathlib.Path(self.directory.name)
        (fixture_dir / "schema.json").write_text("{}", encoding="utf-8")
        (fixture_dir / "test-fixture.cedar").write_text(
            "permit (principal, action, resource);",
            encoding="utf-8",
        )
        handler = functools.partial(
            QuietFixtureHandler,
            directory=self.directory.name,
            schema_permits=threading.Semaphore(0),
            schema_wait_timeout=0.05,
        )
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=1)
        self.directory.cleanup()

    def request(self, path: str, method: str = "GET") -> int:
        request = urllib.request.Request(f"{self.base_url}{path}", method=method)
        with urllib.request.urlopen(request, timeout=1) as response:
            return response.status

    def test_policy_head_does_not_wait_for_schema(self) -> None:
        self.assertEqual(self.request("/test-fixture.cedar", method="HEAD"), 200)

    def test_policy_get_requires_a_schema_fetch(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.request("/test-fixture.cedar")

        self.assertEqual(raised.exception.code, 503)

    def test_each_policy_get_consumes_one_schema_fetch(self) -> None:
        for _ in range(10):
            self.assertEqual(self.request("/schema.json"), 200)
            self.assertEqual(self.request("/test-fixture.cedar"), 200)

            with self.assertRaises(urllib.error.HTTPError) as raised:
                self.request("/test-fixture.cedar")
            self.assertEqual(raised.exception.code, 503)


if __name__ == "__main__":
    unittest.main()
