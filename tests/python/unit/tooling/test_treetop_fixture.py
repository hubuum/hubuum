"""Regression tests for the ordered Treetop fixture server."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import functools
import http.server
import pathlib
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from support import treetop_server as MODULE


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
        self.schema_loaded = threading.Event()
        self.schema_loaded.set()
        self.readiness_checked = threading.Event()

        def schema_ready() -> bool:
            self.readiness_checked.set()
            return self.schema_loaded.is_set()

        handler = functools.partial(
            QuietFixtureHandler,
            directory=self.directory.name,
            schema_permits=threading.Semaphore(0),
            schema_ready=schema_ready,
            schema_wait_timeout=0.2,
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
        raised.exception.close()

    def test_policy_waits_for_treetop_to_load_the_fetched_schema(self) -> None:
        self.schema_loaded.clear()
        self.assertEqual(self.request("/schema.json"), 200)
        with ThreadPoolExecutor(max_workers=1) as executor:
            policy = executor.submit(self.request, "/test-fixture.cedar")
            self.assertTrue(self.readiness_checked.wait(timeout=1))
            self.assertFalse(policy.done())
            self.schema_loaded.set()
            self.assertEqual(policy.result(timeout=1), 200)

    def test_policy_rejects_a_schema_that_was_fetched_but_not_loaded(self) -> None:
        self.schema_loaded.clear()
        self.assertEqual(self.request("/schema.json"), 200)
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.request("/test-fixture.cedar")
        self.assertEqual(raised.exception.code, 503)
        raised.exception.close()

    def test_each_policy_get_consumes_one_schema_fetch(self) -> None:
        for _ in range(10):
            self.assertEqual(self.request("/schema.json"), 200)
            self.assertEqual(self.request("/test-fixture.cedar"), 200)

            with self.assertRaises(urllib.error.HTTPError) as raised:
                self.request("/test-fixture.cedar")
            self.assertEqual(raised.exception.code, 503)
            raised.exception.close()
